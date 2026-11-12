using System.Security.Cryptography;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Modules.Clusters;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

internal static class RegisteredHttpServiceEndpoints
{
    private static readonly SemaphoreSlim Probes = new(4);
    private static readonly SemaphoreSlim CatalogCreations = new(4);

    internal static void MapRegisteredHttpServices(this WebApplication app, ManagedIdentityStore store,
        RegisteredHttpServiceRuntime runtime)
    {
        MapService(app, store, runtime, RegisteredHttpService.SchemaRegistry, "schema-registry", "Schema Registry");
        MapService(app, store, runtime, RegisteredHttpService.KafkaConnect, "kafka-connect", "Kafka Connect");
        MapService(app, store, runtime, RegisteredHttpService.KsqlDb, "ksqldb", "ksqlDB");

        MapCatalogCreation(app, store, runtime, KsqlCatalogKind.Stream);
        MapCatalogCreation(app, store, runtime, KsqlCatalogKind.Table);
    }

    private static void MapCatalogCreation(WebApplication app, ManagedIdentityStore store,
        RegisteredHttpServiceRuntime runtime, KsqlCatalogKind kind)
    {
        var resource = kind == KsqlCatalogKind.Table ? "table" : "stream";
        var eventType = kind == KsqlCatalogKind.Table ? SecurityAuditEventType.KsqlTableCreation : SecurityAuditEventType.KsqlStreamCreation;
        IResult Problem(int status, string code, string detail) => ApiResults.Problem(new ApiProblemDefinition(
            status, $"urn:kafdeck:problem:{code}", $"Create ksqlDB {resource}", detail, code));

        app.MapPost($"/api/v1/connections/{{clusterId}}/ksqldb/{resource}s",
            async (string clusterId, KsqlCatalogDefinition input, HttpContext context,
                ISecurityAuditSink audit, GovernanceRuntimeOptions governance, ClusterExplorerService clusters,
                CancellationToken cancellation) =>
            {
                context.Response.Headers.CacheControl = "no-store";
                if (Owner(context, store) is not { } owner)
                    return Problem(403, $"ksql_{resource}_create_denied", $"Only the installation administrator can create {resource}s.");
                if (!KsqlDbCatalogCreator.TryBuildStatement(kind, input, out var name, out var statement))
                    return Problem(400, $"ksql_{resource}_invalid", $"Check the {resource} and topic names, column definitions, format, and topic sizing.");
                if (governance.DeploymentGeneration is not null)
                    return Problem(403, "ksql_catalog_governed", "Direct ksqlDB catalog changes are unavailable in governed deployments.");
                using var client = runtime.OpenKsql(clusterId);
                if (client is null)
                    return Problem(404, "ksql_not_configured", "Configure ksqlDB for this Kafka connection first.");
                if (!CatalogCreations.Wait(0))
                    return Problem(429, $"ksql_{resource}_create_busy", "Too many ksqlDB creation requests are in progress.");

                KsqlCatalogCreation result;
                try
                {
                    using var preflight = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                    preflight.CancelAfter(TimeSpan.FromSeconds(4));
                    var policyStore = context.RequestServices.GetService<IGovernancePolicyStore>();
                    if (policyStore is null)
                        return Problem(503, "ksql_catalog_policy_unavailable", "The ksqlDB catalog policy could not be verified.");
                    GovernancePolicyState policy;
                    try
                    {
                        policy = await policyStore.GetStateAsync(clusterId, preflight.Token);
                    }
                    catch (Exception) when (!cancellation.IsCancellationRequested)
                    {
                        return Problem(503, "ksql_catalog_policy_unavailable", "The ksqlDB catalog policy could not be verified.");
                    }
                    if (policy.Enabled)
                        return Problem(403, "ksql_catalog_governed", "Direct ksqlDB catalog changes are unavailable while a policy is active.");
                    var providerId = await KsqlDbCatalogCreator.GetKafkaClusterIdAsync(client, preflight.Token);
                    ClusterProjection kafka;
                    try
                    {
                        kafka = await clusters.GetClusterAsync(clusterId, preflight.Token);
                    }
                    catch (OperationCanceledException) when (!cancellation.IsCancellationRequested)
                    {
                        return Problem(503, "ksql_catalog_cluster_unavailable", "The Kafka cluster could not be verified.");
                    }
                    if (string.IsNullOrEmpty(providerId) || kafka.Failure is not null ||
                        kafka.Observation.Source == ObservationSource.StaleSnapshot)
                        return Problem(503, "ksql_catalog_cluster_unavailable", "The Kafka cluster could not be verified.");
                    if (!string.Equals(providerId, kafka.KafkaClusterId, StringComparison.Ordinal))
                        return Problem(409, "ksql_catalog_cluster_mismatch", "The ksqlDB endpoint belongs to a different Kafka cluster.");
                    result = await KsqlDbCatalogCreator.CreateAsync(client, kind, input, name, statement, cancellation);
                }
                finally
                {
                    CatalogCreations.Release();
                }

                await audit.WriteAsync(new SecurityAuditEvent(DateTimeOffset.UtcNow, eventType,
                    SecurityAuditPrincipal.FromOperator(owner.Identity), null, clusterId, name,
                    result == KsqlCatalogCreation.Created ? SecurityAuditOutcome.Succeeded : SecurityAuditOutcome.Failed,
                    result switch
                    {
                        KsqlCatalogCreation.Created => $"ksql_{resource}_created",
                        KsqlCatalogCreation.Unconfirmed => $"ksql_{resource}_unconfirmed",
                        KsqlCatalogCreation.Denied => $"ksql_{resource}_provider_denied",
                        _ => $"ksql_{resource}_rejected",
                    }));

                return result switch
                {
                    KsqlCatalogCreation.Created => Results.Json(new { name, status = "created" }, statusCode: 201),
                    KsqlCatalogCreation.Unconfirmed => Results.Accepted(value: new { name, status = "unconfirmed" }),
                    KsqlCatalogCreation.Denied => Problem(403, $"ksql_{resource}_provider_denied", $"ksqlDB did not authorize {resource} creation."),
                    _ => Problem(422, $"ksql_{resource}_rejected", $"ksqlDB rejected the {resource}. Check for an existing name and verify the topic and format."),
                };
            })
            .WithManagedBrowserContract($"managed-ksqldb-{resource}s-create", typeof(KsqlCatalogDefinition))
            .WithMetadata(new RequestSizeLimitAttribute(32768)).RequireKafdeckAntiforgery();
    }

    private static void MapService(WebApplication app, ManagedIdentityStore store, RegisteredHttpServiceRuntime runtime,
        RegisteredHttpService service, string slug, string label)
    {
        var path = "/api/v1/connections/{clusterId}/" + slug;
        var (codePrefix, eventType, auditCode) = service switch
        {
            RegisteredHttpService.SchemaRegistry => ("registry", SecurityAuditEventType.SchemaRegistryConfigurationChanged, "schema_registry_configuration_saved"),
            RegisteredHttpService.KafkaConnect => ("connect", SecurityAuditEventType.KafkaConnectConfigurationChanged, "kafka_connect_configuration_saved"),
            RegisteredHttpService.KsqlDb => ("ksql", SecurityAuditEventType.KsqlDbConfigurationChanged, "ksqldb_configuration_saved"),
            _ => throw new ArgumentOutOfRangeException(nameof(service)),
        };

        IResult Problem(int status, string code, string detail) => ApiResults.Problem(new ApiProblemDefinition(
            status, $"urn:kafdeck:problem:{code}", $"{label} settings", detail, code));
        IResult Missing() => Problem(404, $"{codePrefix}-connection-missing", "The saved Kafka connection was not found.");
        IResult Denied() => Problem(403, $"{codePrefix}-settings-denied", $"Only the installation administrator can change {label} settings.");
        IResult Conflict() => Problem(409, $"{codePrefix}-settings-conflict", "The settings changed or another save is in progress. Reload the settings and try again.");

        app.MapGet(path, (string clusterId, HttpContext context) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            if (Owner(context, store) is null) return Denied();
            var target = Find(store.Read(), clusterId);
            return target is null ? Missing() : Results.Ok(PublicSettings(target.GetService(service)));
        }).WithManagedBrowserContract($"managed-{slug}-settings-get");

        async Task<IResult> Probe(string clusterId, RegisteredHttpServiceRuntime.SettingsInput input,
            HttpContext context, ISecurityAuditSink audit, CancellationToken cancellation, bool save)
        {
            context.Response.Headers.CacheControl = "no-store";
            if (Owner(context, store) is null) return Denied();
            var target = Find(store.Read(), clusterId);
            if (target is null) return Missing();
            var current = target.GetService(service);
            if (input.Revision != current?.Revision) return Conflict();
            var candidate = runtime.Prepare(clusterId, service, input, current);
            if (candidate is null)
                return Problem(400, $"{codePrefix}-settings-invalid", $"Enter a valid {label} URL and credentials. Authenticated connections require HTTPS.");

            if (!Probes.Wait(0)) return Problem(429, $"{codePrefix}-settings-busy", $"Too many {label} connection tests are running.");
            try
            {
                var operation = new ReadViewOperationContext(DateTimeOffset.UtcNow.AddSeconds(8), 2048, 2 * 1024 * 1024);
                HttpClient OpenCandidate(string _) => runtime.Open(clusterId, service, candidate);
                if (service == RegisteredHttpService.SchemaRegistry)
                {
                    using var adapter = new ConfluentSchemaCatalogReadAdapter([], new SecretResolver(), registeredClient: OpenCandidate);
                    var subjects = await adapter.ListSubjectsAsync(clusterId, operation, cancellation);
                    if (!subjects.IsSuccess) return ApiResults.Problem(ApiProblemMapper.FromReadView(subjects.Failure!));
                }
                else if (service == RegisteredHttpService.KafkaConnect)
                {
                    using var adapter = new KafkaConnectReadAdapter([], new SecretResolver(), registeredClient: OpenCandidate);
                    var info = await adapter.GetClusterInfoAsync(clusterId, operation, cancellation);
                    if (!info.IsSuccess) return ApiResults.Problem(ApiProblemMapper.FromReadView(info.Failure!));
                    if (string.IsNullOrWhiteSpace(info.Value?.Version))
                        return Problem(502, "connect-service-not-recognized", "The endpoint did not report a Kafka Connect version.");
                    var connectors = await adapter.ListConnectorsAsync(clusterId, operation, cancellation);
                    if (!connectors.IsSuccess) return ApiResults.Problem(ApiProblemMapper.FromReadView(connectors.Failure!));
                }
                else
                {
                    using var adapter = new KsqlDbMetadataReadAdapter([], new SecretResolver(), registeredClient: OpenCandidate);
                    var info = await adapter.GetServerInfoAsync(clusterId, operation, cancellation);
                    if (!info.IsSuccess) return ApiResults.Problem(ApiProblemMapper.FromReadView(info.Failure!));
                    if (string.IsNullOrWhiteSpace(info.Value?.Version))
                        return Problem(502, "ksql-service-not-recognized", "The endpoint did not report a ksqlDB version.");
                    var metadata = await adapter.ListMetadataAsync(clusterId, operation, cancellation);
                    if (!metadata.IsSuccess) return ApiResults.Problem(ApiProblemMapper.FromReadView(metadata.Failure!));
                }
            }
            catch (CryptographicException)
            {
                return Problem(400, $"{codePrefix}-credential-unavailable", $"Enter the {label} credentials again.");
            }
            finally { Probes.Release(); }

            if (!save) return Results.Ok(new { connected = true });
            cancellation.ThrowIfCancellationRequested();
            OperatorSessionContext owner;
            using (var lease = store.TryLock())
            {
                if (lease is null) return Conflict();
                if (Owner(context, store) is not { } currentOwner) return Denied();
                owner = currentOwner;
                var state = store.Read();
                var latest = Find(state, clusterId);
                if (latest is null) return Missing();
                if (latest.GetService(service)?.Revision != input.Revision) return Conflict();
                var updated = service switch
                {
                    RegisteredHttpService.SchemaRegistry => latest with { SchemaRegistry = candidate },
                    RegisteredHttpService.KafkaConnect => latest with { Connect = candidate },
                    RegisteredHttpService.KsqlDb => latest with { KsqlDb = candidate },
                    _ => throw new ArgumentOutOfRangeException(nameof(service)),
                };
                store.Write(state with
                {
                    Connections = state.Connections!.Select(item => item.Id == clusterId ? updated : item).ToArray(),
                });
            }
            await audit.WriteAsync(new SecurityAuditEvent(DateTimeOffset.UtcNow, eventType,
                SecurityAuditPrincipal.FromOperator(owner.Identity), null, clusterId, null,
                SecurityAuditOutcome.Succeeded, auditCode));
            return Results.Ok(PublicSettings(candidate));
        }

        app.MapPost(path + "/test", (string clusterId, RegisteredHttpServiceRuntime.SettingsInput input,
            HttpContext context, ISecurityAuditSink audit, CancellationToken token) => Probe(clusterId, input, context, audit, token, false))
            .WithManagedBrowserContract($"managed-{slug}-settings-test", typeof(RegisteredHttpServiceRuntime.SettingsInput))
            .WithMetadata(new RequestSizeLimitAttribute(32768)).RequireKafdeckAntiforgery();
        app.MapPut(path, (string clusterId, RegisteredHttpServiceRuntime.SettingsInput input,
            HttpContext context, ISecurityAuditSink audit, CancellationToken token) => Probe(clusterId, input, context, audit, token, true))
            .WithManagedBrowserContract($"managed-{slug}-settings-save", typeof(RegisteredHttpServiceRuntime.SettingsInput))
            .WithMetadata(new RequestSizeLimitAttribute(32768)).RequireKafdeckAntiforgery();
    }

    private static RegisteredKafkaConnection? Find(ManagedIdentityState state, string id) =>
        state.Connections?.SingleOrDefault(item => item.Id == id);

    private static OperatorSessionContext? Owner(HttpContext context, ManagedIdentityStore store) =>
        context.User.Identity is not NonBrowserReadIdentity &&
        OperatorSessionContextFactory.TryCreate(context.User, out var session) &&
        session is not null && ManagedIdentityEndpoints.IsOwner(store, session) ? session : null;

    private static object PublicSettings(RegisteredHttpServiceSettings? settings) => new
    {
        url = settings?.Url,
        authentication = settings?.Authentication ?? "none",
        revision = settings?.Revision,
    };

}
