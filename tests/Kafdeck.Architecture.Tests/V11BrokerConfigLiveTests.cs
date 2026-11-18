using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Topics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V11BrokerConfigLiveTests
{
    private const string Key = DynamicConfigurationRegistry.FleetKey;
    private const string Root = "/api/v1/clusters/broker-config-pilot/broker-config";
    private const string Issuer = "https://broker-config.test";

    [BrokerConfigLiveFact]
    public async Task Native_and_compose_use_the_governed_http_path_to_apply_restore_and_reject_stale_configuration()
    {
        var bootstrap = Required("BOOTSTRAP");
        Assert.All(bootstrap.Split(','), broker => Assert.StartsWith("127.0.0.1:", broker, StringComparison.Ordinal));
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        var profile = new ClusterProfile("broker-config-pilot", bootstrap.Split(','), KafkaSecurityProtocol.Plaintext, null, null);
        ClusterProfile[] profiles = [profile];
        var secrets = new SecretResolver();
        using var provider = new ConfluentKafkaDynamicConfigurationAdapter(profiles, secrets);
        using var kafka = new ConfluentKafkaAdministrationAdapter(profiles, secrets);
        using var consumers = new ConfluentKafkaConsumerMutationObservationAdapter(profiles, secrets);
        using var purge = new ConfluentKafkaRecordsPurgeObservationAdapter(profiles, secrets);
        var broker = new DynamicConfigurationTarget(profile.Id, DynamicConfigurationScope.BrokerOverride, 1, Key);
        var cluster = broker with { Scope = DynamicConfigurationScope.ClusterDefault, BrokerId = null };
        var registry = new DynamicConfigurationRegistry();

        async Task<DynamicConfigurationObservation> Observe(DynamicConfigurationTarget target)
        {
            var result = await provider.DescribeAsync(target, new(DateTimeOffset.UtcNow.AddSeconds(10)), token);
            Assert.True(result.IsSuccess, result.Failure?.Code);
            Assert.Equal(Required("CLUSTER_ID"), result.Value!.KafkaClusterId);
            return result.Value;
        }

        var initial = await Observe(broker);
        Assert.Equal("DefaultConfig", initial.EffectiveSource);
        Assert.Equal("15000", initial.EffectiveValue);

        var file = Path.Combine(Path.GetTempPath(), $"kafdeck-488-{Guid.NewGuid():N}.db");
        try
        {
            var options = new KafdeckOptions(new("http://127.0.0.1:0", null, AccessMode.Oidc, null), profiles,
                Administration: new(new MutationOptions(true, null, null, TimeSpan.FromMinutes(5), 2)));
            var read = new AuthorizationPermissionDefinition(AuthorizationAction.ClusterConfigRead, [profile.Id], ["*"]);
            var alter = new AuthorizationPermissionDefinition(AuthorizationAction.ClusterConfigAlter, [profile.Id], ["*"]);
            var clusterRead = new AuthorizationPermissionDefinition(AuthorizationAction.ClusterRead, [profile.Id], ["*"]);
            var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
                [new("read", [read, clusterRead]), new("change", [read, alter, clusterRead])],
                [new(Issuer, "viewer", ["read"]), new(Issuer, "operator", ["change"])], [])));
            var access = new KafdeckAuthorizationService(options, evaluator);
            var authorization = new MutationRequestAuthorizationService(access);
            var repo = new AdoMutationOperationRepository(new SqliteMutationDbConnectionFactory(file));
            await repo.InitializeAsync(token);
            var audit = new CapturedAudit();
            var dispatchContext = new MutationExecutionRequestContextAccessor();
            using var digest = new HmacMutationMaterialDigestService(new string('d', 32));
            var guard = new W39MutationPreDispatchGuard(dispatchContext, authorization,
                new TopicMutationPreconditionValidator(kafka), new RecordProductionPreconditionValidator(kafka),
                new ConsumerMutationPreconditionValidator(consumers), new RecordsPurgePreconditionValidator(purge),
                brokerConfig: new DynamicConfigurationPreconditionValidator(provider, registry));
            var executor = new MutationExecutor(repo, audit, guard,
                new([new DynamicBrokerConfigExecutionHandler(provider, TimeProvider.System)]),
                digest, new(2, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(2)));

            var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Production" });
            builder.WebHost.UseKestrel().UseUrls(options.Deployment.ListenUrl);
            builder.Services.ConfigureHttpJsonOptions(config =>
                config.SerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)));
            builder.Services.AddKafdeckAntiforgery(options.Deployment.ListenUrl);
            builder.Services.AddSingleton<IDataProtectionProvider>(new EphemeralDataProtectionProvider());
            builder.Services.AddSingleton<ISecurityAuditSink>(audit);
            builder.Services.AddSingleton(access);
            builder.Services.AddSingleton(authorization);
            builder.Services.AddSingleton(registry);
            builder.Services.AddSingleton(new DynamicConfigurationPlanner(provider, registry));
            builder.Services.AddSingleton<IMutationOperationRepository>(repo);
            builder.Services.AddSingleton(new MutationAdmissionService(repo, authorization, options));
            builder.Services.AddSingleton(new MutationCommandService(repo, new(evaluator), authorization));
            builder.Services.AddSingleton(new MutationDispatchService(repo, authorization, dispatchContext, digest, executor));
            await using var app = builder.Build();
            var viewer = Principal("viewer");
            var changeOperator = Principal("operator");
            app.UseRouting();
            app.Use((context, next) =>
            {
                context.User = context.Request.Headers["X-Pilot-Actor"] == "viewer" ? viewer : changeOperator;
                return next(context);
            });
            app.UseAntiforgery();
            app.MapKafdeckAntiforgeryEndpoint();
            app.MapKafdeckMutationEndpoints();
            app.MapKafdeckBrokerConfigEndpoints();
            await app.StartAsync(token);

            using var writer = await NewClient("operator");
            using var reader = await NewClient("viewer");
            async Task<HttpClient> NewClient(string actor)
            {
                var client = new HttpClient(new SocketsHttpHandler { UseProxy = false })
                {
                    BaseAddress = new Uri(Assert.Single(app.Urls)),
                    Timeout = TimeSpan.FromSeconds(35),
                };
                client.DefaultRequestHeaders.Add("X-Pilot-Actor", actor);
                using var csrf = await client.GetFromJsonAsync<JsonDocument>("/api/v1/auth/csrf", token);
                client.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName,
                    csrf!.RootElement.GetProperty("requestToken").GetString());
                return client;
            }

            async Task<JsonElement> Post(HttpClient client, string route, object body, HttpStatusCode expected)
            {
                using var req = new HttpRequestMessage(HttpMethod.Post, route) { Content = JsonContent.Create(body) };
                req.Headers.Add("Idempotency-Key", Guid.NewGuid().ToString("N"));
                using var response = await client.SendAsync(req, token);
                var payload = await response.Content.ReadAsStringAsync(token);
                Assert.True(response.StatusCode == expected, $"{route}: expected {expected}, got {response.StatusCode}. {payload}");
                using var json = JsonDocument.Parse(payload);
                return json.RootElement.Clone();
            }

            object Input(DynamicConfigurationTarget target, string? value, Guid? restore = null, string key = Key) =>
                new { key, scope = target.BrokerId is null ? "clusterDefault" : "brokerOverride", target.BrokerId,
                    command = restore.HasValue ? "restore" : value is null ? "inherit" : "set", value, restoreOperationId = restore };

            Task<JsonElement> Preview(DynamicConfigurationTarget target, string? value, Guid? restore = null) =>
                Post(writer, Root + "/preview", Input(target, value, restore), HttpStatusCode.Created);

            async Task<Guid> Execute(JsonElement preview, DynamicConfigurationTarget target, string value, string source, bool stale = false)
            {
                var operation = preview.GetProperty("operation");
                var id = operation.GetProperty("operationId").GetGuid();
                var route = $"/api/v1/mutations/{id:D}";
                var confirmed = await Post(writer, route + "/confirm", new
                {
                    previewHash = operation.GetProperty("previewHash").GetString(),
                    typedTargetChallenge = operation.GetProperty("confirmationChallenge").GetString(),
                }, HttpStatusCode.OK);
                Assert.Equal("ready", confirmed.GetProperty("state").GetString());
                var result = await Post(writer, route + "/execute", new { }, HttpStatusCode.OK);
                Assert.True(result.GetProperty("state").GetString() == (stale ? "stalePreview" : "appliedVerified"),
                    $"Unexpected governed execution for {value} / {source}: {result}");
                var live = await Observe(target);
                Assert.Equal(value, live.EffectiveValue);
                Assert.Equal(source, live.EffectiveSource);
                if (!stale)
                {
                    var verified = result.GetProperty("safeProviderEvidence");
                    Assert.Equal(value, verified.GetProperty("configuration.value").GetString());
                    Assert.Equal(source, verified.GetProperty("configuration.source").GetString());
                    Assert.Contains(audit.Mutations, item => item.OperationId == id && item.EventType == MutationAuditEventType.Completed &&
                        item.State == MutationOperationState.AppliedVerified);
                }
                else
                {
                    Assert.Contains(audit.Mutations, item => item.OperationId == id && item.EventType == MutationAuditEventType.StalePreview);
                    Assert.DoesNotContain(audit.Mutations, item => item.OperationId == id && item.EventType == MutationAuditEventType.DispatchStarted);
                }
                return id;
            }

            var catalog = await reader.GetFromJsonAsync<JsonElement>(Root + "/settings", token);
            var setting = Assert.Single(catalog.EnumerateArray());
            Assert.Equal(Key, setting.GetProperty("key").GetString());
            Assert.Equal("brokerOverride", Assert.Single(setting.GetProperty("scopes").EnumerateArray()).GetString());
            await Post(writer, Root + "/preview", Input(cluster, "16000"), HttpStatusCode.BadRequest);
            await Post(reader, Root + "/preview", Input(broker, "17000"), HttpStatusCode.Forbidden);
            Assert.Contains(audit.Security, item => item.EventType == SecurityAuditEventType.AuthorizationDenied);
            await Post(writer, Root + "/preview", Input(broker, "17000", key: "listeners"), HttpStatusCode.BadRequest);
            await Post(writer, Root + "/preview", Input(broker, "3600001"), HttpStatusCode.BadRequest);

            var first = await Preview(broker, "17000");
            Assert.Equal("15000", first.GetProperty("effectiveValue").GetString());
            Assert.Equal("DefaultConfig", first.GetProperty("effectiveSource").GetString());
            Assert.Equal("DynamicBrokerConfig", first.GetProperty("expectedSource").GetString());
            var overrideChange = await Execute(first, broker, "17000", "DynamicBrokerConfig");
            var changed = await Execute(await Preview(broker, "18000"), broker, "18000", "DynamicBrokerConfig");
            await Execute(await Preview(broker, null, changed), broker, "17000", "DynamicBrokerConfig");
            await Execute(await Preview(broker, null, overrideChange), broker, "15000", "DefaultConfig");

            async Task ExternalChange(string value)
            {
                var result = await provider.AlterAsync(new(broker, value), new(DateTimeOffset.UtcNow.AddSeconds(10)), token);
                Assert.Equal(MutationExecutionResultKind.AppliedUnverified, result.ResultKind);
                var until = DateTimeOffset.UtcNow.AddSeconds(15);
                do
                {
                    var live = await Observe(broker);
                    if (live.EffectiveSource == "DynamicBrokerConfig" && live.EffectiveValue == value) return;
                    await Task.Delay(100, token);
                } while (DateTimeOffset.UtcNow < until);
                Assert.Fail("Independent Kafka config change did not propagate.");
            }

            var sourceStale = await Preview(broker, "19000");
            await ExternalChange("15000");
            await Execute(sourceStale, broker, "15000", "DynamicBrokerConfig", stale: true);
            var valueStale = await Preview(broker, "19000");
            await ExternalChange("15001");
            await Execute(valueStale, broker, "15001", "DynamicBrokerConfig", stale: true);
            await Execute(await Preview(broker, null), broker, "15000", "DefaultConfig");
            Assert.Equal(5, audit.Mutations.Count(item => item.EventType == MutationAuditEventType.DispatchStarted));

            var evidence = Required("EVIDENCE");
            Directory.CreateDirectory(evidence);
            await File.WriteAllTextAsync(Path.Combine(evidence, "broker-config.json"), JsonSerializer.Serialize(new
            {
                environment = Required("ENVIRONMENT"),
                baseline = new { initial.EffectiveValue, initial.EffectiveSource },
                unsupportedClusterDefaultWriteRejected = true,
                brokerSet = "17000 / DynamicBrokerConfig",
                brokerChanged = "18000 / DynamicBrokerConfig",
                restoredExisting = "17000 / DynamicBrokerConfig",
                restoredBrokerInheritance = "15000 / DefaultConfig",
                staleSourceRejectedWithSameValue = true,
                staleValueRejectedWithSameSource = true,
                changePermissionDenied = true,
                auditedDispatches = 5,
                registeredSshHosts = 0,
            }, new JsonSerializerOptions { WriteIndented = true }), token);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var path in new[] { file, file + "-wal", file + "-shm" }) File.Delete(path);
        }
    }

    private static ClaimsPrincipal Principal(string subject) =>
        OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", subject)], "oidc")),
            Issuer, null, DateTimeOffset.UtcNow);

    private static string Required(string key) => Environment.GetEnvironmentVariable("KAFDECK_BROKER_CONFIG_TEST_" + key)
        ?? throw new InvalidOperationException($"Missing isolated broker configuration fixture setting {key}.");

    private sealed class CapturedAudit : IMutationAuditSink, ISecurityAuditSink
    {
        internal List<MutationAuditEvent> Mutations { get; } = [];
        internal List<SecurityAuditEvent> Security { get; } = [];
        public ValueTask WriteAsync(MutationAuditEvent item, CancellationToken cancellationToken = default)
        {
            Mutations.Add(item);
            return ValueTask.CompletedTask;
        }
        public ValueTask WriteAsync(SecurityAuditEvent item, CancellationToken cancellationToken = default)
        {
            Security.Add(item);
            return ValueTask.CompletedTask;
        }
    }
}

internal sealed class BrokerConfigLiveFactAttribute : FactAttribute
{
    public BrokerConfigLiveFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("KAFDECK_RUN_BROKER_CONFIG_LIVE") != "1")
            Skip = "Requires a disposable Kafka cluster with an explicitly bound identity.";
    }
}
