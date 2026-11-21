using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Clusters;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

internal sealed record FleetBootstrapPreview(string Principal, string ClusterId, string Name,
    string Address, int Port, string Fingerprint, string AdminUser, long SettingsRevision,
    BrokerServiceTarget[] Targets, int ExpectedBrokers, long ExpiresAt);
internal sealed record FleetBootstrapOperation(string Id, string Principal, string ClusterId,
    BrokerHostDraft Connection, string Payload, string ProbeCredentialId, string AgentCredentialId,
    string Stage, DateTimeOffset CreatedAtUtc);
internal sealed record FleetBootstrapInspect(string ClusterId, string Name, FleetBootstrapConnection Connection);
internal sealed record FleetBootstrapPrepare(string Preview, string[] TargetIds, FleetBootstrapConnection Connection,
    string Endpoint, string? CaPem, int IntervalSeconds, long MaintenanceFrom, long MaintenanceTo,
    bool ConfirmPreparation);
internal sealed record FleetBootstrapResume(string OperationId, FleetBootstrapConnection? Connection, bool ConfirmPreparation = false);
internal sealed record FleetBootstrapSource(string Instance, string Token);
internal sealed record FleetBootstrapResult(string Stage, string? Code, BrokerServiceTarget[]? Targets);

internal static class FleetBootstrapEndpoints
{
    private static readonly JsonSerializerOptions Wire = new(JsonSerializerDefaults.Web);
    private static readonly SemaphoreSlim Dispatch = new(1);

    internal static void MapFleetBootstrap(this WebApplication app, ManagedIdentityStore identity,
        BrokerHostOptions options, FleetSettingsStore fleet, BrokerHostStore registry,
        IBrokerHostProbe probe, IFleetBootstrapSsh ssh, BrokerAgentRecoveryStore recovery)
    {
        var group = app.MapGroup("/api/v1/fleet/bootstrap");
        group.AddEndpointFilter(async (context, next) =>
        {
            var http = context.HttpContext;
            http.Response.Headers.CacheControl = "no-store";
            if (FleetSettingsEndpoints.Owner(http, identity) is null) return FleetSettingsEndpoints.Failure("host_access_denied", 403);
            try { return await next(context); }
            catch (BrokerHostFailure error) { return FleetSettingsEndpoints.Failure(error.Code); }
            catch (OperationCanceledException) when (!http.RequestAborted.IsCancellationRequested)
            { return FleetSettingsEndpoints.Failure("bootstrap_observation_timeout", 504); }
        });

        group.MapGet("/operations", (HttpContext http) =>
        {
            var principal = Principal(http, identity);
            return Results.Ok(new { operations = (fleet.Read().Operations ?? []).Where(item => item.Principal == principal)
                .Select(item => new { item.Id, item.ClusterId, item.Connection.Name, item.Connection.Address, item.Stage, item.CreatedAtUtc }) });
        }).WithManagedBrowserContract("managed-fleet-bootstrap-operations");

        group.MapPost("/inspect", async (FleetBootstrapInspect input, HttpContext http,
            ClusterExplorerService clusters, ISecurityAuditSink audit, CancellationToken cancellation) =>
        {
            if (!FleetBootstrapSsh.Valid(input.Connection, options) || !BrokerHostValidation.ClusterId(input.ClusterId) ||
                string.IsNullOrWhiteSpace(input.Name) || input.Name.Length > 100 || input.Name.Any(char.IsControl))
                return FleetSettingsEndpoints.Failure("bootstrap_input_invalid", 400);
            var state = fleet.Read();
            var principal = Principal(http, identity);
            var bytes = JsonSerializer.SerializeToUtf8Bytes(new { action = "inspect", files = FleetBootstrapResources.Helpers() }, Wire);
            var observed = Parse(await ssh.CallAsync(input.Connection, bytes, cancellation));
            var cluster = await clusters.GetClusterAsync(input.ClusterId, cancellation);
            var candidates = observed.Targets!.Where(item => item.KafkaClusterId == cluster.KafkaClusterId &&
                cluster.Brokers.Any(broker => broker.BrokerId == item.BrokerId)).ToArray();
            if (cluster.Failure is not null || !cluster.Observation.IsFreshAt(DateTimeOffset.UtcNow) || candidates.Length == 0)
                return FleetSettingsEndpoints.Failure("bootstrap_cluster_identity_unavailable");
            if (Principal(http, identity) != principal || fleet.Read().SettingsRevision != state.SettingsRevision)
                return FleetSettingsEndpoints.Failure("fleet_revision_conflict");
            var preview = new FleetBootstrapPreview(principal, input.ClusterId, input.Name, input.Connection.Address,
                input.Connection.Port, input.Connection.Fingerprint, input.Connection.User, state.SettingsRevision,
                candidates, cluster.Brokers.Count, DateTimeOffset.UtcNow.AddMinutes(10).ToUnixTimeSeconds());
            await audit.WriteAsync(FleetSettingsEndpoints.Audit(http, identity, "fleet_host_setup_inspected", input.Connection.Address, input.ClusterId), cancellation);
            return Results.Ok(new { preview = fleet.SealPreview(preview), targets = candidates,
                package = new { version = FleetBootstrapResources.Version, digest = Digest(FleetBootstrapResources.Agent()) },
                changes = new[] { "restrictedSshAccounts", "fixedHostHelpers", "agentPackageAndToken", "agentLaunchPreparation" },
                brokerRestarted = false });
        }).WithManagedBrowserContract("managed-fleet-bootstrap-inspect", typeof(FleetBootstrapInspect))
            .WithMetadata(new RequestSizeLimitAttribute(131072)).RequireKafdeckAntiforgery();

        group.MapPost("/prepare", async (FleetBootstrapPrepare input, HttpContext http,
            MonitoringStore metrics, ISecurityAuditSink audit, CancellationToken cancellation) =>
        {
            if (!input.ConfirmPreparation) return FleetSettingsEndpoints.Failure("bootstrap_confirmation_required", 400);
            var plan = fleet.ReadPreview(input.Preview);
            var principal = Principal(http, identity);
            if (plan is null || plan.Principal != principal || plan.ExpiresAt < DateTimeOffset.UtcNow.ToUnixTimeSeconds() ||
                !FleetBootstrapSsh.Valid(input.Connection, options) || !SameConnection(input.Connection, plan) ||
                input.TargetIds is not { Length: > 0 and <= 32 } || input.TargetIds.Distinct().Count() != input.TargetIds.Length ||
                input.TargetIds.Any(id => !plan.Targets.Any(target => target.Id == id)) ||
                plan.Targets.Where(target => input.TargetIds.Contains(target.Id)).Select(target => target.BrokerId).Distinct().Count() != input.TargetIds.Length)
                return FleetSettingsEndpoints.Failure("bootstrap_preview_invalid", 400);
            if (!Uri.TryCreate(input.Endpoint, UriKind.Absolute, out var endpoint) || endpoint.Scheme != "https" ||
                endpoint.UserInfo.Length != 0 || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0 ||
                endpoint.AbsolutePath != "/monitoring/ingest" || input.IntervalSeconds is < 10 or > 60 ||
                input.MaintenanceFrom >= input.MaintenanceTo || input.MaintenanceTo <= DateTimeOffset.UtcNow.ToUnixTimeSeconds() ||
                input.MaintenanceTo - input.MaintenanceFrom > 8 * 3600 || input.CaPem is { Length: > 16384 } ||
                input.CaPem?.Contains("PRIVATE KEY", StringComparison.Ordinal) == true)
                return FleetSettingsEndpoints.Failure("bootstrap_agent_settings_invalid", 400);
            if (!string.IsNullOrEmpty(input.CaPem))
            {
                try
                {
                    var certificates = new System.Security.Cryptography.X509Certificates.X509Certificate2Collection();
                    certificates.ImportFromPem(input.CaPem);
                    if (certificates.Count == 0) throw new System.Security.Cryptography.CryptographicException();
                    foreach (var certificate in certificates) certificate.Dispose();
                }
                catch (System.Security.Cryptography.CryptographicException)
                { return FleetSettingsEndpoints.Failure("bootstrap_agent_settings_invalid", 400); }
            }
            if (recovery.Find(plan.ClusterId) is not null) return FleetSettingsEndpoints.Failure("bootstrap_agent_operation_pending");
            if (!await Dispatch.WaitAsync(0, cancellation)) return FleetSettingsEndpoints.Failure("host_probe_busy", 429);
            try
            {
                var targets = plan.Targets.Where(target => input.TargetIds.Contains(target.Id, StringComparer.Ordinal)).ToArray();
                var id = Guid.NewGuid().ToString("N");
                var probeKey = FleetBootstrapResources.KeyPair();
                var agentKey = FleetBootstrapResources.KeyPair();
                var probeId = "setup-" + id + "-probe";
                var agentId = "setup-" + id + "-agent";
                var connection = new BrokerHostDraft(plan.Name, plan.Address, plan.Port, "kafdeck-probe", plan.Fingerprint, probeId);
                // Reserve the operation before allocating ingestion credentials. A failed preparation
                // remains visible and can be resumed using the same operation ID and staged payload.
                var operation = new FleetBootstrapOperation(id, principal, plan.ClusterId, connection, "", probeId,
                    agentId, "reserved", DateTimeOffset.UtcNow);
                fleet.Change(state =>
                {
                    if (state.SettingsRevision != plan.SettingsRevision) throw new BrokerHostFailure("fleet_revision_conflict");
                    if (state.Credentials.Length > 62 || (state.Operations ?? []).Count(item => item.Stage is not ("complete" or "failed")) >= 8)
                        throw new BrokerHostFailure("bootstrap_operation_limit");
                    return FleetSettingsStore.AddAudit(state with
                    {
                        SettingsRevision = state.SettingsRevision + 1,
                        Credentials = [.. state.Credentials,
                            new(probeId, "privateKey", probeKey.Private, null, "probe", false, 1),
                            new(agentId, "privateKey", agentKey.Private, null, "agent", false, 1)],
                        Operations = [.. (state.Operations ?? []).Where(item => item.Stage is not ("complete" or "failed")),
                            .. (state.Operations ?? []).Where(item => item.Stage is "complete" or "failed").TakeLast(23), operation],
                    }, FleetSettingsEndpoints.Audit(http, identity, "fleet_host_preparation_authorized", id, plan.ClusterId));
                });
                var sources = new Dictionary<string, FleetBootstrapSource>();
                foreach (var target in targets)
                {
                    var instance = "broker-" + target.BrokerId + "-" + id[..12];
                    var enrolled = metrics.Enroll(plan.ClusterId, instance, "create");
                    if (enrolled is not IValueHttpResult { Value: { } value }) throw new BrokerHostFailure("bootstrap_monitoring_unavailable");
                    var result = JsonSerializer.SerializeToElement(value, Wire);
                    if (!result.TryGetProperty("token", out var token)) throw new BrokerHostFailure("bootstrap_monitoring_unavailable");
                    sources[target.Id] = new(instance, token.GetString()!);
                }
                var agent = FleetBootstrapResources.Agent();
                var payload = JsonSerializer.Serialize(new
                {
                    action = "prepare", operationId = id, proof = Convert.ToHexString(RandomNumberGenerator.GetBytes(32)), files = FleetBootstrapResources.Helpers(), targets,
                    probePublicKey = probeKey.Public, agentPublicKey = agentKey.Public, agentJar = Convert.ToBase64String(agent),
                    version = FleetBootstrapResources.Version, digest = Digest(agent), endpoint = endpoint.AbsoluteUri,
                    input.CaPem, input.IntervalSeconds, input.MaintenanceFrom, input.MaintenanceTo, expectedBrokers = plan.ExpectedBrokers, sources,
                }, Wire);
                operation = operation with { Payload = payload, Stage = "pending" };
                Update(fleet, operation);
                await fleet.DrainAuditAsync(audit, cancellation);
                return await Continue(operation, input.Connection, http, identity, options, fleet, registry,
                    probe, ssh, recovery, audit, cancellation);
            }
            finally { Dispatch.Release(); }
        }).WithManagedBrowserContract("managed-fleet-bootstrap-prepare", typeof(FleetBootstrapPrepare))
            .WithMetadata(new RequestSizeLimitAttribute(262144)).RequireKafdeckAntiforgery();

        group.MapPost("/resume", async (FleetBootstrapResume input, HttpContext http, ISecurityAuditSink audit, MonitoringStore metrics,
            CancellationToken cancellation) =>
        {
            if (!await Dispatch.WaitAsync(0, cancellation)) return FleetSettingsEndpoints.Failure("host_probe_busy", 429);
            try
            {
            var operation = (fleet.Read().Operations ?? []).SingleOrDefault(item => item.Id == input.OperationId &&
                item.Principal == Principal(http, identity));
            if (operation is null) return FleetSettingsEndpoints.Failure("bootstrap_operation_missing", 404);
            if (operation.Stage == "complete") return Results.Ok(new { operation.Id, stage = "complete" });
            if (operation.Payload.Length == 0)
            {
                // Claim an unused reservation before revoking enrollment, including across processes.
                // A committed SSH payload must never be mistaken for an unused reservation.
                if (operation.Stage != "failed")
                    fleet.Change(state =>
                    {
                        if (!(state.Operations ?? []).Any(item => item.Id == operation.Id && item.Stage == "reserved" && item.Payload.Length == 0))
                            throw new BrokerHostFailure("fleet_revision_conflict");
                        return FleetSettingsStore.AddAudit(state with
                        {
                            SettingsRevision = state.SettingsRevision + 1,
                            Credentials = state.Credentials.Select(item => item.Id == operation.ProbeCredentialId || item.Id == operation.AgentCredentialId
                                ? item with { Revoked = true, Secret = "", Passphrase = null, Revision = item.Revision + 1 } : item).ToArray(),
                            Operations = (state.Operations ?? []).Select(item => item.Id == operation.Id ? item with { Stage = "failed" } : item).ToArray(),
                        }, FleetSettingsEndpoints.Audit(http, identity, "fleet_host_reservation_recovered", operation.Id, operation.ClusterId));
                    });
                foreach (var source in metrics.Sources().Where(item => item.ClusterId == operation.ClusterId &&
                    item.Instance.EndsWith("-" + operation.Id[..12], StringComparison.Ordinal)))
                    metrics.Enroll(source.ClusterId, source.Instance, "revoke");
                await fleet.DrainAuditAsync(audit, cancellation);
                return Results.Ok(new { operation.Id, stage = "failed", code = "bootstrap_reservation_recovered" });
            }
            if (input.Connection is not null && (!input.ConfirmPreparation || !FleetBootstrapSsh.Valid(input.Connection, options) ||
                input.Connection.Address != operation.Connection.Address || input.Connection.Port != operation.Connection.Port ||
                input.Connection.Fingerprint != operation.Connection.Fingerprint))
                return FleetSettingsEndpoints.Failure("bootstrap_preview_invalid", 400);
            return await Continue(operation, input.Connection, http, identity, options, fleet, registry, probe, ssh, recovery, audit, cancellation);
            }
            finally { Dispatch.Release(); }
        }).WithManagedBrowserContract("managed-fleet-bootstrap-resume", typeof(FleetBootstrapResume))
            .WithMetadata(new RequestSizeLimitAttribute(131072)).RequireKafdeckAntiforgery();
    }

    private static async Task<IResult> Continue(FleetBootstrapOperation operation, FleetBootstrapConnection? temporary,
        HttpContext http, ManagedIdentityStore identity, BrokerHostOptions options, FleetSettingsStore fleet,
        BrokerHostStore registry, IBrokerHostProbe probe, IFleetBootstrapSsh ssh, BrokerAgentRecoveryStore recovery, ISecurityAuditSink audit,
        CancellationToken cancellation)
    {
        try
        {
            if (!options.Current.Enabled || !options.Allows(operation.Connection.Address) ||
                fleet.Credential(operation.ProbeCredentialId) is null || fleet.Credential(operation.AgentCredentialId) is null)
                throw new BrokerHostFailure("host_access_denied");
            if (Principal(http, identity) != operation.Principal) throw new BrokerHostFailure("host_access_denied");
            if (temporary is not null)
            {
                fleet.Change(state => FleetSettingsStore.AddAudit(state,
                    FleetSettingsEndpoints.Audit(http, identity, "fleet_host_preparation_dispatched", operation.Id, operation.ClusterId)));
                await fleet.DrainAuditAsync(audit, cancellation);
                var prepared = Parse(await ssh.CallAsync(temporary, System.Text.Encoding.UTF8.GetBytes(operation.Payload), cancellation));
                if (prepared.Stage != "prepared") throw new BrokerHostFailure("bootstrap_preparation_unverified");
            }
            var observation = await probe.ObserveAsync(operation.Connection, cancellation);
            using var payload = JsonDocument.Parse(operation.Payload);
            if (probe is not IFleetPreparationVerifier verifier ||
                await verifier.VerifyAsync(operation.Connection, operation.Id, cancellation) != payload.RootElement.GetProperty("proof").GetString())
                throw new BrokerHostFailure("bootstrap_preparation_unverified");
            var selected = payload.RootElement.GetProperty("targets").Deserialize<BrokerServiceTarget[]>(Wire)!;
            if (selected.Any(target => !observation.Targets.Any(current => current.Id == target.Id && current.Status == "verified" &&
                current.BrokerId == target.BrokerId && current.KafkaClusterId == target.KafkaClusterId)))
                throw new BrokerHostFailure("bootstrap_service_changed");
            if (Principal(http, identity) != operation.Principal) throw new BrokerHostFailure("host_access_denied");
            var owner = FleetSettingsEndpoints.Owner(http, identity)!;
            var managed = identity.Find(owner) ?? throw new BrokerHostFailure("host_access_denied");
            using var lease = recovery.ConfigurationLease();
            if (recovery.Find(operation.ClusterId) is not null) throw new BrokerHostFailure("bootstrap_agent_operation_pending");
            fleet.Change(state =>
            {
                if (state.Credentials.Any(item => item.Revoked && (item.Id == operation.ProbeCredentialId || item.Id == operation.AgentCredentialId)) ||
                    !state.Configuration.Enabled || !state.Configuration.AllowedNetworks.Any(network =>
                        System.Net.IPNetwork.Parse(network).Contains(System.Net.IPAddress.Parse(operation.Connection.Address))))
                    throw new BrokerHostFailure("host_access_denied");
                var existing = state.Registry.Hosts.SingleOrDefault(item => item.Connection.Address == operation.Connection.Address &&
                    item.Connection.Port == operation.Connection.Port && item.Connection.User == operation.Connection.User);
                var host = new BrokerHost(existing?.Id ?? Guid.NewGuid(), operation.Connection, DateTimeOffset.UtcNow);
                var bindings = observation.Targets.Where(item => selected.Any(target => target.Id == item.Id)).Select(item =>
                    new BrokerHostBinding(operation.ClusterId, item.BrokerId!.Value, host.Id, item.Id,
                        item.KafkaClusterId!, item.Instance!, DateTimeOffset.UtcNow)).ToArray();
                if (state.Registry.Hosts.Length >= 64 && existing is null) throw new BrokerHostFailure("host_already_registered_or_limit");
                var sources = payload.RootElement.GetProperty("sources").Deserialize<Dictionary<string, FleetBootstrapSource>>(Wire)!;
                var first = bindings[0];
                var config = state.Configuration;
                var package = new BrokerAgentPackage(payload.RootElement.GetProperty("version").GetString()!, payload.RootElement.GetProperty("digest").GetString()!);
                return FleetSettingsStore.AddAudit(state with
                {
                    SettingsRevision = state.SettingsRevision + 1,
                    Configuration = config with
                    {
                        LogReaderUserIds = [.. config.LogReaderUserIds.Append(managed.Id).Distinct()],
                        AgentInstallerUserIds = [.. config.AgentInstallerUserIds.Append(managed.Id).Distinct()],
                        LogSources = [.. config.LogSources.Where(item => !bindings.Any(binding => binding.ClusterId == item.ClusterId && binding.BrokerId == item.BrokerId)),
                            .. bindings.Select(item => new BrokerLogSource(item.ClusterId, item.BrokerId, sources[item.TargetId].Instance))],
                        AgentPackages = [.. config.AgentPackages.Where(item => item.Version != package.Version), package],
                        AgentTargets = [.. config.AgentTargets.Where(item => item.ClusterId != operation.ClusterId),
                            new(operation.ClusterId, first.BrokerId, sources[first.TargetId].Instance, operation.AgentCredentialId,
                                payload.RootElement.GetProperty("expectedBrokers").GetInt32())],
                    },
                    Registry = state.Registry with
                    {
                        Revision = state.Registry.Revision + 1,
                        Hosts = [.. state.Registry.Hosts.Where(item => item.Id != host.Id), host],
                        Bindings = [.. state.Registry.Bindings.Where(item => !bindings.Any(binding => binding.ClusterId == item.ClusterId && binding.BrokerId == item.BrokerId)), .. bindings],
                    },
                    Operations = (state.Operations ?? []).Select(item => item.Id == operation.Id ? item with { Stage = "complete", Payload = "" } : item).ToArray(),
                }, FleetSettingsEndpoints.Audit(http, identity, "fleet_host_preparation_verified", operation.Id, operation.ClusterId));
            });
            await fleet.DrainAuditAsync(audit, cancellation);
            return Results.Ok(new { operation.Id, stage = "complete", targets = observation.Targets,
                next = "previewAgentChange", brokerRestarted = false });
        }
        catch (BrokerHostFailure error) when (error.Code is not "host_access_denied")
        {
            // The host may already have completed preparation. Preserve the exact payload,
            // generated credentials and operation identity for an explicit resume.
            return Results.Ok(new { operation.Id, stage = "pending", code = error.Code });
        }
    }

    private static void Update(FleetSettingsStore store, FleetBootstrapOperation operation) => store.Change(state =>
    {
        if (!(state.Operations ?? []).Any(item => item.Id == operation.Id && item.Stage == "reserved") ||
            state.Credentials.Any(item => item.Revoked && (item.Id == operation.ProbeCredentialId || item.Id == operation.AgentCredentialId)))
            throw new BrokerHostFailure("fleet_revision_conflict");
        return state with { Operations = (state.Operations ?? []).Select(item => item.Id == operation.Id ? operation : item).ToArray() };
    });
    private static string Principal(HttpContext http, ManagedIdentityStore identity) =>
        SecurityAuditPrincipal.FromOperator((FleetSettingsEndpoints.Owner(http, identity) ?? throw new BrokerHostFailure("host_access_denied")).Identity);
    private static bool SameConnection(FleetBootstrapConnection connection, FleetBootstrapPreview preview) =>
        connection.Address == preview.Address && connection.Port == preview.Port && connection.User == preview.AdminUser && connection.Fingerprint == preview.Fingerprint;
    private static string Digest(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    private static FleetBootstrapResult Parse(byte[] bytes)
    {
        FleetBootstrapResult? result;
        try { result = JsonSerializer.Deserialize<FleetBootstrapResult>(bytes, Wire); }
        catch (JsonException) { throw new BrokerHostFailure("bootstrap_response_invalid"); }
        if (result?.Stage == "blocked") throw new BrokerHostFailure(result.Code is { Length: <= 80 } && BrokerHostValidation.Name(result.Code)
            ? result.Code : "bootstrap_host_unavailable");
        if (result is null || result.Stage is not ("prepared" or "inspected") || result.Targets is not { Length: <= 64 } ||
            result.Targets.Any(target => !BrokerHostSsh.ValidTarget(target)))
            throw new BrokerHostFailure("bootstrap_response_invalid");
        return result;
    }
}
