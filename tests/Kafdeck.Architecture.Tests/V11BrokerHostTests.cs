using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Clusters;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V11BrokerHostTests
{
    internal static BrokerHostOptions Options(string address = "127.0.0.1", string? key = null) => new()
    {
        Enabled = true, AllowedNetworks = [address + "/32"],
        Credentials = [new() { Id = "probe", Secret = key is null ? "env:KAFDECK_HOST_TEST_SECRET" : "file:" + key }],
    };
    internal static BrokerHostDraft Draft(string address = "127.0.0.1", string? pin = null) => new(
        "Test host", address, 22, "kafdeck-probe", pin ?? "SHA256:" + new string('A', 43), "probe");
    internal static ObservationMetadata Fresh() => new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddSeconds(30), DateTimeOffset.UtcNow.AddSeconds(60), ObservationSource.Live);
    internal static ClusterProjection Cluster() => new("test-cluster", "cluster-identity", 1,
        [new(1, "127.0.0.1", 9092, null, true)], ClusterHealthState.Healthy, [], [], Fresh(), null);
    internal static BrokerServiceTarget Target() => new("broker-1", "systemd", "kafka.service", null, null, 1, "cluster-identity", "100:1000", "verified");

    [Theory]
    [InlineData("192.0.2.1", "probe", "kafdeck-probe")]
    [InlineData("localhost", "probe", "kafdeck-probe")]
    [InlineData("127.0.0.1", "file:/secret", "kafdeck-probe")]
    [InlineData("127.0.0.1", "probe", "user\n")]
    [InlineData("127.0.0.1", "probe", "user;id")]
    public void Rejects_non_allowlisted_destinations_credential_locators_and_control_characters(string address, string credential, string user)
        => Assert.False(BrokerHostValidation.Draft(Draft(address) with { CredentialId = credential, User = user }, Options()));

    [Theory]
    [InlineData("cluster", "host_broker_identity_mismatch")]
    [InlineData("broker", "host_broker_identity_mismatch")]
    [InlineData("inactive", "host_identity_unavailable")]
    [InlineData("missing", "host_identity_unavailable")]
    [InlineData("stale", "host_identity_unavailable")]
    public void Rejects_wrong_cluster_broker_inactive_missing_and_stale_metadata(string kind, string code)
    {
        var target = Target(); var cluster = Cluster();
        if (kind == "cluster") target = target with { KafkaClusterId = "other-cluster" };
        if (kind == "broker") target = target with { BrokerId = 2 };
        if (kind == "inactive") target = target with { Status = "identity_unavailable" };
        if (kind == "missing") cluster = cluster with { Brokers = [] };
        if (kind == "stale") cluster = cluster with { Observation = new(DateTimeOffset.UtcNow.AddMinutes(-3), DateTimeOffset.UtcNow.AddMinutes(-2), DateTimeOffset.UtcNow.AddMinutes(-1), ObservationSource.StaleSnapshot) };
        Assert.Equal(code, Assert.Throws<BrokerHostFailure>(() => BrokerHostEndpoints.ValidateBinding(target, cluster, 1)).Code);
    }

    [Fact]
    public async Task Registry_CAS_and_durable_audit_survive_restart_without_credentials()
    {
        using var f = new ManagedProvisioningFixture();
        var store = new BrokerHostStore(f.StatePath + ".hosts.json");
        var host = new BrokerHost(Guid.NewGuid(), Draft(), DateTimeOffset.UtcNow);
        var audit = new SecurityAuditEvent(DateTimeOffset.UtcNow, SecurityAuditEventType.BrokerHostManagement,
            SecurityAuditPrincipal.FromOperator(f.Owner.Identity), "durable-correlation", null, null, SecurityAuditOutcome.Succeeded, "host_registered");
        store.Commit(0, s => s with { Hosts = [host] }, audit);
        Assert.Equal("host_revision_conflict", Assert.Throws<BrokerHostFailure>(() => store.Commit(0, s => s, audit)).Code);
        f.Audit.FailNext = true;
        await Assert.ThrowsAsync<IOException>(() => store.DrainAuditAsync(f.Audit, default));
        var restarted = new BrokerHostStore(f.StatePath + ".hosts.json");
        Assert.Single(restarted.Read().PendingAudit);
        await restarted.DrainAuditAsync(f.Audit, default);
        Assert.Empty(restarted.Read().PendingAudit);
        Assert.Equal("durable-correlation", Assert.Single(f.Audit.Events).SessionCorrelationId);
        Assert.Equal(host, Assert.Single(restarted.Read().Hosts));
        Assert.DoesNotContain("KAFDECK_HOST_TEST_SECRET", File.ReadAllText(f.StatePath + ".hosts.json"));
    }

    [Fact]
    public async Task Browser_boundary_requires_owner_and_CSRF_and_never_returns_secret_locations()
    {
        await using var f = await HostFixture.Start();
        using var response = await f.Client.GetAsync("/api/v1/fleet/hosts");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.DoesNotContain("KAFDECK_HOST_TEST_SECRET", await response.Content.ReadAsStringAsync());
        Assert.Contains("no-store", response.Headers.CacheControl!.ToString());
        f.Client.DefaultRequestHeaders.Remove(KafdeckAntiforgeryExtensions.HeaderName);
        using var noCsrf = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts", new BrokerHostWrite(0, Draft()));
        Assert.Equal(HttpStatusCode.Forbidden, noCsrf.StatusCode); Assert.Equal(0, f.Probe.Calls);
        f.Actor = new ClaimsPrincipal(new ClaimsIdentity());
        using var anonymous = await f.Client.GetAsync("/api/v1/fleet/hosts");
        Assert.Equal(HttpStatusCode.Forbidden, anonymous.StatusCode);
    }

    [Fact]
    public async Task Changed_pin_and_owner_revoked_during_probe_leave_registry_unchanged()
    {
        await using var f = await HostFixture.Start();
        f.Probe.Failure = "host_key_mismatch";
        using var changed = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts", new BrokerHostWrite(0, Draft()));
        Assert.Equal(HttpStatusCode.Conflict, changed.StatusCode);
        Assert.Contains("host_key_mismatch", await changed.Content.ReadAsStringAsync()); Assert.Empty(f.Store.Read().Hosts);
        f.Probe.Failure = null;
        f.Probe.AfterObserve = () => { var s = f.Identity.Store.Read(); var nextOwner = Guid.NewGuid(); f.Identity.Store.Write(s with { OwnerId = nextOwner, Users = [s.Users[0] with { Role = "viewer" }, s.Users[0] with { Id = nextOwner, Email = "new-owner@example.test" }] }); };
        using var revoked = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts", new BrokerHostWrite(0, Draft()));
        Assert.Equal(HttpStatusCode.Forbidden, revoked.StatusCode); Assert.Empty(f.Store.Read().Hosts);
    }

    [Fact]
    public async Task Register_probe_bind_and_reverify_use_revision_and_identity_fences()
    {
        await using var f = await HostFixture.Start();
        using var registered = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts", new BrokerHostWrite(0, Draft()));
        Assert.Equal(HttpStatusCode.Created, registered.StatusCode);
        var host = Assert.Single(f.Store.Read().Hosts);
        using var observed = await f.Client.PostAsJsonAsync($"/api/v1/fleet/hosts/{host.Id}/probe", new { });
        Assert.Equal(HttpStatusCode.OK, observed.StatusCode);
        using var bound = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts/bindings", new BrokerHostBind(1, "test-cluster", 1, host.Id, "broker-1"));
        Assert.Equal(HttpStatusCode.OK, bound.StatusCode);
        f.Probe.Observation = new(1, [Target() with { KafkaClusterId = "different" }]);
        using var wrong = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts/bindings", new BrokerHostBind(2, "test-cluster", 1, host.Id, "broker-1"));
        Assert.Equal(HttpStatusCode.Conflict, wrong.StatusCode); Assert.Equal(2, f.Store.Read().Revision);
        Assert.Single(f.Store.Read().Bindings);
        var inventory = ManagedBrowserOpenApi.ForApplication(f.App);
        Assert.Equal(3, inventory["paths"]!.AsObject().Count);
    }

    internal sealed class ProbeStub : IBrokerHostProbe
    {
        internal int Calls;
        internal string? Failure;
        internal Action? AfterObserve;
        internal BrokerHostObservation Observation = new(1, [Target()]);
        public Task<BrokerHostObservation> ObserveAsync(BrokerHostDraft host, CancellationToken cancellation)
        {
            Calls++; if (Failure is not null) throw new BrokerHostFailure(Failure);
            AfterObserve?.Invoke(); return Task.FromResult(Observation);
        }
    }

    internal sealed class HostFixture : IAsyncDisposable
    {
        internal readonly ManagedProvisioningFixture Identity = new();
        internal readonly ProbeStub Probe = new();
        internal FleetSettingsStore? Fleet;
        internal BrokerAgentRecoveryStore Recovery => new(Identity.StatePath + ".agent-operations.json");
        internal BrokerHostStore Store => new(Identity.StatePath + ".hosts.json", Fleet);
        internal WebApplication App = null!;
        internal HttpClient Client = null!;
        internal ClaimsPrincipal Actor = null!;
        internal static async Task<HostFixture> Start(BrokerHostOptions? options = null, IBrokerHostProbe? probe = null,
            IKafkaAdministrationPort? port = null, IBrokerLogReader? logReader = null, long? alertTime = null, IFleetBootstrapSsh? bootstrap = null, bool fleet = false)
        {
            var f = new HostFixture();
            var builder = WebApplication.CreateBuilder(); builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Services.AddSingleton<ISecurityAuditSink>(f.Identity.Audit);
            if (fleet || bootstrap is not null)
            {
                builder.Services.AddSingleton(TimeProvider.System);
                builder.Services.AddSingleton(new MonitoringSettings(f.Identity.StatePath + ".monitoring", []));
                builder.Services.AddSingleton<MonitoringStore>();
            }
            builder.Services.AddSingleton(new ClusterExplorerService(port ?? new MetadataStub(), new KafkaSnapshotCoordinator()));
            builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
            f.Actor = OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", f.Identity.OwnerId.ToString("D"))], "oidc")), ManagedProvisioningFixture.Issuer, null, DateTimeOffset.UtcNow);
            f.App = builder.Build(); f.App.UseRouting(); f.App.Use((context, next) => { context.User = f.Actor; return next(context); });
            f.App.UseAntiforgery(); f.App.MapKafdeckAntiforgeryEndpoint();
            if (Environment.GetEnvironmentVariable("KAFDECK_HOST_UI_DIST") is { Length: > 0 } directory)
            {
                f.App.UseDefaultFiles(new DefaultFilesOptions { FileProvider = new PhysicalFileProvider(directory) });
                f.App.UseStaticFiles(new StaticFileOptions { FileProvider = new PhysicalFileProvider(directory) });
            }
            f.App.MapGet("/api/v1/clusters/test-cluster", async (ClusterExplorerService clusters) => new { data = await clusters.GetClusterAsync("test-cluster") });
            var configured = options ?? Options();
            if (fleet || bootstrap is not null)
            {
                f.Fleet = new FleetSettingsStore(f.Identity.StatePath + ".fleet.sqlite",
                    new Microsoft.AspNetCore.DataProtection.EphemeralDataProtectionProvider(), configured, f.Store.Read());
                configured.Runtime = f.Fleet.Options;
                f.App.MapFleetSettings(f.Identity.Store, f.Fleet, f.Recovery);
                if (bootstrap is not null) f.App.MapFleetBootstrap(f.Identity.Store, configured, f.Fleet, f.Store,
                    probe ?? f.Probe, bootstrap, f.Recovery);
                f.App.MapManagedBrowserOpenApi();
                f.App.MapGet("/api/v1/identity/users", () => new { users = f.Identity.Store.Read().Users });
            }
            f.App.MapBrokerHosts(f.Identity.Store, configured, f.Store, probe ?? f.Probe);
            if (logReader is not null)
            {
                var logOptions = new BrokerHostOptions { Enabled = configured.Enabled, AllowedNetworks = configured.AllowedNetworks,
                    Credentials = configured.Credentials, LogReaderUserIds = [f.Identity.OwnerId],
                    LogSources = [new("test-cluster", 1, "fixture-broker-1")] };
                f.App.MapBrokerLogs(f.Identity.Store, logOptions, f.Store, logReader);
                if (alertTime is { } time)
                    f.App.MapGet("/api/v1/clusters/test-cluster/monitoring/alerts", () => new
                    {
                        state = "available", observedAt = time, truncated = false, warning = false,
                        rows = new[] { new { name = "TestBrokerDiagnostic", state = "firing", severity = "warning",
                            instance = "fixture-broker-1", metric = "fixture-diagnostic", labels = new { },
                            time, since = time, value = 1, threshold = 0 } },
                    });
            }
            await f.App.StartAsync();
            f.Client = new(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new(Assert.Single(f.App.Urls)) };
            using var csrf = await f.Client.GetFromJsonAsync<JsonDocument>("/api/v1/auth/csrf");
            f.Client.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName, csrf!.RootElement.GetProperty("requestToken").GetString());
            return f;
        }
        public async ValueTask DisposeAsync() { Client.Dispose(); await App.StopAsync(); await App.DisposeAsync();
            if (Fleet is not null) V11BrokerAgentTests.ClearMonitoringPool(Identity.StatePath + ".monitoring");
            Identity.Dispose(); }
    }

    private sealed class MetadataStub : IKafkaAdministrationPort
    {
        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(string id, KafkaOperationContext operation, CancellationToken token) => Task.FromResult(KafkaResult<ClusterMetadata>.Success(new(id, "cluster-identity", 1, [new(1, "127.0.0.1", 9092, null, true)]), Fresh()));
        public Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(string id, KafkaOperationContext operation, CancellationToken token) => Task.FromResult(KafkaResult<KafkaCapabilities>.Success(new([]), Fresh()));
        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(string id, KafkaOperationContext operation, CancellationToken token) => throw new NotSupportedException();
        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(string id, string topic, KafkaOperationContext operation, CancellationToken token) => throw new NotSupportedException();
        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(string id, string topic, KafkaOperationContext operation, CancellationToken token) => throw new NotSupportedException();
        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(string id, int broker, KafkaOperationContext operation, CancellationToken token) => throw new NotSupportedException();
    }
}

public sealed class V11BrokerHostLiveTests
{
    [HostBrowserFact]
    public async Task Rendered_Fleet_component_registers_and_binds_real_brokers()
    {
        string Env(string key) => Environment.GetEnvironmentVariable("KAFDECK_HOST_" + key) ?? throw new InvalidOperationException("Missing host fixture setting: " + key);
        var options = V11BrokerHostTests.Options(Env("ADDRESS"), Env("KEY"));
        using var adapter = new ConfluentKafkaAdministrationAdapter([new ClusterProfile("test-cluster", Env("KAFKA").Split(','), KafkaSecurityProtocol.Plaintext, null, null)], new SecretResolver());
        await using var f = await V11BrokerHostTests.HostFixture.Start(options, new BrokerHostSsh(options), adapter);
        var start = new System.Diagnostics.ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        start.ArgumentList.Add(Env("BROWSER_SCRIPT")); start.ArgumentList.Add(f.Client.BaseAddress!.ToString());
        using var process = System.Diagnostics.Process.Start(start)!;
        var stdout = process.StandardOutput.ReadToEndAsync(); var stderr = process.StandardError.ReadToEndAsync();
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        try { await process.WaitForExitAsync(deadline.Token); }
        finally { if (!process.HasExited) process.Kill(entireProcessTree: true); }
        Assert.True(process.ExitCode == 0, await stdout + await stderr);
        Assert.Equal(int.Parse(Env("COUNT")), f.Store.Read().Bindings.Length);
    }

    [HostLiveFact]
    public async Task Pinned_SSH_registry_and_binding_use_real_services_and_Kafka_metadata()
    {
        string Env(string key) => Environment.GetEnvironmentVariable("KAFDECK_HOST_" + key) ?? throw new InvalidOperationException("Missing host fixture setting: " + key);
        var options = V11BrokerHostTests.Options(Env("ADDRESS"), Env("KEY"));
        var draft = V11BrokerHostTests.Draft(Env("ADDRESS"), Env("PIN")) with { Port = int.Parse(Env("PORT")) };
        using var adapter = new ConfluentKafkaAdministrationAdapter([new ClusterProfile("test-cluster", Env("KAFKA").Split(','), KafkaSecurityProtocol.Plaintext, null, null)], new SecretResolver());
        await using var f = await V11BrokerHostTests.HostFixture.Start(options, new BrokerHostSsh(options), adapter);
        using var changed = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts", new BrokerHostWrite(0, draft with { Fingerprint = V11BrokerHostTests.Draft().Fingerprint }));
        Assert.Equal(HttpStatusCode.Conflict, changed.StatusCode); Assert.Contains("host_key_mismatch", await changed.Content.ReadAsStringAsync()); Assert.Empty(f.Store.Read().Hosts);
        using var registered = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts", new BrokerHostWrite(0, draft));
        Assert.True(registered.StatusCode == HttpStatusCode.Created, await registered.Content.ReadAsStringAsync());
        var host = Assert.Single(f.Store.Read().Hosts);
        using var observed = await f.Client.PostAsJsonAsync($"/api/v1/fleet/hosts/{host.Id}/probe", new { });
        var observation = await observed.Content.ReadFromJsonAsync<BrokerHostObservation>();
        Assert.Equal(int.Parse(Env("COUNT")), observation!.Targets.Length);
        foreach (var target in observation.Targets)
        {
            Assert.Equal("verified", target.Status); Assert.Equal(Env("ADAPTER"), target.Adapter);
            using var bound = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts/bindings", new BrokerHostBind(f.Store.Read().Revision, "test-cluster", target.BrokerId!.Value, host.Id, target.Id));
            Assert.True(bound.IsSuccessStatusCode, await bound.Content.ReadAsStringAsync());
        }
        var revision = f.Store.Read().Revision;
        using var wrong = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts/bindings", new BrokerHostBind(revision, "test-cluster", 9999, host.Id, observation.Targets[0].Id));
        Assert.Equal(HttpStatusCode.Conflict, wrong.StatusCode); Assert.Equal(revision, f.Store.Read().Revision);
        using var wrongCluster = await f.Client.PostAsJsonAsync("/api/v1/fleet/hosts/bindings", new BrokerHostBind(revision, "unregistered-cluster", observation.Targets[0].BrokerId!.Value, host.Id, observation.Targets[0].Id));
        Assert.Equal(HttpStatusCode.Conflict, wrongCluster.StatusCode); Assert.Equal(revision, f.Store.Read().Revision);
        Assert.Equal(observation.Targets.Length, f.Store.Read().Bindings.Length);
        Assert.Equal(observation.Targets.Length, f.Store.Read().Bindings.Select(b => (b.HostId, b.TargetId)).Distinct().Count());
        var directory = Env("EVIDENCE"); Directory.CreateDirectory(directory);
        await File.WriteAllTextAsync(Path.Combine(directory, Env("ADAPTER") + ".json"), JsonSerializer.Serialize(new {
            adapter = Env("ADAPTER"), observedAtUtc = DateTimeOffset.UtcNow, sourceSha = Environment.GetEnvironmentVariable("KAFDECK_SOURCE_SHA"),
            observation.Targets, bindings = f.Store.Read().Bindings, changedPinRejected = true, mismatchedBrokerRejected = true,
            wrongClusterRejected = true, auditEvents = f.Identity.Audit.Events.Count, noBrokerRestart = true,
        }, new JsonSerializerOptions(JsonSerializerDefaults.Web) { WriteIndented = true }));
    }
}

internal sealed class HostLiveFactAttribute : FactAttribute
{
    public HostLiveFactAttribute() { if (Environment.GetEnvironmentVariable("KAFDECK_RUN_HOST_LIVE") != "1") Skip = "Requires an explicitly provisioned real SSH/Kafka fixture."; }
}

internal sealed class HostBrowserFactAttribute : FactAttribute
{
    public HostBrowserFactAttribute() { if (Environment.GetEnvironmentVariable("KAFDECK_RUN_HOST_LIVE") != "1" || string.IsNullOrEmpty(Environment.GetEnvironmentVariable("KAFDECK_HOST_UI_DIST"))) Skip = "Requires the rendered Fleet harness and a real SSH/Kafka fixture."; }
}
