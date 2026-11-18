using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V11BrokerAgentTests
{
    [Fact]
    public void Restart_guard_requires_leader_complete_isr_and_minimum_survivors()
    {
        HashSet<int> brokers = [1, 2, 3];
        Assert.True(BrokerAgentHealthCheck.PartitionSafe(new(0, 1, [1, 2, 3], [1, 2, 3]), brokers, 2, true, 2));
        Assert.False(BrokerAgentHealthCheck.PartitionSafe(new(0, 2, [2], [2]), brokers, 2, true, 1));
        Assert.False(BrokerAgentHealthCheck.PartitionSafe(new(0, 1, [1, 2], [1, 2]), brokers, 2, true, 2));
        Assert.False(BrokerAgentHealthCheck.PartitionSafe(new(0, 1, [1, 2, 3], [1, 3]), brokers, 2, false, 1));
        Assert.False(BrokerAgentHealthCheck.PartitionSafe(new(0, null, [1, 2], [1, 2]), brokers, 2, true, 1));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Approved_plan_preserves_restart_intent_and_recovers_expired_publication(bool renew)
    {
        const string cluster = "test-cluster";
        const string version = "1.1.0";
        var digest = new string('a', 64);
        var priorDigest = new string('b', 64);
        var now = DateTimeOffset.UtcNow;
        var seconds = now.ToUnixTimeSeconds();
        using var fixture = new ManagedProvisioningFixture();
        var principal = SecurityAuditPrincipal.FromOperator(fixture.Owner.Identity);
        var target = new BrokerAgentTarget(cluster, 2, "metric-2", "agent-key", renew ? 2 : 1);
        var package = new BrokerAgentPackage(version, digest);
        var options = new BrokerHostOptions
        {
            Enabled = true, AgentInstallerUserIds = [fixture.OwnerId], AgentTargets = [target], AgentPackages = [package],
        };
        var draft = new BrokerHostDraft("Fixture", "127.0.0.1", 22, "kafdeck-probe", "", "read-only");
        var connection = draft with { User = "kafdeck-agent", CredentialId = target.CredentialId };
        var registered = new BrokerHost(Guid.NewGuid(), draft, now);
        var binding = new BrokerHostBinding(cluster, 2, registered.Id, "broker-2", "identity", "fixture", now);
        var registry = new BrokerHostStore(fixture.StatePath + ".hosts.json");
        registry.Commit(0, state => state with { Hosts = [registered], Bindings = [binding] },
            new(now, SecurityAuditEventType.BrokerHostManagement, principal, "fixture-binding", cluster, "2",
                SecurityAuditOutcome.Succeeded, "fixture_bound"));
        var recovery = new BrokerAgentRecoveryStore(fixture.StatePath + ".agent-operations.json");
        var testHost = new AgentPlanHost(renew, seconds, priorDigest);
        var kafka = new PlanKafka(renew);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(TimeProvider.System);
        builder.Services.AddSingleton(new MonitoringSettings(fixture.StatePath + ".monitoring", [new(cluster, "metric-2", new byte[32])]));
        builder.Services.AddSingleton<MonitoringStore>();
        builder.Services.AddSingleton<ISecurityAuditSink>(fixture.Audit);
        builder.Services.AddSingleton<IKafkaAdministrationPort>(kafka);
        builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
        var actor = OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim("sub", fixture.OwnerId.ToString("D"))], "oidc")), ManagedProvisioningFixture.Issuer, null, now);
        await using var app = builder.Build();
        app.UseRouting();
        app.Use((context, next) => { context.User = actor; return next(context); });
        app.UseAntiforgery();
        app.MapKafdeckAntiforgeryEndpoint();
        app.MapBrokerAgents(fixture.Store, options, registry, testHost, recovery);
        await app.StartAsync();
        using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new(Assert.Single(app.Urls)) };
        using var csrf = await client.GetFromJsonAsync<JsonDocument>("/api/v1/auth/csrf");
        client.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName, csrf!.RootElement.GetProperty("requestToken").GetString());
        const string prefix = "/api/v1/fleet/hosts/agent/";
        var operationId = Guid.NewGuid().ToString("N");
        if (renew)
        {
            var command = new BrokerAgentCommand("preview", operationId, binding.TargetId, 2, binding.KafkaClusterId,
                version, digest, "42:100", priorDigest, "argfile", seconds - 1, true);
            var expired = new BrokerAgentPlan(principal, cluster, target, package, binding, connection, command, seconds - 400);
            var token = app.Services.GetRequiredService<IDataProtectionProvider>().CreateProtector("kafdeck-broker-agent-plan-v1")
                .Protect(JsonSerializer.Serialize(expired));
            recovery.Begin(new(cluster, operationId, principal, token));
            using var denied = await client.PostAsJsonAsync(prefix + "apply", new BrokerAgentPlanInput(token));
            Assert.Equal(HttpStatusCode.Conflict, denied.StatusCode);
            using var current = await client.PostAsJsonAsync(prefix + "status", new BrokerAgentPlanInput(token));
            using var currentBody = await current.Content.ReadFromJsonAsync<JsonDocument>();
            Assert.True(currentBody!.RootElement.GetProperty("requiresRenewal").GetBoolean());
            Assert.Empty(testHost.Applies);
        }
        using var preview = await client.PostAsJsonAsync(prefix + "preview", new BrokerAgentPreviewInput(cluster, 2, version));
        Assert.True(preview.IsSuccessStatusCode, "Preview HTTP " + preview.StatusCode);
        using var before = await preview.Content.ReadFromJsonAsync<JsonDocument>();
        Assert.Equal(renew, before!.RootElement.GetProperty("restartRequired").GetBoolean());
        var approved = new BrokerAgentPlanInput(before.RootElement.GetProperty("plan").GetString()!);
        using var application = await client.PostAsJsonAsync(prefix + "apply", approved);
        Assert.True(application.IsSuccessStatusCode, "Apply HTTP " + application.StatusCode);
        using var result = await application.Content.ReadFromJsonAsync<JsonDocument>();
        Assert.Equal(renew ? "awaitingRestart" : "alreadyInstalled", result!.RootElement.GetProperty("stage").GetString());
        var applied = Assert.Single(testHost.Applies);
        Assert.Equal(renew, applied.ExpectRestart);
        Assert.True(applied.ApplyUntil > seconds);
        if (renew)
        {
            Assert.Equal(operationId, applied.OperationId);
            Assert.Equal(priorDigest, applied.CurrentDigest);
            Assert.Equal(approved.Plan, recovery.Find(cluster)?.Token);
        }
        else
        {
            Assert.Equal(0, kafka.MinIsrReads);
            Assert.Null(recovery.Find(cluster));
        }
        await app.StopAsync();
        ClearMonitoringPool(fixture.StatePath + ".monitoring");
    }

    internal static void ClearMonitoringPool(string database)
    {
        using var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = database, DefaultTimeout = 3,
        }.ToString());
        SqliteConnection.ClearPool(connection);
    }

    private sealed class AgentPlanHost(bool renew, long now, string priorDigest) : IBrokerAgentLifecycle
    {
        internal List<BrokerAgentCommand> Applies { get; } = [];

        public Task<BrokerAgentHostResult> CallAsync(BrokerHostDraft host, BrokerAgentCommand input, CancellationToken cancellation)
        {
            if (input.Action == "apply") Applies.Add(input);
            var stage = input.Action switch
            {
                "preview" => "preview",
                "status" => renew ? "publishing" : "unknown",
                "apply" => renew ? "awaitingRestart" : "alreadyInstalled",
                _ => throw new InvalidOperationException(),
            };
            return Task.FromResult(new BrokerAgentHostResult(stage, null, input.Version, input.Digest, "systemd",
                renew ? null : input.Version, renew ? priorDigest : input.Digest, "42:100", "argfile", renew,
                now - 180, renew ? now + 300 : now - 60, null, null));
        }
    }

    private sealed class PlanKafka(bool replicated) : IKafkaAdministrationPort
    {
        private readonly int[] _brokers = replicated ? [1, 2] : [2];
        internal int MinIsrReads;
        private static Task<KafkaResult<T>> Live<T>(T value) => Task.FromResult(KafkaResult<T>.Success(value,
            V11BrokerHostTests.Fresh()));
        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(string cluster, KafkaOperationContext operation, CancellationToken token) =>
            Live(new ClusterMetadata(cluster, "identity", 2, _brokers.Select(id => new BrokerMetadata(id, "127.0.0.1", 9092, null, id == 2)).ToArray()));
        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(string id, KafkaOperationContext operation, CancellationToken token) =>
            Live<IReadOnlyList<TopicSummary>>([new("topic", 1, false)]);
        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(string id, string topic, KafkaOperationContext operation, CancellationToken token) =>
            Live(new TopicMetadata(topic, false, [new(0, 2, _brokers, _brokers)]));
        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(string id, string topic, KafkaOperationContext operation, CancellationToken token)
        {
            Interlocked.Increment(ref MinIsrReads);
            return Live<IReadOnlyList<KafkaConfigurationEntry>>([new("min.insync.replicas", "1", false, false, "fixture")]);
        }
        public Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(string id, KafkaOperationContext operation, CancellationToken token) => throw new NotSupportedException();
        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(string id, int broker, KafkaOperationContext operation, CancellationToken token) => throw new NotSupportedException();
    }
}

public sealed class V11BrokerAgentLiveTests
{
    [AgentLiveFact]
    public async Task Offline_install_upgrade_and_lost_response_are_verified_in_Fleet()
    {
        string Env(string key) => Environment.GetEnvironmentVariable("KAFDECK_" + key)
            ?? throw new InvalidOperationException("Missing agent fixture setting " + key);
        var address = Env("HOST_ADDRESS");
        var brokerId = int.Parse(Env("AGENT_BROKER"));
        var clusterIdentity = Env("AGENT_EXPECTED_CLUSTER");
        var targetId = Env("AGENT_TARGET");
        var packages = JsonSerializer.Deserialize<Dictionary<string, string>>(File.ReadAllText(Env("AGENT_PACKAGE_CATALOG")))!
            .Select(pair => new BrokerAgentPackage(pair.Key, pair.Value)).ToArray();
        using var fixture = new ManagedProvisioningFixture();
        var deployment = new DeploymentOptions("http://127.0.0.1:18090", null, AccessMode.Oidc, null);
        var profile = new ClusterProfile("test-cluster", Env("AGENT_BOOTSTRAP").Split(','),
            KafkaSecurityProtocol.Plaintext, null, null);
        var kafdeck = new KafdeckOptions(deployment, [profile]);
        var instance = "fixture-agent-" + brokerId;
        var options = new BrokerHostOptions
        {
            Enabled = true, AllowedNetworks = [address + "/32"],
            Credentials = [new() { Id = "agent-test", Secret = "file:" + Env("AGENT_KEY") }],
            AgentInstallerUserIds = [fixture.OwnerId], AgentPackages = packages,
            AgentTargets = [new("test-cluster", brokerId, instance, "agent-test", 2)],
        };
        options.Validate();
        var db = fixture.StatePath + ".monitoring";
        var tokenHash = SHA256.HashData(Encoding.UTF8.GetBytes(File.ReadAllText(Env("AGENT_INGEST_SECRET_FILE")).Trim()));
        var settings = new MonitoringSettings(db, [new("test-cluster", instance, tokenHash)]);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls(deployment.ListenUrl);
        builder.Services.AddSingleton(TimeProvider.System);
        builder.Services.AddSingleton(settings);
        builder.Services.AddSingleton<MonitoringStore>();
        builder.Services.AddSingleton<ISecurityAuditSink>(fixture.Audit);
        builder.Services.AddSingleton<IKafkaAdministrationPort>(
            new ConfluentKafkaAdministrationAdapter([profile], new SecretResolver()));
        builder.Services.AddKafdeckAntiforgery(deployment.ListenUrl);
        var actor = OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim("sub", fixture.OwnerId.ToString("D"))], "oidc")), ManagedProvisioningFixture.Issuer, null, DateTimeOffset.UtcNow);
        var registry = new BrokerHostStore(fixture.StatePath + ".hosts.json");
        var registered = new BrokerHost(Guid.NewGuid(), new("Fixture", address, int.Parse(Env("HOST_PORT")),
            "kafdeck-probe", Env("HOST_PIN"), "read-only-probe"), DateTimeOffset.UtcNow);
        var binding = new BrokerHostBinding("test-cluster", brokerId, registered.Id, targetId, clusterIdentity,
            "binding-established", DateTimeOffset.UtcNow);
        registry.Commit(0, state => state with { Hosts = [registered], Bindings = [binding] },
            new(DateTimeOffset.UtcNow, SecurityAuditEventType.BrokerHostManagement, SecurityAuditPrincipal.FromOperator(fixture.Owner.Identity),
                "agent-fixture-binding", "test-cluster", brokerId.ToString(), SecurityAuditOutcome.Succeeded, "fixture_bound"));
        await using var app = builder.Build();
        app.UseRouting();
        app.Use((context, next) =>
        {
            context.User = actor;
            return next(context);
        });
        app.UseAntiforgery();
        app.MapKafdeckAntiforgeryEndpoint();
        var files = new PhysicalFileProvider(Env("HOST_UI_DIST"));
        app.UseDefaultFiles(new DefaultFilesOptions { FileProvider = files });
        app.UseStaticFiles(new StaticFileOptions { FileProvider = files });
        KafkaMetricSources.Map(app, kafdeck);
        var ssh = new LostApplyReply(new BrokerHostSsh(options));
        app.MapBrokerAgents(fixture.Store, options, registry, ssh, new(fixture.StatePath + ".agent-operations.json"));
        await app.StartAsync();
        try
        {
            var start = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            start.ArgumentList.Add(Env("AGENT_BROWSER_SCRIPT")); start.ArgumentList.Add(deployment.ListenUrl);
            using var browser = Process.Start(start)!;
            var output = browser.StandardOutput.ReadToEndAsync();
            var errors = browser.StandardError.ReadToEndAsync();
            using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(6));
            try { await browser.WaitForExitAsync(deadline.Token); }
            finally { if (!browser.HasExited) browser.Kill(entireProcessTree: true); }
            Assert.True(browser.ExitCode == 0, await output + await errors);
            Assert.Equal(2, ssh.DispatchCount);
            Assert.Equal(2, fixture.Audit.Events.Count(item => item.EventType == SecurityAuditEventType.BrokerHostManagement &&
                item.Outcome == SecurityAuditOutcome.Succeeded && item.ReasonCategory == "agent_recovery_observed"));
        }
        finally
        {
            await app.StopAsync();
            V11BrokerAgentTests.ClearMonitoringPool(db);
        }
    }

    private sealed class LostApplyReply(IBrokerAgentLifecycle inner) : IBrokerAgentLifecycle
    {
        internal int DispatchCount;
        public async Task<BrokerAgentHostResult> CallAsync(BrokerHostDraft host, BrokerAgentCommand input, CancellationToken cancellation)
        {
            var result = await inner.CallAsync(host, input, cancellation);
            if (input.Action == "apply" && Interlocked.Increment(ref DispatchCount) == 1 && result.Stage != "blocked")
                throw new BrokerHostFailure("host_probe_unavailable");
            return result;
        }
    }
}

internal sealed class AgentLiveFactAttribute : FactAttribute
{
    public AgentLiveFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("KAFDECK_RUN_V11_AGENT_LIVE") != "1")
            Skip = "Requires an explicitly configured real broker lifecycle fixture.";
    }
}
