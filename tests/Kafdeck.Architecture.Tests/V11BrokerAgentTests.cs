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
using Microsoft.AspNetCore.Http;
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

    [Fact]
    public async Task Recovery_reports_kafka_before_ingestion_and_fences_other_brokers_through_lost_status()
    {
        const string cluster = "test-cluster";
        var now = DateTimeOffset.UtcNow;
        var target = new BrokerAgentTarget(cluster, 2, "metric-2", "agent-key", 2);
        var package = new BrokerAgentPackage("1.1.0", new string('a', 64));
        using var fixture = new ManagedProvisioningFixture();
        var principal = SecurityAuditPrincipal.FromOperator(fixture.Owner.Identity);
        var options = new BrokerHostOptions
        {
            Enabled = true, AgentInstallerUserIds = [fixture.OwnerId], AgentTargets = [target], AgentPackages = [package],
        };
        var draft = new BrokerHostDraft("Fixture", "127.0.0.1", 22, "kafdeck-probe", "", "read-only");
        var registered = new BrokerHost(Guid.NewGuid(), draft, now);
        var bindings = new[] { new BrokerHostBinding(cluster, 2, registered.Id, "broker-2", "identity", "fixture", now),
            new BrokerHostBinding(cluster, 1, registered.Id, "broker-1", "identity", "fixture", now) };
        var registry = new BrokerHostStore(fixture.StatePath + ".hosts.json");
        registry.Commit(0, state => state with { Hosts = [registered], Bindings = bindings },
            new(now, SecurityAuditEventType.BrokerHostManagement, principal, "fixture-binding", cluster, "2",
                SecurityAuditOutcome.Succeeded, "fixture_bound"));
        var recoveryPath = fixture.StatePath + ".agent-operations.json";
        var recovery = new BrokerAgentRecoveryStore(recoveryPath);
        var kafka = new PlanKafka(true);
        var host = new FaultAgentHost(now.ToUnixTimeSeconds(), kafka);
        var database = fixture.StatePath + ".monitoring";
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(TimeProvider.System);
        builder.Services.AddSingleton(new MonitoringSettings(database, [new(cluster, "metric-2", new byte[32]),
            new(cluster, "metric-1", new byte[32])]));
        builder.Services.AddSingleton<MonitoringStore>();
        builder.Services.AddSingleton<IKafkaAdministrationPort>(kafka);
        builder.Services.AddSingleton<ISecurityAuditSink>(fixture.Audit);
        builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
        var actor = OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity(
            [new Claim("sub", fixture.OwnerId.ToString("D"))], "oidc")), ManagedProvisioningFixture.Issuer, null, now);
        await using var app = builder.Build();
        app.UseRouting();
        app.Use((context, next) => { context.User = actor; return next(context); });
        app.UseAntiforgery();
        app.MapKafdeckAntiforgeryEndpoint();
        app.MapBrokerAgents(fixture.Store, options, registry, host, recovery);
        await app.StartAsync();
        try
        {
            using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new(Assert.Single(app.Urls)) };
            using var csrf = await client.GetFromJsonAsync<JsonDocument>("/api/v1/auth/csrf");
            client.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName, csrf!.RootElement.GetProperty("requestToken").GetString());
            async Task<JsonElement> Post(string path, object payload, HttpStatusCode expected = HttpStatusCode.OK)
            {
                using var result = await client.PostAsJsonAsync("/api/v1/fleet/hosts/agent/" + path, payload);
                Assert.Equal(expected, result.StatusCode);
                using var body = await result.Content.ReadFromJsonAsync<JsonDocument>();
                return body!.RootElement.Clone();
            }
            static void Stage(JsonElement result, string stage, string checkpoint)
            {
                Assert.Equal(stage, result.GetProperty("stage").GetString());
                Assert.Equal(checkpoint, result.GetProperty("lastConfirmed").GetString());
            }
            var preview = await Post("preview", new BrokerAgentPreviewInput(cluster, 2, package.Version));
            var plan = new BrokerAgentPlanInput(preview.GetProperty("plan").GetString()!);
            var operation = preview.GetProperty("operationId").GetString();
            var unhealthy = await Post("apply", plan);
            Stage(unhealthy, "awaitingHealth", "RestartIntent");
            Assert.False(unhealthy.GetProperty("health").GetProperty("healthy").GetBoolean());

            host.Stage = "restartObserved";
            Stage(await Post("status", plan), "awaitingHealth", "RestartObserved");
            host.Interrupted = true;
            var reconnect = await Post("status", plan);
            Stage(reconnect, "reconcile", "RestartObserved");
            Assert.Equal(operation, reconnect.GetProperty("operationId").GetString());
            Assert.Equal(BrokerAgentCheckpoint.RestartObserved, new BrokerAgentRecoveryStore(recoveryPath).Find(cluster)?.Checkpoint);
            host.Interrupted = false;

            host.Stage = "unknown";
            Stage(await Post("apply", plan), "reconcile", "RestartObserved");
            Assert.Single(host.Calls, command => command.Action == "apply");

            options.AgentTargets[0] = target with { BrokerId = 1, MonitoringInstance = "metric-1" };
            var blocked = await Post("preview", new BrokerAgentPreviewInput(cluster, 1, package.Version), HttpStatusCode.Conflict);
            Assert.Equal("agent_previous_operation_pending", blocked.GetProperty("code").GetString());
            Assert.All(host.Calls, command => Assert.Equal(2, command.BrokerId));
            options.AgentTargets[0] = target;

            host.Stage = "restartObserved";
            kafka.Healthy = true;
            var awaitingMetric = await Post("status", plan);
            Stage(awaitingMetric, "awaitingMetrics", "KafkaHealthy");
            Assert.True(awaitingMetric.GetProperty("health").GetProperty("healthy").GetBoolean());
            Assert.Equal("metric-2", awaitingMetric.GetProperty("monitoringInstance").GetString());
            Assert.Equal(operation, recovery.Find(cluster)?.OperationId);
            Assert.DoesNotContain(host.Calls, command => command.Action == "complete");
            var dispatch = Assert.Single(fixture.Audit.Events, entry => entry.ReasonCategory == "agent_dispatch_authorized");
            Assert.Equal(principal, dispatch.Principal);
            Assert.Equal(operation, dispatch.SessionCorrelationId);
        }
        finally
        {
            await app.StopAsync();
            ClearMonitoringPool(database);
        }
    }

    [Fact]
    public async Task Log_reader_agent_installer_and_config_manager_have_independent_authority()
    {
        const string cluster = "test-cluster";
        var log = Guid.NewGuid();
        var install = Guid.NewGuid();
        var config = Guid.NewGuid();
        using var fixture = new ManagedProvisioningFixture();
        var state = fixture.Store.Read();
        fixture.Store.Write(state with
        {
            Users = [.. state.Users, new(log, "logs@example.test", "operator", [cluster], true, default),
                new(install, "install@example.test", "operator", [cluster], true, default),
                new(config, "config@example.test", "manager", [cluster], true, default)],
        });
        var options = new BrokerHostOptions
        {
            Enabled = true, LogReaderUserIds = [log], AgentInstallerUserIds = [install],
            AgentTargets = [new(cluster, 2, "metric-2", "agent-key", 2)],
            AgentPackages = [new("1.1.0", new string('a', 64))],
        };
        var now = DateTimeOffset.UtcNow;
        var host = new BrokerHost(Guid.NewGuid(), new("Fixture", "127.0.0.1", 22, "kafdeck-probe", "", "read-only"), now);
        var registry = new BrokerHostStore(fixture.StatePath + ".hosts.json");
        registry.Commit(0, current => current with { Hosts = [host], Bindings = [new(cluster, 2, host.Id, "broker-2", "identity", "fixture", now)] },
            new(now, SecurityAuditEventType.BrokerHostManagement, SecurityAuditPrincipal.FromOperator(fixture.Owner.Identity),
                "fixture-bind", cluster, "2", SecurityAuditOutcome.Succeeded, "fixture_bound"));

        var policy = AuthorizationPolicyCompiler.Compile(new(
            [new("config", [new(AuthorizationAction.ClusterConfigRead, [cluster], ["*"]),
                new(AuthorizationAction.ClusterConfigAlter, [cluster], ["*"])])],
            [new(ManagedProvisioningFixture.Issuer, config.ToString("D"), ["config"])], []));
        var auth = new MutationRequestAuthorizationService(new KafdeckAuthorizationService(
            new KafdeckOptions(new("http://127.0.0.1:0", null, AccessMode.Oidc, null), []), new AuthorizationPolicyEvaluator(policy)));
        var targetResource = Kafdeck.Modules.Administration.DynamicConfigurationPolicy.ConflictResource(new(cluster,
            Kafdeck.Modules.Administration.DynamicConfigurationScope.BrokerOverride, 2, "log.cleaner.backoff.ms"));

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(TimeProvider.System);
        builder.Services.AddSingleton(new MonitoringSettings(fixture.StatePath + ".monitoring", []));
        builder.Services.AddSingleton<MonitoringStore>();
        builder.Services.AddSingleton<IKafkaAdministrationPort>(new PlanKafka(true));
        builder.Services.AddSingleton<ISecurityAuditSink>(fixture.Audit);
        builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
        static ClaimsPrincipal Actor(Guid actor) => OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(
            new ClaimsIdentity([new Claim("sub", actor.ToString("D"))], "oidc")), ManagedProvisioningFixture.Issuer, null, DateTimeOffset.UtcNow);
        await using var app = builder.Build();
        app.UseRouting();
        app.Use((context, next) =>
        {
            context.User = Actor(Guid.Parse(context.Request.Headers["X-Fixture-Actor"].ToString()));
            return next(context);
        });
        app.UseAntiforgery();
        app.MapBrokerAgents(fixture.Store, options, registry, new FaultAgentHost(now.ToUnixTimeSeconds(), new PlanKafka(true)),
            new BrokerAgentRecoveryStore(fixture.StatePath + ".agent-operations.json"));
        await app.StartAsync();
        using var http = new HttpClient(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new(Assert.Single(app.Urls)) };
        foreach (var actorId in new[] { log, install, config })
        {
            var actor = Actor(actorId);
            Assert.True(OperatorSessionContextFactory.TryCreate(actor, out var session));
            Assert.Equal(actorId == log, BrokerLogEndpoints.MayRead(fixture.Store, options, session, cluster));
            http.DefaultRequestHeaders.Remove("X-Fixture-Actor");
            http.DefaultRequestHeaders.Add("X-Fixture-Actor", actorId.ToString("D"));
            using var targets = await http.GetFromJsonAsync<JsonDocument>("/api/v1/fleet/hosts/agent/targets?clusterId=" + cluster);
            Assert.Equal(actorId == install, targets!.RootElement.GetProperty("brokerId").ValueKind == JsonValueKind.Number);
            var decision = auth.AuthorizeTargets(actor, [new(AuthorizationAction.ClusterConfigRead, cluster, targetResource),
                new(AuthorizationAction.ClusterConfigAlter, cluster, targetResource)]);
            Assert.Equal(actorId == config ? KafdeckAuthorizationOutcome.Allowed : KafdeckAuthorizationOutcome.Forbidden, decision);
        }
        await app.StopAsync();
    }

    private sealed class FaultAgentHost(long timestamp, PlanKafka kafka) : IBrokerAgentLifecycle
    {
        internal string Stage = "unknown";
        internal bool Interrupted;
        internal List<BrokerAgentCommand> Calls { get; } = [];

        public Task<BrokerAgentHostResult> CallAsync(BrokerHostDraft host, BrokerAgentCommand input, CancellationToken cancellation)
        {
            Calls.Add(input);
            if (Interrupted) throw new BrokerHostFailure("host_probe_unavailable");
            if (input.Action == "apply") { Stage = "awaitingRestart"; kafka.Healthy = false; }
            var stage = input.Action == "preview" ? "preview" : Stage;
            return Task.FromResult(new BrokerAgentHostResult(stage, null, input.Version, input.Digest, "systemd",
                null, null, "42:100", "argfile", true, timestamp - 60, timestamp + 300,
                stage is "restartObserved" or "awaitingRestart" ? timestamp : null,
                stage == "restartObserved" ? timestamp : null));
        }
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
        internal bool Healthy = true;
        private static Task<KafkaResult<T>> Live<T>(T value) => Task.FromResult(KafkaResult<T>.Success(value,
            V11BrokerHostTests.Fresh()));
        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(string cluster, KafkaOperationContext operation, CancellationToken token) =>
            Live(new ClusterMetadata(cluster, "identity", 2, (Healthy ? _brokers : [1]).Select(id => new BrokerMetadata(id, "127.0.0.1", 9092, null, id == 2)).ToArray()));
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
    public Task Offline_install_upgrade_and_lost_response_are_verified_in_Fleet() => RunFleetAsync(faults: false);

    [AgentFaultLiveFact]
    public Task Fleet_recovers_an_interrupted_apply_then_distinguishes_kafka_from_ingestion() => RunFleetAsync(faults: true);

    private static async Task RunFleetAsync(bool faults)
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
        var ssh = new LostApplyReply(new BrokerHostSsh(options));
        var fault = faults ? new AgentFaultFixture(Env("AGENT_ADAPTER"), ssh, options.AgentTargets[0]) : null;
        await using var app = builder.Build();
        app.UseRouting();
        app.Use((context, next) =>
        {
            context.User = actor;
            if (fault is { IngestionAllowed: false } && context.Request.Path == "/monitoring/ingest")
            {
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                return Task.CompletedTask;
            }
            return next(context);
        });
        app.UseAntiforgery();
        app.MapKafdeckAntiforgeryEndpoint();
        var files = new PhysicalFileProvider(Env("HOST_UI_DIST"));
        app.UseDefaultFiles(new DefaultFilesOptions { FileProvider = files });
        app.UseStaticFiles(new StaticFileOptions { FileProvider = files });
        KafkaMetricSources.Map(app, kafdeck);
        fault?.Map(app);
        app.MapBrokerAgents(fixture.Store, options, registry, ssh, new(fixture.StatePath + ".agent-operations.json"));
        await app.StartAsync();
        try
        {
            var start = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            start.ArgumentList.Add(Env(faults ? "AGENT_FAULT_BROWSER_SCRIPT" : "AGENT_BROWSER_SCRIPT"));
            start.ArgumentList.Add(deployment.ListenUrl);
            if (fault is not null) start.Environment["KAFDECK_AGENT_FAULT_FIXTURE_KEY"] = fault.Key;
            using var browser = Process.Start(start)!;
            var output = browser.StandardOutput.ReadToEndAsync();
            var errors = browser.StandardError.ReadToEndAsync();
            using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(6));
            try { await browser.WaitForExitAsync(deadline.Token); }
            finally { if (!browser.HasExited) browser.Kill(entireProcessTree: true); }
            Assert.True(browser.ExitCode == 0, await output + await errors);
            Assert.Equal(faults ? 1 : 2, ssh.DispatchCount);
            Assert.Equal(faults ? 1 : 2, fixture.Audit.Events.Count(item => item.EventType == SecurityAuditEventType.BrokerHostManagement &&
                item.Outcome == SecurityAuditOutcome.Succeeded && item.ReasonCategory == "agent_recovery_observed"));
        }
        finally
        {
            try { if (fault is not null) await fault.ResumeAsync(CancellationToken.None); }
            finally
            {
                await app.StopAsync();
                V11BrokerAgentTests.ClearMonitoringPool(db);
            }
        }
    }

    private sealed class AgentFaultFixture(string adapter, LostApplyReply transport, BrokerAgentTarget target)
    {
        internal readonly string Key = Convert.ToHexString(RandomNumberGenerator.GetBytes(24));
        private bool _paused;
        private volatile bool _allowMetrics;
        internal bool IngestionAllowed => _allowMetrics;

        internal void Map(WebApplication app) => app.MapPost("/fixture/agent-faults/{operation}",
            async (string operation, HttpContext http, CancellationToken cancellationToken) =>
            {
                if (http.Request.Headers["X-Kafdeck-Fault-Fixture"] != Key) return Results.NotFound();
                switch (operation)
                {
                    case "pause":
                        var (host, command) = transport.LastApply ?? throw new InvalidOperationException("No pilot dispatch.");
                        using (var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                        {
                            deadline.CancelAfter(TimeSpan.FromSeconds(45));
                            while ((await transport.CallAsync(host, command with { Action = "status" }, deadline.Token)).Stage != "restartObserved")
                                await Task.Delay(300, deadline.Token);
                        }
                        _paused = true;
                        await SetPausedAsync(true, cancellationToken);
                        break;
                    case "resume":
                        await ResumeAsync(cancellationToken);
                        break;
                    case "allow-metrics":
                        if (_paused) return Results.Conflict();
                        _allowMetrics = true;
                        break;
                    default:
                        return Results.NotFound();
                }
                return Results.NoContent();
            });

        internal async Task ResumeAsync(CancellationToken cancellation)
        {
            if (!_paused) return;
            await SetPausedAsync(false, cancellation);
            _paused = false;
        }

        private async Task SetPausedAsync(bool pause, CancellationToken cancellation)
        {
            if (adapter == "systemd" && Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true" &&
                target.BrokerId == 2)
            {
                // The accepted native fixture owns this exact unit on its disposable runner.
                var start = new ProcessStartInfo("sudo") { UseShellExecute = false };
                foreach (var arg in new[] { "-n", "systemctl", "kill", "--kill-whom=main",
                    pause ? "--signal=STOP" : "--signal=CONT", "kafdeck-v11-kafka-sibling.service" })
                    start.ArgumentList.Add(arg);
                using var process = Process.Start(start)!;
                await process.WaitForExitAsync(cancellation);
                Assert.Equal(0, process.ExitCode);
                return;
            }

            var address = Environment.GetEnvironmentVariable("KAFDECK_AGENT_FAULT_CONTROL");
            if (adapter != "compose" || !Uri.TryCreate(address, UriKind.Absolute, out var controller) ||
                controller.Scheme != "http" || controller.Host != "127.0.0.1" || controller.AbsolutePath != "/")
                throw new InvalidOperationException("A loopback-only pilot controller is required.");
            var key = Environment.GetEnvironmentVariable("KAFDECK_AGENT_FAULT_CONTROL_KEY");
            Assert.False(string.IsNullOrWhiteSpace(key));
            using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false }) { Timeout = TimeSpan.FromSeconds(25) };
            using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(controller, pause ? "pause" : "resume"));
            request.Headers.Add("X-Kafdeck-Pilot-Control", key);
            using var response = await client.SendAsync(request, cancellation);
            response.EnsureSuccessStatusCode();
        }
    }

    private sealed class LostApplyReply(IBrokerAgentLifecycle inner) : IBrokerAgentLifecycle
    {
        internal int DispatchCount;
        internal (BrokerHostDraft Host, BrokerAgentCommand Command)? LastApply { get; private set; }
        public async Task<BrokerAgentHostResult> CallAsync(BrokerHostDraft host, BrokerAgentCommand input, CancellationToken cancellation)
        {
            var result = await inner.CallAsync(host, input, cancellation);
            if (input.Action == "apply")
            {
                LastApply = (host, input);
                if (Interlocked.Increment(ref DispatchCount) == 1 && result.Stage != "blocked")
                    throw new BrokerHostFailure("host_probe_unavailable");
            }
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

internal sealed class AgentFaultLiveFactAttribute : FactAttribute
{
    public AgentFaultLiveFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("KAFDECK_RUN_V11_AGENT_FAULTS") != "1")
            Skip = "Requires an explicitly configured isolated broker and fault controller.";
    }
}
