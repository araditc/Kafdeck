using System.Diagnostics;
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
using Microsoft.AspNetCore.Hosting;
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
        finally { await app.StopAsync(); }
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
