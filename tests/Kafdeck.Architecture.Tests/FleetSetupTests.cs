using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("Fleet bootstrap global dispatch")]
public sealed class FleetSetupTests
{
    private static readonly JsonSerializerOptions Wire = new(JsonSerializerDefaults.Web);
    private static FleetBootstrapConnection Connection => new("127.0.0.1", 22, "temporary-admin",
        V11BrokerHostTests.Draft().Fingerprint, "password", "TEMPORARY-ADMIN-SECRET", null, "TEMPORARY-SUDO-SECRET");

    [Fact]
    public async Task Settings_are_owner_only_CSRF_protected_write_only_and_live_without_restart()
    {
        await using var f = await V11BrokerHostTests.HostFixture.Start(new(), fleet: true);
        using var initial = await f.Client.GetFromJsonAsync<JsonDocument>("/api/v1/fleet/settings/");
        Assert.False(initial!.RootElement.GetProperty("configuration").GetProperty("enabled").GetBoolean());
        var csrf = f.Client.DefaultRequestHeaders.GetValues(KafdeckAntiforgeryExtensions.HeaderName).Single();
        f.Client.DefaultRequestHeaders.Remove(KafdeckAntiforgeryExtensions.HeaderName);
        using var rejected = await f.Client.PostAsJsonAsync("/api/v1/fleet/settings/credentials", new FleetCredentialInput(0, "office", "password", "NEVER-RETURN-THIS", null, "probe"));
        Assert.Equal(HttpStatusCode.Forbidden, rejected.StatusCode);
        Assert.Empty(f.Fleet!.Read().Credentials);
        f.Client.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName, csrf);
        using var saved = await f.Client.PostAsJsonAsync("/api/v1/fleet/settings/credentials", new FleetCredentialInput(0, "office", "password", "NEVER-RETURN-THIS", null, "probe"));
        Assert.True(saved.IsSuccessStatusCode, await saved.Content.ReadAsStringAsync());
        Assert.DoesNotContain("NEVER-RETURN-THIS", await saved.Content.ReadAsStringAsync());
        var configuration = f.Fleet.Read().Configuration with { Enabled = true, AllowedNetworks = ["127.0.0.1/32"] };
        using var enabled = await f.Client.PostAsJsonAsync("/api/v1/fleet/settings/configuration", new FleetConfigurationInput(1, configuration));
        Assert.True(enabled.IsSuccessStatusCode, await enabled.Content.ReadAsStringAsync());
        using var registry = await f.Client.GetFromJsonAsync<JsonDocument>("/api/v1/fleet/hosts");
        Assert.True(registry!.RootElement.GetProperty("enabled").GetBoolean());
        Assert.Single(registry.RootElement.GetProperty("credentials").EnumerateArray());
        var owner = f.Actor;
        f.Actor = new(new ClaimsIdentity([new Claim("sub", Guid.NewGuid().ToString())], "oidc"));
        using var denied = await f.Client.GetAsync("/api/v1/fleet/settings/");
        Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
        f.Actor = owner;
        using var revoked = await f.Client.PostAsJsonAsync("/api/v1/fleet/settings/credentials", new FleetCredentialInput(2, "office", "password", null, null, "probe", true));
        Assert.True(revoked.IsSuccessStatusCode);
        Assert.Empty(f.Fleet.Options().Credentials);
        Assert.DoesNotContain("NEVER-RETURN-THIS", JsonSerializer.Serialize(f.Identity.Audit.Events));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Preparation_requires_a_receipt_and_lost_SSH_response_recovers_without_admin_credentials(bool loseResponse)
    {
        var remote = new Host { LoseResponse = loseResponse };
        await using var f = await V11BrokerHostTests.HostFixture.Start(probe: remote, bootstrap: remote);
        var plan = await Inspect(f);
        remote.ReceiptReadable = false;
        using var pending = await f.Client.PostAsJsonAsync("/api/v1/fleet/bootstrap/prepare", Prepare(plan));
        Assert.True(pending.IsSuccessStatusCode, await pending.Content.ReadAsStringAsync());
        var result = await pending.Content.ReadFromJsonAsync<JsonDocument>();
        Assert.Equal("pending", result!.RootElement.GetProperty("stage").GetString());
        Assert.Empty(f.Store.Read().Bindings);
        var operation = Assert.Single(f.Fleet!.Read().Operations!);
        Assert.False(JsonSerializer.Serialize(f.Fleet.Read()).Contains(Connection.Secret, StringComparison.Ordinal));
        Assert.False(JsonSerializer.Serialize(f.Fleet.Read()).Contains(Connection.SudoPassword!, StringComparison.Ordinal));
        Assert.DoesNotContain(Connection.Secret, Encoding.UTF8.GetString(File.ReadAllBytes(f.Fleet.DatabasePath)));
        remote.ReceiptReadable = true;
        using var resumed = await f.Client.PostAsJsonAsync("/api/v1/fleet/bootstrap/resume", new FleetBootstrapResume(operation.Id, null));
        Assert.True(resumed.IsSuccessStatusCode, await resumed.Content.ReadAsStringAsync());
        using var completed = await resumed.Content.ReadFromJsonAsync<JsonDocument>();
        Assert.Equal("complete", completed!.RootElement.GetProperty("stage").GetString());
        Assert.Single(f.Store.Read().Bindings);
        Assert.Equal(1, remote.Preparations);
        Assert.Empty(Assert.Single(f.Fleet.Read().Operations!).Payload);
        Assert.Equal(2, f.Fleet.Read().Credentials.Length);
        Assert.All(f.Fleet.Read().Credentials, value => Assert.Equal("privateKey", value.Authentication));
        using var publicOperations = await f.Client.GetAsync("/api/v1/fleet/bootstrap/operations");
        var response = await publicOperations.Content.ReadAsStringAsync();
        Assert.DoesNotContain("PRIVATE KEY", response);
        Assert.DoesNotContain(remote.Proof!, response);
        Assert.Equal(1, f.Fleet.Read().Configuration.AgentTargets[0].ExpectedBrokers);
    }

    [Fact]
    public async Task Changed_network_or_actor_and_unconfirmed_preparation_dispatch_nothing()
    {
        var remote = new Host();
        await using var f = await V11BrokerHostTests.HostFixture.Start(probe: remote, bootstrap: remote);
        var plan = await Inspect(f);
        using var unconfirmed = await f.Client.PostAsJsonAsync("/api/v1/fleet/bootstrap/prepare", Prepare(plan) with { ConfirmPreparation = false });
        Assert.Equal(HttpStatusCode.BadRequest, unconfirmed.StatusCode);
        var state = f.Fleet!.Read();
        f.Fleet.SaveConfiguration(new(state.SettingsRevision, state.Configuration with { AllowedNetworks = ["192.0.2.0/24"] }),
            new(DateTimeOffset.UtcNow, SecurityAuditEventType.BrokerHostManagement, "test-owner", "change-network", null, null, SecurityAuditOutcome.Succeeded, "settings_changed"));
        using var denied = await f.Client.PostAsJsonAsync("/api/v1/fleet/bootstrap/prepare", Prepare(plan));
        Assert.Equal(HttpStatusCode.BadRequest, denied.StatusCode);
        Assert.Equal(0, remote.Preparations);
        Assert.Empty(f.Fleet.Read().Credentials);
    }

    [Fact]
    public async Task Reserved_operation_recovers_without_remote_effects_or_orphan_sources()
    {
        var remote = new Host();
        await using var f = await V11BrokerHostTests.HostFixture.Start(probe: remote, bootstrap: remote);
        var id = Guid.NewGuid().ToString("N");
        var operation = new FleetBootstrapOperation(id, SecurityAuditPrincipal.FromOperator(f.Identity.Owner.Identity),
            "test-cluster", V11BrokerHostTests.Draft(), "", "orphan-probe", "orphan-agent", "reserved", DateTimeOffset.UtcNow);
        var metrics = f.App.Services.GetRequiredService<MonitoringStore>();
        metrics.Enroll("test-cluster", "broker-1-" + id[..12], "create");
        f.Fleet!.Change(state => state with { Operations = [operation], Credentials = [new("orphan-probe", "password", "ORPHAN", null, "probe", false, 1)] });
        using var resumed = await f.Client.PostAsJsonAsync("/api/v1/fleet/bootstrap/resume", new FleetBootstrapResume(id, null));
        Assert.True(resumed.IsSuccessStatusCode, await resumed.Content.ReadAsStringAsync());
        Assert.Equal("failed", Assert.Single(f.Fleet.Read().Operations!).Stage);
        Assert.Empty(metrics.Sources());
        Assert.DoesNotContain(f.Fleet.Options().Credentials, item => item.Id == "orphan-probe");
        Assert.Equal(0, remote.Preparations);
    }

    [Fact]
    public async Task New_routes_are_in_the_deployed_contract_with_write_only_administrative_secrets()
    {
        var remote = new Host();
        await using var f = await V11BrokerHostTests.HostFixture.Start(probe: remote, bootstrap: remote);
        using var document = JsonDocument.Parse(ManagedBrowserOpenApi.ForApplication(f.App).ToJsonString());
        Assert.Equal("managed-fleet-bootstrap-prepare", document!.RootElement.GetProperty("paths")
            .GetProperty("/api/v1/fleet/bootstrap/prepare").GetProperty("post").GetProperty("operationId").GetString());
        var properties = document.RootElement.GetProperty("components").GetProperty("schemas").GetProperty("FleetBootstrapConnection").GetProperty("properties");
        foreach (var name in new[] { "secret", "passphrase", "sudoPassword" }) Assert.True(properties.GetProperty(name).GetProperty("writeOnly").GetBoolean());
    }

    [Fact]
    public async Task Pending_agent_target_cannot_be_replaced_and_dispatch_rechecks_configuration_under_the_same_lease()
    {
        await using var f = await V11BrokerHostTests.HostFixture.Start(fleet: true);
        var target = new BrokerAgentTarget("test-cluster", 1, "broker-1", "lifecycle", 2);
        f.Fleet!.Change(state => state with { Configuration = state.Configuration with { AgentTargets = [target] } });
        var pending = new BrokerAgentRecovery("test-cluster", Guid.NewGuid().ToString("N"), "owner", "protected-plan");
        using (f.Recovery.ConfigurationLease())
            Assert.Equal("agent_recovery_busy", Assert.Throws<BrokerHostFailure>(() => f.Recovery.Begin(pending)).Code);
        Assert.Equal("agent_install_denied", Assert.Throws<BrokerHostFailure>(() => f.Recovery.Begin(pending, () => false)).Code);
        Assert.Null(f.Recovery.Find("test-cluster"));
        f.Recovery.Begin(pending, () => true);
        var config = f.Fleet.Read().Configuration with { AgentTargets = [target with { BrokerId = 2 }] };
        using var rejected = await f.Client.PostAsJsonAsync("/api/v1/fleet/settings/configuration", new FleetConfigurationInput(0, config));
        Assert.Equal(HttpStatusCode.Conflict, rejected.StatusCode);
        Assert.Contains("bootstrap_agent_operation_pending", await rejected.Content.ReadAsStringAsync());
        Assert.Equal(target, Assert.Single(f.Fleet.Read().Configuration.AgentTargets));
    }

    private static async Task<string> Inspect(V11BrokerHostTests.HostFixture fixture)
    {
        using var inspected = await fixture.Client.PostAsJsonAsync("/api/v1/fleet/bootstrap/inspect", new FleetBootstrapInspect("test-cluster", "Office host", Connection));
        Assert.True(inspected.IsSuccessStatusCode, await inspected.Content.ReadAsStringAsync());
        using var plan = await inspected.Content.ReadFromJsonAsync<JsonDocument>();
        return plan!.RootElement.GetProperty("preview").GetString()!;
    }
    private static FleetBootstrapPrepare Prepare(string plan) => new(plan, ["broker-1"], Connection,
        "https://192.0.2.10:8080/monitoring/ingest", null, 30, DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds(), true);

    private sealed class Host : IFleetBootstrapSsh, IBrokerHostProbe, IFleetPreparationVerifier
    {
        internal bool LoseResponse;
        internal bool ReceiptReadable = true;
        internal string? Proof;
        internal int Preparations;
        public Task<byte[]> CallAsync(FleetBootstrapConnection connection, byte[] payload, CancellationToken cancellation)
        {
            using var request = JsonDocument.Parse(payload);
            var prepare = request.RootElement.GetProperty("action").GetString() == "prepare";
            if (prepare)
            {
                Preparations++;
                Proof = request.RootElement.GetProperty("proof").GetString();
                if (LoseResponse) throw new BrokerHostFailure("bootstrap_connection_unavailable");
            }
            return Task.FromResult(JsonSerializer.SerializeToUtf8Bytes(new { stage = prepare ? "prepared" : "inspected", targets = new[] { V11BrokerHostTests.Target() } }, Wire));
        }
        public Task<BrokerHostObservation> ObserveAsync(BrokerHostDraft host, CancellationToken cancellation) => Task.FromResult(new BrokerHostObservation(1, [V11BrokerHostTests.Target()]));
        public Task<string?> VerifyAsync(BrokerHostDraft host, string operationId, CancellationToken cancellation) => Task.FromResult(ReceiptReadable ? Proof : null);
    }
}

[CollectionDefinition("Fleet bootstrap global dispatch", DisableParallelization = true)]
public sealed class FleetBootstrapDispatchCollection { }
