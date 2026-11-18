using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V11BrokerLogTests
{
    internal static BrokerLogRead Window(long at, int broker = 1) => new("test-cluster", broker, at - 20, at + 1);
    private static long Now() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    [Fact]
    public async Task Dedicated_permission_checks_scope_and_denies_before_ssh()
    {
        var reader = new LogStub();
        await using var fixture = await V11BrokerHostTests.HostFixture.Start(logReader: reader);
        var state = fixture.Identity.Store.Read();
        var operatorId = Guid.NewGuid();
        fixture.Identity.Store.Write(state with { Users = [.. state.Users,
            new(operatorId, "operator@example.test", "operator", ["test-cluster"], true, DateTimeOffset.MinValue)] });
        var claims = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", operatorId.ToString("D"))], "oidc"));
        fixture.Actor = OidcIdentityNormalizer.Normalize(claims, ManagedProvisioningFixture.Issuer, null, DateTimeOffset.UtcNow);
        Assert.True(OperatorSessionContextFactory.TryCreate(fixture.Actor, out var session));
        Assert.True(ManagedIdentityEndpoints.Allows(fixture.Identity.Store, session!, AuthorizationAction.BrokerRead, "test-cluster"));
        var options = new BrokerHostOptions { Enabled = true, LogReaderUserIds = [operatorId] };
        Assert.True(BrokerLogEndpoints.MayRead(fixture.Identity.Store, options, session, "test-cluster"));
        Assert.False(BrokerLogEndpoints.MayRead(fixture.Identity.Store, options, session, "other-cluster"));
        fixture.Client.DefaultRequestHeaders.Remove(KafdeckAntiforgeryExtensions.HeaderName);
        using var csrf = await fixture.Client.GetFromJsonAsync<JsonDocument>("/api/v1/auth/csrf");
        fixture.Client.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName,
            csrf!.RootElement.GetProperty("requestToken").GetString());
        using var denied = await fixture.Client.PostAsJsonAsync("/api/v1/fleet/hosts/logs", Window(Now()));
        Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
        Assert.Equal(0, reader.Calls);
        Assert.Contains(fixture.Identity.Audit.Events, e => e.Outcome == SecurityAuditOutcome.Denied);
    }

    [Fact]
    public async Task Reads_a_bound_broker_only_redacts_before_response_and_audits_without_content()
    {
        var now = Now();
        var reader = new LogStub { Observation = new(1, "systemd",
            [new(now * 1000, "Diagnostic complete"), new(now * 1000, "PASSWORD=example-private-value")], false) };
        await using var fixture = await V11BrokerHostTests.HostFixture.Start(logReader: reader);
        Seed(fixture, V11BrokerHostTests.Draft(), "cluster-identity", "broker");
        using var unbound = await fixture.Client.PostAsJsonAsync("/api/v1/fleet/hosts/logs", Window(now, 2));
        Assert.Equal(HttpStatusCode.NotFound, unbound.StatusCode);
        Assert.Equal(0, reader.Calls);
        using (var response = await fixture.Client.PostAsJsonAsync("/api/v1/fleet/hosts/logs", Window(now)))
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(response.Headers.CacheControl?.NoStore);
            var content = await response.Content.ReadAsStringAsync();
            Assert.Contains("Diagnostic complete", content);
            Assert.Contains("[REDACTED]", content);
            Assert.DoesNotContain("example-private-value", content);
        }
        Assert.Equal(new BrokerLogCommand("broker-1", 1, "cluster-identity", now - 20, now + 1), reader.Request);
        Assert.Contains(fixture.Identity.Audit.Events, e => e.EventType == SecurityAuditEventType.SensitiveRead && e.Outcome == SecurityAuditOutcome.Succeeded);
        Assert.DoesNotContain("example-private-value", JsonSerializer.Serialize(fixture.Identity.Audit.Events));
        fixture.Client.DefaultRequestHeaders.Remove(KafdeckAntiforgeryExtensions.HeaderName);
        using var noCsrf = await fixture.Client.PostAsJsonAsync("/api/v1/fleet/hosts/logs", Window(now));
        Assert.Equal(HttpStatusCode.Forbidden, noCsrf.StatusCode);
        Assert.Equal(1, reader.Calls);
    }

    [Fact]
    public async Task Binding_replaced_during_read_discards_result()
    {
        var reader = new LogStub();
        await using var fixture = await V11BrokerHostTests.HostFixture.Start(logReader: reader);
        Seed(fixture, V11BrokerHostTests.Draft(), "cluster-identity", "broker");
        reader.DuringRead = () => fixture.Store.Commit(1, state => state with { Bindings = [] }, Audit(fixture));
        using var response = await fixture.Client.PostAsJsonAsync("/api/v1/fleet/hosts/logs", Window(Now()));
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Contains("broker_log_binding_changed", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public void Enforces_time_byte_and_line_limits_and_masks_sensitive_formats()
    {
        var at = Now();
        var input = Window(at);
        Assert.True(BrokerLogEndpoints.ValidWindow(input, at));
        Assert.False(BrokerLogEndpoints.ValidWindow(input with { From = at - 3601 }, at));
        Assert.False(BrokerLogEndpoints.ValidWindow(input with { From = long.MinValue, To = long.MaxValue }, at));
        var page = BrokerLogEndpoints.Prepare(input, new(1, "systemd",
            [.. Enumerable.Repeat(new BrokerLogLine(at * 1000, new string('ژ', 700)), 200)], false));
        Assert.True(page.Truncated);
        Assert.InRange(page.Lines.Count, 1, 199);
        Assert.Throws<BrokerHostFailure>(() => BrokerLogEndpoints.Prepare(input, new(1, "compose",
            [new((at + 2) * 1000, "out of window")], false)));
        Assert.Throws<BrokerHostFailure>(() => BrokerLogEndpoints.Prepare(input, new(1, "compose",
            [.. Enumerable.Repeat(new BrokerLogLine(at * 1000, "line"), 201)], false)));
        var sensitive = new[] { "https://user:sentinel@example.test/path", "Bearer sentinel", "a\nsecond value",
            """{"Authorization":"value","related":"value"}""", "sasl.jaas.config = value", "Cookie: specimen" };
        var redacted = BrokerLogEndpoints.Prepare(input, new(1, "compose", sensitive.Select(text =>
            new BrokerLogLine(at * 1000, text)).ToArray(), false));
        Assert.All(redacted.Lines, line => Assert.Equal("[REDACTED]", line.Text));
    }

    internal static void Seed(V11BrokerHostTests.HostFixture fixture, BrokerHostDraft draft, string kafkaClusterId, string prefix)
    {
        var host = new BrokerHost(Guid.NewGuid(), draft, DateTimeOffset.UtcNow);
        var bindings = Enumerable.Range(1, 2).Select(id => new BrokerHostBinding("test-cluster", id, host.Id,
            prefix + "-" + id, kafkaClusterId, "previously-observed", DateTimeOffset.UtcNow)).ToArray();
        // The regression fixture exposes just broker 1. Live fixtures have both.
        if (prefix == "broker") bindings = [bindings[0]];
        fixture.Store.Commit(0, state => state with { Hosts = [host], Bindings = bindings }, Audit(fixture));
    }

    private static SecurityAuditEvent Audit(V11BrokerHostTests.HostFixture fixture) => new(DateTimeOffset.UtcNow,
        SecurityAuditEventType.BrokerHostManagement, SecurityAuditPrincipal.FromOperator(fixture.Identity.Owner.Identity),
        "fixture-binding", "test-cluster", "1", SecurityAuditOutcome.Succeeded, "fixture_bound");

    private sealed class LogStub : IBrokerLogReader
    {
        internal int Calls;
        internal BrokerLogCommand? Request;
        internal Action? DuringRead;
        internal BrokerLogObservation Observation = new(1, "systemd", [], false);
        public Task<BrokerLogObservation> ReadAsync(BrokerHostDraft host, BrokerLogCommand request, CancellationToken cancellation)
        {
            Calls++;
            Request = request;
            DuringRead?.Invoke();
            return Task.FromResult(Observation);
        }
    }
}

public sealed class V11BrokerLogLiveTests
{
    [BrokerLogLiveFact]
    public async Task Same_broker_view_and_alert_link_read_real_scoped_logs()
    {
        string Env(string name) => Environment.GetEnvironmentVariable("KAFDECK_HOST_" + name)
            ?? throw new InvalidOperationException("Missing log fixture setting: " + name);
        var at = long.Parse(Env("LOG_TIME"), System.Globalization.CultureInfo.InvariantCulture);
        var options = V11BrokerHostTests.Options(Env("ADDRESS"), Env("KEY"));
        var draft = V11BrokerHostTests.Draft(Env("ADDRESS"), Env("PIN")) with { Port = int.Parse(Env("PORT")) };
        using var kafka = new ConfluentKafkaAdministrationAdapter([new ClusterProfile("test-cluster", Env("KAFKA").Split(','),
            KafkaSecurityProtocol.Plaintext, null, null)], new SecretResolver());
        var ssh = new BrokerHostSsh(options);
        await using var fixture = await V11BrokerHostTests.HostFixture.Start(options, ssh, kafka, ssh, at);
        V11BrokerLogTests.Seed(fixture, draft, Env("LOG_CLUSTER_ID"), Env("LOG_TARGET_PREFIX"));
        var siblingAt = long.Parse(Env("LOG_SIBLING_TIME"), System.Globalization.CultureInfo.InvariantCulture);
        var request = V11BrokerLogTests.Window(Math.Min(at, siblingAt)) with { To = Math.Max(at, siblingAt) + 5 };
        using (var siblingResponse = await fixture.Client.PostAsJsonAsync("/api/v1/fleet/hosts/logs", request with { BrokerId = 2 }))
        {
            Assert.True(siblingResponse.IsSuccessStatusCode, "Sibling log read returned " + siblingResponse.StatusCode);
            var sibling = await siblingResponse.Content.ReadFromJsonAsync<BrokerLogPage>();
            Assert.True(sibling?.Lines.Any(line => line.Text.Contains(Env("LOG_SIBLING"), StringComparison.Ordinal)) == true, "Sibling marker absent from its own service.");
            Assert.False(sibling?.Lines.Any(line => line.Text.Contains(Env("LOG_MARKER"), StringComparison.Ordinal)) == true);
        }
        var start = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        start.ArgumentList.Add(Env("LOG_BROWSER_SCRIPT"));
        start.ArgumentList.Add(fixture.Client.BaseAddress!.ToString());
        using var process = Process.Start(start)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        try { await process.WaitForExitAsync(deadline.Token); }
        finally { if (!process.HasExited) process.Kill(entireProcessTree: true); }
        Assert.True(process.ExitCode == 0, await stdout + await stderr);
    }
}

internal sealed class BrokerLogLiveFactAttribute : FactAttribute
{
    public BrokerLogLiveFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("KAFDECK_RUN_BROKER_LOG_LIVE") != "1")
            Skip = "Requires provisioned native or Compose broker log fixtures.";
    }
}
