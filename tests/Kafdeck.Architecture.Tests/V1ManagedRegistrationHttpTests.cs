using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Confluent.Kafka;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedRegistrationHttpTests
{
    [Fact]
    public async Task Registration_and_duplicate_have_one_target_and_distinct_redacted_audit_outcomes()
    {
        await using var f = await ManagedRegistrationHttpFixture.Start();
        using var first = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", f.Draft());
        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        var target = Assert.Single(f.Identity.Store.Read().Connections!);
        using var duplicate = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", f.Draft());
        Assert.Equal(HttpStatusCode.OK, duplicate.StatusCode);
        Assert.Single(f.Identity.Store.Read().Connections!);
        Assert.Equal(2, f.Identity.Audit.Events.Count);
        var created = f.Identity.Audit.Events[0];
        Assert.Equal(SecurityAuditEventType.KafkaTargetRegistration, created.EventType);
        Assert.Equal(SecurityAuditOutcome.Succeeded, created.Outcome);
        Assert.Equal("kafka_target_registered", created.ReasonCategory);
        Assert.Equal(target.Id, created.ClusterId);
        Assert.Equal($"oidc:{ManagedProvisioningFixture.Issuer}|{f.Identity.OwnerId:D}", created.Principal);
        Assert.Equal("kafka_target_registration_reused", f.Identity.Audit.Events[1].ReasonCategory);
        var audit = JsonSerializer.Serialize(f.Identity.Audit.Events);
        foreach (var secret in new[] { "fixture-password-123!", "fixture-username", "127.0.0.1:19092", "Fixture office" })
            Assert.DoesNotContain(secret, audit, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false, 400)]
    [InlineData(true, 502)]
    public async Task Failed_existing_target_registration_retains_its_authorized_audit_identity(bool providerFailure, int status)
    {
        await using var f = await ManagedRegistrationHttpFixture.Start();
        using var registered = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", f.Draft());
        Assert.Equal(HttpStatusCode.OK, registered.StatusCode);
        var target = Assert.Single(f.Identity.Store.Read().Connections!);
        f.Identity.Audit.Events.Clear();
        var draft = f.Draft();
        if (providerFailure) f.ProbeError = new KafkaException(new Error(ErrorCode.Local_AllBrokersDown));
        else draft = draft with { Password = null };
        using var failed = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", draft);
        Assert.Equal(status, (int)failed.StatusCode);
        var audit = Assert.Single(f.Identity.Audit.Events);
        Assert.Equal(SecurityAuditOutcome.Failed, audit.Outcome);
        Assert.Equal(target.Id, audit.ClusterId);
        Assert.Single(f.Identity.Store.Read().Connections!);
    }

    [Fact]
    public async Task Connection_test_does_not_claim_registration_or_emit_registration_success()
    {
        await using var f = await ManagedRegistrationHttpFixture.Start();
        using var response = await f.Client.PostAsJsonAsync("/api/v1/connections/test", f.Draft());
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Null(f.Identity.Store.Read().Connections);
        Assert.Empty(f.Identity.Audit.Events);
    }

    [Fact]
    public async Task Non_owner_registration_is_audited_as_denied_without_metadata_io()
    {
        await using var f = await ManagedRegistrationHttpFixture.Start(owner: false);
        using var response = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", f.Draft());
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal(0, f.ProbeCalls);
        Assert.Equal(SecurityAuditOutcome.Denied, Assert.Single(f.Identity.Audit.Events).Outcome);
        Assert.Null(f.Identity.Store.Read().Connections);
    }

    [Theory]
    [InlineData("network", 403)]
    [InlineData("input", 400)]
    [InlineData("provider", 502)]
    public async Task Rejection_has_no_saved_target_or_success_audit(string cause, int expected)
    {
        await using var f = await ManagedRegistrationHttpFixture.Start();
        var draft = f.Draft();
        if (cause == "network") draft = draft with { BootstrapServers = ["192.0.2.10:9092"] };
        if (cause == "input") draft = draft with { Password = null };
        if (cause == "provider") f.ProbeError = new KafkaException(new Error(ErrorCode.Local_AllBrokersDown));
        using var response = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", draft);
        Assert.Equal(expected, (int)response.StatusCode);
        Assert.Null(f.Identity.Store.Read().Connections);
        Assert.NotEqual(SecurityAuditOutcome.Succeeded, Assert.Single(f.Identity.Audit.Events).Outcome);
    }

    [Fact]
    public async Task Real_filesystem_write_failure_cannot_emit_registration_success()
    {
        await using var f = await ManagedRegistrationHttpFixture.Start();
        f.AfterProbe = f.Identity.BreakStateWrites;
        using var response = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", f.Draft());
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal(SecurityAuditOutcome.Failed, Assert.Single(f.Identity.Audit.Events).Outcome);
        f.Identity.HealStateWrites();
        Assert.Null(f.Identity.Store.Read().Connections);
    }

    [Fact]
    public async Task Csrf_failure_has_no_registration_effect_and_no_false_success_event()
    {
        await using var f = await ManagedRegistrationHttpFixture.Start();
        f.Client.DefaultRequestHeaders.Remove("X-Kafdeck-CSRF");
        using var response = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", f.Draft());
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal(0, f.ProbeCalls);
        Assert.Null(f.Identity.Store.Read().Connections);
        Assert.DoesNotContain(f.Identity.Audit.Events, item => item.Outcome == SecurityAuditOutcome.Succeeded);
    }

    [Fact]
    public async Task Http_pending_list_and_recovery_never_expose_proof_or_resubmit_creation()
    {
        await using var f = await ManagedRegistrationHttpFixture.Start();
        f.Identity.Provider.LoseReply = true;
        using var create = await f.Client.PostAsJsonAsync("/api/v1/identity/users", f.Identity.Input);
        Assert.Equal(HttpStatusCode.Conflict, create.StatusCode);
        var intent = Assert.Single(f.Identity.Store.Read().UserProvisionings!);
        var listing = await f.Client.GetStringAsync("/api/v1/identity/users");
        Assert.Contains(intent.RequestId.ToString("D"), listing, StringComparison.Ordinal);
        Assert.DoesNotContain(intent.Proof, listing, StringComparison.Ordinal);
        Assert.DoesNotContain(f.Identity.Input.Password!, listing, StringComparison.Ordinal);
        using var recovery = await f.Client.PostAsJsonAsync($"/api/v1/identity/users/provisioning/{intent.RequestId:D}/recover", new { });
        Assert.Equal(HttpStatusCode.Created, recovery.StatusCode);
        Assert.Equal(1, f.Identity.Provider.CreateCount);
        Assert.Equal(2, f.Identity.Store.Read().Users.Length);
    }

    [Fact]
    public async Task Recovery_requires_csrf_and_does_not_grant_membership_when_the_token_is_absent()
    {
        await using var f = await ManagedRegistrationHttpFixture.Start();
        f.Identity.Provider.LoseReply = true;
        using var created = await f.Client.PostAsJsonAsync("/api/v1/identity/users", f.Identity.Input);
        Assert.Equal(HttpStatusCode.Conflict, created.StatusCode);
        var intent = Assert.Single(f.Identity.Store.Read().UserProvisionings!);
        f.Client.DefaultRequestHeaders.Remove("X-Kafdeck-CSRF");
        using var denied = await f.Client.PostAsJsonAsync($"/api/v1/identity/users/provisioning/{intent.RequestId:D}/recover", new { });
        Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
        Assert.Single(f.Identity.Store.Read().Users);
        Assert.Equal(1, f.Identity.Provider.CreateCount);
    }

    [Fact]
    public async Task Non_owner_cannot_recover_another_users_provisioning_request()
    {
        await using var f = await ManagedRegistrationHttpFixture.Start(owner: false);
        using var response = await f.Client.PostAsJsonAsync($"/api/v1/identity/users/provisioning/{Guid.NewGuid():D}/recover", new { });
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal(0, f.Identity.Provider.CreateCount);
    }
}

internal sealed class ManagedRegistrationHttpFixture : IAsyncDisposable
{
    internal readonly ManagedProvisioningFixture Identity = new();
    internal HttpClient Client = null!;
    internal int ProbeCalls;
    internal Exception? ProbeError;
    internal Action? AfterProbe;
    private WebApplication _app = null!;
    internal KafkaConnectionWorkspace.ConnectionDraft Draft() => new(["127.0.0.1:19092"], "SASL_PLAINTEXT", "PLAIN",
        "fixture-username", "fixture-password-123!", null, null, null, null, "Fixture office");
    internal static async Task<ManagedRegistrationHttpFixture> Start(bool owner = true)
    {
        var f = new ManagedRegistrationHttpFixture();
        var builder = WebApplication.CreateBuilder(); builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Configuration["Kafdeck:ConnectionWorkspace:RegistrationNetworks:0"] = "127.0.0.0/8";
        var options = new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0", null, AccessMode.Oidc, null), []);
        var policy = AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition([], [], []));
        builder.Services.AddSingleton(new KafdeckAuthorizationService(options, new AuthorizationPolicyEvaluator(policy)) { ManagedIdentity = f.Identity.Store });
        builder.Services.AddSingleton<ISecurityAuditSink>(f.Identity.Audit);
        builder.Services.AddSingleton(f.Identity.Client);
        builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
        var actor = owner ? f.Identity.OwnerId : Guid.NewGuid();
        if (!owner)
        {
            var state = f.Identity.Store.Read();
            f.Identity.Store.Write(state with { Users = [.. state.Users, new(actor, "viewer@example.test", "viewer", [], true, default)] });
        }
        var principal = OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", actor.ToString("D"))], "oidc")),
            ManagedProvisioningFixture.Issuer, null, DateTimeOffset.UtcNow);
        f._app = builder.Build(); f._app.UseRouting();
        f._app.Use((context, next) => { context.User = principal; return next(context); });
        f._app.UseAntiforgery(); f._app.MapKafdeckAntiforgeryEndpoint();
        f._app.MapManagedIdentity(f.Identity.Store, options);
        f._app.MapKafkaConnectionWorkspace(options, f.Identity.Store, (_, _) =>
        {
            f.ProbeCalls++;
            if (f.ProbeError is { } error) throw error;
            f.AfterProbe?.Invoke();
            return Task.FromResult(new Metadata([new Confluent.Kafka.BrokerMetadata(1, "127.0.0.1", 19092)], [], 1, "127.0.0.1:19092"));
        });
        await f._app.StartAsync();
        f.Client = new(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new Uri(Assert.Single(f._app.Urls)) };
        using var token = await f.Client.GetFromJsonAsync<JsonDocument>("/api/v1/auth/csrf");
        f.Client.DefaultRequestHeaders.Add("X-Kafdeck-CSRF", token!.RootElement.GetProperty("requestToken").GetString());
        return f;
    }
    public async ValueTask DisposeAsync()
    {
        Client.Dispose(); await _app.StopAsync(); await _app.DisposeAsync(); Identity.Dispose();
    }
}
