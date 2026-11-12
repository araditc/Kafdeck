using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedProvisioningTests
{
    [Fact]
    public async Task Creation_has_durable_intent_before_provider_dispatch_and_exact_retry_has_one_effect()
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.AfterCreate = () => Assert.Single(f.Store.Read().UserProvisionings!);
        var result = await f.Service.Create(f.Owner, f.Input);
        Assert.Equal(201, result.Status);
        var retry = await f.Service.Create(f.Owner, f.Input);
        Assert.Equal(result.User!.Id, retry.User!.Id);
        Assert.Equal(result.User.ClusterIds, retry.User.ClusterIds);
        Assert.Equal(1, f.Provider.CreateCount);
        Assert.Single(f.Audit.Events);
        var intent = Assert.Single(f.Store.Read().UserProvisionings!);
        Assert.True(intent.Completed && intent.Audited);
        Assert.DoesNotContain(f.Input.Password!, File.ReadAllText(f.StatePath), StringComparison.Ordinal);
        Assert.DoesNotContain(intent.Proof, JsonSerializer.Serialize(f.Audit.Events), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Lost_reply_recovers_after_restart_without_another_create_or_password_reset()
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.LoseReply = true;
        var failed = await f.Service.Create(f.Owner, f.Input);
        Assert.Equal("identity-provisioning-pending", failed.Code);
        Assert.Single(f.Store.Read().Users); // Only the owner can log in.
        var service = f.Restart();
        var recovered = await service.Recover(f.Owner, failed.OperationId!.Value);
        Assert.Equal(201, recovered.Status);
        Assert.Equal(2, f.Store.Read().Users.Length);
        Assert.Equal(1, f.Provider.CreateCount);
        Assert.Single(f.Audit.Events);
    }

    [Fact]
    public async Task Exact_retry_of_unknown_outcome_reconciles_without_using_a_reentered_password()
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.LoseReply = true;
        Assert.Equal("identity-provisioning-pending", (await f.Service.Create(f.Owner, f.Input)).Code);
        var recovered = await f.Restart().Create(f.Owner, f.Input with { Password = "Different-password-456!" });
        Assert.Equal(201, recovered.Status);
        Assert.Equal(1, f.Provider.CreateCount);
        Assert.Equal(2, f.Store.Read().Users.Length);
    }

    [Fact]
    public async Task Failed_local_receipt_write_leaves_recoverable_intent_on_disk()
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.AfterCreate = f.BreakStateWrites;
        var failed = await f.Service.Create(f.Owner, f.Input);
        Assert.Equal("identity-provisioning-pending", failed.Code);
        f.HealStateWrites();
        Assert.Single(f.Store.Read().Users);
        Assert.Empty(f.Audit.Events);
        var recovered = await f.Restart().Recover(f.Owner, failed.OperationId!.Value);
        Assert.Equal(201, recovered.Status);
        Assert.Equal(1, f.Provider.CreateCount);
        Assert.Single(f.Audit.Events);
    }

    [Fact]
    public async Task Intent_write_failure_never_dispatches_to_Keycloak()
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.AfterProfile = f.BreakStateWrites;
        var failed = await f.Service.Create(f.Owner, f.Input);
        Assert.Equal("identity-storage", failed.Code);
        Assert.Equal(0, f.Provider.CreateCount);
        f.HealStateWrites();
        Assert.Null(f.Store.Read().UserProvisionings);
    }

    [Fact]
    public async Task Account_absence_or_provider_outage_never_triggers_blind_redispatch()
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.Status = HttpStatusCode.ServiceUnavailable;
        var failed = await f.Service.Create(f.Owner, f.Input);
        var key = failed.OperationId!.Value;
        Assert.Equal("identity-provisioning-pending", (await f.Restart().Recover(f.Owner, key)).Code);
        f.Provider.Unavailable = true;
        Assert.Equal("identity-provisioning-pending", (await f.Service.Recover(f.Owner, key)).Code);
        Assert.Equal(1, f.Provider.CreateCount);
        Assert.Single(f.Store.Read().Users);
    }

    [Fact]
    public async Task Unrelated_existing_email_is_never_adopted()
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.Accounts.Add(Guid.NewGuid(), new(f.Input.Email!, "unrelated", true));
        var failed = await f.Service.Create(f.Owner, f.Input);
        Assert.Equal("identity-duplicate", failed.Code);
        Assert.Single(f.Store.Read().Users);
        Assert.Empty(f.Audit.Events);
        Assert.Equal("identity-duplicate", (await f.Service.Create(f.Owner, f.Input)).Code);
        Assert.Equal(1, f.Provider.CreateCount);
    }

    [Theory]
    [InlineData("proof")]
    [InlineData("email")]
    [InlineData("disabled")]
    public async Task Recovery_rejects_changed_or_unrelated_provider_evidence(string changed)
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.LoseReply = true;
        var failed = await f.Service.Create(f.Owner, f.Input);
        var id = Assert.Single(f.Provider.Accounts.Keys);
        var account = f.Provider.Accounts[id];
        f.Provider.Accounts[id] = changed switch
        {
            "proof" => account with { Proof = "other" },
            "email" => account with { Email = "other@example.test" },
            _ => account with { Enabled = false },
        };
        Assert.Equal("identity-provisioning-pending", (await f.Restart().Recover(f.Owner, failed.OperationId!.Value)).Code);
        Assert.Single(f.Store.Read().Users);
        Assert.Empty(f.Audit.Events);
        Assert.Equal(1, f.Provider.CreateCount);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    public async Task Missing_or_user_editable_proof_attribute_blocks_before_dispatch(bool present, bool editable)
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.ProfilePresent = present; f.Provider.UserEditable = editable;
        Assert.Equal("identity-provisioning-profile", (await f.Service.Create(f.Owner, f.Input)).Code);
        Assert.Equal(0, f.Provider.CreateCount);
        Assert.Null(f.Store.Read().UserProvisionings);
    }

    [Fact]
    public async Task Changed_material_and_revoked_authority_cannot_recover_an_old_intent()
    {
        using var f = new ManagedProvisioningFixture();
        f.Provider.LoseReply = true;
        var pending = await f.Service.Create(f.Owner, f.Input);
        Assert.Equal("identity-provisioning-conflict", (await f.Service.Create(f.Owner, f.Input with { Role = "manager" })).Code);
        var state = f.Store.Read();
        f.Store.Write(state with { Users = [state.Users[0] with { SessionsValidFrom = DateTimeOffset.UtcNow.AddMinutes(1) }] });
        Assert.Equal("identity-forbidden", (await f.Restart().Recover(f.Owner, pending.OperationId!.Value)).Code);
        Assert.Equal(1, f.Provider.CreateCount);
        Assert.Empty(f.Audit.Events);
    }

    [Fact]
    public async Task Concurrent_dispatch_is_fenced_by_the_real_state_file_lease()
    {
        using var f = new ManagedProvisioningFixture();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        f.Provider.DispatchGate = async () => { entered.SetResult(); await release.Task; };
        var first = f.Service.Create(f.Owner, f.Input);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try { Assert.Equal("identity-busy", (await f.Restart().Create(f.Owner, f.Input)).Code); }
        finally { release.SetResult(); }
        Assert.Equal(201, (await first).Status);
        Assert.Equal(1, f.Provider.CreateCount);
    }

    [Fact]
    public async Task Audit_failure_keeps_a_durable_receipt_and_recovery_does_not_recreate_account()
    {
        using var f = new ManagedProvisioningFixture();
        f.Audit.FailNext = true;
        var pending = await f.Service.Create(f.Owner, f.Input);
        Assert.Equal("identity-provisioning-pending", pending.Code);
        Assert.Equal(2, f.Store.Read().Users.Length);
        Assert.Equal(201, (await f.Restart().Recover(f.Owner, pending.OperationId!.Value)).Status);
        Assert.Single(f.Audit.Events);
        Assert.Equal(pending.OperationId.Value.ToString("N"), f.Audit.Events[0].SessionCorrelationId);
        Assert.Equal(1, f.Provider.CreateCount);
    }
}

internal sealed class ManagedProvisioningFixture : IDisposable
{
    internal const string Issuer = "https://idp.example.test/realms/kafdeck";
    internal readonly string DirectoryPath = Path.Combine(Path.GetTempPath(), "kafdeck-g01-" + Guid.NewGuid().ToString("N"));
    internal string StatePath => Path.Combine(DirectoryPath, "identity.json");
    internal ManagedIdentityStore Store => new(StatePath, Issuer);
    internal readonly Guid OwnerId = Guid.NewGuid();
    internal readonly KeycloakProvisioningHandler Provider = new();
    internal readonly ManagedAuditCapture Audit = new();
    internal readonly ManagedKeycloakUsers Client;
    internal OperatorSessionContext Owner { get; }
    internal ManagedIdentityEndpoints.NewUserInput Input { get; } = new("new@example.test", "Temporary-password-123!", "viewer", [], Guid.NewGuid());
    internal ManagedUserProvisioningService Service => new(Store, Client, Audit, ["fixture"]);
    internal ManagedUserProvisioningService Restart() => new(new ManagedIdentityStore(StatePath, Issuer), Client, Audit, ["fixture"]);
    internal ManagedProvisioningFixture()
    {
        Directory.CreateDirectory(DirectoryPath);
        Store.Write(new(Issuer, OwnerId, false, null, default, [new(OwnerId, "owner@example.test", "owner", [], true, default)]));
        Owner = new(new(new(Issuer, OwnerId.ToString("D"))), new SessionId(Guid.NewGuid()), DateTimeOffset.UtcNow);
        Client = new(new HttpClient(Provider), Issuer, "provisioner", "fixture-secret");
    }
    internal void BreakStateWrites()
    {
        File.Move(StatePath, StatePath + ".saved");
        Directory.CreateDirectory(StatePath);
    }
    internal void HealStateWrites()
    {
        if (!File.Exists(StatePath + ".saved")) return;
        Directory.Delete(StatePath);
        File.Move(StatePath + ".saved", StatePath);
    }
    public void Dispose()
    {
        Client.Dispose(); HealStateWrites();
        if (!Path.GetFullPath(DirectoryPath).StartsWith(Path.GetFullPath(Path.GetTempPath()), StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Unexpected fixture path.");
        Directory.Delete(DirectoryPath, recursive: true);
    }
}

internal sealed class ManagedAuditCapture : ISecurityAuditSink
{
    internal readonly List<SecurityAuditEvent> Events = [];
    internal bool FailNext;
    public ValueTask WriteAsync(SecurityAuditEvent value, CancellationToken cancellationToken = default)
    {
        if (FailNext) { FailNext = false; throw new IOException("Fixture audit unavailable."); }
        Events.Add(value); return ValueTask.CompletedTask;
    }
}

internal sealed class KeycloakProvisioningHandler : HttpMessageHandler
{
    internal sealed record Account(string Email, string Proof, bool Enabled);
    internal readonly Dictionary<Guid, Account> Accounts = [];
    internal int CreateCount;
    internal bool LoseReply, Unavailable, UserEditable;
    internal bool ProfilePresent = true;
    internal HttpStatusCode Status = HttpStatusCode.Created;
    internal Action? AfterCreate, AfterProfile;
    internal Func<Task>? DispatchGate;
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
    {
        var uri = request.RequestUri!;
        if (Unavailable) throw new HttpRequestException("Fixture provider unavailable.");
        if (uri.AbsolutePath.EndsWith("/token", StringComparison.Ordinal)) return Json(new { access_token = "fixture-token" });
        if (uri.AbsolutePath.EndsWith("/profile", StringComparison.Ordinal))
        {
            AfterProfile?.Invoke();
            return Json(new { attributes = ProfilePresent ? new[] { new { name = ManagedKeycloakUsers.ProvisioningAttribute,
                multivalued = false, permissions = new { view = new[] { "admin" }, edit = UserEditable ? new[] { "admin", "user" } : new[] { "admin" } } } } : [] });
        }
        if (request.Method == HttpMethod.Post)
        {
            CreateCount++;
            if (DispatchGate is { } gate) await gate();
            if (Status != HttpStatusCode.Created) return new(Status);
            using var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(token));
            var email = body.RootElement.GetProperty("email").GetString()!;
            if (Accounts.Values.Any(account => account.Email == email)) return new(HttpStatusCode.Conflict);
            var proof = body.RootElement.GetProperty("attributes").GetProperty(ManagedKeycloakUsers.ProvisioningAttribute)[0].GetString()!;
            var id = Guid.NewGuid(); Accounts.Add(id, new(email, proof, true));
            AfterCreate?.Invoke();
            if (LoseReply) throw new HttpRequestException("Fixture dropped the already committed reply.");
            var response = new HttpResponseMessage(HttpStatusCode.Created);
            response.Headers.Location = new Uri(uri.AbsoluteUri + "/" + id); return response;
        }
        object Representation(KeyValuePair<Guid, Account> pair) => new { id = pair.Key.ToString("D"), username = pair.Value.Email,
            email = pair.Value.Email, enabled = pair.Value.Enabled,
            attributes = new Dictionary<string, string[]> { [ManagedKeycloakUsers.ProvisioningAttribute] = [pair.Value.Proof] } };
        if (Guid.TryParse(uri.Segments[^1], out var key))
            return Accounts.TryGetValue(key, out var account) ? Json(Representation(new(key, account))) : new(HttpStatusCode.NotFound);
        var query = Uri.UnescapeDataString(uri.Query);
        return Json(Accounts.Where(pair => query.Contains("username=" + pair.Value.Email + "&", StringComparison.Ordinal)).Select(Representation).ToArray());
    }
    private static HttpResponseMessage Json(object value) => new(HttpStatusCode.OK) { Content = JsonContent.Create(value) };
}
