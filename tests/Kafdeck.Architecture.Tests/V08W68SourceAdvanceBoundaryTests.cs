using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

/// <summary>Production typed handlers and real SQLite CAS. Identity injection is a boundary fixture; live Keycloak proof is separate.</summary>
public sealed class V08W68SourceAdvanceBoundaryTests
{
    [Fact]
    public async Task Fixed_pin_default_never_grants_source_advance()
    {
        await using var fixture = await Fixture.CreateAsync(false);
        var response = await fixture.AdvanceAsync(Manifest("Paused"), Guid.Empty, 0, fixture.BaselineHash);
        Assert.Equal(403, ((IStatusCodeHttpResult)response).StatusCode);
        Assert.Null(await fixture.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, "deployment", "source"));
        Assert.Null(await fixture.Routing.GetSubscriptionAsync("subscription"));
    }

    [Fact]
    public async Task Plan_only_grants_cannot_advance_source_or_bootstrap_registrations()
    {
        await using var fixture = await Fixture.CreateAsync(true);
        fixture.SourceManage = false;
        fixture.AllowPlanningOnly();
        Assert.True(fixture.Authority.Authorize(fixture.Principal, "deployment", "owner", "source", fixture.BaselineHash,
            "subscription", ["new"], DeclarativeRuntimeVerb.Plan));
        var response = await fixture.AdvanceAsync(Manifest("Paused"), Guid.Empty, 0, fixture.BaselineHash);
        Assert.Equal(403, ((IStatusCodeHttpResult)response).StatusCode);
        Assert.Null(await fixture.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, "deployment", "source"));
        Assert.Null(await fixture.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Owner, "deployment", "owner"));
    }

    [Fact]
    public async Task Explicit_source_manager_without_plan_or_apply_advances_only_source_hash_with_monotonic_CAS()
    {
        await using var fixture = await Fixture.CreateAsync(true);
        var baseline = Ok(await fixture.AdvanceAsync(Manifest("Active"), Guid.Empty, 0, fixture.BaselineHash));
        Assert.Equal(1, baseline.Revision); Assert.False(baseline.Changed);
        var next = Ok(await fixture.AdvanceAsync(Manifest("Paused"), baseline.Generation, baseline.Revision, baseline.SourceHash));
        Assert.Equal(baseline.Generation, next.Generation); Assert.Equal(2, next.Revision); Assert.True(next.Changed);
        Assert.Equal(Desired("Paused").Fingerprint(), next.SourceHash);
        Assert.False(fixture.Authority.Authorize(fixture.Principal, "deployment", "owner", "source", next.SourceHash,
            "subscription", ["new"], DeclarativeRuntimeVerb.Plan));
        Assert.False(fixture.Authority.Authorize(fixture.Principal, "deployment", "owner", "source", next.SourceHash,
            "subscription", ["new"], DeclarativeRuntimeVerb.Apply));
        Assert.Null(await fixture.Routing.GetSubscriptionAsync("subscription"));
        Assert.Null(await fixture.Store.GetOwnershipAsync(new("deployment", "Deployment", "deployment", "NotificationSubscription", "subscription")));
        var replay = Ok(await fixture.AdvanceAsync(Manifest("Paused"), next.Generation, next.Revision, next.SourceHash));
        Assert.False(replay.Changed); Assert.Equal(next.Revision, replay.Revision);
        var stale = await fixture.AdvanceAsync(Manifest("Active"), baseline.Generation, baseline.Revision, baseline.SourceHash);
        Assert.Equal(409, ((IStatusCodeHttpResult)stale).StatusCode);
        var source = await fixture.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, "deployment", "source");
        Assert.Equal(next.Revision, source!.Revision); Assert.Equal(next.SourceHash, source.PinnedRevision);
    }

    [Theory]
    [InlineData("source")][InlineData("source-cycle")][InlineData("policy")][InlineData("registration")]
    [InlineData("registered-actions")][InlineData("observed-state")][InlineData("generation")]
    public async Task Lifecycle_freshness_rejects_changed_inputs_before_confirmation_or_approval(string changed)
    {
        await using var fixture = await Fixture.CreateAsync(true);
        fixture.AllowFullMutation();
        var baseline = Ok(await fixture.AdvanceAsync(Manifest("Active"), Guid.Empty, 0, fixture.BaselineHash));
        var plan = await fixture.CreatePlanAsync(baseline);
        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(plan.PlanId, fixture.PrincipalId, DeclarativeSubscriptionMutation.Intent(plan),
            MutationRiskClassifier.Classify(new(MutationOperationKind.DeclarativeNotificationSubscription)),
            "v0.8-bearer:" + fixture.Authority.PolicyFingerprint, now.AddMinutes(2), now, "lifecycle");
        var guard = new DeclarativeLifecycleGuard(fixture.Store, fixture.Authority);
        Assert.True(await guard.IsCurrentAsync(operation.Snapshot));
        if (changed is "source" or "source-cycle")
        {
            var next = Ok(await fixture.AdvanceAsync(Manifest("Paused"), baseline.Generation, baseline.Revision, baseline.SourceHash));
            if (changed == "source-cycle") Ok(await fixture.AdvanceAsync(Manifest("Active"), next.Generation, next.Revision, next.SourceHash));
        }
        else if (changed == "policy") fixture.SourceManage = false;
        else if (changed == "registration") fixture.Configuration["Kafdeck:Declarative:Registrations:0:Enabled"] = "false";
        else if (changed == "registered-actions") fixture.Configuration["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:1"] = "DeclarativeAdopt";
        else if (changed == "observed-state") await fixture.Routing.CreateSubscriptionAsync(plan.DesiredSubscription, NotificationSubscriptionState.Active, now);
        else await W68RuntimeSchema.AdmitGenerationAsync(fixture.Connections, "deployment", fixture.Generation, Guid.NewGuid(), new string('b', 64));
        Assert.False(await guard.IsCurrentAsync(operation.Snapshot));
        Assert.Equal(MutationOperationState.Previewed, operation.Snapshot.State);
        Assert.Null(operation.Snapshot.ConfirmedAtUtc); Assert.Null(operation.Snapshot.ApprovedAtUtc);
        Assert.Null(await fixture.Store.GetCommitReceiptAsync(plan.PlanId));
    }

    [Theory]
    [InlineData("owner")][InlineData("source")][InlineData("resource")][InlineData("generation")][InlineData("hash")]
    public async Task Foreign_scope_or_stale_proof_cannot_rebind_or_advance_source(string failure)
    {
        await using var fixture = await Fixture.CreateAsync(true);
        var baseline = Ok(await fixture.AdvanceAsync(Manifest("Active"), Guid.Empty, 0, fixture.BaselineHash));
        var manifest = Manifest("Paused");
        if (failure == "owner") manifest = manifest.Replace("\"owner\"", "\"foreign\"", StringComparison.Ordinal);
        if (failure == "resource") manifest = manifest.Replace("\"subscription\"", "\"foreign\"", StringComparison.Ordinal);
        var result = await fixture.AdvanceAsync(manifest,
            failure == "generation" ? Guid.NewGuid() : baseline.Generation, baseline.Revision,
            failure == "hash" ? new string('0', 64) : baseline.SourceHash,
            failure == "source" ? "foreign" : "source");
        Assert.Equal(failure is "generation" or "hash" ? 409 : 403, ((IStatusCodeHttpResult)result).StatusCode);
        var current = await fixture.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, "deployment", "source");
        Assert.Equal(baseline.Generation, current!.GenerationId); Assert.Equal(baseline.Revision, current.Revision);
        Assert.Equal("owner", current.OwnerId); Assert.Equal("source", current.RepositoryId); Assert.Equal("subscription", current.Path);
        Assert.Equal(baseline.SourceHash, current.PinnedRevision);
    }

    [Fact]
    public async Task Source_hash_A_to_B_to_A_does_not_make_original_immutable_plan_current_again()
    {
        await using var fixture = await Fixture.CreateAsync(true);
        var baseline = Ok(await fixture.AdvanceAsync(Manifest("Active"), Guid.Empty, 0, fixture.BaselineHash));
        var id = Guid.NewGuid(); var key = new W68RuntimeResourceKey("deployment", "Deployment", "deployment", "NotificationSubscription", "subscription");
        var scope = W68RuntimeFingerprints.LeaseScope("subscription");
        var lease = await fixture.Store.AcquireLeaseAsync("deployment", scope, "controller", "owner", fixture.PrincipalId,
            id.ToString("N"), 120, _ => Task.FromResult(true));
        Assert.NotNull(lease);
        var manifest = Desired("Active");
        var now = await fixture.Store.ReadDatabaseUtcNowAsync();
        var plan = new W68StoredRuntimePlan(id, W68RuntimePlanCommand.Apply, key, "owner", "source", baseline.Generation,
            baseline.SourceHash, fixture.PrincipalId, 0, null, null, "missing", null, manifest.Definition, manifest.State,
            Encoding.UTF8.GetString(manifest.ToCanonicalUtf8()), manifest.Fingerprint(), fixture.Authority.PolicyFingerprint,
            "controller", scope, id.ToString("N"), lease!.Epoch, now.AddSeconds(100), "", DeploymentGeneration: fixture.Generation,
            SourceRegistrationRevision: baseline.Revision);
        plan = plan with { PlanFingerprint = W68RuntimeFingerprints.Plan(plan) };
        // This storage setup intentionally supplies trusted test authority; the
        // test isolates the independent persisted source-version fence.
        Assert.NotNull(await fixture.Store.SavePlanAsync(plan, (_, _) => Task.FromResult(true)));
        var next = Ok(await fixture.AdvanceAsync(Manifest("Paused"), baseline.Generation, baseline.Revision, baseline.SourceHash));
        var again = Ok(await fixture.AdvanceAsync(Manifest("Active"), next.Generation, next.Revision, next.SourceHash));
        Assert.Equal(baseline.SourceHash, again.SourceHash); Assert.Equal(3, again.Revision);
        Assert.Null(await fixture.Store.ApplySubscriptionAsync(id, plan.PlanFingerprint, fixture.PrincipalId, (_, _) => Task.FromResult(true)));
        Assert.Null(await fixture.Store.GetCommitReceiptAsync(id));
        Assert.Null(await fixture.Routing.GetSubscriptionAsync("subscription"));
    }

    [Theory]
    [InlineData("resourceId=subscription&resourceId=subscription")]
    [InlineData("resourceId=subscription&owner=foreign")]
    [InlineData("")]
    public async Task Source_CAS_read_query_is_closed_and_never_accepts_duplicate_selectors(string query)
    {
        await using var fixture = await Fixture.CreateAsync(true);
        fixture.Context.Request.QueryString = new QueryString(query.Length == 0 ? "" : "?" + query);
        var result = await KafdeckDeclarativeSourceEndpoints.ReadAsync("source", fixture.Context, fixture.Store, fixture.Routing, fixture.Authority, default);
        Assert.Equal(400, ((IStatusCodeHttpResult)result).StatusCode);
    }

    [Fact]
    public async Task Source_management_does_not_derive_read_authority_and_CAS_projection_omits_internal_bindings()
    {
        await using var fixture = await Fixture.CreateAsync(true);
        var baseline = Ok(await fixture.AdvanceAsync(Manifest("Active"), Guid.Empty, 0, fixture.BaselineHash));
        fixture.Context.Request.QueryString = new QueryString("?resourceId=subscription");
        var read = Ok(await KafdeckDeclarativeSourceEndpoints.ReadAsync("source", fixture.Context, fixture.Store, fixture.Routing, fixture.Authority, default));
        Assert.Equal(baseline.Generation, read.Generation);
        var projection = JsonSerializer.Serialize(read);
        Assert.DoesNotContain("Principal", projection, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Path", projection, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Owner", projection, StringComparison.OrdinalIgnoreCase);
        fixture.Configuration["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:1"] = "SystemRead";
        var denied = await KafdeckDeclarativeSourceEndpoints.ReadAsync("source", fixture.Context, fixture.Store, fixture.Routing, fixture.Authority, default);
        Assert.Equal(403, ((IStatusCodeHttpResult)denied).StatusCode);
        var next = Ok(await fixture.AdvanceAsync(Manifest("Paused"), baseline.Generation, baseline.Revision, baseline.SourceHash));
        Assert.Equal(2, next.Revision);
    }

    private static DeclarativeSourceRevisionData Ok(IResult result)
    {
        Assert.Equal(200, ((IStatusCodeHttpResult)result).StatusCode);
        return Assert.IsType<DeclarativeSourceRevisionData>(((IValueHttpResult)result).Value);
    }
    private static string Manifest(string state) => """{"apiVersion":"kafdeck.io/v0.8","kind":"NotificationSubscription","metadata":{"deploymentRef":"deployment","name":"subscription","ownerRef":"owner"},"spec":{"destinationRef":"new","state":"STATE","eventClasses":["Operational"],"eventTypes":[]}}""".Replace("STATE", state, StringComparison.Ordinal);
    private static DeclarativeSubscriptionManifest Desired(string state) => DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(Manifest(state)), "json");

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "kafdeck-w68-source-" + Guid.NewGuid().ToString("N"));
        private readonly IConfigurationRoot _configuration;
        public Guid Generation { get; } = Guid.NewGuid();
        public string BaselineHash => Desired("Active").Fingerprint();
        public ClaimsPrincipal Principal { get; }
        public string PrincipalId => ((NonBrowserReadIdentity)Principal.Identity!).AuditId;
        public DefaultHttpContext Context { get; } = new();
        public W68RuntimeStore Store { get; private set; } = null!;
        public AdoNotificationRoutingStore Routing { get; private set; } = null!;
        public DeclarativeRuntimeAuthorizer Authority { get; }
        public bool SourceManage { get; set; } = true;
        private bool _fullMutation;
        private bool _planOnly;
        public IConfiguration Configuration => _configuration;
        public INotificationDeliveryDbConnectionFactory Connections { get; private set; } = null!;
        private Fixture(bool advance)
        {
            Directory.CreateDirectory(_directory);
            Principal = new(new NonBrowserReadIdentity("service", new("https://idp.example", "subject", "client", "api", DateTimeOffset.UtcNow.AddMinutes(2))));
            Context.User = Principal;
            var anchorFile = Path.Combine(_directory, "external-anchor.json");
            File.WriteAllText(anchorFile, JsonSerializer.Serialize(new { version = 1, deploymentId = "deployment", generation = Generation, state = "active" }));
            _configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafdeck:NonBrowserRead:Enabled"]="true",["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"]="true",
                ["Kafdeck:NonBrowserRead:Issuer"]="https://idp.example",["Kafdeck:NonBrowserRead:ApiAudience"]="api",
                ["Kafdeck:NonBrowserRead:Registrations:0:Id"]="service",["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"]="owner",
                ["Kafdeck:NonBrowserRead:Registrations:0:Subject"]="subject",["Kafdeck:NonBrowserRead:Registrations:0:ClientId"]="client",
                ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"]="true",["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"]=DateTimeOffset.UtcNow.AddHours(1).ToString("O"),
                ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"]="NotificationRead",["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:1"]="DeclarativeRead",
                ["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:0"]="DeclarativeSourceManage",
                ["Kafdeck:Declarative:Enabled"]="true",["Kafdeck:Declarative:DeploymentId"]="deployment",
                ["Kafdeck:Declarative:DeploymentGeneration"]=Generation.ToString("D"),["Kafdeck:Declarative:RecoveryAnchorFile"]=anchorFile,
                ["Kafdeck:Declarative:RecoveryAnchorSha256"]=Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(anchorFile))).ToLowerInvariant(),
                ["Kafdeck:Declarative:Registrations:0:Id"]="controller",["Kafdeck:Declarative:Registrations:0:OwnerId"]="owner",
                ["Kafdeck:Declarative:Registrations:0:SourceRef"]="source",["Kafdeck:Declarative:Registrations:0:SourceRevision"]=BaselineHash,
                ["Kafdeck:Declarative:Registrations:0:AllowRevisionAdvance"]=advance.ToString(),
                ["Kafdeck:Declarative:Registrations:0:PrincipalId"]=PrincipalId,["Kafdeck:Declarative:Registrations:0:Enabled"]="true",
                ["Kafdeck:Declarative:Registrations:0:ExpiresAtUtc"]=DateTimeOffset.UtcNow.AddHours(1).ToString("O"),
                ["Kafdeck:Declarative:Registrations:0:ResourceIds:0"]="subscription",
            }).Build();
            AuthorizationPolicySnapshot Snapshot() => AuthorizationPolicyCompiler.Compile(new([new("controller",
                (SourceManage ? new[] { new AuthorizationPermissionDefinition(AuthorizationAction.DeclarativeSourceManage, ["deployment"], ["source"]) } : [])
                .Concat([new(AuthorizationAction.DeclarativeRead, ["deployment"], ["subscription"]), new(AuthorizationAction.NotificationRead, null, ["subscription", "new"])])
                .Concat(_fullMutation || _planOnly ? new[] { new AuthorizationPermissionDefinition(AuthorizationAction.DeclarativePlan, ["deployment"], ["subscription"]) } : [])
                .Concat(_fullMutation ? new[] { new AuthorizationPermissionDefinition(AuthorizationAction.DeclarativeApply, ["deployment"], ["subscription"]),
                    new(AuthorizationAction.NotificationManage, null, ["subscription", "new"]) } : []).ToArray())],
                [new("https://idp.example", "subject", ["controller"])], []));
            var evaluator = new AuthorizationPolicyEvaluator(Snapshot(), Snapshot);
            var registry = new NonBrowserReadRegistry(_configuration);
            Authority = new(_configuration, new(new KafdeckOptions(new("http://127.0.0.1:8080", null, AccessMode.Oidc, null), []), evaluator, registry), registry);
        }
        public static async Task<Fixture> CreateAsync(bool advance)
        {
            var fixture = new Fixture(advance);
            var factory = new SqliteNotificationDeliveryDbConnectionFactory(Path.Combine(fixture._directory, "state.db"));
            fixture.Connections = factory;
            fixture.Routing = new(factory); await fixture.Routing.InitializeAsync();
            await W68RuntimeSchema.InstallOrUpgradeAsync(factory, default(CancellationToken));
            await W68RuntimeSchema.AdmitGenerationAsync(factory, "deployment", null, fixture.Generation, new string('a',64));
            fixture.Store = new(factory, fixture.Generation); await fixture.Store.InitializeAsync();
            return fixture;
        }
        public Task<IResult> AdvanceAsync(string manifest, Guid generation, long revision, string hash, string source = "source") =>
            KafdeckDeclarativeSourceEndpoints.AdvanceAsync(source, new(manifest,"json",generation,revision,hash), Context, Store, Routing, Authority, default);
        public void AllowFullMutation()
        {
            _fullMutation = true;
            _configuration["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:1"] = "DeclarativePlan";
            _configuration["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:2"] = "DeclarativeApply";
            _configuration["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:3"] = "NotificationManage";
        }
        public void AllowPlanningOnly()
        {
            _planOnly = true;
            _configuration["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:1"] = "DeclarativePlan";
        }
        public async Task<W68StoredRuntimePlan> CreatePlanAsync(DeclarativeSourceRevisionData source)
        {
            var id = Guid.NewGuid(); var scope = W68RuntimeFingerprints.LeaseScope("subscription");
            var lease = await Store.AcquireLeaseAsync("deployment", scope, "controller", "owner", PrincipalId, id.ToString("N"), 120, _ => Task.FromResult(true));
            Assert.NotNull(lease);
            var desired = Desired("Active"); var now = await Store.ReadDatabaseUtcNowAsync();
            var plan = new W68StoredRuntimePlan(id, W68RuntimePlanCommand.Apply,
                new("deployment", "Deployment", "deployment", "NotificationSubscription", "subscription"), "owner", "source", source.Generation,
                source.SourceHash, PrincipalId, 0, null, null, "missing", null, desired.Definition, desired.State,
                Encoding.UTF8.GetString(desired.ToCanonicalUtf8()), desired.Fingerprint(), Authority.PolicyFingerprint, "controller", scope,
                id.ToString("N"), lease!.Epoch, now.AddSeconds(100), "", DeploymentGeneration: Generation, SourceRegistrationRevision: source.Revision);
            plan = plan with { PlanFingerprint = W68RuntimeFingerprints.Plan(plan) };
            Assert.NotNull(await Store.SavePlanAsync(plan, (_, _) => Task.FromResult(true))); return plan;
        }
        public ValueTask DisposeAsync()
        {
            (_configuration as IDisposable)?.Dispose(); SqliteConnection.ClearAllPools();
            foreach (var file in new[] { "state.db", "state.db-wal", "state.db-shm", "external-anchor.json" })
            { var path = Path.Combine(_directory, file); if(File.Exists(path)) File.Delete(path); }
            Directory.Delete(_directory); return ValueTask.CompletedTask;
        }
    }
}
