using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67CriticalApprovalBoundaryTests
{
    private const string Issuer = "https://sso.test/realms/g2";

    [Fact]
    public async Task Critical_flow_requires_typed_confirmation_distinct_live_human_grants_and_rechecks_revoked_approval_before_dispatch()
    {
        var fixture = Fixture();
        var operation = Operation(fixture.Authorization, fixture.Requester);
        var repository = new MemoryRepository(operation.Snapshot);
        var commands = new MutationCommandService(repository, new(fixture.Evaluator), fixture.Authorization);
        var confirmed = await commands.ConfirmAsync(fixture.Requester, operation.Snapshot.OperationId,
            operation.Snapshot.PreviewHash, operation.Snapshot.ConfirmationChallenge);
        Assert.Equal(MutationCommandOutcome.Saved, confirmed.Outcome);
        Assert.Equal(MutationOperationState.AwaitingApproval, repository.Current.State);
        Assert.False(fixture.Authorization.IsApprovalAllowed(fixture.Requester, repository.Current));
        Assert.False(fixture.Authorization.IsApprovalAllowed(fixture.Service, repository.Current));
        Assert.Equal(MutationCommandOutcome.Forbidden, (await commands.ApproveAsync(fixture.Requester,
            operation.Snapshot.OperationId, operation.Snapshot.PreviewHash)).Outcome);
        Assert.Equal(MutationCommandOutcome.Forbidden, (await commands.ApproveAsync(fixture.Service,
            operation.Snapshot.OperationId, operation.Snapshot.PreviewHash)).Outcome);
        var approved = await commands.ApproveAsync(fixture.Approver, operation.Snapshot.OperationId, operation.Snapshot.PreviewHash);
        Assert.Equal(MutationCommandOutcome.Saved, approved.Outcome);
        Assert.Equal(MutationOperationState.Ready, approved.Operation!.State);
        Assert.Equal(((NonBrowserReadIdentity)fixture.Approver.Identity!).AuditId, approved.Operation.ApprovedByPrincipalId);
        Assert.NotNull(approved.Operation.ApprovalAuthorizationEvidenceHash);
        Assert.Equal(KafdeckAuthorizationOutcome.Allowed, fixture.Authorization.AuthorizeForDispatch(fixture.Requester, approved.Operation));
        fixture.Configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:Enabled"] = "false";
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, fixture.Authorization.AuthorizeForDispatch(fixture.Requester, approved.Operation));
        Assert.Equal(MutationOperationState.Ready, repository.Current.State);
    }

    [Fact]
    public void Approval_needs_its_own_switch_registration_grant_and_both_current_scoped_actions()
    {
        var fixture = Fixture();
        var operation = Operation(fixture.Authorization, fixture.Requester).Snapshot;
        Assert.True(fixture.Authorization.IsApprovalAllowed(fixture.Approver, operation));
        fixture.Configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:ApprovalActions:0"] = "TopicCreate";
        Assert.False(fixture.Authorization.IsApprovalAllowed(fixture.Approver, operation));
        fixture.Configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:ApprovalActions:0"] = "TopicDelete";
        fixture.Configuration["Kafdeck:NonBrowserRead:GovernedApprovalsEnabled"] = "false";
        Assert.False(fixture.Authorization.IsApprovalAllowed(fixture.Approver, operation));
        foreach (var (grantApproval, pattern) in new[] { (false, "orders*"), (true, "other*") })
        {
            var denied = Fixture(grantApproval, pattern);
            Assert.False(denied.Authorization.IsApprovalAllowed(denied.Approver, Operation(denied.Authorization, denied.Requester).Snapshot));
        }
    }

    [Fact]
    public void Same_human_is_not_distinct_after_changing_client_or_browser_transport()
    {
        var fixture = Fixture();
        var operation = Operation(fixture.Authorization, fixture.Requester).Snapshot;
        var otherClient = new ClaimsPrincipal(new NonBrowserReadIdentity("other-registration",
            new(Issuer, "alice", "other-client", "api", DateTimeOffset.UtcNow.AddMinutes(2)), NonBrowserPrincipalKind.Human));
        Assert.False(fixture.Authorization.IsApprovalAllowed(otherClient, operation));
        var browser = OidcIdentityNormalizer.Normalize(new(new ClaimsIdentity([new Claim("sub", "alice")], "oidc")),
            Issuer, null, DateTimeOffset.UtcNow);
        Assert.False(fixture.Authorization.IsApprovalAllowed(browser, operation));
        Assert.True(OperatorSessionContextFactory.TryCreate(browser, out var session));
        Assert.True(MutationActor.IsSameHuman(operation.RequesterPrincipalId, session!.Identity.Key));
        Assert.False(KafdeckMutationApprovalEndpoints.IsIndependentCandidate(operation, session.Identity.Key, DateTimeOffset.UtcNow));
        Assert.True(KafdeckMutationApprovalEndpoints.IsIndependentCandidate(operation, new(Issuer, "bob"), DateTimeOffset.UtcNow));
        Assert.False(OperatorSessionContextFactory.TryCreate(fixture.Approver, out _));
    }

    [Fact]
    public void Approval_commands_are_named_human_only_and_require_exact_preview_hash()
    {
        const string id = "00000000-0000-0000-0000-000000000001";
        var preview = Kafdeck.Cli.CliParser.Parse(["--service-profile", "machine.json", "topics", "delete-preview", "cluster", "orders", "--idempotency-key", "key"]);
        Assert.EndsWith("/topics/orders/delete/preview", Kafdeck.Cli.CliRouteBuilder.Build(preview), StringComparison.Ordinal);
        var approve = Kafdeck.Cli.CliParser.Parse(["--oidc-profile", "human.json", "operations", "approve", id, new string('a', 64)]);
        Assert.EndsWith("/approve", Kafdeck.Cli.CliRouteBuilder.Build(approve), StringComparison.Ordinal);
        Assert.Throws<Kafdeck.Cli.CliUsageException>(() => Kafdeck.Cli.CliParser.Parse(["--service-profile", "machine.json", "operations", "approve", id, new string('a', 64)]));
        Assert.Throws<Kafdeck.Cli.CliUsageException>(() => Kafdeck.Cli.CliParser.Parse(["--oidc-profile", "human.json", "operations", "approve", id, "wrong"]));
    }

    internal static MutationOperation Operation(MutationRequestAuthorizationService authorization, ClaimsPrincipal requester)
    {
        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(((NonBrowserReadIdentity)requester.Identity!).AuditId,
            new(MutationOperationKind.TopicDelete, "allowed", "{\"delete\":\"orders\"}", ["topic/orders"],
                AuthorizationTargets: [new(AuthorizationAction.TopicDelete, "allowed", "orders")]),
            MutationRiskClassifier.Classify(new(MutationOperationKind.TopicDelete)), authorization.PolicyVersionFor(requester),
            now.AddMinutes(5), now, "critical-key");
        operation.OpenForConfirmation(now);
        return operation;
    }

    internal static (IConfigurationRoot Configuration, AuthorizationPolicyEvaluator Evaluator, MutationRequestAuthorizationService Authorization,
        ClaimsPrincipal Requester, ClaimsPrincipal Approver, ClaimsPrincipal Service) Fixture(bool grantApproval = true, string approverPattern = "orders*")
    {
        var config = new Dictionary<string, string?>
        {
            ["Kafdeck:NonBrowserRead:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Issuer"] = Issuer,
            ["Kafdeck:NonBrowserRead:ApiAudience"] = "api", ["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"] = "true",
            ["Kafdeck:NonBrowserRead:GovernedApprovalsEnabled"] = "true",
        };
        foreach (var (prefix, id, subject) in new[] { ("HumanRegistrations:0", "requester", "alice"), ("HumanRegistrations:1", "approver", "bob"), ("Registrations:0", "service", "machine") })
        {
            var key = "Kafdeck:NonBrowserRead:" + prefix + ":";
            config[key + "Id"] = id; config[key + "OwnerId"] = "owner"; config[key + "Subject"] = subject;
            config[key + "ClientId"] = "client"; config[key + "Enabled"] = "true";
            config[key + "ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O");
            config[key + "ReadActions:0"] = "SystemRead";
            if (id != "approver") config[key + "MutationActions:0"] = "TopicDelete";
            if (id != "service") config[key + "ApprovalActions:0"] = "TopicDelete";
        }
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(config).Build();
        var approverPermissions = new List<AuthorizationPermissionDefinition> { new(AuthorizationAction.TopicDelete, ["allowed"], [approverPattern]) };
        if (grantApproval) approverPermissions.Add(new(AuthorizationAction.MutationApprove, ["allowed"], [approverPattern]));
        var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
            [new("requester", [new(AuthorizationAction.TopicDelete, ["allowed"], ["orders*"]), new(AuthorizationAction.MutationApprove, ["allowed"], ["orders*"])]), new("approver", approverPermissions)],
            [new(Issuer, "alice", ["requester"]), new(Issuer, "machine", ["requester"]), new(Issuer, "bob", ["approver"])], [])));
        var options = new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:8080", null, AccessMode.Oidc, null), []);
        var authorization = new MutationRequestAuthorizationService(new(options, evaluator, new(configuration)));
        ClaimsPrincipal Principal(string id, string subject, NonBrowserPrincipalKind kind) => new(new NonBrowserReadIdentity(id,
            new(Issuer, subject, "client", "api", DateTimeOffset.UtcNow.AddMinutes(2)), kind));
        return (configuration, evaluator, authorization, Principal("requester", "alice", NonBrowserPrincipalKind.Human),
            Principal("approver", "bob", NonBrowserPrincipalKind.Human), Principal("service", "machine", NonBrowserPrincipalKind.Service));
    }

    internal sealed class MemoryRepository(MutationOperationSnapshot current) : IMutationOperationRepository
    {
        public MutationOperationSnapshot Current { get; private set; } = current;
        public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<MutationOperationSnapshot?> GetAsync(Guid id, CancellationToken cancellationToken = default) => Task.FromResult(id == Current.OperationId ? Current : null);
        public Task<MutationSaveResult> TrySaveAsync(MutationOperationSnapshot operation, long expectedVersion, CancellationToken cancellationToken = default)
        { if (Current.Version != expectedVersion) return Task.FromResult(new MutationSaveResult(MutationSaveOutcome.VersionConflict, Current)); Current = operation; return Task.FromResult(new MutationSaveResult(MutationSaveOutcome.Saved, operation)); }
        public Task<MutationCreateResult> CreateAsync(MutationOperationSnapshot operation, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<MutationOperationSnapshot>> ListByStateAsync(MutationOperationState state, int limit, CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<MutationOperationSnapshot>>([Current]);
        public Task<IReadOnlyList<MutationOperationSnapshot>> ListRecoverableExecutionsAsync(DateTimeOffset nowUtc, bool includeActiveLeases, int limit, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<MutationClusterSlotResult> TryAcquireClusterExecutionSlotAsync(Guid id, long generation, string cluster, int max, DateTimeOffset expires, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<MutationClusterSlotRenewOutcome> TryRenewClusterExecutionSlotAsync(Guid id, long generation, string cluster, DateTimeOffset expires, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<MutationResourceClaimResult> TryAcquireResourceClaimsAsync(Guid id, long generation, IReadOnlyList<string> keys, DateTimeOffset expires, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<MutationLeaseRenewResult> TryRenewExecutionLeaseAsync(MutationOperationSnapshot operation, long version, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task ReleaseClusterExecutionSlotAsync(Guid id, long generation, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task ReleaseResourceClaimsAsync(Guid id, long generation, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
