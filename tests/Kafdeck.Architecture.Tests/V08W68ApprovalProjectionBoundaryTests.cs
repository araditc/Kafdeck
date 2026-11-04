using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68ApprovalProjectionBoundaryTests
{
    [Fact]
    public async Task Cross_principal_projection_requires_destination_approval_scope_and_pending_approval_state()
    {
        var denied = await Read(false, MutationOperationState.AwaitingApproval);
        Assert.Equal(403, denied.Status); Assert.Equal(0, denied.FreshnessReads);
        var allowed = await Read(true, MutationOperationState.AwaitingApproval);
        Assert.Equal(200, allowed.Status); Assert.Equal(1, allowed.FreshnessReads);
        var noLongerPending = await Read(true, MutationOperationState.Ready);
        Assert.Equal(403, noLongerPending.Status); Assert.Equal(0, noLongerPending.FreshnessReads);

        static async Task<(int? Status, int FreshnessReads)> Read(bool destinationApproval, MutationOperationState state)
        {
            const string issuer = "https://idp.example";
            var generation = Guid.NewGuid();
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafdeck:Declarative:DeploymentGeneration"] = generation.ToString("D"),
                ["Kafdeck:NonBrowserRead:Enabled"] = "true",
                ["Kafdeck:NonBrowserRead:GovernedApprovalsEnabled"] = "true",
                ["Kafdeck:NonBrowserRead:Issuer"] = issuer, ["Kafdeck:NonBrowserRead:ApiAudience"] = "api",
                ["Kafdeck:NonBrowserRead:Registrations:0:Id"] = "fixture-service",
                ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"] = "owner",
                ["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = "fixture-service",
                ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = "client",
                ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true",
                ["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O"),
                ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"] = "SystemRead",
            }).Build();
            var permissions = new List<AuthorizationPermissionDefinition>
            {
                new(AuthorizationAction.DeclarativeRead, ["deployment"], ["subscription"]),
                new(AuthorizationAction.DeclarativePlan, ["deployment"], ["subscription"]),
                new(AuthorizationAction.DeclarativeAdopt, ["deployment"], ["subscription"]),
                new(AuthorizationAction.NotificationRead, null, ["subscription", "old", "new"]),
                new(AuthorizationAction.MutationApprove, ["deployment"], ["subscription"]),
                new(AuthorizationAction.MutationApprove, null, destinationApproval ? ["subscription", "old", "new"] : ["subscription"]),
            };
            var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
                [new("reviewer", permissions)], [new(issuer, "bob", ["reviewer"])], [])));
            var policy = new KafdeckAuthorizationService(new KafdeckOptions(new("http://127.0.0.1:8080", null, AccessMode.Oidc, null), []),
                evaluator, new(configuration));
            var requestAuthorization = new MutationRequestAuthorizationService(policy);
            var plan = V08W68GovernedApplyBoundaryTests.Plan() with
            { Command = W68RuntimePlanCommand.Adopt, PolicyFingerprint = policy.PolicyFingerprint, DeploymentGeneration = generation };
            plan = plan with { PlanFingerprint = W68RuntimeFingerprints.Plan(plan) };
            var now = DateTimeOffset.UtcNow;
            var operation = MutationOperation.CreatePreview(plan.PlanId, plan.RequesterPrincipalId, DeclarativeSubscriptionMutation.Intent(plan),
                MutationRiskClassifier.Classify(new(MutationOperationKind.DeclarativeNotificationSubscription)),
                "v0.8-bearer:" + policy.PolicyFingerprint, now.AddMinutes(2), now, "projection");
            operation.OpenForConfirmation(now);
            operation.Confirm(plan.RequesterPrincipalId, operation.Snapshot.PreviewHash, now, operation.Snapshot.ConfirmationChallenge);
            var repository = new V08W67CriticalApprovalBoundaryTests.MemoryRepository(operation.Snapshot with { State = state });
            var store = new PlanStore(plan);
            var authorizer = new DeclarativeRuntimeAuthorizer(configuration, policy);
            var service = new DeclarativeEndpointService(store, null!, authorizer, null!, repository, null!, policy,
                null!, new DeclarativeLifecycleGuard(store, authorizer), requestAuthorization);
            using var services = new ServiceCollection().AddSingleton<IConfiguration>(configuration).BuildServiceProvider();
            var context = new DefaultHttpContext
            {
                RequestServices = services,
                // Trusted operator-session boundary fixture; this is not a forged token or an IdP proof.
                User = OidcIdentityNormalizer.Normalize(new(new ClaimsIdentity([new Claim("sub", "bob")], "oidc")), issuer, null, now),
            };
            var response = await service.ReadPlanAsync(plan.PlanId, context, default);
            return (((IStatusCodeHttpResult)response).StatusCode, store.FreshnessReads);
        }
    }

    // This test isolates projection eligibility. Immutable-plan freshness is a
    // separately proven read-only seam; every write method is forbidden here.
    private sealed class PlanStore(W68StoredRuntimePlan plan) : IW68RuntimeStore
    {
        public int FreshnessReads { get; private set; }
        public Task<W68StoredRuntimePlan?> GetPlanAsync(Guid id, CancellationToken token = default) => Task.FromResult(id == plan.PlanId ? plan : null);
        public Task<bool> IsPlanCurrentAsync(Guid id, string hash, Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> authority, CancellationToken token = default)
        { FreshnessReads++; return Task.FromResult(id == plan.PlanId && hash == plan.PlanFingerprint); }
        public Task InitializeAsync(CancellationToken token = default) => throw new NotSupportedException();
        public Task<DateTimeOffset> ReadDatabaseUtcNowAsync(CancellationToken token = default) => throw new NotSupportedException();
        public Task<Guid?> GetDeploymentGenerationAsync(string deployment, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68RuntimeRegistration?> GetRegistrationAsync(W68RuntimeRegistrationKind kind, string deployment, string id, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68RuntimeRegistration?> UpsertRegistrationAsync(W68RuntimeRegistration registration, long revision, Func<CancellationToken, Task<bool>> authority, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68RuntimeOwnership?> GetOwnershipAsync(W68RuntimeResourceKey key, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68StoredRuntimePlan?> LoadPlanAsync(Guid id, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68StoredRuntimePlan?> SavePlanAsync(W68StoredRuntimePlan value, Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> authority, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68RuntimeLease?> AcquireLeaseAsync(string deployment, string scope, string controller, string owner, string principal, string holder, int ttl, Func<CancellationToken, Task<bool>> authority, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68RuntimeLease?> RenewLeaseAsync(W68RuntimeLease lease, string controller, string owner, string principal, int ttl, Func<CancellationToken, Task<bool>> authority, CancellationToken token = default) => throw new NotSupportedException();
        public Task<bool> ReleaseLeaseAsync(W68RuntimeLease lease, string controller, string owner, string principal, Func<CancellationToken, Task<bool>> authority, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68RuntimeCommitReceipt?> GetCommitReceiptAsync(Guid id, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68RuntimeCommitReceipt?> GetVerifiedCommitReceiptAsync(Guid id, string hash, string principal, Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> authority, CancellationToken token = default) => throw new NotSupportedException();
        public Task<W68RuntimeCommitReceipt?> ApplySubscriptionAsync(Guid id, string hash, string principal, Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> authority, CancellationToken token = default) => throw new NotSupportedException();
    }
}
