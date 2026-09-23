using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05MutationCommandServiceTests
{
    [Fact]
    public async Task Confirm_rechecks_current_requester_authorization_before_state_transition()
    {
        var operation = CreateAwaitingConfirmation(
            MutationOperationKind.TopicCreate,
            AuthorizationAction.TopicCreate,
            "payments.events");
        var repository = new StubMutationRepository(operation.Snapshot);
        var service = CreateService(
            repository,
            AuthorizationAction.TopicCreate,
            "payments.*",
            "alice");

        var result = await service.ConfirmAsync(
            CreateOperatorPrincipal("alice"),
            operation.Snapshot.OperationId,
            operation.Snapshot.PreviewHash,
            operation.Snapshot.ConfirmationChallenge);

        Assert.Equal(MutationCommandOutcome.Saved, result.Outcome);
        Assert.Equal(MutationOperationState.Ready, result.Operation!.State);
    }

    [Fact]
    public async Task Confirm_fails_closed_when_current_permission_no_longer_covers_target()
    {
        var operation = CreateAwaitingConfirmation(
            MutationOperationKind.TopicCreate,
            AuthorizationAction.TopicCreate,
            "payments.events");
        var repository = new StubMutationRepository(operation.Snapshot);
        var service = CreateService(
            repository,
            AuthorizationAction.TopicCreate,
            "audit.*",
            "alice");

        var result = await service.ConfirmAsync(
            CreateOperatorPrincipal("alice"),
            operation.Snapshot.OperationId,
            operation.Snapshot.PreviewHash,
            operation.Snapshot.ConfirmationChallenge);

        Assert.Equal(MutationCommandOutcome.Forbidden, result.Outcome);
        Assert.Equal(
            MutationOperationState.AwaitingConfirmation,
            repository.Current.State);
    }

    [Fact]
    public async Task Critical_approval_requires_a_distinct_currently_authorized_principal()
    {
        var operation = CreateAwaitingConfirmation(
            MutationOperationKind.RecordsPurge,
            AuthorizationAction.RecordsPurge,
            "payments.events");
        operation.Confirm(
            "oidc:https://idp.example|alice",
            operation.Snapshot.PreviewHash,
            DateTimeOffset.UtcNow,
            operation.Snapshot.ConfirmationChallenge);

        Assert.Equal(
            MutationOperationState.AwaitingApproval,
            operation.Snapshot.State);

        var repository = new StubMutationRepository(operation.Snapshot);
        var service = CreateService(
            repository,
            AuthorizationAction.RecordsPurge,
            "payments.*",
            "alice",
            "bob");

        var requesterApproval = await service.ApproveAsync(
            CreateOperatorPrincipal("alice"),
            operation.Snapshot.OperationId,
            operation.Snapshot.PreviewHash);
        Assert.Equal(MutationCommandOutcome.InvalidState, requesterApproval.Outcome);

        var independentApproval = await service.ApproveAsync(
            CreateOperatorPrincipal("bob"),
            operation.Snapshot.OperationId,
            operation.Snapshot.PreviewHash);

        Assert.Equal(MutationCommandOutcome.Saved, independentApproval.Outcome);
        Assert.Equal(MutationOperationState.Ready, independentApproval.Operation!.State);
        Assert.Equal("oidc:https://idp.example|bob", independentApproval.Operation.ApprovedByPrincipalId);
    }

    private static MutationOperation CreateAwaitingConfirmation(
        MutationOperationKind kind,
        AuthorizationAction action,
        string resource)
    {
        var now = DateTimeOffset.UtcNow;
        var intent = new MutationIntentDescriptor(
            kind,
            "prod",
            "{\"operation\":\"test\"}",
            new[] { $"resource/{resource}" },
            Preconditions:
                new[] { new MutationPrecondition("state", "sha256:abc") },
            AuthorizationTargets:
                new[]
                {
                    new MutationAuthorizationTarget(action, "prod", resource),
                });

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            intent,
            MutationRiskClassifier.Classify(new MutationRiskInput(kind)),
            "v0.5-w39",
            now.AddMinutes(5),
            now,
            $"command-{kind}");
        operation.OpenForConfirmation(now.AddSeconds(1));
        return operation;
    }

    private static MutationCommandService CreateService(
        IMutationOperationRepository repository,
        AuthorizationAction action,
        string resourcePattern,
        params string[] subjects)
    {
        var role = new AuthorizationRoleDefinition(
            "mutation-role",
            new[]
            {
                new AuthorizationPermissionDefinition(
                    action,
                    new[] { "prod" },
                    new[] { resourcePattern }),
            });
        var definition = new AuthorizationPolicyDefinition(
            new[] { role },
            subjects
                .Select(subject => new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    subject,
                    new[] { role.Id }))
                .ToArray(),
            Array.Empty<AuthorizationGroupBindingDefinition>());
        var evaluator = new AuthorizationPolicyEvaluator(
            AuthorizationPolicyCompiler.Compile(definition));
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null,
                AccessMode.Oidc,
                null),
            Array.Empty<ClusterProfile>());
        var requestAuthorization = new MutationRequestAuthorizationService(
            new KafdeckAuthorizationService(options, evaluator));

        return new MutationCommandService(
            repository,
            new MutationApprovalAuthorizer(evaluator),
            requestAuthorization);
    }

    private static ClaimsPrincipal CreateOperatorPrincipal(string subject)
    {
        var external = new ClaimsPrincipal(
            new ClaimsIdentity(
                new[]
                {
                    new Claim("sub", subject),
                    new Claim("name", subject),
                },
                authenticationType: "oidc"));

        return OidcIdentityNormalizer.Normalize(
            external,
            "https://idp.example",
            groupClaim: null,
            DateTimeOffset.UtcNow);
    }

    private sealed class StubMutationRepository : IMutationOperationRepository
    {
        public StubMutationRepository(MutationOperationSnapshot current)
        {
            Current = current;
        }

        public MutationOperationSnapshot Current { get; private set; }

        public Task InitializeAsync(CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<MutationCreateResult> CreateAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationOperationSnapshot?> GetAsync(
            Guid operationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<MutationOperationSnapshot?>(
                operationId == Current.OperationId ? Current : null);

        public Task<IReadOnlyList<MutationOperationSnapshot>> ListByStateAsync(
            MutationOperationState state,
            int limit,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<MutationOperationSnapshot>> ListRecoverableExecutionsAsync(
            DateTimeOffset nowUtc,
            bool includeActiveLeases,
            int limit,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationSaveResult> TrySaveAsync(
            MutationOperationSnapshot operation,
            long expectedVersion,
            CancellationToken cancellationToken = default)
        {
            if (expectedVersion != Current.Version)
            {
                return Task.FromResult(
                    new MutationSaveResult(
                        MutationSaveOutcome.VersionConflict,
                        Current));
            }

            Current = operation;
            return Task.FromResult(
                new MutationSaveResult(
                    MutationSaveOutcome.Saved,
                    Current));
        }

        public Task<MutationClusterSlotResult> TryAcquireClusterExecutionSlotAsync(
            Guid operationId,
            long executionClaimGeneration,
            string clusterId,
            int maxConcurrentPerCluster,
            DateTimeOffset expiresAtUtc,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationClusterSlotRenewOutcome> TryRenewClusterExecutionSlotAsync(
            Guid operationId,
            long executionClaimGeneration,
            string clusterId,
            DateTimeOffset expiresAtUtc,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationResourceClaimResult> TryAcquireResourceClaimsAsync(
            Guid operationId,
            long executionClaimGeneration,
            IReadOnlyList<string> resourceKeys,
            DateTimeOffset expiresAtUtc,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationLeaseRenewResult> TryRenewExecutionLeaseAsync(
            MutationOperationSnapshot operation,
            long expectedVersion,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task ReleaseClusterExecutionSlotAsync(
            Guid operationId,
            long executionClaimGeneration,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task ReleaseResourceClaimsAsync(
            Guid operationId,
            long executionClaimGeneration,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
