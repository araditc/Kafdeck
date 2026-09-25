using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05MutationAdmissionServiceTests
{
    [Fact]
    public async Task Admission_requires_idempotency_key_before_durable_creation()
    {
        var repository = new AdmissionRepository();
        var service = CreateService(repository, "payments.*");

        var result = await service.AdmitAsync(
            CreateOperatorPrincipal("alice"),
            Intent("payments.events"),
            Risk(),
            idempotencyKey: null);

        Assert.Equal(MutationAdmissionOutcome.InvalidRequest, result.Outcome);
        Assert.Equal("idempotency_key_required", result.Code);
        Assert.Null(repository.Current);
    }

    [Fact]
    public async Task Admission_fails_closed_before_persistence_when_current_target_is_denied()
    {
        var repository = new AdmissionRepository();
        var service = CreateService(repository, "audit.*");

        var result = await service.AdmitAsync(
            CreateOperatorPrincipal("alice"),
            Intent("payments.events"),
            Risk(),
            "idem-denied");

        Assert.Equal(MutationAdmissionOutcome.Forbidden, result.Outcome);
        Assert.Null(repository.Current);
    }

    [Fact]
    public async Task Admission_can_preserve_a_server_preallocated_operation_identity()
    {
        var repository = new AdmissionRepository();
        var service = CreateService(repository, "payments.*");
        var operationId = Guid.NewGuid();

        var result = await service.AdmitAsync(
            CreateOperatorPrincipal("alice"),
            operationId,
            Intent("payments.events"),
            Risk(),
            "idem-explicit-operation");

        Assert.Equal(MutationAdmissionOutcome.Created, result.Outcome);
        Assert.NotNull(result.Operation);
        Assert.Equal(operationId, result.Operation!.OperationId);
        Assert.Equal(operationId, repository.Current!.OperationId);
    }

    [Fact]
    public async Task Admission_persists_only_the_governed_preview_and_opens_confirmation()
    {
        var repository = new AdmissionRepository();
        var service = CreateService(repository, "payments.*");

        var result = await service.AdmitAsync(
            CreateOperatorPrincipal("alice"),
            Intent("payments.events"),
            Risk(),
            "idem-created");

        Assert.Equal(MutationAdmissionOutcome.Created, result.Outcome);
        Assert.NotNull(result.Operation);
        Assert.Equal(MutationOperationState.AwaitingConfirmation, result.Operation!.State);
        Assert.Equal("oidc:https://idp.example|alice", result.Operation.RequesterPrincipalId);
        Assert.Equal(MutationAdmissionService.PolicyVersion, result.Operation.PolicyVersion);
        Assert.Single(result.Operation.AuthorizationTargets);
        Assert.Equal("payments.events", result.Operation.AuthorizationTargets[0].ResourceName);
    }

    private static MutationIntentDescriptor Intent(string topic) =>
        new(
            MutationOperationKind.TopicCreate,
            "prod",
            "{\"operation\":\"topic-create\"}",
            new[] { $"topic/{topic}" },
            Preconditions:
                new[] { new MutationPrecondition("topic.absence", "sha256:abc") },
            AuthorizationTargets:
                new[]
                {
                    new MutationAuthorizationTarget(
                        AuthorizationAction.TopicCreate,
                        "prod",
                        topic),
                });

    private static MutationRiskDecision Risk() =>
        MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicCreate));

    private static MutationAdmissionService CreateService(
        IMutationOperationRepository repository,
        string resourcePattern)
    {
        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition(
                    "topic-creator",
                    new[]
                    {
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.TopicCreate,
                            new[] { "prod" },
                            new[] { resourcePattern }),
                    }),
            },
            new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "topic-creator" }),
            },
            Array.Empty<AuthorizationGroupBindingDefinition>());
        var evaluator = new AuthorizationPolicyEvaluator(
            AuthorizationPolicyCompiler.Compile(definition));
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null,
                AccessMode.Oidc,
                null),
            Array.Empty<ClusterProfile>(),
            Administration: new AdministrationOptions(
                new MutationOptions(
                    Enabled: true,
                    Persistence: null,
                    MaterialDigestKey: null,
                    PreviewTtl: TimeSpan.FromMinutes(5),
                    MaxConcurrentPerCluster: 2)));
        var authorization = new MutationRequestAuthorizationService(
            new KafdeckAuthorizationService(options, evaluator));

        return new MutationAdmissionService(
            repository,
            authorization,
            options);
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

    private sealed class AdmissionRepository : IMutationOperationRepository
    {
        public MutationOperationSnapshot? Current { get; private set; }

        public Task InitializeAsync(CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<MutationCreateResult> CreateAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
        {
            Current = operation;
            return Task.FromResult(
                new MutationCreateResult(
                    MutationCreateOutcome.Created,
                    operation));
        }

        public Task<MutationOperationSnapshot?> GetAsync(
            Guid operationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(Current?.OperationId == operationId ? Current : null);

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
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

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
