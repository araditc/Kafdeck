using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W43ScramAdmissionTests
{
    [Fact]
    public async Task Scram_admission_uses_fleet_conjunction_and_keeps_idempotent_retry_stable()
    {
        const string requester = "oidc:https://idp.example|alice";
        const string idempotencyKey = "w43-admission-stable";
        var operationId = MutationIdempotency.DeriveOperationId(
            requester,
            "prod",
            MutationOperationKind.ScramAlter,
            idempotencyKey);

        var observation = new EmptyObservation();
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var planner = new ScramMutationPlanner(
            observation,
            digest,
            new ScramServerPolicy(
                Array.Empty<string>(),
                new[] { KafkaScramMechanism.ScramSha256 }));
        var password = Encoding.UTF8.GetBytes(
            "synthetic-w43-admission-secret");
        var changedPassword = Encoding.UTF8.GetBytes(
            "synthetic-w43-admission-secret-changed");

        try
        {
            var binding = new ScramPreviewBindingContext(
                operationId,
                requester,
                MutationAdmissionService.PolicyVersion,
                "digest-key-v1");
            var firstPlan = await planner.PlanUpsertAsync(
                binding,
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096,
                password);
            var changedPlan = await planner.PlanUpsertAsync(
                binding,
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096,
                changedPassword);
            Assert.True(firstPlan.IsSuccess);
            Assert.True(changedPlan.IsSuccess);

            var resource = Assert.Single(
                firstPlan.Intent!.AuthorizationTargets!,
                target => target.Action == AuthorizationAction.ScramAlter)
                .ResourceName;
            var repository = new IdempotentRepository();
            var service = CreateService(repository, resource);
            var principal = CreateOperatorPrincipal("alice");

            var created = await service.AdmitAsync(
                principal,
                firstPlan.Intent!,
                firstPlan.Risk!,
                idempotencyKey);
            Assert.Equal(
                MutationAdmissionOutcome.Created,
                created.Outcome);
            Assert.NotNull(created.Operation);
            Assert.Equal(operationId, created.Operation!.OperationId);
            Assert.Equal(
                new[]
                {
                    AuthorizationAction.ScramRead,
                    AuthorizationAction.ScramAlter,
                }.OrderBy(value => value).ToArray(),
                created.Operation.AuthorizationTargets
                    .Select(target => target.Action)
                    .OrderBy(value => value)
                    .ToArray());

            var retry = await service.AdmitAsync(
                principal,
                firstPlan.Intent!,
                firstPlan.Risk!,
                idempotencyKey);
            Assert.Equal(
                MutationAdmissionOutcome.ExistingSameIntent,
                retry.Outcome);
            Assert.Equal(operationId, retry.Operation!.OperationId);

            var changed = await service.AdmitAsync(
                principal,
                changedPlan.Intent!,
                changedPlan.Risk!,
                idempotencyKey);
            Assert.Equal(
                MutationAdmissionOutcome.IdempotencyConflict,
                changed.Outcome);
            Assert.Equal(
                "idempotency_key_conflict",
                changed.Code);
            Assert.Equal(operationId, changed.Operation!.OperationId);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(password);
            CryptographicOperations.ZeroMemory(changedPassword);
        }
    }

    private static MutationAdmissionService CreateService(
        IMutationOperationRepository repository,
        string resource)
    {
        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition(
                    "scram-operator",
                    new[]
                    {
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.ScramRead,
                            new[] { "prod" },
                            new[] { resource }),
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.ScramAlter,
                            new[] { "prod" },
                            new[] { resource }),
                    }),
            },
            new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "scram-operator" }),
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

    private sealed class EmptyObservation : IScramObservationPort
    {
        public Task<KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>>
            DescribeUserAsync(
                string clusterId,
                string user,
                KafkaOperationContext operation,
                CancellationToken cancellationToken = default)
        {
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>.Success(
                    Array.Empty<KafkaScramCredentialMetadata>(),
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }
    }

    private sealed class IdempotentRepository :
        IMutationOperationRepository
    {
        private MutationOperationSnapshot? _current;

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<MutationCreateResult> CreateAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
        {
            if (_current is null)
            {
                _current = operation;
                return Task.FromResult(
                    new MutationCreateResult(
                        MutationCreateOutcome.Created,
                        operation));
            }

            if (string.Equals(
                    _current.IdempotencyScope,
                    operation.IdempotencyScope,
                    StringComparison.Ordinal) &&
                string.Equals(
                    _current.IdempotencyKeyHash,
                    operation.IdempotencyKeyHash,
                    StringComparison.Ordinal))
            {
                return Task.FromResult(
                    new MutationCreateResult(
                        string.Equals(
                            _current.CanonicalIntentHash,
                            operation.CanonicalIntentHash,
                            StringComparison.Ordinal)
                            ? MutationCreateOutcome.ExistingSameIntent
                            : MutationCreateOutcome.IdempotencyConflict,
                        _current));
            }

            throw new InvalidOperationException(
                "Test repository supports one idempotency scope.");
        }

        public Task<MutationOperationSnapshot?> GetAsync(
            Guid operationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                _current?.OperationId == operationId
                    ? _current
                    : null);

        public Task<IReadOnlyList<MutationOperationSnapshot>>
            ListByStateAsync(
                MutationOperationState state,
                int limit,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<MutationOperationSnapshot>>
            ListRecoverableExecutionsAsync(
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

        public Task<MutationClusterSlotResult>
            TryAcquireClusterExecutionSlotAsync(
                Guid operationId,
                long executionClaimGeneration,
                string clusterId,
                int maxConcurrentPerCluster,
                DateTimeOffset expiresAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationClusterSlotRenewOutcome>
            TryRenewClusterExecutionSlotAsync(
                Guid operationId,
                long executionClaimGeneration,
                string clusterId,
                DateTimeOffset expiresAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationResourceClaimResult>
            TryAcquireResourceClaimsAsync(
                Guid operationId,
                long executionClaimGeneration,
                IReadOnlyList<string> resourceKeys,
                DateTimeOffset expiresAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationLeaseRenewResult>
            TryRenewExecutionLeaseAsync(
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
