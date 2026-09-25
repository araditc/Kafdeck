using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W43ScramPlanningTests
{
    [Fact]
    public async Task Upsert_plan_is_critical_closed_and_contains_no_secret_material()
    {
        var observation = new StubScramObservation(
            new[]
            {
                new KafkaScramCredentialMetadata(
                    "User:alice",
                    KafkaScramMechanism.ScramSha256,
                    4096),
            });
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var planner = new ScramMutationPlanner(
            observation,
            digest,
            Policy());
        var password = Encoding.UTF8.GetBytes(
            "synthetic-w43-planning-secret");

        try
        {
            var result = await planner.PlanUpsertAsync(
                Preview(),
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha512,
                8192,
                password);

            Assert.True(result.IsSuccess);
            Assert.NotNull(result.Plan);
            Assert.Equal(
                ScramMutationMode.Upsert,
                result.Plan!.Mode);
            Assert.Equal(
                MutationRiskClass.Critical,
                result.Risk!.RiskClass);
            Assert.True(result.Risk.RequiresIndependentApproval);
            Assert.Equal(
                MutationConfirmationMode.TypedTarget,
                result.Risk.ConfirmationMode);

            Assert.Equal(
                MutationOperationKind.ScramAlter,
                result.Intent!.Kind);
            Assert.Equal(
                new[]
                {
                    AuthorizationAction.ScramRead,
                    AuthorizationAction.ScramAlter,
                }.OrderBy(value => value).ToArray(),
                result.Intent.AuthorizationTargets!
                    .Select(target => target.Action)
                    .OrderBy(value => value)
                    .ToArray());

            Assert.Single(result.Intent.ResourceKeys);
            var conflict = FleetConflictKeyCodec.Decode(
                result.Intent.ResourceKeys[0]);
            Assert.Equal(
                FleetConflictTargetKind.ScramCredential,
                conflict.Kind);
            Assert.Equal("prod", conflict.PhysicalClusterId);
            Assert.Equal("User:alice", conflict.ResourceId);
            Assert.Equal(
                ((int)KafkaScramMechanism.ScramSha512).ToString(
                    System.Globalization.CultureInfo.InvariantCulture),
                conflict.SubresourceId);

            Assert.DoesNotContain(
                "synthetic-w43-planning-secret",
                result.Intent.CanonicalIntent,
                StringComparison.Ordinal);
            Assert.Single(result.Intent.MaterialDigests!);
            Assert.Equal(
                ScramMutationPlanner.MaterialName,
                result.Intent.MaterialDigests![0].Name);

            var envelope = ScramCredentialMaterialCodec.Encode(
                new ScramCredentialMaterialBindingContext(
                    result.Plan.PreviewBinding.OperationId,
                    result.Plan.PreviewBinding.RequesterPrincipalId,
                    result.Plan.PreviewBinding.PolicyVersion,
                    result.Plan.PreviewBinding.DigestKeyId!,
                    result.Plan.Credential),
                password);
            try
            {
                Assert.Equal(
                    digest.ComputeDigest(envelope),
                    result.Intent.MaterialDigests[0].Digest);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(envelope);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(password);
        }
    }

    [Fact]
    public async Task Delete_plan_requires_observed_exact_mechanism_and_no_material()
    {
        var observation = new StubScramObservation(
            new[]
            {
                new KafkaScramCredentialMetadata(
                    "User:alice",
                    KafkaScramMechanism.ScramSha256,
                    4096),
            });
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var planner = new ScramMutationPlanner(
            observation,
            digest,
            Policy());

        var missing = await planner.PlanDeleteAsync(
            Preview(),
            "prod",
            "User:alice",
            KafkaScramMechanism.ScramSha512);
        Assert.False(missing.IsSuccess);
        Assert.Equal(
            ScramMutationPlanningFailureCode.CredentialNotFound,
            missing.Failure!.Code);

        var result = await planner.PlanDeleteAsync(
            Preview(),
            "prod",
            "User:alice",
            KafkaScramMechanism.ScramSha256);

        Assert.True(result.IsSuccess);
        Assert.Equal(ScramMutationMode.Delete, result.Plan!.Mode);
        Assert.Null(result.Plan.MaterialName);
        Assert.Empty(result.Intent!.MaterialDigests!);
        Assert.Equal(MutationRiskClass.Critical, result.Risk!.RiskClass);
        Assert.True(result.Risk.RequiresIndependentApproval);
    }

    [Fact]
    public async Task Server_policy_blocks_protected_users_and_out_of_policy_iterations()
    {
        var observation = new StubScramObservation(
            Array.Empty<KafkaScramCredentialMetadata>());
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var policy = new ScramServerPolicy(
            new[] { "User:kafdeck-service" },
            new[] { KafkaScramMechanism.ScramSha256 },
            minIterations: 4096,
            maxIterations: 32768);
        var planner = new ScramMutationPlanner(
            observation,
            digest,
            policy);
        var password = Encoding.UTF8.GetBytes("synthetic-secret");

        try
        {
            var protectedResult = await planner.PlanUpsertAsync(
                Preview(),
                "prod",
                "User:kafdeck-service",
                KafkaScramMechanism.ScramSha256,
                4096,
                password);
            Assert.False(protectedResult.IsSuccess);
            Assert.Equal(
                ScramMutationPlanningFailureCode.ProtectedUser,
                protectedResult.Failure!.Code);

            var iterationResult = await planner.PlanUpsertAsync(
                Preview(),
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                2048,
                password);
            Assert.False(iterationResult.IsSuccess);
            Assert.Equal(
                ScramMutationPlanningFailureCode.PolicyDenied,
                iterationResult.Failure!.Code);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(password);
        }
    }

    [Fact]
    public async Task Common_preview_and_finalization_bind_the_same_durable_operation_identity()
    {
        var preview = Preview();
        var requester = preview.RequesterPrincipalId;
        var observation = new StubScramObservation(
            Array.Empty<KafkaScramCredentialMetadata>());
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var planner = new ScramMutationPlanner(
            observation,
            digest,
            Policy());
        var password = Encoding.UTF8.GetBytes(
            "synthetic-w43-finalization-secret");

        try
        {
            var planned = await planner.PlanUpsertAsync(
                preview,
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096,
                password);
            Assert.True(planned.IsSuccess);

            var now = DateTimeOffset.UtcNow;
            var created = MutationOperation.CreatePreview(
                preview.OperationId,
                requester,
                planned.Intent!,
                planned.Risk!,
                preview.PolicyVersion,
                now.AddMinutes(5),
                now,
                $"w43-finalize-{Guid.NewGuid():N}");

            Assert.Equal(preview.OperationId, created.Snapshot.OperationId);
            Assert.Equal(
                MutationRiskClass.Critical,
                created.Snapshot.Risk.RiskClass);
            Assert.True(created.Snapshot.Risk.RequiresIndependentApproval);

            var ready = created.Snapshot with
            {
                State = MutationOperationState.Ready,
                ConfirmedByPrincipalId = requester,
                ConfirmedAtUtc = now,
                ApprovedByPrincipalId = "principal:independent-approver",
                ApprovalAuthorizationEvidenceHash = new string('a', 64),
                ApprovedAtUtc = now,
            };

            using var material =
                ScramMutationExecutionMaterialBuilder.BuildUpsert(
                    ready,
                    password);
            var envelope = material.GetRequired(
                ScramMutationPlanner.MaterialName);
            using var decoded =
                ScramCredentialMaterialCodec.Decode(envelope);

            Assert.Equal(
                ready.OperationId,
                decoded.Context.OperationId);
            Assert.Equal(
                ready.RequesterPrincipalId,
                decoded.Context.RequesterPrincipalId);
            Assert.Equal(
                ready.PolicyVersion,
                decoded.Context.PolicyVersion);
            Assert.Equal(
                preview.DigestKeyId,
                decoded.Context.DigestKeyId);
            Assert.Equal(
                planned.Plan!.Credential,
                decoded.Context.Credential);
            Assert.True(
                decoded.Password.Span.SequenceEqual(password));

            var wrongIdentity = ready with
            {
                OperationId = Guid.NewGuid(),
            };
            Assert.Throws<MutationStateException>(() =>
                ScramMutationExecutionMaterialBuilder.BuildUpsert(
                    wrongIdentity,
                    password));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(password);
        }
    }

    [Fact]
    public void Metadata_fingerprint_is_order_independent_and_iteration_sensitive()
    {
        var first = new KafkaScramCredentialMetadata(
            "User:alice",
            KafkaScramMechanism.ScramSha256,
            4096);
        var second = new KafkaScramCredentialMetadata(
            "User:alice",
            KafkaScramMechanism.ScramSha512,
            8192);

        var baseline = ScramMutationPlanner.FingerprintMetadata(
            "User:alice",
            new[] { first, second });
        var reordered = ScramMutationPlanner.FingerprintMetadata(
            "User:alice",
            new[] { second, first });
        var changed = ScramMutationPlanner.FingerprintMetadata(
            "User:alice",
            new[]
            {
                first,
                second with { Iterations = 16384 },
            });

        Assert.Equal(baseline, reordered);
        Assert.NotEqual(baseline, changed);
    }

    private static ScramPreviewBindingContext Preview() =>
        new(
            Guid.Parse("11111111-2222-3333-4444-555555555555"),
            "principal:owner",
            "policy-v1",
            "digest-key-v1");

    private static ScramServerPolicy Policy() =>
        new(
            Array.Empty<string>(),
            new[]
            {
                KafkaScramMechanism.ScramSha256,
                KafkaScramMechanism.ScramSha512,
            },
            minIterations: 4096,
            maxIterations: 1_000_000);

    private sealed class StubScramObservation : IScramObservationPort
    {
        private readonly IReadOnlyList<KafkaScramCredentialMetadata> _values;

        public StubScramObservation(
            IReadOnlyList<KafkaScramCredentialMetadata> values)
        {
            _values = values;
        }

        public Task<KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>>
            DescribeUserAsync(
                string clusterId,
                string user,
                KafkaOperationContext operation,
                CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>.Success(
                    _values,
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }
    }
}
