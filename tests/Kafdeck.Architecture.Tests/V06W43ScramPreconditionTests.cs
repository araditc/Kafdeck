using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W43ScramPreconditionTests
{
    [Fact]
    public async Task Digest_key_rotation_stales_upsert_before_provider_observation()
    {
        var state = new MutableObservation(
            Array.Empty<KafkaScramCredentialMetadata>());
        var (operation, policy) = await CreateUpsertAsync(state);

        var validator = new ScramMutationPreconditionValidator(
            state,
            policy,
            "digest-key-v2");

        var result = await validator.ValidateAsync(operation);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            result.Outcome);
        Assert.Equal(
            "scram_precondition_digest_key_changed",
            result.ResultCode);
        Assert.Equal(1, state.Calls); // planning observation only
    }

    [Fact]
    public async Task Metadata_drift_stales_preview_before_effect()
    {
        var state = new MutableObservation(
            Array.Empty<KafkaScramCredentialMetadata>());
        var (operation, policy) = await CreateUpsertAsync(state);
        state.Values =
        [
            new KafkaScramCredentialMetadata(
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096),
        ];

        var validator = new ScramMutationPreconditionValidator(
            state,
            policy,
            "digest-key-v1");

        var result = await validator.ValidateAsync(operation);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            result.Outcome);
        Assert.Equal(
            "scram_precondition_metadata_changed",
            result.ResultCode);
        Assert.Equal(2, state.Calls);
    }

    [Fact]
    public async Task Unchanged_bound_metadata_is_allowed()
    {
        var state = new MutableObservation(
            Array.Empty<KafkaScramCredentialMetadata>());
        var (operation, policy) = await CreateUpsertAsync(state);

        var validator = new ScramMutationPreconditionValidator(
            state,
            policy,
            "digest-key-v1");

        var result = await validator.ValidateAsync(operation);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.Allowed,
            result.Outcome);
        Assert.Equal(2, state.Calls);
    }

    private static async Task<(
        MutationOperationSnapshot Operation,
        ScramServerPolicy Policy)> CreateUpsertAsync(
        MutableObservation state)
    {
        var policy = new ScramServerPolicy(
            Array.Empty<string>(),
            new[]
            {
                KafkaScramMechanism.ScramSha256,
                KafkaScramMechanism.ScramSha512,
            });
        using var digest =
            new HmacMutationMaterialDigestService(new string('k', 32));
        var operationId = Guid.NewGuid();
        var requester = "oidc:https://idp.example|alice";
        var binding = new ScramPreviewBindingContext(
            operationId,
            requester,
            "policy-v1",
            "digest-key-v1");
        var planner = new ScramMutationPlanner(
            state,
            digest,
            policy);
        var password = Encoding.UTF8.GetBytes(
            "synthetic-w43-precondition-secret");

        try
        {
            var planned = await planner.PlanUpsertAsync(
                binding,
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096,
                password);
            Assert.True(planned.IsSuccess);

            var now = DateTimeOffset.UtcNow;
            return (
                MutationOperation.CreatePreview(
                        operationId,
                        requester,
                        planned.Intent!,
                        planned.Risk!,
                        binding.PolicyVersion,
                        now.AddMinutes(5),
                        now,
                        $"w43-precondition-{Guid.NewGuid():N}")
                    .Snapshot,
                policy);
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(
                password);
        }
    }

    private sealed class MutableObservation : IScramObservationPort
    {
        public MutableObservation(
            IReadOnlyList<KafkaScramCredentialMetadata> values)
        {
            Values = values;
        }

        public IReadOnlyList<KafkaScramCredentialMetadata> Values { get; set; }
        public int Calls { get; private set; }

        public Task<KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>>
            DescribeUserAsync(
                string clusterId,
                string user,
                KafkaOperationContext operation,
                CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>.Success(
                    Values,
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }
    }
}
