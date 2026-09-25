using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Revalidates the immutable W43 SCRAM preview immediately before the common
/// mutation executor may cross the provider dispatch boundary.
/// </summary>
public sealed class ScramMutationPreconditionValidator
{
    private readonly IScramObservationPort _observations;
    private readonly ScramServerPolicy _serverPolicy;
    private readonly string _currentDigestKeyId;
    private readonly ScramMutationPlannerPolicy _plannerPolicy;
    private readonly TimeProvider _timeProvider;

    public ScramMutationPreconditionValidator(
        IScramObservationPort observations,
        ScramServerPolicy serverPolicy,
        string currentDigestKeyId,
        ScramMutationPlannerPolicy? plannerPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ??
            throw new ArgumentNullException(nameof(observations));
        _serverPolicy = serverPolicy ??
            throw new ArgumentNullException(nameof(serverPolicy));
        _currentDigestKeyId = RequireDigestKeyId(currentDigestKeyId);
        _plannerPolicy = plannerPolicy ?? ScramMutationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        cancellationToken.ThrowIfCancellationRequested();

        ScramMutationPlan plan;
        try
        {
            plan = ScramMutationContract.ValidateBoundOperation(operation);
            if (plan.Mode == ScramMutationMode.Upsert &&
                !string.Equals(
                    plan.PreviewBinding.DigestKeyId,
                    _currentDigestKeyId,
                    StringComparison.Ordinal))
            {
                return Stale("scram_precondition_digest_key_changed");
            }

            if (plan.Mode == ScramMutationMode.Upsert)
            {
                _serverPolicy.ValidateMutation(
                    plan.Credential.User,
                    plan.Credential.Mechanism,
                    plan.Credential.Iterations);
            }
            else
            {
                _serverPolicy.ValidateDelete(
                    plan.Credential.User,
                    plan.Credential.Mechanism);
            }
        }
        catch (ScramPolicyException)
        {
            return Stale("scram_precondition_policy_changed");
        }
        catch (Exception exception)
            when (exception is ArgumentException or MutationStateException)
        {
            return Stale("scram_precondition_binding_changed");
        }

        var observed = await _observations.DescribeUserAsync(
                plan.Credential.ClusterId,
                plan.Credential.User,
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow().Add(
                        _plannerPolicy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess || observed.Value is null)
        {
            return Unobservable(observed.Failure);
        }

        string fingerprint;
        try
        {
            fingerprint = ScramMutationPlanner.FingerprintMetadata(
                plan.Credential.User,
                observed.Value);
        }
        catch (ArgumentException)
        {
            return Stale("scram_precondition_provider_metadata_invalid");
        }

        return string.Equals(
                fingerprint,
                plan.ObservedMetadataFingerprint,
                StringComparison.Ordinal)
            ? MutationPreDispatchGuardResult.Allowed
            : Stale("scram_precondition_metadata_changed");
    }

    private static string RequireDigestKeyId(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length > 256 ||
            value.Any(char.IsControl) ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal))
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                "SCRAM digest-key ID is invalid or exceeds the admitted bound.");
        }

        return value;
    }

    private static MutationPreDispatchGuardResult Stale(string code) =>
        new(MutationPreDispatchGuardOutcome.StalePreview, code);

    private static MutationPreDispatchGuardResult Unobservable(
        KafkaFailure? failure) =>
        new(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            failure?.Category switch
            {
                KafkaFailureCategory.Unauthorized =>
                    "scram_precondition_authorization_unavailable",
                KafkaFailureCategory.NotSupported =>
                    "scram_precondition_capability_unsupported",
                _ => "scram_precondition_observation_unavailable",
            });
}
