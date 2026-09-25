using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

public sealed class DynamicConfigurationPreconditionValidator
{
    private readonly IDynamicConfigurationPort _provider;
    private readonly DynamicConfigurationRegistry _registry;
    private readonly DynamicConfigurationPlannerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public DynamicConfigurationPreconditionValidator(
        IDynamicConfigurationPort provider,
        DynamicConfigurationRegistry registry,
        DynamicConfigurationPlannerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _policy = policy ?? DynamicConfigurationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        cancellationToken.ThrowIfCancellationRequested();

        DynamicConfigurationPlan plan;
        DynamicConfigurationKeyPolicy keyPolicy;
        try
        {
            if (operation.OperationKind !=
                MutationOperationKind.ClusterConfigAlter)
            {
                return Unsupported(
                    "dynamic_config_precondition_operation_not_supported");
            }

            plan = DynamicConfigurationPolicy.DeserializePlan(
                operation.CanonicalIntent);
            keyPolicy = _registry.Require(
                plan.Compatibility,
                plan.Mutation.Target.Key);

            var expectedIntent =
                DynamicConfigurationPolicy.BuildIntent(
                    plan,
                    keyPolicy);
            var expectedRequirements =
                MutationAuthorizationRequirements.Normalize(
                    expectedIntent.Kind,
                    expectedIntent.ClusterId,
                    expectedIntent.AuthorizationTargets,
                    expectedIntent.ResourceKeys);

            if (!string.Equals(
                    operation.ClusterId,
                    expectedIntent.ClusterId,
                    StringComparison.Ordinal) ||
                operation.ResourceKeys.Count != 1 ||
                !string.Equals(
                    operation.ResourceKeys[0],
                    expectedIntent.ResourceKeys[0],
                    StringComparison.Ordinal) ||
                !operation.AuthorizationTargets.SequenceEqual(
                    expectedRequirements) ||
                operation.Preconditions.Count != 1 ||
                !string.Equals(
                    operation.Preconditions[0].Key,
                    "cluster.config",
                    StringComparison.Ordinal) ||
                !string.Equals(
                    operation.Preconditions[0].Fingerprint,
                    plan.ObservedFingerprint,
                    StringComparison.Ordinal))
            {
                return Stale(
                    "dynamic_config_precondition_binding_changed");
            }

            if (plan.Mutation.Value is not null)
            {
                _ = keyPolicy.NormalizeValue(
                    plan.Mutation.Value);
            }
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                MutationStateException)
        {
            return Stale(
                "dynamic_config_precondition_policy_or_binding_changed");
        }

        var observed = await _provider.DescribeAsync(
                plan.Mutation.Target,
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow().Add(
                        _policy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess || observed.Value is null)
        {
            return Unobservable(observed.Failure);
        }

        DynamicConfigurationObservation normalized;
        try
        {
            normalized =
                DynamicConfigurationPolicy.NormalizeObservation(
                    observed.Value);
        }
        catch (DynamicConfigurationPolicyException)
        {
            return Stale(
                "dynamic_config_precondition_provider_evidence_invalid");
        }

        if (normalized.IsSensitive || normalized.IsReadOnly)
        {
            return Stale(
                "dynamic_config_precondition_capability_changed");
        }

        var fingerprint =
            DynamicConfigurationPolicy.Fingerprint(
                normalized);
        return string.Equals(
                fingerprint,
                plan.ObservedFingerprint,
                StringComparison.Ordinal)
            ? MutationPreDispatchGuardResult.Allowed
            : Stale(
                "dynamic_config_precondition_changed");
    }

    private static MutationPreDispatchGuardResult Stale(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.StalePreview,
            code);

    private static MutationPreDispatchGuardResult Unsupported(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            code);

    private static MutationPreDispatchGuardResult Unobservable(
        KafkaFailure? failure) =>
        Unsupported(
            failure?.Category switch
            {
                KafkaFailureCategory.Unauthorized =>
                    "dynamic_config_precondition_authorization_unavailable",
                KafkaFailureCategory.NotSupported =>
                    "dynamic_config_precondition_capability_unsupported",
                _ =>
                    "dynamic_config_precondition_observation_unavailable",
            });
}
