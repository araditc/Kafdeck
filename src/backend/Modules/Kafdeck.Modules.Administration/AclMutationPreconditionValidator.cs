using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Re-observes the immutable ACL preview and server-owned ACL policy immediately
/// before the existing mutation executor is allowed to cross the provider
/// dispatch boundary. Requester authorization remains the responsibility of
/// the common pre-dispatch guard and is intentionally not duplicated here.
/// </summary>
public sealed class AclMutationPreconditionValidator
{
    private readonly IAclObservationPort _observations;
    private readonly AclServerPolicy _policy;
    private readonly AclMutationPlannerPolicy _plannerPolicy;
    private readonly TimeProvider _timeProvider;

    public AclMutationPreconditionValidator(
        IAclObservationPort observations,
        AclServerPolicy policy,
        AclMutationPlannerPolicy? plannerPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _plannerPolicy = plannerPolicy ?? AclMutationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        cancellationToken.ThrowIfCancellationRequested();

        if (operation.OperationKind != MutationOperationKind.AclAlter)
        {
            return Unsupported("acl_precondition_operation_not_supported");
        }

        AclMutationPlan plan;
        try
        {
            plan = AclMutationPolicy.DeserializePlan(operation.CanonicalIntent);
            if (!TryValidateImmutableBinding(operation, plan))
            {
                return Stale("acl_precondition_binding_changed");
            }

            if (plan.CreateBindings.Count > 0)
            {
                _ = AclMutationPolicy.ValidateCreates(plan.CreateBindings, _policy);
            }

            if (plan.RemoveBindings.Count > 0)
            {
                _ = AclMutationPolicy.ValidateRemovals(plan.RemoveBindings, _policy);
            }

            var requiredRisk = AclMutationPolicy.ClassifyRisk(
                plan.CreateBindings,
                plan.RemoveBindings);
            if ((int)operation.Risk.RiskClass < (int)requiredRisk.RiskClass ||
                (requiredRisk.RequiresIndependentApproval &&
                 !operation.Risk.RequiresIndependentApproval))
            {
                return Stale("acl_precondition_risk_floor_changed");
            }
        }
        catch (AclPolicyException)
        {
            return Stale("acl_precondition_policy_changed");
        }
        catch (ArgumentException)
        {
            return Stale("acl_precondition_canonical_invalid");
        }
        catch (MutationStateException)
        {
            return Stale("acl_precondition_canonical_invalid");
        }

        var expected = RequiredPrecondition(operation, "acl.binding-set");
        if (expected is null)
        {
            return Stale("acl_precondition_missing");
        }

        KafkaResult<IReadOnlyList<KafkaAclBinding>> observed;
        if (plan.SourceFilter is not null)
        {
            observed = await _observations.DescribeAsync(
                    plan.ClusterId,
                    AclMutationPolicy.NormalizeMutationFilter(plan.SourceFilter),
                    Operation(),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            observed = await ObserveExactCreateTargetsAsync(
                    plan,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        if (!observed.IsSuccess || observed.Value is null)
        {
            return Unobservable(observed.Failure);
        }

        var fingerprint = AclMutationPolicy.FingerprintBindings(observed.Value);
        return string.Equals(
                expected.Fingerprint,
                fingerprint,
                StringComparison.Ordinal)
            ? MutationPreDispatchGuardResult.Allowed
            : Stale("acl_precondition_binding_set_changed");
    }

    private async Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>>
        ObserveExactCreateTargetsAsync(
            AclMutationPlan plan,
            CancellationToken cancellationToken)
    {
        var observed = new HashSet<KafkaAclBinding>();
        ObservationMetadata? metadata = null;
        var operation = Operation();

        foreach (var binding in plan.CreateBindings)
        {
            var result = await _observations.DescribeAsync(
                    plan.ClusterId,
                    AclMutationPolicy.ExactFilter(binding),
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);
            metadata = result.Observation;
            if (!result.IsSuccess || result.Value is null)
            {
                return KafkaResult<IReadOnlyList<KafkaAclBinding>>.Failed(
                    result.Failure!,
                    result.Observation);
            }

            foreach (var item in result.Value)
            {
                observed.Add(item);
            }
        }

        return KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
            Array.AsReadOnly(
                observed
                    .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
                    .ToArray()),
            metadata ?? LiveObservation());
    }

    private static bool TryValidateImmutableBinding(
        MutationOperationSnapshot operation,
        AclMutationPlan plan)
    {
        if (!string.Equals(
                operation.ClusterId,
                plan.ClusterId,
                StringComparison.Ordinal))
        {
            return false;
        }

        MutationIntentDescriptor rebuilt;
        try
        {
            rebuilt = AclMutationPolicy.BuildIntent(plan);
        }
        catch
        {
            return false;
        }

        return operation.ResourceKeys.SequenceEqual(
                   rebuilt.ResourceKeys,
                   StringComparer.Ordinal) &&
               operation.AuthorizationTargets.SequenceEqual(
                   MutationAuthorizationRequirements.Normalize(
                       rebuilt.Kind,
                       rebuilt.ClusterId,
                       rebuilt.AuthorizationTargets,
                       rebuilt.ResourceKeys));
    }

    private KafkaOperationContext Operation() =>
        new(_timeProvider.GetUtcNow().Add(_plannerPolicy.ObservationTimeout));

    private ObservationMetadata LiveObservation()
    {
        var now = _timeProvider.GetUtcNow();
        return new ObservationMetadata(now, now, now, ObservationSource.Live);
    }

    private static MutationPrecondition? RequiredPrecondition(
        MutationOperationSnapshot operation,
        string key)
    {
        var matches = operation.Preconditions
            .Where(item => string.Equals(item.Key, key, StringComparison.Ordinal))
            .Take(2)
            .ToArray();
        return matches.Length == 1 ? matches[0] : null;
    }

    private static MutationPreDispatchGuardResult Stale(string code) =>
        new(MutationPreDispatchGuardOutcome.StalePreview, code);

    private static MutationPreDispatchGuardResult Unsupported(string code) =>
        new(MutationPreDispatchGuardOutcome.CapabilityUnsupported, code);

    private static MutationPreDispatchGuardResult Unobservable(
        KafkaFailure? failure) =>
        new(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            failure?.Category switch
            {
                KafkaFailureCategory.Unauthorized =>
                    "acl_precondition_authorization_unavailable",
                KafkaFailureCategory.NotSupported =>
                    "acl_precondition_capability_unsupported",
                _ => "acl_precondition_observation_unavailable",
            });
}
