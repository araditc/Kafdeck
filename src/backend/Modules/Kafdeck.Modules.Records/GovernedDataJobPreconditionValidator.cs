using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed class GovernedDataJobPreconditionValidator
{
    private readonly GovernedDataJobPlanner _planner;

    public GovernedDataJobPreconditionValidator(
        GovernedDataJobPlanner planner)
    {
        _planner =
            planner ??
            throw new ArgumentNullException(
                nameof(planner));
    }

    public async Task<MutationPreDispatchGuardResult>
        ValidateAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (operation.OperationKind !=
            MutationOperationKind.DataJob)
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .CapabilityUnsupported,
                "data_job_operation_kind_mismatch");
        }

        GovernedDataJobPlan frozen;
        try
        {
            frozen =
                GovernedDataJobPolicy.DeserializePlan(
                    operation.CanonicalIntent);
        }
        catch
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .StalePreview,
                "data_job_canonical_invalid");
        }

        var expected =
            GovernedDataJobPolicy.BuildIntent(frozen);
        if (!BindingsMatch(
                operation,
                expected))
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .StalePreview,
                "data_job_operation_binding_mismatch");
        }

        var replanning =
            await _planner.PlanAsync(
                    new GovernedDataJobPlanningRequest(
                        frozen.Kind,
                        frozen.Source.ClusterId,
                        frozen.Source.ProfileVersion,
                        frozen.Destination.ClusterId,
                        frozen.Destination.ProfileVersion,
                        frozen.Ranges
                            .Select(range =>
                                new ClusterTransferMappingRequest(
                                    range.SourceTopic,
                                    range.SourcePartition,
                                    range.DestinationTopic,
                                    range.DestinationPartition,
                                    range.StartInclusive,
                                    range.EndExclusive))
                            .ToArray(),
                        frozen.Budget,
                        frozen.Transform),
                    cancellationToken)
                .ConfigureAwait(false);

        if (!replanning.IsSuccess ||
            replanning.Plan is null)
        {
            return MapFailure(replanning.Failure);
        }

        if (!string.Equals(
                replanning.Plan.PlanFingerprint,
                frozen.PlanFingerprint,
                StringComparison.Ordinal))
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .StalePreview,
                "data_job_plan_drift");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private static MutationPreDispatchGuardResult
        MapFailure(
            GovernedDataJobPlanningFailure? failure)
    {
        if (failure?.TransferFailure ==
            ClusterTransferPlanningFailureCode
                .ProviderUnauthorized)
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .AuthorizationDenied,
                "data_job_provider_authorization_denied");
        }

        if (failure?.TransferFailure ==
                ClusterTransferPlanningFailureCode
                    .ProviderUnsupported ||
            failure?.Code ==
                GovernedDataJobPlanningFailureCode
                    .TransformUnsupported)
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .CapabilityUnsupported,
                "data_job_capability_unsupported");
        }

        return new(
            MutationPreDispatchGuardOutcome.StalePreview,
            "data_job_preview_stale");
    }

    private static bool BindingsMatch(
        MutationOperationSnapshot operation,
        MutationIntentDescriptor expected)
    {
        if (operation.OperationKind !=
                expected.Kind ||
            !string.Equals(
                operation.ClusterId,
                expected.ClusterId,
                StringComparison.Ordinal) ||
            !string.Equals(
                operation.CanonicalIntent,
                expected.CanonicalIntent,
                StringComparison.Ordinal))
        {
            return false;
        }

        if (!operation.ResourceKeys.SequenceEqual(
                expected.ResourceKeys,
                StringComparer.Ordinal))
        {
            return false;
        }

        var expectedPreconditions =
            expected.Preconditions ??
            Array.Empty<MutationPrecondition>();
        if (!operation.Preconditions.SequenceEqual(
                expectedPreconditions))
        {
            return false;
        }

        var expectedAuthorization =
            expected.AuthorizationTargets ??
            Array.Empty<MutationAuthorizationTarget>();
        return operation.AuthorizationTargets.SequenceEqual(
            expectedAuthorization);
    }
}
