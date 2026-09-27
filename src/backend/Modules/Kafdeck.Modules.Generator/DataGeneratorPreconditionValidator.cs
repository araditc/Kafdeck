using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Generator;

public sealed class DataGeneratorPreconditionValidator
{
    private readonly DataGeneratorPlanner _planner;

    public DataGeneratorPreconditionValidator(
        DataGeneratorPlanner planner)
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
            MutationOperationKind.DataGenerator)
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .CapabilityUnsupported,
                "data_generator_operation_kind_mismatch");
        }

        DataGeneratorPlan frozen;
        try
        {
            frozen =
                DataGeneratorPolicy.DeserializePlan(
                    operation.CanonicalIntent);
        }
        catch
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .StalePreview,
                "data_generator_canonical_invalid");
        }

        MutationIntentDescriptor expected;
        try
        {
            expected =
                DataGeneratorPolicy.BuildIntent(
                    frozen);
        }
        catch
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .StalePreview,
                "data_generator_binding_invalid");
        }

        if (!BindingsMatch(
                operation,
                expected))
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .StalePreview,
                "data_generator_operation_binding_mismatch");
        }

        var replanning =
            await _planner.PlanAsync(
                    ToPlanningRequest(frozen),
                    cancellationToken)
                .ConfigureAwait(false);

        if (!replanning.IsSuccess ||
            replanning.Plan is null)
        {
            return MapFailure(
                replanning.Failure);
        }

        if (!string.Equals(
                replanning.Plan.PlanFingerprint,
                frozen.PlanFingerprint,
                StringComparison.Ordinal))
        {
            return new(
                MutationPreDispatchGuardOutcome
                    .StalePreview,
                "data_generator_plan_drift");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private static DataGeneratorPlanningRequest
        ToPlanningRequest(
            DataGeneratorPlan plan)
    {
        var source =
            plan.Source.Kind switch
            {
                DataGeneratorSourceKind.Schema =>
                    new DataGeneratorPlanningSourceRequest(
                        DataGeneratorSourceKind.Schema,
                        plan.Source.Schema!.Subject,
                        plan.Source.Schema.Version),

                DataGeneratorSourceKind
                    .BuiltInTemplate =>
                    new DataGeneratorPlanningSourceRequest(
                        DataGeneratorSourceKind
                            .BuiltInTemplate,
                        Template:
                            plan.Source.Template!
                                .Template),

                _ =>
                    throw new MutationStateException(
                        "Generator source kind is unsupported."),
            };

        return new DataGeneratorPlanningRequest(
            plan.Destination.ClusterId,
            plan.Destination.ProfileVersion,
            plan.Destination.TopicName,
            plan.Destination.Partition,
            plan.RecordCount,
            plan.Seed,
            plan.Budget,
            source);
    }

    private static MutationPreDispatchGuardResult
        MapFailure(
            DataGeneratorPlanningFailure? failure) =>
        failure?.Code switch
        {
            DataGeneratorPlanningFailureCode
                .DestinationUnauthorized =>
                new(
                    MutationPreDispatchGuardOutcome
                        .AuthorizationDenied,
                    "data_generator_provider_authorization_denied"),

            DataGeneratorPlanningFailureCode
                .DestinationUnsupported or
            DataGeneratorPlanningFailureCode
                .SourceUnsupported =>
                new(
                    MutationPreDispatchGuardOutcome
                        .CapabilityUnsupported,
                    "data_generator_capability_unsupported"),

            DataGeneratorPlanningFailureCode
                .PolicyDenied =>
                new(
                    MutationPreDispatchGuardOutcome
                        .StalePreview,
                    "data_generator_deployment_policy_denied"),

            _ =>
                new(
                    MutationPreDispatchGuardOutcome
                        .StalePreview,
                    "data_generator_preview_stale"),
        };

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

        return operation.AuthorizationTargets
            .SequenceEqual(expectedAuthorization);
    }
}
