using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed record GovernedDataJobPlanningRequest(
    GovernedDataJobKind Kind,
    string SourceClusterId,
    string SourceProfileVersion,
    string DestinationClusterId,
    string DestinationProfileVersion,
    IReadOnlyList<ClusterTransferMappingRequest> Ranges,
    ClusterTransferBudget Budget,
    GovernedDataTransform Transform);

public enum GovernedDataJobPlanningFailureCode
{
    InvalidInput = 1,
    TransformUnsupported = 2,
    TransferPlanningFailed = 3,
}

public sealed record GovernedDataJobPlanningFailure(
    GovernedDataJobPlanningFailureCode Code,
    string SafeMessage,
    ClusterTransferPlanningFailureCode? TransferFailure = null);

public sealed record GovernedDataJobPlanResult(
    GovernedDataJobPlan? Plan,
    MutationIntentDescriptor? Intent,
    MutationRiskDecision? Risk,
    GovernedDataJobPlanningFailure? Failure)
{
    public bool IsSuccess =>
        Plan is not null &&
        Intent is not null &&
        Risk is not null &&
        Failure is null;

    public static GovernedDataJobPlanResult Success(
        GovernedDataJobPlan plan,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk) =>
        new(plan, intent, risk, null);

    public static GovernedDataJobPlanResult Failed(
        GovernedDataJobPlanningFailureCode code,
        string message,
        ClusterTransferPlanningFailureCode? transferFailure = null) =>
        new(
            null,
            null,
            null,
            new GovernedDataJobPlanningFailure(
                code,
                message,
                transferFailure));
}

public sealed class GovernedDataJobPlanner
{
    private readonly ClusterTransferPlanner _transfer;

    public GovernedDataJobPlanner(
        ClusterTransferPlanner transfer)
    {
        _transfer =
            transfer ??
            throw new ArgumentNullException(
                nameof(transfer));
    }

    public async Task<GovernedDataJobPlanResult> PlanAsync(
        GovernedDataJobPlanningRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (!Enum.IsDefined(request.Kind))
        {
            return GovernedDataJobPlanResult.Failed(
                GovernedDataJobPlanningFailureCode.InvalidInput,
                "Data-job kind is invalid.");
        }

        if (request.Transform is null)
        {
            return GovernedDataJobPlanResult.Failed(
                GovernedDataJobPlanningFailureCode.InvalidInput,
                "Data-job transform is required.");
        }

        if (request.Transform.Kind !=
            GovernedDataTransformKind.BytePreserving)
        {
            return GovernedDataJobPlanResult.Failed(
                GovernedDataJobPlanningFailureCode.TransformUnsupported,
                "Structured reprocess planning remains blocked until the closed transform executor and masking integration are active.");
        }

        ClusterTransferPlanningRequest transferRequest;
        try
        {
            transferRequest =
                new ClusterTransferPlanningRequest(
                    request.SourceClusterId,
                    request.SourceProfileVersion,
                    request.DestinationClusterId,
                    request.DestinationProfileVersion,
                    request.Ranges,
                    request.Budget);
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                OverflowException)
        {
            return GovernedDataJobPlanResult.Failed(
                GovernedDataJobPlanningFailureCode.InvalidInput,
                "Data-job planning request is invalid.");
        }

        var transfer =
            await _transfer.PlanAsync(
                    transferRequest,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!transfer.IsSuccess ||
            transfer.Plan is null)
        {
            return GovernedDataJobPlanResult.Failed(
                GovernedDataJobPlanningFailureCode.TransferPlanningFailed,
                transfer.Failure?.SafeMessage ??
                "Data-job source/destination planning failed.",
                transfer.Failure?.Code);
        }

        try
        {
            var plan =
                GovernedDataJobPolicy.FromTransfer(
                    request.Kind,
                    transfer.Plan,
                    request.Transform);
            var intent =
                GovernedDataJobPolicy.BuildIntent(plan);
            var risk =
                MutationRiskClassifier.EnforceBuiltInFloor(
                    new MutationRiskInput(
                        MutationOperationKind.DataJob,
                        TargetCount: 1),
                    GovernedDataJobPolicy
                        .ClassifyRisk(plan));

            return GovernedDataJobPlanResult.Success(
                plan,
                intent,
                risk);
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                MutationStateException or
                OverflowException)
        {
            return GovernedDataJobPlanResult.Failed(
                GovernedDataJobPlanningFailureCode.InvalidInput,
                "Data-job canonical plan could not be created safely.");
        }
    }
}
