using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed class GovernedDataJobActivationHandler :
    IMutationExecutionHandler
{
    private readonly IFleetMutationStateStore _store;
    private readonly GovernedDataJobStateCoordinator _state;
    private readonly TimeProvider _timeProvider;

    public GovernedDataJobActivationHandler(
        IFleetMutationStateStore store,
        GovernedDataJobStateCoordinator state,
        TimeProvider? timeProvider = null)
    {
        _store =
            store ??
            throw new ArgumentNullException(nameof(store));
        _state =
            state ??
            throw new ArgumentNullException(nameof(state));
        _timeProvider =
            timeProvider ?? TimeProvider.System;
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.DataJob;

    public async Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Operation.OperationKind !=
                MutationOperationKind.DataJob ||
            context.Operation.ExecutionClaimGeneration <= 0)
        {
            return FailedBeforeDispatch(
                "data_job_activation_operation_invalid");
        }

        GovernedDataJobPlan plan;
        try
        {
            plan =
                GovernedDataJobPolicy.DeserializePlan(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return FailedBeforeDispatch(
                "data_job_activation_plan_invalid");
        }

        var now = _timeProvider.GetUtcNow();

        GovernedDataJobStateResult initialized;
        try
        {
            initialized =
                await _state.InitializeAsync(
                        context.Operation.OperationId,
                        context.Operation
                            .ExecutionClaimGeneration,
                        plan,
                        now,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            return FailedBeforeDispatch(
                "data_job_progress_persistence_unavailable");
        }

        if (initialized.Outcome is not (
                GovernedDataJobStateOutcome.Applied or
                GovernedDataJobStateOutcome.Existing))
        {
            return FailedBeforeDispatch(
                initialized.Code);
        }

        IReadOnlyList<FleetConflictObligationSnapshot>
            obligations;
        try
        {
            obligations =
                BuildConflictObligations(
                    context.Operation.OperationId,
                    plan,
                    now);
        }
        catch
        {
            await StopBestEffortAsync(
                    context.Operation,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false);

            return FailedBeforeDispatch(
                "data_job_conflict_contract_invalid");
        }

        FleetConflictObligationBatchCreateResult admitted;
        try
        {
            admitted =
                await _store
                    .CreateConflictObligationsAsync(
                        obligations,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            await StopBestEffortAsync(
                    context.Operation,
                    plan,
                    CancellationToken.None)
                .ConfigureAwait(false);

            return FailedBeforeDispatch(
                "data_job_conflict_persistence_unavailable");
        }

        if (admitted.Outcome is
            FleetConflictObligationBatchCreateOutcome.Created or
            FleetConflictObligationBatchCreateOutcome.ExistingSameEffects)
        {
            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "data_job_activated",
                new Dictionary<string, string>(
                    StringComparer.Ordinal)
                {
                    ["data-job.plan"] =
                        plan.PlanFingerprint,
                    ["data-job.conflict-obligations"] =
                        admitted.Obligations.Count
                            .ToString(
                                System.Globalization
                                    .CultureInfo
                                    .InvariantCulture),
                });
        }

        await StopBestEffortAsync(
                context.Operation,
                plan,
                CancellationToken.None)
            .ConfigureAwait(false);

        return FailedBeforeDispatch(
            admitted.Outcome switch
            {
                FleetConflictObligationBatchCreateOutcome
                    .LegacyResourceClaimConflict =>
                    "data_job_legacy_resource_conflict",

                FleetConflictObligationBatchCreateOutcome
                    .FleetConflictScopeConflict =>
                    "data_job_fleet_conflict",

                FleetConflictObligationBatchCreateOutcome
                    .ExistingDifferentEffect =>
                    "data_job_existing_effect_conflict",

                FleetConflictObligationBatchCreateOutcome
                    .ParentOperationNotFound =>
                    "data_job_parent_operation_missing",

                _ =>
                    "data_job_conflict_admission_failed",
            });
    }

    private static IReadOnlyList<
        FleetConflictObligationSnapshot>
        BuildConflictObligations(
            Guid operationId,
            GovernedDataJobPlan plan,
            DateTimeOffset nowUtc)
    {
        var transferIntent =
            ClusterTransferPolicy.BuildIntent(
                GovernedDataJobPolicy
                    .ToTransferPlan(plan));

        var conflictKeys =
            transferIntent.ResourceKeys
                .Select(key =>
                {
                    _ = FleetConflictKeyCodec.Decode(key);
                    return key;
                })
                .Distinct(StringComparer.Ordinal)
                .OrderBy(
                    key => key,
                    StringComparer.Ordinal)
                .ToArray();

        if (conflictKeys.Length is < 1 or > 100)
        {
            throw new MutationStateException(
                "Data-job conflict set cannot be admitted atomically.");
        }

        return conflictKeys
            .Select((key, index) =>
                FleetConflictObligation.Create(
                        operationId,
                        $"data-job-{index:D3}",
                        key,
                        plan.PlanFingerprint,
                        nowUtc)
                    .Snapshot)
            .ToArray();
    }

    private async Task StopBestEffortAsync(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        CancellationToken cancellationToken)
    {
        try
        {
            _ = await _state.StopAsync(
                    operation.OperationId,
                    plan,
                    operation.ExecutionClaimGeneration,
                    _timeProvider.GetUtcNow(),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            // The parent mutation will report failed-before-dispatch.
            // Persisted progress without admitted conflict obligations
            // cannot cross the W57 effect guard and is therefore inert.
        }
    }

    private static MutationProviderResult
        FailedBeforeDispatch(
            string code) =>
        new(
            MutationExecutionResultKind
                .FailedBeforeDispatch,
            code);
}
