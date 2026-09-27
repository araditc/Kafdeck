using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public enum GovernedDataJobStateOutcome
{
    Applied = 1,
    Existing = 2,
    NotFound = 3,
    ParentNotFound = 4,
    VersionConflict = 5,
    StaleWorker = 6,
    InvalidState = 7,
}

public sealed record GovernedDataJobStateResult(
    GovernedDataJobStateOutcome Outcome,
    FleetOperationProgressSnapshot? Progress,
    string Code);

public sealed class GovernedDataJobStateCoordinator
{
    private readonly IFleetMutationStateStore _store;

    public GovernedDataJobStateCoordinator(
        IFleetMutationStateStore store)
    {
        _store =
            store ??
            throw new ArgumentNullException(
                nameof(store));
    }

    public async Task<GovernedDataJobStateResult>
        InitializeAsync(
            Guid operationId,
            long workerGeneration,
            GovernedDataJobPlan plan,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default)
    {
        var initial =
            GovernedDataJobProgress.CreateInitial(
                operationId,
                workerGeneration,
                plan,
                nowUtc);

        var result =
            await _store.CreateProgressAsync(
                    initial,
                    cancellationToken)
                .ConfigureAwait(false);

        return result.Outcome switch
        {
            FleetProgressCreateOutcome.Created =>
                Applied(
                    result.Progress,
                    "data_job_progress_created"),

            FleetProgressCreateOutcome.Existing =>
                ExistingForPlan(
                    result.Progress,
                    plan),

            FleetProgressCreateOutcome
                .ParentOperationNotFound =>
                new(
                    GovernedDataJobStateOutcome
                        .ParentNotFound,
                    null,
                    "data_job_parent_not_found"),

            _ => new(
                GovernedDataJobStateOutcome
                    .InvalidState,
                result.Progress,
                "data_job_progress_create_failed"),
        };
    }

    public async Task<GovernedDataJobStateResult>
        GetAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            CancellationToken cancellationToken =
                default)
    {
        var snapshot =
            await _store.GetProgressAsync(
                    operationId,
                    cancellationToken)
                .ConfigureAwait(false);

        if (snapshot is null)
        {
            return new(
                GovernedDataJobStateOutcome.NotFound,
                null,
                "data_job_progress_not_found");
        }

        return BoundToPlan(snapshot, plan)
            ? Applied(
                snapshot,
                "data_job_progress_loaded")
            : new(
                GovernedDataJobStateOutcome
                    .InvalidState,
                snapshot,
                "data_job_plan_binding_mismatch");
    }

    public Task<GovernedDataJobStateResult>
        FenceAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long newWorkerGeneration,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            expectedWorkerGeneration: null,
            snapshot =>
                GovernedDataJobProgress.Fence(
                    snapshot,
                    newWorkerGeneration,
                    nowUtc),
            "data_job_worker_fenced",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        ChargeRuntimeAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            TimeSpan elapsed,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress
                    .ChargeRuntime(
                        snapshot,
                        plan,
                        elapsed,
                        nowUtc),
            "data_job_runtime_charged",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        ReserveBeforeDispatchAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            int rangeIndex,
            long sourceOffset,
            int destinationPartition,
            long rawBytes,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress
                    .ReserveBeforeDispatch(
                        snapshot,
                        plan,
                        rangeIndex,
                        sourceOffset,
                        destinationPartition,
                        rawBytes,
                        nowUtc),
            "data_job_batch_reserved",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        MarkDispatchStartedAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            Guid batchId,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress
                    .MarkDispatchStarted(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_job_dispatch_started",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        CompleteAcknowledgedAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            Guid batchId,
            long nextSourceOffset,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress
                    .CompleteAcknowledged(
                        snapshot,
                        plan,
                        batchId,
                        nextSourceOffset,
                        nowUtc),
            "data_job_batch_acknowledged",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        MarkAmbiguousAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            Guid batchId,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress
                    .MarkAmbiguous(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_job_dispatch_ambiguous",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        ResolveProvenNonApplicationAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            Guid batchId,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress
                    .ResolveProvenNonApplication(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_job_non_application_proven",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        ReleaseBeforeDispatchAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            Guid batchId,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress
                    .ReleaseBeforeDispatch(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_job_batch_released",
            cancellationToken);

    private async Task<GovernedDataJobStateResult>
        MutateAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long? expectedWorkerGeneration,
            Func<
                FleetOperationProgressSnapshot,
                FleetOperationProgressSnapshot>
                transition,
            string successCode,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(transition);

        var current =
            await _store.GetProgressAsync(
                    operationId,
                    cancellationToken)
                .ConfigureAwait(false);

        if (current is null)
        {
            return new(
                GovernedDataJobStateOutcome.NotFound,
                null,
                "data_job_progress_not_found");
        }

        if (!BoundToPlan(current, plan))
        {
            return new(
                GovernedDataJobStateOutcome
                    .InvalidState,
                current,
                "data_job_plan_binding_mismatch");
        }

        if (expectedWorkerGeneration.HasValue &&
            current.WorkerGeneration !=
                expectedWorkerGeneration.Value)
        {
            return new(
                GovernedDataJobStateOutcome.StaleWorker,
                current,
                "data_job_stale_worker");
        }

        FleetOperationProgressSnapshot next;
        try
        {
            next = transition(current);
        }
        catch (
            Exception exception)
            when (exception is
                MutationStateException or
                ArgumentException or
                OverflowException)
        {
            return new(
                GovernedDataJobStateOutcome
                    .InvalidState,
                current,
                "data_job_transition_invalid");
        }

        var save =
            await _store.TrySaveProgressAsync(
                    next,
                    current.Version,
                    cancellationToken)
                .ConfigureAwait(false);

        return save.Outcome switch
        {
            FleetProgressSaveOutcome.Saved =>
                Applied(
                    save.Progress,
                    successCode),

            FleetProgressSaveOutcome.NotFound =>
                new(
                    GovernedDataJobStateOutcome
                        .NotFound,
                    null,
                    "data_job_progress_not_found"),

            FleetProgressSaveOutcome
                .VersionConflict =>
                new(
                    GovernedDataJobStateOutcome
                        .VersionConflict,
                    save.Progress,
                    "data_job_progress_version_conflict"),

            _ => new(
                GovernedDataJobStateOutcome
                    .InvalidState,
                save.Progress,
                "data_job_progress_save_failed"),
        };
    }

    private static GovernedDataJobStateResult
        ExistingForPlan(
            FleetOperationProgressSnapshot? snapshot,
            GovernedDataJobPlan plan)
    {
        if (snapshot is null ||
            !BoundToPlan(snapshot, plan))
        {
            return new(
                GovernedDataJobStateOutcome
                    .InvalidState,
                snapshot,
                "data_job_existing_plan_conflict");
        }

        return new(
            GovernedDataJobStateOutcome.Existing,
            snapshot,
            "data_job_progress_existing");
    }

    private static bool BoundToPlan(
        FleetOperationProgressSnapshot snapshot,
        GovernedDataJobPlan plan) =>
        snapshot.Transfer is not null &&
        string.Equals(
            snapshot.Transfer.PlanFingerprint,
            plan.PlanFingerprint,
            StringComparison.Ordinal) &&
        snapshot.Transfer.Checkpoints.Count ==
            plan.Ranges.Count;

    private static GovernedDataJobStateResult
        Applied(
            FleetOperationProgressSnapshot? snapshot,
            string code) =>
        new(
            GovernedDataJobStateOutcome.Applied,
            snapshot,
            code);
}
