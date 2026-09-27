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
    LeaseUnavailable = 8,
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
        FleetOperationProgressSnapshot initial;
        try
        {
            initial =
                GovernedDataJobProgress.CreateInitial(
                    operationId,
                    workerGeneration,
                    plan,
                    nowUtc);
        }
        catch (Exception exception)
            when (exception is
                MutationStateException or
                ArgumentException or
                OverflowException)
        {
            return new(
                GovernedDataJobStateOutcome.InvalidState,
                null,
                "data_job_initial_state_invalid");
        }

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
        AcquireLeaseAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            string workerId,
            DateTimeOffset nowUtc,
            TimeSpan leaseTtl,
            CancellationToken cancellationToken =
                default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);
        var normalizedWorker = workerId.Trim();
        if (normalizedWorker.Length > 256 ||
            normalizedWorker.Any(char.IsControl))
        {
            return new(
                GovernedDataJobStateOutcome.InvalidState,
                null,
                "data_job_worker_id_invalid");
        }

        if (leaseTtl < TimeSpan.FromSeconds(5) ||
            leaseTtl > TimeSpan.FromMinutes(5))
        {
            return new(
                GovernedDataJobStateOutcome.InvalidState,
                null,
                "data_job_lease_ttl_invalid");
        }

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
                GovernedDataJobStateOutcome.InvalidState,
                current,
                "data_job_plan_binding_mismatch");
        }

        if (current.Phase is
            FleetProgressPhase.Completed or
            FleetProgressPhase.Stopped or
            FleetProgressPhase.Unknown or
            FleetProgressPhase.WaitingForExternalAction)
        {
            return new(
                GovernedDataJobStateOutcome.InvalidState,
                current,
                "data_job_not_lease_eligible");
        }

        var leaseActive =
            current.WorkerLeaseExpiresAtUtc.HasValue &&
            current.WorkerLeaseExpiresAtUtc.Value > nowUtc;

        if (leaseActive &&
            !string.Equals(
                current.WorkerLeaseOwner,
                normalizedWorker,
                StringComparison.Ordinal))
        {
            return new(
                GovernedDataJobStateOutcome.LeaseUnavailable,
                current,
                "data_job_lease_unavailable");
        }

        var generation =
            leaseActive &&
            string.Equals(
                current.WorkerLeaseOwner,
                normalizedWorker,
                StringComparison.Ordinal)
                ? current.WorkerGeneration
                : checked(current.WorkerGeneration + 1);

        var next = FleetOperationProgress.Restore(
                current with
                {
                    WorkerGeneration = generation,
                    WorkerLeaseOwner = normalizedWorker,
                    WorkerLeaseExpiresAtUtc =
                        nowUtc.Add(leaseTtl),
                    Version = checked(current.Version + 1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;

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
                    "data_job_lease_acquired"),
            FleetProgressSaveOutcome.VersionConflict =>
                new(
                    GovernedDataJobStateOutcome.VersionConflict,
                    save.Progress,
                    "data_job_progress_version_conflict"),
            FleetProgressSaveOutcome.NotFound =>
                new(
                    GovernedDataJobStateOutcome.NotFound,
                    null,
                    "data_job_progress_not_found"),
            _ =>
                new(
                    GovernedDataJobStateOutcome.InvalidState,
                    save.Progress,
                    "data_job_lease_acquire_failed"),
        };
    }

    public Task<GovernedDataJobStateResult>
        RenewLeaseAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            string workerId,
            long workerGeneration,
            DateTimeOffset nowUtc,
            TimeSpan leaseTtl,
            CancellationToken cancellationToken =
                default) =>
        UpdateLeaseAsync(
            operationId,
            plan,
            workerId,
            workerGeneration,
            nowUtc,
            leaseTtl,
            release: false,
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        ReleaseLeaseAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            string workerId,
            long workerGeneration,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        UpdateLeaseAsync(
            operationId,
            plan,
            workerId,
            workerGeneration,
            nowUtc,
            leaseTtl: TimeSpan.Zero,
            release: true,
            cancellationToken);

    private async Task<GovernedDataJobStateResult>
        UpdateLeaseAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            string workerId,
            long workerGeneration,
            DateTimeOffset nowUtc,
            TimeSpan leaseTtl,
            bool release,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(plan);

        if (string.IsNullOrWhiteSpace(workerId))
        {
            return new(
                GovernedDataJobStateOutcome.InvalidState,
                null,
                "data_job_worker_id_invalid");
        }

        var normalizedWorker = workerId.Trim();
        if (normalizedWorker.Length > 256 ||
            normalizedWorker.Any(char.IsControl) ||
            workerGeneration <= 0)
        {
            return new(
                GovernedDataJobStateOutcome.InvalidState,
                null,
                "data_job_worker_identity_invalid");
        }

        if (!release &&
            (leaseTtl < TimeSpan.FromSeconds(5) ||
             leaseTtl > TimeSpan.FromMinutes(5)))
        {
            return new(
                GovernedDataJobStateOutcome.InvalidState,
                null,
                "data_job_lease_ttl_invalid");
        }

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
                GovernedDataJobStateOutcome.InvalidState,
                current,
                "data_job_plan_binding_mismatch");
        }

        if (current.WorkerGeneration != workerGeneration)
        {
            return new(
                GovernedDataJobStateOutcome.StaleWorker,
                current,
                "data_job_stale_worker");
        }

        if (!string.Equals(
                current.WorkerLeaseOwner,
                normalizedWorker,
                StringComparison.Ordinal) ||
            !current.WorkerLeaseExpiresAtUtc.HasValue)
        {
            return new(
                GovernedDataJobStateOutcome.LeaseUnavailable,
                current,
                "data_job_lease_unavailable");
        }

        if (!release &&
            current.WorkerLeaseExpiresAtUtc.Value <= nowUtc)
        {
            return new(
                GovernedDataJobStateOutcome.LeaseUnavailable,
                current,
                "data_job_lease_expired");
        }

        FleetOperationProgressSnapshot next;
        try
        {
            next =
                FleetOperationProgress.Restore(
                        current with
                        {
                            WorkerLeaseOwner =
                                release
                                    ? null
                                    : normalizedWorker,
                            WorkerLeaseExpiresAtUtc =
                                release
                                    ? null
                                    : nowUtc.Add(leaseTtl),
                            Version =
                                checked(current.Version + 1),
                            UpdatedAtUtc = nowUtc,
                        })
                    .Snapshot;
        }
        catch (
            Exception exception)
            when (exception is
                MutationStateException or
                ArgumentException or
                OverflowException)
        {
            return new(
                GovernedDataJobStateOutcome.InvalidState,
                current,
                "data_job_lease_transition_invalid");
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
                    release
                        ? "data_job_lease_released"
                        : "data_job_lease_renewed"),
            FleetProgressSaveOutcome.VersionConflict =>
                new(
                    GovernedDataJobStateOutcome.VersionConflict,
                    save.Progress,
                    "data_job_progress_version_conflict"),
            FleetProgressSaveOutcome.NotFound =>
                new(
                    GovernedDataJobStateOutcome.NotFound,
                    null,
                    "data_job_progress_not_found"),
            _ =>
                new(
                    GovernedDataJobStateOutcome.InvalidState,
                    save.Progress,
                    "data_job_lease_update_failed"),
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
        PauseAuthorizationAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken = default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress.MarkPausedAuthorization(
                    snapshot,
                    nowUtc),
            "data_job_authorization_paused",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        ResumeObservingAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken = default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress.MarkObserving(
                    snapshot,
                    nowUtc),
            "data_job_observing_resumed",
            cancellationToken);

    public Task<GovernedDataJobStateResult>
        StopAsync(
            Guid operationId,
            GovernedDataJobPlan plan,
            long workerGeneration,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                GovernedDataJobProgress.MarkStopped(
                    snapshot,
                    nowUtc),
            "data_job_stopped",
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
