using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Generator;

public enum DataGeneratorStateOutcome
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

public sealed record DataGeneratorStateResult(
    DataGeneratorStateOutcome Outcome,
    FleetOperationProgressSnapshot? Progress,
    string Code);

public sealed class DataGeneratorStateCoordinator
{
    private readonly IFleetMutationStateStore _store;

    public DataGeneratorStateCoordinator(
        IFleetMutationStateStore store)
    {
        _store =
            store ??
            throw new ArgumentNullException(
                nameof(store));
    }

    public async Task<DataGeneratorStateResult>
        InitializeAsync(
            Guid operationId,
            long workerGeneration,
            DataGeneratorPlan plan,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default)
    {
        FleetOperationProgressSnapshot initial;
        try
        {
            initial =
                DataGeneratorProgress.CreateInitial(
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
                DataGeneratorStateOutcome.InvalidState,
                null,
                "data_generator_initial_state_invalid");
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
                    "data_generator_progress_created"),

            FleetProgressCreateOutcome.Existing =>
                ExistingForPlan(
                    result.Progress,
                    plan),

            FleetProgressCreateOutcome
                .ParentOperationNotFound =>
                new(
                    DataGeneratorStateOutcome
                        .ParentNotFound,
                    null,
                    "data_generator_parent_not_found"),

            _ => new(
                DataGeneratorStateOutcome
                    .InvalidState,
                result.Progress,
                "data_generator_progress_create_failed"),
        };
    }

    public async Task<DataGeneratorStateResult>
        AcquireLeaseAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorStateOutcome.InvalidState,
                null,
                "data_generator_worker_id_invalid");
        }

        if (leaseTtl < TimeSpan.FromSeconds(5) ||
            leaseTtl > TimeSpan.FromMinutes(5))
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                null,
                "data_generator_lease_ttl_invalid");
        }

        var current =
            await _store.GetProgressAsync(
                    operationId,
                    cancellationToken)
                .ConfigureAwait(false);

        if (current is null)
        {
            return new(
                DataGeneratorStateOutcome.NotFound,
                null,
                "data_generator_progress_not_found");
        }

        if (!BoundToPlan(current, plan))
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                current,
                "data_generator_plan_binding_mismatch");
        }

        if (current.Phase is
            FleetProgressPhase.Completed or
            FleetProgressPhase.Stopped or
            FleetProgressPhase.Unknown or
            FleetProgressPhase.WaitingForExternalAction)
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                current,
                "data_generator_not_lease_eligible");
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
                DataGeneratorStateOutcome.LeaseUnavailable,
                current,
                "data_generator_lease_unavailable");
        }

        var generation =
            leaseActive &&
            string.Equals(
                current.WorkerLeaseOwner,
                normalizedWorker,
                StringComparison.Ordinal)
                ? current.WorkerGeneration
                : checked(current.WorkerGeneration + 1);

        var next =
            FleetOperationProgress.Restore(
                    current with
                    {
                        WorkerGeneration = generation,
                        WorkerLeaseOwner =
                            normalizedWorker,
                        WorkerLeaseExpiresAtUtc =
                            nowUtc.Add(leaseTtl),
                        Version =
                            checked(current.Version + 1),
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
                    "data_generator_lease_acquired"),

            FleetProgressSaveOutcome.VersionConflict =>
                new(
                    DataGeneratorStateOutcome.VersionConflict,
                    save.Progress,
                    "data_generator_progress_version_conflict"),

            FleetProgressSaveOutcome.NotFound =>
                new(
                    DataGeneratorStateOutcome.NotFound,
                    null,
                    "data_generator_progress_not_found"),

            _ =>
                new(
                    DataGeneratorStateOutcome.InvalidState,
                    save.Progress,
                    "data_generator_lease_acquire_failed"),
        };
    }

    public Task<DataGeneratorStateResult>
        RenewLeaseAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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

    public Task<DataGeneratorStateResult>
        ReleaseLeaseAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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

    private async Task<DataGeneratorStateResult>
        UpdateLeaseAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorStateOutcome.InvalidState,
                null,
                "data_generator_worker_id_invalid");
        }

        var normalizedWorker = workerId.Trim();
        if (normalizedWorker.Length > 256 ||
            normalizedWorker.Any(char.IsControl) ||
            workerGeneration <= 0)
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                null,
                "data_generator_worker_identity_invalid");
        }

        if (!release &&
            (leaseTtl < TimeSpan.FromSeconds(5) ||
             leaseTtl > TimeSpan.FromMinutes(5)))
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                null,
                "data_generator_lease_ttl_invalid");
        }

        var current =
            await _store.GetProgressAsync(
                    operationId,
                    cancellationToken)
                .ConfigureAwait(false);

        if (current is null)
        {
            return new(
                DataGeneratorStateOutcome.NotFound,
                null,
                "data_generator_progress_not_found");
        }

        if (!BoundToPlan(current, plan))
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                current,
                "data_generator_plan_binding_mismatch");
        }

        if (current.WorkerGeneration != workerGeneration)
        {
            return new(
                DataGeneratorStateOutcome.StaleWorker,
                current,
                "data_generator_stale_worker");
        }

        if (!string.Equals(
                current.WorkerLeaseOwner,
                normalizedWorker,
                StringComparison.Ordinal) ||
            !current.WorkerLeaseExpiresAtUtc.HasValue)
        {
            return new(
                DataGeneratorStateOutcome.LeaseUnavailable,
                current,
                "data_generator_lease_unavailable");
        }

        if (!release &&
            current.WorkerLeaseExpiresAtUtc.Value <= nowUtc)
        {
            return new(
                DataGeneratorStateOutcome.LeaseUnavailable,
                current,
                "data_generator_lease_expired");
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
        catch (Exception exception)
            when (exception is
                MutationStateException or
                ArgumentException or
                OverflowException)
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                current,
                "data_generator_lease_transition_invalid");
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
                        ? "data_generator_lease_released"
                        : "data_generator_lease_renewed"),

            FleetProgressSaveOutcome.VersionConflict =>
                new(
                    DataGeneratorStateOutcome.VersionConflict,
                    save.Progress,
                    "data_generator_progress_version_conflict"),

            FleetProgressSaveOutcome.NotFound =>
                new(
                    DataGeneratorStateOutcome.NotFound,
                    null,
                    "data_generator_progress_not_found"),

            _ =>
                new(
                    DataGeneratorStateOutcome.InvalidState,
                    save.Progress,
                    "data_generator_lease_update_failed"),
        };
    }

    public async Task<DataGeneratorStateResult> GetAsync(
        Guid operationId,
        DataGeneratorPlan plan,
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
                DataGeneratorStateOutcome.NotFound,
                null,
                "data_generator_progress_not_found");
        }

        return BoundToPlan(snapshot, plan)
            ? Applied(
                snapshot,
                "data_generator_progress_loaded")
            : new(
                DataGeneratorStateOutcome.InvalidState,
                snapshot,
                "data_generator_plan_binding_mismatch");
    }

    public Task<DataGeneratorStateResult> FenceAsync(
        Guid operationId,
        DataGeneratorPlan plan,
        long newWorkerGeneration,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken =
            default) =>
        MutateAsync(
            operationId,
            plan,
            expectedWorkerGeneration: null,
            snapshot =>
                DataGeneratorProgress.Fence(
                    snapshot,
                    newWorkerGeneration,
                    nowUtc),
            "data_generator_worker_fenced",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        PauseAuthorizationAsync(
            Guid operationId,
            DataGeneratorPlan plan,
            long workerGeneration,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                DataGeneratorProgress
                    .MarkPausedAuthorization(
                        snapshot,
                        nowUtc),
            "data_generator_authorization_paused",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        ResumeObservingAsync(
            Guid operationId,
            DataGeneratorPlan plan,
            long workerGeneration,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                DataGeneratorProgress
                    .MarkObserving(
                        snapshot,
                        nowUtc),
            "data_generator_observing_resumed",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        CancelAndFenceAsync(
            Guid operationId,
            DataGeneratorPlan plan,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            expectedWorkerGeneration: null,
            snapshot =>
                DataGeneratorProgress
                    .CancelAndFence(
                        snapshot,
                        nowUtc),
            "data_generator_cancelled",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        ReconcileProvenNonApplicationAsync(
            Guid operationId,
            DataGeneratorPlan plan,
            Guid batchId,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            expectedWorkerGeneration: null,
            snapshot =>
                DataGeneratorProgress
                    .ResolveProvenNonApplicationAndFence(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_generator_non_application_reconciled",
            cancellationToken);

    public Task<DataGeneratorStateResult> StopAsync(
        Guid operationId,
        DataGeneratorPlan plan,
        long workerGeneration,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken =
            default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                DataGeneratorProgress.MarkStopped(
                    snapshot,
                    nowUtc),
            "data_generator_stopped",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        ChargeRuntimeAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorProgress
                    .ChargeRuntime(
                        snapshot,
                        plan,
                        elapsed,
                        nowUtc),
            "data_generator_runtime_charged",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        ReserveBeforeDispatchAsync(
            Guid operationId,
            DataGeneratorPlan plan,
            long workerGeneration,
            long recordIndex,
            long rawBytes,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken =
                default) =>
        MutateAsync(
            operationId,
            plan,
            workerGeneration,
            snapshot =>
                DataGeneratorProgress
                    .ReserveBeforeDispatch(
                        snapshot,
                        plan,
                        recordIndex,
                        rawBytes,
                        nowUtc),
            "data_generator_batch_reserved",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        MarkDispatchStartedAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorProgress
                    .MarkDispatchStarted(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_generator_dispatch_started",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        CompleteAcknowledgedAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorProgress
                    .CompleteAcknowledged(
                        snapshot,
                        plan,
                        batchId,
                        nowUtc),
            "data_generator_batch_acknowledged",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        MarkAmbiguousAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorProgress
                    .MarkAmbiguous(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_generator_dispatch_ambiguous",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        ResolveProvenNonApplicationAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorProgress
                    .ResolveProvenNonApplication(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_generator_non_application_proven",
            cancellationToken);

    public Task<DataGeneratorStateResult>
        ReleaseBeforeDispatchAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorProgress
                    .ReleaseBeforeDispatch(
                        snapshot,
                        batchId,
                        nowUtc),
            "data_generator_batch_released",
            cancellationToken);

    private async Task<DataGeneratorStateResult>
        MutateAsync(
            Guid operationId,
            DataGeneratorPlan plan,
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
                DataGeneratorStateOutcome.NotFound,
                null,
                "data_generator_progress_not_found");
        }

        if (!BoundToPlan(current, plan))
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                current,
                "data_generator_plan_binding_mismatch");
        }

        if (expectedWorkerGeneration.HasValue &&
            current.WorkerGeneration !=
                expectedWorkerGeneration.Value)
        {
            return new(
                DataGeneratorStateOutcome.StaleWorker,
                current,
                "data_generator_stale_worker");
        }

        FleetOperationProgressSnapshot next;
        try
        {
            next = transition(current);
        }
        catch (Exception exception)
            when (exception is
                MutationStateException or
                ArgumentException or
                OverflowException)
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                current,
                "data_generator_transition_invalid");
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
                    DataGeneratorStateOutcome.NotFound,
                    null,
                    "data_generator_progress_not_found"),

            FleetProgressSaveOutcome.VersionConflict =>
                new(
                    DataGeneratorStateOutcome.VersionConflict,
                    save.Progress,
                    "data_generator_progress_version_conflict"),

            _ => new(
                DataGeneratorStateOutcome.InvalidState,
                save.Progress,
                "data_generator_progress_save_failed"),
        };
    }

    private static DataGeneratorStateResult
        ExistingForPlan(
            FleetOperationProgressSnapshot? snapshot,
            DataGeneratorPlan plan)
    {
        if (snapshot is null ||
            !BoundToPlan(snapshot, plan))
        {
            return new(
                DataGeneratorStateOutcome.InvalidState,
                snapshot,
                "data_generator_existing_plan_conflict");
        }

        return new(
            DataGeneratorStateOutcome.Existing,
            snapshot,
            "data_generator_progress_existing");
    }

    private static bool BoundToPlan(
        FleetOperationProgressSnapshot snapshot,
        DataGeneratorPlan plan) =>
        snapshot.Generator is not null &&
        string.Equals(
            snapshot.Generator.PlanFingerprint,
            plan.PlanFingerprint,
            StringComparison.Ordinal);

    private static DataGeneratorStateResult Applied(
        FleetOperationProgressSnapshot? snapshot,
        string code) =>
        new(
            DataGeneratorStateOutcome.Applied,
            snapshot,
            code);
}
