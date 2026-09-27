using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public static class GovernedDataJobProgress
{
    public static FleetOperationProgressSnapshot CreateInitial(
        Guid operationId,
        long workerGeneration,
        GovernedDataJobPlan plan,
        DateTimeOffset nowUtc)
    {
        GovernedDataJobPolicy.ValidatePlan(plan);

        var progress = FleetOperationProgress.Create(
            operationId,
            workerGeneration,
            nowUtc);

        var transfer = FleetTransferProgress.Create(
            plan.PlanFingerprint,
            plan.Ranges
                .Select(range => range.StartInclusive)
                .ToArray());

        return FleetOperationProgress.Restore(
                progress.Snapshot with
                {
                    Phase = FleetProgressPhase.Observing,
                    Transfer = transfer.Snapshot,
                })
            .Snapshot;
    }

    public static FleetOperationProgressSnapshot Fence(
        FleetOperationProgressSnapshot snapshot,
        long workerGeneration,
        DateTimeOffset nowUtc)
    {
        var progress = FleetOperationProgress.Restore(snapshot);
        progress.FenceToGeneration(
            workerGeneration,
            nowUtc);
        return progress.Snapshot;
    }

    public static FleetOperationProgressSnapshot ChargeRuntime(
        FleetOperationProgressSnapshot snapshot,
        GovernedDataJobPlan plan,
        TimeSpan elapsed,
        DateTimeOffset nowUtc)
    {
        GovernedDataJobPolicy.ValidatePlan(plan);
        var progress = FleetOperationProgress.Restore(snapshot);

        if (progress.Snapshot.ActiveObservationElapsed >=
            plan.Budget.MaxDuration)
        {
            throw new MutationStateException(
                "Data-job duration budget is exhausted.");
        }

        var remaining =
            plan.Budget.MaxDuration -
            progress.Snapshot.ActiveObservationElapsed;
        var requested = elapsed <= remaining
            ? elapsed
            : remaining;

        _ = progress.ChargeActiveObservation(
            requested,
            nowUtc);

        if (elapsed > remaining)
        {
            throw new MutationStateException(
                "Data-job duration budget would be exceeded.");
        }

        return progress.Snapshot;
    }

    public static FleetOperationProgressSnapshot ReserveBeforeDispatch(
        FleetOperationProgressSnapshot snapshot,
        GovernedDataJobPlan plan,
        int rangeIndex,
        long sourceOffset,
        int destinationPartition,
        long rawBytes,
        DateTimeOffset nowUtc)
    {
        GovernedDataJobPolicy.ValidatePlan(plan);
        var progress = FleetOperationProgress.Restore(snapshot);
        RequirePlan(progress, plan);

        if (progress.Snapshot.WorkerGeneration <= 0)
        {
            throw new MutationStateException(
                "Data-job worker generation is invalid.");
        }

        var transfer = FleetTransferProgress.Restore(
            progress.Snapshot.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable."));

        if (rawBytes < 0 ||
            rawBytes > plan.Budget.MaxBatchBytes)
        {
            throw new MutationStateException(
                "Data-job batch byte budget would be exceeded.");
        }

        if (1 > plan.Budget.MaxBatchRecords)
        {
            throw new MutationStateException(
                "Data-job batch record budget is unavailable.");
        }

        if (transfer.Snapshot.AcknowledgedRecords >=
            plan.Budget.MaxTotalRecords)
        {
            throw new MutationStateException(
                "Data-job record budget is exhausted.");
        }

        if (checked(
                transfer.Snapshot.AcknowledgedBytes +
                rawBytes) >
            plan.Budget.MaxTotalBytes)
        {
            throw new MutationStateException(
                "Data-job byte budget would be exceeded.");
        }

        _ = transfer.ReserveBeforeDispatch(
            rangeIndex,
            sourceOffset,
            destinationPartition,
            rawBytes,
            nowUtc);

        return Next(
            progress.Snapshot,
            transfer.Snapshot,
            FleetProgressPhase.Submitting,
            nowUtc);
    }

    public static FleetOperationProgressSnapshot MarkDispatchStarted(
        FleetOperationProgressSnapshot snapshot,
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        var progress = FleetOperationProgress.Restore(snapshot);
        var transfer = FleetTransferProgress.Restore(
            progress.Snapshot.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable."));

        transfer.MarkDispatchStarted(
            batchId,
            nowUtc);

        return Next(
            progress.Snapshot,
            transfer.Snapshot,
            FleetProgressPhase.Submitting,
            nowUtc);
    }

    public static FleetOperationProgressSnapshot CompleteAcknowledged(
        FleetOperationProgressSnapshot snapshot,
        GovernedDataJobPlan plan,
        Guid batchId,
        long nextSourceOffset,
        DateTimeOffset nowUtc)
    {
        GovernedDataJobPolicy.ValidatePlan(plan);
        var progress = FleetOperationProgress.Restore(snapshot);
        RequirePlan(progress, plan);

        var transfer = FleetTransferProgress.Restore(
            progress.Snapshot.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable."));

        transfer.CompleteAcknowledged(
            batchId,
            nextSourceOffset);

        if (transfer.Snapshot.AcknowledgedRecords >
                plan.Budget.MaxTotalRecords ||
            transfer.Snapshot.AcknowledgedBytes >
                plan.Budget.MaxTotalBytes)
        {
            throw new MutationStateException(
                "Data-job acknowledgement exceeded the immutable budget.");
        }

        var completed =
            plan.Ranges
                .Select((range, index) =>
                    transfer.Snapshot.Checkpoints[index]
                        .NextSourceOffset >=
                    range.EndExclusive)
                .All(value => value);

        return Next(
            progress.Snapshot,
            transfer.Snapshot,
            completed
                ? FleetProgressPhase.Completed
                : FleetProgressPhase.Observing,
            nowUtc);
    }

    public static FleetOperationProgressSnapshot ReleaseBeforeDispatch(
        FleetOperationProgressSnapshot snapshot,
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        var progress = FleetOperationProgress.Restore(snapshot);
        var transfer = FleetTransferProgress.Restore(
            progress.Snapshot.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable."));

        transfer.ReleaseBeforeDispatch(batchId);

        return Next(
            progress.Snapshot,
            transfer.Snapshot,
            FleetProgressPhase.Observing,
            nowUtc);
    }

    public static FleetOperationProgressSnapshot ResolveProvenNonApplication(
        FleetOperationProgressSnapshot snapshot,
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        var progress = FleetOperationProgress.Restore(snapshot);
        var transfer = FleetTransferProgress.Restore(
            progress.Snapshot.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable."));

        transfer.ResolveProvenNonApplication(batchId);

        return Next(
            progress.Snapshot,
            transfer.Snapshot,
            FleetProgressPhase.Observing,
            nowUtc);
    }

    public static FleetOperationProgressSnapshot MarkAmbiguous(
        FleetOperationProgressSnapshot snapshot,
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        var progress = FleetOperationProgress.Restore(snapshot);
        var transfer = FleetTransferProgress.Restore(
            progress.Snapshot.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable."));

        var pending = transfer.Snapshot.PendingBatch;
        if (pending is null ||
            pending.BatchId != batchId ||
            pending.State !=
                FleetTransferBatchState.DispatchStarted)
        {
            throw new MutationStateException(
                "Only a dispatched unresolved data-job batch can become ambiguous.");
        }

        // Deliberately retain the pending batch. This is the durable no-blind-
        // replay marker. A governed reconciliation must prove application or
        // non-application before any new batch can be reserved.
        return progress.Snapshot with
        {
            Phase = FleetProgressPhase
                .WaitingForExternalAction,
            Version = checked(
                progress.Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };
    }

    public static void EnsureCanDispatch(
        FleetOperationProgressSnapshot snapshot,
        GovernedDataJobPlan plan)
    {
        GovernedDataJobPolicy.ValidatePlan(plan);
        var progress = FleetOperationProgress.Restore(snapshot);
        RequirePlan(progress, plan);

        if (progress.Snapshot.Phase is
            FleetProgressPhase.Completed or
            FleetProgressPhase.Stopped or
            FleetProgressPhase.Unknown or
            FleetProgressPhase
                .WaitingForExternalAction)
        {
            throw new MutationStateException(
                "Data-job progress is not eligible for a new external dispatch.");
        }

        var transfer = FleetTransferProgress.Restore(
            progress.Snapshot.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable."));

        if (transfer.Snapshot.PendingBatch is not null)
        {
            throw new MutationStateException(
                "Data-job has an unresolved destination-write batch.");
        }

        if (transfer.Snapshot.AcknowledgedRecords >=
            plan.Budget.MaxTotalRecords)
        {
            throw new MutationStateException(
                "Data-job record budget is exhausted.");
        }

        if (transfer.Snapshot.AcknowledgedBytes >=
            plan.Budget.MaxTotalBytes)
        {
            throw new MutationStateException(
                "Data-job byte budget is exhausted.");
        }

        if (progress.Snapshot.ActiveObservationElapsed >=
            plan.Budget.MaxDuration)
        {
            throw new MutationStateException(
                "Data-job duration budget is exhausted.");
        }
    }

    private static FleetOperationProgressSnapshot Next(
        FleetOperationProgressSnapshot current,
        FleetTransferProgressSnapshot transfer,
        FleetProgressPhase phase,
        DateTimeOffset nowUtc) =>
        FleetOperationProgress.Restore(
                current with
                {
                    Transfer = transfer,
                    Phase = phase,
                    Version = checked(
                        current.Version + 1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;

    private static void RequirePlan(
        FleetOperationProgress progress,
        GovernedDataJobPlan plan)
    {
        var transfer =
            progress.Snapshot.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable.");

        if (!string.Equals(
                transfer.PlanFingerprint,
                plan.PlanFingerprint,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Data-job progress is bound to a different immutable plan.");
        }

        if (transfer.Checkpoints.Count !=
            plan.Ranges.Count)
        {
            throw new MutationStateException(
                "Data-job checkpoint count does not match the immutable plan.");
        }
    }
}
