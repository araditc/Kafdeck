using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Generator;

public sealed class DataGeneratorRateLimitException :
    InvalidOperationException
{
    public DataGeneratorRateLimitException(string message)
        : base(message)
    {
    }
}

public static class DataGeneratorProgress
{
    public static FleetOperationProgressSnapshot CreateInitial(
        Guid operationId,
        long workerGeneration,
        DataGeneratorPlan plan,
        DateTimeOffset nowUtc)
    {
        DataGeneratorPolicy.ValidatePlan(plan);

        var progress =
            FleetOperationProgress.Create(
                operationId,
                workerGeneration,
                nowUtc);
        var generator =
            FleetGeneratorProgress.Create(
                plan.PlanFingerprint);

        return FleetOperationProgress.Restore(
                progress.Snapshot with
                {
                    Phase = FleetProgressPhase.Observing,
                    Generator = generator.Snapshot,
                })
            .Snapshot;
    }

    public static FleetOperationProgressSnapshot Fence(
        FleetOperationProgressSnapshot snapshot,
        long workerGeneration,
        DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);
        progress.FenceToGeneration(
            workerGeneration,
            nowUtc);
        return progress.Snapshot;
    }

    public static FleetOperationProgressSnapshot ChargeRuntime(
        FleetOperationProgressSnapshot snapshot,
        DataGeneratorPlan plan,
        TimeSpan elapsed,
        DateTimeOffset nowUtc)
    {
        DataGeneratorPolicy.ValidatePlan(plan);
        var progress =
            FleetOperationProgress.Restore(snapshot);
        RequirePlan(progress, plan);

        if (progress.Snapshot.ActiveObservationElapsed >=
            plan.Budget.MaxDuration)
        {
            throw new MutationStateException(
                "Generator duration budget is exhausted.");
        }

        var remaining =
            plan.Budget.MaxDuration -
            progress.Snapshot.ActiveObservationElapsed;
        var requested =
            elapsed <= remaining
                ? elapsed
                : remaining;

        _ = progress.ChargeActiveObservation(
            requested,
            nowUtc);

        if (elapsed > remaining)
        {
            throw new MutationStateException(
                "Generator duration budget would be exceeded.");
        }

        return progress.Snapshot;
    }

    public static FleetOperationProgressSnapshot ReserveBeforeDispatch(
        FleetOperationProgressSnapshot snapshot,
        DataGeneratorPlan plan,
        long recordIndex,
        long rawBytes,
        DateTimeOffset nowUtc)
    {
        DataGeneratorPolicy.ValidatePlan(plan);
        var progress =
            FleetOperationProgress.Restore(snapshot);
        RequirePlan(progress, plan);

        if (progress.Snapshot.WorkerGeneration <= 0)
        {
            throw new MutationStateException(
                "Generator worker generation is invalid.");
        }

        var generator =
            FleetGeneratorProgress.Restore(
                progress.Snapshot.Generator ??
                throw new MutationStateException(
                    "Generator progress is unavailable."));

        if (recordIndex !=
                generator.Snapshot.NextRecordIndex ||
            recordIndex < 0 ||
            recordIndex >= plan.RecordCount)
        {
            throw new MutationStateException(
                "Generator record index is outside the immutable plan.");
        }

        if (rawBytes < 0 ||
            rawBytes > plan.Budget.MaxBatchBytes)
        {
            throw new MutationStateException(
                "Generator record byte budget would be exceeded.");
        }

        if (generator.Snapshot.AcknowledgedRecords >=
                plan.RecordCount ||
            generator.Snapshot.AcknowledgedRecords >=
                plan.Budget.MaxTotalRecords)
        {
            throw new MutationStateException(
                "Generator record budget is exhausted.");
        }

        if (checked(
                generator.Snapshot.AcknowledgedBytes +
                rawBytes) >
            plan.Budget.MaxTotalBytes)
        {
            throw new MutationStateException(
                "Generator byte budget would be exceeded.");
        }

        EnsureRateBudget(
            progress.Snapshot,
            generator.Snapshot,
            plan,
            rawBytes);

        _ = generator.ReserveBeforeDispatch(
            recordIndex,
            plan.Destination.Partition,
            rawBytes,
            nowUtc);

        return Next(
            progress.Snapshot,
            generator.Snapshot,
            FleetProgressPhase.Submitting,
            nowUtc);
    }

    private static void EnsureRateBudget(
        FleetOperationProgressSnapshot progress,
        FleetGeneratorProgressSnapshot generator,
        DataGeneratorPlan plan,
        long nextRecordBytes)
    {
        var elapsedTicks =
            Math.Max(
                TimeSpan.TicksPerSecond,
                progress.ActiveObservationElapsedTicks);
        var elapsedSeconds =
            (decimal)elapsedTicks /
            TimeSpan.TicksPerSecond;

        var recordAllowance =
            decimal.Floor(
                plan.Budget.MaxRecordsPerSecond *
                elapsedSeconds);
        var projectedRecords =
            checked(
                generator.AcknowledgedRecords + 1);

        if (projectedRecords > recordAllowance)
        {
            throw new DataGeneratorRateLimitException(
                "Generator record rate budget would be exceeded.");
        }

        var byteAllowance =
            decimal.Floor(
                plan.Budget.MaxBytesPerSecond *
                elapsedSeconds);
        var projectedBytes =
            checked(
                generator.AcknowledgedBytes +
                nextRecordBytes);

        if (projectedBytes > byteAllowance)
        {
            throw new DataGeneratorRateLimitException(
                "Generator byte rate budget would be exceeded.");
        }
    }

    public static FleetOperationProgressSnapshot MarkDispatchStarted(
        FleetOperationProgressSnapshot snapshot,
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);
        var generator =
            FleetGeneratorProgress.Restore(
                progress.Snapshot.Generator ??
                throw new MutationStateException(
                    "Generator progress is unavailable."));

        generator.MarkDispatchStarted(
            batchId,
            nowUtc);

        return Next(
            progress.Snapshot,
            generator.Snapshot,
            FleetProgressPhase.Submitting,
            nowUtc);
    }

    public static FleetOperationProgressSnapshot CompleteAcknowledged(
        FleetOperationProgressSnapshot snapshot,
        DataGeneratorPlan plan,
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        DataGeneratorPolicy.ValidatePlan(plan);
        var progress =
            FleetOperationProgress.Restore(snapshot);
        RequirePlan(progress, plan);

        var generator =
            FleetGeneratorProgress.Restore(
                progress.Snapshot.Generator ??
                throw new MutationStateException(
                    "Generator progress is unavailable."));

        generator.CompleteAcknowledged(batchId);

        if (generator.Snapshot.AcknowledgedRecords >
                plan.RecordCount ||
            generator.Snapshot.AcknowledgedRecords >
                plan.Budget.MaxTotalRecords ||
            generator.Snapshot.AcknowledgedBytes >
                plan.Budget.MaxTotalBytes)
        {
            throw new MutationStateException(
                "Generator acknowledgement exceeded the immutable budget.");
        }

        var completed =
            generator.Snapshot.AcknowledgedRecords >=
            plan.RecordCount;

        return Next(
            progress.Snapshot,
            generator.Snapshot,
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
        var progress =
            FleetOperationProgress.Restore(snapshot);
        var generator =
            FleetGeneratorProgress.Restore(
                progress.Snapshot.Generator ??
                throw new MutationStateException(
                    "Generator progress is unavailable."));

        generator.ReleaseBeforeDispatch(batchId);

        return Next(
            progress.Snapshot,
            generator.Snapshot,
            FleetProgressPhase.Observing,
            nowUtc);
    }

    public static FleetOperationProgressSnapshot ResolveProvenNonApplication(
        FleetOperationProgressSnapshot snapshot,
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);
        var generator =
            FleetGeneratorProgress.Restore(
                progress.Snapshot.Generator ??
                throw new MutationStateException(
                    "Generator progress is unavailable."));

        generator.ResolveProvenNonApplication(batchId);

        return Next(
            progress.Snapshot,
            generator.Snapshot,
            FleetProgressPhase.Observing,
            nowUtc);
    }

    public static FleetOperationProgressSnapshot MarkAmbiguous(
        FleetOperationProgressSnapshot snapshot,
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);
        var generator =
            FleetGeneratorProgress.Restore(
                progress.Snapshot.Generator ??
                throw new MutationStateException(
                    "Generator progress is unavailable."));

        var pending =
            generator.Snapshot.PendingBatch;
        if (pending is null ||
            pending.BatchId != batchId ||
            pending.State !=
                FleetGeneratorBatchState
                    .DispatchStarted)
        {
            throw new MutationStateException(
                "Only a dispatched unresolved generator batch can become ambiguous.");
        }

        return FleetOperationProgress.Restore(
                progress.Snapshot with
                {
                    Phase =
                        FleetProgressPhase
                            .WaitingForExternalAction,
                    Version =
                        checked(
                            progress.Snapshot.Version +
                            1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;
    }

    public static FleetOperationProgressSnapshot MarkPausedAuthorization(
        FleetOperationProgressSnapshot snapshot,
        DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);

        if (progress.Snapshot.Generator?
                .PendingBatch is not null)
        {
            throw new MutationStateException(
                "Generator with an unresolved batch cannot pause authorization.");
        }

        return FleetOperationProgress.Restore(
                progress.Snapshot with
                {
                    Phase =
                        FleetProgressPhase
                            .PausedAuthorization,
                    Version =
                        checked(
                            progress.Snapshot.Version +
                            1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;
    }

    public static FleetOperationProgressSnapshot MarkObserving(
        FleetOperationProgressSnapshot snapshot,
        DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);

        if (progress.Snapshot.Generator?
                .PendingBatch is not null)
        {
            throw new MutationStateException(
                "Generator with an unresolved batch cannot resume observation.");
        }

        if (progress.Snapshot.Phase is not (
                FleetProgressPhase.PausedAuthorization or
                FleetProgressPhase.Observing))
        {
            throw new MutationStateException(
                "Generator phase is not eligible to resume observation.");
        }

        return FleetOperationProgress.Restore(
                progress.Snapshot with
                {
                    Phase =
                        FleetProgressPhase.Observing,
                    Version =
                        checked(
                            progress.Snapshot.Version +
                            1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;
    }

    public static FleetOperationProgressSnapshot CancelAndFence(
        FleetOperationProgressSnapshot snapshot,
        DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);

        if (progress.Snapshot.Generator?
                .PendingBatch is not null)
        {
            throw new MutationStateException(
                "Generator with an unresolved batch requires reconciliation before cancellation.");
        }

        return FleetOperationProgress.Restore(
                progress.Snapshot with
                {
                    WorkerGeneration =
                        checked(
                            progress.Snapshot
                                .WorkerGeneration + 1),
                    WorkerLeaseOwner = null,
                    WorkerLeaseExpiresAtUtc = null,
                    Phase = FleetProgressPhase.Stopped,
                    Version =
                        checked(
                            progress.Snapshot.Version +
                            1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;
    }

    public static FleetOperationProgressSnapshot
        ResolveProvenNonApplicationAndFence(
            FleetOperationProgressSnapshot snapshot,
            Guid batchId,
            DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);
        var generator =
            FleetGeneratorProgress.Restore(
                progress.Snapshot.Generator ??
                throw new MutationStateException(
                    "Generator progress is unavailable."));

        generator.ResolveProvenNonApplication(batchId);

        return FleetOperationProgress.Restore(
                progress.Snapshot with
                {
                    Generator = generator.Snapshot,
                    WorkerGeneration =
                        checked(
                            progress.Snapshot
                                .WorkerGeneration + 1),
                    WorkerLeaseOwner = null,
                    WorkerLeaseExpiresAtUtc = null,
                    Phase = FleetProgressPhase.Observing,
                    Version =
                        checked(
                            progress.Snapshot.Version +
                            1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;
    }

    public static FleetOperationProgressSnapshot MarkStopped(
        FleetOperationProgressSnapshot snapshot,
        DateTimeOffset nowUtc)
    {
        var progress =
            FleetOperationProgress.Restore(snapshot);

        if (progress.Snapshot.Generator?
                .PendingBatch is not null)
        {
            throw new MutationStateException(
                "Generator with an unresolved batch cannot be stopped as a pre-dispatch failure.");
        }

        return FleetOperationProgress.Restore(
                progress.Snapshot with
                {
                    Phase = FleetProgressPhase.Stopped,
                    Version =
                        checked(
                            progress.Snapshot.Version +
                            1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;
    }

    public static void EnsureCanDispatch(
        FleetOperationProgressSnapshot snapshot,
        DataGeneratorPlan plan)
    {
        DataGeneratorPolicy.ValidatePlan(plan);
        var progress =
            FleetOperationProgress.Restore(snapshot);
        RequirePlan(progress, plan);

        if (progress.Snapshot.Phase is
            FleetProgressPhase.Completed or
            FleetProgressPhase.Stopped or
            FleetProgressPhase.Unknown or
            FleetProgressPhase.WaitingForExternalAction)
        {
            throw new MutationStateException(
                "Generator progress is not eligible for a new external dispatch.");
        }

        var generator =
            FleetGeneratorProgress.Restore(
                progress.Snapshot.Generator ??
                throw new MutationStateException(
                    "Generator progress is unavailable."));

        if (generator.Snapshot.PendingBatch is not null)
        {
            throw new MutationStateException(
                "Generator has an unresolved destination-write batch.");
        }

        if (generator.Snapshot.AcknowledgedRecords >=
                plan.RecordCount ||
            generator.Snapshot.AcknowledgedRecords >=
                plan.Budget.MaxTotalRecords)
        {
            throw new MutationStateException(
                "Generator record budget is exhausted.");
        }

        if (generator.Snapshot.AcknowledgedBytes >=
            plan.Budget.MaxTotalBytes)
        {
            throw new MutationStateException(
                "Generator byte budget is exhausted.");
        }

        if (progress.Snapshot.ActiveObservationElapsed >=
            plan.Budget.MaxDuration)
        {
            throw new MutationStateException(
                "Generator duration budget is exhausted.");
        }
    }

    private static FleetOperationProgressSnapshot Next(
        FleetOperationProgressSnapshot current,
        FleetGeneratorProgressSnapshot generator,
        FleetProgressPhase phase,
        DateTimeOffset nowUtc) =>
        FleetOperationProgress.Restore(
                current with
                {
                    Generator = generator,
                    Phase = phase,
                    Version =
                        checked(current.Version + 1),
                    UpdatedAtUtc = nowUtc,
                })
            .Snapshot;

    private static void RequirePlan(
        FleetOperationProgress progress,
        DataGeneratorPlan plan)
    {
        var generator =
            progress.Snapshot.Generator ??
            throw new MutationStateException(
                "Generator progress is unavailable.");

        if (!string.Equals(
                generator.PlanFingerprint,
                plan.PlanFingerprint,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Generator progress is bound to a different immutable plan.");
        }
    }
}
