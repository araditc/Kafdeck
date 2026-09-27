namespace Kafdeck.Modules.Administration;

public enum FleetGeneratorBatchState
{
    ReservedBeforeDispatch = 1,
    DispatchStarted = 2,
}

public sealed record FleetGeneratorPendingBatch(
    Guid BatchId,
    long RecordIndex,
    int DestinationPartition,
    long RawBytes,
    FleetGeneratorBatchState State,
    DateTimeOffset ReservedAtUtc,
    DateTimeOffset? DispatchStartedAtUtc = null);

public sealed record FleetGeneratorProgressSnapshot(
    string PlanFingerprint,
    long NextRecordIndex,
    long AcknowledgedRecords,
    long AcknowledgedBytes,
    long Version,
    FleetGeneratorPendingBatch? PendingBatch = null);

public sealed class FleetGeneratorProgress
{
    private FleetGeneratorProgress(
        FleetGeneratorProgressSnapshot snapshot)
    {
        Snapshot = Validate(snapshot);
    }

    public FleetGeneratorProgressSnapshot Snapshot { get; private set; }

    public static FleetGeneratorProgress Create(
        string planFingerprint)
    {
        RequireFingerprint(planFingerprint);

        return new FleetGeneratorProgress(
            new FleetGeneratorProgressSnapshot(
                planFingerprint,
                NextRecordIndex: 0,
                AcknowledgedRecords: 0,
                AcknowledgedBytes: 0,
                Version: 0));
    }

    public static FleetGeneratorProgress Restore(
        FleetGeneratorProgressSnapshot snapshot) =>
        new(
            snapshot ??
            throw new ArgumentNullException(nameof(snapshot)));

    public FleetGeneratorPendingBatch ReserveBeforeDispatch(
        long recordIndex,
        int destinationPartition,
        long rawBytes,
        DateTimeOffset nowUtc)
    {
        if (Snapshot.PendingBatch is not null)
        {
            throw new MutationStateException(
                "At most one generator batch may be unresolved.");
        }

        if (recordIndex != Snapshot.NextRecordIndex ||
            recordIndex < 0)
        {
            throw new MutationStateException(
                "Generator record index must equal the durable next index.");
        }

        if (destinationPartition < 0 ||
            rawBytes < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(destinationPartition));
        }

        var pending =
            new FleetGeneratorPendingBatch(
                Guid.NewGuid(),
                recordIndex,
                destinationPartition,
                rawBytes,
                FleetGeneratorBatchState
                    .ReservedBeforeDispatch,
                nowUtc);

        Snapshot = Snapshot with
        {
            PendingBatch = pending,
            Version = checked(Snapshot.Version + 1),
        };

        return pending;
    }

    public void MarkDispatchStarted(
        Guid batchId,
        DateTimeOffset nowUtc)
    {
        var pending =
            RequirePending(
                batchId,
                FleetGeneratorBatchState
                    .ReservedBeforeDispatch);

        Snapshot = Snapshot with
        {
            PendingBatch = pending with
            {
                State =
                    FleetGeneratorBatchState
                        .DispatchStarted,
                DispatchStartedAtUtc = nowUtc,
            },
            Version = checked(Snapshot.Version + 1),
        };
    }

    public void CompleteAcknowledged(
        Guid batchId)
    {
        var pending =
            RequirePending(
                batchId,
                FleetGeneratorBatchState
                    .DispatchStarted);

        Snapshot = Snapshot with
        {
            NextRecordIndex =
                checked(pending.RecordIndex + 1),
            AcknowledgedRecords =
                checked(
                    Snapshot.AcknowledgedRecords + 1),
            AcknowledgedBytes =
                checked(
                    Snapshot.AcknowledgedBytes +
                    pending.RawBytes),
            PendingBatch = null,
            Version = checked(Snapshot.Version + 1),
        };
    }

    public void ResolveProvenNonApplication(
        Guid batchId)
    {
        _ = RequirePending(
            batchId,
            FleetGeneratorBatchState.DispatchStarted);

        Snapshot = Snapshot with
        {
            PendingBatch = null,
            Version = checked(Snapshot.Version + 1),
        };
    }

    public void ReleaseBeforeDispatch(
        Guid batchId)
    {
        _ = RequirePending(
            batchId,
            FleetGeneratorBatchState
                .ReservedBeforeDispatch);

        Snapshot = Snapshot with
        {
            PendingBatch = null,
            Version = checked(Snapshot.Version + 1),
        };
    }

    public static FleetGeneratorProgressSnapshot Validate(
        FleetGeneratorProgressSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        RequireFingerprint(snapshot.PlanFingerprint);

        if (snapshot.NextRecordIndex < 0 ||
            snapshot.AcknowledgedRecords < 0 ||
            snapshot.AcknowledgedBytes < 0 ||
            snapshot.Version < 0)
        {
            throw new MutationStateException(
                "Generator progress counters cannot be negative.");
        }

        if (snapshot.NextRecordIndex !=
            snapshot.AcknowledgedRecords)
        {
            throw new MutationStateException(
                "Generator next index must equal acknowledged record count.");
        }

        if (snapshot.PendingBatch is { } pending)
        {
            if (pending.BatchId == Guid.Empty ||
                pending.RecordIndex !=
                    snapshot.NextRecordIndex ||
                pending.DestinationPartition < 0 ||
                pending.RawBytes < 0 ||
                !Enum.IsDefined(pending.State) ||
                (pending.State ==
                     FleetGeneratorBatchState
                         .DispatchStarted &&
                 pending.DispatchStartedAtUtc is null) ||
                (pending.State ==
                     FleetGeneratorBatchState
                         .ReservedBeforeDispatch &&
                 pending.DispatchStartedAtUtc is not null))
            {
                throw new MutationStateException(
                    "Generator pending-batch evidence is invalid.");
            }
        }

        return snapshot;
    }

    private FleetGeneratorPendingBatch RequirePending(
        Guid batchId,
        FleetGeneratorBatchState expected)
    {
        if (batchId == Guid.Empty ||
            Snapshot.PendingBatch is not { } pending ||
            pending.BatchId != batchId ||
            pending.State != expected)
        {
            throw new MutationStateException(
                "Generator pending batch state does not match the requested transition.");
        }

        return pending;
    }

    private static void RequireFingerprint(
        string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        if (value.Length != 64 ||
            value.Any(character =>
                !char.IsAsciiHexDigit(character)))
        {
            throw new MutationStateException(
                "Generator plan fingerprint must be SHA-256 hex.");
        }
    }
}
