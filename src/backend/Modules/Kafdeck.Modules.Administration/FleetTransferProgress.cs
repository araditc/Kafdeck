namespace Kafdeck.Modules.Administration;

public enum FleetTransferBatchState
{
    ReservedBeforeDispatch = 1,
    DispatchStarted = 2,
}

public sealed record FleetTransferPartitionCheckpoint(
    int MappingIndex,
    long NextSourceOffset);

public sealed record FleetTransferPendingBatch(
    Guid BatchId,
    int MappingIndex,
    long SourceOffset,
    int DestinationPartition,
    long RawBytes,
    FleetTransferBatchState State,
    DateTimeOffset ReservedAtUtc,
    DateTimeOffset? DispatchStartedAtUtc = null);

public sealed record FleetTransferProgressSnapshot(
    string PlanFingerprint,
    IReadOnlyList<FleetTransferPartitionCheckpoint> Checkpoints,
    long AcknowledgedRecords,
    long AcknowledgedBytes,
    long Version,
    FleetTransferPendingBatch? PendingBatch = null);

public sealed class FleetTransferProgress
{
    private FleetTransferProgress(FleetTransferProgressSnapshot snapshot)
    {
        Snapshot = Validate(snapshot);
    }

    public FleetTransferProgressSnapshot Snapshot { get; private set; }

    public static FleetTransferProgress Create(
        string planFingerprint,
        IReadOnlyList<long> mappingStartOffsets)
    {
        RequireFingerprint(planFingerprint);
        ArgumentNullException.ThrowIfNull(mappingStartOffsets);
        if (mappingStartOffsets.Count == 0 || mappingStartOffsets.Count > 64)
            throw new MutationStateException("Transfer progress mapping count is outside the admitted bound.");
        if (mappingStartOffsets.Any(offset => offset < 0))
            throw new MutationStateException("Transfer progress offsets must be non-negative.");

        return new FleetTransferProgress(new FleetTransferProgressSnapshot(
            planFingerprint,
            Array.AsReadOnly(
                mappingStartOffsets
                    .Select((offset, index) =>
                        new FleetTransferPartitionCheckpoint(index, offset))
                    .ToArray()),
            0,
            0,
            0));
    }

    public static FleetTransferProgress Restore(FleetTransferProgressSnapshot snapshot) =>
        new(snapshot ?? throw new ArgumentNullException(nameof(snapshot)));

    public FleetTransferPendingBatch ReserveBeforeDispatch(
        int mappingIndex,
        long sourceOffset,
        int destinationPartition,
        long rawBytes,
        DateTimeOffset nowUtc)
    {
        if (Snapshot.PendingBatch is not null)
            throw new MutationStateException("At most one transfer batch may be unresolved.");
        if (mappingIndex < 0 ||
            mappingIndex >= Snapshot.Checkpoints.Count ||
            Snapshot.Checkpoints[mappingIndex].MappingIndex != mappingIndex)
            throw new MutationStateException("Transfer mapping checkpoint is invalid.");
        if (sourceOffset != Snapshot.Checkpoints[mappingIndex].NextSourceOffset)
            throw new MutationStateException("Transfer source offset must equal the durable next checkpoint.");
        if (destinationPartition < 0 || rawBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(destinationPartition));

        var pending = new FleetTransferPendingBatch(
            Guid.NewGuid(),
            mappingIndex,
            sourceOffset,
            destinationPartition,
            rawBytes,
            FleetTransferBatchState.ReservedBeforeDispatch,
            nowUtc);
        Snapshot = Snapshot with
        {
            PendingBatch = pending,
            Version = checked(Snapshot.Version + 1),
        };
        return pending;
    }

    public void MarkDispatchStarted(Guid batchId, DateTimeOffset nowUtc)
    {
        var pending = RequirePending(batchId, FleetTransferBatchState.ReservedBeforeDispatch);
        Snapshot = Snapshot with
        {
            PendingBatch = pending with
            {
                State = FleetTransferBatchState.DispatchStarted,
                DispatchStartedAtUtc = nowUtc,
            },
            Version = checked(Snapshot.Version + 1),
        };
    }

    public void CompleteAcknowledged(
        Guid batchId,
        long nextSourceOffset)
    {
        var pending = RequirePending(batchId, FleetTransferBatchState.DispatchStarted);
        if (nextSourceOffset != checked(pending.SourceOffset + 1))
            throw new MutationStateException("Transfer acknowledgement must advance exactly one source offset.");

        var checkpoints = Snapshot.Checkpoints.ToArray();
        checkpoints[pending.MappingIndex] = checkpoints[pending.MappingIndex] with
        {
            NextSourceOffset = nextSourceOffset,
        };
        Snapshot = Snapshot with
        {
            Checkpoints = Array.AsReadOnly(checkpoints),
            AcknowledgedRecords = checked(Snapshot.AcknowledgedRecords + 1),
            AcknowledgedBytes = checked(Snapshot.AcknowledgedBytes + pending.RawBytes),
            PendingBatch = null,
            Version = checked(Snapshot.Version + 1),
        };
    }

    public void ResolveProvenNonApplication(Guid batchId)
    {
        _ = RequirePending(batchId, FleetTransferBatchState.DispatchStarted);
        Snapshot = Snapshot with
        {
            PendingBatch = null,
            Version = checked(Snapshot.Version + 1),
        };
    }

    public void ReleaseBeforeDispatch(Guid batchId)
    {
        _ = RequirePending(batchId, FleetTransferBatchState.ReservedBeforeDispatch);
        Snapshot = Snapshot with
        {
            PendingBatch = null,
            Version = checked(Snapshot.Version + 1),
        };
    }

    public static FleetTransferProgressSnapshot Validate(
        FleetTransferProgressSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        RequireFingerprint(snapshot.PlanFingerprint);
        if (snapshot.Checkpoints is null ||
            snapshot.Checkpoints.Count == 0 ||
            snapshot.Checkpoints.Count > 64)
            throw new MutationStateException("Transfer progress requires a bounded checkpoint set.");

        var ordered = snapshot.Checkpoints
            .OrderBy(item => item.MappingIndex)
            .ToArray();
        for (var index = 0; index < ordered.Length; index++)
        {
            if (ordered[index].MappingIndex != index ||
                ordered[index].NextSourceOffset < 0)
                throw new MutationStateException("Transfer progress checkpoint identity is invalid.");
        }

        if (snapshot.AcknowledgedRecords < 0 ||
            snapshot.AcknowledgedBytes < 0 ||
            snapshot.Version < 0)
            throw new MutationStateException("Transfer progress counters cannot be negative.");

        if (snapshot.PendingBatch is { } pending)
        {
            if (pending.BatchId == Guid.Empty ||
                pending.MappingIndex < 0 ||
                pending.MappingIndex >= ordered.Length ||
                pending.SourceOffset != ordered[pending.MappingIndex].NextSourceOffset ||
                pending.DestinationPartition < 0 ||
                pending.RawBytes < 0 ||
                !Enum.IsDefined(pending.State) ||
                (pending.State == FleetTransferBatchState.DispatchStarted &&
                 pending.DispatchStartedAtUtc is null) ||
                (pending.State == FleetTransferBatchState.ReservedBeforeDispatch &&
                 pending.DispatchStartedAtUtc is not null))
                throw new MutationStateException("Transfer pending-batch evidence is invalid.");
        }

        return snapshot with { Checkpoints = Array.AsReadOnly(ordered) };
    }

    private FleetTransferPendingBatch RequirePending(
        Guid batchId,
        FleetTransferBatchState expected)
    {
        if (batchId == Guid.Empty ||
            Snapshot.PendingBatch is not { } pending ||
            pending.BatchId != batchId ||
            pending.State != expected)
            throw new MutationStateException("Transfer pending batch state does not match the requested transition.");
        return pending;
    }

    private static void RequireFingerprint(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length != 64 || value.Any(character => !char.IsAsciiHexDigit(character)))
            throw new MutationStateException("Transfer plan fingerprint must be SHA-256 hex.");
    }
}
