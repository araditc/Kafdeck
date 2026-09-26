namespace Kafdeck.Modules.Administration;

public static class FleetRuntimeLimits
{
    public static readonly TimeSpan DefaultActiveObservationWindow = TimeSpan.FromHours(2);
    public static readonly TimeSpan MaxActiveObservationWindow = TimeSpan.FromHours(24);
    public const int DefaultPostCapReconciliationReadCount = 8;
    public const int MaxPostCapReconciliationReadCount = 64;
    public const int MaxPostCapReconciliationReadsPerRequest = 8;
}

public enum FleetProgressPhase
{
    Planned = 1,
    WaitingForMaterial = 2,
    Submitting = 3,
    Observing = 4,
    PausedAuthorization = 5,
    WaitingForExternalAction = 6,
    Stopped = 7,
    Completed = 8,
    Unknown = 9,
}

public enum FleetPostCapReadReservationState
{
    Reserved = 1,
    Dispatched = 2,
    ReleasedBeforeDispatch = 3,
}

public sealed record FleetPostCapReadReservation(
    Guid ReservationId,
    int Count,
    FleetPostCapReadReservationState State,
    DateTimeOffset ReservedAtUtc,
    DateTimeOffset? StateChangedAtUtc = null);

public sealed record FleetOperationProgressSnapshot
{
    public const int CurrentSchemaVersion = 1;

    public required Guid OperationId { get; init; }
    public int SchemaVersion { get; init; } = CurrentSchemaVersion;
    public FleetProgressPhase Phase { get; init; } = FleetProgressPhase.Planned;
    public long WorkerGeneration { get; init; }
    public long ActiveObservationElapsedTicks { get; init; }
    public int PostCapReconciliationReadCount { get; init; }
    public IReadOnlyList<FleetPostCapReadReservation> PostCapReadReservations { get; init; } =
        Array.Empty<FleetPostCapReadReservation>();
    public FleetTransferProgressSnapshot? Transfer { get; init; }
    public long Version { get; init; }
    public DateTimeOffset UpdatedAtUtc { get; init; }

    public TimeSpan ActiveObservationElapsed =>
        TimeSpan.FromTicks(ActiveObservationElapsedTicks);
}

public sealed class FleetOperationProgress
{
    private FleetOperationProgress(FleetOperationProgressSnapshot snapshot)
    {
        Snapshot = Validate(snapshot);
    }

    public FleetOperationProgressSnapshot Snapshot { get; private set; }

    public static FleetOperationProgress Create(
        Guid operationId,
        long workerGeneration,
        DateTimeOffset nowUtc)
    {
        if (operationId == Guid.Empty)
        {
            throw new ArgumentException("Operation ID is required.", nameof(operationId));
        }

        if (workerGeneration <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(workerGeneration));
        }

        return new FleetOperationProgress(new FleetOperationProgressSnapshot
        {
            OperationId = operationId,
            WorkerGeneration = workerGeneration,
            UpdatedAtUtc = nowUtc,
        });
    }

    public static FleetOperationProgress Restore(FleetOperationProgressSnapshot snapshot) =>
        new(snapshot ?? throw new ArgumentNullException(nameof(snapshot)));

    public void FenceToGeneration(long workerGeneration, DateTimeOffset nowUtc)
    {
        if (workerGeneration <= Snapshot.WorkerGeneration)
        {
            throw new MutationStateException(
                "Fleet progress fencing generation must advance monotonically.");
        }

        Snapshot = Snapshot with
        {
            WorkerGeneration = workerGeneration,
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };
    }

    public TimeSpan ChargeActiveObservation(
        TimeSpan elapsed,
        DateTimeOffset nowUtc)
    {
        if (elapsed < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(elapsed),
                "Active observation time cannot move backwards.");
        }

        if (elapsed == TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var hardCap = FleetRuntimeLimits.MaxActiveObservationWindow;
        var current = Snapshot.ActiveObservationElapsed;
        if (current >= hardCap)
        {
            TransitionToObservationCap(nowUtc);
            return TimeSpan.Zero;
        }

        var remaining = hardCap - current;
        var charged = elapsed <= remaining ? elapsed : remaining;
        var nextTicks = checked(
            Snapshot.ActiveObservationElapsedTicks + charged.Ticks);

        Snapshot = Snapshot with
        {
            ActiveObservationElapsedTicks = nextTicks,
            Phase = nextTicks >= hardCap.Ticks
                ? FleetProgressPhase.WaitingForExternalAction
                : FleetProgressPhase.Observing,
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };

        return charged;
    }

    public FleetPostCapReadReservation ReservePostCapReconciliationReads(
        int count,
        DateTimeOffset nowUtc)
    {
        if (Snapshot.ActiveObservationElapsed < FleetRuntimeLimits.MaxActiveObservationWindow)
        {
            throw new MutationStateException(
                "Post-cap reconciliation reads are unavailable before the active-observation lifetime cap is exhausted.");
        }

        if (count is < 1 or > FleetRuntimeLimits.MaxPostCapReconciliationReadsPerRequest)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        var nextCount = checked(Snapshot.PostCapReconciliationReadCount + count);
        if (nextCount > FleetRuntimeLimits.MaxPostCapReconciliationReadCount)
        {
            throw new MutationStateException(
                "Post-cap reconciliation read lifetime allowance is exhausted.");
        }

        var reservation = new FleetPostCapReadReservation(
            Guid.NewGuid(),
            count,
            FleetPostCapReadReservationState.Reserved,
            nowUtc);

        Snapshot = Snapshot with
        {
            PostCapReconciliationReadCount = nextCount,
            PostCapReadReservations = Snapshot.PostCapReadReservations
                .Append(reservation)
                .ToArray(),
            Phase = FleetProgressPhase.WaitingForExternalAction,
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };

        return reservation;
    }

    public void MarkPostCapReservationDispatched(
        Guid reservationId,
        DateTimeOffset nowUtc) =>
        UpdateReservation(
            reservationId,
            FleetPostCapReadReservationState.Reserved,
            FleetPostCapReadReservationState.Dispatched,
            nowUtc,
            releaseCount: false);

    public void ReleasePostCapReservationBeforeDispatch(
        Guid reservationId,
        DateTimeOffset nowUtc) =>
        UpdateReservation(
            reservationId,
            FleetPostCapReadReservationState.Reserved,
            FleetPostCapReadReservationState.ReleasedBeforeDispatch,
            nowUtc,
            releaseCount: true);

    private void UpdateReservation(
        Guid reservationId,
        FleetPostCapReadReservationState expectedState,
        FleetPostCapReadReservationState newState,
        DateTimeOffset nowUtc,
        bool releaseCount)
    {
        var items = Snapshot.PostCapReadReservations.ToArray();
        var index = Array.FindIndex(
            items,
            item => item.ReservationId == reservationId);

        if (index < 0)
        {
            throw new MutationStateException(
                "Post-cap reconciliation read reservation was not found.");
        }

        var current = items[index];
        if (current.State != expectedState)
        {
            throw new MutationStateException(
                $"Post-cap reservation is in state '{current.State}', expected '{expectedState}'.");
        }

        items[index] = current with
        {
            State = newState,
            StateChangedAtUtc = nowUtc,
        };

        var nextCount = Snapshot.PostCapReconciliationReadCount;
        if (releaseCount)
        {
            nextCount = checked(nextCount - current.Count);
        }

        Snapshot = Snapshot with
        {
            PostCapReconciliationReadCount = nextCount,
            PostCapReadReservations = Array.AsReadOnly(items),
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };
    }

    private void TransitionToObservationCap(DateTimeOffset nowUtc)
    {
        if (Snapshot.Phase == FleetProgressPhase.WaitingForExternalAction)
        {
            return;
        }

        Snapshot = Snapshot with
        {
            Phase = FleetProgressPhase.WaitingForExternalAction,
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };
    }

    private static FleetOperationProgressSnapshot Validate(
        FleetOperationProgressSnapshot snapshot)
    {
        if (snapshot.OperationId == Guid.Empty)
        {
            throw new MutationStateException("Fleet progress requires a parent operation ID.");
        }

        if (snapshot.SchemaVersion != FleetOperationProgressSnapshot.CurrentSchemaVersion)
        {
            throw new MutationStateException(
                $"Fleet progress schema version '{snapshot.SchemaVersion}' is unsupported.");
        }

        if (snapshot.WorkerGeneration <= 0)
        {
            throw new MutationStateException(
                "Fleet progress requires a positive worker generation.");
        }

        if (snapshot.ActiveObservationElapsedTicks < 0 ||
            snapshot.ActiveObservationElapsedTicks > FleetRuntimeLimits.MaxActiveObservationWindow.Ticks)
        {
            throw new MutationStateException(
                "Fleet active-observation accounting is outside the admitted lifetime range.");
        }

        if (snapshot.PostCapReconciliationReadCount is < 0 or > FleetRuntimeLimits.MaxPostCapReconciliationReadCount)
        {
            throw new MutationStateException(
                "Fleet post-cap reconciliation read accounting is outside the admitted lifetime range.");
        }

        var reservations = snapshot.PostCapReadReservations ??
            throw new MutationStateException(
                "Fleet post-cap reconciliation read reservations are required.");

        if (reservations
            .GroupBy(item => item.ReservationId)
            .Any(group => group.Key == Guid.Empty || group.Count() != 1))
        {
            throw new MutationStateException(
                "Fleet post-cap reconciliation read reservation identities must be unique and non-empty.");
        }

        var charged = reservations
            .Where(item => item.State is
                FleetPostCapReadReservationState.Reserved or
                FleetPostCapReadReservationState.Dispatched)
            .Sum(item => item.Count);

        if (charged != snapshot.PostCapReconciliationReadCount)
        {
            throw new MutationStateException(
                "Fleet post-cap reconciliation read counter does not match durable reservations.");
        }

        var transfer = snapshot.Transfer is null
            ? null
            : FleetTransferProgress.Validate(snapshot.Transfer);

        return snapshot with
        {
            PostCapReadReservations = Array.AsReadOnly(reservations.ToArray()),
            Transfer = transfer,
        };
    }
}
