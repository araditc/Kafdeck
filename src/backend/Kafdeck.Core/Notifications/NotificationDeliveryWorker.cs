namespace Kafdeck.Core.Notifications;

public enum NotificationDeliveryDispatchOutcome
{
    Delivered = 1,
    RetryableFailure = 2,
    PermanentFailure = 3,
    UnknownExternalEffect = 4,
}

public sealed record NotificationDeliveryDispatchResult
{
    public NotificationDeliveryDispatchResult(
        NotificationDeliveryDispatchOutcome outcome)
    {
        if (!Enum.IsDefined(outcome))
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome));
        }

        Outcome = outcome;
    }

    public NotificationDeliveryDispatchOutcome Outcome { get; }
}

public interface INotificationDeliveryDispatcher
{
    Task<NotificationDeliveryDispatchResult> DispatchAsync(
        NotificationDeliveryRecord claimedDelivery,
        CancellationToken cancellationToken);
}

public sealed record NotificationDeliveryWorkerPolicy
{
    public const int DefaultMaxDuePerCycle = 50;
    public const int HardMaxDuePerCycle =
        NotificationDeliveryDueQuery.HardMaxResults;
    public const int DefaultMaxRecoveryPerCycle = 100;
    public const int HardMaxRecoveryPerCycle =
        NotificationDeliveryDueQuery.HardMaxResults;

    public static readonly TimeSpan DefaultStaleInFlightAfter =
        TimeSpan.FromMinutes(2);
    public static readonly TimeSpan MinimumStaleInFlightAfter =
        TimeSpan.FromSeconds(10);
    public static readonly TimeSpan HardMaxStaleInFlightAfter =
        TimeSpan.FromMinutes(30);

    public NotificationDeliveryWorkerPolicy(
        int maxDuePerCycle = DefaultMaxDuePerCycle,
        int maxRecoveryPerCycle = DefaultMaxRecoveryPerCycle,
        TimeSpan? staleInFlightAfter = null)
    {
        var stale =
            staleInFlightAfter ??
            DefaultStaleInFlightAfter;

        if (maxDuePerCycle is < 1 or >
                HardMaxDuePerCycle ||
            maxRecoveryPerCycle is < 1 or >
                HardMaxRecoveryPerCycle ||
            stale < MinimumStaleInFlightAfter ||
            stale > HardMaxStaleInFlightAfter)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDuePerCycle),
                "Notification delivery worker policy exceeds admitted bounds.");
        }

        MaxDuePerCycle = maxDuePerCycle;
        MaxRecoveryPerCycle = maxRecoveryPerCycle;
        StaleInFlightAfter = stale;
    }

    public int MaxDuePerCycle { get; }
    public int MaxRecoveryPerCycle { get; }
    public TimeSpan StaleInFlightAfter { get; }

    public static NotificationDeliveryWorkerPolicy Default { get; } =
        new();
}

public enum NotificationDeliveryWorkItemOutcome
{
    CasLost = 1,
    Delivered = 2,
    RetryScheduled = 3,
    Exhausted = 4,
    UnknownExternalEffect = 5,
    FinalizationConflict = 6,
    AdmissionDeferred = 7,
}

public sealed record NotificationDeliveryCycleResult(
    int Visited,
    int Delivered,
    int RetryScheduled,
    int Exhausted,
    int UnknownExternalEffect,
    int CasLost,
    int FinalizationConflicts,
    int AdmissionDeferred,
    bool MoreDue);

public sealed record NotificationDeliveryRecoveryResult(
    int Visited,
    int MarkedUnknownExternalEffect,
    int CasLost,
    bool MoreStale);

public sealed class NotificationDeliveryWorker
{
    private readonly INotificationDeliveryStore
        _store;
    private readonly INotificationDeliveryDispatcher
        _dispatcher;
    private readonly NotificationDeliveryPolicy
        _deliveryPolicy;
    private readonly NotificationDeliveryWorkerPolicy
        _workerPolicy;
    private readonly TimeProvider
        _timeProvider;
    private readonly SemaphoreSlim
        _dueCycleGate = new(1, 1);
    private readonly object
        _dueCursorGate = new();
    private NotificationDeliveryDueCursor?
        _dueCursor;

    public NotificationDeliveryWorker(
        INotificationDeliveryStore store,
        INotificationDeliveryDispatcher dispatcher,
        NotificationDeliveryPolicy? deliveryPolicy = null,
        NotificationDeliveryWorkerPolicy? workerPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _store =
            store ??
            throw new ArgumentNullException(
                nameof(store));
        _dispatcher =
            dispatcher ??
            throw new ArgumentNullException(
                nameof(dispatcher));
        _deliveryPolicy =
            deliveryPolicy ??
            new NotificationDeliveryPolicy();
        _workerPolicy =
            workerPolicy ??
            NotificationDeliveryWorkerPolicy.Default;
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
    }

    public async Task<NotificationDeliveryCycleResult>
        RunDueCycleAsync(
            CancellationToken cancellationToken = default)
    {
        await _dueCycleGate
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            return await RunDueCycleCoreAsync(
                    cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _dueCycleGate.Release();
        }
    }

    private async Task<NotificationDeliveryCycleResult>
        RunDueCycleCoreAsync(
            CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var now =
            _timeProvider
                .GetUtcNow();
        var workRemaining =
            _workerPolicy.MaxDuePerCycle;
        var scanRemaining =
            NotificationDeliveryDueQuery
                .HardMaxResults;
        var cursor =
            GetDueCursor();
        var startedAfterCursor =
            cursor is not null;
        var moreDue =
            false;
        var outcomes =
            new List<
                NotificationDeliveryWorkItemOutcome>(
                _workerPolicy.MaxDuePerCycle);

        using var concurrency =
            new SemaphoreSlim(
                _deliveryPolicy.MaxConcurrency,
                _deliveryPolicy.MaxConcurrency);

        while (workRemaining > 0 &&
               scanRemaining > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var pageLimit =
                Math.Min(
                    workRemaining,
                    scanRemaining);
            var page =
                await _store
                    .ListDueAsync(
                        new NotificationDeliveryDueQuery(
                            now,
                            pageLimit,
                            cursor),
                        cancellationToken)
                    .ConfigureAwait(false);

            if (page.Items.Count == 0)
            {
                SetDueCursor(
                    null);
                moreDue =
                    startedAfterCursor;
                break;
            }

            var tasks =
                page.Items
                    .Select(
                        async item =>
                        {
                            await concurrency
                                .WaitAsync(cancellationToken)
                                .ConfigureAwait(false);
                            try
                            {
                                return await ProcessDueAsync(
                                        item,
                                        cancellationToken)
                                    .ConfigureAwait(false);
                            }
                            finally
                            {
                                concurrency.Release();
                            }
                        })
                    .ToArray();

            var batch =
                await Task.WhenAll(tasks)
                    .ConfigureAwait(false);

            outcomes.AddRange(
                batch);
            scanRemaining -=
                page.Items.Count;

            var consumedWork =
                batch.Count(
                    value =>
                        value !=
                        NotificationDeliveryWorkItemOutcome
                            .AdmissionDeferred);
            workRemaining =
                Math.Max(
                    0,
                    workRemaining -
                    consumedWork);

            if (page.Truncated)
            {
                cursor =
                    page.NextCursor;
                SetDueCursor(
                    cursor);
                moreDue =
                    true;
                continue;
            }

            SetDueCursor(
                null);
            moreDue =
                startedAfterCursor;
            break;
        }

        if (scanRemaining == 0 &&
            GetDueCursor() is not null)
        {
            moreDue =
                true;
        }

        if (outcomes.Any(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome
                        .AdmissionDeferred))
        {
            moreDue =
                true;
        }

        return new NotificationDeliveryCycleResult(
            outcomes.Count,
            outcomes.Count(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome.Delivered),
            outcomes.Count(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome.RetryScheduled),
            outcomes.Count(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome.Exhausted),
            outcomes.Count(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome.UnknownExternalEffect),
            outcomes.Count(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome.CasLost),
            outcomes.Count(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome.FinalizationConflict),
            outcomes.Count(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome.AdmissionDeferred),
            moreDue);
    }

    public async Task<NotificationDeliveryRecoveryResult>
        RecoverStaleInFlightAsync(
            CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var now =
            await _store
                .GetAuthoritativeUtcNowAsync(
                    cancellationToken)
                .ConfigureAwait(false);
        var page =
            await _store
                .ListStaleInFlightAsync(
                    new NotificationStaleInFlightQuery(
                        now -
                        _workerPolicy.StaleInFlightAfter,
                        _workerPolicy.MaxRecoveryPerCycle),
                    cancellationToken)
                .ConfigureAwait(false);

        var marked = 0;
        var casLost = 0;

        foreach (var item in page.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var snapshot =
                TerminalSnapshot(
                    item.Snapshot,
                    NotificationDeliveryState
                        .UnknownExternalEffect,
                    NotificationDeliveryOutcomeCodes
                        .UnknownExternalEffect);

            var replaced =
                await _store
                    .ReplaceAsync(
                        snapshot,
                        item.Revision,
                        now,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (replaced is null)
            {
                casLost++;
            }
            else
            {
                marked++;
            }
        }

        return new NotificationDeliveryRecoveryResult(
            page.Items.Count,
            marked,
            casLost,
            page.Truncated);
    }

    private NotificationDeliveryDueCursor?
        GetDueCursor()
    {
        lock (_dueCursorGate)
        {
            return _dueCursor;
        }
    }

    private void SetDueCursor(
        NotificationDeliveryDueCursor? cursor)
    {
        lock (_dueCursorGate)
        {
            _dueCursor =
                cursor;
        }
    }

    private async Task<NotificationDeliveryWorkItemOutcome>
        ProcessDueAsync(
            NotificationDeliveryRecord due,
            CancellationToken cancellationToken)
    {
        var now =
            _timeProvider
                .GetUtcNow();

        if (ShouldExhaustBeforeAttempt(
                due.Snapshot,
                now))
        {
            var exhausted =
                await _store
                    .ReplaceAsync(
                        TerminalSnapshot(
                            due.Snapshot,
                            NotificationDeliveryState.Exhausted,
                            NotificationDeliveryOutcomeCodes.Exhausted),
                        due.Revision,
                        now,
                        cancellationToken)
                    .ConfigureAwait(false);

            return exhausted is null
                ? NotificationDeliveryWorkItemOutcome.CasLost
                : NotificationDeliveryWorkItemOutcome.Exhausted;
        }

        var claim =
            await _store
                .TryClaimForDispatchAsync(
                    due.Snapshot.NotificationId,
                    due.Snapshot.DestinationId,
                    due.Revision,
                    now,
                    _deliveryPolicy.MaxConcurrency,
                    _deliveryPolicy.RatePerSecond,
                    cancellationToken)
                .ConfigureAwait(false);

        if (claim.Outcome is
            NotificationDeliveryClaimOutcome.RateLimited or
            NotificationDeliveryClaimOutcome.ConcurrencyLimited)
        {
            return NotificationDeliveryWorkItemOutcome
                .AdmissionDeferred;
        }

        if (claim.Outcome !=
                NotificationDeliveryClaimOutcome.Claimed ||
            claim.Record is null)
        {
            return NotificationDeliveryWorkItemOutcome.CasLost;
        }

        var claimed =
            claim.Record;

        NotificationDeliveryDispatchOutcome
            dispatchOutcome;
        try
        {
            var dispatch =
                await _dispatcher
                    .DispatchAsync(
                        claimed,
                        cancellationToken)
                    .ConfigureAwait(false);

            dispatchOutcome =
                dispatch.Outcome;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            // Leave the claimed delivery InFlight. Recovery will conservatively
            // classify it as UnknownExternalEffect because external application
            // may already have occurred.
            throw;
        }
        catch
        {
            dispatchOutcome =
                NotificationDeliveryDispatchOutcome
                    .UnknownExternalEffect;
        }

        var completedAt =
            _timeProvider
                .GetUtcNow();

        var finalSnapshot =
            BuildFinalSnapshot(
                claimed.Snapshot,
                dispatchOutcome,
                completedAt);

        var finalized =
            await _store
                .ReplaceAsync(
                    finalSnapshot,
                    claimed.Revision,
                    completedAt,
                    cancellationToken)
                .ConfigureAwait(false);

        if (finalized is null)
        {
            return NotificationDeliveryWorkItemOutcome
                .FinalizationConflict;
        }

        return finalSnapshot.State switch
        {
            NotificationDeliveryState.Delivered =>
                NotificationDeliveryWorkItemOutcome.Delivered,
            NotificationDeliveryState.Failed =>
                NotificationDeliveryWorkItemOutcome.RetryScheduled,
            NotificationDeliveryState.Exhausted =>
                NotificationDeliveryWorkItemOutcome.Exhausted,
            NotificationDeliveryState.UnknownExternalEffect =>
                NotificationDeliveryWorkItemOutcome.UnknownExternalEffect,
            _ => throw new InvalidOperationException(
                "Notification delivery worker produced a non-terminal/non-retry final state."),
        };
    }

    private NotificationDeliverySnapshot
        BuildFinalSnapshot(
            NotificationDeliverySnapshot claimed,
            NotificationDeliveryDispatchOutcome outcome,
            DateTimeOffset completedAtUtc)
    {
        return outcome switch
        {
            NotificationDeliveryDispatchOutcome.Delivered =>
                TerminalSnapshot(
                    claimed,
                    NotificationDeliveryState.Delivered,
                    NotificationDeliveryOutcomeCodes.Delivered),

            NotificationDeliveryDispatchOutcome
                .UnknownExternalEffect =>
                TerminalSnapshot(
                    claimed,
                    NotificationDeliveryState
                        .UnknownExternalEffect,
                    NotificationDeliveryOutcomeCodes
                        .UnknownExternalEffect),

            NotificationDeliveryDispatchOutcome.PermanentFailure =>
                TerminalSnapshot(
                    claimed,
                    NotificationDeliveryState.Exhausted,
                    NotificationDeliveryOutcomeCodes.Exhausted),

            NotificationDeliveryDispatchOutcome.RetryableFailure =>
                RetryOrExhaust(
                    claimed,
                    completedAtUtc),

            _ => throw new ArgumentOutOfRangeException(
                nameof(outcome)),
        };
    }

    private NotificationDeliverySnapshot RetryOrExhaust(
        NotificationDeliverySnapshot claimed,
        DateTimeOffset completedAtUtc)
    {
        if (claimed.AttemptCount >=
            _deliveryPolicy.MaxAttempts)
        {
            return TerminalSnapshot(
                claimed,
                NotificationDeliveryState.Exhausted,
                NotificationDeliveryOutcomeCodes.Exhausted);
        }

        var retryAt =
            completedAtUtc +
            BackoffForAttempt(
                claimed.AttemptCount);
        var deadline =
            claimed.CreatedAtUtc +
            _deliveryPolicy.Lifetime;

        if (retryAt > deadline)
        {
            return TerminalSnapshot(
                claimed,
                NotificationDeliveryState.Exhausted,
                NotificationDeliveryOutcomeCodes.Exhausted);
        }

        return new NotificationDeliverySnapshot(
            claimed.NotificationId,
            claimed.DestinationId,
            claimed.PayloadFingerprint,
            NotificationDeliveryState.Failed,
            claimed.AttemptCount,
            claimed.CreatedAtUtc,
            retryAt,
            NotificationDeliveryOutcomeCodes
                .RetryableFailure);
    }

    private bool ShouldExhaustBeforeAttempt(
        NotificationDeliverySnapshot snapshot,
        DateTimeOffset nowUtc)
    {
        if (snapshot.AttemptCount >=
            _deliveryPolicy.MaxAttempts)
        {
            return true;
        }

        return nowUtc >
               snapshot.CreatedAtUtc +
               _deliveryPolicy.Lifetime;
    }

    private TimeSpan BackoffForAttempt(
        int completedAttempt)
    {
        if (completedAttempt < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(completedAttempt));
        }

        var multiplier =
            1L <<
            Math.Min(
                completedAttempt - 1,
                NotificationDeliveryPolicy.HardMaxAttempts - 1);

        var ticks =
            Math.Min(
                _deliveryPolicy.MaxBackoff.Ticks,
                checked(
                    _deliveryPolicy.InitialRetry.Ticks *
                    multiplier));

        return TimeSpan.FromTicks(
            ticks);
    }

    private static NotificationDeliverySnapshot
        TerminalSnapshot(
            NotificationDeliverySnapshot current,
            NotificationDeliveryState state,
            string outcomeCode) =>
        new(
            current.NotificationId,
            current.DestinationId,
            current.PayloadFingerprint,
            state,
            current.AttemptCount,
            current.CreatedAtUtc,
            null,
            outcomeCode);
}
