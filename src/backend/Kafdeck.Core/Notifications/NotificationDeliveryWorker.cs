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
        NotificationDeliveryDispatchOutcome outcome,
        string? providerReceiptFingerprint = null)
    {
        if (!Enum.IsDefined(outcome))
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome));
        }

        Outcome = outcome;
        if (providerReceiptFingerprint is not null &&
            (providerReceiptFingerprint.Length != 64 || providerReceiptFingerprint.Any(ch => !Uri.IsHexDigit(ch))))
            throw new ArgumentException("Provider receipt fingerprint must be a SHA-256 digest.", nameof(providerReceiptFingerprint));
        ProviderReceiptFingerprint = providerReceiptFingerprint?.ToLowerInvariant();
    }

    public NotificationDeliveryDispatchOutcome Outcome { get; }
    public string? ProviderReceiptFingerprint { get; }
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
    public static readonly TimeSpan DefaultDispatchTimeout = TimeSpan.FromSeconds(20);
    public static readonly TimeSpan MinimumDispatchTimeout = TimeSpan.FromMilliseconds(100);
    public static readonly TimeSpan HardMaxDispatchTimeout = TimeSpan.FromMinutes(1);

    public NotificationDeliveryWorkerPolicy(
        int maxDuePerCycle = DefaultMaxDuePerCycle,
        int maxRecoveryPerCycle = DefaultMaxRecoveryPerCycle,
        TimeSpan? staleInFlightAfter = null,
        TimeSpan? dispatchTimeout = null)
    {
        var stale =
            staleInFlightAfter ??
            DefaultStaleInFlightAfter;
        var dispatch = dispatchTimeout ??
            (stale <= DefaultDispatchTimeout ? stale / 2 : DefaultDispatchTimeout);

        if (maxDuePerCycle is < 1 or >
                HardMaxDuePerCycle ||
            maxRecoveryPerCycle is < 1 or >
                HardMaxRecoveryPerCycle ||
            stale < MinimumStaleInFlightAfter ||
            stale > HardMaxStaleInFlightAfter ||
            dispatch < MinimumDispatchTimeout || dispatch > HardMaxDispatchTimeout || dispatch >= stale)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDuePerCycle),
                "Notification delivery worker policy exceeds admitted bounds.");
        }

        MaxDuePerCycle = maxDuePerCycle;
        MaxRecoveryPerCycle = maxRecoveryPerCycle;
        StaleInFlightAfter = stale;
        DispatchTimeout = dispatch;
    }

    public int MaxDuePerCycle { get; }
    public int MaxRecoveryPerCycle { get; }
    public TimeSpan StaleInFlightAfter { get; }
    public TimeSpan DispatchTimeout { get; }

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
    // A timed-out asynchronous port may ignore cancellation indefinitely.
    // Retain its admission until its actual task completes; repeated cycles
    // cannot create an unbounded number of abandoned operations.
    private readonly SemaphoreSlim _dispatchSlots;
    private readonly INotificationDeliveryActivationGate? _activationGate;
    private readonly INotificationDeliveryClaimGate? _claimGate;
    private readonly object
        _dueCursorGate = new();
    private NotificationDeliveryDueCursor?
        _dueCursor;

    public NotificationDeliveryWorker(
        INotificationDeliveryStore store,
        INotificationDeliveryDispatcher dispatcher,
        NotificationDeliveryPolicy? deliveryPolicy = null,
        NotificationDeliveryWorkerPolicy? workerPolicy = null,
        TimeProvider? timeProvider = null,
        INotificationDeliveryActivationGate? activationGate = null,
        INotificationDeliveryClaimGate? claimGate = null)
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
        _dispatchSlots = new SemaphoreSlim(_deliveryPolicy.MaxConcurrency, _deliveryPolicy.MaxConcurrency);
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
        _activationGate = activationGate;
        _claimGate = claimGate;
    }

    public async Task<NotificationDeliveryCycleResult>
        RunDueCycleAsync(
            CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_activationGate is not null) await _activationGate.VerifyAsync(cancellationToken).ConfigureAwait(false);

        var localNow =
            _timeProvider
                .GetUtcNow();
        var now =
            await _store
                .GetCoordinationUtcNowAsync(
                    localNow,
                    cancellationToken)
                .ConfigureAwait(false);

        var startingCursor =
            GetDueCursor();
        var page =
            await _store
                .ListDueAsync(
                    new NotificationDeliveryDueQuery(
                        now,
                        _workerPolicy.MaxDuePerCycle,
                        startingCursor),
                    cancellationToken)
                .ConfigureAwait(false);

        if (page.Items.Count == 0 &&
            startingCursor is not null)
        {
            SetDueCursor(
                null);
            startingCursor =
                null;
            page =
                await _store
                    .ListDueAsync(
                        new NotificationDeliveryDueQuery(
                            now,
                            _workerPolicy.MaxDuePerCycle),
                        cancellationToken)
                    .ConfigureAwait(false);
        }

        if (page.Items.Count == 0)
        {
            return new NotificationDeliveryCycleResult(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                page.Truncated);
        }

        using var concurrency =
            new SemaphoreSlim(
                _deliveryPolicy.MaxConcurrency,
                _deliveryPolicy.MaxConcurrency);

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

        var outcomes =
            await Task.WhenAll(tasks)
                .ConfigureAwait(false);

        SetDueCursor(
            page.Truncated
                ? page.NextCursor
                : null);

        var admissionDeferred =
            outcomes.Count(
                value =>
                    value ==
                    NotificationDeliveryWorkItemOutcome.AdmissionDeferred);

        return new NotificationDeliveryCycleResult(
            outcomes.Length,
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
            admissionDeferred,
            page.Truncated ||
            startingCursor is not null ||
            admissionDeferred > 0);
    }

    public async Task<NotificationDeliveryRecoveryResult>
        RecoverStaleInFlightAsync(
            CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var localNow =
            _timeProvider
                .GetUtcNow();
        var now =
            await _store
                .GetCoordinationUtcNowAsync(
                    localNow,
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

    private async Task<NotificationDeliveryWorkItemOutcome>
        ProcessDueAsync(
            NotificationDeliveryRecord due,
            CancellationToken cancellationToken)
    {
        var localNow =
            _timeProvider
                .GetUtcNow();
        var now =
            await _store
                .GetCoordinationUtcNowAsync(
                    localNow,
                    cancellationToken)
                .ConfigureAwait(false);

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

        if (!_dispatchSlots.Wait(0))
            return NotificationDeliveryWorkItemOutcome.AdmissionDeferred;
        var dispatchOwnsSlot = false;
        try
        {
            if (_activationGate is not null) await _activationGate.VerifyAsync(cancellationToken).ConfigureAwait(false);
            if (_claimGate is not null && !await _claimGate.CanClaimAsync(due.Snapshot.DestinationId, cancellationToken).ConfigureAwait(false))
                return NotificationDeliveryWorkItemOutcome.AdmissionDeferred;
            var claim =
                await _store
                    .TryClaimForDispatchAsync(
                        due.Snapshot.NotificationId,
                        due.Snapshot.DestinationId,
                        due.Revision,
                        now,
                        due.Snapshot.CreatedAtUtc +
                        _deliveryPolicy.Lifetime,
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

            if (claim.Outcome ==
                NotificationDeliveryClaimOutcome.Expired)
            {
                var expiredLocal =
                    _timeProvider
                        .GetUtcNow();
                var expiredAt =
                    await _store
                        .GetCoordinationUtcNowAsync(
                            expiredLocal,
                            cancellationToken)
                        .ConfigureAwait(false);
                if (expiredAt <
                    due.UpdatedAtUtc)
                {
                    expiredAt =
                        due.UpdatedAtUtc;
                }

                var exhausted =
                    await _store
                        .ReplaceAsync(
                            TerminalSnapshot(
                                due.Snapshot,
                                NotificationDeliveryState.Exhausted,
                                NotificationDeliveryOutcomeCodes.Exhausted),
                            due.Revision,
                            expiredAt,
                            cancellationToken)
                        .ConfigureAwait(false);

                return exhausted is null
                    ? NotificationDeliveryWorkItemOutcome.CasLost
                    : NotificationDeliveryWorkItemOutcome.Exhausted;
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
            string? receiptFingerprint = null;
            var dispatchCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            Task<NotificationDeliveryDispatchResult>? dispatchTask = null;
            Task cancellationCallbacks = Task.CompletedTask;
            try
            {
                // The whole dispatch includes durable-event/profile reads, DNS,
                // credential admission and HTTPS. HttpClient's own timeout starts
                // too late to bound a stalled resolver. WaitAsync bounds our wait
                // even if an underlying asynchronous port ignores cancellation.
                dispatchTask = _dispatcher.DispatchAsync(claimed, dispatchCancellation.Token)
                    ?? throw new InvalidOperationException("Notification dispatcher returned no task.");
                var dispatch =
                    await dispatchTask
                        .WaitAsync(_workerPolicy.DispatchTimeout, cancellationToken)
                        .ConfigureAwait(false);

                dispatchOutcome =
                    dispatch.Outcome;
                receiptFingerprint = dispatch.ProviderReceiptFingerprint;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                // Leave the claimed delivery InFlight. Recovery will conservatively
                // classify it as UnknownExternalEffect because external application
                // may already have occurred.
                throw;
            }
            catch (TimeoutException)
            {
                // Cancellation is best-effort; arbitrary port callbacks must not
                // hold the worker's finite deadline. Unknown is terminal: a late
                // completion may already represent an external effect, never a
                // retryable failure. The adapter checks this token after DNS and
                // immediately before transport so late DNS cannot begin a send.
                cancellationCallbacks = dispatchCancellation.CancelAsync();
                dispatchOutcome = NotificationDeliveryDispatchOutcome.UnknownExternalEffect;
            }
            catch
            {
                dispatchOutcome =
                    NotificationDeliveryDispatchOutcome
                        .UnknownExternalEffect;
            }
            finally
            {
                if (dispatchTask is null) dispatchCancellation.Dispose();
                else
                {
                    dispatchOwnsSlot = true;
                    _ = CompleteDispatchAdmissionAsync(dispatchTask, cancellationCallbacks, dispatchCancellation);
                }
            }

            var completedLocal =
                _timeProvider
                    .GetUtcNow();
            var completedAt =
                await _store
                    .GetCoordinationUtcNowAsync(
                        completedLocal,
                        cancellationToken)
                    .ConfigureAwait(false);
            if (completedAt <
                claimed.UpdatedAtUtc)
            {
                completedAt =
                    claimed.UpdatedAtUtc;
            }

            var finalSnapshot =
                BuildFinalSnapshot(
                    claimed.Snapshot,
                    dispatchOutcome,
                    completedAt,
                    receiptFingerprint);

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
        finally
        {
            if (!dispatchOwnsSlot) _dispatchSlots.Release();
        }
    }

    private async Task CompleteDispatchAdmissionAsync(Task dispatch, Task cancellationCallbacks,
        CancellationTokenSource cancellation)
    {
        // Observe late faults without logging provider details. Keep both the
        // token source and admission alive until the real port and any async
        // cancellation callbacks finish, even after our finite wait expired.
        try
        {
            try { await dispatch.ConfigureAwait(false); }
            catch (Exception) { }
            try { await cancellationCallbacks.ConfigureAwait(false); }
            catch (Exception) { }
        }
        finally
        {
            cancellation.Dispose();
            _dispatchSlots.Release();
        }
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

    private NotificationDeliverySnapshot
        BuildFinalSnapshot(
            NotificationDeliverySnapshot claimed,
            NotificationDeliveryDispatchOutcome outcome,
            DateTimeOffset completedAtUtc,
            string? receiptFingerprint = null)
    {
        return outcome switch
        {
            NotificationDeliveryDispatchOutcome.Delivered =>
                TerminalSnapshot(
                    claimed,
                    NotificationDeliveryState.Delivered,
                    NotificationDeliveryOutcomeCodes.Delivered,
                    receiptFingerprint),

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
                .RetryableFailure,
            routedProfileRevisionFingerprint:
                claimed.RoutedProfileRevisionFingerprint);
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
            string outcomeCode,
            string? receiptFingerprint = null) =>
        new(
            current.NotificationId,
            current.DestinationId,
            current.PayloadFingerprint,
            state,
            current.AttemptCount,
            current.CreatedAtUtc,
            null,
            outcomeCode,
            routedProfileRevisionFingerprint:
                current.RoutedProfileRevisionFingerprint,
            providerReceiptFingerprint: receiptFingerprint);
}
