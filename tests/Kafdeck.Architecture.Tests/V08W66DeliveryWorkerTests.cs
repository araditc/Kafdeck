using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66DeliveryWorkerTests
{
    private const string Fingerprint =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    [Fact]
    public async Task Delivered_attempt_is_claimed_before_dispatch_and_terminal()
    {
        var path = TempPath();

        try
        {
            var store = Store(path);
            await store.InitializeAsync();

            var time =
                new MutableTimeProvider(
                    new DateTimeOffset(
                        2026,
                        10,
                        7,
                        12,
                        0,
                        0,
                        TimeSpan.Zero));
            var initial =
                await store.CreateOrGetAsync(
                    Pending(
                        Guid.Parse(
                            "11111111-1111-1111-1111-111111111111"),
                        time.GetUtcNow()),
                    time.GetUtcNow());

            var dispatcher =
                new RecordingDispatcher(
                    NotificationDeliveryDispatchOutcome.Delivered);
            var worker =
                new NotificationDeliveryWorker(
                    store,
                    dispatcher,
                    timeProvider:
                        time);

            var result =
                await worker.RunDueCycleAsync();

            Assert.Equal(
                1,
                result.Visited);
            Assert.Equal(
                1,
                result.Delivered);
            var claimed =
                Assert.Single(
                    dispatcher.Claims);
            Assert.Equal(
                NotificationDeliveryState.InFlight,
                claimed.Snapshot.State);
            Assert.Equal(
                1,
                claimed.Snapshot.AttemptCount);
            Assert.Equal(
                initial.Revision + 1,
                claimed.Revision);

            var stored =
                await store.GetAsync(
                    initial.Snapshot.NotificationId,
                    initial.Snapshot.DestinationId);
            Assert.NotNull(
                stored);
            Assert.Equal(
                NotificationDeliveryState.Delivered,
                stored!.Snapshot.State);
            Assert.Equal(
                NotificationDeliveryOutcomeCodes.Delivered,
                stored.Snapshot.OutcomeCode);
            Assert.Equal(
                1,
                stored.Snapshot.AttemptCount);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Retryable_failure_waits_for_bounded_retry_then_can_deliver()
    {
        var path = TempPath();

        try
        {
            var store = Store(path);
            await store.InitializeAsync();

            var time =
                new MutableTimeProvider(
                    new DateTimeOffset(
                        2026,
                        10,
                        7,
                        13,
                        0,
                        0,
                        TimeSpan.Zero));
            var id =
                Guid.Parse(
                    "22222222-2222-2222-2222-222222222222");
            await store.CreateOrGetAsync(
                Pending(
                    id,
                    time.GetUtcNow()),
                time.GetUtcNow());

            var dispatcher =
                new RecordingDispatcher(
                    NotificationDeliveryDispatchOutcome.RetryableFailure,
                    NotificationDeliveryDispatchOutcome.Delivered);
            var policy =
                new NotificationDeliveryPolicy(
                    maxAttempts: 3,
                    initialRetry:
                        TimeSpan.FromSeconds(5),
                    maxBackoff:
                        TimeSpan.FromSeconds(20),
                    lifetime:
                        TimeSpan.FromMinutes(5));
            var worker =
                new NotificationDeliveryWorker(
                    store,
                    dispatcher,
                    policy,
                    timeProvider:
                        time);

            var first =
                await worker.RunDueCycleAsync();

            Assert.Equal(
                1,
                first.RetryScheduled);
            var failed =
                await store.GetAsync(
                    id,
                    "ops-webhook");
            Assert.NotNull(
                failed);
            Assert.Equal(
                NotificationDeliveryState.Failed,
                failed!.Snapshot.State);
            Assert.Equal(
                time.GetUtcNow() +
                TimeSpan.FromSeconds(5),
                failed.Snapshot.NextAttemptAtUtc);

            time.Advance(
                TimeSpan.FromSeconds(4));
            var tooEarly =
                await worker.RunDueCycleAsync();
            Assert.Equal(
                0,
                tooEarly.Visited);
            Assert.Single(
                dispatcher.Claims);

            time.Advance(
                TimeSpan.FromSeconds(1));
            var second =
                await worker.RunDueCycleAsync();

            Assert.Equal(
                1,
                second.Delivered);
            Assert.Equal(
                2,
                dispatcher.Claims.Count);
            var delivered =
                await store.GetAsync(
                    id,
                    "ops-webhook");
            Assert.Equal(
                2,
                delivered!.Snapshot.AttemptCount);
            Assert.Equal(
                NotificationDeliveryState.Delivered,
                delivered.Snapshot.State);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Theory]
    [InlineData(
        NotificationDeliveryDispatchOutcome.PermanentFailure,
        NotificationDeliveryState.Exhausted)]
    [InlineData(
        NotificationDeliveryDispatchOutcome.UnknownExternalEffect,
        NotificationDeliveryState.UnknownExternalEffect)]
    public async Task Non_retryable_outcomes_are_terminal_and_never_requeued(
        NotificationDeliveryDispatchOutcome outcome,
        NotificationDeliveryState expectedState)
    {
        var path = TempPath();

        try
        {
            var store = Store(path);
            await store.InitializeAsync();

            var time =
                new MutableTimeProvider(
                    new DateTimeOffset(
                        2026,
                        10,
                        7,
                        14,
                        0,
                        0,
                        TimeSpan.Zero));
            var id =
                Guid.NewGuid();
            await store.CreateOrGetAsync(
                Pending(
                    id,
                    time.GetUtcNow()),
                time.GetUtcNow());

            var dispatcher =
                new RecordingDispatcher(
                    outcome);
            var worker =
                new NotificationDeliveryWorker(
                    store,
                    dispatcher,
                    timeProvider:
                        time);

            await worker.RunDueCycleAsync();
            time.Advance(
                TimeSpan.FromHours(1));
            var later =
                await worker.RunDueCycleAsync();

            Assert.Equal(
                0,
                later.Visited);
            Assert.Single(
                dispatcher.Claims);

            var stored =
                await store.GetAsync(
                    id,
                    "ops-webhook");
            Assert.Equal(
                expectedState,
                stored!.Snapshot.State);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Dispatcher_exception_is_conservatively_unknown_and_not_retried()
    {
        var path = TempPath();

        try
        {
            var store = Store(path);
            await store.InitializeAsync();

            var time =
                new MutableTimeProvider(
                    new DateTimeOffset(
                        2026,
                        10,
                        7,
                        15,
                        0,
                        0,
                        TimeSpan.Zero));
            var id =
                Guid.NewGuid();
            await store.CreateOrGetAsync(
                Pending(
                    id,
                    time.GetUtcNow()),
                time.GetUtcNow());

            var dispatcher =
                new ThrowingDispatcher();
            var worker =
                new NotificationDeliveryWorker(
                    store,
                    dispatcher,
                    timeProvider:
                        time);

            var result =
                await worker.RunDueCycleAsync();

            Assert.Equal(
                1,
                result.UnknownExternalEffect);
            var stored =
                await store.GetAsync(
                    id,
                    "ops-webhook");
            Assert.Equal(
                NotificationDeliveryState.UnknownExternalEffect,
                stored!.Snapshot.State);

            time.Advance(
                TimeSpan.FromMinutes(10));
            Assert.Equal(
                0,
                (await worker.RunDueCycleAsync())
                    .Visited);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Max_attempts_exhaust_without_another_external_dispatch()
    {
        var path = TempPath();

        try
        {
            var store = Store(path);
            await store.InitializeAsync();

            var time =
                new MutableTimeProvider(
                    new DateTimeOffset(
                        2026,
                        10,
                        7,
                        16,
                        0,
                        0,
                        TimeSpan.Zero));
            var id =
                Guid.NewGuid();
            await store.CreateOrGetAsync(
                Pending(
                    id,
                    time.GetUtcNow()),
                time.GetUtcNow());

            var dispatcher =
                new RecordingDispatcher(
                    NotificationDeliveryDispatchOutcome.RetryableFailure,
                    NotificationDeliveryDispatchOutcome.RetryableFailure,
                    NotificationDeliveryDispatchOutcome.Delivered);
            var policy =
                new NotificationDeliveryPolicy(
                    maxAttempts: 2,
                    initialRetry:
                        TimeSpan.FromSeconds(1),
                    maxBackoff:
                        TimeSpan.FromSeconds(2),
                    lifetime:
                        TimeSpan.FromMinutes(1));
            var worker =
                new NotificationDeliveryWorker(
                    store,
                    dispatcher,
                    policy,
                    timeProvider:
                        time);

            await worker.RunDueCycleAsync();
            time.Advance(
                TimeSpan.FromSeconds(1));
            var second =
                await worker.RunDueCycleAsync();

            Assert.Equal(
                1,
                second.Exhausted);
            Assert.Equal(
                2,
                dispatcher.Claims.Count);

            time.Advance(
                TimeSpan.FromMinutes(1));
            Assert.Equal(
                0,
                (await worker.RunDueCycleAsync())
                    .Visited);
            Assert.Equal(
                2,
                dispatcher.Claims.Count);

            var stored =
                await store.GetAsync(
                    id,
                    "ops-webhook");
            Assert.Equal(
                NotificationDeliveryState.Exhausted,
                stored!.Snapshot.State);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Stale_inflight_recovery_marks_unknown_without_dispatch()
    {
        var path = TempPath();

        try
        {
            var store = Store(path);
            await store.InitializeAsync();

            var created =
                new DateTimeOffset(
                    2026,
                    10,
                    7,
                    17,
                    0,
                    0,
                    TimeSpan.Zero);
            var id =
                Guid.NewGuid();
            var pending =
                await store.CreateOrGetAsync(
                    Pending(
                        id,
                        created),
                    created);

            var claimed =
                await store.ReplaceAsync(
                    new NotificationDeliverySnapshot(
                        id,
                        "ops-webhook",
                        Fingerprint,
                        NotificationDeliveryState.InFlight,
                        1,
                        created),
                    pending.Revision,
                    created.AddSeconds(1));
            Assert.NotNull(
                claimed);

            var time =
                new MutableTimeProvider(
                    created.AddMinutes(5));
            var dispatcher =
                new RecordingDispatcher(
                    NotificationDeliveryDispatchOutcome.Delivered);
            var worker =
                new NotificationDeliveryWorker(
                    store,
                    dispatcher,
                    workerPolicy:
                        new NotificationDeliveryWorkerPolicy(
                            staleInFlightAfter:
                                TimeSpan.FromMinutes(2)),
                    timeProvider:
                        time);

            var recovered =
                await worker.RecoverStaleInFlightAsync();

            Assert.Equal(
                1,
                recovered.MarkedUnknownExternalEffect);
            Assert.Empty(
                dispatcher.Claims);

            var stored =
                await store.GetAsync(
                    id,
                    "ops-webhook");
            Assert.Equal(
                NotificationDeliveryState.UnknownExternalEffect,
                stored!.Snapshot.State);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Cycle_due_limit_is_intersection_of_worker_and_rate_policy()
    {
        var path = TempPath();

        try
        {
            var store = Store(path);
            await store.InitializeAsync();

            var now =
                new DateTimeOffset(
                    2026,
                    10,
                    7,
                    18,
                    0,
                    0,
                    TimeSpan.Zero);

            for (var index = 0;
                 index < 6;
                 index++)
            {
                await store.CreateOrGetAsync(
                    Pending(
                        Guid.NewGuid(),
                        now),
                    now);
            }

            var dispatcher =
                new RecordingDispatcher(
                    Enumerable
                        .Repeat(
                            NotificationDeliveryDispatchOutcome.Delivered,
                            6)
                        .ToArray());
            var worker =
                new NotificationDeliveryWorker(
                    store,
                    dispatcher,
                    new NotificationDeliveryPolicy(
                        ratePerSecond: 2,
                        maxConcurrency: 1),
                    new NotificationDeliveryWorkerPolicy(
                        maxDuePerCycle: 5),
                    new MutableTimeProvider(
                        now));

            var cycle =
                await worker.RunDueCycleAsync();

            Assert.Equal(
                2,
                cycle.Visited);
            Assert.Equal(
                2,
                dispatcher.Claims.Count);
            Assert.True(
                cycle.MoreDue);
        }
        finally
        {
            Cleanup(path);
        }
    }

    private static AdoNotificationDeliveryStore Store(
        string path) =>
        new(
            new SqliteNotificationDeliveryDbConnectionFactory(
                path));

    private static NotificationDeliverySnapshot Pending(
        Guid id,
        DateTimeOffset createdAtUtc) =>
        new(
            id,
            "ops-webhook",
            Fingerprint,
            NotificationDeliveryState.Pending,
            0,
            createdAtUtc);

    private static string TempPath() =>
        Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w66-worker-{Guid.NewGuid():N}.db");

    private static void Cleanup(
        string path)
    {
        SqliteConnection.ClearAllPools();

        foreach (var candidate in
                 new[]
                 {
                     path,
                     path + "-wal",
                     path + "-shm",
                 })
        {
            if (File.Exists(candidate))
            {
                File.Delete(candidate);
            }
        }
    }

    private sealed class MutableTimeProvider :
        TimeProvider
    {
        private DateTimeOffset _utcNow;

        public MutableTimeProvider(
            DateTimeOffset utcNow)
        {
            _utcNow =
                utcNow.ToUniversalTime();
        }

        public override DateTimeOffset GetUtcNow() =>
            _utcNow;

        public void Advance(
            TimeSpan duration)
        {
            _utcNow +=
                duration;
        }
    }

    private sealed class RecordingDispatcher :
        INotificationDeliveryDispatcher
    {
        private readonly Queue<
            NotificationDeliveryDispatchOutcome>
            _outcomes;

        public RecordingDispatcher(
            params NotificationDeliveryDispatchOutcome[] outcomes)
        {
            _outcomes =
                new Queue<
                    NotificationDeliveryDispatchOutcome>(
                    outcomes);
        }

        public List<NotificationDeliveryRecord> Claims { get; } =
            [];

        public Task<NotificationDeliveryDispatchResult>
            DispatchAsync(
                NotificationDeliveryRecord claimedDelivery,
                CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            Assert.Equal(
                NotificationDeliveryState.InFlight,
                claimedDelivery.Snapshot.State);
            Claims.Add(
                claimedDelivery);

            var outcome =
                _outcomes.Count > 0
                    ? _outcomes.Dequeue()
                    : NotificationDeliveryDispatchOutcome.Delivered;

            return Task.FromResult(
                new NotificationDeliveryDispatchResult(
                    outcome));
        }
    }

    private sealed class ThrowingDispatcher :
        INotificationDeliveryDispatcher
    {
        public Task<NotificationDeliveryDispatchResult>
            DispatchAsync(
                NotificationDeliveryRecord claimedDelivery,
                CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            throw new InvalidOperationException(
                "simulated ambiguous dispatcher failure");
        }
    }
}
