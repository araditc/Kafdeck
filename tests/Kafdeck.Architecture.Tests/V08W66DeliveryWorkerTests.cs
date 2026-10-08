using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Npgsql;
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

    [Fact]
    public async Task Rate_limit_is_enforced_across_back_to_back_cycles()
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
                    19,
                    0,
                    0,
                    TimeSpan.Zero);
            for (var index = 0;
                 index < 4;
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
                    NotificationDeliveryDispatchOutcome.Delivered,
                    NotificationDeliveryDispatchOutcome.Delivered,
                    NotificationDeliveryDispatchOutcome.Delivered,
                    NotificationDeliveryDispatchOutcome.Delivered);
            var time =
                new MutableTimeProvider(
                    now);
            var worker =
                new NotificationDeliveryWorker(
                    store,
                    dispatcher,
                    new NotificationDeliveryPolicy(
                        ratePerSecond: 2,
                        maxConcurrency: 2),
                    new NotificationDeliveryWorkerPolicy(
                        maxDuePerCycle: 4),
                    time);

            var first =
                await worker.RunDueCycleAsync();
            Assert.Equal(
                2,
                first.Delivered);
            Assert.Equal(
                2,
                dispatcher.Claims.Count);

            var immediate =
                await worker.RunDueCycleAsync();
            Assert.Equal(
                2,
                immediate.AdmissionDeferred);
            Assert.Equal(
                2,
                dispatcher.Claims.Count);

            time.Advance(
                TimeSpan.FromSeconds(1));

            var afterWindow =
                await worker.RunDueCycleAsync();
            Assert.Equal(
                2,
                afterWindow.Delivered);
            Assert.Equal(
                4,
                dispatcher.Claims.Count);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Concurrency_limit_is_shared_across_worker_instances()
    {
        var path = TempPath();

        try
        {
            var storeA = Store(path);
            var storeB = Store(path);
            await storeA.InitializeAsync();
            await storeB.InitializeAsync();

            var now =
                new DateTimeOffset(
                    2026,
                    10,
                    7,
                    20,
                    0,
                    0,
                    TimeSpan.Zero);
            await storeA.CreateOrGetAsync(
                Pending(
                    Guid.NewGuid(),
                    now),
                now);
            await storeA.CreateOrGetAsync(
                Pending(
                    Guid.NewGuid(),
                    now),
                now);

            var blocking =
                new BlockingDispatcher();
            var policy =
                new NotificationDeliveryPolicy(
                    ratePerSecond: 10,
                    maxConcurrency: 1);
            var time =
                new MutableTimeProvider(
                    now);

            var workerA =
                new NotificationDeliveryWorker(
                    storeA,
                    blocking,
                    policy,
                    new NotificationDeliveryWorkerPolicy(
                        maxDuePerCycle: 1),
                    time);
            var workerB =
                new NotificationDeliveryWorker(
                    storeB,
                    blocking,
                    policy,
                    new NotificationDeliveryWorkerPolicy(
                        maxDuePerCycle: 1),
                    time);

            var firstCycle =
                workerA.RunDueCycleAsync();

            await blocking.FirstDispatchStarted;

            var secondCycle =
                await workerB.RunDueCycleAsync();

            Assert.True(
                secondCycle.AdmissionDeferred >= 1);
            Assert.Equal(
                1,
                blocking.ActiveDispatches);
            Assert.Equal(
                1,
                blocking.MaxObservedActiveDispatches);

            blocking.Release();
            var first =
                await firstCycle;

            Assert.Equal(
                1,
                first.Delivered);
            Assert.Equal(
                1,
                blocking.MaxObservedActiveDispatches);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task PostgreSql_admission_is_shared_across_replicas_when_available()
    {
        var baseConnectionString =
            Environment.GetEnvironmentVariable(
                "KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(
                baseConnectionString))
        {
            return;
        }

        var schema =
            $"w66_worker_{Guid.NewGuid():N}";
        var adminBuilder =
            new NpgsqlConnectionStringBuilder(
                baseConnectionString)
            {
                Pooling = false,
            };
        await using var admin =
            new NpgsqlConnection(
                adminBuilder.ConnectionString);
        await admin.OpenAsync();

        try
        {
            await using (var create =
                         admin.CreateCommand())
            {
                create.CommandText =
                    $"CREATE SCHEMA \"{schema}\"";
                await create.ExecuteNonQueryAsync();
            }

            var scoped =
                new NpgsqlConnectionStringBuilder(
                    baseConnectionString)
                {
                    SearchPath = schema,
                    Pooling = false,
                };
            var storeA =
                new AdoNotificationDeliveryStore(
                    new PostgreSqlNotificationDeliveryDbConnectionFactory(
                        scoped.ConnectionString));
            var storeB =
                new AdoNotificationDeliveryStore(
                    new PostgreSqlNotificationDeliveryDbConnectionFactory(
                        scoped.ConnectionString));

            await storeA.InitializeAsync();
            await storeB.InitializeAsync();

            var now =
                new DateTimeOffset(
                    2026,
                    10,
                    7,
                    21,
                    0,
                    0,
                    TimeSpan.Zero);
            await storeA.CreateOrGetAsync(
                Pending(
                    Guid.NewGuid(),
                    now),
                now);
            await storeA.CreateOrGetAsync(
                Pending(
                    Guid.NewGuid(),
                    now),
                now);

            var timeA =
                new MutableTimeProvider(
                    now);
            var timeB =
                new MutableTimeProvider(
                    now.AddSeconds(30));
            var dispatcher =
                new BlockingDispatcher();
            var policy =
                new NotificationDeliveryPolicy(
                    ratePerSecond: 1,
                    maxConcurrency: 1);
            var workerA =
                new NotificationDeliveryWorker(
                    storeA,
                    dispatcher,
                    policy,
                    new NotificationDeliveryWorkerPolicy(
                        maxDuePerCycle: 2),
                    timeA);
            var workerB =
                new NotificationDeliveryWorker(
                    storeB,
                    dispatcher,
                    policy,
                    new NotificationDeliveryWorkerPolicy(
                        maxDuePerCycle: 2),
                    timeB);

            var firstCycle =
                workerA.RunDueCycleAsync();
            await dispatcher.FirstDispatchStarted;

            var concurrent =
                await workerB.RunDueCycleAsync();

            Assert.Equal(
                1,
                concurrent.AdmissionDeferred);
            Assert.Equal(
                1,
                dispatcher.ActiveDispatches);

            dispatcher.Release();
            Assert.Equal(
                1,
                (await firstCycle).Delivered);

            var sameWindow =
                await workerB.RunDueCycleAsync();

            Assert.Equal(
                1,
                sameWindow.AdmissionDeferred);
            Assert.Equal(
                1,
                dispatcher.DispatchCount);

            await Task.Delay(
                TimeSpan.FromMilliseconds(1100));

            var afterWindow =
                await workerB.RunDueCycleAsync();

            Assert.Equal(
                1,
                afterWindow.Delivered);
            Assert.Equal(
                2,
                dispatcher.DispatchCount);
            Assert.Equal(
                1,
                dispatcher.MaxObservedActiveDispatches);
        }
        finally
        {
            await using var drop =
                admin.CreateCommand();
            drop.CommandText =
                $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
            await drop.ExecuteNonQueryAsync();
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

    private sealed class BlockingDispatcher :
        INotificationDeliveryDispatcher
    {
        private readonly TaskCompletionSource
            _started =
                new(
                    TaskCreationOptions
                        .RunContinuationsAsynchronously);
        private readonly TaskCompletionSource
            _release =
                new(
                    TaskCreationOptions
                        .RunContinuationsAsynchronously);
        private int _active;
        private int _maxActive;
        private int _dispatchCount;

        public Task FirstDispatchStarted =>
            _started.Task;

        public int ActiveDispatches =>
            Volatile.Read(
                ref _active);

        public int MaxObservedActiveDispatches =>
            Volatile.Read(
                ref _maxActive);

        public int DispatchCount =>
            Volatile.Read(
                ref _dispatchCount);

        public void Release() =>
            _release.TrySetResult();

        public async Task<NotificationDeliveryDispatchResult>
            DispatchAsync(
                NotificationDeliveryRecord claimedDelivery,
                CancellationToken cancellationToken)
        {
            Interlocked.Increment(
                ref _dispatchCount);
            var active =
                Interlocked.Increment(
                    ref _active);
            UpdateMax(
                active);
            _started.TrySetResult();

            try
            {
                await _release.Task
                    .WaitAsync(cancellationToken);
                return new NotificationDeliveryDispatchResult(
                    NotificationDeliveryDispatchOutcome.Delivered);
            }
            finally
            {
                Interlocked.Decrement(
                    ref _active);
            }
        }

        private void UpdateMax(
            int value)
        {
            while (true)
            {
                var current =
                    Volatile.Read(
                        ref _maxActive);
                if (value <= current ||
                    Interlocked.CompareExchange(
                        ref _maxActive,
                        value,
                        current) ==
                    current)
                {
                    return;
                }
            }
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
