using Kafdeck.Core.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaSnapshotCoordinatorTests
{
    [Fact]
    public async Task Concurrent_requests_for_same_key_are_single_flight()
    {
        var coordinator = new KafkaSnapshotCoordinator();
        var entered = NewSignal();
        var release = NewSignal();
        var calls = 0;

        async Task<KafkaResult<string>> Read(KafkaOperationContext _, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref calls);
            entered.TrySetResult(true);
            await release.Task.WaitAsync(cancellationToken);
            return Success("snapshot-value");
        }

        var tasks = Enumerable.Range(0, 32)
            .Select(_ => coordinator.ObserveAsync("cluster-a", "cluster-metadata", TimeSpan.FromSeconds(5), Read))
            .ToArray();

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(50);

        Assert.Equal(1, Volatile.Read(ref calls));

        release.TrySetResult(true);
        var results = await Task.WhenAll(tasks);

        Assert.All(results, result =>
        {
            Assert.True(result.IsSuccess);
            Assert.Equal("snapshot-value", result.Value);
        });
    }

    [Fact]
    public async Task Fresh_value_is_served_from_snapshot_without_another_Kafka_read()
    {
        var coordinator = new KafkaSnapshotCoordinator();
        var calls = 0;

        Task<KafkaResult<string>> Read(KafkaOperationContext _, CancellationToken __)
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(Success("value"));
        }

        var first = await coordinator.ObserveAsync(
            "cluster-a",
            "cluster-metadata",
            TimeSpan.FromSeconds(5),
            Read);

        var second = await coordinator.ObserveAsync(
            "cluster-a",
            "cluster-metadata",
            TimeSpan.FromSeconds(5),
            Read);

        Assert.Equal(1, calls);
        Assert.Equal(ObservationSource.Live, first.Observation.Source);
        Assert.Equal(ObservationSource.Snapshot, second.Observation.Source);
        Assert.Equal(first.Observation.ObservedAtUtc, second.Observation.ObservedAtUtc);
    }

    [Fact]
    public async Task Retryable_failure_is_retried_within_the_bounded_attempt_budget()
    {
        var coordinator = new KafkaSnapshotCoordinator();
        var calls = 0;

        Task<KafkaResult<string>> Read(KafkaOperationContext _, CancellationToken __)
        {
            var attempt = Interlocked.Increment(ref calls);
            return Task.FromResult(attempt < 3 ? RetryableFailure() : Success("recovered"));
        }

        var result = await coordinator.ObserveAsync(
            "cluster-a",
            "topics",
            TimeSpan.FromSeconds(5),
            Read);

        Assert.True(result.IsSuccess);
        Assert.Equal("recovered", result.Value);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Retryable_refresh_failure_can_serve_explicit_stale_snapshot()
    {
        var coordinator = new KafkaSnapshotCoordinator();
        var calls = 0;
        var ttl = TimeSpan.FromSeconds(1);

        Task<KafkaResult<string>> Read(KafkaOperationContext _, CancellationToken __)
        {
            var attempt = Interlocked.Increment(ref calls);
            return Task.FromResult(attempt == 1 ? Success("last-known-good") : RetryableFailure());
        }

        var initial = await coordinator.ObserveAsync("cluster-a", "topics", ttl, Read);
        Assert.Equal(ObservationSource.Live, initial.Observation.Source);

        await Task.Delay(TimeSpan.FromMilliseconds(1200));

        var stale = await coordinator.ObserveAsync("cluster-a", "topics", ttl, Read);

        Assert.True(stale.IsSuccess);
        Assert.Equal("last-known-good", stale.Value);
        Assert.Equal(ObservationSource.StaleSnapshot, stale.Observation.Source);
        Assert.Equal(initial.Observation.ObservedAtUtc, stale.Observation.ObservedAtUtc);
        Assert.Equal(4, calls);
    }

    [Fact]
    public async Task Per_cluster_bulkhead_caps_concurrent_Kafka_reads()
    {
        var policy = new KafkaSnapshotPolicy(perClusterConcurrency: 2, globalConcurrency: 4);
        var coordinator = new KafkaSnapshotCoordinator(policy);
        var release = NewSignal();
        var twoEntered = NewSignal();
        var active = 0;
        var started = 0;
        var maximumActive = 0;

        async Task<KafkaResult<string>> Read(KafkaOperationContext _, CancellationToken cancellationToken)
        {
            var current = Interlocked.Increment(ref active);
            UpdateMaximum(ref maximumActive, current);
            if (Interlocked.Increment(ref started) == 2)
                twoEntered.TrySetResult(true);

            try
            {
                await release.Task.WaitAsync(cancellationToken);
                return Success("ok");
            }
            finally
            {
                Interlocked.Decrement(ref active);
            }
        }

        var tasks = Enumerable.Range(0, 6)
            .Select(i => coordinator.ObserveAsync(
                "cluster-a",
                $"resource-{i}",
                TimeSpan.FromSeconds(5),
                Read))
            .ToArray();

        await twoEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(100);

        Assert.Equal(2, Volatile.Read(ref started));
        Assert.Equal(2, Volatile.Read(ref maximumActive));

        release.TrySetResult(true);
        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task Slow_cluster_does_not_consume_global_capacity_while_waiting_for_cluster_slot()
    {
        var policy = new KafkaSnapshotPolicy(perClusterConcurrency: 1, globalConcurrency: 2);
        var coordinator = new KafkaSnapshotCoordinator(policy);
        var clusterAFirstEntered = NewSignal();
        var releaseClusterA = NewSignal();
        var clusterBEntered = NewSignal();
        var releaseClusterB = NewSignal();
        var clusterASecondReadStarted = 0;

        async Task<KafkaResult<string>> ClusterAFirstRead(
            KafkaOperationContext _,
            CancellationToken cancellationToken)
        {
            clusterAFirstEntered.TrySetResult(true);
            await releaseClusterA.Task.WaitAsync(cancellationToken);
            return Success("a1");
        }

        Task<KafkaResult<string>> ClusterASecondRead(
            KafkaOperationContext _,
            CancellationToken __)
        {
            Interlocked.Exchange(ref clusterASecondReadStarted, 1);
            return Task.FromResult(Success("a2"));
        }

        async Task<KafkaResult<string>> ClusterBRead(
            KafkaOperationContext _,
            CancellationToken cancellationToken)
        {
            clusterBEntered.TrySetResult(true);
            await releaseClusterB.Task.WaitAsync(cancellationToken);
            return Success("b1");
        }

        var a1 = coordinator.ObserveAsync(
            "cluster-a",
            "resource-a1",
            TimeSpan.FromSeconds(5),
            ClusterAFirstRead);

        await clusterAFirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var a2 = coordinator.ObserveAsync(
            "cluster-a",
            "resource-a2",
            TimeSpan.FromSeconds(5),
            ClusterASecondRead);

        await Task.Delay(50);
        Assert.Equal(0, Volatile.Read(ref clusterASecondReadStarted));

        var b1 = coordinator.ObserveAsync(
            "cluster-b",
            "resource-b1",
            TimeSpan.FromSeconds(5),
            ClusterBRead);

        await clusterBEntered.Task.WaitAsync(TimeSpan.FromSeconds(1));

        releaseClusterB.TrySetResult(true);
        releaseClusterA.TrySetResult(true);

        await Task.WhenAll(a1, a2, b1);
        Assert.Equal(1, Volatile.Read(ref clusterASecondReadStarted));
    }

    [Fact]
    public async Task Caller_cancellation_is_propagated_to_the_inflight_read()
    {
        var coordinator = new KafkaSnapshotCoordinator();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        async Task<KafkaResult<string>> Read(KafkaOperationContext _, CancellationToken cancellationToken)
        {
            await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            return Success("never");
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            coordinator.ObserveAsync(
                "cluster-a",
                "topics",
                TimeSpan.FromSeconds(5),
                Read,
                cancellation.Token));
    }

    [Fact]
    public void Accepted_policy_defaults_and_bounds_are_enforced()
    {
        var policy = new KafkaSnapshotPolicy();

        Assert.Equal(8, policy.PerClusterConcurrency);
        Assert.Equal(64, policy.GlobalConcurrency);
        Assert.Equal(TimeSpan.FromSeconds(10), policy.OperationDeadline);
        Assert.Equal(TimeSpan.FromSeconds(5), policy.ClusterMetadataTtl);
        Assert.Equal(TimeSpan.FromSeconds(10), policy.TopicMetadataTtl);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new KafkaSnapshotPolicy(perClusterConcurrency: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new KafkaSnapshotPolicy(perClusterConcurrency: 8, globalConcurrency: 4));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new KafkaSnapshotPolicy(operationDeadline: TimeSpan.FromMilliseconds(500)));
    }

    private static KafkaResult<string> Success(string value)
    {
        var now = DateTimeOffset.UtcNow;
        return KafkaResult<string>.Success(
            value,
            new ObservationMetadata(now, now, now, ObservationSource.Live));
    }

    private static KafkaResult<string> RetryableFailure()
    {
        var now = DateTimeOffset.UtcNow;
        return KafkaResult<string>.Failed(
            new KafkaFailure(
                KafkaFailureCategory.Unavailable,
                "test_unavailable",
                "Kafka is temporarily unavailable.",
                IsRetryable: true),
            new ObservationMetadata(now, now, now, ObservationSource.Live));
    }

    private static TaskCompletionSource<bool> NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static void UpdateMaximum(ref int maximum, int candidate)
    {
        while (true)
        {
            var snapshot = Volatile.Read(ref maximum);
            if (snapshot >= candidate)
                return;

            if (Interlocked.CompareExchange(ref maximum, candidate, snapshot) == snapshot)
                return;
        }
    }
}
