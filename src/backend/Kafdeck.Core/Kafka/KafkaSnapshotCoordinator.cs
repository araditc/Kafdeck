using System.Collections.Concurrent;

namespace Kafdeck.Core.Kafka;

/// <summary>
/// Coordinates bounded, single-flight read observations without exposing provider-specific types.
/// </summary>
public sealed class KafkaSnapshotCoordinator
{
    private const int SnapshotSweepInterval = 256;
    private readonly KafkaSnapshotPolicy _policy;
    private readonly TimeProvider _timeProvider;
    private readonly SemaphoreSlim _globalBulkhead;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _clusterBulkheads = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<SnapshotKey, SnapshotEntry> _snapshots = new();
    private readonly ConcurrentDictionary<SnapshotKey, Lazy<Task<object>>> _refreshes = new();
    private int _observationsSinceSweep;

    public KafkaSnapshotCoordinator(
        KafkaSnapshotPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _policy = policy ?? new KafkaSnapshotPolicy();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _globalBulkhead = new SemaphoreSlim(_policy.GlobalConcurrency, _policy.GlobalConcurrency);
    }

    public async Task<KafkaResult<T>> ObserveAsync<T>(
        string clusterId,
        string resourceKey,
        TimeSpan ttl,
        Func<KafkaOperationContext, CancellationToken, Task<KafkaResult<T>>> read,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(resourceKey);
        ArgumentNullException.ThrowIfNull(read);
        if (ttl <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(ttl));

        var now = _timeProvider.GetUtcNow();
        SweepExpiredSnapshotsIfDue(now);
        var key = new SnapshotKey(clusterId, resourceKey, typeof(T));
        if (TryGetFresh(key, now, out KafkaResult<T> fresh)) return fresh;

        var refresh = _refreshes.GetOrAdd(key, _ => new Lazy<Task<object>>(
            () => RefreshAsync(key, clusterId, ttl, read, cancellationToken),
            LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return (KafkaResult<T>)await refresh.Value.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (refresh.IsValueCreated && refresh.Value.IsCompleted)
                _refreshes.TryRemove(new KeyValuePair<SnapshotKey, Lazy<Task<object>>>(key, refresh));
        }
    }

    private void SweepExpiredSnapshotsIfDue(DateTimeOffset now)
    {
        if (Interlocked.Increment(ref _observationsSinceSweep) < SnapshotSweepInterval)
            return;

        if (Interlocked.Exchange(ref _observationsSinceSweep, 0) < SnapshotSweepInterval)
            return;

        foreach (var snapshot in _snapshots)
        {
            if (snapshot.Value.StaleAfterUtc < now)
                _snapshots.TryRemove(snapshot);
        }
    }

    private bool TryGetFresh<T>(SnapshotKey key, DateTimeOffset now, out KafkaResult<T> result)
    {
        if (_snapshots.TryGetValue(key, out var entry) && entry.FreshUntilUtc >= now)
        {
            result = KafkaResult<T>.Success((T)entry.Value, new ObservationMetadata(
                entry.ObservedAtUtc, entry.FreshUntilUtc, entry.StaleAfterUtc, ObservationSource.Snapshot));
            return true;
        }

        result = null!;
        return false;
    }

    private async Task<object> RefreshAsync<T>(
        SnapshotKey key,
        string clusterId,
        TimeSpan ttl,
        Func<KafkaOperationContext, CancellationToken, Task<KafkaResult<T>>> read,
        CancellationToken callerCancellation)
    {
        var clusterBulkhead = _clusterBulkheads.GetOrAdd(clusterId, _ =>
            new SemaphoreSlim(_policy.PerClusterConcurrency, _policy.PerClusterConcurrency));

        var deadlineAtUtc = _timeProvider.GetUtcNow() + _policy.OperationDeadline;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(callerCancellation);
        deadline.CancelAfter(_policy.OperationDeadline);

        // Acquire the per-cluster bulkhead before the global bulkhead. A slow cluster must
        // never consume global capacity while merely waiting for its own cluster slot.
        await clusterBulkhead.WaitAsync(deadline.Token).ConfigureAwait(false);
        try
        {
            await _globalBulkhead.WaitAsync(deadline.Token).ConfigureAwait(false);
            try
            {
                KafkaResult<T>? live = null;
                for (var attempt = 0; attempt < 3; attempt++)
                {
                    var operation = new KafkaOperationContext(deadlineAtUtc);
                    live = await read(operation, deadline.Token).ConfigureAwait(false);
                    if (live.IsSuccess || live.Failure?.IsRetryable != true || attempt == 2)
                        break;

                    var exponentialMilliseconds = 50 * (1 << attempt);
                    var jitterMilliseconds = Random.Shared.Next(0, 26);
                    await Task.Delay(TimeSpan.FromMilliseconds(exponentialMilliseconds + jitterMilliseconds), deadline.Token)
                        .ConfigureAwait(false);
                }

                if (live is null)
                    throw new InvalidOperationException("Snapshot refresh completed without a Kafka result.");

                if (live.IsSuccess && live.Value is not null)
                {
                    var observedAt = live.Observation.ObservedAtUtc;
                    var freshUntil = observedAt + ttl;
                    var staleAfter = freshUntil + ttl;
                    _snapshots[key] = new SnapshotEntry(live.Value, observedAt, freshUntil, staleAfter);
                    return KafkaResult<T>.Success(live.Value, new ObservationMetadata(
                        observedAt, freshUntil, staleAfter, ObservationSource.Live));
                }

                if (live.Failure?.IsRetryable == true && TryGetStale(key, _timeProvider.GetUtcNow(), out KafkaResult<T> stale))
                    return stale;

                return live;
            }
            finally
            {
                _globalBulkhead.Release();
            }
        }
        finally
        {
            clusterBulkhead.Release();
        }
    }

    private bool TryGetStale<T>(SnapshotKey key, DateTimeOffset now, out KafkaResult<T> result)
    {
        if (_snapshots.TryGetValue(key, out var entry) && entry.FreshUntilUtc < now && entry.StaleAfterUtc >= now)
        {
            result = KafkaResult<T>.Success((T)entry.Value, new ObservationMetadata(
                entry.ObservedAtUtc, entry.FreshUntilUtc, entry.StaleAfterUtc, ObservationSource.StaleSnapshot));
            return true;
        }

        result = null!;
        return false;
    }

    private readonly record struct SnapshotKey(string ClusterId, string ResourceKey, Type ValueType);
    private sealed record SnapshotEntry(object Value, DateTimeOffset ObservedAtUtc, DateTimeOffset FreshUntilUtc, DateTimeOffset StaleAfterUtc);
}
