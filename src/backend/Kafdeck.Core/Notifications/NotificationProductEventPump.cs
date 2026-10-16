namespace Kafdeck.Core.Notifications;

public sealed record NotificationProductEventPolicy
{
    public const int HardMaxPending = 10_000;
    public const int HardMaxBatch = 50;
    public static readonly TimeSpan HardMaxRetention = TimeSpan.FromHours(24);

    public NotificationProductEventPolicy(int maxPending = HardMaxPending, int maxBatch = HardMaxBatch,
        TimeSpan? retention = null, TimeSpan? cycleTimeout = null, int maxPendingDeliveries = HardMaxPending)
    {
        var actualRetention = retention ?? HardMaxRetention;
        var actualTimeout = cycleTimeout ?? TimeSpan.FromSeconds(5);
        if (maxPending is < 1 or > HardMaxPending || maxBatch is < 1 or > HardMaxBatch ||
            maxPendingDeliveries is < NotificationRoutingCoordinator.HardMaxSubscriptionsPerRoute or > HardMaxPending ||
            actualRetention < TimeSpan.FromSeconds(1) || actualRetention > HardMaxRetention ||
            actualTimeout < TimeSpan.FromMilliseconds(100) || actualTimeout > TimeSpan.FromSeconds(30))
            throw new ArgumentOutOfRangeException(nameof(maxPending), "Product notification source policy exceeds finite bounds.");
        MaxPending = maxPending;
        MaxBatch = maxBatch;
        Retention = actualRetention;
        CycleTimeout = actualTimeout;
        MaxPendingDeliveries = maxPendingDeliveries;
    }

    public int MaxPending { get; }
    public int MaxBatch { get; }
    public TimeSpan Retention { get; }
    public TimeSpan CycleTimeout { get; }
    public int MaxPendingDeliveries { get; }
}

public sealed record NotificationProductEvent(Guid EventId, DateTimeOffset OccurredAtUtc)
{
    public NotificationSafeEvent Project() => NotificationSafeEvent.Approved(EventId,
        NotificationApprovedEventKind.MutationStateChanged, OccurredAtUtc);
}

public sealed record NotificationProductEventSourceStatus(int Pending, long Routed, long Expired,
    long Overflowed, DateTimeOffset? LastLossAtUtc, int ExpiredPending = 0);

public interface INotificationProductEventOutbox
{
    Task<IReadOnlyList<NotificationProductEvent>> ReadPendingAsync(int maxResults, CancellationToken cancellationToken = default);
    Task<bool> AcknowledgeAsync(Guid eventId, CancellationToken cancellationToken = default);
    Task<NotificationProductEventSourceStatus> GetStatusAsync(CancellationToken cancellationToken = default);
}

public interface INotificationProductEventBackpressure
{
    Task<bool> CanRouteAsync(int maximumFanout, CancellationToken cancellationToken = default);
}

public sealed record NotificationProductEventCycleResult(int Visited, int Acknowledged, int Unroutable,
    bool DeadlineReached);

// Reads durable server-owned product events. No request, subject, payload, URL or
// principal from a caller can be supplied to this pump.
public sealed class NotificationProductEventPump(INotificationProductEventOutbox outbox,
    NotificationRoutingCoordinator routing, NotificationProductEventPolicy policy,
    INotificationProductEventBackpressure backpressure, TimeProvider? timeProvider = null)
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    public async Task<NotificationProductEventCycleResult> RunCycleAsync(CancellationToken cancellationToken = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(policy.CycleTimeout);
        var visited = 0;
        var acknowledged = 0;
        var unroutable = 0;
        try
        {
            var batch = await outbox.ReadPendingAsync(policy.MaxBatch, deadline.Token).ConfigureAwait(false);
            if (batch.Count > policy.MaxBatch) throw new InvalidOperationException("Product source batch exceeds its hard bound.");
            foreach (var productEvent in batch)
            {
                deadline.Token.ThrowIfCancellationRequested();
                if (!await backpressure.CanRouteAsync(NotificationRoutingCoordinator.HardMaxSubscriptionsPerRoute,
                        deadline.Token).ConfigureAwait(false)) { unroutable++; break; }
                visited++;
                var now = _time.GetUtcNow();
                // Clock-skewed future evidence is retained until comparable or
                // explicitly expired by the source; never invent an earlier event.
                if (productEvent.OccurredAtUtc > now) { unroutable++; continue; }
                var result = await routing.RouteAsync(productEvent.Project(), now, deadline.Token).ConfigureAwait(false);
                if (result.Truncated || result.MissingDestinations != 0 || result.ProfileEventMismatches != 0)
                {
                    unroutable++;
                    continue;
                }
                // Cross-database route and acknowledgement cannot be atomic.
                // Replaying after a crash retains the exact stable event identity
                // and durable router/ledger idempotency; never acknowledge first.
                if (await outbox.AcknowledgeAsync(productEvent.EventId, deadline.Token).ConfigureAwait(false)) acknowledged++;
            }
            return new(visited, acknowledged, unroutable, false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new(visited, acknowledged, unroutable, true);
        }
    }
}
