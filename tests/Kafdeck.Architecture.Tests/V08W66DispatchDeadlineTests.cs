using System.Collections.Concurrent;
using System.Net;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66DispatchDeadlineTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Stalled_dns_has_finite_dispatch_deadline_and_next_cycle_serves_healthy_destination(bool cooperative)
    {
        await using var fixture = await Fixture.CreateAsync(cooperative);
        var stalled = await fixture.SeedAsync("stall-one", DateTimeOffset.UtcNow.AddSeconds(-2));
        var healthy = await fixture.SeedAsync("healthy", DateTimeOffset.UtcNow.AddSeconds(-1));

        var first = await fixture.Worker.RunDueCycleAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, first.UnknownExternalEffect);
        var unknown = await fixture.Store.GetAsync(stalled, "stall-one");
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, unknown!.Snapshot.State);
        Assert.Equal(1, unknown.Snapshot.AttemptCount);
        Assert.True(fixture.Lookup.ObservedCancellation);

        var second = await fixture.Worker.RunDueCycleAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, second.Delivered);
        Assert.Equal(NotificationDeliveryState.Delivered,
            (await fixture.Store.GetAsync(healthy, "healthy"))!.Snapshot.State);
        fixture.Lookup.Release();
        await fixture.Dispatcher.Completion(stalled).WaitAsync(TimeSpan.FromSeconds(5));
        await fixture.Worker.RunDueCycleAsync();
        Assert.Equal(1, fixture.Transport.Sends);
        Assert.Equal("healthy", Assert.Single(fixture.Transport.Destinations));
        var persisted = await fixture.Store.GetAsync(stalled, "stall-one");
        Assert.Equal(unknown.Revision, persisted!.Revision);
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, persisted.Snapshot.State);
        Assert.Equal(1, fixture.Lookup.StalledCalls);
    }

    [Fact]
    public async Task Noncooperative_dns_retains_finite_admission_until_actual_completion()
    {
        // Configured delivery concurrency includes timed-out background tasks.
        await using var fixture = await Fixture.CreateAsync(cooperative: false);
        var firstId = await fixture.SeedAsync("stall-one", DateTimeOffset.UtcNow.AddSeconds(-3));
        var secondId = await fixture.SeedAsync("stall-two", DateTimeOffset.UtcNow.AddSeconds(-2));
        var healthy = await fixture.SeedAsync("healthy", DateTimeOffset.UtcNow.AddSeconds(-1));
        Assert.Equal(1, (await fixture.Worker.RunDueCycleAsync().WaitAsync(TimeSpan.FromSeconds(5))).UnknownExternalEffect);
        Assert.Equal(1, (await fixture.Worker.RunDueCycleAsync().WaitAsync(TimeSpan.FromSeconds(5))).UnknownExternalEffect);

        for (var cycle = 0; cycle < 10; cycle++)
            Assert.Equal(1, (await fixture.Worker.RunDueCycleAsync()).AdmissionDeferred);
        Assert.Equal(2, fixture.Lookup.StalledCalls);
        Assert.Equal(2, fixture.Dispatcher.Calls);
        var waiting = await fixture.Store.GetAsync(healthy, "healthy");
        Assert.Equal(NotificationDeliveryState.Pending, waiting!.Snapshot.State);
        Assert.Equal(0, waiting.Snapshot.AttemptCount);

        fixture.Lookup.Release();
        await Task.WhenAll(fixture.Dispatcher.Completion(firstId), fixture.Dispatcher.Completion(secondId))
            .WaitAsync(TimeSpan.FromSeconds(5));
        using var completionTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while ((await fixture.Worker.RunDueCycleAsync(completionTimeout.Token)).Delivered == 0)
            await Task.Delay(10, completionTimeout.Token);
        Assert.Equal(1, fixture.Transport.Sends);
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect,
            (await fixture.Store.GetAsync(firstId, "stall-one"))!.Snapshot.State);
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect,
            (await fixture.Store.GetAsync(secondId, "stall-two"))!.Snapshot.State);
    }

    [Fact]
    public void Dispatch_policy_has_finite_hard_bounds_before_stale_recovery()
    {
        Assert.Equal(TimeSpan.FromSeconds(20), NotificationDeliveryWorkerPolicy.Default.DispatchTimeout);
        Assert.Throws<ArgumentOutOfRangeException>(() => new NotificationDeliveryWorkerPolicy(dispatchTimeout: Timeout.InfiniteTimeSpan));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NotificationDeliveryWorkerPolicy(dispatchTimeout: TimeSpan.FromMinutes(2)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NotificationDeliveryWorkerPolicy(
            staleInFlightAfter: TimeSpan.FromSeconds(10), dispatchTimeout: TimeSpan.FromSeconds(10)));
        var shortRecovery = new NotificationDeliveryWorkerPolicy(staleInFlightAfter: TimeSpan.FromSeconds(10));
        Assert.True(shortRecovery.DispatchTimeout < shortRecovery.StaleInFlightAfter);
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"kafdeck-w66-deadline-{Guid.NewGuid():N}.db");
        private readonly AdoNotificationRoutingStore _routing;
        private readonly Dictionary<string, NotificationDestinationProfile> _profiles;
        public AdoNotificationDeliveryStore Store { get; }
        public StallingLookup Lookup { get; }
        public RecordingTransport Transport { get; } = new();
        public TrackedDispatcher Dispatcher { get; }
        public NotificationDeliveryWorker Worker { get; }

        private Fixture(bool cooperative)
        {
            var connections = new SqliteNotificationDeliveryDbConnectionFactory(_path);
            Store = new AdoNotificationDeliveryStore(connections);
            _routing = new AdoNotificationRoutingStore(connections);
            _profiles = new[] { "stall-one", "stall-two", "healthy" }.ToDictionary(id => id,
                id => new NotificationDestinationProfile(id, NotificationProviderKind.Webhook, id,
                    [NotificationEventClass.Operational], new Uri($"https://{id}.example.org/notify")));
            Lookup = new StallingLookup(cooperative);
            var resolver = new NotificationDnsEndpointResolver(Lookup, NotificationAddressPolicy.NoConfiguredNat64);
            var adapter = new WebhookNotificationAdapter(resolver, new NoCredentials(), Transport);
            Dispatcher = new TrackedDispatcher(new WebhookNotificationDeliveryDispatcher(_routing,
                new ConfiguredNotificationDestinationProfileCatalog(_profiles.Values), adapter));
            Worker = new NotificationDeliveryWorker(Store, Dispatcher, new NotificationDeliveryPolicy(maxConcurrency: 2),
                workerPolicy: new NotificationDeliveryWorkerPolicy(maxDuePerCycle: 1,
                    dispatchTimeout: TimeSpan.FromMilliseconds(200)));
        }

        public static async Task<Fixture> CreateAsync(bool cooperative)
        {
            var fixture = new Fixture(cooperative);
            await fixture.Store.InitializeAsync();
            await fixture._routing.InitializeAsync();
            return fixture;
        }

        public async Task<Guid> SeedAsync(string destination, DateTimeOffset created)
        {
            var notification = NotificationSafeEvent.Approved(Guid.NewGuid(),
                NotificationApprovedEventKind.ConsumerLagAlert, created);
            await _routing.CreateOrGetEventAsync(notification, created);
            await Store.CreateOrGetAsync(new NotificationDeliverySnapshot(notification.EventId, destination,
                notification.PayloadFingerprint, NotificationDeliveryState.Pending, 0, created,
                routedProfileRevisionFingerprint: _profiles[destination].RevisionFingerprint), created);
            return notification.EventId;
        }

        public ValueTask DisposeAsync()
        {
            Lookup.Release();
            SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-shm", "-wal" }) File.Delete(_path + suffix);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class StallingLookup(bool cooperative) : INotificationHostAddressLookup
    {
        private readonly TaskCompletionSource<IReadOnlyList<IPAddress>> _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _stalledCalls;
        private readonly ConcurrentQueue<CancellationToken> _tokens = new();
        public int StalledCalls => Volatile.Read(ref _stalledCalls);
        public bool ObservedCancellation => _tokens.Any(token => token.IsCancellationRequested);
        public Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(string hostname, CancellationToken cancellationToken)
        {
            if (!hostname.StartsWith("stall-", StringComparison.Ordinal))
                return Task.FromResult<IReadOnlyList<IPAddress>>([IPAddress.Parse("1.1.1.1")]);
            Interlocked.Increment(ref _stalledCalls);
            _tokens.Enqueue(cancellationToken);
            return cooperative ? _release.Task.WaitAsync(cancellationToken) : _release.Task;
        }
        public void Release() => _release.TrySetResult([IPAddress.Parse("1.1.1.1")]);
    }

    private sealed class TrackedDispatcher(INotificationDeliveryDispatcher inner) : INotificationDeliveryDispatcher
    {
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource> _completion = new();
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);
        public Task Completion(Guid id) => _completion.GetOrAdd(id,
            _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        public async Task<NotificationDeliveryDispatchResult> DispatchAsync(NotificationDeliveryRecord claimedDelivery,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _calls);
            try { return await inner.DispatchAsync(claimedDelivery, cancellationToken); }
            finally
            {
                _completion.GetOrAdd(claimedDelivery.Snapshot.NotificationId,
                    _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
            }
        }
    }

    private sealed class RecordingTransport : INotificationPinnedWebhookTransport
    {
        private int _sends;
        public int Sends => Volatile.Read(ref _sends);
        public ConcurrentQueue<string> Destinations { get; } = new();
        public Task<NotificationWebhookTransportResult> SendAsync(NotificationPinnedWebhookRequest request,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Interlocked.Increment(ref _sends);
            Destinations.Enqueue(request.DestinationId);
            return Task.FromResult(new NotificationWebhookTransportResult(NotificationWebhookTransportOutcome.Delivered, "http_receipt_ack"));
        }
    }

    private sealed class NoCredentials : INotificationCredentialResolver
    {
        public ValueTask<NotificationCredentialValue> ResolveAsync(NotificationCredentialResolutionRequest request,
            CancellationToken cancellationToken) => throw new InvalidOperationException("No credential admission in deadline fixture.");
    }
}
