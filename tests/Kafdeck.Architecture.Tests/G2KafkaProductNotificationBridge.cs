using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using TlsProvider = Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;

namespace Kafdeck.Architecture.Tests;

// The actual API captures/routes the real Kafka mutation, with sender off.
// A second production sender host consumes that same durable queue after the
// API process stops. Only the test dialer maps an approved public pin locally.
internal sealed class G2KafkaProductNotificationBridge : IAsyncDisposable
{
    private readonly TlsProvider _tls = new();
    private readonly string _path;
    private readonly NotificationDestinationProfile _profile;
    private readonly SqliteNotificationDeliveryDbConnectionFactory _connections;
    private readonly AdoNotificationRoutingStore _routing;
    private readonly AdoNotificationDeliveryStore _deliveries;

    private G2KafkaProductNotificationBridge(string directory)
    {
        _path = Path.GetFullPath(Path.Combine(directory, "product-notifications-" + Guid.NewGuid().ToString("N") + ".db"));
        _profile = new("g2-product-effect", NotificationProviderKind.Webhook, "Real product effect fixture",
            [NotificationEventClass.Mutation], new Uri($"https://hooks.example.org:{_tls.Port}/notify"));
        _connections = new(_path);
        _routing = new(_connections);
        _deliveries = new(_connections);
    }

    internal static async Task<G2KafkaProductNotificationBridge> PrepareAsync(string directory,
        IDictionary<string, string> configuration)
    {
        var bridge = new G2KafkaProductNotificationBridge(directory);
        try
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await bridge._routing.InitializeAsync(deadline.Token);
            await bridge._deliveries.InitializeAsync(deadline.Token);
            await bridge._routing.CreateSubscriptionAsync(new NotificationSubscriptionDefinition("g2-product-effect-sub",
                bridge._profile.DestinationId, [NotificationEventClass.Mutation], ["mutation.state-changed"]),
                NotificationSubscriptionState.Active, DateTimeOffset.UtcNow, deadline.Token);
            configuration["Kafdeck:Notifications:Enabled"] = "true";
            configuration["Kafdeck:Notifications:Provider"] = "Sqlite";
            configuration["Kafdeck:Notifications:ExecutionMode"] = "Standalone";
            configuration["Kafdeck:Notifications:SqliteDatabasePath"] = bridge._path;
            configuration["Kafdeck:Notifications:ProductEventsEnabled"] = "true";
            configuration["Kafdeck:Notifications:WebhookDeliveryEnabled"] = "false";
            configuration["Kafdeck:Notifications:HttpProviderDeliveryEnabled"] = "false";
            configuration["Kafdeck:Notifications:EmailDeliveryEnabled"] = "false";
            configuration["Kafdeck:Notifications:Destinations:0:DestinationId"] = bridge._profile.DestinationId;
            configuration["Kafdeck:Notifications:Destinations:0:Provider"] = "Webhook";
            configuration["Kafdeck:Notifications:Destinations:0:DisplayName"] = bridge._profile.DisplayName;
            configuration["Kafdeck:Notifications:Destinations:0:EnabledEvents:0"] = "Mutation";
            configuration["Kafdeck:Notifications:Destinations:0:ConfiguredEndpoint"] = bridge._profile.ConfiguredEndpoint!.AbsoluteUri;
            configuration["Kafdeck:Notifications:Destinations:0:WebhookAuthentication"] = "Anonymous";
            return bridge;
        }
        catch { await bridge.DisposeAsync(); throw; }
    }

    internal async Task DeliverAfterApiRestartAsync(Process api, string mutationPath,
        MutationOperationSnapshot mutation, string rawTopic, string evidenceDirectory, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        cancellationToken = deadline.Token;
        Assert.Equal(MutationOperationState.AppliedVerified, mutation.State);
        var source = new MutationNotificationOutbox(new SqliteMutationDbConnectionFactory(mutationPath));
        NotificationDeliveryRecord pending;
        NotificationProductEventSourceStatus sourceStatus;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var queued = await _deliveries.ListDueAsync(new(DateTimeOffset.UtcNow, maxResults: 2), cancellationToken);
            sourceStatus = await source.GetStatusAsync(cancellationToken);
            if (queued.Items.Count == 1 && sourceStatus.Pending == 0 && sourceStatus.Routed == 1)
            {
                Assert.False(queued.Truncated);
                pending = Assert.Single(queued.Items);
                break;
            }
            await Task.Delay(25, cancellationToken);
        }
        Assert.Equal(NotificationDeliveryState.Pending, pending.Snapshot.State);
        Assert.Equal(0, pending.Snapshot.AttemptCount);
        Assert.Equal(0, _tls.RequestCount);
        Assert.Equal(0, sourceStatus.Expired);
        Assert.Equal(0, sourceStatus.Overflowed);
        var routed = await _routing.GetEventAsync(pending.Snapshot.NotificationId, cancellationToken);
        Assert.NotNull(routed);
        Assert.Equal(NotificationEventClass.Mutation, routed.Event.EventClass);
        Assert.Equal("mutation.state-changed", routed.Event.EventType);
        Assert.Equal(mutation.UpdatedAtUtc, routed.Event.OccurredAtUtc);

        api.Kill(entireProcessTree: true);
        await api.WaitForExitAsync(cancellationToken);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([_profile]));
        builder.Services.AddSingleton<INotificationCredentialResolver>(new ConfiguredNotificationCredentialResolver([], new SecretResolver()));
        builder.Services.AddSingleton<INotificationHostAddressLookup>(_tls);
        builder.Services.AddSingleton<INotificationPinnedTcpDialer>(_tls);
        builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(_tls, NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots: new X509Certificate2Collection(_tls.Root)));
        builder.Services.AddSingleton<INotificationRoutingStore>(_routing);
        builder.Services.AddSingleton<INotificationDeliveryStore>(_deliveries);
        builder.Services.AddKafdeckWebhookDelivery(new(true, NotificationPersistenceProvider.Sqlite,
            NotificationExecutionMode.Standalone, _path, null, DestinationProfiles: [_profile], WebhookDeliveryEnabled: true));
        using var sender = builder.Build();
        await sender.StartAsync(cancellationToken);
        var request = await _tls.Received.Task.WaitAsync(cancellationToken);
        NotificationDeliveryRecord? receipt;
        while ((receipt = await _deliveries.GetAsync(pending.Snapshot.NotificationId, _profile.DestinationId,
                   cancellationToken))?.Snapshot.State != NotificationDeliveryState.Delivered)
            await Task.Delay(25, cancellationToken);
        await sender.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync(cancellationToken);
        await sender.StopAsync(cancellationToken);
        Assert.Equal(1, _tls.RequestCount);
        Assert.Equal(1, receipt.Snapshot.AttemptCount);
        Assert.DoesNotContain(rawTopic, request, StringComparison.Ordinal);
        Assert.DoesNotContain(mutation.OperationId.ToString("D"), request, StringComparison.Ordinal);
        Assert.DoesNotContain(mutation.RequesterPrincipalId, request, StringComparison.Ordinal);
        var reopened = new AdoNotificationDeliveryStore(new SqliteNotificationDeliveryDbConnectionFactory(_path));
        var durable = await reopened.GetAsync(pending.Snapshot.NotificationId, _profile.DestinationId, cancellationToken);
        Assert.NotNull(durable);
        Assert.Equal(NotificationDeliveryState.Delivered, durable.Snapshot.State);
        Assert.Equal(1, durable.Snapshot.AttemptCount);
        Assert.Equal(1, (await source.GetStatusAsync(cancellationToken)).Routed);
        await File.WriteAllTextAsync(Path.Combine(evidenceDirectory, "human-kafka-product-notification-evidence.json"),
            JsonSerializer.Serialize(new
            {
                observedAtUtc = DateTimeOffset.UtcNow,
                flow = "genuine human Keycloak PKCE -> shipped CLI -> actual API -> verified Kafka TopicCreate -> atomic product outbox -> product pump -> active subscription -> durable Pending -> API process stop -> second production sender -> physical TLS204 -> reopened Delivered",
                mutationState = mutation.State.ToString(), productSourceEnabledInActualApi = true,
                sourceRouted = sourceStatus.Routed, sourcePending = sourceStatus.Pending,
                queuedBeforeRestart = 1, attemptsBeforeRestart = 0, physicalPostsBeforeRestart = 0,
                physicalProviderPosts = _tls.RequestCount, physicalHttpStatus = 204,
                durableState = durable.Snapshot.State.ToString(), durableAttemptCount = durable.Snapshot.AttemptCount,
                rawTopicOperationAndPrincipalLeaks = 0, persistence = "SQLite",
                caveat = "One low-risk TopicCreate and local TLS fixture; no external SaaS, PostgreSQL effect, product-source HA, other event family, database restore or broker failover acceptance."
            }, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
    }

    public ValueTask DisposeAsync() => _tls.DisposeAsync();
}
