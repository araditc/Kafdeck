using System.Net;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationRoutingTests
{
    private static readonly DateTimeOffset Now =
        new(
            2026,
            10,
            8,
            8,
            30,
            0,
            TimeSpan.Zero);

    [Fact]
    public async Task Sqlite_store_round_trips_safe_event_and_subscription_cas()
    {
        var path = TempPath();

        try
        {
            var store =
                new AdoNotificationRoutingStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var notificationEvent =
                SafeEvent(
                    Guid.NewGuid());
            var created =
                await store.CreateOrGetEventAsync(
                    notificationEvent,
                    Now);

            Assert.Equal(
                notificationEvent.PayloadFingerprint,
                created.Event.PayloadFingerprint);

            var replay =
                await store.CreateOrGetEventAsync(
                    notificationEvent,
                    Now.AddSeconds(10));
            Assert.Equal(
                Now,
                replay.CreatedAtUtc);

            var definition =
                Subscription(
                    "quality-webhook",
                    "ops-webhook",
                    NotificationEventClass.DataQuality);
            var subscription =
                await store.CreateSubscriptionAsync(
                    definition,
                    NotificationSubscriptionState.Active,
                    Now);
            Assert.Equal(
                1,
                subscription.Revision);

            var stale =
                await store.ReplaceSubscriptionAsync(
                    definition,
                    NotificationSubscriptionState.Paused,
                    expectedRevision: 2,
                    Now.AddSeconds(1));
            Assert.Null(
                stale);

            var updated =
                await store.ReplaceSubscriptionAsync(
                    definition,
                    NotificationSubscriptionState.Paused,
                    expectedRevision: 1,
                    Now.AddSeconds(1));
            Assert.NotNull(
                updated);
            Assert.Equal(
                2,
                updated!.Revision);
            Assert.Equal(
                NotificationSubscriptionState.Paused,
                updated.State);

            var page =
                await store.ListSubscriptionsAsync(
                    new NotificationSubscriptionQuery(
                        state:
                            NotificationSubscriptionState.Paused,
                        eventClass:
                            NotificationEventClass.DataQuality));
            var listed =
                Assert.Single(
                    page.Items);
            Assert.Equal(
                "quality-webhook",
                listed.Definition.SubscriptionId);
            Assert.False(
                page.Truncated);
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Durable_event_store_rejects_unclassified_subject_and_summary()
    {
        var path = TempPath();
        try
        {
            var store = new AdoNotificationRoutingStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            await store.InitializeAsync();

            var unclassified = new NotificationSafeEvent(
                Guid.NewGuid(),
                NotificationEventClass.Security,
                "authorization.denied",
                "arbitrary printable secret",
                "arbitrary decoded record value",
                Now.AddMinutes(-1));

            Assert.False(unclassified.IsApprovedForDurability);
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.CreateOrGetEventAsync(unclassified, Now));

            Assert.Throws<InvalidOperationException>(
                () => NotificationSafeEvent.RestoreApproved(
                    unclassified.EventId,
                    unclassified.EventClass,
                    unclassified.EventType,
                    unclassified.Subject,
                    unclassified.Summary,
                    unclassified.OccurredAtUtc));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Exact_event_types_match_only_approved_class_and_type()
    {
        var path = TempPath();
        try
        {
            var store = new AdoNotificationRoutingStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            await store.InitializeAsync();

            var typeMatch = new NotificationSubscriptionDefinition(
                "typed-match",
                "ops-webhook",
                [NotificationEventClass.DataQuality],
                ["data-quality.violation"]);
            var typeMismatch = new NotificationSubscriptionDefinition(
                "typed-mismatch",
                "ops-webhook",
                [NotificationEventClass.DataQuality],
                ["data-quality.unrelated"]);

            var approved = SafeEvent(Guid.NewGuid());
            Assert.True(typeMatch.Matches(approved));
            Assert.False(typeMismatch.Matches(approved));
            Assert.False(typeMatch.Matches(NotificationEventClass.Security));
            Assert.Throws<ArgumentException>(() =>
                new NotificationSubscriptionDefinition(
                    "invalid", "ops-webhook",
                    [NotificationEventClass.DataQuality],
                    ["data-quality.violation", "data-quality.violation"]));
            Assert.Throws<ArgumentException>(() =>
                new NotificationSubscriptionDefinition(
                    "invalid", "ops-webhook",
                    [NotificationEventClass.DataQuality],
                    ["data-quality.violation", "bad type"]));

            await store.CreateSubscriptionAsync(
                typeMatch, NotificationSubscriptionState.Active, Now);
            await store.CreateSubscriptionAsync(
                typeMismatch, NotificationSubscriptionState.Active, Now);
            var readback = await store.GetSubscriptionAsync("typed-match");
            Assert.Equal(["data-quality.violation"],
                readback!.Definition.EventTypes);

            var deliveryStore = new AdoNotificationDeliveryStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            await deliveryStore.InitializeAsync();

            var routed = await new NotificationRoutingCoordinator(
                    store, deliveryStore,
                    new FakeProfileCatalog(WebhookProfile()))
                .RouteAsync(approved, Now);

            Assert.Equal(2, routed.SubscriptionsVisited);
            Assert.Equal(1, routed.DeliveriesCreatedOrMatched);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Retired_subscription_is_terminal_and_never_routed()
    {
        var path = TempPath();
        try
        {
            var store = new AdoNotificationRoutingStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            await store.InitializeAsync();
            var definition = new NotificationSubscriptionDefinition(
                "retired-events",
                "ops-webhook",
                [NotificationEventClass.DataQuality],
                ["data-quality.violation"]);
            var active = await store.CreateSubscriptionAsync(
                definition, NotificationSubscriptionState.Active, Now);
            var retired = await store.ReplaceSubscriptionAsync(
                definition,
                NotificationSubscriptionState.Retired,
                active.Revision,
                Now.AddSeconds(1));
            Assert.NotNull(retired);
            Assert.Equal(NotificationSubscriptionState.Retired,
                retired!.State);

            var selected = await store.ListSubscriptionsAsync(
                new NotificationSubscriptionQuery(
                    state: NotificationSubscriptionState.Retired));
            var record = Assert.Single(selected.Items);
            Assert.Equal(NotificationSubscriptionState.Retired, record.State);
            Assert.Equal(["data-quality.violation"],
                record.Definition.EventTypes);

            Assert.Null(await store.ReplaceSubscriptionAsync(
                definition,
                NotificationSubscriptionState.Active,
                expectedRevision: 1,
                Now.AddSeconds(2)));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.ReplaceSubscriptionAsync(
                    definition,
                    NotificationSubscriptionState.Active,
                    retired.Revision,
                    Now.AddSeconds(2)));

            var deliveryStore = new AdoNotificationDeliveryStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            await deliveryStore.InitializeAsync();
            var routed = await new NotificationRoutingCoordinator(
                    store, deliveryStore,
                    new FakeProfileCatalog(WebhookProfile()))
                .RouteAsync(SafeEvent(Guid.NewGuid()), Now.AddSeconds(2));
            Assert.Equal(0, routed.SubscriptionsVisited);
            Assert.Equal(0, routed.DeliveriesCreatedOrMatched);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Event_idempotency_rejects_different_safe_material()
    {
        var path = TempPath();

        try
        {
            var store =
                new AdoNotificationRoutingStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var id =
                Guid.NewGuid();
            await store.CreateOrGetEventAsync(
                SafeEvent(
                    id),
                Now);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () =>
                    store.CreateOrGetEventAsync(
                        NotificationSafeEvent.Approved(
                            id,
                            NotificationApprovedEventKind.SecurityAuthorizationDenied,
                            Now.AddMinutes(-1)),
                        Now));
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Routing_is_idempotent_and_webhook_dispatch_reconstructs_only_safe_material()
    {
        var path = TempPath();

        try
        {
            var factory =
                new SqliteNotificationDeliveryDbConnectionFactory(
                    path);
            var routingStore =
                new AdoNotificationRoutingStore(
                    factory);
            var deliveryStore =
                new AdoNotificationDeliveryStore(
                    factory);
            await routingStore.InitializeAsync();
            await deliveryStore.InitializeAsync();

            await routingStore.CreateSubscriptionAsync(
                Subscription(
                    "quality-webhook",
                    "ops-webhook",
                    NotificationEventClass.DataQuality),
                NotificationSubscriptionState.Active,
                Now);

            var profile =
                WebhookProfile();
            var catalog =
                new FakeProfileCatalog(
                    profile);
            var coordinator =
                new NotificationRoutingCoordinator(
                    routingStore,
                    deliveryStore,
                    catalog);
            var notificationEvent =
                SafeEvent(
                    Guid.NewGuid());

            var first =
                await coordinator.RouteAsync(
                    notificationEvent,
                    Now);
            var replay =
                await coordinator.RouteAsync(
                    notificationEvent,
                    Now.AddSeconds(30));

            Assert.Equal(
                1,
                first.DeliveriesCreatedOrMatched);
            Assert.Equal(
                1,
                replay.DeliveriesCreatedOrMatched);

            var pending =
                await deliveryStore.GetAsync(
                    notificationEvent.EventId,
                    "ops-webhook");
            Assert.NotNull(
                pending);
            Assert.Equal(
                notificationEvent.PayloadFingerprint,
                pending!.Snapshot.PayloadFingerprint);
            Assert.Equal(
                Now,
                pending.Snapshot.CreatedAtUtc);

            var claim =
                await deliveryStore.TryClaimForDispatchAsync(
                    notificationEvent.EventId,
                    "ops-webhook",
                    pending.Revision,
                    Now,
                    Now.AddHours(1),
                    maxConcurrency: 1,
                    ratePerSecond: 10);
            Assert.Equal(
                NotificationDeliveryClaimOutcome.Claimed,
                claim.Outcome);

            var transport =
                new FakeWebhookTransport();
            var dispatcher =
                new WebhookNotificationDeliveryDispatcher(
                    routingStore,
                    catalog,
                    new WebhookNotificationAdapter(
                        new FakeEndpointResolver(),
                        new FakeCredentialResolver(),
                        transport));

            var dispatched =
                await dispatcher.DispatchAsync(
                    claim.Record!,
                    CancellationToken.None);

            Assert.Equal(
                NotificationDeliveryDispatchOutcome.Delivered,
                dispatched.Outcome);
            Assert.NotNull(
                transport.LastRequest);

            var payloadText =
                System.Text.Encoding.UTF8.GetString(
                    transport.LastRequest!.JsonPayload.Span);
            Assert.Contains(
                "data-quality.violation",
                payloadText,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "secret",
                payloadText,
                StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                notificationEvent.PayloadFingerprint,
                Convert.ToHexString(
                        System.Security.Cryptography.SHA256.HashData(
                            transport.LastRequest.JsonPayload.Span))
                    .ToLowerInvariant());
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Routing_fails_closed_before_delivery_when_matching_subscriptions_exceed_cap()
    {
        var routingStore =
            new OverflowRoutingStore();
        var deliveryStore =
            new RecordingDeliveryStore();
        var coordinator =
            new NotificationRoutingCoordinator(
                routingStore,
                deliveryStore,
                new FakeProfileCatalog(
                    WebhookProfile()));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () =>
                coordinator.RouteAsync(
                    SafeEvent(
                        Guid.NewGuid()),
                    Now));

        Assert.Equal(
            0,
            deliveryStore.CreateCalls);
    }

    [Fact]
    public async Task Sqlite_v1_subscription_upgrade_preserves_wildcard_and_uses_terminal_retirement()
    {
        var path = TempPath();
        try
        {
            await using (var raw = new SqliteConnection(
                new SqliteConnectionStringBuilder { DataSource = path }.ConnectionString))
            {
                await raw.OpenAsync();
                await using var old = raw.CreateCommand();
                old.CommandText =
                    """
                    CREATE TABLE kafdeck_schema_info (
                        component TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL);
                    INSERT INTO kafdeck_schema_info
                        (component, schema_version)
                    VALUES ('notification-routing', 1);
                    CREATE TABLE kafdeck_notification_subscriptions (
                        subscription_id TEXT PRIMARY KEY,
                        destination_id TEXT NOT NULL,
                        lifecycle_state TEXT NOT NULL,
                        revision BIGINT NOT NULL,
                        updated_at_utc TEXT NOT NULL,
                        CHECK (revision >= 1),
                        CHECK (lifecycle_state IN ('Active', 'Paused')));
                    CREATE TABLE kafdeck_notification_subscription_events (
                        subscription_id TEXT NOT NULL,
                        event_class TEXT NOT NULL,
                        PRIMARY KEY(subscription_id, event_class),
                        FOREIGN KEY(subscription_id)
                            REFERENCES kafdeck_notification_subscriptions(subscription_id)
                            ON DELETE CASCADE);
                    INSERT INTO kafdeck_notification_subscriptions
                        (subscription_id, destination_id, lifecycle_state,
                         revision, updated_at_utc)
                    VALUES ('legacy-sub', 'ops-webhook', 'Active', 1,
                            '2026-10-08T08:30:00.0000000+00:00');
                    INSERT INTO kafdeck_notification_subscription_events
                        (subscription_id, event_class)
                    VALUES ('legacy-sub', 'DataQuality');
                    """;
                await old.ExecuteNonQueryAsync();
            }

            var store = new AdoNotificationRoutingStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            await store.InitializeAsync();

            var legacy = await store.GetSubscriptionAsync("legacy-sub");
            Assert.NotNull(legacy);
            Assert.Equal(NotificationSubscriptionState.Active, legacy!.State);
            Assert.Empty(legacy.Definition.EventTypes);
            Assert.True(legacy.Definition.Matches(SafeEvent(Guid.NewGuid())));

            var retired = await store.ReplaceSubscriptionAsync(
                legacy.Definition,
                NotificationSubscriptionState.Retired,
                legacy.Revision,
                Now.AddSeconds(1));
            Assert.NotNull(retired);
            Assert.Equal(NotificationSubscriptionState.Retired,
                (await store.GetSubscriptionAsync("legacy-sub"))!.State);

            await using var verify = new SqliteConnection(
                new SqliteConnectionStringBuilder { DataSource = path }.ConnectionString);
            await verify.OpenAsync();
            await using var version = verify.CreateCommand();
            version.CommandText =
                "SELECT schema_version FROM kafdeck_schema_info WHERE component = 'notification-routing'";
            Assert.Equal(2L, Convert.ToInt64(
                await version.ExecuteScalarAsync()));
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task PostgreSql_store_round_trips_safe_events_and_subscription_queries_when_available()
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
            $"w66_routing_{Guid.NewGuid():N}";
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
            var store =
                new AdoNotificationRoutingStore(
                    new PostgreSqlNotificationDeliveryDbConnectionFactory(
                        scoped.ConnectionString));
            await store.InitializeAsync();

            var notificationEvent =
                SafeEvent(
                    Guid.NewGuid());
            await store.CreateOrGetEventAsync(
                notificationEvent,
                Now);

            var typed = new NotificationSubscriptionDefinition(
                "a-subscription",
                "ops-webhook",
                [NotificationEventClass.DataQuality],
                ["data-quality.violation"]);
            await store.CreateSubscriptionAsync(
                typed,
                NotificationSubscriptionState.Active,
                Now);
            await store.CreateSubscriptionAsync(
                Subscription(
                    "b-subscription",
                    "ops-webhook",
                    NotificationEventClass.Security),
                NotificationSubscriptionState.Active,
                Now);

            var quality =
                await store.ListSubscriptionsAsync(
                    new NotificationSubscriptionQuery(
                        maxResults: 1,
                        eventClass:
                            NotificationEventClass.DataQuality));

            var only =
                Assert.Single(
                    quality.Items);
            Assert.Equal(
                "a-subscription",
                only.Definition.SubscriptionId);
            Assert.False(
                quality.Truncated);
            Assert.Equal(["data-quality.violation"],
                only.Definition.EventTypes);

            var retired = await store.ReplaceSubscriptionAsync(
                typed,
                NotificationSubscriptionState.Retired,
                only.Revision,
                Now.AddSeconds(1));
            Assert.NotNull(retired);
            Assert.Equal(NotificationSubscriptionState.Retired,
                (await store.GetSubscriptionAsync("a-subscription"))!.State);
            var retiredPage = await store.ListSubscriptionsAsync(
                new NotificationSubscriptionQuery(
                    state: NotificationSubscriptionState.Retired));
            Assert.Single(retiredPage.Items);
            Assert.Equal(["data-quality.violation"],
                retiredPage.Items[0].Definition.EventTypes);

            var fetched =
                await store.GetEventAsync(
                    notificationEvent.EventId);
            Assert.NotNull(
                fetched);
            Assert.Equal(
                notificationEvent.PayloadFingerprint,
                fetched!.Event.PayloadFingerprint);
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

    private static NotificationSafeEvent SafeEvent(
        Guid id) =>
        NotificationSafeEvent.Approved(
            id,
            NotificationApprovedEventKind.DataQualityViolation,
            Now.AddMinutes(-1));

    private static NotificationSubscriptionDefinition
        Subscription(
            string id,
            string destinationId,
            params NotificationEventClass[] eventClasses) =>
        new(
            id,
            destinationId,
            eventClasses);

    private static NotificationDestinationProfile
        WebhookProfile() =>
        new(
            "ops-webhook",
            NotificationProviderKind.Webhook,
            "Operations",
            [NotificationEventClass.DataQuality],
            new Uri(
                "https://hooks.example.com/events"));

    private static string TempPath() =>
        Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w66-routing-{Guid.NewGuid():N}.db");

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
            if (File.Exists(
                    candidate))
            {
                File.Delete(
                    candidate);
            }
        }
    }

    private sealed class FakeProfileCatalog :
        INotificationDestinationProfileCatalog
    {
        private readonly NotificationDestinationProfile
            _profile;

        public FakeProfileCatalog(
            NotificationDestinationProfile profile)
        {
            _profile = profile;
        }

        public ValueTask<NotificationDestinationProfile?>
            GetAsync(
                string destinationId,
                CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return ValueTask.FromResult<
                NotificationDestinationProfile?>(
                string.Equals(
                    destinationId,
                    _profile.DestinationId,
                    StringComparison.Ordinal)
                    ? _profile
                    : null);
        }
    }

    private sealed class FakeEndpointResolver :
        INotificationEndpointResolutionPort
    {
        public Task<NotificationPinnedEndpoint>
            ResolveForConnectionAsync(
                Uri configuredEndpoint,
                CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return Task.FromResult(
                NotificationAddressPolicy
                    .NoConfiguredNat64
                    .ValidatePinnedEndpoint(
                        configuredEndpoint,
                        [IPAddress.Parse("1.1.1.1")],
                        Now));
        }
    }

    private sealed class FakeCredentialResolver :
        INotificationCredentialResolver
    {
        public ValueTask<NotificationCredentialValue>
            ResolveAsync(
                NotificationCredentialResolutionRequest request,
                CancellationToken cancellationToken) =>
            ValueTask.FromResult(
                new NotificationCredentialValue(
                    "secret"));
    }

    private sealed class FakeWebhookTransport :
        INotificationPinnedWebhookTransport
    {
        public NotificationPinnedWebhookRequest?
            LastRequest { get; private set; }

        public Task<NotificationWebhookTransportResult>
            SendAsync(
                NotificationPinnedWebhookRequest request,
                CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LastRequest = request;

            return Task.FromResult(
                new NotificationWebhookTransportResult(
                    NotificationWebhookTransportOutcome.Delivered,
                    "http-2xx"));
        }
    }

    private sealed class OverflowRoutingStore :
        INotificationRoutingStore
    {
        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<NotificationSafeEventRecord>
            CreateOrGetEventAsync(
                NotificationSafeEvent notificationEvent,
                DateTimeOffset createdAtUtc,
                CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new NotificationSafeEventRecord(
                    notificationEvent,
                    createdAtUtc));

        public Task<NotificationSafeEventRecord?> GetEventAsync(
            Guid eventId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<NotificationSafeEventRecord?>(
                null);

        public Task<NotificationSubscriptionSnapshot>
            CreateSubscriptionAsync(
                NotificationSubscriptionDefinition definition,
                NotificationSubscriptionState state,
                DateTimeOffset updatedAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<NotificationSubscriptionSnapshot?> GetSubscriptionAsync(
            string subscriptionId,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<NotificationSubscriptionSnapshot?>
            ReplaceSubscriptionAsync(
                NotificationSubscriptionDefinition definition,
                NotificationSubscriptionState state,
                long expectedRevision,
                DateTimeOffset updatedAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<NotificationSubscriptionPage> ListSubscriptionsAsync(
            NotificationSubscriptionQuery query,
            CancellationToken cancellationToken = default)
        {
            var item =
                new NotificationSubscriptionSnapshot(
                    Subscription(
                        "overflow",
                        "ops-webhook",
                        NotificationEventClass.DataQuality),
                    NotificationSubscriptionState.Active,
                    1,
                    Now);

            return Task.FromResult(
                new NotificationSubscriptionPage(
                    [item],
                    true,
                    "overflow"));
        }
    }

    private sealed class RecordingDeliveryStore :
        INotificationDeliveryStore
    {
        public int CreateCalls { get; private set; }

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<DateTimeOffset> GetCoordinationUtcNowAsync(
            DateTimeOffset standaloneFallbackUtc,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                standaloneFallbackUtc);

        public Task<NotificationDeliveryRecord?> GetAsync(
            Guid notificationId,
            string destinationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<NotificationDeliveryRecord?>(
                null);

        public Task<NotificationDeliveryRecord> CreateOrGetAsync(
            NotificationDeliverySnapshot snapshot,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
        {
            CreateCalls++;
            return Task.FromResult(
                new NotificationDeliveryRecord(
                    snapshot,
                    1,
                    updatedAtUtc));
        }

        public Task<NotificationDeliveryRecord?> ReplaceAsync(
            NotificationDeliverySnapshot snapshot,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<NotificationDeliveryClaimResult> TryClaimForDispatchAsync(
            Guid notificationId,
            string destinationId,
            long expectedRevision,
            DateTimeOffset claimedAtUtc,
            DateTimeOffset notAfterUtc,
            int maxConcurrency,
            int ratePerSecond,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<NotificationDeliveryPage> ListDueAsync(
            NotificationDeliveryDueQuery query,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<NotificationDeliveryRecoveryPage>
            ListStaleInFlightAsync(
                NotificationStaleInFlightQuery query,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
