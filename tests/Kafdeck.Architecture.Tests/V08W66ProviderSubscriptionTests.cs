using System.Data.Common;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66ProviderSubscriptionTests
{
    private const string Fingerprint =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    [Fact]
    public async Task Email_adapter_uses_typed_recipient_and_revision_bound_credential()
    {
        var profile =
            Profile(
                "email-ops",
                NotificationProviderKind.Email);
        var destination =
            new EmailNotificationDestination(
                profile,
                "ops@example.com");
        var resolver =
            new RecordingCredentialResolver();
        var transport =
            new RecordingEmailTransport();
        var adapter =
            new EmailNotificationAdapter(
                resolver,
                transport);
        var notificationEvent =
            Event(
                NotificationEventClass.Operational,
                "consumer-lag");

        var result =
            await adapter.DispatchAsync(
                destination,
                notificationEvent);

        Assert.Equal(
            NotificationProviderTransportOutcome.Delivered,
            result.TransportResult.Outcome);
        Assert.Equal(
            64,
            result.PayloadFingerprint.Length);

        var resolution =
            Assert.Single(
                resolver.Requests);
        Assert.Equal(
            profile.DestinationId,
            resolution.DestinationId);
        Assert.Equal(
            profile.RevisionFingerprint,
            resolution.ProfileRevisionFingerprint);
        Assert.Equal(
            NotificationProviderKind.Email,
            resolution.Provider);

        var request =
            Assert.Single(
                transport.Requests);
        Assert.Equal(
            "ops@example.com",
            request.RecipientAddress);
        Assert.Contains(
            "consumer lag high",
            request.Body,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "credential-secret",
            request.Body,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(NotificationProviderKind.Slack)]
    [InlineData(NotificationProviderKind.MicrosoftTeams)]
    [InlineData(NotificationProviderKind.Telegram)]
    [InlineData(NotificationProviderKind.PagerDuty)]
    public async Task Bound_provider_adapters_expose_no_caller_endpoint_or_headers(
        NotificationProviderKind provider)
    {
        var profile =
            Profile(
                $"dest-{provider}",
                provider);
        var destination =
            new BoundNotificationDestination(
                profile,
                provider);
        var resolver =
            new RecordingCredentialResolver();
        var transport =
            new RecordingBoundTransport();
        var notificationEvent =
            Event(
                NotificationEventClass.Security,
                "audit-denied");

        NotificationProviderDispatchResult result =
            provider switch
            {
                NotificationProviderKind.Slack =>
                    await new SlackNotificationAdapter(
                            resolver,
                            transport)
                        .DispatchAsync(
                            destination,
                            notificationEvent),
                NotificationProviderKind.MicrosoftTeams =>
                    await new TeamsNotificationAdapter(
                            resolver,
                            transport)
                        .DispatchAsync(
                            destination,
                            notificationEvent),
                NotificationProviderKind.Telegram =>
                    await new TelegramNotificationAdapter(
                            resolver,
                            transport)
                        .DispatchAsync(
                            destination,
                            notificationEvent),
                NotificationProviderKind.PagerDuty =>
                    await new PagerDutyNotificationAdapter(
                            resolver,
                            transport)
                        .DispatchAsync(
                            destination,
                            notificationEvent),
                _ => throw new InvalidOperationException(),
            };

        Assert.Equal(
            NotificationProviderTransportOutcome.Delivered,
            result.TransportResult.Outcome);
        var request =
            Assert.Single(
                transport.Requests);
        Assert.Equal(
            provider,
            request.Provider);
        Assert.Equal(
            notificationEvent.EventId,
            request.Event.EventId);

        var publicNames =
            typeof(NotificationBoundTransportRequest)
                .GetProperties()
                .Select(property => property.Name)
                .ToArray();

        Assert.DoesNotContain(
            publicNames,
            name =>
                name.Contains(
                    "Url",
                    StringComparison.OrdinalIgnoreCase) ||
                name.Contains(
                    "Header",
                    StringComparison.OrdinalIgnoreCase) ||
                name.Contains(
                    "Method",
                    StringComparison.OrdinalIgnoreCase) ||
                name.Contains(
                    "Path",
                    StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Provider_destination_requires_exact_typed_provider_and_opaque_binding()
    {
        var webhook =
            new NotificationDestinationProfile(
                "webhook",
                NotificationProviderKind.Webhook,
                "Webhook",
                [NotificationEventClass.Operational],
                new Uri("https://alerts.example.com/hook"),
                new NotificationCredentialBindingId(
                    "hook-secret"));

        Assert.Throws<ArgumentException>(
            () =>
                new BoundNotificationDestination(
                    webhook,
                    NotificationProviderKind.Slack));

        var noCredential =
            new NotificationDestinationProfile(
                "slack",
                NotificationProviderKind.Slack,
                "Slack",
                [NotificationEventClass.Operational]);

        Assert.Throws<ArgumentException>(
            () =>
                new BoundNotificationDestination(
                    noCredential,
                    NotificationProviderKind.Slack));
    }

    [Fact]
    public async Task Sqlite_subscription_store_round_trips_cas_and_stable_pages()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationSubscriptionStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;

            foreach (var id in
                     new[]
                     {
                         "a-sub",
                         "b-sub",
                         "c-sub",
                     })
            {
                await store.CreateAsync(
                    Subscription(
                        id,
                        id == "c-sub"
                            ? "slack-security"
                            : "email-ops",
                        now),
                    now);
            }

            var first =
                await store.ListAsync(
                    new NotificationSubscriptionListQuery(
                        maxResults: 2));
            Assert.Equal(
                2,
                first.Items.Count);
            Assert.True(
                first.Truncated);
            Assert.Equal(
                "b-sub",
                first.NextSubscriptionId);

            var second =
                await store.ListAsync(
                    new NotificationSubscriptionListQuery(
                        maxResults: 2,
                        afterSubscriptionId:
                            first.NextSubscriptionId));
            Assert.Single(
                second.Items);
            Assert.Equal(
                "c-sub",
                second.Items[0].Snapshot.SubscriptionId);
            Assert.False(
                second.Truncated);

            var current =
                await store.GetAsync(
                    "a-sub");
            Assert.NotNull(
                current);

            var paused =
                new NotificationSubscriptionSnapshot(
                    current!.Snapshot.SubscriptionId,
                    current.Snapshot.DestinationId,
                    current.Snapshot.Filter,
                    NotificationSubscriptionState.Paused,
                    current.Snapshot.CreatedAtUtc);

            var replaced =
                await store.ReplaceAsync(
                    paused,
                    current.Revision,
                    now.AddSeconds(1));
            Assert.NotNull(
                replaced);
            Assert.Equal(
                2,
                replaced!.Revision);

            var stale =
                await store.ReplaceAsync(
                    current.Snapshot,
                    expectedRevision: 1,
                    now.AddSeconds(2));
            Assert.Null(
                stale);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Subscription_router_is_bounded_and_matches_only_active_exact_filters()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationSubscriptionStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            await store.CreateAsync(
                Subscription(
                    "ops-exact",
                    "email-ops",
                    now,
                    NotificationSubscriptionState.Active,
                    [NotificationEventClass.Operational],
                    ["consumer-lag"]),
                now);
            await store.CreateAsync(
                Subscription(
                    "ops-other",
                    "slack-ops",
                    now,
                    NotificationSubscriptionState.Active,
                    [NotificationEventClass.Operational],
                    ["broker-health"]),
                now);
            await store.CreateAsync(
                Subscription(
                    "paused",
                    "teams-paused",
                    now,
                    NotificationSubscriptionState.Paused,
                    [NotificationEventClass.Operational]),
                now);
            await store.CreateAsync(
                Subscription(
                    "security",
                    "pager-security",
                    now,
                    NotificationSubscriptionState.Active,
                    [NotificationEventClass.Security]),
                now);

            var router =
                new NotificationSubscriptionRouter(
                    store,
                    new NotificationSubscriptionRoutingPolicy(
                        maxScanned: 10,
                        maxDestinations: 2));

            var result =
                await router.RouteAsync(
                    Event(
                        NotificationEventClass.Operational,
                        "consumer-lag"));

            Assert.Equal(
                ["email-ops"],
                result.DestinationIds);
            Assert.Equal(
                3,
                result.ScannedSubscriptions);
            Assert.False(
                result.Truncated);
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Subscription_schema_contains_no_payload_secret_header_or_body_columns()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationSubscriptionStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            await using var connection =
                new SqliteConnection(
                    new SqliteConnectionStringBuilder
                    {
                        DataSource = path,
                    }.ConnectionString);
            await connection.OpenAsync();

            await using var command =
                connection.CreateCommand();
            command.CommandText =
                "PRAGMA table_info(kafdeck_notification_subscriptions)";

            var names =
                new List<string>();
            await using var reader =
                await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                names.Add(
                    reader.GetString(1));
            }

            Assert.NotEmpty(
                names);

            foreach (var forbidden in
                     new[]
                     {
                         "payload",
                         "secret",
                         "credential",
                         "header",
                         "body",
                         "token",
                         "url",
                     })
            {
                Assert.DoesNotContain(
                    names,
                    name =>
                        name.Contains(
                            forbidden,
                            StringComparison.OrdinalIgnoreCase));
            }
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task PostgreSql_subscription_store_exercises_cas_and_pagination_when_available()
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
            $"w66_sub_{Guid.NewGuid():N}";
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
                new AdoNotificationSubscriptionStore(
                    new PostgreSqlNotificationDeliveryDbConnectionFactory(
                        scoped.ConnectionString));
            await store.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            await store.CreateAsync(
                Subscription(
                    "a-sub",
                    "email-ops",
                    now),
                now);
            await store.CreateAsync(
                Subscription(
                    "b-sub",
                    "slack-ops",
                    now),
                now);

            var first =
                await store.ListAsync(
                    new NotificationSubscriptionListQuery(
                        maxResults: 1));
            Assert.Single(
                first.Items);
            Assert.True(
                first.Truncated);
            Assert.Equal(
                "a-sub",
                first.NextSubscriptionId);

            var current =
                first.Items[0];
            var paused =
                new NotificationSubscriptionSnapshot(
                    current.Snapshot.SubscriptionId,
                    current.Snapshot.DestinationId,
                    current.Snapshot.Filter,
                    NotificationSubscriptionState.Paused,
                    current.Snapshot.CreatedAtUtc);
            var replaced =
                await store.ReplaceAsync(
                    paused,
                    current.Revision,
                    now.AddSeconds(1));
            Assert.NotNull(
                replaced);

            var filtered =
                await store.ListAsync(
                    new NotificationSubscriptionListQuery(
                        destinationId:
                            "email-ops",
                        state:
                            NotificationSubscriptionState.Paused));
            Assert.Single(
                filtered.Items);
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

    private static NotificationDestinationProfile Profile(
        string id,
        NotificationProviderKind provider) =>
        new(
            id,
            provider,
            id,
            [
                NotificationEventClass.Operational,
                NotificationEventClass.Security,
            ],
            credentialBindingId:
                new NotificationCredentialBindingId(
                    $"{id}-credential"));

    private static NotificationSafeEvent Event(
        NotificationEventClass eventClass,
        string eventType) =>
        new(
            Guid.NewGuid(),
            eventClass,
            eventType,
            "consumer lag high",
            "consumer lag high",
            DateTimeOffset.UtcNow);

    private static NotificationSubscriptionSnapshot Subscription(
        string subscriptionId,
        string destinationId,
        DateTimeOffset createdAtUtc,
        NotificationSubscriptionState state =
            NotificationSubscriptionState.Active,
        IReadOnlyList<NotificationEventClass>? eventClasses = null,
        IReadOnlyList<string>? eventTypes = null) =>
        new(
            subscriptionId,
            destinationId,
            new NotificationSubscriptionFilter(
                eventClasses ??
                [NotificationEventClass.Operational],
                eventTypes),
            state,
            createdAtUtc);

    private static string TempPath() =>
        Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w66-sub-{Guid.NewGuid():N}.db");

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

    private sealed class RecordingCredentialResolver :
        INotificationCredentialResolver
    {
        public List<NotificationCredentialResolutionRequest>
            Requests { get; } = [];

        public ValueTask<NotificationCredentialValue> ResolveAsync(
            NotificationCredentialResolutionRequest request,
            CancellationToken cancellationToken)
        {
            Requests.Add(
                request);
            return ValueTask.FromResult(
                new NotificationCredentialValue(
                    "credential-secret"));
        }
    }

    private sealed class RecordingEmailTransport :
        INotificationEmailTransport
    {
        public List<NotificationEmailTransportRequest>
            Requests { get; } = [];

        public Task<NotificationProviderTransportResult> SendAsync(
            NotificationEmailTransportRequest request,
            CancellationToken cancellationToken)
        {
            Requests.Add(
                request);
            return Task.FromResult(
                new NotificationProviderTransportResult(
                    NotificationProviderTransportOutcome.Delivered,
                    "email-accepted",
                    "mail-1"));
        }
    }

    private sealed class RecordingBoundTransport :
        INotificationSlackTransport,
        INotificationTeamsTransport,
        INotificationTelegramTransport,
        INotificationPagerDutyTransport
    {
        public List<NotificationBoundTransportRequest>
            Requests { get; } = [];

        public Task<NotificationProviderTransportResult> SendAsync(
            NotificationBoundTransportRequest request,
            CancellationToken cancellationToken)
        {
            Requests.Add(
                request);
            return Task.FromResult(
                new NotificationProviderTransportResult(
                    NotificationProviderTransportOutcome.Delivered,
                    "provider-accepted",
                    "provider-1"));
        }
    }
}
