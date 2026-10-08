using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66TypedProviderTests
{
    [Fact]
    public async Task Email_adapter_uses_typed_recipient_and_revision_bound_credential()
    {
        var profile =
            Profile(
                "email-ops",
                NotificationProviderKind.Email);
        var destination =
            new EmailNotificationDestination(
                profile);
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
            NotificationDeliveryDispatchOutcome.Delivered,
            result.DeliveryOutcome);
        Assert.Equal(
            notificationEvent.PayloadFingerprint,
            result.PayloadFingerprint);

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
        Assert.Equal(
            notificationEvent.PayloadFingerprint,
            result.PayloadFingerprint);
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
    public async Task Routed_email_delivery_is_finalized_by_real_ledger_worker()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w66-provider-{Guid.NewGuid():N}.db");
        try
        {
            var factory =
                new SqliteNotificationDeliveryDbConnectionFactory(path);
            var routingStore =
                new AdoNotificationRoutingStore(factory);
            var deliveryStore =
                new AdoNotificationDeliveryStore(factory);
            await routingStore.InitializeAsync();
            await deliveryStore.InitializeAsync();

            var profile =
                Profile("email-ops", NotificationProviderKind.Email);
            var catalog =
                new OneProfileCatalog(profile);
            var now = DateTimeOffset.UtcNow;
            await routingStore.CreateSubscriptionAsync(
                new NotificationSubscriptionDefinition(
                    "email-ops-sub",
                    profile.DestinationId,
                    [NotificationEventClass.Operational]),
                NotificationSubscriptionState.Active,
                now.AddSeconds(-3));

            var notificationEvent = NotificationSafeEvent.Approved(
                Guid.NewGuid(),
                NotificationApprovedEventKind.ConsumerLagAlert,
                now.AddSeconds(-2));
            var routed = await new NotificationRoutingCoordinator(
                    routingStore,
                    deliveryStore,
                    catalog)
                .RouteAsync(
                    notificationEvent,
                    now.AddSeconds(-1));
            Assert.Equal(
                1,
                routed.DeliveriesCreatedOrMatched);

            var credentialResolver =
                new RecordingCredentialResolver();
            var emailTransport =
                new RecordingEmailTransport();
            var boundTransport =
                new RecordingBoundTransport();
            var dispatcher =
                ProviderDispatcher(
                    routingStore,
                    catalog,
                    credentialResolver,
                    emailTransport,
                    boundTransport);

            var result =
                await new NotificationDeliveryWorker(
                        deliveryStore,
                        dispatcher)
                    .RunDueCycleAsync();

            Assert.Equal(1, result.Delivered);
            Assert.Equal(0, result.UnknownExternalEffect);
            var delivered =
                await deliveryStore.GetAsync(
                    notificationEvent.EventId,
                    profile.DestinationId);
            Assert.NotNull(delivered);
            Assert.Equal(
                NotificationDeliveryState.Delivered,
                delivered!.Snapshot.State);
            Assert.Equal(
                "ops@example.com",
                Assert.Single(emailTransport.Requests).RecipientAddress);
            Assert.Empty(boundTransport.Requests);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var candidate in new[]
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
    }

    [Fact]
    public async Task Routed_email_delivery_fails_closed_on_profile_revision_drift()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w66-drift-{Guid.NewGuid():N}.db");
        try
        {
            var factory = new SqliteNotificationDeliveryDbConnectionFactory(path);
            var routingStore = new AdoNotificationRoutingStore(factory);
            var deliveryStore = new AdoNotificationDeliveryStore(factory);
            await routingStore.InitializeAsync();
            await deliveryStore.InitializeAsync();

            var approved = Profile("email-ops", NotificationProviderKind.Email);
            var catalog = new OneProfileCatalog(approved);
            var now = DateTimeOffset.UtcNow;
            await routingStore.CreateSubscriptionAsync(
                new NotificationSubscriptionDefinition(
                    "email-subscriber",
                    approved.DestinationId,
                    [NotificationEventClass.Operational]),
                NotificationSubscriptionState.Active,
                now.AddSeconds(-3));
            var evt = NotificationSafeEvent.Approved(
                Guid.NewGuid(),
                NotificationApprovedEventKind.ConsumerLagAlert,
                now.AddSeconds(-2));
            await new NotificationRoutingCoordinator(
                    routingStore, deliveryStore, catalog)
                .RouteAsync(evt, now.AddSeconds(-1));

            var pending = await deliveryStore.GetAsync(
                evt.EventId, approved.DestinationId);
            Assert.NotNull(pending);
            Assert.Equal(
                approved.RevisionFingerprint,
                pending!.Snapshot.RoutedProfileRevisionFingerprint);

            var replacement = new NotificationDestinationProfile(
                approved.DestinationId,
                NotificationProviderKind.Email,
                approved.DisplayName,
                approved.EnabledEvents,
                credentialBindingId:
                    new NotificationCredentialBindingId("email-ops-credential"),
                emailRecipientAddress: "alternate@example.com");
            Assert.NotEqual(
                approved.RevisionFingerprint,
                replacement.RevisionFingerprint);
            catalog.ReplaceProfile(replacement);

            var resolver = new RecordingCredentialResolver();
            var transport = new RecordingEmailTransport();
            var bound = new RecordingBoundTransport();
            var result = await new NotificationDeliveryWorker(
                    deliveryStore,
                    ProviderDispatcher(
                        routingStore, catalog, resolver, transport, bound))
                .RunDueCycleAsync();

            Assert.Equal(1, result.UnknownExternalEffect);
            Assert.Empty(transport.Requests);
            Assert.Empty(resolver.Requests);
            var terminal = await deliveryStore.GetAsync(
                evt.EventId, approved.DestinationId);
            Assert.Equal(
                NotificationDeliveryState.UnknownExternalEffect,
                terminal!.Snapshot.State);
            Assert.Equal(
                approved.RevisionFingerprint,
                terminal.Snapshot.RoutedProfileRevisionFingerprint);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var candidate in new[] { path, path + "-wal", path + "-shm" })
            {
                if (File.Exists(candidate))
                {
                    File.Delete(candidate);
                }
            }
        }
    }

    [Fact]
    public void Email_recipient_is_fixed_by_server_profile_revision()
    {
        var profile =
            Profile("email-ops", NotificationProviderKind.Email);
        var destination =
            new EmailNotificationDestination(profile);
        Assert.Equal("ops@example.com", destination.RecipientAddress);
        Assert.Single(
            typeof(EmailNotificationDestination)
                .GetConstructors());
        Assert.Single(
            typeof(EmailNotificationDestination)
                .GetConstructors()[0].GetParameters());

        var second = new NotificationDestinationProfile(
            "email-ops",
            NotificationProviderKind.Email,
            "email-ops",
            [
                NotificationEventClass.Operational,
                NotificationEventClass.Security,
            ],
            credentialBindingId:
                new NotificationCredentialBindingId(
                    "email-ops-credential"),
            emailRecipientAddress: "security@example.com");
        Assert.NotEqual(
            profile.RevisionFingerprint,
            second.RevisionFingerprint);

        Assert.Throws<ArgumentException>(
            () => new NotificationDestinationProfile(
                "email-ops",
                NotificationProviderKind.Email,
                "email-ops",
                [NotificationEventClass.Operational],
                credentialBindingId:
                    new NotificationCredentialBindingId(
                        "email-ops-credential")));
        Assert.Throws<ArgumentException>(
            () => new NotificationDestinationProfile(
                "slack-ops",
                NotificationProviderKind.Slack,
                "slack-ops",
                [NotificationEventClass.Operational],
                emailRecipientAddress: "wrong@example.com"));
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
                    $"{id}-credential"),
            emailRecipientAddress:
                provider == NotificationProviderKind.Email
                    ? "ops@example.com"
                    : null);

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

    private static NotificationProviderDeliveryDispatcher ProviderDispatcher(
        INotificationRoutingStore routingStore,
        INotificationDestinationProfileCatalog profiles,
        RecordingCredentialResolver resolver,
        RecordingEmailTransport email,
        RecordingBoundTransport bound)
    {
        var webhook =
            new WebhookNotificationDeliveryDispatcher(
                routingStore,
                profiles,
                new WebhookNotificationAdapter(
                    new NeverResolveWebhookEndpoint(),
                    resolver,
                    new NeverSendWebhookTransport()));

        return new NotificationProviderDeliveryDispatcher(
            routingStore,
            profiles,
            new EmailNotificationAdapter(resolver, email),
            new SlackNotificationAdapter(resolver, bound),
            new TeamsNotificationAdapter(resolver, bound),
            new TelegramNotificationAdapter(resolver, bound),
            new PagerDutyNotificationAdapter(resolver, bound),
            webhook);
    }

    private sealed class NeverResolveWebhookEndpoint :
        INotificationEndpointResolutionPort
    {
        public Task<NotificationPinnedEndpoint> ResolveForConnectionAsync(
            Uri configuredEndpoint,
            CancellationToken cancellationToken) =>
            throw new InvalidOperationException(
                "Webhook endpoint resolution must not be invoked in Email test.");
    }

    private sealed class NeverSendWebhookTransport :
        INotificationPinnedWebhookTransport
    {
        public Task<NotificationWebhookTransportResult> SendAsync(
            NotificationPinnedWebhookRequest request,
            CancellationToken cancellationToken) =>
            throw new InvalidOperationException(
                "Webhook transport must not be invoked in Email test.");
    }

    private sealed class OneProfileCatalog :
        INotificationDestinationProfileCatalog
    {
        private NotificationDestinationProfile _profile;

        public OneProfileCatalog(NotificationDestinationProfile profile)
        {
            _profile = profile;
        }

        public void ReplaceProfile(NotificationDestinationProfile updated) =>
            _profile = updated;

        public ValueTask<NotificationDestinationProfile?> GetAsync(
            string destinationId,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult<NotificationDestinationProfile?>(
                string.Equals(
                    destinationId,
                    _profile.DestinationId,
                    StringComparison.Ordinal)
                    ? _profile
                    : null);
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
