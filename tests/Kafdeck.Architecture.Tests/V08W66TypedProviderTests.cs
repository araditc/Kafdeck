using Kafdeck.Core.Notifications;
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
