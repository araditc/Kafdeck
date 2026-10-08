using System.Net;
using System.Text.Json;
using Kafdeck.Core.Notifications;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66WebhookRuntimeTests
{
    [Fact]
    public async Task Adapter_uses_only_configured_pinned_endpoint_and_opaque_credential_binding()
    {
        var profile =
            WebhookProfile(
                credentialBindingId:
                    new NotificationCredentialBindingId(
                        "notifications.ops-webhook"));
        var resolver =
            new FakeEndpointResolver();
        var credentials =
            new FakeCredentialResolver();
        var transport =
            new FakeWebhookTransport();
        var adapter =
            new WebhookNotificationAdapter(
                resolver,
                credentials,
                transport);

        var result =
            await adapter.DispatchAsync(
                profile,
                Event());

        Assert.Equal(
            profile.ConfiguredEndpoint,
            resolver.LastEndpoint);
        Assert.NotNull(
            credentials.LastRequest);
        Assert.Equal(
            "notifications.ops-webhook",
            credentials.LastRequest!.BindingId.Value);
        Assert.Equal(
            profile.RevisionFingerprint,
            credentials.LastRequest
                .ProfileRevisionFingerprint);

        var request =
            Assert.IsType<NotificationPinnedWebhookRequest>(
                transport.LastRequest);
        Assert.Equal(
            profile.ConfiguredEndpoint,
            request.PinnedEndpoint.Endpoint);
        Assert.Equal(
            "application/json",
            request.ContentType);
        Assert.NotNull(
            request.Credential);
        Assert.Equal(
            "[redacted-notification-credential]",
            request.Credential!.ToString());

        using var document =
            JsonDocument.Parse(
                request.JsonPayload);
        Assert.Equal(
            "DataQuality",
            document.RootElement
                .GetProperty("eventClass")
                .GetString());
        Assert.False(
            document.RootElement
                .TryGetProperty(
                    "credential",
                    out _));
        Assert.Equal(
            NotificationWebhookTransportOutcome.Delivered,
            result.TransportResult.Outcome);
        Assert.Equal(
            64,
            result.PayloadFingerprint.Length);
    }

    [Fact]
    public async Task Adapter_rejects_disabled_event_before_resolution_or_transport()
    {
        var profile =
            new NotificationDestinationProfile(
                "ops-webhook",
                NotificationProviderKind.Webhook,
                "Operations",
                [NotificationEventClass.Operational],
                new Uri(
                    "https://hooks.example.com/events"));
        var resolver =
            new FakeEndpointResolver();
        var transport =
            new FakeWebhookTransport();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () =>
                new WebhookNotificationAdapter(
                        resolver,
                        new FakeCredentialResolver(),
                        transport)
                    .DispatchAsync(
                        profile,
                        Event()));

        Assert.Equal(
            0,
            resolver.Calls);
        Assert.Equal(
            0,
            transport.Calls);
    }

    [Fact]
    public async Task Adapter_rejects_non_webhook_profile_without_touching_network_boundary()
    {
        var profile =
            new NotificationDestinationProfile(
                "ops-email",
                NotificationProviderKind.Email,
                "Operations email",
                [NotificationEventClass.DataQuality],
                emailRecipientAddress: "ops@example.com");
        var resolver =
            new FakeEndpointResolver();
        var transport =
            new FakeWebhookTransport();

        await Assert.ThrowsAsync<ArgumentException>(
            () =>
                new WebhookNotificationAdapter(
                        resolver,
                        new FakeCredentialResolver(),
                        transport)
                    .DispatchAsync(
                        profile,
                        Event()));

        Assert.Equal(
            0,
            resolver.Calls);
        Assert.Equal(
            0,
            transport.Calls);
    }

    [Fact]
    public async Task Adapter_enforces_payload_ceiling_before_resolution()
    {
        var resolver =
            new FakeEndpointResolver();
        var transport =
            new FakeWebhookTransport();
        var adapter =
            new WebhookNotificationAdapter(
                resolver,
                new FakeCredentialResolver(),
                transport,
                new NotificationDeliveryPolicy(
                    maxPayloadBytes: 256));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () =>
                adapter.DispatchAsync(
                    WebhookProfile(),
                    new NotificationWebhookEvent(
                        Guid.NewGuid(),
                        NotificationEventClass.DataQuality,
                        "data-quality.violation",
                        "Orders quality",
                        new string(
                            'x',
                            1_000),
                        DateTimeOffset.UtcNow)));

        Assert.Equal(
            0,
            resolver.Calls);
        Assert.Equal(
            0,
            transport.Calls);
    }

    [Fact]
    public async Task Adapter_rejects_resolver_endpoint_substitution()
    {
        var transport =
            new FakeWebhookTransport();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () =>
                new WebhookNotificationAdapter(
                        new FakeEndpointResolver(
                            new Uri(
                                "https://other.example.com/events")),
                        new FakeCredentialResolver(),
                        transport)
                    .DispatchAsync(
                        WebhookProfile(),
                        Event()));

        Assert.Equal(
            0,
            transport.Calls);
    }

    private static NotificationDestinationProfile
        WebhookProfile(
            NotificationCredentialBindingId?
                credentialBindingId = null) =>
        new(
            "ops-webhook",
            NotificationProviderKind.Webhook,
            "Operations",
            [NotificationEventClass.DataQuality],
            new Uri(
                "https://hooks.example.com/events"),
            credentialBindingId);

    private static NotificationWebhookEvent Event() =>
        new(
            Guid.NewGuid(),
            NotificationEventClass.DataQuality,
            "data-quality.violation",
            "Orders quality",
            "Required field violation count exceeded the configured policy.",
            DateTimeOffset.UtcNow);

    private sealed class FakeEndpointResolver :
        INotificationEndpointResolutionPort
    {
        private readonly Uri?
            _substituteEndpoint;

        public FakeEndpointResolver(
            Uri? substituteEndpoint = null)
        {
            _substituteEndpoint =
                substituteEndpoint;
        }

        public int Calls { get; private set; }
        public Uri? LastEndpoint { get; private set; }

        public Task<NotificationPinnedEndpoint>
            ResolveForConnectionAsync(
                Uri configuredEndpoint,
                CancellationToken cancellationToken)
        {
            Calls++;
            LastEndpoint =
                configuredEndpoint;

            return Task.FromResult(
                NotificationAddressPolicy
                    .NoConfiguredNat64
                    .ValidatePinnedEndpoint(
                        _substituteEndpoint ??
                        configuredEndpoint,
                        [
                            IPAddress.Parse(
                                "1.1.1.1"),
                        ],
                        DateTimeOffset.UtcNow));
        }
    }

    private sealed class FakeCredentialResolver :
        INotificationCredentialResolver
    {
        public NotificationCredentialResolutionRequest?
            LastRequest { get; private set; }

        public ValueTask<NotificationCredentialValue>
            ResolveAsync(
                NotificationCredentialResolutionRequest request,
                CancellationToken cancellationToken)
        {
            LastRequest =
                request;
            return ValueTask.FromResult(
                new NotificationCredentialValue(
                    "secret"));
        }
    }

    private sealed class FakeWebhookTransport :
        INotificationPinnedWebhookTransport
    {
        public int Calls { get; private set; }
        public NotificationPinnedWebhookRequest?
            LastRequest { get; private set; }

        public Task<NotificationWebhookTransportResult>
            SendAsync(
                NotificationPinnedWebhookRequest request,
                CancellationToken cancellationToken)
        {
            Calls++;
            LastRequest =
                request;
            return Task.FromResult(
                new NotificationWebhookTransportResult(
                    NotificationWebhookTransportOutcome.Delivered,
                    "http-2xx",
                    "request-1"));
        }
    }
}
