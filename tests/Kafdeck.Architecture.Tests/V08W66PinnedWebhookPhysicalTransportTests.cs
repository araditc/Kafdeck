using System.Net;
using System.Net.Sockets;
using System.Text;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66PinnedWebhookPhysicalTransportTests
{
    private static NotificationDestinationProfile Profile(bool credential = false) =>
        new("ops-webhook", NotificationProviderKind.Webhook, "Operations",
            [NotificationEventClass.Operational],
            new Uri("https://hooks.example.org/notify"),
            credential ? new NotificationCredentialBindingId("ops-key") : null);

    private static NotificationPinnedWebhookRequest Approved(
        NotificationDestinationProfile profile,
        string? endpoint = null,
        byte[]? payload = null,
        NotificationCredentialValue? credential = null,
        DateTimeOffset? resolvedAtUtc = null)
    {
        var pin = NotificationAddressPolicy.NoConfiguredNat64.ValidatePinnedEndpoint(
            new Uri(endpoint ?? "https://hooks.example.org/notify"),
            [IPAddress.Parse("1.1.1.1"), IPAddress.Parse("8.8.8.8")],
            resolvedAtUtc ?? DateTimeOffset.UtcNow);
        return new NotificationPinnedWebhookRequest(
            profile.DestinationId, profile.RevisionFingerprint, pin,
            payload ?? Encoding.UTF8.GetBytes("{\"event\":\"safe\"}"),
            credential);
    }

    private sealed class RefusingDialer : INotificationPinnedTcpDialer
    {
        public List<string> Dialed { get; } = [];

        public ValueTask<Stream> DialAsync(
            IPAddress address, int port, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Dialed.Add(address + ":" + port);
            throw new SocketException((int)SocketError.HostUnreachable);
        }
    }

    private static NotificationPinnedWebhookHttpTransport Create(
        NotificationDestinationProfile profile, RefusingDialer dialer,
        NotificationDeliveryPolicy? policy = null) =>
        new(profile, new NotificationPinnedHttpHandlerFactory(
            dialer, NotificationAddressPolicy.NoConfiguredNat64), policy);

    [Fact]
    public async Task Credential_bound_destination_is_rejected_before_any_dial()
    {
        var dialer = new RefusingDialer();
        var profile = Profile(credential: true);
        var transport = Create(profile, dialer);
        var failure = await Assert.ThrowsAsync<KafdeckConfigurationException>(
            () => transport.SendAsync(Approved(profile), CancellationToken.None));

        Assert.Contains("not yet admitted", failure.Message,
            StringComparison.Ordinal);
        Assert.Empty(dialer.Dialed);
    }

    [Fact]
    public async Task Supplying_a_credential_to_anonymous_profile_is_rejected_before_dial()
    {
        var dialer = new RefusingDialer();
        var profile = Profile();
        var token = new NotificationCredentialValue("never-log-provider-credential");
        var failure = await Assert.ThrowsAsync<KafdeckConfigurationException>(
            () => Create(profile, dialer).SendAsync(
                Approved(profile, credential: token), CancellationToken.None));

        Assert.Empty(dialer.Dialed);
        Assert.DoesNotContain(token.Reveal(), failure.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Different_pinned_endpoint_is_rejected_without_socket_attempt()
    {
        var dialer = new RefusingDialer();
        var profile = Profile();
        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            () => Create(profile, dialer).SendAsync(
                Approved(profile, endpoint: "https://hooks.example.org/other"),
                CancellationToken.None));
        Assert.Empty(dialer.Dialed);
    }

    [Fact]
    public async Task Oversized_body_is_rejected_before_socket_attempt()
    {
        var dialer = new RefusingDialer();
        var profile = Profile();
        var body = Enumerable.Repeat((byte)'x',
            NotificationDeliveryPolicy.DefaultPayloadBytes + 1).ToArray();
        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            () => Create(profile, dialer).SendAsync(
                Approved(profile, payload: body), CancellationToken.None));
        Assert.Empty(dialer.Dialed);
    }

    [Fact]
    public async Task Explicit_cancellation_before_send_never_dials()
    {
        var dialer = new RefusingDialer();
        var profile = Profile();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Create(profile, dialer).SendAsync(
                Approved(profile), cancellation.Token));
        Assert.Empty(dialer.Dialed);
    }

    [Fact]
    public async Task Stale_pin_is_rejected_before_any_dial()
    {
        var dialer = new RefusingDialer();
        var profile = Profile();
        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            () => Create(profile, dialer).SendAsync(
                Approved(profile,
                    resolvedAtUtc: DateTimeOffset.UtcNow - TimeSpan.FromMinutes(1)),
                CancellationToken.None));
        Assert.Empty(dialer.Dialed);
    }

    [Fact]
    public async Task Failed_pinned_connection_has_unknown_effect_and_no_dns_fallback()
    {
        var dialer = new RefusingDialer();
        var profile = Profile();
        var result = await Create(profile, dialer).SendAsync(
            Approved(profile), CancellationToken.None);

        Assert.Equal(NotificationWebhookTransportOutcome.UnknownExternalEffect,
            result.Outcome);
        Assert.Equal("http_effect_ambiguous", result.OutcomeCode);
        Assert.Null(result.ProviderRequestId);
        Assert.NotEmpty(dialer.Dialed);
        Assert.All(dialer.Dialed, target =>
            Assert.Contains(target, new[] { "1.1.1.1:443", "8.8.8.8:443" }));
    }
}
