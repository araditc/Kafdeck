using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Sockets;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Physical Webhook HTTPS transport. Host DI activates it only through the
/// separate default-off standalone WebhookDeliveryEnabled admission.
///
/// Its exact owner-provisioned profile, immutable dispatch intent and
/// original DNS pin are the only permitted source of HTTP request material.
/// Provider-specific authentication is intentionally refused until admitted.
/// </summary>
public sealed class NotificationPinnedWebhookHttpTransport :
    INotificationPinnedWebhookTransport
{
    public static readonly TimeSpan HardRequestTimeout = TimeSpan.FromSeconds(15);

    private readonly NotificationDestinationProfile _profile;
    private readonly NotificationWebhookHttpRequestFactory _requests;
    private readonly NotificationPinnedHttpHandlerFactory _handlers;

    public NotificationPinnedWebhookHttpTransport(
        NotificationDestinationProfile profile,
        NotificationPinnedHttpHandlerFactory handlers,
        NotificationDeliveryPolicy? policy = null)
    {
        ArgumentNullException.ThrowIfNull(profile);
        if (profile.Provider != NotificationProviderKind.Webhook ||
            profile.ConfiguredEndpoint is null)
            throw new ArgumentException(
                "Physical webhook transport requires an exact configured HTTPS destination.",
                nameof(profile));
        _profile = profile;
        _handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
        _requests = new NotificationWebhookHttpRequestFactory(policy);
    }

    public async Task<NotificationWebhookTransportResult> SendAsync(
        NotificationPinnedWebhookRequest approved,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Authorization, revision, credential downgrade and finite payload
        // validation happen BEFORE any socket, handler or request creation.
        var intent = _requests.Create(_profile, approved);
        using var handler = _handlers.CreateGuardedHandler(intent.PinnedEndpoint);
        using var client = new HttpClient(handler, disposeHandler: false)
        {
            Timeout = HardRequestTimeout,
            DefaultRequestVersion = HttpVersion.Version11,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
        using var message = new HttpRequestMessage(HttpMethod.Post, intent.Endpoint)
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent(intent.JsonPayload.ToArray()),
        };
        message.Content.Headers.ContentType =
            new MediaTypeHeaderValue(intent.ContentType);

        // A newly allocated, private HttpClient has no user-supplied default
        // headers, cookies, credentials or sender-controlled request objects.
        // The guarded SocketsHttpHandler pins TCP, keeps normal TLS hostname
        // verification, and refuses redirects/proxies on every connection.
        // After SendAsync starts, even connect/timeout/cancellation errors are
        // conservatively UnknownExternalEffect: no blind ledger replay.
        try
        {
            using var response = await client.SendAsync(
                message, HttpCompletionOption.ResponseHeadersRead,
                cancellationToken).ConfigureAwait(false);
            return NotificationWebhookHttpOutcomePolicy.FromResponse(
                (int)response.StatusCode);
        }
        catch (Exception error)
            when (error is HttpRequestException or IOException or
                  SocketException or OperationCanceledException or
                  KafdeckConfigurationException)
        {
            // Never use exception.Message, destination, provider request IDs,
            // body or headers as a durable/logged error code.
            return NotificationWebhookHttpOutcomePolicy.AfterPhysicalSendUncertain();
        }
    }
}
