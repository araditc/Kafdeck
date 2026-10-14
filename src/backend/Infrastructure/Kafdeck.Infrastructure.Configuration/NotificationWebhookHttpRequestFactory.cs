using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Produces one closed HTTP request for an already admitted server-owned
/// Webhook profile and DNS-pinned safe-event dispatch. This class does not
/// perform I/O, resolve DNS, register a transport or schedule deliveries.
///
/// Credential attachment requires a separately reviewed, typed provider
/// authorization contract. Until then, refuse credential-bearing sends.
/// </summary>
public sealed class NotificationWebhookHttpRequestFactory
{
    private readonly NotificationDeliveryPolicy _policy;

    public NotificationWebhookHttpRequestFactory(NotificationDeliveryPolicy? policy = null)
    {
        _policy = policy ?? new NotificationDeliveryPolicy();
    }

    public HttpRequestMessage Create(
        NotificationDestinationProfile profile,
        NotificationPinnedWebhookRequest approvedRequest)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(approvedRequest);

        if (profile.Provider != NotificationProviderKind.Webhook ||
            profile.ConfiguredEndpoint is null ||
            !string.Equals(
                profile.DestinationId,
                approvedRequest.DestinationId,
                StringComparison.Ordinal) ||
            !string.Equals(
                profile.RevisionFingerprint,
                approvedRequest.ProfileRevisionFingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                profile.ConfiguredEndpoint.AbsoluteUri,
                approvedRequest.PinnedEndpoint.Endpoint.AbsoluteUri,
                StringComparison.Ordinal))
        {
            throw new KafdeckConfigurationException(
                "Webhook request does not match the approved destination identity and revision.");
        }

        if (approvedRequest.Credential is not null)
        {
            // An opaque credential value does NOT tell us whether this
            // provider expects Bearer, HMAC, API key or another scheme.
            // Guessing an Authorization header would risk credential leaks.
            throw new KafdeckConfigurationException(
                "Webhook credential authentication is not yet admitted for physical HTTP transport.");
        }

        if (approvedRequest.JsonPayload.Length is < 1 or >
            NotificationDeliveryPolicy.HardMaxPayloadBytes ||
            approvedRequest.JsonPayload.Length > _policy.MaxPayloadBytes)
        {
            throw new KafdeckConfigurationException(
                "Approved webhook payload exceeds the configured finite transport budget.");
        }

        // No caller-controlled URL, method, query, Host, Authorization or
        // other request headers. Future physical transport MUST compose
        // this request with the pinned per-request guard and TCP dialer.
        var message = new HttpRequestMessage(
            HttpMethod.Post, profile.ConfiguredEndpoint)
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent(approvedRequest.JsonPayload.ToArray()),
        };
        message.Content.Headers.ContentType =
            new MediaTypeHeaderValue("application/json");
        return message;
    }
}
