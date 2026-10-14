using System.Collections.Immutable;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Immutable, data-only authorization intent for one server-owned,
/// revision-bound Webhook event. No HttpRequestMessage or mutable request
/// headers escape this boundary. A separately admitted transport must
/// enforce the intent during physical request construction and sending.
/// </summary>
public sealed class NotificationWebhookHttpIntent
{
    internal NotificationWebhookHttpIntent(
        string destinationId,
        string profileRevisionFingerprint,
        Uri endpoint,
        ImmutableArray<byte> jsonPayload)
    {
        DestinationId = destinationId;
        ProfileRevisionFingerprint = profileRevisionFingerprint;
        Endpoint = endpoint;
        JsonPayload = jsonPayload;
    }

    public string DestinationId { get; }
    public string ProfileRevisionFingerprint { get; }
    public Uri Endpoint { get; }
    public string Method => "POST";
    public string ContentType => "application/json";
    public ImmutableArray<byte> JsonPayload { get; }
}

/// <summary>
/// Validates a pinned Webhook dispatch and produces only immutable intent.
/// Does not create a mutable HttpRequestMessage, perform network I/O,
/// attach provider credentials or start a delivery worker.
/// </summary>
public sealed class NotificationWebhookHttpRequestFactory
{
    private readonly NotificationDeliveryPolicy _policy;

    public NotificationWebhookHttpRequestFactory(
        NotificationDeliveryPolicy? policy = null)
    {
        _policy = policy ?? new NotificationDeliveryPolicy();
    }

    public NotificationWebhookHttpIntent Create(
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

        if (profile.CredentialBindingId is not null ||
            approvedRequest.Credential is not null)
        {
            // Opaque credential values cannot determine Bearer/HMAC/API-key
            // semantics. Do not downgrade a bound destination to anonymous.
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

        // ImmutableArray does not expose the backing mutable byte array.
        // Even direct callers cannot change HTTP method, Host, headers or
        // authorization because no mutable HTTP request is returned.
        return new NotificationWebhookHttpIntent(
            profile.DestinationId,
            profile.RevisionFingerprint,
            profile.ConfiguredEndpoint,
            ImmutableArray.CreateRange(approvedRequest.JsonPayload.ToArray()));
    }
}
