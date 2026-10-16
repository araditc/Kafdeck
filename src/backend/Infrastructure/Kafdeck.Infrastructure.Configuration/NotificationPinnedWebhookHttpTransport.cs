using System.Net.Sockets;
using System.Security.Authentication;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Physical Webhook HTTPS transport. Host DI activates it only through the
/// separate default-off standalone WebhookDeliveryEnabled admission.
///
/// Its exact owner-provisioned profile, immutable dispatch intent and
/// original DNS pin are the only permitted source of HTTP request material.
/// Explicit Anonymous/Bearer authentication is revision-bound; no guessed
/// credential/header semantics or anonymous fallback is permitted.
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
        _handlers.VerifyPinned(intent.PinnedEndpoint);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(HardRequestTimeout);

        // The private wire has no user-supplied headers, cookies or mutable
        // request objects. It pins TCP, retains normal TLS/SNI checks and
        // never redirects, proxies, reuses another path or enters HttpClient
        // URL telemetry (configured paths can themselves be confidential).
        // After SendAsync starts, even connect/timeout/cancellation errors are
        // conservatively UnknownExternalEffect: no blind ledger replay.
        try
        {
            var response = await NotificationPrivateHttpWire.PostAsync(_handlers, intent.PinnedEndpoint,
                intent.JsonPayload.ToArray(), deadline.Token, readBody: false,
                bearer: intent.Authentication == NotificationWebhookAuthentication.Bearer ? intent.Credential : null, profile: _profile).ConfigureAwait(false);
            return NotificationWebhookHttpOutcomePolicy.FromResponse(
                response.Status);
        }
        catch (Exception error)
            when (error is IOException or AuthenticationException or
                  SocketException or OperationCanceledException or
                  KafdeckConfigurationException)
        {
            // Never use exception.Message, destination, provider request IDs,
            // body or headers as a durable/logged error code.
            return NotificationWebhookHttpOutcomePolicy.AfterPhysicalSendUncertain();
        }
    }
}
