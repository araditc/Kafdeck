using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Kafdeck.Core.Notifications;

public sealed record NotificationWebhookEvent
{
    public const int MaxEventTypeLength = 128;
    public const int MaxSubjectLength = 256;
    public const int MaxSummaryLength = 8_192;

    public NotificationWebhookEvent(
        Guid eventId,
        NotificationEventClass eventClass,
        string eventType,
        string subject,
        string summary,
        DateTimeOffset occurredAtUtc)
    {
        if (eventId == Guid.Empty)
        {
            throw new ArgumentOutOfRangeException(
                nameof(eventId));
        }

        if (!Enum.IsDefined(eventClass))
        {
            throw new ArgumentOutOfRangeException(
                nameof(eventClass));
        }

        EventType = RequireSafeText(
            eventType,
            MaxEventTypeLength,
            nameof(eventType));
        Subject = RequireSafeText(
            subject,
            MaxSubjectLength,
            nameof(subject));
        Summary = RequireSafeText(
            summary,
            MaxSummaryLength,
            nameof(summary));

        if (occurredAtUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(occurredAtUtc));
        }

        EventId = eventId;
        EventClass = eventClass;
        OccurredAtUtc =
            occurredAtUtc.ToUniversalTime();
    }

    public Guid EventId { get; }
    public NotificationEventClass EventClass { get; }
    public string EventType { get; }
    public string Subject { get; }
    public string Summary { get; }
    public DateTimeOffset OccurredAtUtc { get; }

    private static string RequireSafeText(
        string value,
        int maxLength,
        string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.Length < 1 ||
            value.Length > maxLength ||
            !string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Notification webhook event text must be exact, bounded and control-character free.",
                parameterName);
        }

        return value;
    }
}

public enum NotificationWebhookTransportOutcome
{
    Delivered = 1,
    RetryableFailure = 2,
    PermanentFailure = 3,
    UnknownExternalEffect = 4,
}

public sealed record NotificationWebhookTransportResult
{
    public NotificationWebhookTransportResult(
        NotificationWebhookTransportOutcome outcome,
        string outcomeCode,
        string? providerRequestId = null)
    {
        if (!Enum.IsDefined(outcome))
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome));
        }

        OutcomeCode =
            RequireSafeMetadata(
                outcomeCode,
                128,
                nameof(outcomeCode))!;
        ProviderRequestId =
            RequireSafeMetadata(
                providerRequestId,
                256,
                nameof(providerRequestId));
        Outcome = outcome;
    }

    public NotificationWebhookTransportOutcome Outcome { get; }
    public string OutcomeCode { get; }
    public string? ProviderRequestId { get; }

    private static string? RequireSafeMetadata(
        string? value,
        int maxLength,
        string parameterName)
    {
        if (value is null)
        {
            return null;
        }

        if (value.Length < 1 ||
            value.Length > maxLength ||
            !string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Notification webhook transport metadata is invalid.",
                parameterName);
        }

        return value;
    }
}

public sealed record NotificationPinnedWebhookRequest
{
    public NotificationPinnedWebhookRequest(
        string destinationId,
        string profileRevisionFingerprint,
        NotificationPinnedEndpoint pinnedEndpoint,
        ReadOnlyMemory<byte> jsonPayload,
        NotificationCredentialValue? credential)
    {
        DestinationId =
            NotificationDestinationProfile
                .NormalizeDestinationId(
                    destinationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            profileRevisionFingerprint);
        ArgumentNullException.ThrowIfNull(
            pinnedEndpoint);

        if (profileRevisionFingerprint.Length != 64 ||
            profileRevisionFingerprint.Any(
                character =>
                    !Uri.IsHexDigit(character)))
        {
            throw new ArgumentException(
                "Notification profile revision fingerprint must be a SHA-256 hex digest.",
                nameof(profileRevisionFingerprint));
        }

        if (jsonPayload.Length is < 1 or >
            NotificationDeliveryPolicy.HardMaxPayloadBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(jsonPayload));
        }

        ProfileRevisionFingerprint =
            profileRevisionFingerprint
                .ToLowerInvariant();
        PinnedEndpoint =
            pinnedEndpoint;
        JsonPayload =
            new ReadOnlyMemory<byte>(
                jsonPayload.ToArray());
        Credential =
            credential;
    }

    public string DestinationId { get; }
    public string ProfileRevisionFingerprint { get; }
    public NotificationPinnedEndpoint PinnedEndpoint { get; }
    public ReadOnlyMemory<byte> JsonPayload { get; }
    public NotificationCredentialValue? Credential { get; }
    public string ContentType => "application/json";
}

public interface INotificationPinnedWebhookTransport
{
    Task<NotificationWebhookTransportResult> SendAsync(
        NotificationPinnedWebhookRequest request,
        CancellationToken cancellationToken);
}

public sealed record NotificationWebhookDispatchResult(
    string PayloadFingerprint,
    NotificationWebhookTransportResult TransportResult);

public sealed class WebhookNotificationAdapter
{
    private readonly INotificationEndpointResolutionPort
        _endpointResolver;
    private readonly INotificationCredentialResolver
        _credentialResolver;
    private readonly INotificationPinnedWebhookTransport
        _transport;
    private readonly NotificationDeliveryPolicy
        _deliveryPolicy;

    public WebhookNotificationAdapter(
        INotificationEndpointResolutionPort endpointResolver,
        INotificationCredentialResolver credentialResolver,
        INotificationPinnedWebhookTransport transport,
        NotificationDeliveryPolicy? deliveryPolicy = null)
    {
        _endpointResolver =
            endpointResolver ??
            throw new ArgumentNullException(
                nameof(endpointResolver));
        _credentialResolver =
            credentialResolver ??
            throw new ArgumentNullException(
                nameof(credentialResolver));
        _transport =
            transport ??
            throw new ArgumentNullException(
                nameof(transport));
        _deliveryPolicy =
            deliveryPolicy ??
            new NotificationDeliveryPolicy();
    }

    public async Task<NotificationWebhookDispatchResult>
        DispatchAsync(
            NotificationDestinationProfile profile,
            NotificationWebhookEvent notificationEvent,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(notificationEvent);
        cancellationToken.ThrowIfCancellationRequested();

        if (profile.Provider !=
            NotificationProviderKind.Webhook)
        {
            throw new ArgumentException(
                "Webhook adapter accepts only webhook destination profiles.",
                nameof(profile));
        }

        if (profile.ConfiguredEndpoint is null)
        {
            throw new ArgumentException(
                "Webhook destination is missing its configured endpoint.",
                nameof(profile));
        }

        if (!profile.EnabledEvents.Contains(
                notificationEvent.EventClass))
        {
            throw new InvalidOperationException(
                "Webhook destination does not admit this event class.");
        }

        var payload =
            JsonSerializer.SerializeToUtf8Bytes(
                new
                {
                    eventId =
                        notificationEvent.EventId,
                    eventClass =
                        notificationEvent.EventClass
                            .ToString(),
                    eventType =
                        notificationEvent.EventType,
                    subject =
                        notificationEvent.Subject,
                    summary =
                        notificationEvent.Summary,
                    occurredAtUtc =
                        notificationEvent
                            .OccurredAtUtc,
                });

        if (payload.Length >
            _deliveryPolicy.MaxPayloadBytes)
        {
            throw new InvalidOperationException(
                "Projected webhook payload exceeds the configured delivery payload ceiling.");
        }

        var pinned =
            await _endpointResolver
                .ResolveForConnectionAsync(
                    profile.ConfiguredEndpoint,
                    cancellationToken)
                .ConfigureAwait(false);

        if (!string.Equals(
                pinned.Endpoint.AbsoluteUri,
                profile.ConfiguredEndpoint.AbsoluteUri,
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Notification endpoint resolver returned a different endpoint than the configured destination.");
        }

        NotificationCredentialValue?
            credential = null;
        if (profile.CredentialBindingId is not null)
        {
            credential =
                await _credentialResolver
                    .ResolveAsync(
                        new NotificationCredentialResolutionRequest(
                            profile),
                        cancellationToken)
                    .ConfigureAwait(false);
        }

        cancellationToken.ThrowIfCancellationRequested();

        var transportResult =
            await _transport
                .SendAsync(
                    new NotificationPinnedWebhookRequest(
                        profile.DestinationId,
                        profile.RevisionFingerprint,
                        pinned,
                        payload,
                        credential),
                    cancellationToken)
                .ConfigureAwait(false);

        var fingerprint =
            Convert
                .ToHexString(
                    SHA256.HashData(
                        payload))
                .ToLowerInvariant();

        return new NotificationWebhookDispatchResult(
            fingerprint,
            transportResult);
    }
}
