using System.Globalization;
using System.Net.Mail;
using System.Security.Cryptography;
using System.Text;

namespace Kafdeck.Core.Notifications;

public sealed record NotificationSafeEvent
{
    public NotificationSafeEvent(
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
            NotificationWebhookEvent.MaxEventTypeLength,
            nameof(eventType));
        Subject = RequireSafeText(
            subject,
            NotificationWebhookEvent.MaxSubjectLength,
            nameof(subject));
        Summary = RequireSafeText(
            summary,
            NotificationWebhookEvent.MaxSummaryLength,
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

    public static NotificationSafeEvent FromWebhook(
        NotificationWebhookEvent value)
    {
        ArgumentNullException.ThrowIfNull(value);

        return new NotificationSafeEvent(
            value.EventId,
            value.EventClass,
            value.EventType,
            value.Subject,
            value.Summary,
            value.OccurredAtUtc);
    }

    private static string RequireSafeText(
        string value,
        int maxLength,
        string parameterName)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.Length is < 1 ||
            value.Length > maxLength ||
            !string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Notification event text must be exact, bounded and control-character free.",
                parameterName);
        }

        return value;
    }
}

public enum NotificationProviderTransportOutcome
{
    Delivered = 1,
    RetryableFailure = 2,
    PermanentFailure = 3,
    UnknownExternalEffect = 4,
}

public sealed record NotificationProviderTransportResult
{
    public NotificationProviderTransportResult(
        NotificationProviderTransportOutcome outcome,
        string outcomeCode,
        string? providerRequestId = null)
    {
        if (!Enum.IsDefined(outcome))
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome));
        }

        Outcome =
            outcome;
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
    }

    public NotificationProviderTransportOutcome Outcome { get; }
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

        if (value.Length is < 1 ||
            value.Length > maxLength ||
            !string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Notification provider result metadata is invalid.",
                parameterName);
        }

        return value;
    }
}

public sealed record NotificationProviderDispatchResult(
    string PayloadFingerprint,
    NotificationProviderTransportResult TransportResult);

public sealed record EmailNotificationDestination
{
    public const int MaxAddressLength = 254;

    public EmailNotificationDestination(
        NotificationDestinationProfile profile,
        string recipientAddress)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(
            recipientAddress);

        if (profile.Provider !=
            NotificationProviderKind.Email)
        {
            throw new ArgumentException(
                "Email destination requires an Email notification profile.",
                nameof(profile));
        }

        if (profile.CredentialBindingId is null)
        {
            throw new ArgumentException(
                "Email destination requires an opaque credential binding.",
                nameof(profile));
        }

        if (recipientAddress.Length is < 3 or >
                MaxAddressLength ||
            !string.Equals(
                recipientAddress,
                recipientAddress.Trim(),
                StringComparison.Ordinal) ||
            recipientAddress.Any(char.IsControl) ||
            !MailAddress.TryCreate(
                recipientAddress,
                out var parsed) ||
            !string.Equals(
                parsed.Address,
                recipientAddress,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Email recipient must be one exact bounded address without display-name or control metadata.",
                nameof(recipientAddress));
        }

        Profile = profile;
        RecipientAddress =
            recipientAddress;
    }

    public NotificationDestinationProfile Profile { get; }
    public string RecipientAddress { get; }
}

public sealed record BoundNotificationDestination
{
    public BoundNotificationDestination(
        NotificationDestinationProfile profile,
        NotificationProviderKind expectedProvider)
    {
        ArgumentNullException.ThrowIfNull(profile);

        if (expectedProvider is
            NotificationProviderKind.Webhook or
            NotificationProviderKind.Email ||
            !Enum.IsDefined(expectedProvider) ||
            profile.Provider != expectedProvider)
        {
            throw new ArgumentException(
                "Bound provider destination does not match the expected typed provider.",
                nameof(profile));
        }

        if (profile.CredentialBindingId is null)
        {
            throw new ArgumentException(
                "Bound provider destination requires an opaque credential binding.",
                nameof(profile));
        }

        Profile = profile;
        Provider =
            expectedProvider;
    }

    public NotificationDestinationProfile Profile { get; }
    public NotificationProviderKind Provider { get; }
}

public sealed record NotificationEmailTransportRequest(
    string DestinationId,
    string ProfileRevisionFingerprint,
    string RecipientAddress,
    string Subject,
    string Body,
    NotificationCredentialValue Credential);

public sealed record NotificationBoundTransportRequest(
    string DestinationId,
    string ProfileRevisionFingerprint,
    NotificationProviderKind Provider,
    NotificationSafeEvent Event,
    NotificationCredentialValue Credential);

public interface INotificationEmailTransport
{
    Task<NotificationProviderTransportResult> SendAsync(
        NotificationEmailTransportRequest request,
        CancellationToken cancellationToken);
}

public interface INotificationSlackTransport
{
    Task<NotificationProviderTransportResult> SendAsync(
        NotificationBoundTransportRequest request,
        CancellationToken cancellationToken);
}

public interface INotificationTeamsTransport
{
    Task<NotificationProviderTransportResult> SendAsync(
        NotificationBoundTransportRequest request,
        CancellationToken cancellationToken);
}

public interface INotificationTelegramTransport
{
    Task<NotificationProviderTransportResult> SendAsync(
        NotificationBoundTransportRequest request,
        CancellationToken cancellationToken);
}

public interface INotificationPagerDutyTransport
{
    Task<NotificationProviderTransportResult> SendAsync(
        NotificationBoundTransportRequest request,
        CancellationToken cancellationToken);
}

public sealed class EmailNotificationAdapter
{
    private readonly INotificationCredentialResolver
        _credentialResolver;
    private readonly INotificationEmailTransport
        _transport;

    public EmailNotificationAdapter(
        INotificationCredentialResolver credentialResolver,
        INotificationEmailTransport transport)
    {
        _credentialResolver =
            credentialResolver ??
            throw new ArgumentNullException(
                nameof(credentialResolver));
        _transport =
            transport ??
            throw new ArgumentNullException(
                nameof(transport));
    }

    public async Task<NotificationProviderDispatchResult>
        DispatchAsync(
            EmailNotificationDestination destination,
            NotificationSafeEvent notificationEvent,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(notificationEvent);

        ValidateEventAdmission(
            destination.Profile,
            notificationEvent);

        var credential =
            await _credentialResolver
                .ResolveAsync(
                    new NotificationCredentialResolutionRequest(
                        destination.Profile),
                    cancellationToken)
                .ConfigureAwait(false);

        var subject =
            $"[{notificationEvent.EventClass}] {notificationEvent.Subject}";
        if (subject.Length > 512)
        {
            throw new InvalidOperationException(
                "Projected email subject exceeds the bounded provider contract.");
        }

        var body =
            string.Join(
                "\n",
                notificationEvent.EventType,
                notificationEvent.Summary,
                notificationEvent.OccurredAtUtc.ToString(
                    "O",
                    CultureInfo.InvariantCulture),
                notificationEvent.EventId.ToString("D"));

        var result =
            await _transport
                .SendAsync(
                    new NotificationEmailTransportRequest(
                        destination.Profile.DestinationId,
                        destination.Profile.RevisionFingerprint,
                        destination.RecipientAddress,
                        subject,
                        body,
                        credential),
                    cancellationToken)
                .ConfigureAwait(false);

        return new NotificationProviderDispatchResult(
            Fingerprint(
                destination.Profile,
                notificationEvent,
                destination.RecipientAddress),
            result);
    }

    private static void ValidateEventAdmission(
        NotificationDestinationProfile profile,
        NotificationSafeEvent notificationEvent) =>
        ProviderNotificationAdapter.ValidateEventAdmission(
            profile,
            notificationEvent);
}

public sealed class SlackNotificationAdapter
{
    private readonly ProviderNotificationAdapter
        _inner;

    public SlackNotificationAdapter(
        INotificationCredentialResolver credentialResolver,
        INotificationSlackTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _inner =
            new ProviderNotificationAdapter(
                NotificationProviderKind.Slack,
                credentialResolver,
                transport.SendAsync);
    }

    public Task<NotificationProviderDispatchResult> DispatchAsync(
        BoundNotificationDestination destination,
        NotificationSafeEvent notificationEvent,
        CancellationToken cancellationToken = default) =>
        _inner.DispatchAsync(
            destination,
            notificationEvent,
            cancellationToken);
}

public sealed class TeamsNotificationAdapter
{
    private readonly ProviderNotificationAdapter
        _inner;

    public TeamsNotificationAdapter(
        INotificationCredentialResolver credentialResolver,
        INotificationTeamsTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _inner =
            new ProviderNotificationAdapter(
                NotificationProviderKind.MicrosoftTeams,
                credentialResolver,
                transport.SendAsync);
    }

    public Task<NotificationProviderDispatchResult> DispatchAsync(
        BoundNotificationDestination destination,
        NotificationSafeEvent notificationEvent,
        CancellationToken cancellationToken = default) =>
        _inner.DispatchAsync(
            destination,
            notificationEvent,
            cancellationToken);
}

public sealed class TelegramNotificationAdapter
{
    private readonly ProviderNotificationAdapter
        _inner;

    public TelegramNotificationAdapter(
        INotificationCredentialResolver credentialResolver,
        INotificationTelegramTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _inner =
            new ProviderNotificationAdapter(
                NotificationProviderKind.Telegram,
                credentialResolver,
                transport.SendAsync);
    }

    public Task<NotificationProviderDispatchResult> DispatchAsync(
        BoundNotificationDestination destination,
        NotificationSafeEvent notificationEvent,
        CancellationToken cancellationToken = default) =>
        _inner.DispatchAsync(
            destination,
            notificationEvent,
            cancellationToken);
}

public sealed class PagerDutyNotificationAdapter
{
    private readonly ProviderNotificationAdapter
        _inner;

    public PagerDutyNotificationAdapter(
        INotificationCredentialResolver credentialResolver,
        INotificationPagerDutyTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _inner =
            new ProviderNotificationAdapter(
                NotificationProviderKind.PagerDuty,
                credentialResolver,
                transport.SendAsync);
    }

    public Task<NotificationProviderDispatchResult> DispatchAsync(
        BoundNotificationDestination destination,
        NotificationSafeEvent notificationEvent,
        CancellationToken cancellationToken = default) =>
        _inner.DispatchAsync(
            destination,
            notificationEvent,
            cancellationToken);
}

internal sealed class ProviderNotificationAdapter
{
    private readonly NotificationProviderKind
        _provider;
    private readonly INotificationCredentialResolver
        _credentialResolver;
    private readonly Func<
        NotificationBoundTransportRequest,
        CancellationToken,
        Task<NotificationProviderTransportResult>>
        _send;

    public ProviderNotificationAdapter(
        NotificationProviderKind provider,
        INotificationCredentialResolver credentialResolver,
        Func<
            NotificationBoundTransportRequest,
            CancellationToken,
            Task<NotificationProviderTransportResult>> send)
    {
        if (provider is
            NotificationProviderKind.Webhook or
            NotificationProviderKind.Email ||
            !Enum.IsDefined(provider))
        {
            throw new ArgumentOutOfRangeException(
                nameof(provider));
        }

        _provider =
            provider;
        _credentialResolver =
            credentialResolver ??
            throw new ArgumentNullException(
                nameof(credentialResolver));
        _send =
            send ??
            throw new ArgumentNullException(
                nameof(send));
    }

    public async Task<NotificationProviderDispatchResult>
        DispatchAsync(
            BoundNotificationDestination destination,
            NotificationSafeEvent notificationEvent,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(notificationEvent);

        if (destination.Provider !=
            _provider)
        {
            throw new ArgumentException(
                "Typed notification adapter received the wrong provider destination.",
                nameof(destination));
        }

        ValidateEventAdmission(
            destination.Profile,
            notificationEvent);

        var credential =
            await _credentialResolver
                .ResolveAsync(
                    new NotificationCredentialResolutionRequest(
                        destination.Profile),
                    cancellationToken)
                .ConfigureAwait(false);

        var result =
            await _send(
                    new NotificationBoundTransportRequest(
                        destination.Profile.DestinationId,
                        destination.Profile.RevisionFingerprint,
                        _provider,
                        notificationEvent,
                        credential),
                    cancellationToken)
                .ConfigureAwait(false);

        return new NotificationProviderDispatchResult(
            Fingerprint(
                destination.Profile,
                notificationEvent,
                null),
            result);
    }

    internal static void ValidateEventAdmission(
        NotificationDestinationProfile profile,
        NotificationSafeEvent notificationEvent)
    {
        if (!profile.EnabledEvents.Contains(
                notificationEvent.EventClass))
        {
            throw new InvalidOperationException(
                "Notification destination does not admit this event class.");
        }
    }
}

internal static string Fingerprint(
    NotificationDestinationProfile profile,
    NotificationSafeEvent notificationEvent,
    string? target)
{
    var canonical =
        string.Join(
            "\n",
            profile.DestinationId,
            profile.Provider.ToString(),
            profile.RevisionFingerprint,
            target ?? string.Empty,
            notificationEvent.EventId.ToString("D"),
            notificationEvent.EventClass.ToString(),
            notificationEvent.EventType,
            notificationEvent.Subject,
            notificationEvent.Summary,
            notificationEvent.OccurredAtUtc.ToString(
                "O",
                CultureInfo.InvariantCulture));

    return Convert
        .ToHexString(
            SHA256.HashData(
                Encoding.UTF8.GetBytes(
                    canonical)))
        .ToLowerInvariant();
}
