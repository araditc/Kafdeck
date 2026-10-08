using System.Globalization;

namespace Kafdeck.Core.Notifications;

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
    NotificationProviderTransportResult TransportResult)
{
    public NotificationDeliveryDispatchOutcome DeliveryOutcome =>
        NotificationProviderOutcomeMapper.ToDeliveryOutcome(
            TransportResult.Outcome);
}

public static class NotificationProviderOutcomeMapper
{
    public static NotificationDeliveryDispatchOutcome
        ToDeliveryOutcome(
            NotificationProviderTransportOutcome outcome) =>
        outcome switch
        {
            NotificationProviderTransportOutcome.Delivered =>
                NotificationDeliveryDispatchOutcome.Delivered,
            NotificationProviderTransportOutcome.RetryableFailure =>
                NotificationDeliveryDispatchOutcome.RetryableFailure,
            NotificationProviderTransportOutcome.PermanentFailure =>
                NotificationDeliveryDispatchOutcome.PermanentFailure,
            NotificationProviderTransportOutcome.UnknownExternalEffect =>
                NotificationDeliveryDispatchOutcome.UnknownExternalEffect,
            _ => throw new ArgumentOutOfRangeException(
                nameof(outcome)),
        };

    public static NotificationDeliveryDispatchOutcome
        ToDeliveryOutcome(
            NotificationWebhookTransportOutcome outcome) =>
        outcome switch
        {
            NotificationWebhookTransportOutcome.Delivered =>
                NotificationDeliveryDispatchOutcome.Delivered,
            NotificationWebhookTransportOutcome.RetryableFailure =>
                NotificationDeliveryDispatchOutcome.RetryableFailure,
            NotificationWebhookTransportOutcome.PermanentFailure =>
                NotificationDeliveryDispatchOutcome.PermanentFailure,
            NotificationWebhookTransportOutcome.UnknownExternalEffect =>
                NotificationDeliveryDispatchOutcome.UnknownExternalEffect,
            _ => throw new ArgumentOutOfRangeException(
                nameof(outcome)),
        };
}

public sealed record EmailNotificationDestination
{
    public const int MaxAddressLength = 254;

    public EmailNotificationDestination(
        NotificationDestinationProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

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

        if (profile.EmailRecipientAddress is null)
        {
            throw new ArgumentException(
                "Email recipient must be configured and revision-bound to its server-owned destination profile.",
                nameof(profile));
        }

        Profile = profile;
        RecipientAddress = profile.EmailRecipientAddress;
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
            notificationEvent.PayloadFingerprint,
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
            notificationEvent.PayloadFingerprint,
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

public sealed class NotificationProviderDeliveryDispatcher :
    INotificationDeliveryDispatcher
{
    private readonly INotificationRoutingStore _routingStore;
    private readonly INotificationDestinationProfileCatalog _profiles;
    private readonly EmailNotificationAdapter _email;
    private readonly SlackNotificationAdapter _slack;
    private readonly TeamsNotificationAdapter _teams;
    private readonly TelegramNotificationAdapter _telegram;
    private readonly PagerDutyNotificationAdapter _pagerDuty;
    private readonly WebhookNotificationDeliveryDispatcher _webhook;

    public NotificationProviderDeliveryDispatcher(
        INotificationRoutingStore routingStore,
        INotificationDestinationProfileCatalog profiles,
        EmailNotificationAdapter email,
        SlackNotificationAdapter slack,
        TeamsNotificationAdapter teams,
        TelegramNotificationAdapter telegram,
        PagerDutyNotificationAdapter pagerDuty,
        WebhookNotificationDeliveryDispatcher webhook)
    {
        _routingStore = routingStore ??
            throw new ArgumentNullException(nameof(routingStore));
        _profiles = profiles ??
            throw new ArgumentNullException(nameof(profiles));
        _email = email ??
            throw new ArgumentNullException(nameof(email));
        _slack = slack ??
            throw new ArgumentNullException(nameof(slack));
        _teams = teams ??
            throw new ArgumentNullException(nameof(teams));
        _telegram = telegram ??
            throw new ArgumentNullException(nameof(telegram));
        _pagerDuty = pagerDuty ??
            throw new ArgumentNullException(nameof(pagerDuty));
        _webhook = webhook ??
            throw new ArgumentNullException(nameof(webhook));
    }

    public async Task<NotificationDeliveryDispatchResult> DispatchAsync(
        NotificationDeliveryRecord claimedDelivery,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(claimedDelivery);
        cancellationToken.ThrowIfCancellationRequested();

        var stored = await _routingStore.GetEventAsync(
                claimedDelivery.Snapshot.NotificationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (stored is null ||
            !stored.Event.IsApprovedForDurability ||
            !string.Equals(
                stored.Event.PayloadFingerprint,
                claimedDelivery.Snapshot.PayloadFingerprint,
                StringComparison.Ordinal))
        {
            return new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.PermanentFailure);
        }

        var profile = await _profiles.GetAsync(
                claimedDelivery.Snapshot.DestinationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (profile is null ||
            !string.Equals(
                profile.DestinationId,
                claimedDelivery.Snapshot.DestinationId,
                StringComparison.Ordinal) ||
            !profile.EnabledEvents.Contains(stored.Event.EventClass))
        {
            return new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.PermanentFailure);
        }

        // A routed destination/credential/recipient is immutable across
        // every attempt. Never send with a newly selected profile revision.
        if (claimedDelivery.Snapshot.RoutedProfileRevisionFingerprint is null ||
            !string.Equals(
                claimedDelivery.Snapshot.RoutedProfileRevisionFingerprint,
                profile.RevisionFingerprint,
                StringComparison.Ordinal))
        {
            return new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.UnknownExternalEffect);
        }

        // The existing Webhook dispatcher is a required dependency.
        if (profile.Provider == NotificationProviderKind.Webhook)
        {
            return await _webhook.DispatchAsync(
                    claimedDelivery,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        // Profiles for typed providers must have an approved opaque
        // credential binding; never substitute an arbitrary request target.
        if (profile.CredentialBindingId is null)
        {
            return new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.PermanentFailure);
        }

        var notificationEvent = stored.Event;
        NotificationProviderDispatchResult result =
            profile.Provider switch
            {
                NotificationProviderKind.Email =>
                    await _email.DispatchAsync(
                            new EmailNotificationDestination(profile),
                            notificationEvent,
                            cancellationToken)
                        .ConfigureAwait(false),
                NotificationProviderKind.Slack =>
                    await _slack.DispatchAsync(
                            new BoundNotificationDestination(
                                profile,
                                NotificationProviderKind.Slack),
                            notificationEvent,
                            cancellationToken)
                        .ConfigureAwait(false),
                NotificationProviderKind.MicrosoftTeams =>
                    await _teams.DispatchAsync(
                            new BoundNotificationDestination(
                                profile,
                                NotificationProviderKind.MicrosoftTeams),
                            notificationEvent,
                            cancellationToken)
                        .ConfigureAwait(false),
                NotificationProviderKind.Telegram =>
                    await _telegram.DispatchAsync(
                            new BoundNotificationDestination(
                                profile,
                                NotificationProviderKind.Telegram),
                            notificationEvent,
                            cancellationToken)
                        .ConfigureAwait(false),
                NotificationProviderKind.PagerDuty =>
                    await _pagerDuty.DispatchAsync(
                            new BoundNotificationDestination(
                                profile,
                                NotificationProviderKind.PagerDuty),
                            notificationEvent,
                            cancellationToken)
                        .ConfigureAwait(false),
                _ => throw new InvalidOperationException(
                    "Notification destination provider is not admitted."),
            };

        if (!string.Equals(
                result.PayloadFingerprint,
                claimedDelivery.Snapshot.PayloadFingerprint,
                StringComparison.Ordinal))
        {
            // Transport may have taken effect; NEVER blindly retry.
            return new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.UnknownExternalEffect);
        }

        return new NotificationDeliveryDispatchResult(
            result.DeliveryOutcome);
    }
}
