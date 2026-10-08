using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

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
        PayloadFingerprint =
            ComputePayloadFingerprint();
    }

    public Guid EventId { get; }
    public NotificationEventClass EventClass { get; }
    public string EventType { get; }
    public string Subject { get; }
    public string Summary { get; }
    public DateTimeOffset OccurredAtUtc { get; }
    public string PayloadFingerprint { get; }

    public ReadOnlyMemory<byte> ProjectJsonPayload() =>
        NotificationSafeEventProjection.Serialize(
            this);

    public NotificationWebhookEvent ToWebhookEvent() =>
        new(
            EventId,
            EventClass,
            EventType,
            Subject,
            Summary,
            OccurredAtUtc);

    private string ComputePayloadFingerprint() =>
        Convert
            .ToHexString(
                SHA256.HashData(
                    NotificationSafeEventProjection
                        .Serialize(this)
                        .Span))
            .ToLowerInvariant();

    private static string RequireSafeText(
        string value,
        int maxLength,
        string parameterName)
    {
        ArgumentNullException.ThrowIfNull(
            value);

        if (value.Length < 1 ||
            value.Length > maxLength ||
            !string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Notification safe-event text must be exact, bounded and control-character free.",
                parameterName);
        }

        return value;
    }
}

public static class NotificationSafeEventProjection
{
    public static ReadOnlyMemory<byte> Serialize(
        NotificationSafeEvent notificationEvent)
    {
        ArgumentNullException.ThrowIfNull(
            notificationEvent);

        return JsonSerializer.SerializeToUtf8Bytes(
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
                    notificationEvent.OccurredAtUtc,
            });
    }
}

public sealed record NotificationSafeEventRecord
{
    public NotificationSafeEventRecord(
        NotificationSafeEvent notificationEvent,
        DateTimeOffset createdAtUtc)
    {
        Event =
            notificationEvent ??
            throw new ArgumentNullException(
                nameof(notificationEvent));

        if (createdAtUtc == default ||
            createdAtUtc <
                notificationEvent.OccurredAtUtc)
        {
            throw new ArgumentException(
                "Notification safe-event creation time must not precede the event.",
                nameof(createdAtUtc));
        }

        CreatedAtUtc =
            createdAtUtc.ToUniversalTime();
    }

    public NotificationSafeEvent Event { get; }
    public DateTimeOffset CreatedAtUtc { get; }
}

public enum NotificationSubscriptionState
{
    Active = 1,
    Paused = 2,
}

public sealed record NotificationSubscriptionDefinition
{
    public const int MaxSubscriptionIdLength = 128;
    public const int HardMaxEventClasses = 16;

    public NotificationSubscriptionDefinition(
        string subscriptionId,
        string destinationId,
        IReadOnlyList<NotificationEventClass> eventClasses)
    {
        SubscriptionId =
            NormalizeSubscriptionId(
                subscriptionId);
        DestinationId =
            NotificationDeliveryIdentity
                .NormalizeDestinationId(
                    destinationId);
        ArgumentNullException.ThrowIfNull(
            eventClasses);

        var normalized =
            eventClasses
                .Distinct()
                .OrderBy(value => value)
                .ToArray();
        if (normalized.Length is < 1 or >
                HardMaxEventClasses ||
            normalized.Length !=
                eventClasses.Count ||
            normalized.Any(
                value =>
                    !Enum.IsDefined(
                        value)))
        {
            throw new ArgumentException(
                "Notification subscription event classes must be unique, defined and bounded.",
                nameof(eventClasses));
        }

        EventClasses =
            Array.AsReadOnly(
                normalized);
    }

    public string SubscriptionId { get; }
    public string DestinationId { get; }
    public IReadOnlyList<NotificationEventClass> EventClasses { get; }

    public bool Matches(
        NotificationEventClass eventClass) =>
        EventClasses.Contains(
            eventClass);

    public static string NormalizeSubscriptionId(
        string subscriptionId)
    {
        ArgumentNullException.ThrowIfNull(
            subscriptionId);

        if (subscriptionId.Length is < 1 or >
                MaxSubscriptionIdLength ||
            !string.Equals(
                subscriptionId,
                subscriptionId.Trim(),
                StringComparison.Ordinal) ||
            subscriptionId.Any(char.IsControl) ||
            subscriptionId.Any(
                character =>
                    !(char.IsAsciiLetterOrDigit(
                          character) ||
                      character is '.' or '_' or '-')))
        {
            throw new ArgumentException(
                "Notification subscription ID must be a bounded ASCII identifier.",
                nameof(subscriptionId));
        }

        return subscriptionId;
    }
}

public sealed record NotificationSubscriptionSnapshot
{
    public NotificationSubscriptionSnapshot(
        NotificationSubscriptionDefinition definition,
        NotificationSubscriptionState state,
        long revision,
        DateTimeOffset updatedAtUtc)
    {
        Definition =
            definition ??
            throw new ArgumentNullException(
                nameof(definition));

        if (!Enum.IsDefined(state) ||
            revision < 1 ||
            updatedAtUtc == default)
        {
            throw new ArgumentException(
                "Notification subscription snapshot is invalid.");
        }

        State = state;
        Revision = revision;
        UpdatedAtUtc =
            updatedAtUtc.ToUniversalTime();
    }

    public NotificationSubscriptionDefinition Definition { get; }
    public NotificationSubscriptionState State { get; }
    public long Revision { get; }
    public DateTimeOffset UpdatedAtUtc { get; }
}

public sealed record NotificationSubscriptionQuery
{
    public const int DefaultMaxResults = 100;
    public const int HardMaxResults = 500;

    public NotificationSubscriptionQuery(
        int maxResults = DefaultMaxResults,
        string? afterSubscriptionId = null,
        NotificationSubscriptionState? state = null,
        NotificationEventClass? eventClass = null)
    {
        if (maxResults is < 1 or >
            HardMaxResults)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxResults));
        }

        if (afterSubscriptionId is not null)
        {
            afterSubscriptionId =
                NotificationSubscriptionDefinition
                    .NormalizeSubscriptionId(
                        afterSubscriptionId);
        }

        if (state is not null &&
            !Enum.IsDefined(
                state.Value))
        {
            throw new ArgumentOutOfRangeException(
                nameof(state));
        }

        if (eventClass is not null &&
            !Enum.IsDefined(
                eventClass.Value))
        {
            throw new ArgumentOutOfRangeException(
                nameof(eventClass));
        }

        MaxResults = maxResults;
        AfterSubscriptionId =
            afterSubscriptionId;
        State = state;
        EventClass = eventClass;
    }

    public int MaxResults { get; }
    public string? AfterSubscriptionId { get; }
    public NotificationSubscriptionState? State { get; }
    public NotificationEventClass? EventClass { get; }
}

public sealed record NotificationSubscriptionPage
{
    public NotificationSubscriptionPage(
        IReadOnlyList<NotificationSubscriptionSnapshot> items,
        bool truncated,
        string? nextSubscriptionId)
    {
        ArgumentNullException.ThrowIfNull(
            items);

        if (items.Count >
                NotificationSubscriptionQuery.HardMaxResults ||
            items.Any(item => item is null))
        {
            throw new ArgumentException(
                "Notification subscription page exceeds admitted bounds.",
                nameof(items));
        }

        if (truncated !=
            (nextSubscriptionId is not null))
        {
            throw new ArgumentException(
                "Truncated notification subscription pages require a continuation.");
        }

        if (truncated &&
            (items.Count == 0 ||
             !string.Equals(
                 items[^1].Definition.SubscriptionId,
                 nextSubscriptionId,
                 StringComparison.Ordinal)))
        {
            throw new ArgumentException(
                "Notification subscription continuation must identify the last returned item.",
                nameof(nextSubscriptionId));
        }

        Items =
            Array.AsReadOnly(
                items.ToArray());
        Truncated = truncated;
        NextSubscriptionId =
            nextSubscriptionId;
    }

    public IReadOnlyList<NotificationSubscriptionSnapshot> Items { get; }
    public bool Truncated { get; }
    public string? NextSubscriptionId { get; }
}

public interface INotificationRoutingStore
{
    Task InitializeAsync(
        CancellationToken cancellationToken = default);

    Task<NotificationSafeEventRecord>
        CreateOrGetEventAsync(
            NotificationSafeEvent notificationEvent,
            DateTimeOffset createdAtUtc,
            CancellationToken cancellationToken = default);

    Task<NotificationSafeEventRecord?> GetEventAsync(
        Guid eventId,
        CancellationToken cancellationToken = default);

    Task<NotificationSubscriptionSnapshot>
        CreateSubscriptionAsync(
            NotificationSubscriptionDefinition definition,
            NotificationSubscriptionState state,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default);

    Task<NotificationSubscriptionSnapshot?> GetSubscriptionAsync(
        string subscriptionId,
        CancellationToken cancellationToken = default);

    Task<NotificationSubscriptionSnapshot?>
        ReplaceSubscriptionAsync(
            NotificationSubscriptionDefinition definition,
            NotificationSubscriptionState state,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default);

    Task<NotificationSubscriptionPage> ListSubscriptionsAsync(
        NotificationSubscriptionQuery query,
        CancellationToken cancellationToken = default);
}

public interface INotificationDestinationProfileCatalog
{
    ValueTask<NotificationDestinationProfile?> GetAsync(
        string destinationId,
        CancellationToken cancellationToken);
}

public sealed record NotificationRoutingResult(
    Guid EventId,
    int SubscriptionsVisited,
    int DeliveriesCreatedOrMatched,
    int MissingDestinations,
    int ProfileEventMismatches,
    bool Truncated);

public sealed class NotificationRoutingCoordinator
{
    public const int HardMaxSubscriptionsPerRoute = 500;

    private readonly INotificationRoutingStore
        _routingStore;
    private readonly INotificationDeliveryStore
        _deliveryStore;
    private readonly INotificationDestinationProfileCatalog
        _profileCatalog;

    public NotificationRoutingCoordinator(
        INotificationRoutingStore routingStore,
        INotificationDeliveryStore deliveryStore,
        INotificationDestinationProfileCatalog profileCatalog)
    {
        _routingStore =
            routingStore ??
            throw new ArgumentNullException(
                nameof(routingStore));
        _deliveryStore =
            deliveryStore ??
            throw new ArgumentNullException(
                nameof(deliveryStore));
        _profileCatalog =
            profileCatalog ??
            throw new ArgumentNullException(
                nameof(profileCatalog));
    }

    public async Task<NotificationRoutingResult> RouteAsync(
        NotificationSafeEvent notificationEvent,
        DateTimeOffset routedAtUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(
            notificationEvent);
        if (routedAtUtc == default ||
            routedAtUtc <
                notificationEvent.OccurredAtUtc)
        {
            throw new ArgumentException(
                "Notification routing time is invalid.",
                nameof(routedAtUtc));
        }

        var durableEvent =
            await _routingStore
                .CreateOrGetEventAsync(
                    notificationEvent,
                    routedAtUtc,
                    cancellationToken)
                .ConfigureAwait(false);

        var page =
            await _routingStore
                .ListSubscriptionsAsync(
                    new NotificationSubscriptionQuery(
                        HardMaxSubscriptionsPerRoute,
                        state:
                            NotificationSubscriptionState.Active,
                        eventClass:
                            notificationEvent.EventClass),
                    cancellationToken)
                .ConfigureAwait(false);

        if (page.Truncated)
        {
            throw new InvalidOperationException(
                "Notification routing fan-out exceeds the admitted subscription ceiling.");
        }

        var matched = 0;
        var missing = 0;
        var mismatches = 0;

        foreach (var subscription in
                 page.Items)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            var profile =
                await _profileCatalog
                    .GetAsync(
                        subscription.Definition.DestinationId,
                        cancellationToken)
                    .ConfigureAwait(false);
            if (profile is null)
            {
                missing++;
                continue;
            }

            if (!profile.EnabledEvents.Contains(
                    notificationEvent.EventClass))
            {
                mismatches++;
                continue;
            }

            await _deliveryStore
                .CreateOrGetAsync(
                    new NotificationDeliverySnapshot(
                        durableEvent.Event.EventId,
                        profile.DestinationId,
                        durableEvent.Event.PayloadFingerprint,
                        NotificationDeliveryState.Pending,
                        0,
                        durableEvent.CreatedAtUtc),
                    durableEvent.CreatedAtUtc,
                    cancellationToken)
                .ConfigureAwait(false);
            matched++;
        }

        return new NotificationRoutingResult(
            notificationEvent.EventId,
            page.Items.Count,
            matched,
            missing,
            mismatches,
            page.Truncated);
    }
}

public sealed class WebhookNotificationDeliveryDispatcher :
    INotificationDeliveryDispatcher
{
    private readonly INotificationRoutingStore
        _routingStore;
    private readonly INotificationDestinationProfileCatalog
        _profileCatalog;
    private readonly WebhookNotificationAdapter
        _adapter;

    public WebhookNotificationDeliveryDispatcher(
        INotificationRoutingStore routingStore,
        INotificationDestinationProfileCatalog profileCatalog,
        WebhookNotificationAdapter adapter)
    {
        _routingStore =
            routingStore ??
            throw new ArgumentNullException(
                nameof(routingStore));
        _profileCatalog =
            profileCatalog ??
            throw new ArgumentNullException(
                nameof(profileCatalog));
        _adapter =
            adapter ??
            throw new ArgumentNullException(
                nameof(adapter));
    }

    public async Task<NotificationDeliveryDispatchResult>
        DispatchAsync(
            NotificationDeliveryRecord claimedDelivery,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            claimedDelivery);

        var notificationEvent =
            await _routingStore
                .GetEventAsync(
                    claimedDelivery.Snapshot.NotificationId,
                    cancellationToken)
                .ConfigureAwait(false);
        if (notificationEvent is null ||
            !string.Equals(
                notificationEvent.Event.PayloadFingerprint,
                claimedDelivery.Snapshot.PayloadFingerprint,
                StringComparison.Ordinal))
        {
            return new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.PermanentFailure);
        }

        var profile =
            await _profileCatalog
                .GetAsync(
                    claimedDelivery.Snapshot.DestinationId,
                    cancellationToken)
                .ConfigureAwait(false);
        if (profile is null ||
            profile.Provider !=
                NotificationProviderKind.Webhook ||
            !profile.EnabledEvents.Contains(
                notificationEvent.Event.EventClass))
        {
            return new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.PermanentFailure);
        }

        var result =
            await _adapter
                .DispatchAsync(
                    profile,
                    notificationEvent.Event.ToWebhookEvent(),
                    cancellationToken)
                .ConfigureAwait(false);

        if (!string.Equals(
                result.PayloadFingerprint,
                claimedDelivery.Snapshot.PayloadFingerprint,
                StringComparison.Ordinal))
        {
            return new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.UnknownExternalEffect);
        }

        return new NotificationDeliveryDispatchResult(
            result.TransportResult.Outcome switch
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
                    nameof(result.TransportResult.Outcome)),
            });
    }
}
