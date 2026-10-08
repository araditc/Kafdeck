namespace Kafdeck.Core.Notifications;

public enum NotificationSubscriptionState
{
    Active = 1,
    Paused = 2,
    Retired = 3,
}

public sealed record NotificationSubscriptionFilter
{
    public const int HardMaxEventTypes = 32;
    public const int MaxEventTypeLength =
        NotificationWebhookEvent.MaxEventTypeLength;

    public NotificationSubscriptionFilter(
        IReadOnlyList<NotificationEventClass> eventClasses,
        IReadOnlyList<string>? eventTypes = null)
    {
        ArgumentNullException.ThrowIfNull(
            eventClasses);

        var classes =
            eventClasses
                .Distinct()
                .OrderBy(value => value)
                .ToArray();
        if (classes.Length is < 1 or > 16 ||
            classes.Length != eventClasses.Count ||
            classes.Any(value => !Enum.IsDefined(value)))
        {
            throw new ArgumentException(
                "Notification subscription event classes must be unique and bounded.",
                nameof(eventClasses));
        }

        var types =
            (eventTypes ??
             Array.Empty<string>())
            .ToArray();
        if (types.Length > HardMaxEventTypes ||
            types.Distinct(StringComparer.Ordinal).Count() !=
                types.Length)
        {
            throw new ArgumentException(
                "Notification subscription event types must be unique and bounded.",
                nameof(eventTypes));
        }

        foreach (var type in types)
        {
            if (string.IsNullOrWhiteSpace(type) ||
                type.Length > MaxEventTypeLength ||
                !string.Equals(
                    type,
                    type.Trim(),
                    StringComparison.Ordinal) ||
                type.Any(char.IsControl))
            {
                throw new ArgumentException(
                    "Notification subscription event types must be exact bounded safe identifiers.",
                    nameof(eventTypes));
            }
        }

        EventClasses =
            Array.AsReadOnly(classes);
        EventTypes =
            Array.AsReadOnly(
                types
                    .OrderBy(
                        value => value,
                        StringComparer.Ordinal)
                    .ToArray());
    }

    public IReadOnlyList<NotificationEventClass> EventClasses { get; }
    public IReadOnlyList<string> EventTypes { get; }

    public bool Matches(
        NotificationSafeEvent notificationEvent)
    {
        ArgumentNullException.ThrowIfNull(
            notificationEvent);

        return EventClasses.Contains(
                   notificationEvent.EventClass) &&
               (EventTypes.Count == 0 ||
                EventTypes.Contains(
                    notificationEvent.EventType,
                    StringComparer.Ordinal));
    }
}

public sealed record NotificationSubscriptionSnapshot
{
    public const int MaxSubscriptionIdLength = 128;

    public NotificationSubscriptionSnapshot(
        string subscriptionId,
        string destinationId,
        NotificationSubscriptionFilter filter,
        NotificationSubscriptionState state,
        DateTimeOffset createdAtUtc)
    {
        SubscriptionId =
            NormalizeSubscriptionId(
                subscriptionId);
        DestinationId =
            NotificationDestinationProfile
                .NormalizeDestinationId(
                    destinationId);
        Filter =
            filter ??
            throw new ArgumentNullException(
                nameof(filter));

        if (!Enum.IsDefined(state))
        {
            throw new ArgumentOutOfRangeException(
                nameof(state));
        }

        if (createdAtUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(createdAtUtc));
        }

        State =
            state;
        CreatedAtUtc =
            createdAtUtc.ToUniversalTime();
    }

    public string SubscriptionId { get; }
    public string DestinationId { get; }
    public NotificationSubscriptionFilter Filter { get; }
    public NotificationSubscriptionState State { get; }
    public DateTimeOffset CreatedAtUtc { get; }

    internal static string NormalizeSubscriptionId(
        string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.Length is < 1 or >
                MaxSubscriptionIdLength ||
            !string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Any(char.IsControl) ||
            value.Any(character =>
                !(char.IsAsciiLetterOrDigit(character) ||
                  character is '.' or '_' or '-')))
        {
            throw new ArgumentException(
                "Notification subscription ID must be a bounded ASCII identifier.",
                nameof(value));
        }

        return value;
    }
}

public sealed record NotificationSubscriptionRecord
{
    public NotificationSubscriptionRecord(
        NotificationSubscriptionSnapshot snapshot,
        long revision,
        DateTimeOffset updatedAtUtc)
    {
        Snapshot =
            snapshot ??
            throw new ArgumentNullException(
                nameof(snapshot));

        if (revision < 1 ||
            updatedAtUtc == default ||
            updatedAtUtc <
                snapshot.CreatedAtUtc)
        {
            throw new ArgumentException(
                "Notification subscription revision metadata is invalid.");
        }

        Revision =
            revision;
        UpdatedAtUtc =
            updatedAtUtc.ToUniversalTime();
    }

    public NotificationSubscriptionSnapshot Snapshot { get; }
    public long Revision { get; }
    public DateTimeOffset UpdatedAtUtc { get; }
}

public sealed record NotificationSubscriptionListQuery
{
    public const int DefaultMaxResults = 100;
    public const int HardMaxResults = 500;

    public NotificationSubscriptionListQuery(
        int maxResults = DefaultMaxResults,
        string? destinationId = null,
        NotificationSubscriptionState? state = null,
        string? afterSubscriptionId = null)
    {
        if (maxResults is < 1 or >
            HardMaxResults)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxResults));
        }

        if (state is not null &&
            !Enum.IsDefined(state.Value))
        {
            throw new ArgumentOutOfRangeException(
                nameof(state));
        }

        MaxResults =
            maxResults;
        DestinationId =
            destinationId is null
                ? null
                : NotificationDestinationProfile
                    .NormalizeDestinationId(
                        destinationId);
        State =
            state;
        AfterSubscriptionId =
            afterSubscriptionId is null
                ? null
                : NotificationSubscriptionSnapshot
                    .NormalizeSubscriptionId(
                        afterSubscriptionId);
    }

    public int MaxResults { get; }
    public string? DestinationId { get; }
    public NotificationSubscriptionState? State { get; }
    public string? AfterSubscriptionId { get; }
}

public sealed record NotificationSubscriptionPage
{
    public NotificationSubscriptionPage(
        IReadOnlyList<NotificationSubscriptionRecord> items,
        bool truncated,
        string? nextSubscriptionId)
    {
        ArgumentNullException.ThrowIfNull(items);

        if (items.Count >
            NotificationSubscriptionListQuery.HardMaxResults ||
            truncated !=
            (nextSubscriptionId is not null))
        {
            throw new ArgumentException(
                "Notification subscription page metadata is invalid.");
        }

        if (truncated &&
            (items.Count == 0 ||
             !string.Equals(
                 items[^1].Snapshot.SubscriptionId,
                 nextSubscriptionId,
                 StringComparison.Ordinal)))
        {
            throw new ArgumentException(
                "Notification subscription continuation must identify the last returned subscription.");
        }

        Items =
            Array.AsReadOnly(
                items.ToArray());
        Truncated =
            truncated;
        NextSubscriptionId =
            nextSubscriptionId;
    }

    public IReadOnlyList<NotificationSubscriptionRecord> Items { get; }
    public bool Truncated { get; }
    public string? NextSubscriptionId { get; }
}

public interface INotificationSubscriptionStore
{
    Task InitializeAsync(
        CancellationToken cancellationToken = default);

    Task<NotificationSubscriptionRecord?> GetAsync(
        string subscriptionId,
        CancellationToken cancellationToken = default);

    Task<NotificationSubscriptionRecord> CreateAsync(
        NotificationSubscriptionSnapshot snapshot,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<NotificationSubscriptionRecord?> ReplaceAsync(
        NotificationSubscriptionSnapshot snapshot,
        long expectedRevision,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<NotificationSubscriptionPage> ListAsync(
        NotificationSubscriptionListQuery query,
        CancellationToken cancellationToken = default);
}

public sealed record NotificationSubscriptionRoutingPolicy
{
    public const int DefaultMaxScanned = 500;
    public const int HardMaxScanned = 500;
    public const int DefaultMaxDestinations = 100;
    public const int HardMaxDestinations = 200;

    public NotificationSubscriptionRoutingPolicy(
        int maxScanned = DefaultMaxScanned,
        int maxDestinations = DefaultMaxDestinations)
    {
        if (maxScanned is < 1 or > HardMaxScanned ||
            maxDestinations is < 1 or >
                HardMaxDestinations)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxScanned));
        }

        MaxScanned =
            maxScanned;
        MaxDestinations =
            maxDestinations;
    }

    public int MaxScanned { get; }
    public int MaxDestinations { get; }
}

public sealed record NotificationSubscriptionRoutingResult(
    IReadOnlyList<string> DestinationIds,
    int ScannedSubscriptions,
    bool Truncated);

public sealed class NotificationSubscriptionRouter
{
    private readonly INotificationSubscriptionStore
        _store;
    private readonly NotificationSubscriptionRoutingPolicy
        _policy;

    public NotificationSubscriptionRouter(
        INotificationSubscriptionStore store,
        NotificationSubscriptionRoutingPolicy? policy = null)
    {
        _store =
            store ??
            throw new ArgumentNullException(
                nameof(store));
        _policy =
            policy ??
            new NotificationSubscriptionRoutingPolicy();
    }

    public async Task<NotificationSubscriptionRoutingResult>
        RouteAsync(
            NotificationSafeEvent notificationEvent,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(
            notificationEvent);

        var destinations =
            new HashSet<string>(
                StringComparer.Ordinal);
        var scanned = 0;
        string? cursor = null;
        var truncated = false;

        while (scanned < _policy.MaxScanned &&
               destinations.Count <
               _policy.MaxDestinations)
        {
            var remaining =
                _policy.MaxScanned -
                scanned;
            var page =
                await _store.ListAsync(
                        new NotificationSubscriptionListQuery(
                            Math.Min(
                                remaining,
                                NotificationSubscriptionListQuery
                                    .DefaultMaxResults),
                            state:
                                NotificationSubscriptionState.Active,
                            afterSubscriptionId:
                                cursor),
                        cancellationToken)
                    .ConfigureAwait(false);

            foreach (var item in page.Items)
            {
                scanned++;

                if (item.Snapshot.Filter.Matches(
                        notificationEvent))
                {
                    destinations.Add(
                        item.Snapshot.DestinationId);

                    if (destinations.Count >=
                        _policy.MaxDestinations)
                    {
                        truncated =
                            page.Truncated ||
                            scanned <
                            _policy.MaxScanned;
                        break;
                    }
                }
            }

            if (destinations.Count >=
                    _policy.MaxDestinations ||
                !page.Truncated)
            {
                truncated =
                    truncated ||
                    page.Truncated &&
                    destinations.Count >=
                    _policy.MaxDestinations;
                break;
            }

            cursor =
                page.NextSubscriptionId;
        }

        if (scanned >= _policy.MaxScanned)
        {
            truncated =
                true;
        }

        return new NotificationSubscriptionRoutingResult(
            destinations
                .OrderBy(
                    value => value,
                    StringComparer.Ordinal)
                .ToArray(),
            scanned,
            truncated);
    }
}
