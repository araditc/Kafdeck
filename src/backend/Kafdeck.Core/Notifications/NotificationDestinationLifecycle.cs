namespace Kafdeck.Core.Notifications;

public enum NotificationDestinationLifecycleState
{
    Active = 1,
    Paused = 2,
    Retired = 3,
}

public sealed record NotificationDestinationDefinition
{
    public NotificationDestinationDefinition(
        NotificationDestinationProfile profile,
        NotificationDestinationLifecycleState state,
        DateTimeOffset createdAtUtc,
        string? providerTarget = null)
    {
        Profile =
            profile ??
            throw new ArgumentNullException(
                nameof(profile));

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

        switch (profile.Provider)
        {
            case NotificationProviderKind.Email:
                if (profile.ConfiguredEndpoint is not null)
                {
                    throw new ArgumentException(
                        "Email destinations do not accept caller-supplied provider endpoints.",
                        nameof(profile));
                }

                if (providerTarget is null)
                {
                    throw new ArgumentException(
                        "Email destinations require one exact recipient target.",
                        nameof(providerTarget));
                }

                _ =
                    new EmailNotificationDestination(
                        profile,
                        providerTarget);
                break;

            case NotificationProviderKind.Webhook:
                if (providerTarget is not null)
                {
                    throw new ArgumentException(
                        "Webhook targets are owned by the configured HTTPS endpoint, not a generic provider target.",
                        nameof(providerTarget));
                }

                break;

            case NotificationProviderKind.Slack:
            case NotificationProviderKind.MicrosoftTeams:
            case NotificationProviderKind.Telegram:
            case NotificationProviderKind.PagerDuty:
                if (profile.ConfiguredEndpoint is not null)
                {
                    throw new ArgumentException(
                        "Bound provider destinations do not accept caller-supplied provider endpoints.",
                        nameof(profile));
                }

                if (providerTarget is not null)
                {
                    throw new ArgumentException(
                        "Bound provider destinations do not accept caller-supplied provider targets.",
                        nameof(providerTarget));
                }

                _ =
                    new BoundNotificationDestination(
                        profile,
                        profile.Provider);
                break;

            default:
                throw new ArgumentOutOfRangeException(
                    nameof(profile));
        }

        State =
            state;
        CreatedAtUtc =
            createdAtUtc.ToUniversalTime();
        ProviderTarget =
            providerTarget;
    }

    public NotificationDestinationProfile Profile { get; }
    public NotificationDestinationLifecycleState State { get; }
    public DateTimeOffset CreatedAtUtc { get; }
    public string? ProviderTarget { get; }
}

public sealed record NotificationDestinationRecord
{
    public NotificationDestinationRecord(
        NotificationDestinationDefinition definition,
        long revision,
        DateTimeOffset updatedAtUtc)
    {
        Definition =
            definition ??
            throw new ArgumentNullException(
                nameof(definition));

        if (revision < 1 ||
            updatedAtUtc == default ||
            updatedAtUtc <
                definition.CreatedAtUtc)
        {
            throw new ArgumentException(
                "Notification destination revision metadata is invalid.");
        }

        Revision =
            revision;
        UpdatedAtUtc =
            updatedAtUtc.ToUniversalTime();
    }

    public NotificationDestinationDefinition Definition { get; }
    public long Revision { get; }
    public DateTimeOffset UpdatedAtUtc { get; }
}

public sealed record NotificationDestinationListQuery
{
    public const int DefaultMaxResults = 100;
    public const int HardMaxResults = 500;

    public NotificationDestinationListQuery(
        int maxResults = DefaultMaxResults,
        NotificationProviderKind? provider = null,
        NotificationDestinationLifecycleState? state = null,
        string? afterDestinationId = null)
    {
        if (maxResults is < 1 or > HardMaxResults)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxResults));
        }

        if (provider is not null &&
            !Enum.IsDefined(provider.Value))
        {
            throw new ArgumentOutOfRangeException(
                nameof(provider));
        }

        if (state is not null &&
            !Enum.IsDefined(state.Value))
        {
            throw new ArgumentOutOfRangeException(
                nameof(state));
        }

        MaxResults =
            maxResults;
        Provider =
            provider;
        State =
            state;
        AfterDestinationId =
            afterDestinationId is null
                ? null
                : NotificationDestinationProfile
                    .NormalizeDestinationId(
                        afterDestinationId);
    }

    public int MaxResults { get; }
    public NotificationProviderKind? Provider { get; }
    public NotificationDestinationLifecycleState? State { get; }
    public string? AfterDestinationId { get; }
}

public sealed record NotificationDestinationPage
{
    public NotificationDestinationPage(
        IReadOnlyList<NotificationDestinationRecord> items,
        bool truncated,
        string? nextDestinationId)
    {
        ArgumentNullException.ThrowIfNull(items);

        if (items.Count >
                NotificationDestinationListQuery.HardMaxResults ||
            truncated !=
            (nextDestinationId is not null))
        {
            throw new ArgumentException(
                "Notification destination page metadata is invalid.");
        }

        if (truncated &&
            (items.Count == 0 ||
             !string.Equals(
                 items[^1].Definition.Profile.DestinationId,
                 nextDestinationId,
                 StringComparison.Ordinal)))
        {
            throw new ArgumentException(
                "Notification destination continuation must identify the last returned destination.");
        }

        Items =
            Array.AsReadOnly(
                items.ToArray());
        Truncated =
            truncated;
        NextDestinationId =
            nextDestinationId;
    }

    public IReadOnlyList<NotificationDestinationRecord> Items { get; }
    public bool Truncated { get; }
    public string? NextDestinationId { get; }
}

public interface INotificationDestinationStore
{
    Task InitializeAsync(
        CancellationToken cancellationToken = default);

    Task<NotificationDestinationRecord?> GetAsync(
        string destinationId,
        CancellationToken cancellationToken = default);

    Task<NotificationDestinationRecord> CreateAsync(
        NotificationDestinationDefinition definition,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<NotificationDestinationRecord?> ReplaceAsync(
        NotificationDestinationDefinition definition,
        long expectedRevision,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<NotificationDestinationPage> ListAsync(
        NotificationDestinationListQuery query,
        CancellationToken cancellationToken = default);
}
