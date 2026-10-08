namespace Kafdeck.Core.Notifications;

/// <summary>
/// Read-only metadata history for exactly one authorized destination.
/// Independent of the finite due/recovery worker queries.
/// </summary>
public sealed record NotificationDestinationDeliveryCursor
{
    public NotificationDestinationDeliveryCursor(DateTimeOffset createdAtUtc, Guid notificationId)
    {
        if (createdAtUtc == default || notificationId == Guid.Empty)
            throw new ArgumentException("Invalid notification history cursor.");
        CreatedAtUtc = createdAtUtc.ToUniversalTime();
        NotificationId = notificationId;
    }
    public DateTimeOffset CreatedAtUtc { get; }
    public Guid NotificationId { get; }
}

public sealed record NotificationDestinationDeliveryQuery
{
    public const int DefaultMaxResults = 50;
    public const int HardMaxResults = 200;

    public NotificationDestinationDeliveryQuery(
        string destinationId,
        int maxResults = DefaultMaxResults,
        NotificationDestinationDeliveryCursor? after = null)
    {
        DestinationId = NotificationDeliveryIdentity.NormalizeDestinationId(destinationId);
        if (maxResults is < 1 or > HardMaxResults)
            throw new ArgumentOutOfRangeException(nameof(maxResults));
        MaxResults = maxResults;
        After = after;
    }

    public string DestinationId { get; }
    public int MaxResults { get; }
    public NotificationDestinationDeliveryCursor? After { get; }
}

public sealed record NotificationDestinationDeliveryPage
{
    public NotificationDestinationDeliveryPage(
        IReadOnlyList<NotificationDeliveryRecord> items,
        bool truncated,
        NotificationDestinationDeliveryCursor? next)
    {
        ArgumentNullException.ThrowIfNull(items);
        if (items.Count > NotificationDestinationDeliveryQuery.HardMaxResults ||
            (truncated != (next is not null)))
            throw new ArgumentException("Invalid bounded notification delivery history page.");
        if (truncated &&
            (items.Count == 0 ||
             items[^1].Snapshot.NotificationId != next!.NotificationId ||
             items[^1].Snapshot.CreatedAtUtc != next.CreatedAtUtc))
            throw new ArgumentException("History cursor must identify the last visible row.");
        Items = Array.AsReadOnly(items.ToArray());
        Truncated = truncated;
        Next = next;
    }

    public IReadOnlyList<NotificationDeliveryRecord> Items { get; }
    public bool Truncated { get; }
    public NotificationDestinationDeliveryCursor? Next { get; }
}

public interface INotificationDeliveryHistoryReader
{
    Task<NotificationDestinationDeliveryPage> ListByDestinationAsync(
        NotificationDestinationDeliveryQuery query,
        CancellationToken cancellationToken = default);
}
