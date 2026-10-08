namespace Kafdeck.Core.Notifications;

public static class NotificationDeliveryIdentity
{
    public static string NormalizeDestinationId(
        string destinationId)
    {
        ArgumentNullException.ThrowIfNull(
            destinationId);

        if (destinationId.Length is < 1 or >
            NotificationDestinationProfile
                .MaxDestinationIdLength ||
            !string.Equals(
                destinationId,
                destinationId.Trim(),
                StringComparison.Ordinal) ||
            destinationId.Any(char.IsControl) ||
            destinationId.Any(
                character =>
                    !(char.IsAsciiLetterOrDigit(
                          character) ||
                      character is '.' or '_' or '-')))
        {
            throw new ArgumentException(
                "Notification delivery destination ID must be a bounded ASCII identifier.",
                nameof(destinationId));
        }

        return destinationId;
    }
}

public static class NotificationDeliveryOutcomeCodes
{
    public const string Delivered =
        "delivered";
    public const string RetryableFailure =
        "retryable-failure";
    public const string UnknownExternalEffect =
        "unknown-external-effect";
    public const string Exhausted =
        "exhausted";

    public static void ValidateForState(
        NotificationDeliverySnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(
            snapshot);

        var expected =
            snapshot.State switch
            {
                NotificationDeliveryState.Pending or
                NotificationDeliveryState.InFlight =>
                    null,
                NotificationDeliveryState.Delivered =>
                    Delivered,
                NotificationDeliveryState.Failed =>
                    RetryableFailure,
                NotificationDeliveryState.UnknownExternalEffect =>
                    UnknownExternalEffect,
                NotificationDeliveryState.Exhausted =>
                    Exhausted,
                _ => throw new ArgumentOutOfRangeException(
                    nameof(snapshot)),
            };

        if (!string.Equals(
                snapshot.OutcomeCode,
                expected,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Notification delivery outcome code is not the admitted server-derived value for this state.",
                nameof(snapshot));
        }
    }
}

public sealed record NotificationDeliveryRecord
{
    public NotificationDeliveryRecord(
        NotificationDeliverySnapshot snapshot,
        long revision,
        DateTimeOffset updatedAtUtc)
    {
        Snapshot =
            snapshot ??
            throw new ArgumentNullException(
                nameof(snapshot));

        if (revision < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(revision));
        }

        if (updatedAtUtc == default ||
            updatedAtUtc < snapshot.CreatedAtUtc)
        {
            throw new ArgumentException(
                "Notification delivery record timestamps are invalid.",
                nameof(updatedAtUtc));
        }

        Revision = revision;
        UpdatedAtUtc =
            updatedAtUtc.ToUniversalTime();
    }

    public NotificationDeliverySnapshot Snapshot { get; }
    public long Revision { get; }
    public DateTimeOffset UpdatedAtUtc { get; }
}

public sealed record NotificationDeliveryDueCursor
{
    public NotificationDeliveryDueCursor(
        DateTimeOffset dueAtUtc,
        Guid notificationId,
        string destinationId)
    {
        if (dueAtUtc == default ||
            notificationId == Guid.Empty)
        {
            throw new ArgumentException(
                "Notification delivery due cursor is invalid.");
        }

        DueAtUtc =
            dueAtUtc.ToUniversalTime();
        NotificationId =
            notificationId;
        DestinationId =
            NotificationDeliveryIdentity
                .NormalizeDestinationId(
                    destinationId);
    }

    public DateTimeOffset DueAtUtc { get; }
    public Guid NotificationId { get; }
    public string DestinationId { get; }
}

public sealed record NotificationDeliveryDueQuery
{
    public const int DefaultMaxResults = 100;
    public const int HardMaxResults = 500;

    public NotificationDeliveryDueQuery(
        DateTimeOffset nowUtc,
        int maxResults = DefaultMaxResults,
        NotificationDeliveryDueCursor? after = null)
    {
        if (nowUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(nowUtc));
        }

        if (maxResults is < 1 or >
            HardMaxResults)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxResults));
        }

        NowUtc =
            nowUtc.ToUniversalTime();
        MaxResults =
            maxResults;
        After =
            after;
    }

    public DateTimeOffset NowUtc { get; }
    public int MaxResults { get; }
    public NotificationDeliveryDueCursor? After { get; }
}

public sealed record NotificationDeliveryPage
{
    public NotificationDeliveryPage(
        IReadOnlyList<NotificationDeliveryRecord> items,
        bool truncated,
        NotificationDeliveryDueCursor? nextCursor)
    {
        ArgumentNullException.ThrowIfNull(items);

        if (items.Count >
            NotificationDeliveryDueQuery.HardMaxResults)
        {
            throw new ArgumentOutOfRangeException(
                nameof(items));
        }

        foreach (var item in items)
        {
            ArgumentNullException.ThrowIfNull(
                item);
        }

        if (truncated !=
            (nextCursor is not null))
        {
            throw new ArgumentException(
                "Truncated notification delivery pages require exactly one continuation cursor.");
        }

        if (truncated)
        {
            if (items.Count == 0)
            {
                throw new ArgumentException(
                    "A truncated notification delivery page cannot be empty.",
                    nameof(items));
            }

            var last =
                items[^1].Snapshot;
            var expectedDueAt =
                (last.NextAttemptAtUtc ??
                 last.CreatedAtUtc)
                .ToUniversalTime();

            if (nextCursor!.DueAtUtc !=
                    expectedDueAt ||
                nextCursor.NotificationId !=
                    last.NotificationId ||
                !string.Equals(
                    nextCursor.DestinationId,
                    last.DestinationId,
                    StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    "Notification delivery continuation must identify the last returned due item.",
                    nameof(nextCursor));
            }
        }

        Items =
            Array.AsReadOnly(
                items.ToArray());
        Truncated =
            truncated;
        NextCursor =
            nextCursor;
    }

    public IReadOnlyList<NotificationDeliveryRecord> Items { get; }
    public bool Truncated { get; }
    public NotificationDeliveryDueCursor? NextCursor { get; }
}

public static class NotificationDeliveryTransition
{
    public static void ValidateInitial(
        NotificationDeliverySnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(
            snapshot);

        if (snapshot.State !=
                NotificationDeliveryState.Pending ||
            snapshot.AttemptCount != 0 ||
            snapshot.OutcomeCode is not null ||
            snapshot.ProviderRequestId is not null)
        {
            throw new ArgumentException(
                "A durable notification delivery must be created in Pending state with zero attempts and no provider outcome metadata.",
                nameof(snapshot));
        }
    }

    public static void ValidateReplacement(
        NotificationDeliverySnapshot previous,
        NotificationDeliverySnapshot next)
    {
        ArgumentNullException.ThrowIfNull(
            previous);
        ArgumentNullException.ThrowIfNull(
            next);

        if (next.ProviderRequestId is not null)
        {
            throw new ArgumentException(
                "Provider request identifiers are not admitted to the durable delivery ledger until a provider-specific safe projection is defined.",
                nameof(next));
        }

        NotificationDeliveryOutcomeCodes
            .ValidateForState(
                next);

        if (previous.NotificationId !=
                next.NotificationId ||
            !string.Equals(
                previous.DestinationId,
                next.DestinationId,
                StringComparison.Ordinal) ||
            !string.Equals(
                previous.PayloadFingerprint,
                next.PayloadFingerprint,
                StringComparison.Ordinal) ||
            previous.CreatedAtUtc !=
                next.CreatedAtUtc)
        {
            throw new ArgumentException(
                "Notification delivery identity, payload fingerprint and creation time are immutable.",
                nameof(next));
        }

        var valid =
            (previous.State,
             next.State) switch
            {
                (
                    NotificationDeliveryState.Pending,
                    NotificationDeliveryState.InFlight) =>
                    next.AttemptCount ==
                        previous.AttemptCount + 1,
                (
                    NotificationDeliveryState.Pending,
                    NotificationDeliveryState.Exhausted) =>
                    next.AttemptCount ==
                        previous.AttemptCount,
                (
                    NotificationDeliveryState.Failed,
                    NotificationDeliveryState.InFlight) =>
                    next.AttemptCount ==
                        previous.AttemptCount + 1,
                (
                    NotificationDeliveryState.Failed,
                    NotificationDeliveryState.Exhausted) =>
                    next.AttemptCount ==
                        previous.AttemptCount,
                (
                    NotificationDeliveryState.InFlight,
                    NotificationDeliveryState.Delivered or
                    NotificationDeliveryState.Failed or
                    NotificationDeliveryState.UnknownExternalEffect or
                    NotificationDeliveryState.Exhausted) =>
                    next.AttemptCount ==
                        previous.AttemptCount,
                _ =>
                    false,
            };

        if (!valid)
        {
            throw new ArgumentException(
                $"Notification delivery transition {previous.State} -> {next.State} with attempts {previous.AttemptCount} -> {next.AttemptCount} is not admitted.",
                nameof(next));
        }

        if ((next.State is
                 NotificationDeliveryState.InFlight or
                 NotificationDeliveryState.Delivered or
                 NotificationDeliveryState.UnknownExternalEffect or
                 NotificationDeliveryState.Exhausted) &&
            next.NextAttemptAtUtc is not null)
        {
            throw new ArgumentException(
                "Only pending or retryable failed deliveries may carry a next-attempt timestamp.",
                nameof(next));
        }

        if (next.State ==
                NotificationDeliveryState.Failed &&
            next.NextAttemptAtUtc is null)
        {
            throw new ArgumentException(
                "Retryable failed deliveries require a bounded next-attempt timestamp.",
                nameof(next));
        }
    }
}

public sealed record NotificationDeliveryRecoveryCursor
{
    public NotificationDeliveryRecoveryCursor(
        DateTimeOffset updatedAtUtc,
        Guid notificationId,
        string destinationId)
    {
        if (updatedAtUtc == default ||
            notificationId == Guid.Empty)
        {
            throw new ArgumentException(
                "Notification delivery recovery cursor is invalid.");
        }

        UpdatedAtUtc =
            updatedAtUtc.ToUniversalTime();
        NotificationId =
            notificationId;
        DestinationId =
            NotificationDeliveryIdentity
                .NormalizeDestinationId(
                    destinationId);
    }

    public DateTimeOffset UpdatedAtUtc { get; }
    public Guid NotificationId { get; }
    public string DestinationId { get; }
}

public sealed record NotificationStaleInFlightQuery
{
    public NotificationStaleInFlightQuery(
        DateTimeOffset staleBeforeUtc,
        int maxResults =
            NotificationDeliveryDueQuery.DefaultMaxResults,
        NotificationDeliveryRecoveryCursor? after = null)
    {
        if (staleBeforeUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(staleBeforeUtc));
        }

        if (maxResults is < 1 or >
            NotificationDeliveryDueQuery.HardMaxResults)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxResults));
        }

        StaleBeforeUtc =
            staleBeforeUtc.ToUniversalTime();
        MaxResults =
            maxResults;
        After =
            after;
    }

    public DateTimeOffset StaleBeforeUtc { get; }
    public int MaxResults { get; }
    public NotificationDeliveryRecoveryCursor? After { get; }
}

public sealed record NotificationDeliveryRecoveryPage
{
    public NotificationDeliveryRecoveryPage(
        IReadOnlyList<NotificationDeliveryRecord> items,
        bool truncated,
        NotificationDeliveryRecoveryCursor? nextCursor)
    {
        ArgumentNullException.ThrowIfNull(items);

        if (items.Count >
            NotificationDeliveryDueQuery.HardMaxResults)
        {
            throw new ArgumentOutOfRangeException(
                nameof(items));
        }

        foreach (var item in items)
        {
            ArgumentNullException.ThrowIfNull(
                item);
            if (item.Snapshot.State !=
                NotificationDeliveryState.InFlight)
            {
                throw new ArgumentException(
                    "Notification recovery pages may contain only in-flight deliveries.",
                    nameof(items));
            }
        }

        if (truncated !=
            (nextCursor is not null))
        {
            throw new ArgumentException(
                "Truncated notification recovery pages require exactly one continuation cursor.");
        }

        if (truncated)
        {
            if (items.Count == 0)
            {
                throw new ArgumentException(
                    "A truncated notification recovery page cannot be empty.",
                    nameof(items));
            }

            var last =
                items[^1];

            if (nextCursor!.UpdatedAtUtc !=
                    last.UpdatedAtUtc ||
                nextCursor.NotificationId !=
                    last.Snapshot.NotificationId ||
                !string.Equals(
                    nextCursor.DestinationId,
                    last.Snapshot.DestinationId,
                    StringComparison.Ordinal))
            {
                throw new ArgumentException(
                    "Notification recovery continuation must identify the last returned stale in-flight item.",
                    nameof(nextCursor));
            }
        }

        Items =
            Array.AsReadOnly(
                items.ToArray());
        Truncated =
            truncated;
        NextCursor =
            nextCursor;
    }

    public IReadOnlyList<NotificationDeliveryRecord> Items { get; }
    public bool Truncated { get; }
    public NotificationDeliveryRecoveryCursor? NextCursor { get; }
}

public enum NotificationDeliveryClaimOutcome
{
    Claimed = 1,
    VersionConflict = 2,
    RateLimited = 3,
    ConcurrencyLimited = 4,
    NotClaimable = 5,
}

public sealed record NotificationDeliveryClaimResult
{
    public NotificationDeliveryClaimResult(
        NotificationDeliveryClaimOutcome outcome,
        NotificationDeliveryRecord? record = null)
    {
        if (!Enum.IsDefined(outcome))
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome));
        }

        if ((outcome ==
                 NotificationDeliveryClaimOutcome.Claimed) !=
            (record is not null))
        {
            throw new ArgumentException(
                "A notification delivery claim record is present exactly when the claim succeeds.",
                nameof(record));
        }

        Outcome = outcome;
        Record = record;
    }

    public NotificationDeliveryClaimOutcome Outcome { get; }
    public NotificationDeliveryRecord? Record { get; }
}

public interface INotificationDeliveryStore
{
    Task InitializeAsync(
        CancellationToken cancellationToken = default);

    Task<NotificationDeliveryRecord?> GetAsync(
        Guid notificationId,
        string destinationId,
        CancellationToken cancellationToken = default);

    Task<NotificationDeliveryRecord> CreateOrGetAsync(
        NotificationDeliverySnapshot snapshot,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<NotificationDeliveryRecord?> ReplaceAsync(
        NotificationDeliverySnapshot snapshot,
        long expectedRevision,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<NotificationDeliveryClaimResult> TryClaimForDispatchAsync(
        Guid notificationId,
        string destinationId,
        long expectedRevision,
        DateTimeOffset claimedAtUtc,
        int maxConcurrency,
        int ratePerSecond,
        CancellationToken cancellationToken = default);

    Task<NotificationDeliveryPage> ListDueAsync(
        NotificationDeliveryDueQuery query,
        CancellationToken cancellationToken = default);

    Task<NotificationDeliveryRecoveryPage>
        ListStaleInFlightAsync(
            NotificationStaleInFlightQuery query,
            CancellationToken cancellationToken = default);
}
