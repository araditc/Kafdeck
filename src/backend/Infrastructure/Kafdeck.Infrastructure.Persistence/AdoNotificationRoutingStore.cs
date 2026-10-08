using System.Data.Common;
using System.Globalization;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoNotificationRoutingStore :
    INotificationRoutingStore
{
    private const int SchemaVersion = 1;
    private const string Component =
        "notification-routing";

    private readonly INotificationDeliveryDbConnectionFactory
        _connectionFactory;

    public AdoNotificationRoutingStore(
        INotificationDeliveryDbConnectionFactory connectionFactory)
    {
        _connectionFactory =
            connectionFactory ??
            throw new ArgumentNullException(
                nameof(connectionFactory));
    }

    public async Task InitializeAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        if (_connectionFactory.SupportsSelectForUpdate)
        {
            await using var lockCommand =
                connection.CreateCommand();
            lockCommand.Transaction =
                transaction;
            lockCommand.CommandText =
                "SELECT pg_advisory_xact_lock(@lock_key)";
            AddParameter(
                lockCommand,
                "@lock_key",
                PersistenceMigrationLocks
                    .SharedSchemaInfo);
            await lockCommand
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (var statement in
                 InitializationStatements)
        {
            await using var command =
                connection.CreateCommand();
            command.Transaction =
                transaction;
            command.CommandText =
                statement;
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        var existingVersion =
            await ReadSchemaVersionAsync(
                    connection,
                    transaction,
                    cancellationToken)
                .ConfigureAwait(false);

        if (existingVersion is not null &&
            existingVersion.Value !=
                SchemaVersion)
        {
            throw new InvalidOperationException(
                $"Notification-routing schema version {existingVersion.Value} is unsupported by this binary (expected {SchemaVersion}).");
        }

        await using var versionCommand =
            connection.CreateCommand();
        versionCommand.Transaction =
            transaction;
        versionCommand.CommandText =
            """
            INSERT INTO kafdeck_schema_info (
                component,
                schema_version)
            VALUES (
                @component,
                @schema_version)
            ON CONFLICT (component)
            DO NOTHING
            """;
        AddParameter(
            versionCommand,
            "@component",
            Component);
        AddParameter(
            versionCommand,
            "@schema_version",
            SchemaVersion);
        await versionCommand
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<NotificationSafeEventRecord>
        CreateOrGetEventAsync(
            NotificationSafeEvent notificationEvent,
            DateTimeOffset createdAtUtc,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(
            notificationEvent);

        var record =
            new NotificationSafeEventRecord(
                notificationEvent,
                createdAtUtc);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            """
            INSERT INTO kafdeck_notification_events (
                event_id,
                event_class,
                event_type,
                subject,
                summary,
                occurred_at_utc,
                payload_fingerprint,
                created_at_utc)
            VALUES (
                @event_id,
                @event_class,
                @event_type,
                @subject,
                @summary,
                @occurred_at_utc,
                @payload_fingerprint,
                @created_at_utc)
            ON CONFLICT (event_id)
            DO NOTHING
            """;
        BindEvent(
            command,
            record);
        var affected =
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);

        NotificationSafeEventRecord result;
        if (affected == 1)
        {
            result = record;
        }
        else
        {
            result =
                await ReadEventAsync(
                        connection,
                        transaction,
                        notificationEvent.EventId,
                        cancellationToken)
                    .ConfigureAwait(false) ??
                throw new InvalidOperationException(
                    "Notification safe-event conflict could not be resolved.");

            if (!SameEvent(
                    result.Event,
                    notificationEvent))
            {
                throw new InvalidOperationException(
                    "Notification event ID is already bound to different safe event material.");
            }
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return result;
    }

    public async Task<NotificationSafeEventRecord?>
        GetEventAsync(
            Guid eventId,
            CancellationToken cancellationToken = default)
    {
        if (eventId == Guid.Empty)
        {
            throw new ArgumentOutOfRangeException(
                nameof(eventId));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);

        return await ReadEventAsync(
                connection,
                transaction: null,
                eventId,
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<NotificationSubscriptionSnapshot>
        CreateSubscriptionAsync(
            NotificationSubscriptionDefinition definition,
            NotificationSubscriptionState state,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ValidateSubscriptionWrite(
            definition,
            state,
            updatedAtUtc);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            """
            INSERT INTO kafdeck_notification_subscriptions (
                subscription_id,
                destination_id,
                lifecycle_state,
                revision,
                updated_at_utc)
            VALUES (
                @subscription_id,
                @destination_id,
                @state,
                1,
                @updated_at_utc)
            ON CONFLICT (subscription_id)
            DO NOTHING
            """;
        BindSubscription(
            command,
            definition,
            state,
            updatedAtUtc);

        if (await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false) != 1)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            throw new InvalidOperationException(
                $"Notification subscription '{definition.SubscriptionId}' already exists.");
        }

        await ReplaceEventClassesAsync(
                connection,
                transaction,
                definition,
                cancellationToken)
            .ConfigureAwait(false);

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new NotificationSubscriptionSnapshot(
            definition,
            state,
            1,
            updatedAtUtc);
    }

    public async Task<NotificationSubscriptionSnapshot?>
        GetSubscriptionAsync(
            string subscriptionId,
            CancellationToken cancellationToken = default)
    {
        subscriptionId =
            NotificationSubscriptionDefinition
                .NormalizeSubscriptionId(
                    subscriptionId);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        var row =
            await ReadSubscriptionRowAsync(
                    connection,
                    transaction,
                    subscriptionId,
                    lockForUpdate: false,
                    cancellationToken)
                .ConfigureAwait(false);
        if (row is null)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        var eventClasses =
            await ReadEventClassesAsync(
                    connection,
                    transaction,
                    [subscriptionId],
                    cancellationToken)
                .ConfigureAwait(false);

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return ToSnapshot(
            row,
            eventClasses[subscriptionId]);
    }

    public async Task<NotificationSubscriptionSnapshot?>
        ReplaceSubscriptionAsync(
            NotificationSubscriptionDefinition definition,
            NotificationSubscriptionState state,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ValidateSubscriptionWrite(
            definition,
            state,
            updatedAtUtc);
        if (expectedRevision < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedRevision));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        var existing =
            await ReadSubscriptionRowAsync(
                    connection,
                    transaction,
                    definition.SubscriptionId,
                    _connectionFactory.SupportsSelectForUpdate,
                    cancellationToken)
                .ConfigureAwait(false);
        if (existing is null ||
            existing.Revision !=
                expectedRevision)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        if (updatedAtUtc.ToUniversalTime() <
            existing.UpdatedAtUtc)
        {
            throw new ArgumentException(
                "Notification subscription update timestamp must be monotonic.",
                nameof(updatedAtUtc));
        }

        await using var update =
            connection.CreateCommand();
        update.Transaction =
            transaction;
        update.CommandText =
            """
            UPDATE kafdeck_notification_subscriptions
            SET
                destination_id = @destination_id,
                lifecycle_state = @state,
                revision = revision + 1,
                updated_at_utc = @updated_at_utc
            WHERE subscription_id = @subscription_id
              AND revision = @expected_revision
            """;
        BindSubscription(
            update,
            definition,
            state,
            updatedAtUtc);
        AddParameter(
            update,
            "@expected_revision",
            expectedRevision);

        if (await update
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false) != 1)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        await ReplaceEventClassesAsync(
                connection,
                transaction,
                definition,
                cancellationToken)
            .ConfigureAwait(false);

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new NotificationSubscriptionSnapshot(
            definition,
            state,
            checked(
                expectedRevision + 1),
            updatedAtUtc);
    }

    public async Task<NotificationSubscriptionPage>
        ListSubscriptionsAsync(
            NotificationSubscriptionQuery query,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(
            query);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);

        var predicates =
            new List<string>();
        if (query.AfterSubscriptionId is not null)
        {
            predicates.Add(
                "s.subscription_id > @after_subscription_id");
        }

        if (query.State is not null)
        {
            predicates.Add(
                "s.lifecycle_state = @state");
        }

        if (query.EventClass is not null)
        {
            predicates.Add(
                """
                EXISTS (
                    SELECT 1
                    FROM kafdeck_notification_subscription_events se
                    WHERE se.subscription_id = s.subscription_id
                      AND se.event_class = @event_class)
                """);
        }

        await using var command =
            connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT
                s.subscription_id,
                s.destination_id,
                s.lifecycle_state,
                s.revision,
                s.updated_at_utc
            FROM kafdeck_notification_subscriptions s
            {(predicates.Count == 0
                ? string.Empty
                : "WHERE " + string.Join(
                    " AND ",
                    predicates))}
            ORDER BY s.subscription_id
            LIMIT @row_limit
            """;

        if (query.AfterSubscriptionId is not null)
        {
            AddParameter(
                command,
                "@after_subscription_id",
                query.AfterSubscriptionId);
        }

        if (query.State is not null)
        {
            AddParameter(
                command,
                "@state",
                query.State.Value.ToString());
        }

        if (query.EventClass is not null)
        {
            AddParameter(
                command,
                "@event_class",
                query.EventClass.Value.ToString());
        }

        AddParameter(
            command,
            "@row_limit",
            query.MaxResults + 1);

        var rows =
            new List<SubscriptionRow>(
                query.MaxResults + 1);
        await using (var reader =
                     await command
                         .ExecuteReaderAsync(cancellationToken)
                         .ConfigureAwait(false))
        {
            while (await reader
                       .ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
            {
                rows.Add(
                    ReadSubscriptionRow(
                        reader));
            }
        }

        var truncated =
            rows.Count >
            query.MaxResults;
        if (truncated)
        {
            rows.RemoveAt(
                rows.Count - 1);
        }

        if (rows.Count == 0)
        {
            return new NotificationSubscriptionPage(
                Array.Empty<NotificationSubscriptionSnapshot>(),
                false,
                null);
        }

        var ids =
            rows.Select(row => row.SubscriptionId)
                .ToArray();
        var eventClasses =
            await ReadEventClassesAsync(
                    connection,
                    transaction: null,
                    ids,
                    cancellationToken)
                .ConfigureAwait(false);

        var snapshots =
            rows.Select(
                    row =>
                        ToSnapshot(
                            row,
                            eventClasses[row.SubscriptionId]))
                .ToArray();

        return new NotificationSubscriptionPage(
            snapshots,
            truncated,
            truncated
                ? snapshots[^1].Definition.SubscriptionId
                : null);
    }

    private async Task<NotificationSafeEventRecord?>
        ReadEventAsync(
            DbConnection connection,
            DbTransaction? transaction,
            Guid eventId,
            CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            """
            SELECT
                event_class,
                event_type,
                subject,
                summary,
                occurred_at_utc,
                payload_fingerprint,
                created_at_utc
            FROM kafdeck_notification_events
            WHERE event_id = @event_id
            """;
        AddParameter(
            command,
            "@event_id",
            eventId.ToString("D"));

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);
        if (!await reader
                .ReadAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            return null;
        }

        var notificationEvent =
            new NotificationSafeEvent(
                eventId,
                Enum.Parse<NotificationEventClass>(
                    reader.GetString(0),
                    ignoreCase: false),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                Parse(
                    reader.GetString(4)));

        if (!string.Equals(
                notificationEvent.PayloadFingerprint,
                reader.GetString(5),
                StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Persisted notification safe-event fingerprint is inconsistent.");
        }

        return new NotificationSafeEventRecord(
            notificationEvent,
            Parse(
                reader.GetString(6)));
    }

    private async Task<SubscriptionRow?>
        ReadSubscriptionRowAsync(
            DbConnection connection,
            DbTransaction transaction,
            string subscriptionId,
            bool lockForUpdate,
            CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            """
            SELECT
                subscription_id,
                destination_id,
                lifecycle_state,
                revision,
                updated_at_utc
            FROM kafdeck_notification_subscriptions
            WHERE subscription_id = @subscription_id
            """ +
            (lockForUpdate
                ? "\nFOR UPDATE"
                : string.Empty);
        AddParameter(
            command,
            "@subscription_id",
            subscriptionId);

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        return await reader
            .ReadAsync(cancellationToken)
            .ConfigureAwait(false)
            ? ReadSubscriptionRow(
                reader)
            : null;
    }

    private static SubscriptionRow
        ReadSubscriptionRow(
            DbDataReader reader) =>
        new(
            reader.GetString(0),
            reader.GetString(1),
            Enum.Parse<NotificationSubscriptionState>(
                reader.GetString(2),
                ignoreCase: false),
            Convert.ToInt64(
                reader.GetValue(3),
                CultureInfo.InvariantCulture),
            Parse(
                reader.GetString(4)));

    private async Task<Dictionary<string, IReadOnlyList<NotificationEventClass>>>
        ReadEventClassesAsync(
            DbConnection connection,
            DbTransaction? transaction,
            IReadOnlyList<string> subscriptionIds,
            CancellationToken cancellationToken)
    {
        if (subscriptionIds.Count == 0)
        {
            return new Dictionary<
                string,
                IReadOnlyList<NotificationEventClass>>(
                StringComparer.Ordinal);
        }

        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;

        var names =
            new string[subscriptionIds.Count];
        for (var index = 0;
             index < subscriptionIds.Count;
             index++)
        {
            names[index] =
                $"@subscription_{index}";
            AddParameter(
                command,
                names[index],
                subscriptionIds[index]);
        }

        command.CommandText =
            $"""
            SELECT
                subscription_id,
                event_class
            FROM kafdeck_notification_subscription_events
            WHERE subscription_id IN (
                {string.Join(", ", names)})
            ORDER BY
                subscription_id,
                event_class
            """;

        var mutable =
            subscriptionIds.ToDictionary(
                id => id,
                _ => new List<NotificationEventClass>(),
                StringComparer.Ordinal);

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);
        while (await reader
                   .ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            var id =
                reader.GetString(0);
            mutable[id].Add(
                Enum.Parse<NotificationEventClass>(
                    reader.GetString(1),
                    ignoreCase: false));
        }

        return mutable.ToDictionary(
            pair => pair.Key,
            pair =>
                (IReadOnlyList<NotificationEventClass>)
                Array.AsReadOnly(
                    pair.Value.ToArray()),
            StringComparer.Ordinal);
    }

    private async Task ReplaceEventClassesAsync(
        DbConnection connection,
        DbTransaction transaction,
        NotificationSubscriptionDefinition definition,
        CancellationToken cancellationToken)
    {
        await using (var delete =
                     connection.CreateCommand())
        {
            delete.Transaction =
                transaction;
            delete.CommandText =
                """
                DELETE FROM kafdeck_notification_subscription_events
                WHERE subscription_id = @subscription_id
                """;
            AddParameter(
                delete,
                "@subscription_id",
                definition.SubscriptionId);
            await delete
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (var eventClass in
                 definition.EventClasses)
        {
            await using var insert =
                connection.CreateCommand();
            insert.Transaction =
                transaction;
            insert.CommandText =
                """
                INSERT INTO kafdeck_notification_subscription_events (
                    subscription_id,
                    event_class)
                VALUES (
                    @subscription_id,
                    @event_class)
                """;
            AddParameter(
                insert,
                "@subscription_id",
                definition.SubscriptionId);
            AddParameter(
                insert,
                "@event_class",
                eventClass.ToString());
            await insert
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static NotificationSubscriptionSnapshot
        ToSnapshot(
            SubscriptionRow row,
            IReadOnlyList<NotificationEventClass> eventClasses) =>
        new(
            new NotificationSubscriptionDefinition(
                row.SubscriptionId,
                row.DestinationId,
                eventClasses),
            row.State,
            row.Revision,
            row.UpdatedAtUtc);

    private static void ValidateSubscriptionWrite(
        NotificationSubscriptionDefinition definition,
        NotificationSubscriptionState state,
        DateTimeOffset updatedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(
            definition);

        if (!Enum.IsDefined(state) ||
            updatedAtUtc == default)
        {
            throw new ArgumentException(
                "Notification subscription write is invalid.");
        }
    }

    private static bool SameEvent(
        NotificationSafeEvent left,
        NotificationSafeEvent right) =>
        left.EventId == right.EventId &&
        left.EventClass == right.EventClass &&
        string.Equals(
            left.EventType,
            right.EventType,
            StringComparison.Ordinal) &&
        string.Equals(
            left.Subject,
            right.Subject,
            StringComparison.Ordinal) &&
        string.Equals(
            left.Summary,
            right.Summary,
            StringComparison.Ordinal) &&
        left.OccurredAtUtc ==
            right.OccurredAtUtc &&
        string.Equals(
            left.PayloadFingerprint,
            right.PayloadFingerprint,
            StringComparison.Ordinal);

    private static void BindEvent(
        DbCommand command,
        NotificationSafeEventRecord record)
    {
        AddParameter(
            command,
            "@event_id",
            record.Event.EventId.ToString("D"));
        AddParameter(
            command,
            "@event_class",
            record.Event.EventClass.ToString());
        AddParameter(
            command,
            "@event_type",
            record.Event.EventType);
        AddParameter(
            command,
            "@subject",
            record.Event.Subject);
        AddParameter(
            command,
            "@summary",
            record.Event.Summary);
        AddParameter(
            command,
            "@occurred_at_utc",
            Format(
                record.Event.OccurredAtUtc));
        AddParameter(
            command,
            "@payload_fingerprint",
            record.Event.PayloadFingerprint);
        AddParameter(
            command,
            "@created_at_utc",
            Format(
                record.CreatedAtUtc));
    }

    private static void BindSubscription(
        DbCommand command,
        NotificationSubscriptionDefinition definition,
        NotificationSubscriptionState state,
        DateTimeOffset updatedAtUtc)
    {
        AddParameter(
            command,
            "@subscription_id",
            definition.SubscriptionId);
        AddParameter(
            command,
            "@destination_id",
            definition.DestinationId);
        AddParameter(
            command,
            "@state",
            state.ToString());
        AddParameter(
            command,
            "@updated_at_utc",
            Format(
                updatedAtUtc));
    }

    private static async Task<int?>
        ReadSchemaVersionAsync(
            DbConnection connection,
            DbTransaction transaction,
            CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_schema_info
            WHERE component = @component
            """;
        AddParameter(
            command,
            "@component",
            Component);

        var value =
            await command
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return value is null ||
               value is DBNull
            ? null
            : Convert.ToInt32(
                value,
                CultureInfo.InvariantCulture);
    }

    private static DateTimeOffset Parse(
        string value) =>
        DateTimeOffset.Parse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind);

    private static string Format(
        DateTimeOffset value) =>
        value.ToUniversalTime()
            .ToString(
                "O",
                CultureInfo.InvariantCulture);

    private static void AddParameter(
        DbCommand command,
        string name,
        object value)
    {
        var parameter =
            command.CreateParameter();
        parameter.ParameterName =
            name;
        parameter.Value =
            value;
        command.Parameters.Add(
            parameter);
    }

    private sealed record SubscriptionRow(
        string SubscriptionId,
        string DestinationId,
        NotificationSubscriptionState State,
        long Revision,
        DateTimeOffset UpdatedAtUtc);

    private static readonly string[]
        InitializationStatements =
    [
        """
        CREATE TABLE IF NOT EXISTS kafdeck_schema_info (
            component TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_notification_events (
            event_id TEXT PRIMARY KEY,
            event_class TEXT NOT NULL,
            event_type TEXT NOT NULL,
            subject TEXT NOT NULL,
            summary TEXT NOT NULL,
            occurred_at_utc TEXT NOT NULL,
            payload_fingerprint TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_notification_subscriptions (
            subscription_id TEXT PRIMARY KEY,
            destination_id TEXT NOT NULL,
            lifecycle_state TEXT NOT NULL,
            revision BIGINT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            CHECK (revision >= 1),
            CHECK (lifecycle_state IN ('Active', 'Paused'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_notification_subscription_events (
            subscription_id TEXT NOT NULL,
            event_class TEXT NOT NULL,
            PRIMARY KEY (
                subscription_id,
                event_class),
            FOREIGN KEY (subscription_id)
                REFERENCES kafdeck_notification_subscriptions (subscription_id)
                ON DELETE CASCADE
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_subscription_list
        ON kafdeck_notification_subscriptions (
            lifecycle_state,
            subscription_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_subscription_event
        ON kafdeck_notification_subscription_events (
            event_class,
            subscription_id)
        """,
    ];
}
