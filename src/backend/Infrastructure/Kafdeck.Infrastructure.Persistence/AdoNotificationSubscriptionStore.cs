using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoNotificationSubscriptionStore :
    INotificationSubscriptionStore
{
    private const int SchemaVersion = 1;
    private const string Component =
        "notification-subscriptions";

    private static readonly JsonSerializerOptions
        JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly INotificationDeliveryDbConnectionFactory
        _connectionFactory;

    public AdoNotificationSubscriptionStore(
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
                PersistenceMigrationLocks.SharedSchemaInfo);
            await lockCommand
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (var statement in InitializationStatements)
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

        var existing =
            await ReadSchemaVersionAsync(
                    connection,
                    transaction,
                    cancellationToken)
                .ConfigureAwait(false);
        if (existing is not null &&
            existing.Value != SchemaVersion)
        {
            throw new InvalidOperationException(
                $"Notification-subscription schema version {existing.Value} is unsupported by this binary (expected {SchemaVersion}).");
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

    public async Task<NotificationSubscriptionRecord?>
        GetAsync(
            string subscriptionId,
            CancellationToken cancellationToken = default)
    {
        var normalized =
            NotificationSubscriptionSnapshot
                .NormalizeSubscriptionId(
                    subscriptionId);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            SelectColumns +
            """
            
            FROM kafdeck_notification_subscriptions
            WHERE subscription_id = @subscription_id
            """;
        AddParameter(
            command,
            "@subscription_id",
            normalized);

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        return await reader
            .ReadAsync(cancellationToken)
            .ConfigureAwait(false)
            ? ReadRecord(reader)
            : null;
    }

    public async Task<NotificationSubscriptionRecord>
        CreateAsync(
            NotificationSubscriptionSnapshot snapshot,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ValidateWrite(
            snapshot,
            updatedAtUtc);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO kafdeck_notification_subscriptions (
                subscription_id,
                destination_id,
                lifecycle_state,
                revision,
                created_at_utc,
                updated_at_utc,
                event_classes_json,
                event_types_json)
            VALUES (
                @subscription_id,
                @destination_id,
                @lifecycle_state,
                1,
                @created_at_utc,
                @updated_at_utc,
                @event_classes_json,
                @event_types_json)
            ON CONFLICT (subscription_id)
            DO NOTHING
            """;
        BindSnapshot(
            command,
            snapshot,
            updatedAtUtc);

        var affected =
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);

        if (affected != 1)
        {
            throw new InvalidOperationException(
                $"Notification subscription '{snapshot.SubscriptionId}' already exists.");
        }

        return new NotificationSubscriptionRecord(
            snapshot,
            1,
            updatedAtUtc);
    }

    public async Task<NotificationSubscriptionRecord?>
        ReplaceAsync(
            NotificationSubscriptionSnapshot snapshot,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ValidateWrite(
            snapshot,
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
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            """
            UPDATE kafdeck_notification_subscriptions
            SET
                destination_id = @destination_id,
                lifecycle_state = @lifecycle_state,
                revision = revision + 1,
                updated_at_utc = @updated_at_utc,
                event_classes_json = @event_classes_json,
                event_types_json = @event_types_json
            WHERE subscription_id = @subscription_id
              AND revision = @expected_revision
              AND created_at_utc = @created_at_utc
            """;
        BindSnapshot(
            command,
            snapshot,
            updatedAtUtc);
        AddParameter(
            command,
            "@expected_revision",
            expectedRevision);

        var affected =
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        if (affected != 1)
        {
            return null;
        }

        return new NotificationSubscriptionRecord(
            snapshot,
            checked(
                expectedRevision + 1),
            updatedAtUtc);
    }

    public async Task<NotificationSubscriptionPage>
        ListAsync(
            NotificationSubscriptionListQuery query,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        var predicates =
            new List<string>();

        if (query.DestinationId is not null)
        {
            predicates.Add(
                "destination_id = @destination_id");
        }

        if (query.State is not null)
        {
            predicates.Add(
                "lifecycle_state = @lifecycle_state");
        }

        if (query.AfterSubscriptionId is not null)
        {
            predicates.Add(
                "subscription_id > @after_subscription_id");
        }

        var where =
            predicates.Count == 0
                ? string.Empty
                : " WHERE " +
                  string.Join(
                      " AND ",
                      predicates);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();

        command.CommandText =
            SelectColumns +
            "
FROM kafdeck_notification_subscriptions" +
            where +
            """
            
            ORDER BY subscription_id
            LIMIT @row_limit
            """;

        if (query.DestinationId is not null)
        {
            AddParameter(
                command,
                "@destination_id",
                query.DestinationId);
        }

        if (query.State is not null)
        {
            AddParameter(
                command,
                "@lifecycle_state",
                query.State.Value.ToString());
        }

        if (query.AfterSubscriptionId is not null)
        {
            AddParameter(
                command,
                "@after_subscription_id",
                query.AfterSubscriptionId);
        }

        AddParameter(
            command,
            "@row_limit",
            query.MaxResults + 1);

        var items =
            new List<NotificationSubscriptionRecord>(
                query.MaxResults);
        var truncated =
            false;

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        while (await reader
                   .ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            if (items.Count >= query.MaxResults)
            {
                truncated =
                    true;
                break;
            }

            items.Add(
                ReadRecord(reader));
        }

        return new NotificationSubscriptionPage(
            items,
            truncated,
            truncated
                ? items[^1].Snapshot.SubscriptionId
                : null);
    }

    private static void ValidateWrite(
        NotificationSubscriptionSnapshot snapshot,
        DateTimeOffset updatedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        if (updatedAtUtc == default ||
            updatedAtUtc <
                snapshot.CreatedAtUtc)
        {
            throw new ArgumentException(
                "Notification subscription update timestamp is invalid.",
                nameof(updatedAtUtc));
        }
    }

    private static void BindSnapshot(
        DbCommand command,
        NotificationSubscriptionSnapshot snapshot,
        DateTimeOffset updatedAtUtc)
    {
        AddParameter(
            command,
            "@subscription_id",
            snapshot.SubscriptionId);
        AddParameter(
            command,
            "@destination_id",
            snapshot.DestinationId);
        AddParameter(
            command,
            "@lifecycle_state",
            snapshot.State.ToString());
        AddParameter(
            command,
            "@created_at_utc",
            Format(snapshot.CreatedAtUtc));
        AddParameter(
            command,
            "@updated_at_utc",
            Format(updatedAtUtc));
        AddParameter(
            command,
            "@event_classes_json",
            JsonSerializer.Serialize(
                snapshot.Filter.EventClasses,
                JsonOptions));
        AddParameter(
            command,
            "@event_types_json",
            JsonSerializer.Serialize(
                snapshot.Filter.EventTypes,
                JsonOptions));
    }

    private static NotificationSubscriptionRecord
        ReadRecord(
            DbDataReader reader)
    {
        var subscriptionId =
            reader.GetString(0);
        var destinationId =
            reader.GetString(1);
        var state =
            Enum.Parse<NotificationSubscriptionState>(
                reader.GetString(2),
                ignoreCase: false);
        var revision =
            Convert.ToInt64(
                reader.GetValue(3),
                CultureInfo.InvariantCulture);
        var createdAt =
            Parse(reader.GetString(4));
        var updatedAt =
            Parse(reader.GetString(5));

        var classes =
            JsonSerializer.Deserialize<
                NotificationEventClass[]>(
                reader.GetString(6),
                JsonOptions) ??
            throw new InvalidOperationException(
                "Persisted notification subscription event classes are invalid.");
        var types =
            JsonSerializer.Deserialize<
                string[]>(
                reader.GetString(7),
                JsonOptions) ??
            throw new InvalidOperationException(
                "Persisted notification subscription event types are invalid.");

        return new NotificationSubscriptionRecord(
            new NotificationSubscriptionSnapshot(
                subscriptionId,
                destinationId,
                new NotificationSubscriptionFilter(
                    classes,
                    types),
                state,
                createdAt),
            revision,
            updatedAt);
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
        value
            .ToUniversalTime()
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
        command.Parameters.Add(parameter);
    }

    private const string SelectColumns =
        """
        SELECT
            subscription_id,
            destination_id,
            lifecycle_state,
            revision,
            created_at_utc,
            updated_at_utc,
            event_classes_json,
            event_types_json
        """;

    private static readonly string[] InitializationStatements =
    [
        """
        CREATE TABLE IF NOT EXISTS kafdeck_schema_info (
            component TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_notification_subscriptions (
            subscription_id TEXT PRIMARY KEY,
            destination_id TEXT NOT NULL,
            lifecycle_state TEXT NOT NULL
                CHECK (lifecycle_state IN ('Active', 'Paused', 'Retired')),
            revision BIGINT NOT NULL
                CHECK (revision >= 1),
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            event_classes_json TEXT NOT NULL,
            event_types_json TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_subscription_destination
        ON kafdeck_notification_subscriptions (
            destination_id,
            subscription_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_subscription_state
        ON kafdeck_notification_subscriptions (
            lifecycle_state,
            subscription_id)
        """,
    ];
}
