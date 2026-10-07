using System.Data.Common;
using System.Globalization;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoNotificationDeliveryStore :
    INotificationDeliveryStore
{
    private const int SchemaVersion = 1;
    private const string Component =
        "notification-delivery";

    private readonly INotificationDeliveryDbConnectionFactory
        _connectionFactory;

    public AdoNotificationDeliveryStore(
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

        if (_connectionFactory
            .SupportsSelectForUpdate)
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
                $"Notification-delivery schema version {existing.Value} is unsupported by this binary (expected {SchemaVersion}).");
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

    public async Task<NotificationDeliveryRecord?>
        GetAsync(
            Guid notificationId,
            string destinationId,
            CancellationToken cancellationToken = default)
    {
        ValidateIdentity(
            notificationId,
            destinationId,
            out var normalizedDestination);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            SelectColumns +
            "\n" +
            """
            FROM kafdeck_notification_deliveries
            WHERE notification_id = @notification_id
              AND destination_id = @destination_id
            """;
        AddParameter(
            command,
            "@notification_id",
            notificationId.ToString("D"));
        AddParameter(
            command,
            "@destination_id",
            normalizedDestination);

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

    public async Task<NotificationDeliveryRecord>
        CreateOrGetAsync(
            NotificationDeliverySnapshot snapshot,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        NotificationDeliveryTransition
            .ValidateInitial(snapshot);
        ValidateUpdatedAt(
            snapshot,
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
            INSERT INTO kafdeck_notification_deliveries (
                notification_id,
                destination_id,
                payload_fingerprint,
                state,
                attempt_count,
                created_at_utc,
                next_attempt_at_utc,
                due_at_utc,
                outcome_code,
                revision,
                updated_at_utc)
            VALUES (
                @notification_id,
                @destination_id,
                @payload_fingerprint,
                @state,
                @attempt_count,
                @created_at_utc,
                @next_attempt_at_utc,
                @due_at_utc,
                @outcome_code,
                1,
                @updated_at_utc)
            ON CONFLICT (
                notification_id,
                destination_id)
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

        NotificationDeliveryRecord record;
        if (affected == 1)
        {
            record =
                new NotificationDeliveryRecord(
                    snapshot,
                    1,
                    updatedAtUtc);
        }
        else
        {
            record =
                await ReadExistingAsync(
                        connection,
                        transaction,
                        snapshot.NotificationId,
                        snapshot.DestinationId,
                        lockForUpdate: false,
                        cancellationToken)
                    .ConfigureAwait(false) ??
                throw new InvalidOperationException(
                    "Notification delivery create conflict could not be resolved.");

            if (!string.Equals(
                    record.Snapshot.PayloadFingerprint,
                    snapshot.PayloadFingerprint,
                    StringComparison.Ordinal) ||
                record.Snapshot.CreatedAtUtc !=
                    snapshot.CreatedAtUtc)
            {
                throw new InvalidOperationException(
                    "Notification delivery identity already exists with different immutable delivery metadata.");
            }
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);
        return record;
    }

    public async Task<NotificationDeliveryRecord?>
        ReplaceAsync(
            NotificationDeliverySnapshot snapshot,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(
            snapshot);

        if (expectedRevision < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedRevision));
        }

        ValidateUpdatedAt(
            snapshot,
            updatedAtUtc);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        var existing =
            await ReadExistingAsync(
                    connection,
                    transaction,
                    snapshot.NotificationId,
                    snapshot.DestinationId,
                    _connectionFactory
                        .SupportsSelectForUpdate,
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
                "Notification delivery update timestamps must be monotonic across revisions.",
                nameof(updatedAtUtc));
        }

        NotificationDeliveryTransition
            .ValidateReplacement(
                existing.Snapshot,
                snapshot);

        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            """
            UPDATE kafdeck_notification_deliveries
            SET
                state = @state,
                attempt_count = @attempt_count,
                next_attempt_at_utc = @next_attempt_at_utc,
                due_at_utc = @due_at_utc,
                outcome_code = @outcome_code,
                revision = revision + 1,
                updated_at_utc = @updated_at_utc
            WHERE notification_id = @notification_id
              AND destination_id = @destination_id
              AND revision = @expected_revision
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
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new NotificationDeliveryRecord(
            snapshot,
            checked(
                expectedRevision + 1),
            updatedAtUtc);
    }

    public async Task<NotificationDeliveryPage>
        ListDueAsync(
            NotificationDeliveryDueQuery query,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(
            query);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();

        command.CommandText =
            SelectColumns +
            "\n" +
            """
            FROM kafdeck_notification_deliveries
            WHERE state IN ('Pending', 'Failed')
              AND due_at_utc <= @now_utc
              AND (
                    CAST(@after_due_at_utc AS TEXT) IS NULL
                    OR due_at_utc > @after_due_at_utc
                    OR (
                        due_at_utc = @after_due_at_utc
                        AND notification_id > @after_notification_id)
                    OR (
                        due_at_utc = @after_due_at_utc
                        AND notification_id = @after_notification_id
                        AND destination_id > @after_destination_id))
            ORDER BY
                due_at_utc,
                notification_id,
                destination_id
            LIMIT @row_limit
            """;

        AddParameter(
            command,
            "@now_utc",
            Format(query.NowUtc));
        AddParameter(
            command,
            "@after_due_at_utc",
            query.After is null
                ? DBNull.Value
                : Format(
                    query.After.DueAtUtc));
        AddParameter(
            command,
            "@after_notification_id",
            query.After is null
                ? string.Empty
                : query.After.NotificationId
                    .ToString("D"));
        AddParameter(
            command,
            "@after_destination_id",
            query.After?.DestinationId ??
            string.Empty);
        AddParameter(
            command,
            "@row_limit",
            query.MaxResults + 1);

        var items =
            new List<NotificationDeliveryRecord>(
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
            if (items.Count >=
                query.MaxResults)
            {
                truncated = true;
                break;
            }

            items.Add(
                ReadRecord(reader));
        }

        NotificationDeliveryDueCursor?
            nextCursor = null;
        if (truncated)
        {
            var last =
                items[^1].Snapshot;
            nextCursor =
                new NotificationDeliveryDueCursor(
                    last.NextAttemptAtUtc ??
                    last.CreatedAtUtc,
                    last.NotificationId,
                    last.DestinationId);
        }

        return new NotificationDeliveryPage(
            items,
            truncated,
            nextCursor);
    }

    public async Task<NotificationDeliveryRecoveryPage>
        ListStaleInFlightAsync(
            NotificationStaleInFlightQuery query,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(
            query);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();

        command.CommandText =
            SelectColumns +
            "\n" +
            """
            FROM kafdeck_notification_deliveries
            WHERE state = 'InFlight'
              AND updated_at_utc <= @stale_before_utc
              AND (
                    CAST(@after_updated_at_utc AS TEXT) IS NULL
                    OR updated_at_utc > @after_updated_at_utc
                    OR (
                        updated_at_utc = @after_updated_at_utc
                        AND notification_id > @after_notification_id)
                    OR (
                        updated_at_utc = @after_updated_at_utc
                        AND notification_id = @after_notification_id
                        AND destination_id > @after_destination_id))
            ORDER BY
                updated_at_utc,
                notification_id,
                destination_id
            LIMIT @row_limit
            """;

        AddParameter(
            command,
            "@stale_before_utc",
            Format(
                query.StaleBeforeUtc));
        AddParameter(
            command,
            "@after_updated_at_utc",
            query.After is null
                ? DBNull.Value
                : Format(
                    query.After.UpdatedAtUtc));
        AddParameter(
            command,
            "@after_notification_id",
            query.After is null
                ? string.Empty
                : query.After.NotificationId
                    .ToString("D"));
        AddParameter(
            command,
            "@after_destination_id",
            query.After?.DestinationId ??
            string.Empty);
        AddParameter(
            command,
            "@row_limit",
            query.MaxResults + 1);

        var items =
            new List<NotificationDeliveryRecord>(
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
            if (items.Count >=
                query.MaxResults)
            {
                truncated = true;
                break;
            }

            items.Add(
                ReadRecord(reader));
        }

        NotificationDeliveryRecoveryCursor?
            nextCursor = null;
        if (truncated)
        {
            var last =
                items[^1];
            nextCursor =
                new NotificationDeliveryRecoveryCursor(
                    last.UpdatedAtUtc,
                    last.Snapshot.NotificationId,
                    last.Snapshot.DestinationId);
        }

        return new NotificationDeliveryRecoveryPage(
            items,
            truncated,
            nextCursor);
    }

    private async Task<NotificationDeliveryRecord?>
        ReadExistingAsync(
            DbConnection connection,
            DbTransaction transaction,
            Guid notificationId,
            string destinationId,
            bool lockForUpdate,
            CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            SelectColumns +
            "\n" +
            """
            FROM kafdeck_notification_deliveries
            WHERE notification_id = @notification_id
              AND destination_id = @destination_id
            """ +
            (lockForUpdate
                ? " FOR UPDATE"
                : string.Empty);

        AddParameter(
            command,
            "@notification_id",
            notificationId.ToString("D"));
        AddParameter(
            command,
            "@destination_id",
            destinationId);

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

    private static NotificationDeliveryRecord
        ReadRecord(
            DbDataReader reader)
    {
        var snapshot =
            new NotificationDeliverySnapshot(
                Guid.Parse(
                    reader.GetString(0)),
                reader.GetString(1),
                reader.GetString(2),
                Enum.Parse<NotificationDeliveryState>(
                    reader.GetString(3),
                    ignoreCase: false),
                Convert.ToInt32(
                    reader.GetValue(4),
                    CultureInfo.InvariantCulture),
                Parse(
                    reader.GetString(5)),
                reader.IsDBNull(6)
                    ? null
                    : Parse(
                        reader.GetString(6)),
                reader.IsDBNull(7)
                    ? null
                    : reader.GetString(7),
                null);

        return new NotificationDeliveryRecord(
            snapshot,
            Convert.ToInt64(
                reader.GetValue(8),
                CultureInfo.InvariantCulture),
            Parse(
                reader.GetString(9)));
    }

    private static void BindSnapshot(
        DbCommand command,
        NotificationDeliverySnapshot snapshot,
        DateTimeOffset updatedAtUtc)
    {
        AddParameter(
            command,
            "@notification_id",
            snapshot.NotificationId
                .ToString("D"));
        AddParameter(
            command,
            "@destination_id",
            snapshot.DestinationId);
        AddParameter(
            command,
            "@payload_fingerprint",
            snapshot.PayloadFingerprint);
        AddParameter(
            command,
            "@state",
            snapshot.State.ToString());
        AddParameter(
            command,
            "@attempt_count",
            snapshot.AttemptCount);
        AddParameter(
            command,
            "@created_at_utc",
            Format(
                snapshot.CreatedAtUtc));
        AddParameter(
            command,
            "@next_attempt_at_utc",
            snapshot.NextAttemptAtUtc is null
                ? DBNull.Value
                : Format(
                    snapshot.NextAttemptAtUtc.Value));
        AddParameter(
            command,
            "@due_at_utc",
            Format(
                snapshot.NextAttemptAtUtc ??
                snapshot.CreatedAtUtc));
        AddParameter(
            command,
            "@outcome_code",
            snapshot.OutcomeCode is null
                ? DBNull.Value
                : snapshot.OutcomeCode);
        AddParameter(
            command,
            "@updated_at_utc",
            Format(
                updatedAtUtc));
    }

    private static void ValidateIdentity(
        Guid notificationId,
        string destinationId,
        out string normalizedDestination)
    {
        if (notificationId ==
            Guid.Empty)
        {
            throw new ArgumentOutOfRangeException(
                nameof(notificationId));
        }

        normalizedDestination =
            NotificationDeliveryIdentity
                .NormalizeDestinationId(
                    destinationId);
    }

    private static void ValidateUpdatedAt(
        NotificationDeliverySnapshot snapshot,
        DateTimeOffset updatedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(
            snapshot);

        if (updatedAtUtc == default ||
            updatedAtUtc <
                snapshot.CreatedAtUtc)
        {
            throw new ArgumentException(
                "Notification delivery update timestamp is invalid.",
                nameof(updatedAtUtc));
        }
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

    private const string SelectColumns =
        """
        SELECT
            notification_id,
            destination_id,
            payload_fingerprint,
            state,
            attempt_count,
            created_at_utc,
            next_attempt_at_utc,
            outcome_code,
            revision,
            updated_at_utc
        """;

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
        CREATE TABLE IF NOT EXISTS kafdeck_notification_deliveries (
            notification_id TEXT NOT NULL,
            destination_id TEXT NOT NULL,
            payload_fingerprint TEXT NOT NULL,
            state TEXT NOT NULL,
            attempt_count INTEGER NOT NULL,
            created_at_utc TEXT NOT NULL,
            next_attempt_at_utc TEXT NULL,
            due_at_utc TEXT NOT NULL,
            outcome_code TEXT NULL,
            revision BIGINT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (
                notification_id,
                destination_id),
            CHECK (revision >= 1),
            CHECK (attempt_count >= 0 AND attempt_count <= 10),
            CHECK (
                state IN (
                    'Pending',
                    'InFlight',
                    'Delivered',
                    'Failed',
                    'UnknownExternalEffect',
                    'Exhausted')),
            CHECK (
                (state IN ('Pending', 'InFlight') AND outcome_code IS NULL)
                OR (state = 'Delivered' AND outcome_code IS NOT NULL AND outcome_code = 'delivered')
                OR (state = 'Failed' AND outcome_code IS NOT NULL AND outcome_code = 'retryable-failure')
                OR (state = 'UnknownExternalEffect' AND outcome_code IS NOT NULL AND outcome_code = 'unknown-external-effect')
                OR (state = 'Exhausted' AND outcome_code IS NOT NULL AND outcome_code = 'exhausted'))
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_delivery_due
        ON kafdeck_notification_deliveries (
            due_at_utc,
            notification_id,
            destination_id)
        WHERE state IN ('Pending', 'Failed')
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_delivery_recovery
        ON kafdeck_notification_deliveries (
            state,
            updated_at_utc,
            notification_id,
            destination_id)
        """,
    ];

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
}
