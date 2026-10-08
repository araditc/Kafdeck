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

    public async Task<DateTimeOffset>
        GetCoordinationUtcNowAsync(
            DateTimeOffset standaloneFallbackUtc,
            CancellationToken cancellationToken = default)
    {
        if (standaloneFallbackUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(standaloneFallbackUtc));
        }

        if (!_connectionFactory.SupportsSelectForUpdate)
        {
            return standaloneFallbackUtc.ToUniversalTime();
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            _connectionFactory.DatabaseUtcNowSql;

        var value =
            await command
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return ParseDatabaseClock(
            value);
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

    public async Task<NotificationDeliveryClaimResult>
        TryClaimForDispatchAsync(
            Guid notificationId,
            string destinationId,
            long expectedRevision,
            DateTimeOffset claimedAtUtc,
            DateTimeOffset notAfterUtc,
            int maxConcurrency,
            int ratePerSecond,
            CancellationToken cancellationToken = default)
    {
        ValidateIdentity(
            notificationId,
            destinationId,
            out var normalizedDestination);

        if (expectedRevision < 1 ||
            claimedAtUtc == default ||
            notAfterUtc == default ||
            maxConcurrency is < 1 or >
                NotificationDeliveryPolicy.HardMaxConcurrency ||
            ratePerSecond is < 1 or >
                NotificationDeliveryPolicy.HardMaxRatePerSecond)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedRevision),
                "Notification dispatch admission parameters are outside admitted bounds.");
        }

        var claimedAt =
            claimedAtUtc.ToUniversalTime();
        var notAfter =
            notAfterUtc.ToUniversalTime();

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        await using (var ensureGuard =
                     connection.CreateCommand())
        {
            ensureGuard.Transaction =
                transaction;
            ensureGuard.CommandText =
                """
                INSERT INTO kafdeck_notification_dispatch_guards (
                    destination_id,
                    fence)
                VALUES (
                    @destination_id,
                    0)
                ON CONFLICT (destination_id)
                DO NOTHING
                """;
            AddParameter(
                ensureGuard,
                "@destination_id",
                normalizedDestination);
            await ensureGuard
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        await using (var lockGuard =
                     connection.CreateCommand())
        {
            lockGuard.Transaction =
                transaction;
            lockGuard.CommandText =
                """
                UPDATE kafdeck_notification_dispatch_guards
                SET fence = fence + 1
                WHERE destination_id = @destination_id
                """;
            AddParameter(
                lockGuard,
                "@destination_id",
                normalizedDestination);
            var affected =
                await lockGuard
                    .ExecuteNonQueryAsync(cancellationToken)
                    .ConfigureAwait(false);
            if (affected != 1)
            {
                throw new InvalidOperationException(
                    "Notification dispatch guard could not be acquired.");
            }
        }

        var rateClockUtc =
            await ReadRateClockUtcAsync(
                    connection,
                    transaction,
                    claimedAt,
                    cancellationToken)
                .ConfigureAwait(false);
        claimedAt =
            rateClockUtc;
        var rateWindowStart =
            rateClockUtc -
            TimeSpan.FromSeconds(1);

        await using (var cleanup =
                     connection.CreateCommand())
        {
            cleanup.Transaction =
                transaction;
            cleanup.CommandText =
                """
                DELETE FROM kafdeck_notification_dispatch_attempts
                WHERE destination_id = @destination_id
                  AND started_at_utc <= @window_start_utc
                """;
            AddParameter(
                cleanup,
                "@destination_id",
                normalizedDestination);
            AddParameter(
                cleanup,
                "@window_start_utc",
                Format(rateWindowStart));
            await cleanup
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        var activeCount =
            await CountAsync(
                    connection,
                    transaction,
                    """
                    SELECT COUNT(*)
                    FROM kafdeck_notification_deliveries
                    WHERE destination_id = @destination_id
                      AND state = 'InFlight'
                    """,
                    normalizedDestination,
                    cancellationToken)
                .ConfigureAwait(false);

        if (activeCount >=
            maxConcurrency)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return new NotificationDeliveryClaimResult(
                NotificationDeliveryClaimOutcome.ConcurrencyLimited);
        }

        var rateCount =
            await CountAsync(
                    connection,
                    transaction,
                    """
                    SELECT COUNT(*)
                    FROM kafdeck_notification_dispatch_attempts
                    WHERE destination_id = @destination_id
                      AND started_at_utc > @window_start_utc
                    """,
                    normalizedDestination,
                    cancellationToken,
                    rateWindowStart)
                .ConfigureAwait(false);

        if (rateCount >=
            ratePerSecond)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return new NotificationDeliveryClaimResult(
                NotificationDeliveryClaimOutcome.RateLimited);
        }

        var existing =
            await ReadExistingAsync(
                    connection,
                    transaction,
                    notificationId,
                    normalizedDestination,
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
            return new NotificationDeliveryClaimResult(
                NotificationDeliveryClaimOutcome.VersionConflict);
        }

        var hardLatest =
            existing.Snapshot.CreatedAtUtc <=
                    DateTimeOffset.MaxValue -
                    NotificationDeliveryPolicy.HardMaxLifetime
                ? existing.Snapshot.CreatedAtUtc +
                  NotificationDeliveryPolicy.HardMaxLifetime
                : DateTimeOffset.MaxValue;
        if (notAfter <
                existing.Snapshot.CreatedAtUtc ||
            notAfter >
                hardLatest)
        {
            throw new ArgumentException(
                "Notification dispatch claim lifetime boundary is outside admitted bounds.",
                nameof(notAfterUtc));
        }

        if (claimedAt >
            notAfter)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return new NotificationDeliveryClaimResult(
                NotificationDeliveryClaimOutcome.Expired);
        }

        var dueAt =
            existing.Snapshot.NextAttemptAtUtc ??
            existing.Snapshot.CreatedAtUtc;

        if (existing.Snapshot.State is not
                (NotificationDeliveryState.Pending or
                 NotificationDeliveryState.Failed) ||
            dueAt >
                claimedAt)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return new NotificationDeliveryClaimResult(
                NotificationDeliveryClaimOutcome.NotClaimable);
        }

        if (claimedAt <
            existing.UpdatedAtUtc)
        {
            throw new ArgumentException(
                "Notification dispatch claim timestamp must be monotonic.",
                nameof(claimedAtUtc));
        }

        var claimedSnapshot =
            new NotificationDeliverySnapshot(
                existing.Snapshot.NotificationId,
                existing.Snapshot.DestinationId,
                existing.Snapshot.PayloadFingerprint,
                NotificationDeliveryState.InFlight,
                checked(
                    existing.Snapshot.AttemptCount + 1),
                existing.Snapshot.CreatedAtUtc);

        NotificationDeliveryTransition
            .ValidateReplacement(
                existing.Snapshot,
                claimedSnapshot);

        await using (var update =
                     connection.CreateCommand())
        {
            update.Transaction =
                transaction;
            update.CommandText =
                """
                UPDATE kafdeck_notification_deliveries
                SET
                    state = @state,
                    attempt_count = @attempt_count,
                    next_attempt_at_utc = NULL,
                    due_at_utc = @due_at_utc,
                    outcome_code = NULL,
                    revision = revision + 1,
                    updated_at_utc = @updated_at_utc
                WHERE notification_id = @notification_id
                  AND destination_id = @destination_id
                  AND revision = @expected_revision
                """;
            AddParameter(
                update,
                "@state",
                NotificationDeliveryState.InFlight.ToString());
            AddParameter(
                update,
                "@attempt_count",
                claimedSnapshot.AttemptCount);
            AddParameter(
                update,
                "@due_at_utc",
                Format(
                    claimedSnapshot.CreatedAtUtc));
            AddParameter(
                update,
                "@updated_at_utc",
                Format(claimedAt));
            AddParameter(
                update,
                "@notification_id",
                notificationId.ToString("D"));
            AddParameter(
                update,
                "@destination_id",
                normalizedDestination);
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
                return new NotificationDeliveryClaimResult(
                    NotificationDeliveryClaimOutcome.VersionConflict);
            }
        }

        await using (var attempt =
                     connection.CreateCommand())
        {
            attempt.Transaction =
                transaction;
            attempt.CommandText =
                """
                INSERT INTO kafdeck_notification_dispatch_attempts (
                    notification_id,
                    destination_id,
                    attempt_no,
                    started_at_utc)
                VALUES (
                    @notification_id,
                    @destination_id,
                    @attempt_no,
                    @started_at_utc)
                """;
            AddParameter(
                attempt,
                "@notification_id",
                notificationId.ToString("D"));
            AddParameter(
                attempt,
                "@destination_id",
                normalizedDestination);
            AddParameter(
                attempt,
                "@attempt_no",
                claimedSnapshot.AttemptCount);
            AddParameter(
                attempt,
                "@started_at_utc",
                Format(rateClockUtc));

            await attempt
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new NotificationDeliveryClaimResult(
            NotificationDeliveryClaimOutcome.Claimed,
            new NotificationDeliveryRecord(
                claimedSnapshot,
                checked(
                    expectedRevision + 1),
                claimedAt));
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
            (query.After is null
                ? """
                  FROM kafdeck_notification_deliveries
                  WHERE state IN ('Pending', 'Failed')
                    AND due_at_utc <= @now_utc
                  ORDER BY
                      due_at_utc,
                      notification_id,
                      destination_id
                  LIMIT @row_limit
                  """
                : """
                  FROM kafdeck_notification_deliveries
                  WHERE state IN ('Pending', 'Failed')
                    AND due_at_utc <= @now_utc
                    AND (
                        due_at_utc,
                        notification_id,
                        destination_id) > (
                        @after_due_at_utc,
                        @after_notification_id,
                        @after_destination_id)
                  ORDER BY
                      due_at_utc,
                      notification_id,
                      destination_id
                  LIMIT @row_limit
                  """);

        AddParameter(
            command,
            "@now_utc",
            Format(query.NowUtc));
        if (query.After is not null)
        {
            AddParameter(
                command,
                "@after_due_at_utc",
                Format(
                    query.After.DueAtUtc));
            AddParameter(
                command,
                "@after_notification_id",
                query.After.NotificationId
                    .ToString("D"));
            AddParameter(
                command,
                "@after_destination_id",
                query.After.DestinationId);
        }

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
            (query.After is null
                ? """
                  FROM kafdeck_notification_deliveries
                  WHERE state = 'InFlight'
                    AND updated_at_utc <= @stale_before_utc
                  ORDER BY
                      updated_at_utc,
                      notification_id,
                      destination_id
                  LIMIT @row_limit
                  """
                : """
                  FROM kafdeck_notification_deliveries
                  WHERE state = 'InFlight'
                    AND updated_at_utc <= @stale_before_utc
                    AND (
                        updated_at_utc,
                        notification_id,
                        destination_id) > (
                        @after_updated_at_utc,
                        @after_notification_id,
                        @after_destination_id)
                  ORDER BY
                      updated_at_utc,
                      notification_id,
                      destination_id
                  LIMIT @row_limit
                  """);

        AddParameter(
            command,
            "@stale_before_utc",
            Format(
                query.StaleBeforeUtc));
        if (query.After is not null)
        {
            AddParameter(
                command,
                "@after_updated_at_utc",
                Format(
                    query.After.UpdatedAtUtc));
            AddParameter(
                command,
                "@after_notification_id",
                query.After.NotificationId
                    .ToString("D"));
            AddParameter(
                command,
                "@after_destination_id",
                query.After.DestinationId);
        }

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

    private static async Task<int> CountAsync(
        DbConnection connection,
        DbTransaction transaction,
        string sql,
        string destinationId,
        CancellationToken cancellationToken,
        DateTimeOffset? windowStartUtc = null)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            sql;
        AddParameter(
            command,
            "@destination_id",
            destinationId);
        if (windowStartUtc is not null)
        {
            AddParameter(
                command,
                "@window_start_utc",
                Format(
                    windowStartUtc.Value));
        }

        var value =
            await command
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return Convert.ToInt32(
            value,
            CultureInfo.InvariantCulture);
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
        CREATE TABLE IF NOT EXISTS kafdeck_notification_dispatch_guards (
            destination_id TEXT PRIMARY KEY,
            fence BIGINT NOT NULL,
            CHECK (fence >= 0)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_notification_dispatch_attempts (
            notification_id TEXT NOT NULL,
            destination_id TEXT NOT NULL,
            attempt_no INTEGER NOT NULL,
            started_at_utc TEXT NOT NULL,
            PRIMARY KEY (
                notification_id,
                destination_id,
                attempt_no),
            CHECK (attempt_no >= 1 AND attempt_no <= 10)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_dispatch_attempt_window
        ON kafdeck_notification_dispatch_attempts (
            destination_id,
            started_at_utc)
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
            updated_at_utc,
            notification_id,
            destination_id)
        WHERE state = 'InFlight'
        """,
    ];

    private async Task<DateTimeOffset>
        ReadRateClockUtcAsync(
            DbConnection connection,
            DbTransaction transaction,
            DateTimeOffset standaloneFallbackUtc,
            CancellationToken cancellationToken)
    {
        if (!_connectionFactory.SupportsSelectForUpdate)
        {
            return standaloneFallbackUtc.ToUniversalTime();
        }

        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            _connectionFactory.DatabaseUtcNowSql;

        var value =
            await command
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return ParseDatabaseClock(
            value);
    }

    private static DateTimeOffset ParseDatabaseClock(
        object? value) =>
        value switch
        {
            DateTimeOffset offset =>
                offset.ToUniversalTime(),
            DateTime dateTime =>
                new DateTimeOffset(
                    dateTime.Kind == DateTimeKind.Unspecified
                        ? DateTime.SpecifyKind(
                            dateTime,
                            DateTimeKind.Utc)
                        : dateTime)
                .ToUniversalTime(),
            string text =>
                DateTimeOffset.Parse(
                    text,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal |
                    DateTimeStyles.AdjustToUniversal),
            _ => throw new InvalidOperationException(
                "Notification delivery database clock returned an unsupported value."),
        };

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
