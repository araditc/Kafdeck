using System.Data.Common;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Notifications;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Persistence;

// Minimal product-event metadata in the SAME transaction as the verified
// terminal mutation. No canonical intent, subject, identity or provider values.
public sealed class MutationNotificationOutbox(IMutationDbConnectionFactory connections,
    NotificationProductEventPolicy? configuredPolicy = null) : INotificationProductEventOutbox
{
    private readonly NotificationProductEventPolicy _policy = configuredPolicy ?? new();

    internal static async Task InitializeSchemaAsync(DbConnection connection, DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        foreach (var sql in new[]
        {
            """
            INSERT INTO kafdeck_schema_info(component, schema_version)
            VALUES ('notification-product-outbox', 1) ON CONFLICT(component) DO NOTHING
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_notification_product_source (
                singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
                source_id TEXT NOT NULL,
                routed_count BIGINT NOT NULL DEFAULT 0 CHECK(routed_count >= 0),
                expired_count BIGINT NOT NULL DEFAULT 0 CHECK(expired_count >= 0),
                overflow_count BIGINT NOT NULL DEFAULT 0 CHECK(overflow_count >= 0),
                last_loss_at_utc TEXT NULL)
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_notification_product_outbox (
                event_id TEXT PRIMARY KEY,
                operation_id TEXT NOT NULL,
                operation_version BIGINT NOT NULL CHECK(operation_version > 0),
                kind TEXT NOT NULL CHECK(kind = 'mutation-applied-verified'),
                occurred_at_utc TEXT NOT NULL,
                captured_at_utc TEXT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                UNIQUE(operation_id, operation_version),
                FOREIGN KEY(operation_id) REFERENCES kafdeck_mutation_operations(operation_id) ON DELETE CASCADE)
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_notification_product_outbox_pending
            ON kafdeck_notification_product_outbox(captured_at_utc, event_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_notification_product_outbox_expiry
            ON kafdeck_notification_product_outbox(expires_at_utc)
            """
        })
            await ExecuteAsync(connection, transaction, sql, cancellationToken).ConfigureAwait(false);
        await using (var version = Command(connection, transaction,
                         "SELECT schema_version FROM kafdeck_schema_info WHERE component = 'notification-product-outbox'"))
        {
            if (Convert.ToInt32(await version.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture) != 1)
                throw new InvalidOperationException("Product notification outbox schema is unsupported.");
        }
        await using var insert = Command(connection, transaction,
            "INSERT INTO kafdeck_notification_product_source(singleton_id, source_id) VALUES (1, @source) ON CONFLICT(singleton_id) DO NOTHING");
        Add(insert, "@source", Guid.NewGuid().ToString("D"));
        await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    internal static async Task CaptureVerifiedAsync(DbConnection connection, DbTransaction transaction,
        MutationOperationSnapshot operation, NotificationProductEventPolicy policy, bool postgres,
        CancellationToken cancellationToken)
    {
        if (operation.State != MutationOperationState.AppliedVerified) return;
        await LockSourceAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        var now = await DatabaseUtcAsync(connection, transaction, postgres, cancellationToken).ConfigureAwait(false);
        await PruneExpiredAsync(connection, transaction, now, cancellationToken).ConfigureAwait(false);
        string source;
        await using (var query = Command(connection, transaction,
                         "SELECT source_id FROM kafdeck_notification_product_source WHERE singleton_id = 1"))
            source = (string)(await query.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("Product notification source metadata is missing."));
        if (!Guid.TryParseExact(source, "D", out _)) throw new InvalidOperationException("Product notification source identity is invalid.");
        var material = $"kafdeck.product.mutation-applied-verified.v1\n{source}\n{operation.OperationId:D}\n{operation.Version.ToString(CultureInfo.InvariantCulture)}";
        var eventId = new Guid(SHA256.HashData(Encoding.UTF8.GetBytes(material)).AsSpan(0, 16));
        await using (var exists = Command(connection, transaction,
                         "SELECT COUNT(*) FROM kafdeck_notification_product_outbox WHERE event_id = @event"))
        {
            Add(exists, "@event", eventId.ToString("D"));
            if (Convert.ToInt64(await exists.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture) != 0) return;
        }
        var pending = await PendingCountAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        if (pending >= policy.MaxPending)
        {
            await IncrementLossAsync(connection, transaction, "overflow_count", 1, now, cancellationToken).ConfigureAwait(false);
            return; // Alert saturation must never undo a real mutation's terminal result.
        }
        await using var insert = Command(connection, transaction, """
            INSERT INTO kafdeck_notification_product_outbox(event_id, operation_id, operation_version,
                kind, occurred_at_utc, captured_at_utc, expires_at_utc)
            VALUES(@event, @operation, @version, 'mutation-applied-verified', @occurred, @captured, @expires)
            ON CONFLICT(operation_id, operation_version) DO NOTHING
            """);
        Add(insert, "@event", eventId.ToString("D"));
        Add(insert, "@operation", operation.OperationId.ToString("D"));
        Add(insert, "@version", operation.Version);
        Add(insert, "@occurred", Format(operation.UpdatedAtUtc));
        Add(insert, "@captured", Format(now));
        Add(insert, "@expires", Format(now + policy.Retention));
        await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<NotificationProductEvent>> ReadPendingAsync(int maxResults,
        CancellationToken cancellationToken = default)
    {
        if (maxResults is < 1 or > NotificationProductEventPolicy.HardMaxBatch)
            throw new ArgumentOutOfRangeException(nameof(maxResults));
        await using var connection = await connections.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await LockSourceAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        var now = await DatabaseUtcAsync(connection, transaction, connections.SupportsSelectForUpdate, cancellationToken).ConfigureAwait(false);
        await PruneExpiredAsync(connection, transaction, now, cancellationToken).ConfigureAwait(false);
        var result = new List<NotificationProductEvent>();
        await using (var command = Command(connection, transaction, """
                         SELECT event_id, occurred_at_utc, kind FROM kafdeck_notification_product_outbox
                         ORDER BY captured_at_utc, event_id LIMIT @limit
                         """))
        {
            Add(command, "@limit", Math.Min(maxResults, _policy.MaxBatch));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (reader.GetString(2) != "mutation-applied-verified")
                    throw new InvalidOperationException("Unapproved product notification kind.");
                result.Add(new(Guid.ParseExact(reader.GetString(0), "D"), Parse(reader.GetString(1))));
            }
        }
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return result.AsReadOnly();
    }

    public async Task<bool> AcknowledgeAsync(Guid eventId, CancellationToken cancellationToken = default)
    {
        if (eventId == Guid.Empty) throw new ArgumentException("Product event identity is required.", nameof(eventId));
        await using var connection = await connections.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await LockSourceAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        await using var command = Command(connection, transaction,
            "DELETE FROM kafdeck_notification_product_outbox WHERE event_id = @event");
        Add(command, "@event", eventId.ToString("D"));
        var removed = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (removed == 1)
            await ExecuteAsync(connection, transaction, """
                UPDATE kafdeck_notification_product_source
                SET routed_count = CASE WHEN routed_count < 9223372036854775807 THEN routed_count + 1 ELSE routed_count END
                WHERE singleton_id = 1
                """, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return removed == 1;
    }

    public async Task<NotificationProductEventSourceStatus> GetStatusAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await connections.OpenAsync(cancellationToken).ConfigureAwait(false);
        var now = await DatabaseUtcAsync(connection, null, connections.SupportsSelectForUpdate, cancellationToken).ConfigureAwait(false);
        // Status is read-only. Pending includes every unacknowledged row;
        // ExpiredPending exposes the subset awaiting the pump's durable cleanup.
        // Never discard an expired row or increment a loss counter from GET.
        await using (var command = Command(connection, null, """
                         SELECT (SELECT COUNT(*) FROM kafdeck_notification_product_outbox),
                             (SELECT COUNT(*) FROM kafdeck_notification_product_outbox WHERE expires_at_utc <= @now),
                             routed_count, expired_count, overflow_count, last_loss_at_utc
                         FROM kafdeck_notification_product_source WHERE singleton_id = 1
                         """))
        {
            Add(command, "@now", Format(now));
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    throw new InvalidOperationException("Product notification status metadata is missing.");
                return new(checked((int)reader.GetInt64(0)), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : Parse(reader.GetString(5)), checked((int)reader.GetInt64(1)));
            }
        }
    }

    private static async Task LockSourceAsync(DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
    {
        if (await ExecuteAsync(connection, transaction,
                "UPDATE kafdeck_notification_product_source SET routed_count = routed_count WHERE singleton_id = 1", cancellationToken)
            .ConfigureAwait(false) != 1)
            throw new InvalidOperationException("Product notification source metadata is missing.");
    }

    private static async Task<int> PendingCountAsync(DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, "SELECT COUNT(*) FROM kafdeck_notification_product_outbox");
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
    }

    private static async Task PruneExpiredAsync(DbConnection connection, DbTransaction transaction, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using var prune = Command(connection, transaction,
            "DELETE FROM kafdeck_notification_product_outbox WHERE expires_at_utc <= @now");
        Add(prune, "@now", Format(now));
        var removed = await prune.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (removed != 0)
            await IncrementLossAsync(connection, transaction, "expired_count", removed, now, cancellationToken).ConfigureAwait(false);
    }

    private static async Task IncrementLossAsync(DbConnection connection, DbTransaction transaction, string column,
        int count, DateTimeOffset now, CancellationToken cancellationToken)
    {
        // Column is a private closed call-site choice, never caller/manifest input.
        if (column is not ("expired_count" or "overflow_count")) throw new ArgumentException("Unknown source loss counter.");
        await using var update = Command(connection, transaction, $"""
            UPDATE kafdeck_notification_product_source
            SET {column} = CASE WHEN {column} <= 9223372036854775807 - @count THEN {column} + @count ELSE 9223372036854775807 END,
                last_loss_at_utc = @now WHERE singleton_id = 1
            """);
        Add(update, "@count", count);
        Add(update, "@now", Format(now));
        await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<DateTimeOffset> DatabaseUtcAsync(DbConnection connection, DbTransaction? transaction,
        bool postgres, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction,
            postgres ? "SELECT clock_timestamp()" : "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')");
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value switch
        {
            DateTimeOffset offset => offset.ToUniversalTime(),
            DateTime date => new DateTimeOffset(DateTime.SpecifyKind(date, DateTimeKind.Utc)),
            string text => Parse(text),
            _ => throw new InvalidOperationException("Product source database clock is unavailable.")
        };
    }

    private static DateTimeOffset Parse(string value) => DateTimeOffset.Parse(value, CultureInfo.InvariantCulture,
        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
    private static string Format(DateTimeOffset value) => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
    private static DbCommand Command(DbConnection connection, DbTransaction? transaction, string sql)
    {
        var command = connection.CreateCommand(); command.Transaction = transaction; command.CommandText = sql; return command;
    }
    private static void Add(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter(); parameter.ParameterName = name; parameter.Value = value; command.Parameters.Add(parameter);
    }
    private static async Task<int> ExecuteAsync(DbConnection connection, DbTransaction transaction, string sql,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, sql);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

// Standalone pump admission. Reserve worst-case finite routing fan-out before
// each source event; this is backpressure, not a claim of cross-host HA admission.
public sealed class NotificationProductEventBackpressure(INotificationDeliveryDbConnectionFactory connections,
    NotificationProductEventPolicy policy) : INotificationProductEventBackpressure
{
    public async Task<bool> CanRouteAsync(int maximumFanout, CancellationToken cancellationToken = default)
    {
        if (maximumFanout is < 1 or > NotificationRoutingCoordinator.HardMaxSubscriptionsPerRoute)
            throw new ArgumentOutOfRangeException(nameof(maximumFanout));
        await using var connection = await connections.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*) FROM (
                SELECT 1 FROM kafdeck_notification_deliveries
                WHERE state IN ('Pending', 'Failed', 'InFlight') LIMIT @limit
            ) bounded_backlog
            """;
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@limit"; parameter.Value = policy.MaxPendingDeliveries;
        command.Parameters.Add(parameter);
        var count = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
        return count <= policy.MaxPendingDeliveries - maximumFanout;
    }
}
