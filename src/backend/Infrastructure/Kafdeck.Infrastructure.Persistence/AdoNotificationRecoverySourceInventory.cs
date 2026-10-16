using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Data.Common;
using Kafdeck.Core.Notifications;
using Microsoft.Data.Sqlite;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Explicit server-owned source database. No schema changes or acknowledgment during inventory.</summary>
public sealed class AdoNotificationRecoverySourceInventory : INotificationRecoverySourceInventory
{
    private readonly string _connectionString;
    private readonly bool _postgres;
    private AdoNotificationRecoverySourceInventory(string connectionString, bool postgres)
    { _connectionString = connectionString; _postgres = postgres; }
    public static AdoNotificationRecoverySourceInventory ForSqlite(string absolutePath)
    {
        if (!Path.IsPathFullyQualified(absolutePath)) throw new ArgumentException("Recovery source SQLite path must be absolute.");
        return new(new SqliteConnectionStringBuilder { DataSource = absolutePath, Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Private, Pooling = false }.ConnectionString, false);
    }
    public static AdoNotificationRecoverySourceInventory ForPostgreSql(string connectionString)
    {
        var b = new NpgsqlConnectionStringBuilder(connectionString) { IncludeErrorDetail = false, Pooling = false };
        b.Options = (b.Options ?? "") + " -c default_transaction_read_only=on";
        return new(b.ConnectionString, true);
    }
    public async Task<NotificationRecoverySourceInventory> ReadAsync(CancellationToken token)
    {
        await using DbConnection c = _postgres ? new NpgsqlConnection(_connectionString) : new SqliteConnection(_connectionString);
        await c.OpenAsync(token).ConfigureAwait(false);
        // SQLite's nondeferred Serializable transaction requests a writer lock.
        // Inventory is read-only, including when the database is mounted read-only.
        await using DbTransaction transaction = c is SqliteConnection sqlite
            ? sqlite.BeginTransaction(System.Data.IsolationLevel.Serializable, deferred: true)
            : await c.BeginTransactionAsync(System.Data.IsolationLevel.Serializable, token).ConfigureAwait(false);
        await using var exists = c.CreateCommand(); exists.Transaction = transaction;
        exists.CommandText = _postgres ? "SELECT to_regclass('kafdeck_notification_product_source')::text" :
            "SELECT name FROM sqlite_master WHERE type='table' AND name='kafdeck_notification_product_source'";
        var available = await exists.ExecuteScalarAsync(token).ConfigureAwait(false) is not (null or DBNull);
        string identity;
        List<Guid> ids = [];
        if (available)
        {
            await using var source = c.CreateCommand(); source.Transaction = transaction;
            source.CommandText = "SELECT source_id FROM kafdeck_notification_product_source WHERE singleton_id=1";
            var value = await source.ExecuteScalarAsync(token).ConfigureAwait(false);
            if (value is not string id || !Guid.TryParseExact(id, "D", out var parsed) || parsed == Guid.Empty)
                throw new InvalidOperationException("Notification product source identity is missing or invalid.");
            identity = id;
            await using var rows = c.CreateCommand(); rows.Transaction = transaction;
            rows.CommandText = "SELECT event_id FROM kafdeck_notification_product_outbox ORDER BY event_id LIMIT 10001";
            await using var reader = await rows.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                if (ids.Count == 10000) throw new InvalidOperationException("Notification recovery source inventory exceeds the finite maintenance budget.");
                ids.Add(Guid.ParseExact(reader.GetString(0), "D"));
            }
        }
        else throw new InvalidOperationException("Configured recovery source schema is missing; read-only inventory never creates or enrolls it.");
        var fingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity))).ToLowerInvariant();
        var hash = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new { fingerprint, ids }))).ToLowerInvariant();
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new(fingerprint, hash, ids.AsReadOnly());
    }
}
