using System.Data;
using System.Data.Common;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Internal transaction-bound candidate, not a registered runtime authority.
// The caller MUST bind holderId to authenticated server principal and roll back
// the entire transaction if an acquisition attempt throws or cannot be admitted.
internal static class W68PostgreSqlLeaseAcquireCandidate
{
    internal const string ClaimSql = """
        INSERT INTO kafdeck_declarative_controller_leases
            (deployment_id, lease_scope_id, holder_id, epoch, expires_at_utc)
        VALUES (@deployment, @scope, @holder, @epoch,
                clock_timestamp() + (@seconds * INTERVAL '1 second'))
        ON CONFLICT (deployment_id, lease_scope_id)
        DO UPDATE SET holder_id = EXCLUDED.holder_id,
                      epoch = EXCLUDED.epoch,
                      expires_at_utc = EXCLUDED.expires_at_utc
        WHERE kafdeck_declarative_controller_leases.expires_at_utc <= clock_timestamp()
        RETURNING epoch
        """;

    internal static async Task<long> AcquireAsync(DbConnection connection, DbTransaction transaction,
        string deployment, string scope, string authenticatedHolderId, int ttlSeconds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (!ReferenceEquals(connection, transaction.Connection))
            throw new ArgumentException("Transaction must belong to connection.", nameof(transaction));
        if (connection is not NpgsqlConnection || transaction is not NpgsqlTransaction ||
            connection.State != ConnectionState.Open)
            throw new NotSupportedException("Live PostgreSQL transaction required.");
        foreach (var id in new[] { deployment, scope, authenticatedHolderId })
            if (string.IsNullOrWhiteSpace(id) || id.Length > 128 || id != id.Trim())
                throw new ArgumentException("Invalid canonical lease identity.");
        if (ttlSeconds is < 1 or > 300) throw new ArgumentOutOfRangeException(nameof(ttlSeconds));

        // Persisted high water serializes competing acquisition contenders.
        // A denied claim throws: caller MUST rollback the high-water advance.
        var epoch = await W68PostgreSqlEpochAdvanceCandidate.AdvanceAsync(
            connection, transaction, deployment, scope, ct).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = ClaimSql;
        foreach (var (key, value) in new (string, object)[] {
            ("@deployment", deployment), ("@scope", scope), ("@holder", authenticatedHolderId),
            ("@epoch", epoch), ("@seconds", ttlSeconds) })
        {
            var p = cmd.CreateParameter(); p.ParameterName = key; p.Value = value;
            cmd.Parameters.Add(p);
        }
        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        if (result is not long stored || stored != epoch)
            throw new InvalidOperationException("Active lease prevents acquisition. Roll back transaction.");
        return epoch;
    }
}
