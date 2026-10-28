using System.Data;
using System.Data.Common;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Not registered with runtime. Authentication, audit and admission belong to caller.
internal static class W68PostgreSqlLeaseReleaseCandidate
{
    internal const string Sql = """
        DELETE FROM kafdeck_declarative_controller_leases
         WHERE deployment_id = @deployment AND lease_scope_id = @scope
           AND holder_id = @holder AND epoch = @epoch
           AND expires_at_utc > clock_timestamp()
        RETURNING epoch
        """;

    internal static async Task<bool> TryReleaseAsync(DbConnection connection, DbTransaction transaction,
        string deploymentId, string scopeId, string holderId, long epoch, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        if (!ReferenceEquals(transaction.Connection, connection))
            throw new ArgumentException("Transaction connection mismatch.", nameof(transaction));
        if (connection is not NpgsqlConnection || transaction is not NpgsqlTransaction)
            throw new NotSupportedException("PostgreSQL only.");
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
        foreach (var part in new[] { deploymentId, scopeId, holderId })
            if (string.IsNullOrWhiteSpace(part) || part.Length > 128 || part != part.Trim())
                throw new ArgumentException("Invalid canonical lease identity.");
        if (epoch < 1) throw new ArgumentOutOfRangeException(nameof(epoch));
        // NOWAIT lock first: expiry must not be checked before waiting on a row.
        var locked = await W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
            connection, transaction, deploymentId, scopeId, ct).ConfigureAwait(false);
        if (locked is null || locked.Lease.Epoch != epoch ||
            locked.Lease.ExpiresAtUtc <= locked.DatabaseObservedAtUtc ||
            !string.Equals(locked.Lease.HolderId, holderId, StringComparison.Ordinal))
            return false;
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = Sql;
        foreach (var (key, value) in new (string, object)[] {
            ("@deployment", deploymentId), ("@scope", scopeId), ("@holder", holderId), ("@epoch", epoch) })
        {
            var p = cmd.CreateParameter(); p.ParameterName = key; p.Value = value; cmd.Parameters.Add(p);
        }
        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is long returned && returned == epoch;
    }
}
