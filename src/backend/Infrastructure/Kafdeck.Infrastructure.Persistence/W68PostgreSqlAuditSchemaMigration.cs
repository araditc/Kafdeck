using System.Data;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// Explicit, admin-invoked migration candidate. This is not called by API startup.
// Serializes concurrent migrators and validates existing schema before acceptance.
internal static class W68PostgreSqlAuditSchemaMigration
{
    internal const long AdvisoryLockKey = 68080041;

    internal static async Task ApplyAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Open PostgreSQL connection required.");
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            await using (var locked = connection.CreateCommand())
            {
                locked.Transaction = transaction;
                locked.CommandText = "SELECT pg_advisory_xact_lock(@key)";
                locked.Parameters.AddWithValue("@key", AdvisoryLockKey);
                await locked.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            await using (var migrate = connection.CreateCommand())
            {
                migrate.Transaction = transaction;
                migrate.CommandText = W68PostgreSqlOwnershipAuditCandidate.Schema;
                await migrate.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            await using (var inspect = connection.CreateCommand())
            {
                inspect.Transaction = transaction;
                inspect.CommandText = """
                    SELECT count(*)::integer FROM information_schema.columns
                     WHERE table_schema = current_schema()
                       AND table_name = 'kafdeck_declarative_ownership_audit'
                       AND column_name IN (
                           'operation_id','deployment_id','scope_kind','scope_id',
                           'resource_kind','resource_id','principal_id','owner_id',
                           'source_id','lease_epoch','new_revision','event_fingerprint',
                           'recorded_at_utc')
                    """;
                var count = await inspect.ExecuteScalarAsync(ct).ConfigureAwait(false);
                if (count is not int columns || columns != 13)
                    throw new InvalidOperationException("Audit schema incompatible; migration not admitted.");
            }
            await transaction.CommitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }
}
