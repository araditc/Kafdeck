using System.Data;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

// First-time bootstrap ONLY. Existing installations must use independently
// admitted versioned migrations. No startup or API registration invokes this.
internal static class W68PostgreSqlFreshSchemaBootstrap
{
    internal static async Task ApplyAsync(NpgsqlConnection connection, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Open PostgreSQL connection required.");
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            await using (var lockCommand = connection.CreateCommand())
            {
                lockCommand.Transaction = transaction;
                lockCommand.CommandText = "SELECT pg_advisory_xact_lock(68080041)";
                await lockCommand.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            // Reject partial/preexisting schemas rather than silently accepting
            // CREATE IF NOT EXISTS with unknown constraints or data.
            await using (var probe = connection.CreateCommand())
            {
                probe.Transaction = transaction;
                probe.CommandText = """
                    SELECT count(*)::integer FROM pg_class c
                    JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname=current_schema()
                      AND c.relname = ANY(@names)
                    """;
                probe.Parameters.AddWithValue("@names", new [] {
                    "kafdeck_declarative_key_versions",
                    "kafdeck_declarative_ownership",
                    "kafdeck_declarative_controller_leases",
                    "kafdeck_declarative_lease_epoch_high_water",
                    "kafdeck_declarative_ownership_audit"
                });
                var existing = await probe.ExecuteScalarAsync(ct).ConfigureAwait(false);
                if (existing is not int count || count != 0)
                    throw new InvalidOperationException(
                        "Existing declarative schema requires governed versioned migration.");
            }
            foreach (var ddl in W68OwnershipLedgerSchema.PostgreSqlStatements.Concat(new[] {
                         W68LeaseSchemaCandidate.PostgreSql,
                         W68PermanentLeaseEpochHighWaterSchema.PostgreSql,
                         W68PostgreSqlOwnershipAuditCandidate.Schema }))
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = ddl;
                await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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
