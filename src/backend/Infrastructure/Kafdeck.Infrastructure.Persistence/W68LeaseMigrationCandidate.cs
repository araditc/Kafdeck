using System.Data.Common;
using Npgsql;
namespace Kafdeck.Infrastructure.Persistence;
// Internal provisioning candidate; intentionally NOT called by application startup.
// A governed migration rollout, backup and DB privileges require separate admission.
internal static class W68LeaseMigrationCandidate
{
    internal static async Task InstallPostgreSqlAsync(DbConnection db, DbTransaction tx, CancellationToken ct=default)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(tx);
        if (!ReferenceEquals(tx.Connection,db)) throw new ArgumentException("Transaction mismatch.",nameof(tx));
        if (db is not NpgsqlConnection || tx is not NpgsqlTransaction)
            throw new NotSupportedException("PostgreSQL migration contract only.");
        foreach(var sql in new[] {W68PermanentLeaseEpochHighWaterSchema.PostgreSql,W68LeaseSchemaCandidate.PostgreSql})
        {
            await using var cmd=db.CreateCommand();
            cmd.Transaction=tx;
            cmd.CommandText=sql;
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }
}
