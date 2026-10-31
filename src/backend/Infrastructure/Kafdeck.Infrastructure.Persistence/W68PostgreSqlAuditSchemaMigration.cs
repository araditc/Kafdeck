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
                    WITH expected(name, datatype, max_length) AS (
                      VALUES
                      ('operation_id','character varying',128),
                      ('deployment_id','character varying',128),
                      ('scope_kind','character varying',64),
                      ('scope_id','character varying',128),
                      ('resource_kind','character varying',64),
                      ('resource_id','character varying',128),
                      ('principal_id','character varying',128),
                      ('owner_id','character varying',128),
                      ('source_id','character varying',128),
                      ('lease_epoch','bigint',NULL::integer),
                      ('new_revision','bigint',NULL::integer),
                      ('event_fingerprint','character',64),
                      ('recorded_at_utc','timestamp with time zone',NULL::integer)
                    ), columns_ok AS (
                      SELECT count(*) = 13 AS valid
                      FROM information_schema.columns c JOIN expected e
                        ON c.column_name=e.name
                      WHERE c.table_schema=current_schema()
                        AND c.table_name='kafdeck_declarative_ownership_audit'
                        AND c.data_type=e.datatype
                        AND c.is_nullable='NO'
                        AND (e.max_length IS NULL OR c.character_maximum_length=e.max_length)
                        AND (e.name <> 'recorded_at_utc'
                          OR c.column_default LIKE '%clock_timestamp()%')
                    ), primary_key_ok AS (
                      SELECT EXISTS (
                        SELECT 1 FROM pg_constraint pk
                        JOIN pg_class tbl ON tbl.oid=pk.conrelid
                        JOIN pg_namespace ns ON ns.oid=tbl.relnamespace
                        JOIN pg_attribute att ON att.attrelid=tbl.oid
                          AND att.attnum=pk.conkey[1]
                        WHERE ns.nspname=current_schema()
                          AND tbl.relname='kafdeck_declarative_ownership_audit'
                          AND tbl.relkind IN ('r','p')
                          AND pk.contype='p' AND cardinality(pk.conkey)=1
                          AND att.attname='operation_id'
                      ) AS valid
                    ), positive_checks AS (
                      SELECT count(*)=2 AS valid FROM pg_constraint ck
                      JOIN pg_class tbl ON tbl.oid=ck.conrelid
                      JOIN pg_namespace ns ON ns.oid=tbl.relnamespace
                      WHERE ns.nspname=current_schema()
                        AND tbl.relname='kafdeck_declarative_ownership_audit'
                        AND ck.contype='c'
                        AND (pg_get_constraintdef(ck.oid) LIKE '%lease_epoch > 0%'
                          OR pg_get_constraintdef(ck.oid) LIKE '%new_revision > 0%')
                    )
                    SELECT (SELECT valid FROM columns_ok)
                       AND (SELECT valid FROM primary_key_ok)
                       AND (SELECT valid FROM positive_checks)
                       AND (SELECT count(*)=13 FROM information_schema.columns
                            WHERE table_schema=current_schema()
                              AND table_name='kafdeck_declarative_ownership_audit')
                    """;
                var structurallyValid = await inspect.ExecuteScalarAsync(ct).ConfigureAwait(false);
                if (structurallyValid is not bool valid || !valid)
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
