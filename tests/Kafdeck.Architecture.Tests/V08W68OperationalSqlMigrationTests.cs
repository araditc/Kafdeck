using System.Reflection;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08W68OperationalSqlMigrationTests
{
    private static string Migration()
    {
        var assembly = typeof(V08W68OperationalSqlMigrationTests).Assembly;
        var resourceName = assembly.GetManifestResourceNames()
            .Single(name => name.EndsWith("w68-g2-baseline-v1.sql", StringComparison.Ordinal));
        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException("Missing embedded W68 migration.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    private static async Task ApplyAsync(NpgsqlConnection db)
    {
        await using var tx = await db.BeginTransactionAsync();
        try
        {
            await using var cmd = db.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = Migration();
            await cmd.ExecuteNonQueryAsync();
            await tx.CommitAsync();
        }
        catch
        {
            await tx.RollbackAsync();
            throw;
        }
    }

    [Fact]
    public async Task Baseline_is_transactional_replayable_and_rejects_missing_primary_key()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var schema = "w68baseline" + Guid.NewGuid().ToString("N");
        await using var admin = new NpgsqlConnection(cs);
        await admin.OpenAsync();
        await using (var create = admin.CreateCommand())
        {
            create.CommandText = $"CREATE SCHEMA \"{schema}\"";
            await create.ExecuteNonQueryAsync();
        }
        try
        {
            await using var db = new NpgsqlConnection(cs);
            await db.OpenAsync();
            await using (var set = db.CreateCommand())
            {
                set.CommandText = $"SET search_path TO \"{schema}\",pg_catalog";
                await set.ExecuteNonQueryAsync();
            }
            await ApplyAsync(db);
            await ApplyAsync(db);
            await using (var count = db.CreateCommand())
            {
                count.CommandText = """
                    SELECT count(*) FROM information_schema.tables
                     WHERE table_schema=current_schema()
                       AND table_name LIKE 'kafdeck_declarative_%'
                    """;
                Assert.Equal(6L, (long)(await count.ExecuteScalarAsync())!);
            }
            await using (var corrupt = db.CreateCommand())
            {
                corrupt.CommandText = """
                    ALTER TABLE kafdeck_declarative_ownership_audit
                    DROP CONSTRAINT kafdeck_declarative_ownership_audit_pkey
                    """;
                await corrupt.ExecuteNonQueryAsync();
            }
            await Assert.ThrowsAsync<PostgresException>(() => ApplyAsync(db));
            // The original finding also covers non-key audit columns.
            // A correct PK alone must not make a damaged audit writer acceptable.
            await using (var missingColumn = db.CreateCommand())
            {
                missingColumn.CommandText = """
                    ALTER TABLE kafdeck_declarative_ownership_audit
                      ADD PRIMARY KEY (operation_id);
                    ALTER TABLE kafdeck_declarative_ownership_audit
                      DROP COLUMN event_fingerprint;
                    """;
                await missingColumn.ExecuteNonQueryAsync();
            }
            await Assert.ThrowsAsync<PostgresException>(() => ApplyAsync(db));
        }
        finally
        {
            await using var drop = admin.CreateCommand();
            drop.CommandText = $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Partial_legacy_schema_is_rejected_without_installing_ledger()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var schema = "w68partial" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(cs);
        await db.OpenAsync();
        await using (var create = db.CreateCommand())
        {
            create.CommandText = $"CREATE SCHEMA \"{schema}\"";
            await create.ExecuteNonQueryAsync();
        }
        try
        {
            await using (var set = db.CreateCommand())
            {
                set.CommandText = $"SET search_path TO \"{schema}\",pg_catalog";
                await set.ExecuteNonQueryAsync();
            }
            await using (var partial = db.CreateCommand())
            {
                partial.CommandText = "CREATE TABLE kafdeck_declarative_controller_leases (id BIGINT)";
                await partial.ExecuteNonQueryAsync();
            }
            await Assert.ThrowsAsync<PostgresException>(() => ApplyAsync(db));
            await using var read = db.CreateCommand();
            read.CommandText = "SELECT to_regclass('kafdeck_declarative_schema_migrations') IS NULL";
            Assert.True((bool)(await read.ExecuteScalarAsync())!);
        }
        finally
        {
            await using var drop = db.CreateCommand();
            drop.CommandText = $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }
}
