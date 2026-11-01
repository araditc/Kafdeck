using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08W68FreshSchemaBootstrapTests
{
    [Fact]
    public async Task Fresh_schema_is_atomic_and_rejects_reentry()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var schema = "w68boot" + Guid.NewGuid().ToString("N");
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
            await using (var select = db.CreateCommand())
            {
                select.CommandText = $"SET search_path TO \"{schema}\"";
                await select.ExecuteNonQueryAsync();
            }
            await W68PostgreSqlFreshSchemaBootstrap.ApplyAsync(db);
            await using (var count = db.CreateCommand())
            {
                count.CommandText = """
                    SELECT count(*) FROM information_schema.tables
                    WHERE table_schema=current_schema()
                    AND table_name LIKE 'kafdeck_declarative_%'
                    """;
                Assert.Equal(5L,(long)(await count.ExecuteScalarAsync())!);
            }
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => W68PostgreSqlFreshSchemaBootstrap.ApplyAsync(db));
        }
        finally
        {
            await using var cleanup = admin.CreateCommand();
            cleanup.CommandText = $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
