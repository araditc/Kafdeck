using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08W68PostgreSqlAuditSchemaMigrationTests
{
    [Fact]
    public async Task Migration_is_idempotent_and_audit_table_is_writeable()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        await using var first = new NpgsqlConnection(cs);
        await using var second = new NpgsqlConnection(cs);
        await first.OpenAsync();
        await second.OpenAsync();
        await Task.WhenAll(
            W68PostgreSqlAuditSchemaMigration.ApplyAsync(first),
            W68PostgreSqlAuditSchemaMigration.ApplyAsync(second));
        // Ensure a second ordinary invocation is also accepted.
        await W68PostgreSqlAuditSchemaMigration.ApplyAsync(first);
        var operation = "migration-" + Guid.NewGuid().ToString("N");
        await using (var transaction = await first.BeginTransactionAsync())
        {
            await W68PostgreSqlOwnershipAuditCandidate.RecordAsync(
                first,transaction,operation,"deployment","Tenant","scope",
                "NotificationSubscription","sub","principal","owner","source",
                1,1,new string('a',64));
            await transaction.RollbackAsync();
        }
        await using var read = first.CreateCommand();
        read.CommandText = """
            SELECT count(*) FROM kafdeck_declarative_ownership_audit
            WHERE operation_id=@operation
            """;
        read.Parameters.AddWithValue("@operation",operation);
        Assert.Equal(0L,(long)(await read.ExecuteScalarAsync())!);
    }
    [Fact]
    public async Task Existing_table_without_primary_key_is_not_accepted()
    {
        var cs=Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if(string.IsNullOrWhiteSpace(cs))return;
        var schema="w68audit"+Guid.NewGuid().ToString("N");
        await using var db=new NpgsqlConnection(cs);
        await db.OpenAsync();
        await using(var create=db.CreateCommand())
        {
            create.CommandText=$"CREATE SCHEMA \\"{schema}\\"";
            await create.ExecuteNonQueryAsync();
        }
        try
        {
            await using(var set=db.CreateCommand())
            {
                set.CommandText=$"SET search_path TO \\"{schema}\\"";
                await set.ExecuteNonQueryAsync();
            }
            await using(var broken=db.CreateCommand())
            {
                broken.CommandText="""
                    CREATE TABLE kafdeck_declarative_ownership_audit (
                      operation_id VARCHAR(128) NOT NULL,
                      deployment_id VARCHAR(128) NOT NULL,
                      scope_kind VARCHAR(64) NOT NULL,
                      scope_id VARCHAR(128) NOT NULL,
                      resource_kind VARCHAR(64) NOT NULL,
                      resource_id VARCHAR(128) NOT NULL,
                      principal_id VARCHAR(128) NOT NULL,
                      owner_id VARCHAR(128) NOT NULL,
                      source_id VARCHAR(128) NOT NULL,
                      lease_epoch BIGINT NOT NULL CHECK (lease_epoch>0),
                      new_revision BIGINT NOT NULL CHECK (new_revision>0),
                      event_fingerprint CHAR(64) NOT NULL,
                      recorded_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    )
                    """;
                await broken.ExecuteNonQueryAsync();
            }
            await Assert.ThrowsAsync<InvalidOperationException>(
                ()=>W68PostgreSqlAuditSchemaMigration.ApplyAsync(db));
        }
        finally
        {
            await using var cleanup=db.CreateCommand();
            cleanup.CommandText=$"DROP SCHEMA IF EXISTS \\"{schema}\\" CASCADE";
            await cleanup.ExecuteNonQueryAsync();
        }
    }

}
