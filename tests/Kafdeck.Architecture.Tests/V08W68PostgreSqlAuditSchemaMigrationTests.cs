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
}
