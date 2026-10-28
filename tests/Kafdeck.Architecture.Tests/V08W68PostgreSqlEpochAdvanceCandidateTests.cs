using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08W68PostgreSqlEpochAdvanceCandidateTests
{
    [Fact]
    public async Task Sqlite_and_mismatched_transaction_fail_closed()
    {
        await using var a = new SqliteConnection("Data Source=:memory:");
        await using var b = new SqliteConnection("Data Source=:memory:");
        await a.OpenAsync(); await b.OpenAsync();
        await using var tx = await a.BeginTransactionAsync();
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            W68PostgreSqlEpochAdvanceCandidate.AdvanceAsync(a, tx, "prod", "writer"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            W68PostgreSqlEpochAdvanceCandidate.AdvanceAsync(b, tx, "prod", "writer"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            W68PostgreSqlEpochAdvanceCandidate.AdvanceAsync(a, tx, "prod", " writer "));
    }

    [Fact]
    public async Task PostgreSql_transaction_is_monotonic_rollback_safe_and_scope_exact()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var deployment = "epoch-advance-" + Guid.NewGuid().ToString("N");
        await using var setup = new NpgsqlConnection(cs);
        await setup.OpenAsync();
        await using (var ddl = setup.CreateCommand())
        {
            ddl.CommandText = W68PermanentLeaseEpochHighWaterSchema.PostgreSql;
            await ddl.ExecuteNonQueryAsync();
        }
        try
        {
            await using var a = new NpgsqlConnection(cs);
            await using var b = new NpgsqlConnection(cs);
            await a.OpenAsync(); await b.OpenAsync();
            await using (var tx = await a.BeginTransactionAsync())
            {
                Assert.Equal(1L, await W68PostgreSqlEpochAdvanceCandidate.AdvanceAsync(
                    a, tx, deployment, "writer"));
                await tx.CommitAsync();
            }
            await using (var tx = await b.BeginTransactionAsync())
            {
                Assert.Equal(2L, await W68PostgreSqlEpochAdvanceCandidate.AdvanceAsync(
                    b, tx, deployment, "writer"));
                await tx.RollbackAsync();
            }
            await using (var tx = await a.BeginTransactionAsync())
            {
                Assert.Equal(2L, await W68PostgreSqlEpochAdvanceCandidate.AdvanceAsync(
                    a, tx, deployment, "writer"));
                Assert.Equal(1L, await W68PostgreSqlEpochAdvanceCandidate.AdvanceAsync(
                    a, tx, deployment, "other"));
                await tx.CommitAsync();
            }
            await using var verify = setup.CreateCommand();
            verify.CommandText = """
                SELECT lease_scope_id, epoch_high_water
                FROM kafdeck_declarative_lease_epoch_high_water
                WHERE deployment_id=@deployment ORDER BY lease_scope_id
                """;
            verify.Parameters.AddWithValue("@deployment", deployment);
            var rows = new Dictionary<string,long>();
            await using (var reader = await verify.ExecuteReaderAsync())
                while (await reader.ReadAsync())
                    rows.Add(reader.GetString(0), reader.GetInt64(1));
            Assert.Equal(2L, rows["writer"]);
            Assert.Equal(1L, rows["other"]);
        }
        finally
        {
            await using var cleanup = setup.CreateCommand();
            cleanup.CommandText = """
                DELETE FROM kafdeck_declarative_lease_epoch_high_water WHERE deployment_id=@deployment
                """;
            cleanup.Parameters.AddWithValue("@deployment", deployment);
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
