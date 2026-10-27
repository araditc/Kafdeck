using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68PostgreSqlLeaseRenewCandidateTests
{
    [Fact]
    public async Task Non_postgresql_renewal_is_denied()
    {
        await using var db = new SqliteConnection("Data Source=:memory:");
        await db.OpenAsync();
        await using var tx = await db.BeginTransactionAsync();
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(db, tx, "prod", "writer", "holder", 1, 30));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(db, tx, "prod", "writer", "holder", 0, 30));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(db, tx, "prod", "writer", "holder", 1, 301));
    }

    [Fact]
    public async Task PostgreSql_renewal_requires_current_holder_epoch_scope_and_unexpired_row()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var deployment = "renew-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(cs);
        await db.OpenAsync();
        await using (var ddl = db.CreateCommand())
        {
            ddl.CommandText = W68LeaseSchemaCandidate.PostgreSql;
            await ddl.ExecuteNonQueryAsync();
        }
        await using (var seed = db.CreateCommand())
        {
            seed.CommandText = """
                INSERT INTO kafdeck_declarative_controller_leases
                    (deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
                VALUES (@deployment,'writer','controller-a',7,clock_timestamp()+interval '30 seconds')
                """;
            seed.Parameters.AddWithValue("@deployment", deployment);
            await seed.ExecuteNonQueryAsync();
        }
        try
        {
            await using (var tx = await db.BeginTransactionAsync())
            {
                Assert.Null(await W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(
                    db, tx, deployment, "writer", "controller-b", 7, 60));
                Assert.Null(await W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(
                    db, tx, deployment, "writer", "controller-a", 6, 60));
                Assert.Null(await W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(
                    db, tx, deployment, "other-scope", "controller-a", 7, 60));
                Assert.Null(await W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(
                    db, tx, "other-deployment", "writer", "controller-a", 7, 60));
                var renewed = await W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(
                    db, tx, deployment, "writer", "controller-a", 7, 60);
                Assert.NotNull(renewed);
                Assert.Equal(7L, renewed.Epoch);
                await tx.CommitAsync();
            }
            await using (var expire = db.CreateCommand())
            {
                expire.CommandText = """
                    UPDATE kafdeck_declarative_controller_leases
                    SET expires_at_utc=clock_timestamp()-interval '1 second'
                    WHERE deployment_id=@deployment
                    """;
                expire.Parameters.AddWithValue("@deployment", deployment);
                await expire.ExecuteNonQueryAsync();
            }
            await using (var tx = await db.BeginTransactionAsync())
            {
                Assert.Null(await W68PostgreSqlLeaseRenewCandidate.TryRenewAsync(
                    db, tx, deployment, "writer", "controller-a", 7, 60));
                await tx.RollbackAsync();
            }
        }
        finally
        {
            await using var delete = db.CreateCommand();
            delete.CommandText = "DELETE FROM kafdeck_declarative_controller_leases WHERE deployment_id=@deployment";
            delete.Parameters.AddWithValue("@deployment", deployment);
            await delete.ExecuteNonQueryAsync();
        }
    }
}
