using System.Data.Common;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68PostgreSqlLockedLeaseReaderTests
{
    [Fact]
    public void Locked_observation_requires_a_database_clock_and_nonblocking_row_lock()
    {
        var sql = W68PostgreSqlLockedControllerLeaseReader.LockedReadSql;
        Assert.Contains("clock_timestamp()", sql, StringComparison.Ordinal);
        Assert.Contains("FOR UPDATE NOWAIT", sql, StringComparison.Ordinal);
        Assert.Contains("deployment_id = @deployment", sql, StringComparison.Ordinal);
        Assert.Contains("lease_scope_id = @scope", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT ", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DELETE ", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Sqlite_does_not_silently_claim_postgresql_row_lock_authority()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        await Assert.ThrowsAsync<NotSupportedException>(() =>
            W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                connection, transaction, "prod", "controller-a"));
    }

    [Fact]
    public async Task Wrong_transaction_or_invalid_scope_is_rejected_before_any_query()
    {
        await using var first = new SqliteConnection("Data Source=:memory:");
        await using var second = new SqliteConnection("Data Source=:memory:");
        await first.OpenAsync();
        await second.OpenAsync();
        await using var transaction = await first.BeginTransactionAsync();

        await Assert.ThrowsAsync<ArgumentException>(() =>
            W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                second, transaction, "prod", "controller-a"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                first, transaction, " prod ", "controller-a"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                first, transaction, "prod", ""));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                first, transaction, "prod", new string('x', 129)));
    }

    [Fact]
    public void PostgreSql_timestamp_projection_refuses_ambiguous_local_or_unspecified_time()
    {
        var utc = new DateTime(2026, 10, 27, 15, 0, 0, DateTimeKind.Utc).AddTicks(1234560);
        Assert.Equal(utc.Ticks,
            W68PostgreSqlLockedControllerLeaseReader.RequireUtc(utc).UtcTicks);
        Assert.Throws<InvalidOperationException>(() =>
            W68PostgreSqlLockedControllerLeaseReader.RequireUtc(
                DateTime.SpecifyKind(utc, DateTimeKind.Unspecified)));
        Assert.Throws<InvalidOperationException>(() =>
            W68PostgreSqlLockedControllerLeaseReader.RequireUtc(
                new DateTimeOffset(2026, 10, 27, 15, 0, 0, TimeSpan.FromHours(3))));
        Assert.Throws<InvalidOperationException>(() =>
            W68PostgreSqlLockedControllerLeaseReader.RequireUtc("2026-10-27T15:00:00Z"));
    }

    [Fact]
    public async Task Real_postgresql_rejects_a_competing_holder_until_first_transaction_releases_row()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
            return; // Quality Gate has no database; v05-persistence supplies real PostgreSQL.

        var deployment = "w68-lease-" + Guid.NewGuid().ToString("N");
        const string scope = "controller-writer";
        await using var setup = new NpgsqlConnection(connectionString);
        await setup.OpenAsync();
        await using (var ddl = setup.CreateCommand())
        {
            ddl.CommandText = W68LeaseSchemaCandidate.PostgreSql;
            await ddl.ExecuteNonQueryAsync();
        }
        await using (var insert = setup.CreateCommand())
        {
            insert.CommandText = """
                INSERT INTO kafdeck_declarative_controller_leases
                  (deployment_id, lease_scope_id, holder_id, epoch, expires_at_utc)
                VALUES (@deployment, @scope, 'controller-a', 19,
                        clock_timestamp() + interval '2 minutes')
                """;
            insert.Parameters.AddWithValue("@deployment", deployment);
            insert.Parameters.AddWithValue("@scope", scope);
            await insert.ExecuteNonQueryAsync();
        }

        try
        {
            await using var first = new NpgsqlConnection(connectionString);
            await using var second = new NpgsqlConnection(connectionString);
            await first.OpenAsync();
            await second.OpenAsync();

            await using (var firstTransaction = await first.BeginTransactionAsync())
            {
                var observed = await W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                    first, firstTransaction, deployment, scope);
                Assert.NotNull(observed);
                Assert.Equal("controller-a", observed.Lease.HolderId);
                Assert.Equal(19L, observed.Lease.Epoch);
                Assert.True(observed.Lease.ExpiresAtUtc > observed.DatabaseObservedAtUtc);
                Assert.Null(await W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                    first, firstTransaction, deployment, "other-scope"));
                Assert.Null(await W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                    first, firstTransaction, "another-deployment", scope));

                await using (var secondTransaction = await second.BeginTransactionAsync())
                {
                    var blocked = await Assert.ThrowsAsync<PostgresException>(() =>
                        W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                            second, secondTransaction, deployment, scope));
                    Assert.Equal("55P03", blocked.SqlState); // lock_not_available
                    await secondTransaction.RollbackAsync();
                }
                await firstTransaction.CommitAsync();
            }

            await using (var successorTransaction = await second.BeginTransactionAsync())
            {
                var successor = await W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                    second, successorTransaction, deployment, scope);
                Assert.NotNull(successor);
                Assert.Equal(19L, successor.Lease.Epoch);
                await successorTransaction.RollbackAsync();
            }
        }
        finally
        {
            await using var cleanup = setup.CreateCommand();
            cleanup.CommandText = """
                DELETE FROM kafdeck_declarative_controller_leases
                 WHERE deployment_id = @deployment
                """;
            cleanup.Parameters.AddWithValue("@deployment", deployment);
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
