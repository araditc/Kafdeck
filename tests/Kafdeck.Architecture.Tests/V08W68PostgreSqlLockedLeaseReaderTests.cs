using System.Data.Common;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
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
}
