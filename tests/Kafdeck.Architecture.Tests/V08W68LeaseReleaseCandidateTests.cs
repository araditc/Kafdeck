using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68LeaseReleaseCandidateTests
{
    [Fact]
    public async Task NonPostgres_and_wrong_transaction_are_denied()
    {
        await using var first = new SqliteConnection("Data Source=:memory:");
        await using var second = new SqliteConnection("Data Source=:memory:");
        await first.OpenAsync(); await second.OpenAsync();
        await using var tx = await first.BeginTransactionAsync();
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            W68PostgreSqlLeaseReleaseCandidate.TryReleaseAsync(first, tx, "prod","writer","holder",2));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            W68PostgreSqlLeaseReleaseCandidate.TryReleaseAsync(second, tx, "prod","writer","holder",2));
    }
    [Fact]
    public void Sql_never_releases_outside_exact_identity_and_epoch()
    {
        var sql = W68PostgreSqlLeaseReleaseCandidate.Sql;
        Assert.Contains("deployment_id = @deployment",sql);
        Assert.Contains("lease_scope_id = @scope",sql);
        Assert.Contains("holder_id = @holder",sql);
        Assert.Contains("epoch = @epoch",sql);
        Assert.Contains("expires_at_utc > clock_timestamp()",sql);
    }
}
