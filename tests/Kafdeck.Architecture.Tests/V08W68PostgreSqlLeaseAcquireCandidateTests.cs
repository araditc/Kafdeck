using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08W68PostgreSqlLeaseAcquireCandidateTests
{
    [Fact]
    public async Task Current_holder_blocks_second_claim_and_epoch_survives_release()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var deployment = "claim-" + Guid.NewGuid().ToString("N");
        await using var db = new NpgsqlConnection(cs);
        await db.OpenAsync();
        foreach (var ddl in new[] {
            W68PermanentLeaseEpochHighWaterSchema.PostgreSql,
            W68LeaseSchemaCandidate.PostgreSql })
        {
            await using var cmd=db.CreateCommand();
            cmd.CommandText=ddl;
            await cmd.ExecuteNonQueryAsync();
        }
        try
        {
            await using (var tx=await db.BeginTransactionAsync())
            {
                Assert.Equal(1L,await W68PostgreSqlLeaseAcquireCandidate.AcquireAsync(
                    db,tx,deployment,"writer","controller-a",120));
                await tx.CommitAsync();
            }
            await using (var tx=await db.BeginTransactionAsync())
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() =>
                    W68PostgreSqlLeaseAcquireCandidate.AcquireAsync(
                        db,tx,deployment,"writer","controller-b",120));
                await tx.RollbackAsync();
            }
            await using (var tx=await db.BeginTransactionAsync())
            {
                Assert.True(await W68PostgreSqlLeaseReleaseCandidate.TryReleaseAsync(
                    db,tx,deployment,"writer","controller-a",1));
                await tx.CommitAsync();
            }
            await using (var tx=await db.BeginTransactionAsync())
            {
                Assert.Equal(2L,await W68PostgreSqlLeaseAcquireCandidate.AcquireAsync(
                    db,tx,deployment,"writer","controller-b",120));
                await tx.CommitAsync();
            }
            await using var check=db.CreateCommand();
            check.CommandText="""
                SELECT epoch_high_water FROM kafdeck_declarative_lease_epoch_high_water
                 WHERE deployment_id=@deployment AND lease_scope_id='writer'
                """;
            check.Parameters.AddWithValue("@deployment",deployment);
            Assert.Equal(2L,(long)(await check.ExecuteScalarAsync())!);
        }
        finally
        {
            await using var cleanup=db.CreateCommand();
            cleanup.CommandText="""
                DELETE FROM kafdeck_declarative_controller_leases WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_lease_epoch_high_water WHERE deployment_id=@d;
                """;
            cleanup.Parameters.AddWithValue("@d",deployment);
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
