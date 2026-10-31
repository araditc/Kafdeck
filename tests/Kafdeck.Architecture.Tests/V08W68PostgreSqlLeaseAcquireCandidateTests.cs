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
    [Fact]
    public async Task Replacement_expiry_is_calculated_after_competing_row_lock_releases()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var deployment = "expiry-" + Guid.NewGuid().ToString("N");
        await using var setup = new NpgsqlConnection(cs);
        await setup.OpenAsync();
        foreach (var ddl in new[] { W68PermanentLeaseEpochHighWaterSchema.PostgreSql,
                                    W68LeaseSchemaCandidate.PostgreSql })
        {
            await using var cmd = setup.CreateCommand();
            cmd.CommandText = ddl;
            await cmd.ExecuteNonQueryAsync();
        }
        try
        {
            await using (var seed = setup.CreateCommand())
            {
                seed.CommandText = """
                    INSERT INTO kafdeck_declarative_lease_epoch_high_water
                        (deployment_id,lease_scope_id,epoch_high_water)
                    VALUES (@d,'writer',1);
                    INSERT INTO kafdeck_declarative_controller_leases
                        (deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
                    VALUES (@d,'writer','old',1,clock_timestamp()-interval '1 second');
                    """;
                seed.Parameters.AddWithValue("@d",deployment);
                await seed.ExecuteNonQueryAsync();
            }
            await using var first = new NpgsqlConnection(cs);
            await using var second = new NpgsqlConnection(cs);
            await first.OpenAsync();
            await second.OpenAsync();
            await using var holder = await first.BeginTransactionAsync();
            Assert.NotNull(await W68PostgreSqlLockedControllerLeaseReader.ReadForUpdateAsync(
                first,holder,deployment,"writer"));
            await using var contenderTx = await second.BeginTransactionAsync();
            var contender = W68PostgreSqlLeaseAcquireCandidate.AcquireAsync(
                second,contenderTx,deployment,"writer","new",1);
            await Task.Delay(TimeSpan.FromMilliseconds(1400));
            Assert.False(contender.IsCompleted);
            await holder.CommitAsync();
            Assert.Equal(2L,await contender);
            await contenderTx.CommitAsync();
            await using var check = setup.CreateCommand();
            check.CommandText = """
                SELECT expires_at_utc > clock_timestamp()
                FROM kafdeck_declarative_controller_leases
                WHERE deployment_id=@d AND lease_scope_id='writer'
                """;
            check.Parameters.AddWithValue("@d",deployment);
            Assert.True((bool)(await check.ExecuteScalarAsync())!);
        }
        finally
        {
            await using var cleanup = setup.CreateCommand();
            cleanup.CommandText = """
                DELETE FROM kafdeck_declarative_controller_leases WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_lease_epoch_high_water WHERE deployment_id=@d;
                """;
            cleanup.Parameters.AddWithValue("@d",deployment);
            await cleanup.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Uncommitted_delete_cannot_install_expired_lease_after_wait()
    {
        var cs=Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if(string.IsNullOrWhiteSpace(cs)) return;
        var deployment="delete-race-"+Guid.NewGuid().ToString("N");
        await using var setup=new NpgsqlConnection(cs);
        await setup.OpenAsync();
        foreach(var ddl in new[]{W68PermanentLeaseEpochHighWaterSchema.PostgreSql,W68LeaseSchemaCandidate.PostgreSql})
        {
            await using var cmd=setup.CreateCommand();
            cmd.CommandText=ddl;
            await cmd.ExecuteNonQueryAsync();
        }
        try
        {
            await using(var seed=setup.CreateCommand())
            {
                seed.CommandText="""
                    INSERT INTO kafdeck_declarative_lease_epoch_high_water
                    (deployment_id,lease_scope_id,epoch_high_water) VALUES (@d,'writer',1);
                    INSERT INTO kafdeck_declarative_controller_leases
                    (deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
                    VALUES (@d,'writer','old',1,clock_timestamp()+interval '5 minutes');
                    """;
                seed.Parameters.AddWithValue("@d",deployment);
                await seed.ExecuteNonQueryAsync();
            }
            await using var first=new NpgsqlConnection(cs);
            await using var second=new NpgsqlConnection(cs);
            await first.OpenAsync();
            await second.OpenAsync();
            await using var deletion=await first.BeginTransactionAsync();
            await using(var cmd=first.CreateCommand())
            {
                cmd.Transaction=deletion;
                cmd.CommandText="DELETE FROM kafdeck_declarative_controller_leases WHERE deployment_id=@d";
                cmd.Parameters.AddWithValue("@d",deployment);
                Assert.Equal(1,await cmd.ExecuteNonQueryAsync());
            }
            await using var attempt=await second.BeginTransactionAsync();
            var claim=W68PostgreSqlLeaseAcquireCandidate.AcquireAsync(
                second,attempt,deployment,"writer","new",1);
            await Task.Delay(TimeSpan.FromMilliseconds(1400));
            Assert.False(claim.IsCompleted);
            await deletion.CommitAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(async()=>await claim);
            await attempt.RollbackAsync();
            await using var verify=setup.CreateCommand();
            verify.CommandText="""
                SELECT count(*) FROM kafdeck_declarative_controller_leases
                WHERE deployment_id=@d
                """;
            verify.Parameters.AddWithValue("@d",deployment);
            Assert.Equal(0L,(long)(await verify.ExecuteScalarAsync())!);
        }
        finally
        {
            await using var cleanup=setup.CreateCommand();
            cleanup.CommandText="""
                DELETE FROM kafdeck_declarative_controller_leases WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_lease_epoch_high_water WHERE deployment_id=@d;
                """;
            cleanup.Parameters.AddWithValue("@d",deployment);
            await cleanup.ExecuteNonQueryAsync();
        }
    }

}
