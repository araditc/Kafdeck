using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Candidate test only; not a production Acquire or migration.
[Collection("W68PostgresLeaseSchema")]
public sealed class V08W68PostgreSqlEpochHighWaterTests
{
    [Fact]
    public async Task Concurrent_stale_cas_and_release_preserve_monotonic_epoch_and_scope()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;
        var deployment = "epoch-" + Guid.NewGuid().ToString("N");
        var other = "epoch-" + Guid.NewGuid().ToString("N");
        await using var setup = new NpgsqlConnection(cs);
        await setup.OpenAsync();
        foreach (var ddl in new[] {
            W68LeaseSchemaCandidate.PostgreSql,
            W68PermanentLeaseEpochHighWaterSchema.PostgreSql
        })
        {
            await using var cmd = setup.CreateCommand();
            cmd.CommandText = ddl;
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var seed = setup.CreateCommand())
        {
            seed.CommandText = """
                INSERT INTO kafdeck_declarative_lease_epoch_high_water
                    (deployment_id, lease_scope_id, epoch_high_water)
                VALUES (@d,'writer',1), (@d,'other',1), (@other,'writer',1);
                INSERT INTO kafdeck_declarative_controller_leases
                    (deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
                VALUES (@d,'writer','holder',1,clock_timestamp()+interval '5 minutes');
                """;
            seed.Parameters.AddWithValue("@d",deployment);
            seed.Parameters.AddWithValue("@other",other);
            await seed.ExecuteNonQueryAsync();
        }
        try
        {
            await using var first = new NpgsqlConnection(cs);
            await using var second = new NpgsqlConnection(cs);
            await first.OpenAsync();
            await second.OpenAsync();
            static async Task<int> Advance(NpgsqlConnection db, NpgsqlTransaction tx,
                string d, long expected)
            {
                await using var cmd = db.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = """
                    UPDATE kafdeck_declarative_lease_epoch_high_water
                       SET epoch_high_water=epoch_high_water+1
                     WHERE deployment_id=@d AND lease_scope_id='writer'
                       AND epoch_high_water=@expected
                    """;
                cmd.Parameters.AddWithValue("@d",d);
                cmd.Parameters.AddWithValue("@expected",expected);
                return await cmd.ExecuteNonQueryAsync();
            }
            await using (var tx1 = await first.BeginTransactionAsync())
            {
                Assert.Equal(1,await Advance(first,tx1,deployment,1));
                await using var tx2 = await second.BeginTransactionAsync();
                var contender = Advance(second,tx2,deployment,1);
                // Start competing UPDATE while first transaction still owns row lock.
                await Task.Delay(150);
                Assert.False(contender.IsCompleted);
                await tx1.CommitAsync();
                Assert.Equal(0,await contender);
                await tx2.CommitAsync();
            }
            await using (var release = setup.CreateCommand())
            {
                release.CommandText = """
                    DELETE FROM kafdeck_declarative_controller_leases
                     WHERE deployment_id=@d AND lease_scope_id='writer';
                    """;
                release.Parameters.AddWithValue("@d",deployment);
                Assert.Equal(1,await release.ExecuteNonQueryAsync());
            }
            await using (var tx = await second.BeginTransactionAsync())
            {
                Assert.Equal(0,await Advance(second,tx,deployment,1));
                Assert.Equal(1,await Advance(second,tx,deployment,2));
                await tx.CommitAsync();
            }
            await using var verify = setup.CreateCommand();
            verify.CommandText = """
                SELECT deployment_id, lease_scope_id, epoch_high_water
                  FROM kafdeck_declarative_lease_epoch_high_water
                 WHERE deployment_id IN (@d,@other)
                """;
            verify.Parameters.AddWithValue("@d",deployment);
            verify.Parameters.AddWithValue("@other",other);
            var rows = new Dictionary<string,long>();
            await using (var reader = await verify.ExecuteReaderAsync())
                while(await reader.ReadAsync())
                    rows[reader.GetString(0)+"/"+reader.GetString(1)] = reader.GetInt64(2);
            Assert.Equal(3L,rows[deployment+"/writer"]);
            Assert.Equal(1L,rows[deployment+"/other"]);
            Assert.Equal(1L,rows[other+"/writer"]);
        }
        finally
        {
            await using var cleanup = setup.CreateCommand();
            cleanup.CommandText = """
                DELETE FROM kafdeck_declarative_controller_leases WHERE deployment_id=@d;
                DELETE FROM kafdeck_declarative_lease_epoch_high_water
                 WHERE deployment_id IN (@d,@other);
                """;
            cleanup.Parameters.AddWithValue("@d",deployment);
            cleanup.Parameters.AddWithValue("@other",other);
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
