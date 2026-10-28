using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Real PostgreSQL proof only; no production acquisition or migration is enabled.
public sealed class V08W68PostgreSqlEpochHighWaterTests
{
    [Fact]
    public async Task Independent_connections_cannot_reuse_epoch_after_release()
    {
        var cs = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(cs)) return;

        var deployment = "epoch-proof-" + Guid.NewGuid().ToString("N");
        await using var setup = new NpgsqlConnection(cs);
        await setup.OpenAsync();
        await using (var ddl = setup.CreateCommand())
        {
            ddl.CommandText = W68PermanentLeaseEpochHighWaterSchema.PostgreSql;
            await ddl.ExecuteNonQueryAsync();
        }
        await using (var seed = setup.CreateCommand())
        {
            seed.CommandText = """
                INSERT INTO kafdeck_declarative_lease_epoch_high_water
                    (deployment_id, lease_scope_id, epoch_high_water)
                VALUES (@deployment, 'writer', 1), (@deployment, 'other', 8)
                """;
            seed.Parameters.AddWithValue("@deployment", deployment);
            await seed.ExecuteNonQueryAsync();
        }
        try
        {
            await using var first = new NpgsqlConnection(cs);
            await using var second = new NpgsqlConnection(cs);
            await first.OpenAsync();
            await second.OpenAsync();

            static async Task<int> Advance(NpgsqlConnection db, NpgsqlTransaction tx,
                string deployment, long expected)
            {
                await using var cmd = db.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = """
                    UPDATE kafdeck_declarative_lease_epoch_high_water
                       SET epoch_high_water = epoch_high_water + 1
                     WHERE deployment_id = @deployment AND lease_scope_id = 'writer'
                       AND epoch_high_water = @expected
                    """;
                cmd.Parameters.AddWithValue("@deployment", deployment);
                cmd.Parameters.AddWithValue("@expected", expected);
                return await cmd.ExecuteNonQueryAsync();
            }

            await using (var tx1 = await first.BeginTransactionAsync())
            {
                Assert.Equal(1, await Advance(first, tx1, deployment, 1));
                await tx1.CommitAsync();
            }
            await using (var tx2 = await second.BeginTransactionAsync())
            {
                Assert.Equal(0, await Advance(second, tx2, deployment, 1));
                Assert.Equal(1, await Advance(second, tx2, deployment, 2));
                await tx2.CommitAsync();
            }

            await using var verify = setup.CreateCommand();
            verify.CommandText = """
                SELECT lease_scope_id, epoch_high_water
                FROM kafdeck_declarative_lease_epoch_high_water
                WHERE deployment_id = @deployment ORDER BY lease_scope_id
                """;
            verify.Parameters.AddWithValue("@deployment", deployment);
            await using var reader = await verify.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal("other", reader.GetString(0));
            Assert.Equal(8L, reader.GetInt64(1));
            Assert.True(await reader.ReadAsync());
            Assert.Equal("writer", reader.GetString(0));
            Assert.Equal(3L, reader.GetInt64(1));
            Assert.False(await reader.ReadAsync());
        }
        finally
        {
            await using var cleanup = setup.CreateCommand();
            cleanup.CommandText =
                "DELETE FROM kafdeck_declarative_lease_epoch_high_water WHERE deployment_id=@deployment";
            cleanup.Parameters.AddWithValue("@deployment", deployment);
            await cleanup.ExecuteNonQueryAsync();
        }
    }
}
