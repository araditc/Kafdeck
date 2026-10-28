using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68PermanentLeaseEpochHighWaterTests
{
    [Fact]
    public async Task High_water_survives_lease_release_and_is_scoped_by_deployment()
    {
        await using var db = new SqliteConnection("Data Source=:memory:");
        await db.OpenAsync();
        foreach (var ddl in new[] {
            W68LeaseSchemaCandidate.Sqlite,
            W68PermanentLeaseEpochHighWaterSchema.Sqlite
        })
        {
            await using var cmd = db.CreateCommand();
            cmd.CommandText = ddl;
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var seed = db.CreateCommand())
        {
            seed.CommandText = """
                INSERT INTO kafdeck_declarative_lease_epoch_high_water
                    (deployment_id, lease_scope_id, epoch_high_water)
                VALUES ('prod', 'writer', 7), ('other', 'writer', 2);
                INSERT INTO kafdeck_declarative_controller_leases
                    (deployment_id, lease_scope_id, holder_id, epoch, expires_at_utc)
                VALUES ('prod', 'writer', 'controller-a', 7, '2026-10-28T10:00:00Z');
                DELETE FROM kafdeck_declarative_controller_leases
                WHERE deployment_id='prod' AND lease_scope_id='writer';
                """;
            await seed.ExecuteNonQueryAsync();
        }

        static async Task<long> Water(SqliteConnection db, string deployment)
        {
            await using var cmd = db.CreateCommand();
            cmd.CommandText = """
                SELECT epoch_high_water
                  FROM kafdeck_declarative_lease_epoch_high_water
                 WHERE deployment_id=@deployment AND lease_scope_id='writer'
                """;
            cmd.Parameters.AddWithValue("@deployment", deployment);
            return (long)(await cmd.ExecuteScalarAsync())!;
        }

        Assert.Equal(7L, await Water(db, "prod"));
        Assert.Equal(2L, await Water(db, "other"));

        await using var duplicate = db.CreateCommand();
        duplicate.CommandText = """
            INSERT INTO kafdeck_declarative_lease_epoch_high_water
                (deployment_id, lease_scope_id, epoch_high_water)
            VALUES ('prod', 'writer', 0)
            """;
        await Assert.ThrowsAsync<SqliteException>(async () => await duplicate.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task Reject_invalid_epoch_values_in_sqlite_and_require_postgresql_key_parity()
    {
        await using var db = new SqliteConnection("Data Source=:memory:");
        await db.OpenAsync();
        await using (var ddl = db.CreateCommand())
        {
            ddl.CommandText = W68PermanentLeaseEpochHighWaterSchema.Sqlite;
            await ddl.ExecuteNonQueryAsync();
        }
        foreach (object invalid in new object[] {-1, 2.5, "not-an-epoch", 1e30})
        {
            await using var cmd = db.CreateCommand();
            cmd.CommandText = """
                INSERT INTO kafdeck_declarative_lease_epoch_high_water
                    (deployment_id, lease_scope_id, epoch_high_water)
                VALUES ('prod', 'writer', @epoch)
                """;
            cmd.Parameters.AddWithValue("@epoch", invalid);
            await Assert.ThrowsAsync<SqliteException>(async () => await cmd.ExecuteNonQueryAsync());
        }
        Assert.Contains("PRIMARY KEY (deployment_id, lease_scope_id)",
            W68PermanentLeaseEpochHighWaterSchema.PostgreSql, StringComparison.Ordinal);
        Assert.Contains("epoch_high_water BIGINT NOT NULL",
            W68PermanentLeaseEpochHighWaterSchema.PostgreSql, StringComparison.Ordinal);
    }
}
