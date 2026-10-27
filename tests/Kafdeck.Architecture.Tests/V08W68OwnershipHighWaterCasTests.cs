using System.Reflection;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Proof of the candidate DB's transactional high-water primitive, NOT an
// admitted ownership-claim API or a substitute for live lease/actor checks.
public sealed class V08W68OwnershipHighWaterCasTests
{
    [Fact]
    public void Independent_connections_cannot_both_advance_one_exact_resource_revision()
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-w68-cas-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using var first = Open(path);
            using var second = Open(path);
            Initialize(first);
            InsertKey(first, "cluster-a", 0);
            InsertKey(first, "cluster-b", 0);

            // This exact conditional update is the proposed primitive for a
            // future authorized transactional allocator. Only one competitor
            // presenting revision 0 can win, even using separate DB handles.
            Assert.Equal(1, Advance(first, "cluster-a", expected: 0));
            Assert.Equal(0, Advance(second, "cluster-a", expected: 0));
            Assert.Equal(1, Advance(second, "cluster-a", expected: 1));
            Assert.Equal(2L, Revision(first, "cluster-a"));
            Assert.Equal(0L, Revision(first, "cluster-b"));
            Assert.Equal(0, Advance(first, "cluster-b", expected: 1));

            // No new generation may reset the permanent high-water; a stale
            // actor must not treat a failed compare-and-swap as success.
            Assert.Equal(0, Advance(second, "cluster-a", expected: 0));
            Assert.Equal(2L, Revision(second, "cluster-a"));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(Path.GetDirectoryName(path)!, Path.GetFileName(path) + "*"))
                File.Delete(file);
        }
    }

    [Fact]
    public void Sqlite_scoped_revision_cannot_be_advanced_by_a_different_resource_identity()
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-w68-cas-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using var connection = Open(path);
            Initialize(connection);
            InsertKey(connection, "cluster-a", 4);
            Assert.Equal(0, Advance(connection, "cluster-other", expected: 4));
            Assert.Equal(0, Advance(connection, "cluster-a", expected: 3));
            Assert.Equal(1, Advance(connection, "cluster-a", expected: 4));
            Assert.Equal(5L, Revision(connection, "cluster-a"));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(Path.GetDirectoryName(path)!, Path.GetFileName(path) + "*"))
                File.Delete(file);
        }
    }

    private static SqliteConnection Open(string path)
    {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = path, Pooling = false, Mode = SqliteOpenMode.ReadWriteCreate
        }.ToString());
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;";
        command.ExecuteNonQuery();
        return connection;
    }

    private static void Initialize(SqliteConnection connection)
    {
        var assembly = typeof(Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory).Assembly;
        var type = assembly.GetType("Kafdeck.Infrastructure.Persistence.W68OwnershipLedgerSchema", throwOnError: true)!;
        var statements = (IReadOnlyList<string>)type.GetProperty("SqliteStatements",
            BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }

    private static void InsertKey(SqliteConnection connection, string scope, long revision)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO kafdeck_declarative_key_versions
              (deployment_id,scope_kind,scope_id,resource_kind,resource_id,revision_high_water)
            VALUES ('production-a','cluster',@scope,'NotificationSubscription','ops-lag',@revision)
            """;
        cmd.Parameters.AddWithValue("@scope", scope);
        cmd.Parameters.AddWithValue("@revision", revision);
        Assert.Equal(1, cmd.ExecuteNonQuery());
    }

    private static int Advance(SqliteConnection connection, string scope, long expected)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            UPDATE kafdeck_declarative_key_versions
               SET revision_high_water=revision_high_water+1
             WHERE deployment_id='production-a' AND scope_kind='cluster'
               AND scope_id=@scope AND resource_kind='NotificationSubscription'
               AND resource_id='ops-lag' AND revision_high_water=@expected
            """;
        cmd.Parameters.AddWithValue("@scope", scope);
        cmd.Parameters.AddWithValue("@expected", expected);
        return cmd.ExecuteNonQuery();
    }

    private static long Revision(SqliteConnection connection, string scope)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT revision_high_water FROM kafdeck_declarative_key_versions
             WHERE deployment_id='production-a' AND scope_kind='cluster'
               AND scope_id=@scope AND resource_kind='NotificationSubscription'
               AND resource_id='ops-lag'
            """;
        cmd.Parameters.AddWithValue("@scope", scope);
        return (long)cmd.ExecuteScalar()!;
    }
}
