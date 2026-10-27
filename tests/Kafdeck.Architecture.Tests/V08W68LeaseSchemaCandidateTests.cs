using System.Reflection;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68LeaseSchemaCandidateTests
{
    [Fact]
    public void Distinct_scopes_are_independent_and_same_scope_lease_is_unique()
    {
        var assembly = typeof(Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory).Assembly;
        var schema = assembly.GetType("Kafdeck.Infrastructure.Persistence.W68LeaseSchemaCandidate", true)!;
        var sqlite = (string)schema.GetField("Sqlite", BindingFlags.Static | BindingFlags.NonPublic)!.GetRawConstantValue()!;
        var postgres = (string)schema.GetField("PostgreSql", BindingFlags.Static | BindingFlags.NonPublic)!.GetRawConstantValue()!;
        Assert.Contains("expires_at_utc TIMESTAMPTZ NOT NULL", postgres, StringComparison.Ordinal);
        Assert.Contains("PRIMARY KEY (deployment_id, lease_scope_id)", postgres, StringComparison.Ordinal);
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using (var command = connection.CreateCommand())
        {
            command.CommandText = sqlite;
            command.ExecuteNonQuery();
        }
        static int Insert(SqliteConnection connection, string scope, string holder, object epoch)
        {
            using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO kafdeck_declarative_controller_leases
                  (deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
                VALUES ('prod',@scope,@holder,@epoch,'2026-10-27T09:00:00Z')
                """;
            command.Parameters.AddWithValue("@scope", scope);
            command.Parameters.AddWithValue("@holder", holder);
            command.Parameters.AddWithValue("@epoch", epoch);
            return command.ExecuteNonQuery();
        }

        Assert.Equal(1, Insert(connection, "controller-a", "holder-a", 1));
        Assert.Throws<SqliteException>(() => Insert(connection, "controller-a", "holder-b", 2));
        Assert.Equal(1, Insert(connection, "controller-b", "holder-b", 1));
        Assert.Throws<SqliteException>(() => Insert(connection, "controller-c", "", 1));
        Assert.Throws<SqliteException>(() => Insert(connection, "controller-c", "holder-c", 0));
        Assert.Throws<SqliteException>(() => Insert(connection, "controller-c", "holder-c", 1.5));
        Assert.Throws<SqliteException>(() => Insert(connection, "controller-c", "holder-c", "not-an-epoch"));
        Assert.Throws<SqliteException>(() => Insert(connection, "controller-c", "holder-c", 1e30));
    }
}
