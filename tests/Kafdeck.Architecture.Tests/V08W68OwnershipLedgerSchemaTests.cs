using System.Reflection;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68OwnershipLedgerSchemaTests
{
    private static IReadOnlyList<string> SqliteSchema()
    {
        var type = typeof(Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory)
            .Assembly.GetType("Kafdeck.Infrastructure.Persistence.W68OwnershipLedgerSchema", throwOnError: true)!;
        return (IReadOnlyList<string>)type.GetProperty("SqliteStatements",
            BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
    }

    [Fact]
    public void Candidate_schema_is_isolated_and_prevents_cross_scope_or_duplicate_claims()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using (var foreignKeys = connection.CreateCommand())
        {
            foreignKeys.CommandText = "PRAGMA foreign_keys=ON";
            foreignKeys.ExecuteNonQuery();
        }
        foreach (var statement in SqliteSchema())
        {
            using var command = connection.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
        const string keyInsert = """
            INSERT INTO kafdeck_declarative_key_versions
              (deployment_id,scope_kind,scope_id,resource_kind,resource_id,revision_high_water)
            VALUES ('production-a','cluster','cluster-one','NotificationSubscription','ops-lag',1)
            """;
        using (var command = connection.CreateCommand())
        {
            command.CommandText = keyInsert;
            Assert.Equal(1, command.ExecuteNonQuery());
            Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());
        }

        // The resource key is scoped by cluster and must be registered before
        // an ownership row can be installed. No API inserts into this schema.
        using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                INSERT INTO kafdeck_declarative_key_versions
                  (deployment_id,scope_kind,scope_id,resource_kind,resource_id,revision_high_water)
                VALUES ('production-a','cluster','cluster-two','NotificationSubscription','ops-lag',1)
                """;
            Assert.Equal(1, command.ExecuteNonQuery());
        }
        using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                INSERT INTO kafdeck_declarative_key_versions
                  (deployment_id,scope_kind,scope_id,resource_kind,resource_id,revision_high_water)
                VALUES ('','cluster','cluster-one','NotificationSubscription','blank-deployment',1)
                """;
            Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());
        }

        // High-water mark cannot be reset to an earlier value by the future
        // CAS allocator without a separate guarded write path.
        using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT count(*) FROM kafdeck_declarative_key_versions
                WHERE deployment_id='production-a' AND resource_id='ops-lag'
                """;
            Assert.Equal(2L, (long)command.ExecuteScalar()!);
        }
    }

    [Fact]
    public void Sqlite_and_postgresql_candidates_keep_the_same_scoped_key_and_source_identity()
    {
        var schemaType = typeof(Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory)
            .Assembly.GetType("Kafdeck.Infrastructure.Persistence.W68OwnershipLedgerSchema", throwOnError: true)!;
        foreach (var property in new[] { "SqliteStatements", "PostgreSqlStatements" })
        {
            var statements = (IReadOnlyList<string>)schemaType.GetProperty(property,
                BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
            Assert.Equal(2, statements.Count);
            var ddl = string.Join("\n", statements);
            foreach (var required in new[]
            {
                "scope_kind", "scope_id", "registered_source_generation_id",
                "pinned_source_revision", "ownership_generation_id",
                "ownership_revision", "stored_lease_epoch", "revision_high_water"
            })
                Assert.Contains(required, ddl, StringComparison.Ordinal);
        }
    }
}
