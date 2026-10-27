using System.Reflection;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68StoredOwnershipReaderTests
{
    [Fact]
    public async Task Exact_scope_read_distinguishes_stored_managed_and_absent_without_adopting()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var assembly = typeof(Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory).Assembly;
        var schema = assembly.GetType("Kafdeck.Infrastructure.Persistence.W68OwnershipLedgerSchema", true)!;
        var statements = (IReadOnlyList<string>)schema.GetProperty("SqliteStatements",
            BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
        foreach (var sql in statements)
        {
            await using var create = connection.CreateCommand();
            create.CommandText = sql;
            await create.ExecuteNonQueryAsync();
        }
        await using (var seed = connection.CreateCommand())
        {
            seed.CommandText = """
                INSERT INTO kafdeck_declarative_key_versions
                  (deployment_id,scope_kind,scope_id,resource_kind,resource_id,revision_high_water)
                VALUES ('prod','cluster','one','NotificationSubscription','sub',4);
                INSERT INTO kafdeck_declarative_ownership
                  (deployment_id,scope_kind,scope_id,resource_kind,resource_id,
                   registered_owner_id,registered_source_id,registered_source_generation_id,
                   pinned_source_revision,ownership_generation_id,ownership_revision,
                   stored_lease_epoch,ownership_state,evidence_fingerprint,
                   claimant_principal_id,policy_version,audit_operation_id,updated_at_utc)
                VALUES ('prod','cluster','one','NotificationSubscription','sub',
                        'owner-a','source-a','source-generation-a',
                        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                        'owner-generation-a',4,7,'Managed',
                        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                        'principal-a','policy-v1','audit-1','2026-10-27T00:00:00Z')
                """;
            await seed.ExecuteNonQueryAsync();
        }

        var type = assembly.GetType("Kafdeck.Infrastructure.Persistence.W68StoredOwnershipReader", true)!;
        var method = type.GetMethod("ReadExactAsync", BindingFlags.Static | BindingFlags.NonPublic)!;
        await using var transaction = await connection.BeginTransactionAsync();
        async Task<object?> Read(string deployment, string scope, string resource)
        {
            var task = (Task)method.Invoke(null, [connection, transaction, deployment, "cluster", scope,
                "NotificationSubscription", resource, CancellationToken.None])!;
            await task;
            return task.GetType().GetProperty("Result")!.GetValue(task);
        }

        var stored = await Read("prod", "one", "sub");
        Assert.NotNull(stored);
        var properties = stored!.GetType();
        Assert.Equal("owner-a", properties.GetProperty("RegisteredOwnerId")!.GetValue(stored));
        Assert.Equal("source-a", properties.GetProperty("RegisteredSourceId")!.GetValue(stored));
        Assert.Equal(4L, properties.GetProperty("OwnershipRevision")!.GetValue(stored));
        Assert.Equal(7L, properties.GetProperty("StoredLeaseEpoch")!.GetValue(stored));
        Assert.Null(await Read("prod", "two", "sub"));
        Assert.Null(await Read("prod", "one", "another"));
        Assert.Null(await Read("another-deployment", "one", "sub"));
        await transaction.CommitAsync();
    }
}
