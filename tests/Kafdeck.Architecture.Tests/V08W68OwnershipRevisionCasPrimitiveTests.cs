using System.Reflection;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68OwnershipRevisionCasPrimitiveTests
{
    [Fact]
    public async Task Scoped_transactional_compare_and_swap_rejects_stale_revision()
    {
        var path = Path.Combine(Path.GetTempPath(), $"w68-primitive-{Guid.NewGuid():N}.db");
        try
        {
            await using var first = new SqliteConnection($"Data Source={path};Pooling=False");
            await first.OpenAsync();
            var assembly = typeof(Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory).Assembly;
            var schema = assembly.GetType("Kafdeck.Infrastructure.Persistence.W68OwnershipLedgerSchema", true)!;
            var statements = (IReadOnlyList<string>)schema.GetProperty("SqliteStatements",
                BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
            foreach (var statement in statements)
            {
                await using var create = first.CreateCommand();
                create.CommandText = statement;
                await create.ExecuteNonQueryAsync();
            }
            await using (var insert = first.CreateCommand())
            {
                insert.CommandText = """
                    INSERT INTO kafdeck_declarative_key_versions
                    (deployment_id,scope_kind,scope_id,resource_kind,resource_id,revision_high_water)
                    VALUES ('prod','cluster','one','NotificationSubscription','sub',0)
                    """;
                await insert.ExecuteNonQueryAsync();
            }

            var primitive = assembly.GetType("Kafdeck.Infrastructure.Persistence.W68OwnershipRevisionCas", true)!;
            var advance = primitive.GetMethod("TryAdvanceAsync", BindingFlags.Static | BindingFlags.NonPublic)!;
            async Task<bool> Execute(SqliteConnection connection, System.Data.Common.DbTransaction transaction,
                string scope, long revision) =>
                await (Task<bool>)advance.Invoke(null,
                [connection, transaction, "prod", "cluster", scope, "NotificationSubscription",
                    "sub", revision, CancellationToken.None])!;

            await using (var tx = await first.BeginTransactionAsync())
            {
                Assert.True(await Execute(first, tx, "one", 0));
                await tx.CommitAsync();
            }

            await using var second = new SqliteConnection($"Data Source={path};Pooling=False");
            await second.OpenAsync();
            await using (var tx = await second.BeginTransactionAsync())
            {
                Assert.False(await Execute(second, tx, "one", 0));
                Assert.False(await Execute(second, tx, "other", 1));
                Assert.True(await Execute(second, tx, "one", 1));
                await tx.CommitAsync();
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(Path.GetDirectoryName(path)!, Path.GetFileName(path) + "*"))
                File.Delete(file);
        }
    }
}
