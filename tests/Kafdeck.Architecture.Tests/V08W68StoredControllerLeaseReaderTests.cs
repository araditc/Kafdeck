using System.Reflection;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68StoredControllerLeaseReaderTests
{
    [Fact]
    public async Task Exact_scope_read_preserves_epoch_and_expiry_without_claiming_a_lease()
    {
        var assembly = typeof(Kafdeck.Infrastructure.Persistence.SqliteMutationDbConnectionFactory).Assembly;
        var schema = assembly.GetType("Kafdeck.Infrastructure.Persistence.W68LeaseSchemaCandidate", true)!;
        var ddl = (string)schema.GetField("Sqlite", BindingFlags.Static | BindingFlags.NonPublic)!.GetRawConstantValue()!;
        await using var db = new SqliteConnection("Data Source=:memory:");
        await db.OpenAsync();
        await using (var setup = db.CreateCommand())
        {
            setup.CommandText = ddl + """
                ;
                INSERT INTO kafdeck_declarative_controller_leases
                   (deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
                VALUES ('prod','scope-a','holder-a',12,'2026-10-27T12:00:00+00:00')
                """;
            await setup.ExecuteNonQueryAsync();
        }

        var type = assembly.GetType("Kafdeck.Infrastructure.Persistence.W68StoredControllerLeaseReader", true)!;
        var method = type.GetMethod("ReadAsync", BindingFlags.Static | BindingFlags.NonPublic)!;
        await using var tx = await db.BeginTransactionAsync();
        async Task<object?> Read(string deployment, string scope)
        {
            var task = (Task)method.Invoke(null, [db, tx, deployment, scope, CancellationToken.None])!;
            await task;
            return task.GetType().GetProperty("Result")!.GetValue(task);
        }
        var lease = await Read("prod", "scope-a");
        Assert.NotNull(lease);
        Assert.Equal(12L, lease!.GetType().GetProperty("Epoch")!.GetValue(lease));
        Assert.Equal("holder-a", lease.GetType().GetProperty("HolderId")!.GetValue(lease));
        Assert.Equal(new DateTimeOffset(2026, 10, 27, 12, 0, 0, TimeSpan.Zero),
            lease.GetType().GetProperty("ExpiresAtUtc")!.GetValue(lease));
        Assert.Null(await Read("prod", "scope-b"));
        Assert.Null(await Read("other", "scope-a"));
        await tx.CommitAsync();
        // PostgreSQL supplies TIMESTAMPTZ as UTC DateTime, not text.
        // Assert the exact fractional ticks survive the production reader's conversion.
        var pgUtc = new DateTime(2026, 10, 27, 12, 0, 0, DateTimeKind.Utc).AddTicks(1234560);
        var convert = type.GetMethod("ConvertExpiryUtc", BindingFlags.Static | BindingFlags.NonPublic)!;
        var projected = (DateTimeOffset)convert.Invoke(null, [pgUtc])!;
        Assert.Equal(1234560, projected.Ticks % TimeSpan.TicksPerSecond);
    }
}
