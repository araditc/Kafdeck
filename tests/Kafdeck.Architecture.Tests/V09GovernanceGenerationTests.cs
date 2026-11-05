using Kafdeck.Infrastructure.Persistence;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09GovernanceGenerationTests
{
    [Fact]
    public async Task Restored_generation_fences_live_old_workers_and_config_disable()
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-v09-generation-" + Guid.NewGuid().ToString("N") + ".db");
        try { await ProveAsync(new SqliteMutationDbConnectionFactory(path)); }
        finally { Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools(); if (File.Exists(path)) File.Delete(path); }
    }

    [G2PostgreSqlFact]
    public async Task PostgreSql_restored_generation_fences_live_old_workers_and_config_disable()
    {
        var builder = new Npgsql.NpgsqlConnectionStringBuilder(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES"));
        var schema = "v09gen_" + Guid.NewGuid().ToString("N");
        await using var admin = new Npgsql.NpgsqlConnection(builder.ConnectionString);
        await admin.OpenAsync();
        await using var create = admin.CreateCommand(); create.CommandText = $"CREATE SCHEMA {schema}"; await create.ExecuteNonQueryAsync();
        try { builder.SearchPath = schema; await ProveAsync(new PostgreSqlMutationDbConnectionFactory(builder.ConnectionString)); }
        finally { await using var drop = admin.CreateCommand(); drop.CommandText = $"DROP SCHEMA {schema} CASCADE"; await drop.ExecuteNonQueryAsync(); }
    }

    private static async Task ProveAsync(IMutationDbConnectionFactory factory)
    {
        var previous = Guid.NewGuid(); var next = Guid.NewGuid();
        var oldWorker = new AdoGovernanceGenerationGate(factory, previous);
        await oldWorker.InitializeAsync();
        var disabled = new AdoGovernanceGenerationGate(factory, null);
        await Assert.ThrowsAsync<GovernanceGenerationException>(() => disabled.InitializeAsync());
        var replacement = new AdoGovernanceGenerationGate(factory, next);
        await Assert.ThrowsAsync<GovernanceGenerationException>(() => replacement.InitializeAsync());
        await Assert.ThrowsAsync<ArgumentException>(() => replacement.AdmitRestoredGenerationAsync(previous, next, "bad", new string('b', 64)));
        await replacement.AdmitRestoredGenerationAsync(previous, next, new string('a', 64), new string('b', 64));
        await replacement.InitializeAsync();
        await using var connection = await factory.OpenAsync(); await using var transaction = await connection.BeginTransactionAsync();
        await Assert.ThrowsAsync<GovernanceGenerationException>(() => oldWorker.ValidateAsync(connection, transaction, CancellationToken.None));
        await replacement.ValidateAsync(connection, transaction, CancellationToken.None);
        await transaction.RollbackAsync();
        await Assert.ThrowsAsync<GovernanceGenerationException>(() => replacement.AdmitRestoredGenerationAsync(previous, next, new string('a', 64), new string('b', 64)));
    }
}
