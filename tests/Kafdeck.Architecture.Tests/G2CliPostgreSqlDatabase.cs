using Npgsql;

namespace Kafdeck.Architecture.Tests;

// Each real API child gets its own database schema, never a shared operation set.
internal sealed class G2CliPostgreSqlDatabase(NpgsqlConnection admin, string schema, string connectionString) : IAsyncDisposable
{
    internal string ConnectionString { get; } = connectionString;

    internal static async Task<G2CliPostgreSqlDatabase> CreateAsync()
    {
        var configured = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")
            ?? throw new InvalidOperationException("The genuine PostgreSQL CLI fixture is required.");
        var settings = new NpgsqlConnectionStringBuilder(configured) { Pooling = false, IncludeErrorDetail = false };
        var admin = new NpgsqlConnection(settings.ConnectionString);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await admin.OpenAsync(deadline.Token);
            var schema = "g2_cli_" + Guid.NewGuid().ToString("N");
            await using var command = admin.CreateCommand();
            command.CommandText = $"CREATE SCHEMA \"{schema}\"";
            await command.ExecuteNonQueryAsync(deadline.Token);
            settings.SearchPath = schema;
            return new(admin, schema, settings.ConnectionString);
        }
        catch { await admin.DisposeAsync(); throw; }
    }

    public async ValueTask DisposeAsync()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await using var command = admin.CreateCommand();
            command.CommandText = $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
            await command.ExecuteNonQueryAsync(deadline.Token);
        }
        finally { await admin.DisposeAsync(); }
    }
}
