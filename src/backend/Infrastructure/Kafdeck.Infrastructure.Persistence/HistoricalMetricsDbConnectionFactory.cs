using System.Data.Common;
using Microsoft.Data.Sqlite;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

public interface IHistoricalMetricsDbConnectionFactory
{
    bool SupportsSelectForUpdate { get; }

    ValueTask<DbConnection> OpenAsync(
        CancellationToken cancellationToken = default);
}

public sealed class SqliteHistoricalMetricsDbConnectionFactory :
    IHistoricalMetricsDbConnectionFactory
{
    private readonly string _connectionString;

    public bool SupportsSelectForUpdate => false;

    public SqliteHistoricalMetricsDbConnectionFactory(
        string databasePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            databasePath);

        if (!Path.IsPathFullyQualified(
                databasePath))
        {
            throw new ArgumentException(
                "SQLite historical metrics database path must be absolute.",
                nameof(databasePath));
        }

        _connectionString =
            new SqliteConnectionStringBuilder
            {
                DataSource = databasePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Shared,
                Pooling = true,
            }.ToString();
    }

    public async ValueTask<DbConnection> OpenAsync(
        CancellationToken cancellationToken = default)
    {
        var connection =
            new SqliteConnection(
                _connectionString);
        try
        {
            await connection
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);

            await using var command =
                connection.CreateCommand();
            command.CommandText =
                "PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL;";
            await command
                .ExecuteNonQueryAsync(
                    cancellationToken)
                .ConfigureAwait(false);

            return connection;
        }
        catch
        {
            await connection
                .DisposeAsync()
                .ConfigureAwait(false);
            throw;
        }
    }
}

public sealed class PostgreSqlHistoricalMetricsDbConnectionFactory :
    IHistoricalMetricsDbConnectionFactory
{
    private readonly string _connectionString;

    public bool SupportsSelectForUpdate => true;

    public PostgreSqlHistoricalMetricsDbConnectionFactory(
        string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            connectionString);

        var builder =
            new NpgsqlConnectionStringBuilder(
                connectionString)
            {
                ApplicationName =
                    "Kafdeck-HistoricalMetrics",
                IncludeErrorDetail = false,
            };

        _connectionString =
            builder.ConnectionString;
    }

    public async ValueTask<DbConnection> OpenAsync(
        CancellationToken cancellationToken = default)
    {
        var connection =
            new NpgsqlConnection(
                _connectionString);
        await connection
            .OpenAsync(cancellationToken)
            .ConfigureAwait(false);
        return connection;
    }
}
