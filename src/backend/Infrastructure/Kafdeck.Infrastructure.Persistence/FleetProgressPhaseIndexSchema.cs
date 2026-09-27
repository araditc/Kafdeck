using System.Data.Common;
using Microsoft.Data.Sqlite;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

internal static class FleetProgressPhaseIndexSchema
{
    private const string Table = "kafdeck_fleet_progress";
    private const string Column = "phase";

    public static async Task EnsureInstalledAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        if (connection is SqliteConnection)
        {
            if (!await SqliteColumnExistsAsync(
                    connection,
                    cancellationToken)
                .ConfigureAwait(false))
            {
                await ExecuteAsync(
                        connection,
                        $"ALTER TABLE {Table} ADD COLUMN {Column} INTEGER NOT NULL DEFAULT 1",
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        else if (connection is NpgsqlConnection)
        {
            await ExecuteAsync(
                    connection,
                    $"ALTER TABLE {Table} ADD COLUMN IF NOT EXISTS {Column} INTEGER NOT NULL DEFAULT 1",
                    cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            throw new NotSupportedException(
                $"Fleet progress phase index does not support connection type '{connection.GetType().FullName}'.");
        }

        await ExecuteAsync(
                connection,
                $"""
                CREATE INDEX IF NOT EXISTS ix_kafdeck_fleet_progress_phase
                ON {Table} ({Column}, updated_at_utc, operation_id)
                """,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<bool> SqliteColumnExistsAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        await using var inspect = connection.CreateCommand();
        inspect.CommandText = $"PRAGMA table_info({Table})";

        await using var reader =
            await inspect.ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            if (string.Equals(
                    reader.GetString(1),
                    Column,
                    StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static async Task ExecuteAsync(
        DbConnection connection,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }
}
