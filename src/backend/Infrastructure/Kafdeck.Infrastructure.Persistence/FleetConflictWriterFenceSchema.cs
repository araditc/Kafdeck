using System.Data.Common;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>
/// Database-enforced writer fence for durable fleet conflict obligations.
///
/// Pre-fence fleet-state binaries do not write the fence version/token columns.
/// After installation, inserts must explicitly carry the current writer version
/// and initial token, while safety-relevant updates must advance the token by
/// exactly one. This prevents an already-running older fleet-state process from
/// recreating an obligation without the canonical legacy guard identity or
/// durable conflict-scope binding after backfill completes.
/// </summary>
internal static class FleetConflictWriterFenceSchema
{
    public const int CurrentWriterVersion = 2;

    private const string Component = "fleet-conflict-writer-fence";
    private const string InsertTrigger = "kafdeck_fleet_conflict_writer_fence_insert";
    private const string UpdateTrigger = "kafdeck_fleet_conflict_writer_fence_update";

    public static async Task EnsureInstalledAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);

        var version = await ReadMarkerAsync(
                connection,
                transaction,
                cancellationToken)
            .ConfigureAwait(false);

        if (version is null)
        {
            await AddColumnsAsync(
                    connection,
                    transaction,
                    cancellationToken)
                .ConfigureAwait(false);

            await using (var backfill = connection.CreateCommand())
            {
                backfill.Transaction = transaction;
                backfill.CommandText =
                    """
                    UPDATE kafdeck_fleet_conflict_obligations
                    SET writer_fence_version = @writer_fence_version,
                        writer_fence_token = 0
                    """;
                AddParameter(
                    backfill,
                    "@writer_fence_version",
                    CurrentWriterVersion);
                await backfill.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using (var marker = connection.CreateCommand())
            {
                marker.Transaction = transaction;
                marker.CommandText =
                    """
                    INSERT INTO kafdeck_schema_info (component, schema_version)
                    VALUES (@component, @schema_version)
                    """;
                AddParameter(marker, "@component", Component);
                AddParameter(marker, "@schema_version", CurrentWriterVersion);
                await marker.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await InstallTriggersAsync(
                    connection,
                    transaction,
                    cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        if (version != CurrentWriterVersion)
        {
            throw new InvalidOperationException(
                $"Fleet conflict writer fence version '{version}' is unsupported. Refusing fleet mutation state.");
        }

        if (!await ValidateInstalledAsync(
                connection,
                transaction,
                cancellationToken)
            .ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                "Fleet conflict writer fence marker is active but the database enforcement is incomplete. Refusing fleet mutation state.");
        }
    }

    private static async Task<int?> ReadMarkerAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_schema_info
            WHERE component = @component
            """;
        AddParameter(command, "@component", Component);
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is null or DBNull
            ? null
            : Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    private static async Task AddColumnsAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        if (connection is SqliteConnection)
        {
            await EnsureSqliteColumnAsync(
                    connection,
                    transaction,
                    "writer_fence_version",
                    "INTEGER NOT NULL DEFAULT 0",
                    cancellationToken)
                .ConfigureAwait(false);
            await EnsureSqliteColumnAsync(
                    connection,
                    transaction,
                    "writer_fence_token",
                    "BIGINT NOT NULL DEFAULT 0",
                    cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        if (connection is NpgsqlConnection)
        {
            await ExecuteAsync(
                    connection,
                    transaction,
                    """
                    ALTER TABLE kafdeck_fleet_conflict_obligations
                        ADD COLUMN IF NOT EXISTS writer_fence_version INTEGER NOT NULL DEFAULT 0;
                    ALTER TABLE kafdeck_fleet_conflict_obligations
                        ADD COLUMN IF NOT EXISTS writer_fence_token BIGINT NOT NULL DEFAULT 0;
                    """,
                    cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        throw new NotSupportedException(
            $"Fleet conflict writer fence does not support connection type '{connection.GetType().FullName}'.");
    }

    private static async Task EnsureSqliteColumnAsync(
        DbConnection connection,
        DbTransaction transaction,
        string column,
        string definition,
        CancellationToken cancellationToken)
    {
        await using var inspect = connection.CreateCommand();
        inspect.Transaction = transaction;
        inspect.CommandText = "PRAGMA table_info(kafdeck_fleet_conflict_obligations)";

        var found = false;
        await using (var reader =
            await inspect.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (string.Equals(reader.GetString(1), column, StringComparison.Ordinal))
                {
                    found = true;
                    break;
                }
            }
        }

        if (found)
        {
            return;
        }

        await ExecuteAsync(
                connection,
                transaction,
                $"ALTER TABLE kafdeck_fleet_conflict_obligations ADD COLUMN {column} {definition}",
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static Task InstallTriggersAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken) =>
        connection switch
        {
            SqliteConnection => ExecuteAsync(
                connection,
                transaction,
                SqliteTriggerSql,
                cancellationToken),
            NpgsqlConnection => ExecuteAsync(
                connection,
                transaction,
                PostgreSqlTriggerSql,
                cancellationToken),
            _ => throw new NotSupportedException(
                $"Fleet conflict writer fence does not support connection type '{connection.GetType().FullName}'."),
        };

    private static async Task<bool> ValidateInstalledAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        if (connection is SqliteConnection)
        {
            await using var columns = connection.CreateCommand();
            columns.Transaction = transaction;
            columns.CommandText =
                """
                SELECT COUNT(1)
                FROM pragma_table_info('kafdeck_fleet_conflict_obligations')
                WHERE name IN ('writer_fence_version', 'writer_fence_token')
                """;
            if (Convert.ToInt32(
                    await columns.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                    CultureInfo.InvariantCulture) != 2)
            {
                return false;
            }

            await using var triggers = connection.CreateCommand();
            triggers.Transaction = transaction;
            triggers.CommandText =
                $"""
                SELECT COUNT(1)
                FROM sqlite_master
                WHERE type = 'trigger'
                  AND name IN ('{InsertTrigger}', '{UpdateTrigger}')
                """;
            return Convert.ToInt32(
                       await triggers.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                       CultureInfo.InvariantCulture) == 2;
        }

        if (connection is NpgsqlConnection)
        {
            await using var columns = connection.CreateCommand();
            columns.Transaction = transaction;
            columns.CommandText =
                """
                SELECT COUNT(1)
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'kafdeck_fleet_conflict_obligations'
                  AND column_name IN ('writer_fence_version', 'writer_fence_token')
                """;
            if (Convert.ToInt32(
                    await columns.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                    CultureInfo.InvariantCulture) != 2)
            {
                return false;
            }

            await using var triggers = connection.CreateCommand();
            triggers.Transaction = transaction;
            triggers.CommandText =
                $"""
                SELECT COUNT(1)
                FROM pg_catalog.pg_trigger AS trigger
                INNER JOIN pg_catalog.pg_class AS relation
                  ON relation.oid = trigger.tgrelid
                INNER JOIN pg_catalog.pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE NOT trigger.tgisinternal
                  AND namespace.nspname = current_schema()
                  AND relation.relname = 'kafdeck_fleet_conflict_obligations'
                  AND trigger.tgname IN ('{InsertTrigger}', '{UpdateTrigger}')
                """;
            return Convert.ToInt32(
                       await triggers.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                       CultureInfo.InvariantCulture) == 2;
        }

        throw new NotSupportedException(
            $"Fleet conflict writer fence does not support connection type '{connection.GetType().FullName}'.");
    }

    private static async Task ExecuteAsync(
        DbConnection connection,
        DbTransaction transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    private const string SqliteTriggerSql =
        """
        DROP TRIGGER IF EXISTS kafdeck_fleet_conflict_writer_fence_insert;
        DROP TRIGGER IF EXISTS kafdeck_fleet_conflict_writer_fence_update;

        CREATE TRIGGER kafdeck_fleet_conflict_writer_fence_insert
        BEFORE INSERT ON kafdeck_fleet_conflict_obligations
        BEGIN
            SELECT RAISE(ABORT, 'KAFDECK_FLEET_WRITER_FENCE')
            WHERE COALESCE((
                    SELECT schema_version
                    FROM kafdeck_schema_info
                    WHERE component = 'fleet-conflict-writer-fence'), -1) <> 2
               OR NEW.writer_fence_version <> 2
               OR NEW.writer_fence_token <> 1;
        END;

        CREATE TRIGGER kafdeck_fleet_conflict_writer_fence_update
        BEFORE UPDATE OF state, blocks_conflicting_dispatch, version, snapshot_json, updated_at_utc
        ON kafdeck_fleet_conflict_obligations
        BEGIN
            SELECT RAISE(ABORT, 'KAFDECK_FLEET_WRITER_FENCE')
            WHERE COALESCE((
                    SELECT schema_version
                    FROM kafdeck_schema_info
                    WHERE component = 'fleet-conflict-writer-fence'), -1) <> 2
               OR NEW.writer_fence_version <> 2
               OR NEW.writer_fence_token <> OLD.writer_fence_token + 1;
        END;
        """;

    private const string PostgreSqlTriggerSql =
        """
        CREATE OR REPLACE FUNCTION kafdeck_enforce_fleet_conflict_writer_fence()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            durable_version INTEGER;
        BEGIN
            SELECT schema_version
            INTO durable_version
            FROM kafdeck_schema_info
            WHERE component = 'fleet-conflict-writer-fence';

            IF durable_version <> 2 OR NEW.writer_fence_version <> 2 THEN
                RAISE EXCEPTION 'KAFDECK_FLEET_WRITER_FENCE';
            END IF;

            IF TG_OP = 'INSERT' THEN
                IF NEW.writer_fence_token <> 1 THEN
                    RAISE EXCEPTION 'KAFDECK_FLEET_WRITER_FENCE';
                END IF;
            ELSIF NEW.writer_fence_token <> OLD.writer_fence_token + 1 THEN
                RAISE EXCEPTION 'KAFDECK_FLEET_WRITER_FENCE';
            END IF;

            RETURN NEW;
        END;
        $function$;

        DROP TRIGGER IF EXISTS kafdeck_fleet_conflict_writer_fence_insert
            ON kafdeck_fleet_conflict_obligations;
        CREATE TRIGGER kafdeck_fleet_conflict_writer_fence_insert
        BEFORE INSERT ON kafdeck_fleet_conflict_obligations
        FOR EACH ROW
        EXECUTE FUNCTION kafdeck_enforce_fleet_conflict_writer_fence();

        DROP TRIGGER IF EXISTS kafdeck_fleet_conflict_writer_fence_update
            ON kafdeck_fleet_conflict_obligations;
        CREATE TRIGGER kafdeck_fleet_conflict_writer_fence_update
        BEFORE UPDATE OF state, blocks_conflicting_dispatch, version, snapshot_json, updated_at_utc
        ON kafdeck_fleet_conflict_obligations
        FOR EACH ROW
        EXECUTE FUNCTION kafdeck_enforce_fleet_conflict_writer_fence();
        """;
}
