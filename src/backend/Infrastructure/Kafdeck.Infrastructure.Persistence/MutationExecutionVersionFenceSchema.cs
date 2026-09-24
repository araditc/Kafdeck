using System.Data.Common;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>
/// Database-enforced W41 execution-version fence.
///
/// Schema v4 represents the admitted v0.5 executor, whose slot/claim INSERTs do
/// not carry an execution version and therefore receive the column default 0.
/// Schema v5 executors must explicitly write version 5. Triggers compare the
/// row admission version with the durable schema marker, so an already-running
/// v0.5 process cannot acquire a new slot/claim after v5 activation.
///
/// The v4->v5 migration runs under database writer/table locks before the drain
/// predicate is evaluated, closing the drain-check-to-commit race.
/// </summary>
internal static class MutationExecutionVersionFenceSchema
{
    private const int LegacySchemaVersion = 4;
    private const int CurrentSchemaVersion = 5;
    private const string SlotTrigger = "kafdeck_slot_execution_version_fence";
    private const string ClaimTrigger = "kafdeck_claim_execution_version_fence";

    public static async Task PrepareMigrationAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);

        if (connection is SqliteConnection)
        {
            await EnsureSqliteColumnAsync(
                connection,
                transaction,
                "kafdeck_mutation_cluster_slots",
                cancellationToken).ConfigureAwait(false);
            await EnsureSqliteColumnAsync(
                connection,
                transaction,
                "kafdeck_mutation_resource_claims",
                cancellationToken).ConfigureAwait(false);

            await ExecuteAsync(
                connection,
                transaction,
                SqliteTriggerSql,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (connection is NpgsqlConnection)
        {
            // Prevent an already-running v0.5 process from inserting between
            // the drain predicate and marker activation.
            await ExecuteAsync(
                connection,
                transaction,
                """
                LOCK TABLE kafdeck_mutation_cluster_slots IN ACCESS EXCLUSIVE MODE;
                LOCK TABLE kafdeck_mutation_resource_claims IN ACCESS EXCLUSIVE MODE;

                ALTER TABLE kafdeck_mutation_cluster_slots
                    ADD COLUMN IF NOT EXISTS execution_schema_version INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE kafdeck_mutation_resource_claims
                    ADD COLUMN IF NOT EXISTS execution_schema_version INTEGER NOT NULL DEFAULT 0;
                """,
                cancellationToken).ConfigureAwait(false);

            await ExecuteAsync(
                connection,
                transaction,
                PostgreSqlTriggerSql,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        throw new NotSupportedException(
            $"Mutation execution-version fence does not support connection type '{connection.GetType().FullName}'.");
    }

    public static async Task ValidateInstalledAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        var valid = connection switch
        {
            SqliteConnection => await ValidateSqliteAsync(
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false),
            NpgsqlConnection => await ValidatePostgreSqlAsync(
                connection,
                transaction,
                cancellationToken).ConfigureAwait(false),
            _ => throw new NotSupportedException(
                $"Mutation execution-version fence does not support connection type '{connection.GetType().FullName}'."),
        };

        if (!valid)
        {
            throw new InvalidOperationException(
                "Mutation persistence schema is marked v5 but the database-enforced execution-version fence is incomplete. Refusing mutation mode.");
        }
    }

    private static async Task EnsureSqliteColumnAsync(
        DbConnection connection,
        DbTransaction transaction,
        string table,
        CancellationToken cancellationToken)
    {
        await using var inspect = connection.CreateCommand();
        inspect.Transaction = transaction;
        inspect.CommandText = $"PRAGMA table_info({table})";

        var found = false;
        await using (var reader = await inspect.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (string.Equals(
                        reader.GetString(1),
                        "execution_schema_version",
                        StringComparison.Ordinal))
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
            $"ALTER TABLE {table} ADD COLUMN execution_schema_version INTEGER NOT NULL DEFAULT 0",
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> ValidateSqliteAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        foreach (var table in new[]
                 {
                     "kafdeck_mutation_cluster_slots",
                     "kafdeck_mutation_resource_claims",
                 })
        {
            await using var inspect = connection.CreateCommand();
            inspect.Transaction = transaction;
            inspect.CommandText = $"PRAGMA table_info({table})";
            var found = false;
            await using var reader = await inspect.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (string.Equals(
                        reader.GetString(1),
                        "execution_schema_version",
                        StringComparison.Ordinal))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                return false;
            }
        }

        await using var triggerCount = connection.CreateCommand();
        triggerCount.Transaction = transaction;
        triggerCount.CommandText =
            $"""
            SELECT COUNT(1)
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name IN ('{SlotTrigger}', '{ClaimTrigger}')
            """;
        return Convert.ToInt32(
                   await triggerCount.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                   CultureInfo.InvariantCulture) == 2;
    }

    private static async Task<bool> ValidatePostgreSqlAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var columns = connection.CreateCommand();
        columns.Transaction = transaction;
        columns.CommandText =
            """
            SELECT COUNT(1)
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name IN (
                  'kafdeck_mutation_cluster_slots',
                  'kafdeck_mutation_resource_claims')
              AND column_name = 'execution_schema_version'
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
              AND (
                    (trigger.tgname = '{SlotTrigger}'
                     AND relation.relname = 'kafdeck_mutation_cluster_slots')
                 OR (trigger.tgname = '{ClaimTrigger}'
                     AND relation.relname = 'kafdeck_mutation_resource_claims')
              )
            """;
        return Convert.ToInt32(
                   await triggers.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                   CultureInfo.InvariantCulture) == 2;
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

    private const string SqliteTriggerSql =
        """
        CREATE TRIGGER IF NOT EXISTS kafdeck_slot_execution_version_fence
        BEFORE INSERT ON kafdeck_mutation_cluster_slots
        BEGIN
            SELECT RAISE(ABORT, 'KAFDECK_EXECUTION_VERSION_FENCE')
            WHERE NEW.execution_schema_version <>
                CASE
                    WHEN COALESCE((
                        SELECT schema_version
                        FROM kafdeck_schema_info
                        WHERE component = 'mutation-operations'), -1) = 4
                    THEN 0
                    ELSE COALESCE((
                        SELECT schema_version
                        FROM kafdeck_schema_info
                        WHERE component = 'mutation-operations'), -1)
                END;
        END;

        CREATE TRIGGER IF NOT EXISTS kafdeck_claim_execution_version_fence
        BEFORE INSERT ON kafdeck_mutation_resource_claims
        BEGIN
            SELECT RAISE(ABORT, 'KAFDECK_EXECUTION_VERSION_FENCE')
            WHERE NEW.execution_schema_version <>
                CASE
                    WHEN COALESCE((
                        SELECT schema_version
                        FROM kafdeck_schema_info
                        WHERE component = 'mutation-operations'), -1) = 4
                    THEN 0
                    ELSE COALESCE((
                        SELECT schema_version
                        FROM kafdeck_schema_info
                        WHERE component = 'mutation-operations'), -1)
                END;
        END;
        """;

    private const string PostgreSqlTriggerSql =
        """
        CREATE OR REPLACE FUNCTION kafdeck_enforce_mutation_execution_version()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            durable_version INTEGER;
            expected_row_version INTEGER;
        BEGIN
            SELECT schema_version
            INTO durable_version
            FROM kafdeck_schema_info
            WHERE component = 'mutation-operations';

            IF durable_version IS NULL THEN
                RAISE EXCEPTION 'KAFDECK_EXECUTION_VERSION_FENCE';
            END IF;

            expected_row_version :=
                CASE
                    WHEN durable_version = 4 THEN 0
                    ELSE durable_version
                END;

            IF NEW.execution_schema_version <> expected_row_version THEN
                RAISE EXCEPTION 'KAFDECK_EXECUTION_VERSION_FENCE';
            END IF;

            RETURN NEW;
        END;
        $function$;

        DROP TRIGGER IF EXISTS kafdeck_slot_execution_version_fence
            ON kafdeck_mutation_cluster_slots;
        CREATE TRIGGER kafdeck_slot_execution_version_fence
        BEFORE INSERT ON kafdeck_mutation_cluster_slots
        FOR EACH ROW
        EXECUTE FUNCTION kafdeck_enforce_mutation_execution_version();

        DROP TRIGGER IF EXISTS kafdeck_claim_execution_version_fence
            ON kafdeck_mutation_resource_claims;
        CREATE TRIGGER kafdeck_claim_execution_version_fence
        BEFORE INSERT ON kafdeck_mutation_resource_claims
        FOR EACH ROW
        EXECUTE FUNCTION kafdeck_enforce_mutation_execution_version();
        """;
}
