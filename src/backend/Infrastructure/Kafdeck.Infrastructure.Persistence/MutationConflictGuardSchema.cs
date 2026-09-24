using System.Data.Common;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Npgsql;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>
/// Installs the W41 database-native serialization boundary shared by the
/// existing v0.5 topic resource-claim path and v0.6 sticky topic obligations.
///
/// The guard is deliberately installed only after both persistence tables are
/// present. No application-level precheck is used as the exclusion authority:
/// SQLite obtains its database writer serialization before evaluating the
/// trigger predicate, while PostgreSQL serializes competing admissions on an
/// exact resource-key guard row locked FOR UPDATE.
/// </summary>
internal static class MutationConflictGuardSchema
{
    private const string ClaimTrigger = "kafdeck_claim_fleet_conflict_guard";
    private const string ObligationInsertTrigger = "kafdeck_obligation_claim_conflict_guard_insert";
    private const string ObligationUpdateTrigger = "kafdeck_obligation_claim_conflict_guard_update";

    public static async Task<bool> EnsureIfAvailableAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        if (connection is SqliteConnection)
        {
            if (!await SqliteTablesAvailableAsync(connection, cancellationToken)
                    .ConfigureAwait(false))
            {
                return false;
            }

            if (await SqliteTriggersInstalledAsync(connection, cancellationToken)
                    .ConfigureAwait(false))
            {
                return true;
            }

            await ExecuteAsync(
                    connection,
                    SqliteInstallSql,
                    cancellationToken)
                .ConfigureAwait(false);
            return true;
        }

        if (connection is NpgsqlConnection)
        {
            if (!await PostgreSqlTablesAvailableAsync(connection, cancellationToken)
                    .ConfigureAwait(false))
            {
                return false;
            }

            if (await PostgreSqlTriggersInstalledAsync(connection, cancellationToken)
                    .ConfigureAwait(false))
            {
                return true;
            }

            await ExecuteAsync(
                    connection,
                    PostgreSqlInstallSql,
                    cancellationToken)
                .ConfigureAwait(false);
            return true;
        }

        throw new NotSupportedException(
            $"Mutation conflict guard does not support connection type '{connection.GetType().FullName}'.");
    }

    private static async Task<bool> SqliteTablesAvailableAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT COUNT(1)
            FROM sqlite_master
            WHERE type = 'table'
              AND name IN (
                  'kafdeck_mutation_resource_claims',
                  'kafdeck_fleet_conflict_obligations')
            """;
        var count = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(count, CultureInfo.InvariantCulture) == 2;
    }

    private static async Task<bool> SqliteTriggersInstalledAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT(1)
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name IN (
                  '{ClaimTrigger}',
                  '{ObligationInsertTrigger}',
                  '{ObligationUpdateTrigger}')
            """;
        var count = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(count, CultureInfo.InvariantCulture) == 3;
    }

    private static async Task<bool> PostgreSqlTablesAvailableAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT CASE
                WHEN to_regclass('kafdeck_mutation_resource_claims') IS NOT NULL
                 AND to_regclass('kafdeck_fleet_conflict_obligations') IS NOT NULL
                THEN 1 ELSE 0
            END
            """;
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(value, CultureInfo.InvariantCulture) == 1;
    }

    private static async Task<bool> PostgreSqlTriggersInstalledAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT(1)
            FROM pg_catalog.pg_trigger
            WHERE NOT tgisinternal
              AND tgname IN (
                  '{ClaimTrigger}',
                  '{ObligationInsertTrigger}',
                  '{ObligationUpdateTrigger}')
              AND tgrelid IN (
                  'kafdeck_mutation_resource_claims'::regclass,
                  'kafdeck_fleet_conflict_obligations'::regclass)
            """;
        var count = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(count, CultureInfo.InvariantCulture) == 3;
    }

    private static async Task ExecuteAsync(
        DbConnection connection,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private const string SqliteInstallSql =
        """
        CREATE TABLE IF NOT EXISTS kafdeck_mutation_conflict_guards (
            legacy_resource_key TEXT PRIMARY KEY
        );

        CREATE TRIGGER IF NOT EXISTS kafdeck_claim_fleet_conflict_guard
        BEFORE INSERT ON kafdeck_mutation_resource_claims
        BEGIN
            INSERT OR IGNORE INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
            VALUES (NEW.resource_key);

            SELECT RAISE(IGNORE)
            WHERE EXISTS (
                SELECT 1
                FROM kafdeck_fleet_conflict_obligations AS obligation
                WHERE obligation.blocks_conflicting_dispatch = 1
                  AND json_extract(obligation.snapshot_json, '$.legacyResourceKey') = NEW.resource_key
                  AND obligation.operation_id <> NEW.operation_id
            );
        END;

        CREATE TRIGGER IF NOT EXISTS kafdeck_obligation_claim_conflict_guard_insert
        BEFORE INSERT ON kafdeck_fleet_conflict_obligations
        WHEN NEW.blocks_conflicting_dispatch = 1
         AND json_extract(NEW.snapshot_json, '$.legacyResourceKey') IS NOT NULL
        BEGIN
            INSERT OR IGNORE INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
            VALUES (json_extract(NEW.snapshot_json, '$.legacyResourceKey'));

            SELECT RAISE(IGNORE)
            WHERE EXISTS (
                SELECT 1
                FROM kafdeck_mutation_resource_claims AS claim
                WHERE claim.resource_key = json_extract(NEW.snapshot_json, '$.legacyResourceKey')
                  AND claim.operation_id <> NEW.operation_id
            );
        END;

        CREATE TRIGGER IF NOT EXISTS kafdeck_obligation_claim_conflict_guard_update
        BEFORE UPDATE OF blocks_conflicting_dispatch, snapshot_json
        ON kafdeck_fleet_conflict_obligations
        WHEN NEW.blocks_conflicting_dispatch = 1
         AND json_extract(NEW.snapshot_json, '$.legacyResourceKey') IS NOT NULL
        BEGIN
            INSERT OR IGNORE INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
            VALUES (json_extract(NEW.snapshot_json, '$.legacyResourceKey'));

            SELECT RAISE(IGNORE)
            WHERE EXISTS (
                SELECT 1
                FROM kafdeck_mutation_resource_claims AS claim
                WHERE claim.resource_key = json_extract(NEW.snapshot_json, '$.legacyResourceKey')
                  AND claim.operation_id <> NEW.operation_id
            );
        END;
        """;

    private const string PostgreSqlInstallSql =
        """
        CREATE TABLE IF NOT EXISTS kafdeck_mutation_conflict_guards (
            legacy_resource_key TEXT PRIMARY KEY
        );

        CREATE OR REPLACE FUNCTION kafdeck_guard_legacy_claim_against_fleet_obligation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            INSERT INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
            VALUES (NEW.resource_key)
            ON CONFLICT (legacy_resource_key) DO NOTHING;

            PERFORM legacy_resource_key
            FROM kafdeck_mutation_conflict_guards
            WHERE legacy_resource_key = NEW.resource_key
            FOR UPDATE;

            IF EXISTS (
                SELECT 1
                FROM kafdeck_fleet_conflict_obligations AS obligation
                WHERE obligation.blocks_conflicting_dispatch = 1
                  AND obligation.snapshot_json::jsonb ->> 'legacyResourceKey' = NEW.resource_key
                  AND obligation.operation_id <> NEW.operation_id
            ) THEN
                RETURN NULL;
            END IF;

            RETURN NEW;
        END;
        $function$;

        CREATE OR REPLACE FUNCTION kafdeck_guard_fleet_obligation_against_legacy_claim()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            legacy_key TEXT;
        BEGIN
            legacy_key := NEW.snapshot_json::jsonb ->> 'legacyResourceKey';
            IF NEW.blocks_conflicting_dispatch <> 1 OR legacy_key IS NULL THEN
                RETURN NEW;
            END IF;

            INSERT INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
            VALUES (legacy_key)
            ON CONFLICT (legacy_resource_key) DO NOTHING;

            PERFORM legacy_resource_key
            FROM kafdeck_mutation_conflict_guards
            WHERE legacy_resource_key = legacy_key
            FOR UPDATE;

            IF EXISTS (
                SELECT 1
                FROM kafdeck_mutation_resource_claims AS claim
                WHERE claim.resource_key = legacy_key
                  AND claim.operation_id <> NEW.operation_id
            ) THEN
                RETURN NULL;
            END IF;

            RETURN NEW;
        END;
        $function$;

        DO $block$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger
                WHERE tgname = 'kafdeck_claim_fleet_conflict_guard'
                  AND tgrelid = 'kafdeck_mutation_resource_claims'::regclass
                  AND NOT tgisinternal
            ) THEN
                CREATE TRIGGER kafdeck_claim_fleet_conflict_guard
                BEFORE INSERT ON kafdeck_mutation_resource_claims
                FOR EACH ROW
                EXECUTE FUNCTION kafdeck_guard_legacy_claim_against_fleet_obligation();
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger
                WHERE tgname = 'kafdeck_obligation_claim_conflict_guard_insert'
                  AND tgrelid = 'kafdeck_fleet_conflict_obligations'::regclass
                  AND NOT tgisinternal
            ) THEN
                CREATE TRIGGER kafdeck_obligation_claim_conflict_guard_insert
                BEFORE INSERT ON kafdeck_fleet_conflict_obligations
                FOR EACH ROW
                EXECUTE FUNCTION kafdeck_guard_fleet_obligation_against_legacy_claim();
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger
                WHERE tgname = 'kafdeck_obligation_claim_conflict_guard_update'
                  AND tgrelid = 'kafdeck_fleet_conflict_obligations'::regclass
                  AND NOT tgisinternal
            ) THEN
                CREATE TRIGGER kafdeck_obligation_claim_conflict_guard_update
                BEFORE UPDATE OF blocks_conflicting_dispatch, snapshot_json
                ON kafdeck_fleet_conflict_obligations
                FOR EACH ROW
                EXECUTE FUNCTION kafdeck_guard_fleet_obligation_against_legacy_claim();
            END IF;
        END;
        $block$;
        """;
}
