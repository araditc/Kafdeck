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
/// trigger predicate, while PostgreSQL serializes competing admissions on a
/// normalized legacy topic guard scope locked FOR UPDATE. Persisted v0.5 claim
/// keys and hashes remain unchanged; normalization exists only inside the
/// cross-generation admission guard.
/// </summary>
internal static class MutationConflictGuardSchema
{
    private const string ClaimTrigger = "kafdeck_claim_fleet_conflict_guard";
    private const string ObligationInsertTrigger = "kafdeck_obligation_claim_conflict_guard_insert";
    private const string ObligationUpdateTrigger = "kafdeck_obligation_claim_conflict_guard_update";
    private const string GuardSchemaDowngradeTrigger = "kafdeck_conflict_guard_schema_no_downgrade";

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

            await using (var transaction =
                await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                await ExecuteAsync(
                        connection,
                        transaction,
                        SqliteInstallSql,
                        cancellationToken)
                    .ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

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

            await using (var transaction =
                await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                await ExecuteAsync(
                        connection,
                        transaction,
                        PostgreSqlInstallSql,
                        cancellationToken)
                    .ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

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
        await using (var triggers = connection.CreateCommand())
        {
            triggers.CommandText =
                $"""
                SELECT COUNT(1)
                FROM sqlite_master
                WHERE type = 'trigger'
                  AND name IN (
                      '{ClaimTrigger}',
                      '{ObligationInsertTrigger}',
                      '{ObligationUpdateTrigger}',
                      '{GuardSchemaDowngradeTrigger}')
                """;
            var count = await triggers.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (Convert.ToInt32(count, CultureInfo.InvariantCulture) != 4)
            {
                return false;
            }
        }

        await using (var markerTable = connection.CreateCommand())
        {
            markerTable.CommandText =
                """
                SELECT COUNT(1)
                FROM sqlite_master
                WHERE type = 'table'
                  AND name = 'kafdeck_mutation_conflict_guard_schema'
                """;
            var exists = await markerTable.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (Convert.ToInt32(exists, CultureInfo.InvariantCulture) != 1)
            {
                return false;
            }
        }

        await using var marker = connection.CreateCommand();
        marker.CommandText =
            """
            SELECT COUNT(1)
            FROM kafdeck_mutation_conflict_guard_schema
            WHERE component = 'legacy-topic-guard-scope'
              AND schema_version = 3
            """;
        var markerCount = await marker.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(markerCount, CultureInfo.InvariantCulture) == 1;
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
        await using (var triggers = connection.CreateCommand())
        {
            triggers.CommandText =
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
            var count = await triggers.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (Convert.ToInt32(count, CultureInfo.InvariantCulture) != 3)
            {
                return false;
            }
        }

        await using var helper = connection.CreateCommand();
        helper.CommandText =
            """
            SELECT COUNT(1)
            FROM pg_catalog.pg_proc AS proc
            INNER JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = proc.pronamespace
            WHERE namespace.nspname = current_schema()
              AND proc.proname = 'kafdeck_legacy_topic_guard_scope_v2'
            """;
        var helperCount = await helper.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(helperCount, CultureInfo.InvariantCulture) == 1;
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

    private const string SqliteInstallSql =
        """
        CREATE TABLE IF NOT EXISTS kafdeck_mutation_conflict_guards (
            legacy_resource_key TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS kafdeck_mutation_conflict_guard_schema (
            component TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL
        );

        DROP TRIGGER IF EXISTS kafdeck_claim_fleet_conflict_guard;
        DROP TRIGGER IF EXISTS kafdeck_obligation_claim_conflict_guard_insert;
        DROP TRIGGER IF EXISTS kafdeck_obligation_claim_conflict_guard_update;

        CREATE TRIGGER kafdeck_claim_fleet_conflict_guard
        BEFORE INSERT ON kafdeck_mutation_resource_claims
        BEGIN
            INSERT OR IGNORE INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
            VALUES (
                CASE
                    WHEN length(rtrim(NEW.resource_key, '0123456789')) <
                         length(NEW.resource_key)
                     AND substr(
                            rtrim(NEW.resource_key, '0123456789'),
                            -length('/partition/')) = '/partition/'
                     AND instr(
                            substr(
                                rtrim(NEW.resource_key, '0123456789'),
                                1,
                                length(rtrim(NEW.resource_key, '0123456789')) -
                                    length('/partition/')),
                            '/topic/') > 0
                    THEN substr(
                            rtrim(NEW.resource_key, '0123456789'),
                            1,
                            length(rtrim(NEW.resource_key, '0123456789')) -
                                length('/partition/'))
                    ELSE NEW.resource_key
                END
            );

            SELECT RAISE(IGNORE)
            WHERE EXISTS (
                SELECT 1
                FROM kafdeck_fleet_conflict_obligations AS obligation
                WHERE obligation.blocks_conflicting_dispatch = 1
                  AND json_extract(obligation.snapshot_json, '$.legacyResourceKey') =
                      CASE
                          WHEN length(rtrim(NEW.resource_key, '0123456789')) <
                               length(NEW.resource_key)
                           AND substr(
                                  rtrim(NEW.resource_key, '0123456789'),
                                  -length('/partition/')) = '/partition/'
                           AND instr(
                                  substr(
                                      rtrim(NEW.resource_key, '0123456789'),
                                      1,
                                      length(rtrim(NEW.resource_key, '0123456789')) -
                                          length('/partition/')),
                                  '/topic/') > 0
                          THEN substr(
                                  rtrim(NEW.resource_key, '0123456789'),
                                  1,
                                  length(rtrim(NEW.resource_key, '0123456789')) -
                                      length('/partition/'))
                          ELSE NEW.resource_key
                      END
                  AND obligation.operation_id <> NEW.operation_id
            );
        END;

        CREATE TRIGGER kafdeck_obligation_claim_conflict_guard_insert
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
                WHERE (
                    CASE
                        WHEN length(rtrim(claim.resource_key, '0123456789')) <
                             length(claim.resource_key)
                         AND substr(
                                rtrim(claim.resource_key, '0123456789'),
                                -length('/partition/')) = '/partition/'
                         AND instr(
                                substr(
                                    rtrim(claim.resource_key, '0123456789'),
                                    1,
                                    length(rtrim(claim.resource_key, '0123456789')) -
                                        length('/partition/')),
                                '/topic/') > 0
                        THEN substr(
                                rtrim(claim.resource_key, '0123456789'),
                                1,
                                length(rtrim(claim.resource_key, '0123456789')) -
                                    length('/partition/'))
                        ELSE claim.resource_key
                    END
                ) = json_extract(NEW.snapshot_json, '$.legacyResourceKey')
                  AND claim.operation_id <> NEW.operation_id
            );
        END;

        CREATE TRIGGER kafdeck_obligation_claim_conflict_guard_update
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
                WHERE (
                    CASE
                        WHEN length(rtrim(claim.resource_key, '0123456789')) <
                             length(claim.resource_key)
                         AND substr(
                                rtrim(claim.resource_key, '0123456789'),
                                -length('/partition/')) = '/partition/'
                         AND instr(
                                substr(
                                    rtrim(claim.resource_key, '0123456789'),
                                    1,
                                    length(rtrim(claim.resource_key, '0123456789')) -
                                        length('/partition/')),
                                '/topic/') > 0
                        THEN substr(
                                rtrim(claim.resource_key, '0123456789'),
                                1,
                                length(rtrim(claim.resource_key, '0123456789')) -
                                    length('/partition/'))
                        ELSE claim.resource_key
                    END
                ) = json_extract(NEW.snapshot_json, '$.legacyResourceKey')
                  AND claim.operation_id <> NEW.operation_id
            );
        END;

        INSERT INTO kafdeck_mutation_conflict_guard_schema (component, schema_version)
        VALUES ('legacy-topic-guard-scope', 3)
        ON CONFLICT (component) DO UPDATE
        SET schema_version = excluded.schema_version;

        CREATE TRIGGER kafdeck_conflict_guard_schema_no_downgrade
        BEFORE UPDATE OF schema_version
        ON kafdeck_mutation_conflict_guard_schema
        WHEN OLD.component = 'legacy-topic-guard-scope'
         AND NEW.schema_version < OLD.schema_version
        BEGIN
            SELECT RAISE(ABORT, 'kafdeck conflict guard schema downgrade');
        END;
        """;

    private const string PostgreSqlInstallSql =
        """
        CREATE TABLE IF NOT EXISTS kafdeck_mutation_conflict_guards (
            legacy_resource_key TEXT PRIMARY KEY
        );

        CREATE OR REPLACE FUNCTION kafdeck_legacy_topic_guard_scope_v2(resource_key TEXT)
        RETURNS TEXT
        LANGUAGE sql
        IMMUTABLE
        STRICT
        AS $function$
            SELECT CASE
                WHEN resource_key ~ '^cluster/.+/topic/[^/]+/partition/[0-9]+$'
                THEN regexp_replace(resource_key, '/partition/[0-9]+$', '')
                ELSE resource_key
            END
        $function$;

        CREATE OR REPLACE FUNCTION kafdeck_guard_legacy_claim_against_fleet_obligation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            guard_key TEXT;
        BEGIN
            guard_key := kafdeck_legacy_topic_guard_scope_v2(NEW.resource_key);

            INSERT INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
            VALUES (guard_key)
            ON CONFLICT (legacy_resource_key) DO NOTHING;

            PERFORM legacy_resource_key
            FROM kafdeck_mutation_conflict_guards
            WHERE legacy_resource_key = guard_key
            FOR UPDATE;

            IF EXISTS (
                SELECT 1
                FROM kafdeck_fleet_conflict_obligations AS obligation
                WHERE obligation.blocks_conflicting_dispatch = 1
                  AND obligation.snapshot_json::jsonb ->> 'legacyResourceKey' = guard_key
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
                WHERE kafdeck_legacy_topic_guard_scope_v2(claim.resource_key) = legacy_key
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
