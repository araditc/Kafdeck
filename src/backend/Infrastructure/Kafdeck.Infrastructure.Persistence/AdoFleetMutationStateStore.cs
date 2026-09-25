using System.Data.Common;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>
/// ADO-backed durable subordinate state for v0.6 fleet operations.
/// This store deliberately shares the mutation database and parent operation
/// identity; it never owns approval, execution authority or provider dispatch.
/// </summary>
public sealed class AdoFleetMutationStateStore : IFleetMutationStateStore
{
    private const int SchemaVersion = 1;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly IMutationDbConnectionFactory _connectionFactory;

    public AdoFleetMutationStateStore(IMutationDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);

        // The parent mutation repository must be initialized first. Keeping the
        // foreign key explicit prevents fleet state from becoming a parallel
        // operation authority with orphan identities.
        var statements = new[]
        {
            """
            CREATE TABLE IF NOT EXISTS kafdeck_schema_info (
                component TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL
            )
            """,
            """
            INSERT INTO kafdeck_schema_info (component, schema_version)
            VALUES ('fleet-mutation-state', 1)
            ON CONFLICT (component) DO NOTHING
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_fleet_progress (
                operation_id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                version BIGINT NOT NULL,
                snapshot_json TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY (operation_id)
                    REFERENCES kafdeck_mutation_operations(operation_id)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_fleet_conflict_obligations (
                obligation_id TEXT PRIMARY KEY,
                operation_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                conflict_key_hash TEXT NOT NULL,
                conflict_key TEXT NOT NULL,
                effect_fingerprint TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                state INTEGER NOT NULL,
                blocks_conflicting_dispatch INTEGER NOT NULL,
                version BIGINT NOT NULL,
                snapshot_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY (operation_id)
                    REFERENCES kafdeck_mutation_operations(operation_id)
                    ON DELETE RESTRICT,
                UNIQUE (operation_id, step_id, conflict_key_hash, conflict_key)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_fleet_conflict_blocking
            ON kafdeck_fleet_conflict_obligations (
                conflict_key_hash,
                blocks_conflicting_dispatch,
                updated_at_utc)
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_fleet_conflict_operation
            ON kafdeck_fleet_conflict_obligations (operation_id, updated_at_utc)
            """,
        };

        foreach (var statement in statements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var versionCommand = connection.CreateCommand();
        versionCommand.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_schema_info
            WHERE component = 'fleet-mutation-state'
            """;
        var version = await versionCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (Convert.ToInt32(version, CultureInfo.InvariantCulture) != SchemaVersion)
        {
            throw new InvalidOperationException(
                "Fleet mutation persistence schema version is unsupported. Refusing to activate fleet mutation state.");
        }

        await EnsureConflictScopePersistenceAsync(
                connection,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task EnsureConflictScopePersistenceAsync(
        DbConnection connection,
        CancellationToken cancellationToken)
    {
        var statements = new[]
        {
            """
            CREATE TABLE IF NOT EXISTS kafdeck_fleet_conflict_scope_guards (
                conflict_scope_hash TEXT NOT NULL,
                conflict_scope_key TEXT NOT NULL,
                PRIMARY KEY (conflict_scope_hash, conflict_scope_key)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_fleet_conflict_scope_bindings (
                obligation_id TEXT PRIMARY KEY,
                conflict_scope_hash TEXT NOT NULL,
                conflict_scope_key TEXT NOT NULL,
                FOREIGN KEY (obligation_id)
                    REFERENCES kafdeck_fleet_conflict_obligations(obligation_id)
                    ON DELETE RESTRICT
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_fleet_conflict_scope_blocking
            ON kafdeck_fleet_conflict_scope_bindings (
                conflict_scope_hash,
                conflict_scope_key,
                obligation_id)
            """,
        };

        foreach (var statement in statements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var transaction =
            await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        if (_connectionFactory.SupportsSelectForUpdate)
        {
            await using var tableLock = connection.CreateCommand();
            tableLock.Transaction = transaction;
            tableLock.CommandText =
                """
                LOCK TABLE kafdeck_fleet_conflict_obligations
                IN SHARE ROW EXCLUSIVE MODE
                """;
            await tableLock.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // SQLite serializes writers at database scope. Take that writer
            // boundary before enumerating legacy rows so backfill cannot miss a
            // concurrent obligation committed by an older process.
            await using var writerLock = connection.CreateCommand();
            writerLock.Transaction = transaction;
            writerLock.CommandText =
                """
                UPDATE kafdeck_schema_info
                SET schema_version = schema_version
                WHERE component = 'fleet-mutation-state'
                """;
            if (await writerLock.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                throw new InvalidOperationException(
                    "Fleet mutation persistence schema metadata is missing while installing canonical conflict scopes.");
            }
        }

        var rows = new List<(string ObligationId, string SnapshotJson, string? ScopeHash, string? ScopeKey)>();
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText =
                """
                SELECT
                    obligation.obligation_id,
                    obligation.snapshot_json,
                    binding.conflict_scope_hash,
                    binding.conflict_scope_key
                FROM kafdeck_fleet_conflict_obligations AS obligation
                LEFT JOIN kafdeck_fleet_conflict_scope_bindings AS binding
                  ON binding.obligation_id = obligation.obligation_id
                ORDER BY obligation.obligation_id
                """;

            await using var reader =
                await read.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rows.Add((
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3)));
            }
        }

        foreach (var row in rows)
        {
            var persistedSnapshot =
                Deserialize<FleetConflictObligationSnapshot>(row.SnapshotJson);
            var snapshot = FleetConflictObligation.Restore(persistedSnapshot).Snapshot;
            var scopeKey = FleetConflictScope.FromFleetConflictKey(snapshot.ConflictKey);
            var scopeHash = HashConflictKey(scopeKey);

            if (persistedSnapshot.LegacyResourceKey is null &&
                snapshot.LegacyResourceKey is not null)
            {
                await PersistNormalizedLegacyResourceKeyAsync(
                        connection,
                        transaction,
                        row.ObligationId,
                        row.SnapshotJson,
                        snapshot,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            else if (!string.Equals(
                         persistedSnapshot.LegacyResourceKey,
                         snapshot.LegacyResourceKey,
                         StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "Persisted fleet conflict obligation legacy resource identity conflicts with its canonical typed conflict identity.");
            }

            if (row.ScopeHash is not null || row.ScopeKey is not null)
            {
                if (!string.Equals(row.ScopeHash, scopeHash, StringComparison.Ordinal) ||
                    !string.Equals(row.ScopeKey, scopeKey, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        "Persisted fleet conflict scope binding does not match the canonical typed conflict identity.");
                }

                continue;
            }

            await EnsureConflictScopeBindingAsync(
                    connection,
                    transaction,
                    row.ObligationId,
                    scopeHash,
                    scopeKey,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        await FleetConflictWriterFenceSchema.EnsureInstalledAsync(
                connection,
                transaction,
                cancellationToken)
            .ConfigureAwait(false);

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task PersistNormalizedLegacyResourceKeyAsync(
        DbConnection connection,
        DbTransaction transaction,
        string obligationId,
        string originalSnapshotJson,
        FleetConflictObligationSnapshot normalizedSnapshot,
        CancellationToken cancellationToken)
    {
        await using var update = connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText =
            """
            UPDATE kafdeck_fleet_conflict_obligations
            SET snapshot_json = @snapshot_json
            WHERE obligation_id = @obligation_id
              AND snapshot_json = @original_snapshot_json
            """;
        AddParameter(update, "@snapshot_json", Serialize(normalizedSnapshot));
        AddParameter(update, "@obligation_id", obligationId);
        AddParameter(update, "@original_snapshot_json", originalSnapshotJson);

        if (await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
        {
            throw new InvalidOperationException(
                "Fleet conflict obligation legacy resource identity could not be atomically backfilled. Refusing fleet mutation state.");
        }
    }

    public async Task<FleetProgressCreateResult> CreateProgressAsync(
        FleetOperationProgressSnapshot progress,
        CancellationToken cancellationToken = default)
    {
        var normalized = FleetOperationProgress.Restore(
                progress ?? throw new ArgumentNullException(nameof(progress)))
            .Snapshot;
        if (normalized.Version != 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(progress),
                "New fleet progress must start at version zero.");
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        if (!await ParentExistsAsync(
                connection,
                transaction,
                normalized.OperationId,
                cancellationToken).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new FleetProgressCreateResult(
                FleetProgressCreateOutcome.ParentOperationNotFound,
                null);
        }

        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText =
            """
            INSERT INTO kafdeck_fleet_progress (
                operation_id,
                schema_version,
                version,
                snapshot_json,
                updated_at_utc)
            VALUES (
                @operation_id,
                @schema_version,
                @version,
                @snapshot_json,
                @updated_at_utc)
            ON CONFLICT (operation_id) DO NOTHING
            """;
        AddParameter(insert, "@operation_id", normalized.OperationId.ToString("D"));
        AddParameter(insert, "@schema_version", normalized.SchemaVersion);
        AddParameter(insert, "@version", normalized.Version);
        AddParameter(insert, "@snapshot_json", Serialize(normalized));
        AddParameter(insert, "@updated_at_utc", FormatTimestamp(normalized.UpdatedAtUtc));

        var inserted = await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (inserted == 1)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new FleetProgressCreateResult(FleetProgressCreateOutcome.Created, normalized);
        }

        var existing = await GetProgressAsync(
            connection,
            transaction,
            normalized.OperationId,
            cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new FleetProgressCreateResult(FleetProgressCreateOutcome.Existing, existing);
    }

    public async Task<FleetOperationProgressSnapshot?> GetProgressAsync(
        Guid operationId,
        CancellationToken cancellationToken = default)
    {
        if (operationId == Guid.Empty)
        {
            throw new ArgumentException("Operation ID is required.", nameof(operationId));
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        return await GetProgressAsync(
            connection,
            transaction: null,
            operationId,
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<FleetProgressSaveResult> TrySaveProgressAsync(
        FleetOperationProgressSnapshot progress,
        long expectedVersion,
        CancellationToken cancellationToken = default)
    {
        var normalized = FleetOperationProgress.Restore(
                progress ?? throw new ArgumentNullException(nameof(progress)))
            .Snapshot;
        if (expectedVersion < 0 || normalized.Version != checked(expectedVersion + 1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedVersion),
                "Fleet progress saves must advance the version by exactly one.");
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            UPDATE kafdeck_fleet_progress
            SET version = @new_version,
                snapshot_json = @snapshot_json,
                updated_at_utc = @updated_at_utc
            WHERE operation_id = @operation_id
              AND schema_version = @schema_version
              AND version = @expected_version
            """;
        AddParameter(command, "@new_version", normalized.Version);
        AddParameter(
            command,
            "@writer_fence_version",
            FleetConflictWriterFenceSchema.CurrentWriterVersion);
        AddParameter(command, "@snapshot_json", Serialize(normalized));
        AddParameter(command, "@updated_at_utc", FormatTimestamp(normalized.UpdatedAtUtc));
        AddParameter(command, "@operation_id", normalized.OperationId.ToString("D"));
        AddParameter(command, "@schema_version", normalized.SchemaVersion);
        AddParameter(command, "@expected_version", expectedVersion);

        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1)
        {
            return new FleetProgressSaveResult(FleetProgressSaveOutcome.Saved, normalized);
        }

        var current = await GetProgressAsync(normalized.OperationId, cancellationToken).ConfigureAwait(false);
        return current is null
            ? new FleetProgressSaveResult(FleetProgressSaveOutcome.NotFound, null)
            : new FleetProgressSaveResult(FleetProgressSaveOutcome.VersionConflict, current);
    }

    public async Task<FleetConflictObligationCreateResult> CreateConflictObligationAsync(
        FleetConflictObligationSnapshot obligation,
        CancellationToken cancellationToken = default)
    {
        var normalized = FleetConflictObligation.Restore(
                obligation ?? throw new ArgumentNullException(nameof(obligation)))
            .Snapshot;
        if (normalized.Version != 0 || normalized.State != FleetConflictObligationState.Outstanding)
        {
            throw new ArgumentOutOfRangeException(
                nameof(obligation),
                "New fleet conflict obligations must start outstanding at version zero.");
        }

        var conflictHash = HashConflictKey(normalized.ConflictKey);
        var conflictScopeKey = FleetConflictScope.FromFleetConflictKey(normalized.ConflictKey);
        var conflictScopeHash = HashConflictKey(conflictScopeKey);
        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        if (!await ParentExistsAsync(
                connection,
                transaction,
                normalized.OperationId,
                cancellationToken).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new FleetConflictObligationCreateResult(
                FleetConflictObligationCreateOutcome.ParentOperationNotFound,
                null);
        }

        await LockConflictScopeAsync(
                connection,
                transaction,
                conflictScopeHash,
                conflictScopeKey,
                cancellationToken)
            .ConfigureAwait(false);

        var blockingScope = await FindBlockingConflictObligationByScopeAsync(
                connection,
                transaction,
                conflictScopeHash,
                conflictScopeKey,
                normalized.OperationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (blockingScope is not null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new FleetConflictObligationCreateResult(
                FleetConflictObligationCreateOutcome.FleetConflictScopeConflict,
                blockingScope);
        }

        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText =
            """
            INSERT INTO kafdeck_fleet_conflict_obligations (
                obligation_id,
                operation_id,
                step_id,
                conflict_key_hash,
                conflict_key,
                effect_fingerprint,
                schema_version,
                state,
                blocks_conflicting_dispatch,
                version,
                writer_fence_version,
                writer_fence_token,
                snapshot_json,
                created_at_utc,
                updated_at_utc)
            VALUES (
                @obligation_id,
                @operation_id,
                @step_id,
                @conflict_key_hash,
                @conflict_key,
                @effect_fingerprint,
                @schema_version,
                @state,
                @blocks_conflicting_dispatch,
                @version,
                @writer_fence_version,
                @writer_fence_token,
                @snapshot_json,
                @created_at_utc,
                @updated_at_utc)
            ON CONFLICT (operation_id, step_id, conflict_key_hash, conflict_key) DO NOTHING
            """;
        AddParameter(insert, "@obligation_id", normalized.ObligationId.ToString("D"));
        AddParameter(insert, "@operation_id", normalized.OperationId.ToString("D"));
        AddParameter(insert, "@step_id", normalized.StepId);
        AddParameter(insert, "@conflict_key_hash", conflictHash);
        AddParameter(insert, "@conflict_key", normalized.ConflictKey);
        AddParameter(insert, "@effect_fingerprint", normalized.EffectFingerprint);
        AddParameter(insert, "@schema_version", normalized.SchemaVersion);
        AddParameter(insert, "@state", (int)normalized.State);
        AddParameter(insert, "@blocks_conflicting_dispatch", normalized.BlocksConflictingDispatch ? 1 : 0);
        AddParameter(insert, "@version", normalized.Version);
        AddParameter(
            insert,
            "@writer_fence_version",
            FleetConflictWriterFenceSchema.CurrentWriterVersion);
        AddParameter(insert, "@writer_fence_token", 1L);
        AddParameter(insert, "@snapshot_json", Serialize(normalized));
        AddParameter(insert, "@created_at_utc", FormatTimestamp(normalized.CreatedAtUtc));
        AddParameter(insert, "@updated_at_utc", FormatTimestamp(normalized.UpdatedAtUtc));

        var inserted = await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (inserted == 1)
        {
            await EnsureConflictScopeBindingAsync(
                    connection,
                    transaction,
                    normalized.ObligationId.ToString("D"),
                    conflictScopeHash,
                    conflictScopeKey,
                    cancellationToken)
                .ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new FleetConflictObligationCreateResult(
                FleetConflictObligationCreateOutcome.Created,
                normalized);
        }

        var existing = await GetConflictObligationByIdentityAsync(
            connection,
            transaction,
            normalized.OperationId,
            normalized.StepId,
            conflictHash,
            normalized.ConflictKey,
            cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            await EnsureConflictScopeBindingAsync(
                    connection,
                    transaction,
                    existing.ObligationId.ToString("D"),
                    conflictScopeHash,
                    conflictScopeKey,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

        if (existing is null)
        {
            // The only admitted non-uniqueness path that can suppress this INSERT
            // without producing an obligation row is the database-native W41
            // legacy-claim guard. Keep it a typed admission conflict rather than
            // throwing or retrying; the competing claim may disappear immediately
            // after the serialized admission point and that must not turn this
            // original attempt into an implicit retry.
            return new FleetConflictObligationCreateResult(
                FleetConflictObligationCreateOutcome.LegacyResourceClaimConflict,
                null);
        }

        return new FleetConflictObligationCreateResult(
            string.Equals(existing.EffectFingerprint, normalized.EffectFingerprint, StringComparison.Ordinal)
                ? FleetConflictObligationCreateOutcome.ExistingSameEffect
                : FleetConflictObligationCreateOutcome.ExistingDifferentEffect,
            existing);
    }

    public async Task<FleetConflictObligationBatchCreateResult> CreateConflictObligationsAsync(
        IReadOnlyList<FleetConflictObligationSnapshot> obligations,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(obligations);
        if (obligations.Count is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(
                nameof(obligations),
                "Fleet conflict obligation batches must contain between one and one hundred exact effects.");
        }

        var normalized = obligations
            .Select(obligation => FleetConflictObligation.Restore(
                    obligation ?? throw new ArgumentException(
                        "Fleet conflict obligation batches cannot contain null entries.",
                        nameof(obligations)))
                .Snapshot)
            .ToArray();

        if (normalized.Any(obligation =>
                obligation.Version != 0 ||
                obligation.State != FleetConflictObligationState.Outstanding))
        {
            throw new ArgumentOutOfRangeException(
                nameof(obligations),
                "New fleet conflict obligations must start outstanding at version zero.");
        }

        var operationId = normalized[0].OperationId;
        if (normalized.Any(obligation => obligation.OperationId != operationId))
        {
            throw new ArgumentException(
                "One atomic fleet conflict obligation batch must belong to one parent operation.",
                nameof(obligations));
        }

        var entries = normalized
            .Select(obligation => new
            {
                Obligation = obligation,
                ConflictHash = HashConflictKey(obligation.ConflictKey),
                ScopeKey = FleetConflictScope.FromFleetConflictKey(obligation.ConflictKey),
            })
            .Select(entry => new
            {
                entry.Obligation,
                entry.ConflictHash,
                entry.ScopeKey,
                ScopeHash = HashConflictKey(entry.ScopeKey),
            })
            .OrderBy(entry => entry.ScopeKey, StringComparer.Ordinal)
            .ThenBy(entry => entry.Obligation.ConflictKey, StringComparer.Ordinal)
            .ThenBy(entry => entry.Obligation.StepId, StringComparer.Ordinal)
            .ToArray();

        var duplicateIdentity = entries
            .GroupBy(
                entry => (entry.Obligation.StepId, entry.Obligation.ConflictKey))
            .Any(group => group.Count() > 1);
        if (duplicateIdentity)
        {
            throw new ArgumentException(
                "Atomic fleet conflict obligation batches cannot contain duplicate step/conflict identities.",
                nameof(obligations));
        }

        await using var connection =
            await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction =
            await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        if (!await ParentExistsAsync(
                connection,
                transaction,
                operationId,
                cancellationToken).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new FleetConflictObligationBatchCreateResult(
                FleetConflictObligationBatchCreateOutcome.ParentOperationNotFound,
                Array.Empty<FleetConflictObligationSnapshot>());
        }

        foreach (var scope in entries
                     .Select(entry => (entry.ScopeHash, entry.ScopeKey))
                     .Distinct()
                     .OrderBy(scope => scope.ScopeKey, StringComparer.Ordinal))
        {
            await LockConflictScopeAsync(
                    connection,
                    transaction,
                    scope.ScopeHash,
                    scope.ScopeKey,
                    cancellationToken)
                .ConfigureAwait(false);

            var blocking = await FindBlockingConflictObligationByScopeAsync(
                    connection,
                    transaction,
                    scope.ScopeHash,
                    scope.ScopeKey,
                    operationId,
                    cancellationToken)
                .ConfigureAwait(false);
            if (blocking is not null)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new FleetConflictObligationBatchCreateResult(
                    FleetConflictObligationBatchCreateOutcome.FleetConflictScopeConflict,
                    Array.Empty<FleetConflictObligationSnapshot>(),
                    blocking);
            }
        }

        var persisted = new List<FleetConflictObligationSnapshot>(entries.Length);
        var createdAny = false;

        foreach (var entry in entries)
        {
            var obligation = entry.Obligation;
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText =
                """
                INSERT INTO kafdeck_fleet_conflict_obligations (
                    obligation_id,
                    operation_id,
                    step_id,
                    conflict_key_hash,
                    conflict_key,
                    effect_fingerprint,
                    schema_version,
                    state,
                    blocks_conflicting_dispatch,
                    version,
                    writer_fence_version,
                    writer_fence_token,
                    snapshot_json,
                    created_at_utc,
                    updated_at_utc)
                VALUES (
                    @obligation_id,
                    @operation_id,
                    @step_id,
                    @conflict_key_hash,
                    @conflict_key,
                    @effect_fingerprint,
                    @schema_version,
                    @state,
                    @blocks_conflicting_dispatch,
                    @version,
                    @writer_fence_version,
                    @writer_fence_token,
                    @snapshot_json,
                    @created_at_utc,
                    @updated_at_utc)
                ON CONFLICT (operation_id, step_id, conflict_key_hash, conflict_key) DO NOTHING
                """;
            AddParameter(insert, "@obligation_id", obligation.ObligationId.ToString("D"));
            AddParameter(insert, "@operation_id", obligation.OperationId.ToString("D"));
            AddParameter(insert, "@step_id", obligation.StepId);
            AddParameter(insert, "@conflict_key_hash", entry.ConflictHash);
            AddParameter(insert, "@conflict_key", obligation.ConflictKey);
            AddParameter(insert, "@effect_fingerprint", obligation.EffectFingerprint);
            AddParameter(insert, "@schema_version", obligation.SchemaVersion);
            AddParameter(insert, "@state", (int)obligation.State);
            AddParameter(
                insert,
                "@blocks_conflicting_dispatch",
                obligation.BlocksConflictingDispatch ? 1 : 0);
            AddParameter(insert, "@version", obligation.Version);
            AddParameter(
                insert,
                "@writer_fence_version",
                FleetConflictWriterFenceSchema.CurrentWriterVersion);
            AddParameter(insert, "@writer_fence_token", 1L);
            AddParameter(insert, "@snapshot_json", Serialize(obligation));
            AddParameter(insert, "@created_at_utc", FormatTimestamp(obligation.CreatedAtUtc));
            AddParameter(insert, "@updated_at_utc", FormatTimestamp(obligation.UpdatedAtUtc));

            if (await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1)
            {
                createdAny = true;
                await EnsureConflictScopeBindingAsync(
                        connection,
                        transaction,
                        obligation.ObligationId.ToString("D"),
                        entry.ScopeHash,
                        entry.ScopeKey,
                        cancellationToken)
                    .ConfigureAwait(false);
                persisted.Add(obligation);
                continue;
            }

            var existing = await GetConflictObligationByIdentityAsync(
                    connection,
                    transaction,
                    obligation.OperationId,
                    obligation.StepId,
                    entry.ConflictHash,
                    obligation.ConflictKey,
                    cancellationToken)
                .ConfigureAwait(false);

            if (existing is null)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new FleetConflictObligationBatchCreateResult(
                    FleetConflictObligationBatchCreateOutcome.LegacyResourceClaimConflict,
                    Array.Empty<FleetConflictObligationSnapshot>());
            }

            if (!string.Equals(
                    existing.EffectFingerprint,
                    obligation.EffectFingerprint,
                    StringComparison.Ordinal))
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new FleetConflictObligationBatchCreateResult(
                    FleetConflictObligationBatchCreateOutcome.ExistingDifferentEffect,
                    Array.Empty<FleetConflictObligationSnapshot>(),
                    existing);
            }

            await EnsureConflictScopeBindingAsync(
                    connection,
                    transaction,
                    existing.ObligationId.ToString("D"),
                    entry.ScopeHash,
                    entry.ScopeKey,
                    cancellationToken)
                .ConfigureAwait(false);
            persisted.Add(existing);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

        return new FleetConflictObligationBatchCreateResult(
            createdAny
                ? FleetConflictObligationBatchCreateOutcome.Created
                : FleetConflictObligationBatchCreateOutcome.ExistingSameEffects,
            Array.AsReadOnly(persisted.ToArray()));
    }

    public async Task<FleetConflictObligationSnapshot?> GetConflictObligationAsync(
        Guid obligationId,
        CancellationToken cancellationToken = default)
    {
        if (obligationId == Guid.Empty)
        {
            throw new ArgumentException("Obligation ID is required.", nameof(obligationId));
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_fleet_conflict_obligations
            WHERE obligation_id = @obligation_id
            """;
        AddParameter(command, "@obligation_id", obligationId.ToString("D"));
        return await ReadConflictObligationAsync(command, cancellationToken).ConfigureAwait(false);
    }

    public async Task<FleetConflictObligationSnapshot?> FindBlockingConflictObligationAsync(
        string conflictKey,
        CancellationToken cancellationToken = default)
    {
        var normalizedKey = RequireConflictKey(conflictKey);
        var conflictScopeKey = FleetConflictScope.FromFleetConflictKey(normalizedKey);
        var conflictScopeHash = HashConflictKey(conflictScopeKey);

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        return await FindBlockingConflictObligationByScopeAsync(
                connection,
                transaction: null,
                conflictScopeHash,
                conflictScopeKey,
                excludedOperationId: null,
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<FleetConflictObligationSaveResult> TrySaveConflictObligationAsync(
        FleetConflictObligationSnapshot obligation,
        long expectedVersion,
        CancellationToken cancellationToken = default)
    {
        var normalized = FleetConflictObligation.Restore(
                obligation ?? throw new ArgumentNullException(nameof(obligation)))
            .Snapshot;
        if (expectedVersion < 0 || normalized.Version != checked(expectedVersion + 1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedVersion),
                "Fleet conflict obligation saves must advance the version by exactly one.");
        }

        var current = await GetConflictObligationAsync(normalized.ObligationId, cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            return new FleetConflictObligationSaveResult(
                FleetConflictObligationSaveOutcome.NotFound,
                null);
        }

        if (!HasSameImmutableIdentity(current, normalized))
        {
            return new FleetConflictObligationSaveResult(
                FleetConflictObligationSaveOutcome.ImmutableIdentityConflict,
                current);
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            UPDATE kafdeck_fleet_conflict_obligations
            SET state = @state,
                blocks_conflicting_dispatch = @blocks_conflicting_dispatch,
                version = @new_version,
                writer_fence_version = @writer_fence_version,
                writer_fence_token = writer_fence_token + 1,
                snapshot_json = @snapshot_json,
                updated_at_utc = @updated_at_utc
            WHERE obligation_id = @obligation_id
              AND schema_version = @schema_version
              AND version = @expected_version
              AND operation_id = @operation_id
              AND step_id = @step_id
              AND conflict_key_hash = @conflict_key_hash
              AND conflict_key = @conflict_key
              AND effect_fingerprint = @effect_fingerprint
            """;
        AddParameter(command, "@state", (int)normalized.State);
        AddParameter(command, "@blocks_conflicting_dispatch", normalized.BlocksConflictingDispatch ? 1 : 0);
        AddParameter(command, "@new_version", normalized.Version);
        AddParameter(
            command,
            "@writer_fence_version",
            FleetConflictWriterFenceSchema.CurrentWriterVersion);
        AddParameter(command, "@snapshot_json", Serialize(normalized));
        AddParameter(command, "@updated_at_utc", FormatTimestamp(normalized.UpdatedAtUtc));
        AddParameter(command, "@obligation_id", normalized.ObligationId.ToString("D"));
        AddParameter(command, "@schema_version", normalized.SchemaVersion);
        AddParameter(command, "@expected_version", expectedVersion);
        AddParameter(command, "@operation_id", normalized.OperationId.ToString("D"));
        AddParameter(command, "@step_id", normalized.StepId);
        AddParameter(command, "@conflict_key_hash", HashConflictKey(normalized.ConflictKey));
        AddParameter(command, "@conflict_key", normalized.ConflictKey);
        AddParameter(command, "@effect_fingerprint", normalized.EffectFingerprint);

        if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1)
        {
            return new FleetConflictObligationSaveResult(
                FleetConflictObligationSaveOutcome.Saved,
                normalized);
        }

        var latest = await GetConflictObligationAsync(normalized.ObligationId, cancellationToken).ConfigureAwait(false);
        if (latest is null)
        {
            return new FleetConflictObligationSaveResult(
                FleetConflictObligationSaveOutcome.NotFound,
                null);
        }

        return !HasSameImmutableIdentity(latest, normalized)
            ? new FleetConflictObligationSaveResult(
                FleetConflictObligationSaveOutcome.ImmutableIdentityConflict,
                latest)
            : new FleetConflictObligationSaveResult(
                FleetConflictObligationSaveOutcome.VersionConflict,
                latest);
    }

    private async Task LockConflictScopeAsync(
        DbConnection connection,
        DbTransaction transaction,
        string conflictScopeHash,
        string conflictScopeKey,
        CancellationToken cancellationToken)
    {
        await using (var ensure = connection.CreateCommand())
        {
            ensure.Transaction = transaction;
            ensure.CommandText =
                """
                INSERT INTO kafdeck_fleet_conflict_scope_guards (
                    conflict_scope_hash,
                    conflict_scope_key)
                VALUES (
                    @conflict_scope_hash,
                    @conflict_scope_key)
                ON CONFLICT (conflict_scope_hash, conflict_scope_key) DO NOTHING
                """;
            AddParameter(ensure, "@conflict_scope_hash", conflictScopeHash);
            AddParameter(ensure, "@conflict_scope_key", conflictScopeKey);
            await ensure.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var lockScope = connection.CreateCommand();
        lockScope.Transaction = transaction;
        lockScope.CommandText = _connectionFactory.SupportsSelectForUpdate
            ? """
              SELECT conflict_scope_key
              FROM kafdeck_fleet_conflict_scope_guards
              WHERE conflict_scope_hash = @conflict_scope_hash
                AND conflict_scope_key = @conflict_scope_key
              FOR UPDATE
              """
            : """
              UPDATE kafdeck_fleet_conflict_scope_guards
              SET conflict_scope_key = conflict_scope_key
              WHERE conflict_scope_hash = @conflict_scope_hash
                AND conflict_scope_key = @conflict_scope_key
              """;
        AddParameter(lockScope, "@conflict_scope_hash", conflictScopeHash);
        AddParameter(lockScope, "@conflict_scope_key", conflictScopeKey);

        if (_connectionFactory.SupportsSelectForUpdate)
        {
            var locked = await lockScope.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (locked is null or DBNull)
            {
                throw new InvalidOperationException(
                    "Canonical fleet conflict scope guard could not be locked.");
            }
        }
        else if (await lockScope.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
        {
            throw new InvalidOperationException(
                "Canonical fleet conflict scope guard could not be locked.");
        }
    }

    private static async Task<FleetConflictObligationSnapshot?>
        FindBlockingConflictObligationByScopeAsync(
            DbConnection connection,
            DbTransaction? transaction,
            string conflictScopeHash,
            string conflictScopeKey,
            Guid? excludedOperationId,
            CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT obligation.snapshot_json
            FROM kafdeck_fleet_conflict_scope_bindings AS binding
            INNER JOIN kafdeck_fleet_conflict_obligations AS obligation
              ON obligation.obligation_id = binding.obligation_id
            WHERE binding.conflict_scope_hash = @conflict_scope_hash
              AND binding.conflict_scope_key = @conflict_scope_key
              AND obligation.blocks_conflicting_dispatch = 1
            """ +
            (excludedOperationId.HasValue
                ? " AND obligation.operation_id <> @excluded_operation_id\n"
                : "\n") +
            """
            ORDER BY obligation.created_at_utc, obligation.obligation_id
            LIMIT 1
            """;
        AddParameter(command, "@conflict_scope_hash", conflictScopeHash);
        AddParameter(command, "@conflict_scope_key", conflictScopeKey);
        if (excludedOperationId.HasValue)
        {
            AddParameter(
                command,
                "@excluded_operation_id",
                excludedOperationId.Value.ToString("D"));
        }

        return await ReadConflictObligationAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task EnsureConflictScopeBindingAsync(
        DbConnection connection,
        DbTransaction transaction,
        string obligationId,
        string conflictScopeHash,
        string conflictScopeKey,
        CancellationToken cancellationToken)
    {
        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText =
                """
                INSERT INTO kafdeck_fleet_conflict_scope_bindings (
                    obligation_id,
                    conflict_scope_hash,
                    conflict_scope_key)
                VALUES (
                    @obligation_id,
                    @conflict_scope_hash,
                    @conflict_scope_key)
                ON CONFLICT (obligation_id) DO NOTHING
                """;
            AddParameter(insert, "@obligation_id", obligationId);
            AddParameter(insert, "@conflict_scope_hash", conflictScopeHash);
            AddParameter(insert, "@conflict_scope_key", conflictScopeKey);
            if (await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1)
            {
                return;
            }
        }

        await using var verify = connection.CreateCommand();
        verify.Transaction = transaction;
        verify.CommandText =
            """
            SELECT conflict_scope_hash, conflict_scope_key
            FROM kafdeck_fleet_conflict_scope_bindings
            WHERE obligation_id = @obligation_id
            """;
        AddParameter(verify, "@obligation_id", obligationId);
        await using var reader =
            await verify.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ||
            !string.Equals(reader.GetString(0), conflictScopeHash, StringComparison.Ordinal) ||
            !string.Equals(reader.GetString(1), conflictScopeKey, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Fleet conflict obligation has a non-canonical or conflicting durable scope binding.");
        }
    }

    private static async Task<bool> ParentExistsAsync(
        DbConnection connection,
        DbTransaction transaction,
        Guid operationId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT COUNT(1)
            FROM kafdeck_mutation_operations
            WHERE operation_id = @operation_id
            """;
        AddParameter(command, "@operation_id", operationId.ToString("D"));
        var count = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(count, CultureInfo.InvariantCulture) == 1;
    }

    private static async Task<FleetOperationProgressSnapshot?> GetProgressAsync(
        DbConnection connection,
        DbTransaction? transaction,
        Guid operationId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_fleet_progress
            WHERE operation_id = @operation_id
            """;
        AddParameter(command, "@operation_id", operationId.ToString("D"));
        var json = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (json is null or DBNull)
        {
            return null;
        }

        return ValidateProgress(Deserialize<FleetOperationProgressSnapshot>((string)json));
    }

    private static async Task<FleetConflictObligationSnapshot?> GetConflictObligationByIdentityAsync(
        DbConnection connection,
        DbTransaction transaction,
        Guid operationId,
        string stepId,
        string conflictHash,
        string conflictKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_fleet_conflict_obligations
            WHERE operation_id = @operation_id
              AND step_id = @step_id
              AND conflict_key_hash = @conflict_key_hash
              AND conflict_key = @conflict_key
            """;
        AddParameter(command, "@operation_id", operationId.ToString("D"));
        AddParameter(command, "@step_id", stepId);
        AddParameter(command, "@conflict_key_hash", conflictHash);
        AddParameter(command, "@conflict_key", conflictKey);
        return await ReadConflictObligationAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<FleetConflictObligationSnapshot?> ReadConflictObligationAsync(
        DbCommand command,
        CancellationToken cancellationToken)
    {
        var json = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (json is null or DBNull)
        {
            return null;
        }

        return FleetConflictObligation.Restore(
                Deserialize<FleetConflictObligationSnapshot>((string)json))
            .Snapshot;
    }

    private static FleetOperationProgressSnapshot ValidateProgress(
        FleetOperationProgressSnapshot snapshot) =>
        FleetOperationProgress.Restore(snapshot).Snapshot;

    private static bool HasSameImmutableIdentity(
        FleetConflictObligationSnapshot left,
        FleetConflictObligationSnapshot right) =>
        left.ObligationId == right.ObligationId &&
        left.OperationId == right.OperationId &&
        left.SchemaVersion == right.SchemaVersion &&
        string.Equals(left.StepId, right.StepId, StringComparison.Ordinal) &&
        string.Equals(left.ConflictKey, right.ConflictKey, StringComparison.Ordinal) &&
        string.Equals(left.EffectFingerprint, right.EffectFingerprint, StringComparison.Ordinal) &&
        left.CreatedAtUtc.Equals(right.CreatedAtUtc);

    private static string HashConflictKey(string conflictKey) =>
        Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(RequireConflictKey(conflictKey))))
            .ToLowerInvariant();

    private static string RequireConflictKey(string conflictKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(conflictKey);
        var normalized = conflictKey.Trim();
        if (normalized.Length > 2048 || normalized.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                nameof(conflictKey),
                "Conflict key must be at most 2048 characters and contain no control characters.");
        }

        return normalized;
    }

    private static string Serialize<T>(T value) =>
        JsonSerializer.Serialize(value, JsonOptions);

    private static T Deserialize<T>(string json) =>
        JsonSerializer.Deserialize<T>(json, JsonOptions) ??
        throw new InvalidOperationException(
            $"Persisted fleet mutation state '{typeof(T).Name}' could not be deserialized.");

    private static string FormatTimestamp(DateTimeOffset value) =>
        value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    private static void AddParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
