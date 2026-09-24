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
        AddParameter(insert, "@snapshot_json", Serialize(normalized));
        AddParameter(insert, "@created_at_utc", FormatTimestamp(normalized.CreatedAtUtc));
        AddParameter(insert, "@updated_at_utc", FormatTimestamp(normalized.UpdatedAtUtc));

        var inserted = await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (inserted == 1)
        {
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
        var hash = HashConflictKey(normalizedKey);

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_fleet_conflict_obligations
            WHERE conflict_key_hash = @conflict_key_hash
              AND conflict_key = @conflict_key
              AND blocks_conflicting_dispatch = 1
            ORDER BY created_at_utc, obligation_id
            LIMIT 1
            """;
        AddParameter(command, "@conflict_key_hash", hash);
        AddParameter(command, "@conflict_key", normalizedKey);
        return await ReadConflictObligationAsync(command, cancellationToken).ConfigureAwait(false);
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
