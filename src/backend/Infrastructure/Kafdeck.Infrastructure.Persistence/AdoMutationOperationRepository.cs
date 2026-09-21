using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoMutationOperationRepository : IMutationOperationRepository
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly IMutationDbConnectionFactory _connectionFactory;
    private readonly TimeProvider _timeProvider;

    public AdoMutationOperationRepository(
        IMutationDbConnectionFactory connectionFactory,
        TimeProvider? timeProvider = null)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);

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
            VALUES ('mutation-operations', 2)
            ON CONFLICT (component) DO NOTHING
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_mutation_operations (
                operation_id TEXT PRIMARY KEY,
                idempotency_scope TEXT NOT NULL,
                idempotency_key_hash TEXT NOT NULL,
                canonical_intent_hash TEXT NOT NULL,
                version BIGINT NOT NULL,
                state INTEGER NOT NULL,
                execution_claim_expires_at_utc TEXT NULL,
                snapshot_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                UNIQUE (idempotency_scope, idempotency_key_hash)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_mutation_operations_state
            ON kafdeck_mutation_operations (state, updated_at_utc)
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_mutation_operations_recovery
            ON kafdeck_mutation_operations (
                state,
                execution_claim_expires_at_utc,
                updated_at_utc)
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_mutation_resource_claims (
                resource_key TEXT PRIMARY KEY,
                operation_id TEXT NOT NULL,
                execution_generation BIGINT NOT NULL,
                expires_at_utc TEXT NOT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_mutation_resource_claims_operation
            ON kafdeck_mutation_resource_claims (operation_id, execution_generation)
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
            WHERE component = 'mutation-operations'
            """;
        var version = await versionCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (Convert.ToInt32(version, CultureInfo.InvariantCulture) != 2)
        {
            throw new InvalidOperationException(
                "Mutation persistence schema version is unsupported. Refusing to start mutation mode.");
        }
    }

    public async Task<MutationCreateResult> CreateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText =
            """
            INSERT INTO kafdeck_mutation_operations (
                operation_id,
                idempotency_scope,
                idempotency_key_hash,
                canonical_intent_hash,
                version,
                state,
                execution_claim_expires_at_utc,
                snapshot_json,
                created_at_utc,
                updated_at_utc)
            VALUES (
                @operation_id,
                @idempotency_scope,
                @idempotency_key_hash,
                @canonical_intent_hash,
                @version,
                @state,
                @execution_claim_expires_at_utc,
                @snapshot_json,
                @created_at_utc,
                @updated_at_utc)
            ON CONFLICT (idempotency_scope, idempotency_key_hash) DO NOTHING
            """;
        AddParameter(insert, "@operation_id", operation.OperationId.ToString("D"));
        AddParameter(insert, "@idempotency_scope", operation.IdempotencyScope);
        AddParameter(insert, "@idempotency_key_hash", operation.IdempotencyKeyHash);
        AddParameter(insert, "@canonical_intent_hash", operation.CanonicalIntentHash);
        AddParameter(insert, "@version", operation.Version);
        AddParameter(insert, "@state", (int)operation.State);
        AddParameter(
            insert,
            "@execution_claim_expires_at_utc",
            operation.ExecutionClaimExpiresAtUtc is { } leaseExpiry
                ? FormatTimestamp(leaseExpiry)
                : null);
        AddParameter(insert, "@snapshot_json", Serialize(operation));
        AddParameter(insert, "@created_at_utc", FormatTimestamp(operation.CreatedAtUtc));
        AddParameter(insert, "@updated_at_utc", FormatTimestamp(operation.UpdatedAtUtc));

        var inserted = await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (inserted == 1)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new MutationCreateResult(MutationCreateOutcome.Created, operation);
        }

        var existing = await GetByIdempotencyAsync(
            connection,
            transaction,
            operation.IdempotencyScope,
            operation.IdempotencyKeyHash,
            cancellationToken).ConfigureAwait(false);

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

        if (existing is null)
        {
            throw new InvalidOperationException("Idempotency conflict was observed but the existing mutation operation could not be read.");
        }

        return new MutationCreateResult(
            string.Equals(
                existing.CanonicalIntentHash,
                operation.CanonicalIntentHash,
                StringComparison.Ordinal)
                ? MutationCreateOutcome.ExistingSameIntent
                : MutationCreateOutcome.IdempotencyConflict,
            existing);
    }

    public async Task<MutationOperationSnapshot?> GetAsync(
        Guid operationId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_mutation_operations
            WHERE operation_id = @operation_id
            """;
        AddParameter(command, "@operation_id", operationId.ToString("D"));

        return await ReadSingleSnapshotAsync(command, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<MutationOperationSnapshot>> ListByStateAsync(
        MutationOperationState state,
        int limit,
        CancellationToken cancellationToken = default)
    {
        if (limit is < 1 or > 10_001)
        {
            throw new ArgumentOutOfRangeException(nameof(limit));
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_mutation_operations
            WHERE state = @state
            ORDER BY updated_at_utc, operation_id
            LIMIT @limit
            """;
        AddParameter(command, "@state", (int)state);
        AddParameter(command, "@limit", limit);

        var items = new List<MutationOperationSnapshot>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var json = reader.GetString(0);
            items.Add(
                JsonSerializer.Deserialize<MutationOperationSnapshot>(json, JsonOptions) ??
                throw new InvalidOperationException("Persisted mutation snapshot could not be deserialized."));
        }

        return Array.AsReadOnly(items.ToArray());
    }

    public async Task<IReadOnlyList<MutationOperationSnapshot>> ListRecoverableExecutionsAsync(
        DateTimeOffset nowUtc,
        bool includeActiveLeases,
        int limit,
        CancellationToken cancellationToken = default)
    {
        if (limit is < 1 or > 10_001)
        {
            throw new ArgumentOutOfRangeException(nameof(limit));
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_mutation_operations
            WHERE state = @state
              AND (
                  @include_active = 1
                  OR execution_claim_expires_at_utc IS NULL
                  OR execution_claim_expires_at_utc <= @now_utc
              )
            ORDER BY updated_at_utc, operation_id
            LIMIT @limit
            """;
        AddParameter(command, "@state", (int)MutationOperationState.Executing);
        AddParameter(command, "@include_active", includeActiveLeases ? 1 : 0);
        AddParameter(command, "@now_utc", FormatTimestamp(nowUtc));
        AddParameter(command, "@limit", limit);

        var items = new List<MutationOperationSnapshot>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var json = reader.GetString(0);
            items.Add(
                JsonSerializer.Deserialize<MutationOperationSnapshot>(json, JsonOptions) ??
                throw new InvalidOperationException("Persisted mutation snapshot could not be deserialized."));
        }

        return Array.AsReadOnly(items.ToArray());
    }

    public async Task<MutationSaveResult> TrySaveAsync(
        MutationOperationSnapshot operation,
        long expectedVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (expectedVersion < 0 || operation.Version != checked(expectedVersion + 1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedVersion),
                "Mutation saves must advance the aggregate version by exactly one.");
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            UPDATE kafdeck_mutation_operations
            SET version = @new_version,
                state = @state,
                execution_claim_expires_at_utc = @execution_claim_expires_at_utc,
                snapshot_json = @snapshot_json,
                updated_at_utc = @updated_at_utc
            WHERE operation_id = @operation_id
              AND version = @expected_version
            """;
        AddParameter(command, "@new_version", operation.Version);
        AddParameter(command, "@state", (int)operation.State);
        AddParameter(
            command,
            "@execution_claim_expires_at_utc",
            operation.ExecutionClaimExpiresAtUtc is { } leaseExpiry
                ? FormatTimestamp(leaseExpiry)
                : null);
        AddParameter(command, "@snapshot_json", Serialize(operation));
        AddParameter(command, "@updated_at_utc", FormatTimestamp(operation.UpdatedAtUtc));
        AddParameter(command, "@operation_id", operation.OperationId.ToString("D"));
        AddParameter(command, "@expected_version", expectedVersion);

        var updated = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (updated == 1)
        {
            return new MutationSaveResult(MutationSaveOutcome.Saved, operation);
        }

        var current = await GetAsync(operation.OperationId, cancellationToken).ConfigureAwait(false);
        return current is null
            ? new MutationSaveResult(MutationSaveOutcome.NotFound, null)
            : new MutationSaveResult(MutationSaveOutcome.VersionConflict, current);
    }

    public async Task<MutationResourceClaimResult> TryAcquireResourceClaimsAsync(
        Guid operationId,
        long executionClaimGeneration,
        IReadOnlyList<string> resourceKeys,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(resourceKeys);
        if (executionClaimGeneration <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(executionClaimGeneration));
        }

        var normalized = resourceKeys
            .Select(value => value?.Trim() ?? string.Empty)
            .Where(value => value.Length > 0)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();

        if (normalized.Length == 0)
        {
            throw new ArgumentException("At least one resource claim is required.", nameof(resourceKeys));
        }

        var nowUtc = _timeProvider.GetUtcNow();
        if (expiresAtUtc <= nowUtc)
        {
            throw new ArgumentOutOfRangeException(nameof(expiresAtUtc), "Resource claim expiry must be in the future.");
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var persistedOperation = await GetByOperationIdAsync(
            connection,
            transaction,
            operationId,
            _connectionFactory.SupportsSelectForUpdate,
            cancellationToken).ConfigureAwait(false);

        if (persistedOperation is null ||
            persistedOperation.State != MutationOperationState.Executing ||
            persistedOperation.ExecutionClaimGeneration != executionClaimGeneration ||
            persistedOperation.ExecutionClaimExpiresAtUtc is not { } persistedLeaseExpiry ||
            persistedLeaseExpiry <= nowUtc ||
            expiresAtUtc > persistedLeaseExpiry)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationResourceClaimResult(
                MutationResourceClaimOutcome.InvalidExecutionClaim);
        }

        await using (var purge = connection.CreateCommand())
        {
            purge.Transaction = transaction;
            purge.CommandText =
                """
                DELETE FROM kafdeck_mutation_resource_claims
                WHERE expires_at_utc <= @now_utc
                  AND operation_id NOT IN (
                      SELECT operation_id
                      FROM kafdeck_mutation_operations
                      WHERE state IN (@executing_state, @unknown_state)
                  )
                """;
            AddParameter(purge, "@now_utc", FormatTimestamp(nowUtc));
            AddParameter(purge, "@executing_state", (int)MutationOperationState.Executing);
            AddParameter(purge, "@unknown_state", (int)MutationOperationState.ExecutionUnknown);
            await purge.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        foreach (var resourceKey in normalized)
        {
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText =
                """
                INSERT INTO kafdeck_mutation_resource_claims (
                    resource_key,
                    operation_id,
                    execution_generation,
                    expires_at_utc)
                VALUES (
                    @resource_key,
                    @operation_id,
                    @execution_generation,
                    @expires_at_utc)
                ON CONFLICT (resource_key) DO NOTHING
                """;
            AddParameter(insert, "@resource_key", resourceKey);
            AddParameter(insert, "@operation_id", operationId.ToString("D"));
            AddParameter(insert, "@execution_generation", executionClaimGeneration);
            AddParameter(insert, "@expires_at_utc", FormatTimestamp(expiresAtUtc));

            var inserted = await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (inserted == 1)
            {
                continue;
            }

            var existing = await GetClaimAsync(
                connection,
                transaction,
                resourceKey,
                cancellationToken).ConfigureAwait(false);

            if (existing is not null &&
                existing.Value.OperationId == operationId &&
                existing.Value.Generation == executionClaimGeneration)
            {
                await using var renew = connection.CreateCommand();
                renew.Transaction = transaction;
                renew.CommandText =
                    """
                    UPDATE kafdeck_mutation_resource_claims
                    SET expires_at_utc = @expires_at_utc
                    WHERE resource_key = @resource_key
                      AND operation_id = @operation_id
                      AND execution_generation = @execution_generation
                    """;
                AddParameter(renew, "@expires_at_utc", FormatTimestamp(expiresAtUtc));
                AddParameter(renew, "@resource_key", resourceKey);
                AddParameter(renew, "@operation_id", operationId.ToString("D"));
                AddParameter(renew, "@execution_generation", executionClaimGeneration);
                await renew.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationResourceClaimResult(
                MutationResourceClaimOutcome.Conflict,
                resourceKey);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new MutationResourceClaimResult(MutationResourceClaimOutcome.Acquired);
    }

    public async Task ReleaseResourceClaimsAsync(
        Guid operationId,
        long executionClaimGeneration,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            DELETE FROM kafdeck_mutation_resource_claims
            WHERE operation_id = @operation_id
              AND execution_generation = @execution_generation
            """;
        AddParameter(command, "@operation_id", operationId.ToString("D"));
        AddParameter(command, "@execution_generation", executionClaimGeneration);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<MutationOperationSnapshot?> GetByOperationIdAsync(
        DbConnection connection,
        DbTransaction transaction,
        Guid operationId,
        bool lockForUpdate,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_mutation_operations
            WHERE operation_id = @operation_id
            """ +
            (lockForUpdate ? " FOR UPDATE" : string.Empty);
        AddParameter(command, "@operation_id", operationId.ToString("D"));
        return await ReadSingleSnapshotAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<MutationOperationSnapshot?> GetByIdempotencyAsync(
        DbConnection connection,
        DbTransaction transaction,
        string scope,
        string keyHash,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_mutation_operations
            WHERE idempotency_scope = @scope
              AND idempotency_key_hash = @key_hash
            """;
        AddParameter(command, "@scope", scope);
        AddParameter(command, "@key_hash", keyHash);
        return await ReadSingleSnapshotAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<(Guid OperationId, long Generation)?> GetClaimAsync(
        DbConnection connection,
        DbTransaction transaction,
        string resourceKey,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT operation_id, execution_generation
            FROM kafdeck_mutation_resource_claims
            WHERE resource_key = @resource_key
            """;
        AddParameter(command, "@resource_key", resourceKey);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return (
            Guid.Parse(reader.GetString(0)),
            reader.GetInt64(1));
    }

    private static async Task<MutationOperationSnapshot?> ReadSingleSnapshotAsync(
        DbCommand command,
        CancellationToken cancellationToken)
    {
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (value is null or DBNull)
        {
            return null;
        }

        var json = Convert.ToString(value, CultureInfo.InvariantCulture);
        if (string.IsNullOrWhiteSpace(json))
        {
            throw new InvalidOperationException("Persisted mutation snapshot is empty.");
        }

        return JsonSerializer.Deserialize<MutationOperationSnapshot>(json, JsonOptions) ??
               throw new InvalidOperationException("Persisted mutation snapshot could not be deserialized.");
    }

    private static string Serialize(MutationOperationSnapshot operation) =>
        JsonSerializer.Serialize(operation, JsonOptions);

    private static string FormatTimestamp(DateTimeOffset timestamp) =>
        timestamp.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    private static void AddParameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
