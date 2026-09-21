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
            VALUES ('mutation-operations', 3)
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
            CREATE TABLE IF NOT EXISTS kafdeck_mutation_cluster_slots (
                cluster_id TEXT NOT NULL,
                slot_number INTEGER NOT NULL,
                operation_id TEXT NOT NULL,
                execution_generation BIGINT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                PRIMARY KEY (cluster_id, slot_number),
                UNIQUE (operation_id, execution_generation)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_mutation_cluster_slots_operation
            ON kafdeck_mutation_cluster_slots (operation_id, execution_generation)
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
        if (Convert.ToInt32(version, CultureInfo.InvariantCulture) != 3)
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

    public async Task<MutationClusterSlotResult> TryAcquireClusterExecutionSlotAsync(
        Guid operationId,
        long executionClaimGeneration,
        string clusterId,
        int maxConcurrentPerCluster,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        if (executionClaimGeneration <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(executionClaimGeneration));
        }

        if (maxConcurrentPerCluster is < 1 or > 16)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrentPerCluster));
        }

        var normalizedClusterId = clusterId.Trim();
        var nowUtc = _timeProvider.GetUtcNow();
        if (expiresAtUtc <= nowUtc)
        {
            throw new ArgumentOutOfRangeException(nameof(expiresAtUtc));
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var persisted = await GetByOperationIdAsync(
            connection,
            transaction,
            operationId,
            _connectionFactory.SupportsSelectForUpdate,
            cancellationToken).ConfigureAwait(false);

        if (persisted is null ||
            persisted.State != MutationOperationState.Executing ||
            persisted.ExecutionClaimGeneration != executionClaimGeneration ||
            !string.Equals(persisted.ClusterId, normalizedClusterId, StringComparison.Ordinal) ||
            persisted.ExecutionClaimExpiresAtUtc is not { } persistedLeaseExpiry ||
            persistedLeaseExpiry <= nowUtc ||
            expiresAtUtc > persistedLeaseExpiry)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationClusterSlotResult(
                MutationClusterSlotOutcome.InvalidExecutionClaim);
        }

        await using (var purge = connection.CreateCommand())
        {
            purge.Transaction = transaction;
            purge.CommandText =
                """
                DELETE FROM kafdeck_mutation_cluster_slots
                WHERE cluster_id = @cluster_id
                  AND expires_at_utc <= @now_utc
                """;
            AddParameter(purge, "@cluster_id", normalizedClusterId);
            AddParameter(purge, "@now_utc", FormatTimestamp(nowUtc));
            await purge.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var existing = connection.CreateCommand())
        {
            existing.Transaction = transaction;
            existing.CommandText =
                """
                SELECT slot_number
                FROM kafdeck_mutation_cluster_slots
                WHERE operation_id = @operation_id
                  AND execution_generation = @execution_generation
                """;
            AddParameter(existing, "@operation_id", operationId.ToString("D"));
            AddParameter(existing, "@execution_generation", executionClaimGeneration);

            var currentSlot = await existing.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (currentSlot is not null and not DBNull)
            {
                var slotNumber = Convert.ToInt32(currentSlot, CultureInfo.InvariantCulture);
                if (slotNumber < 0 || slotNumber >= maxConcurrentPerCluster)
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    return new MutationClusterSlotResult(
                        MutationClusterSlotOutcome.InvalidExecutionClaim);
                }

                await using var renew = connection.CreateCommand();
                renew.Transaction = transaction;
                renew.CommandText =
                    """
                    UPDATE kafdeck_mutation_cluster_slots
                    SET expires_at_utc = @expires_at_utc
                    WHERE cluster_id = @cluster_id
                      AND slot_number = @slot_number
                      AND operation_id = @operation_id
                      AND execution_generation = @execution_generation
                    """;
                AddParameter(renew, "@expires_at_utc", FormatTimestamp(expiresAtUtc));
                AddParameter(renew, "@cluster_id", normalizedClusterId);
                AddParameter(renew, "@slot_number", slotNumber);
                AddParameter(renew, "@operation_id", operationId.ToString("D"));
                AddParameter(renew, "@execution_generation", executionClaimGeneration);

                if (await renew.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    return new MutationClusterSlotResult(
                        MutationClusterSlotOutcome.InvalidExecutionClaim);
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new MutationClusterSlotResult(
                    MutationClusterSlotOutcome.Acquired,
                    slotNumber);
            }
        }

        await using (var activeCount = connection.CreateCommand())
        {
            activeCount.Transaction = transaction;
            activeCount.CommandText =
                """
                SELECT COUNT(*)
                FROM kafdeck_mutation_cluster_slots
                WHERE cluster_id = @cluster_id
                """;
            AddParameter(activeCount, "@cluster_id", normalizedClusterId);

            var countValue = await activeCount.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            var activeSlots = Convert.ToInt32(countValue, CultureInfo.InvariantCulture);
            if (activeSlots >= maxConcurrentPerCluster)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new MutationClusterSlotResult(
                    MutationClusterSlotOutcome.Saturated);
            }
        }

        for (var slotNumber = 0; slotNumber < maxConcurrentPerCluster; slotNumber++)
        {
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText =
                """
                INSERT INTO kafdeck_mutation_cluster_slots (
                    cluster_id,
                    slot_number,
                    operation_id,
                    execution_generation,
                    expires_at_utc)
                VALUES (
                    @cluster_id,
                    @slot_number,
                    @operation_id,
                    @execution_generation,
                    @expires_at_utc)
                ON CONFLICT (cluster_id, slot_number) DO NOTHING
                """;
            AddParameter(insert, "@cluster_id", normalizedClusterId);
            AddParameter(insert, "@slot_number", slotNumber);
            AddParameter(insert, "@operation_id", operationId.ToString("D"));
            AddParameter(insert, "@execution_generation", executionClaimGeneration);
            AddParameter(insert, "@expires_at_utc", FormatTimestamp(expiresAtUtc));

            if (await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                return new MutationClusterSlotResult(
                    MutationClusterSlotOutcome.Acquired,
                    slotNumber);
            }
        }

        await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        return new MutationClusterSlotResult(
            MutationClusterSlotOutcome.Saturated);
    }

    public async Task<MutationClusterSlotRenewOutcome> TryRenewClusterExecutionSlotAsync(
        Guid operationId,
        long executionClaimGeneration,
        string clusterId,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        if (executionClaimGeneration <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(executionClaimGeneration));
        }

        var normalizedClusterId = clusterId.Trim();
        var nowUtc = _timeProvider.GetUtcNow();
        if (expiresAtUtc <= nowUtc)
        {
            throw new ArgumentOutOfRangeException(nameof(expiresAtUtc));
        }

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var persisted = await GetByOperationIdAsync(
            connection,
            transaction,
            operationId,
            _connectionFactory.SupportsSelectForUpdate,
            cancellationToken).ConfigureAwait(false);

        if (persisted is null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return MutationClusterSlotRenewOutcome.NotFound;
        }

        if (persisted.ExecutionClaimGeneration != executionClaimGeneration ||
            !string.Equals(persisted.ClusterId, normalizedClusterId, StringComparison.Ordinal) ||
            persisted.State is not (
                MutationOperationState.Executing or
                MutationOperationState.ExecutionUnknown))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return MutationClusterSlotRenewOutcome.InvalidExecutionClaim;
        }

        await using var current = connection.CreateCommand();
        current.Transaction = transaction;
        current.CommandText =
            """
            SELECT expires_at_utc
            FROM kafdeck_mutation_cluster_slots
            WHERE cluster_id = @cluster_id
              AND operation_id = @operation_id
              AND execution_generation = @execution_generation
            """;
        AddParameter(current, "@cluster_id", normalizedClusterId);
        AddParameter(current, "@operation_id", operationId.ToString("D"));
        AddParameter(current, "@execution_generation", executionClaimGeneration);

        var currentExpiryValue = await current.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (currentExpiryValue is null or DBNull)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return MutationClusterSlotRenewOutcome.NotFound;
        }

        var currentExpiry = DateTimeOffset.Parse(
            Convert.ToString(currentExpiryValue, CultureInfo.InvariantCulture)!,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind);

        if (currentExpiry <= nowUtc)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return MutationClusterSlotRenewOutcome.Expired;
        }

        await using var renew = connection.CreateCommand();
        renew.Transaction = transaction;
        renew.CommandText =
            """
            UPDATE kafdeck_mutation_cluster_slots
            SET expires_at_utc = @expires_at_utc
            WHERE cluster_id = @cluster_id
              AND operation_id = @operation_id
              AND execution_generation = @execution_generation
              AND expires_at_utc = @current_expires_at_utc
            """;
        AddParameter(renew, "@expires_at_utc", FormatTimestamp(expiresAtUtc));
        AddParameter(renew, "@cluster_id", normalizedClusterId);
        AddParameter(renew, "@operation_id", operationId.ToString("D"));
        AddParameter(renew, "@execution_generation", executionClaimGeneration);
        AddParameter(renew, "@current_expires_at_utc", FormatTimestamp(currentExpiry));

        if (await renew.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return MutationClusterSlotRenewOutcome.InvalidExecutionClaim;
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return MutationClusterSlotRenewOutcome.Renewed;
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

    public async Task<MutationLeaseRenewResult> TryRenewExecutionLeaseAsync(
        MutationOperationSnapshot operation,
        long expectedVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (expectedVersion < 0 ||
            operation.Version != checked(expectedVersion + 1) ||
            operation.State != MutationOperationState.Executing ||
            operation.ExecutionClaimGeneration <= 0 ||
            operation.DispatchStartedAtUtc is not null ||
            operation.ExecutionClaimExpiresAtUtc is not { } requestedExpiry)
        {
            throw new ArgumentException(
                "Execution lease renewal requires one valid pre-dispatch Executing transition.",
                nameof(operation));
        }

        var nowUtc = _timeProvider.GetUtcNow();
        if (requestedExpiry <= nowUtc)
        {
            return new MutationLeaseRenewResult(
                MutationLeaseRenewOutcome.Expired,
                operation);
        }

        var expectedResources = operation.ResourceKeys
            .Select(value => value.Trim())
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();

        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        var persisted = await GetByOperationIdAsync(
            connection,
            transaction,
            operation.OperationId,
            _connectionFactory.SupportsSelectForUpdate,
            cancellationToken).ConfigureAwait(false);

        if (persisted is null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationLeaseRenewResult(MutationLeaseRenewOutcome.NotFound, null);
        }

        if (persisted.Version != expectedVersion)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationLeaseRenewResult(
                MutationLeaseRenewOutcome.VersionConflict,
                persisted);
        }

        if (persisted.State != MutationOperationState.Executing ||
            persisted.ExecutionClaimGeneration != operation.ExecutionClaimGeneration ||
            persisted.DispatchStartedAtUtc is not null)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationLeaseRenewResult(
                MutationLeaseRenewOutcome.InvalidExecutionClaim,
                persisted);
        }

        if (persisted.ExecutionClaimExpiresAtUtc is not { } persistedExpiry ||
            persistedExpiry <= nowUtc)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationLeaseRenewResult(
                MutationLeaseRenewOutcome.Expired,
                persisted);
        }

        if (requestedExpiry < persistedExpiry)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationLeaseRenewResult(
                MutationLeaseRenewOutcome.InvalidExecutionClaim,
                persisted);
        }

        var persistedClaimKeys = new List<string>();
        await using (var claims = connection.CreateCommand())
        {
            claims.Transaction = transaction;
            claims.CommandText =
                """
                SELECT resource_key
                FROM kafdeck_mutation_resource_claims
                WHERE operation_id = @operation_id
                  AND execution_generation = @execution_generation
                ORDER BY resource_key
                """;
            AddParameter(claims, "@operation_id", operation.OperationId.ToString("D"));
            AddParameter(claims, "@execution_generation", operation.ExecutionClaimGeneration);

            await using var reader = await claims.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                persistedClaimKeys.Add(reader.GetString(0));
            }
        }

        if (!persistedClaimKeys.SequenceEqual(expectedResources, StringComparer.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationLeaseRenewResult(
                MutationLeaseRenewOutcome.InvalidExecutionClaim,
                persisted);
        }

        string? persistedSlotCluster = null;
        int? persistedSlotNumber = null;
        DateTimeOffset? persistedSlotExpiry = null;
        await using (var slot = connection.CreateCommand())
        {
            slot.Transaction = transaction;
            slot.CommandText =
                """
                SELECT cluster_id, slot_number, expires_at_utc
                FROM kafdeck_mutation_cluster_slots
                WHERE operation_id = @operation_id
                  AND execution_generation = @execution_generation
                """;
            AddParameter(slot, "@operation_id", operation.OperationId.ToString("D"));
            AddParameter(slot, "@execution_generation", operation.ExecutionClaimGeneration);

            await using var reader = await slot.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                persistedSlotCluster = reader.GetString(0);
                persistedSlotNumber = reader.GetInt32(1);
                persistedSlotExpiry = DateTimeOffset.Parse(
                    reader.GetString(2),
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind);

                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    persistedSlotNumber = null;
                }
            }
        }

        if (persistedSlotNumber is null ||
            persistedSlotExpiry is null ||
            persistedSlotExpiry <= nowUtc ||
            !string.Equals(persistedSlotCluster, persisted.ClusterId, StringComparison.Ordinal))
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return new MutationLeaseRenewResult(
                MutationLeaseRenewOutcome.InvalidExecutionClaim,
                persisted);
        }

        await using (var updateOperation = connection.CreateCommand())
        {
            updateOperation.Transaction = transaction;
            updateOperation.CommandText =
                """
                UPDATE kafdeck_mutation_operations
                SET version = @new_version,
                    execution_claim_expires_at_utc = @execution_claim_expires_at_utc,
                    snapshot_json = @snapshot_json,
                    updated_at_utc = @updated_at_utc
                WHERE operation_id = @operation_id
                  AND version = @expected_version
                  AND state = @executing_state
                """;
            AddParameter(updateOperation, "@new_version", operation.Version);
            AddParameter(updateOperation, "@execution_claim_expires_at_utc", FormatTimestamp(requestedExpiry));
            AddParameter(updateOperation, "@snapshot_json", Serialize(operation));
            AddParameter(updateOperation, "@updated_at_utc", FormatTimestamp(operation.UpdatedAtUtc));
            AddParameter(updateOperation, "@operation_id", operation.OperationId.ToString("D"));
            AddParameter(updateOperation, "@expected_version", expectedVersion);
            AddParameter(updateOperation, "@executing_state", (int)MutationOperationState.Executing);

            if (await updateOperation.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new MutationLeaseRenewResult(
                    MutationLeaseRenewOutcome.VersionConflict,
                    persisted);
            }
        }

        await using (var updateClaims = connection.CreateCommand())
        {
            updateClaims.Transaction = transaction;
            updateClaims.CommandText =
                """
                UPDATE kafdeck_mutation_resource_claims
                SET expires_at_utc = @expires_at_utc
                WHERE operation_id = @operation_id
                  AND execution_generation = @execution_generation
                """;
            AddParameter(updateClaims, "@expires_at_utc", FormatTimestamp(requestedExpiry));
            AddParameter(updateClaims, "@operation_id", operation.OperationId.ToString("D"));
            AddParameter(updateClaims, "@execution_generation", operation.ExecutionClaimGeneration);

            var updatedClaims = await updateClaims.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (updatedClaims != expectedResources.Length)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new MutationLeaseRenewResult(
                    MutationLeaseRenewOutcome.InvalidExecutionClaim,
                    persisted);
            }
        }

        await using (var updateSlot = connection.CreateCommand())
        {
            updateSlot.Transaction = transaction;
            updateSlot.CommandText =
                """
                UPDATE kafdeck_mutation_cluster_slots
                SET expires_at_utc = @expires_at_utc
                WHERE cluster_id = @cluster_id
                  AND slot_number = @slot_number
                  AND operation_id = @operation_id
                  AND execution_generation = @execution_generation
                """;
            AddParameter(updateSlot, "@expires_at_utc", FormatTimestamp(requestedExpiry));
            AddParameter(updateSlot, "@cluster_id", persistedSlotCluster!);
            AddParameter(updateSlot, "@slot_number", persistedSlotNumber.Value);
            AddParameter(updateSlot, "@operation_id", operation.OperationId.ToString("D"));
            AddParameter(updateSlot, "@execution_generation", operation.ExecutionClaimGeneration);

            if (await updateSlot.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new MutationLeaseRenewResult(
                    MutationLeaseRenewOutcome.InvalidExecutionClaim,
                    persisted);
            }
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new MutationLeaseRenewResult(
            MutationLeaseRenewOutcome.Renewed,
            operation);
    }

    public async Task ReleaseClusterExecutionSlotAsync(
        Guid operationId,
        long executionClaimGeneration,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await _connectionFactory.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            DELETE FROM kafdeck_mutation_cluster_slots
            WHERE operation_id = @operation_id
              AND execution_generation = @execution_generation
            """;
        AddParameter(command, "@operation_id", operationId.ToString("D"));
        AddParameter(command, "@execution_generation", executionClaimGeneration);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
