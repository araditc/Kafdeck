using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Modules.Connect;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoConnectAutoRestartStateStore :
    IConnectAutoRestartStateStore
{
    private const int SchemaVersion = 1;
    private const string Component =
        "connect-auto-restart-state";
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly IMutationDbConnectionFactory _connectionFactory;

    public AdoConnectAutoRestartStateStore(
        IMutationDbConnectionFactory connectionFactory)
    {
        _connectionFactory =
            connectionFactory ??
            throw new ArgumentNullException(
                nameof(connectionFactory));
    }

    public async Task InitializeAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);

        string[] statements =
        [
            """
            CREATE TABLE IF NOT EXISTS kafdeck_schema_info (
                component TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL
            )
            """,
            """
            INSERT INTO kafdeck_schema_info (
                component,
                schema_version)
            VALUES (
                'connect-auto-restart-state',
                1)
            ON CONFLICT (component) DO NOTHING
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_connect_auto_restart_activations (
                activation_id TEXT PRIMARY KEY,
                cluster_id TEXT NOT NULL,
                connect_profile_id TEXT NOT NULL,
                connector_name TEXT NOT NULL,
                task_id INTEGER NULL,
                circuit_state INTEGER NOT NULL,
                version BIGINT NOT NULL,
                lease_owner TEXT NULL,
                lease_generation BIGINT NOT NULL DEFAULT 0,
                lease_expires_at_utc TEXT NULL,
                snapshot_json TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_connect_auto_restart_profile_slots (
                cluster_id TEXT NOT NULL,
                connect_profile_id TEXT NOT NULL,
                slot_number INTEGER NOT NULL,
                activation_id TEXT NOT NULL UNIQUE,
                PRIMARY KEY (
                    cluster_id,
                    connect_profile_id,
                    slot_number)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS kafdeck_connect_auto_restart_target_claims (
                target_key TEXT PRIMARY KEY,
                activation_id TEXT NOT NULL UNIQUE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_connect_auto_restart_active
            ON kafdeck_connect_auto_restart_activations (
                cluster_id,
                connect_profile_id,
                circuit_state,
                updated_at_utc)
            """,
        ];

        foreach (var statement in statements)
        {
            await using var command =
                connection.CreateCommand();
            command.CommandText = statement;
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        await using var versionCommand =
            connection.CreateCommand();
        versionCommand.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_schema_info
            WHERE component = @component
            """;
        AddParameter(
            versionCommand,
            "@component",
            Component);

        var value = await versionCommand
            .ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false);

        if (value is null ||
            Convert.ToInt32(
                value,
                CultureInfo.InvariantCulture) != SchemaVersion)
        {
            throw new InvalidOperationException(
                "Connect auto-restart persistence schema version is unsupported.");
        }
    }

    public async Task<ConnectAutoRestartActivation?> GetAsync(
        Guid activationId,
        CancellationToken cancellationToken = default)
    {
        if (activationId == Guid.Empty)
        {
            throw new ArgumentOutOfRangeException(
                nameof(activationId));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();

        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_connect_auto_restart_activations
            WHERE activation_id = @activation_id
            """;
        AddParameter(
            command,
            "@activation_id",
            activationId.ToString("D"));

        var value = await command
            .ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false);

        return value is null or DBNull
            ? null
            : Deserialize(
                Convert.ToString(
                    value,
                    CultureInfo.InvariantCulture)!);
    }

    public async Task<bool> TryCreateAsync(
        ConnectAutoRestartActivation activation,
        int aggregateProfileLimit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(activation);

        if (aggregateProfileLimit is < 1 or
            > ConnectAutoRestartPolicy
                .HardMaxActivePoliciesPerProfile)
        {
            throw new ArgumentOutOfRangeException(
                nameof(aggregateProfileLimit));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        if (!activation.ReleasesActiveClaim)
        {
            if (!await TryClaimTargetAsync(
                    connection,
                    transaction,
                    activation,
                    cancellationToken)
                .ConfigureAwait(false))
            {
                await transaction
                    .RollbackAsync(cancellationToken)
                    .ConfigureAwait(false);
                return false;
            }

            if (!await TryClaimProfileSlotAsync(
                    connection,
                    transaction,
                    activation,
                    aggregateProfileLimit,
                    cancellationToken)
                .ConfigureAwait(false))
            {
                await transaction
                    .RollbackAsync(cancellationToken)
                    .ConfigureAwait(false);
                return false;
            }
        }

        await using var insert =
            connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText =
            """
            INSERT INTO kafdeck_connect_auto_restart_activations (
                activation_id,
                cluster_id,
                connect_profile_id,
                connector_name,
                task_id,
                circuit_state,
                version,
                snapshot_json,
                updated_at_utc)
            VALUES (
                @activation_id,
                @cluster_id,
                @connect_profile_id,
                @connector_name,
                @task_id,
                @circuit_state,
                @version,
                @snapshot_json,
                @updated_at_utc)
            ON CONFLICT (activation_id) DO NOTHING
            """;

        BindActivation(
            insert,
            activation);

        var inserted = await insert
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);

        if (inserted != 1)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return false;
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);
        return true;
    }

    public async Task<bool> TryUpdateAsync(
        ConnectAutoRestartActivation activation,
        long expectedVersion,
        ConnectAutoRestartLease lease,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(activation);
        ArgumentNullException.ThrowIfNull(lease);

        if (expectedVersion < 1 ||
            activation.Version != expectedVersion + 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedVersion));
        }

        if (lease.ActivationId != activation.ActivationId ||
            lease.Generation < 1 ||
            string.IsNullOrWhiteSpace(lease.OwnerId))
        {
            throw new ArgumentException(
                "Auto-restart lease does not match the activation.",
                nameof(lease));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        await using var update =
            connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText =
            """
            UPDATE kafdeck_connect_auto_restart_activations
            SET circuit_state = @circuit_state,
                version = @version,
                snapshot_json = @snapshot_json,
                updated_at_utc = @updated_at_utc
            WHERE activation_id = @activation_id
              AND version = @expected_version
              AND lease_owner = @lease_owner
              AND lease_generation = @lease_generation
              AND lease_expires_at_utc = @lease_expires_at_utc
              AND lease_expires_at_utc > @now_utc
            """;

        AddParameter(
            update,
            "@circuit_state",
            (int)activation.CircuitState);
        AddParameter(
            update,
            "@version",
            activation.Version);
        AddParameter(
            update,
            "@snapshot_json",
            Serialize(activation));
        AddParameter(
            update,
            "@updated_at_utc",
            DateTimeOffset.UtcNow.ToString("O"));
        AddParameter(
            update,
            "@activation_id",
            activation.ActivationId.ToString("D"));
        AddParameter(
            update,
            "@expected_version",
            expectedVersion);
        AddParameter(
            update,
            "@lease_owner",
            lease.OwnerId);
        AddParameter(
            update,
            "@lease_generation",
            lease.Generation);
        AddParameter(
            update,
            "@lease_expires_at_utc",
            lease.ExpiresAtUtc.ToString("O"));
        AddParameter(
            update,
            "@now_utc",
            now.ToString("O"));

        if (await update
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false) != 1)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return false;
        }

        if (activation.ReleasesActiveClaim)
        {
            await ReleaseClaimsAsync(
                    connection,
                    transaction,
                    activation.ActivationId,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);
        return true;
    }

    public async Task<ConnectAutoRestartLease?> TryAcquireLeaseAsync(
        Guid activationId,
        string ownerId,
        DateTimeOffset now,
        TimeSpan leaseTtl,
        CancellationToken cancellationToken = default)
    {
        if (activationId == Guid.Empty)
        {
            throw new ArgumentOutOfRangeException(nameof(activationId));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(ownerId);
        var normalizedOwner = ownerId.Trim();
        if (!string.Equals(ownerId, normalizedOwner, StringComparison.Ordinal) ||
            normalizedOwner.Length > 256 ||
            normalizedOwner.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Auto-restart lease owner is invalid.",
                nameof(ownerId));
        }

        if (leaseTtl < TimeSpan.FromSeconds(5) ||
            leaseTtl > TimeSpan.FromMinutes(5))
        {
            throw new ArgumentOutOfRangeException(nameof(leaseTtl));
        }

        var expiresAt = now.Add(leaseTtl);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        await using var update =
            connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText =
            """
            UPDATE kafdeck_connect_auto_restart_activations
            SET lease_owner = @lease_owner,
                lease_generation = lease_generation + 1,
                lease_expires_at_utc = @lease_expires_at_utc
            WHERE activation_id = @activation_id
              AND circuit_state IN (
                    @armed,
                    @waiting,
                    @dispatching,
                    @ambiguous)
              AND (
                    lease_owner IS NULL
                 OR lease_expires_at_utc IS NULL
                 OR lease_expires_at_utc <= @now_utc
                 OR lease_owner = @lease_owner)
            """;
        AddParameter(update, "@lease_owner", normalizedOwner);
        AddParameter(update, "@lease_expires_at_utc", expiresAt.ToString("O"));
        AddParameter(update, "@activation_id", activationId.ToString("D"));
        AddParameter(update, "@armed", (int)ConnectAutoRestartCircuitState.Armed);
        AddParameter(update, "@waiting", (int)ConnectAutoRestartCircuitState.Waiting);
        AddParameter(update, "@dispatching", (int)ConnectAutoRestartCircuitState.Dispatching);
        AddParameter(update, "@ambiguous", (int)ConnectAutoRestartCircuitState.Ambiguous);
        AddParameter(update, "@now_utc", now.ToString("O"));

        if (await update
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false) != 1)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        await using var select =
            connection.CreateCommand();
        select.Transaction = transaction;
        select.CommandText =
            """
            SELECT lease_generation
            FROM kafdeck_connect_auto_restart_activations
            WHERE activation_id = @activation_id
              AND lease_owner = @lease_owner
              AND lease_expires_at_utc = @lease_expires_at_utc
            """;
        AddParameter(select, "@activation_id", activationId.ToString("D"));
        AddParameter(select, "@lease_owner", normalizedOwner);
        AddParameter(select, "@lease_expires_at_utc", expiresAt.ToString("O"));

        var generationValue = await select
            .ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false);

        if (generationValue is null or DBNull)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        var generation = Convert.ToInt64(
            generationValue,
            CultureInfo.InvariantCulture);

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new ConnectAutoRestartLease(
            activationId,
            normalizedOwner,
            generation,
            expiresAt);
    }

    public async Task<bool> ReleaseLeaseAsync(
        ConnectAutoRestartLease lease,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            """
            UPDATE kafdeck_connect_auto_restart_activations
            SET lease_owner = NULL,
                lease_expires_at_utc = NULL
            WHERE activation_id = @activation_id
              AND lease_owner = @lease_owner
              AND lease_generation = @lease_generation
              AND lease_expires_at_utc = @lease_expires_at_utc
            """;
        AddParameter(command, "@activation_id", lease.ActivationId.ToString("D"));
        AddParameter(command, "@lease_owner", lease.OwnerId);
        AddParameter(command, "@lease_generation", lease.Generation);
        AddParameter(command, "@lease_expires_at_utc", lease.ExpiresAtUtc.ToString("O"));

        return await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false) == 1;
    }

    public async Task<IReadOnlyList<ConnectAutoRestartActivation>>
        ListActiveAsync(
            string clusterId,
            string connectProfileId,
            int limit,
            CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectProfileId);

        if (limit is < 1 or > 1000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(limit));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();

        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_connect_auto_restart_activations
            WHERE cluster_id = @cluster_id
              AND connect_profile_id = @connect_profile_id
              AND circuit_state IN (
                    @armed,
                    @waiting,
                    @dispatching,
                    @ambiguous)
            ORDER BY updated_at_utc, activation_id
            LIMIT @limit
            """;

        AddParameter(
            command,
            "@cluster_id",
            clusterId);
        AddParameter(
            command,
            "@connect_profile_id",
            connectProfileId);
        AddParameter(
            command,
            "@armed",
            (int)ConnectAutoRestartCircuitState.Armed);
        AddParameter(
            command,
            "@waiting",
            (int)ConnectAutoRestartCircuitState.Waiting);
        AddParameter(
            command,
            "@dispatching",
            (int)ConnectAutoRestartCircuitState.Dispatching);
        AddParameter(
            command,
            "@ambiguous",
            (int)ConnectAutoRestartCircuitState.Ambiguous);
        AddParameter(
            command,
            "@limit",
            limit);

        var result =
            new List<ConnectAutoRestartActivation>();

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        while (await reader
                   .ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            result.Add(
                Deserialize(reader.GetString(0)));
        }

        return result;
    }

    private static async Task<bool> TryClaimTargetAsync(
        DbConnection connection,
        DbTransaction transaction,
        ConnectAutoRestartActivation activation,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            INSERT INTO kafdeck_connect_auto_restart_target_claims (
                target_key,
                activation_id)
            VALUES (
                @target_key,
                @activation_id)
            ON CONFLICT (target_key) DO NOTHING
            """;
        AddParameter(
            command,
            "@target_key",
            activation.Target.CanonicalKey);
        AddParameter(
            command,
            "@activation_id",
            activation.ActivationId.ToString("D"));

        return await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false) == 1;
    }

    private static async Task<bool> TryClaimProfileSlotAsync(
        DbConnection connection,
        DbTransaction transaction,
        ConnectAutoRestartActivation activation,
        int aggregateProfileLimit,
        CancellationToken cancellationToken)
    {
        for (var slot = 1;
             slot <= aggregateProfileLimit;
             slot++)
        {
            await using var command =
                connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                """
                INSERT INTO kafdeck_connect_auto_restart_profile_slots (
                    cluster_id,
                    connect_profile_id,
                    slot_number,
                    activation_id)
                VALUES (
                    @cluster_id,
                    @connect_profile_id,
                    @slot_number,
                    @activation_id)
                ON CONFLICT (
                    cluster_id,
                    connect_profile_id,
                    slot_number)
                DO NOTHING
                """;

            AddParameter(
                command,
                "@cluster_id",
                activation.Target.ClusterId);
            AddParameter(
                command,
                "@connect_profile_id",
                activation.Target.ConnectProfileId);
            AddParameter(
                command,
                "@slot_number",
                slot);
            AddParameter(
                command,
                "@activation_id",
                activation.ActivationId.ToString("D"));

            if (await command
                    .ExecuteNonQueryAsync(cancellationToken)
                    .ConfigureAwait(false) == 1)
            {
                return true;
            }
        }

        return false;
    }

    private static async Task ReleaseClaimsAsync(
        DbConnection connection,
        DbTransaction transaction,
        Guid activationId,
        CancellationToken cancellationToken)
    {
        foreach (var table in new[]
                 {
                     "kafdeck_connect_auto_restart_profile_slots",
                     "kafdeck_connect_auto_restart_target_claims",
                 })
        {
            await using var command =
                connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                $"DELETE FROM {table} WHERE activation_id = @activation_id";
            AddParameter(
                command,
                "@activation_id",
                activationId.ToString("D"));
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static void BindActivation(
        DbCommand command,
        ConnectAutoRestartActivation activation)
    {
        AddParameter(
            command,
            "@activation_id",
            activation.ActivationId.ToString("D"));
        AddParameter(
            command,
            "@cluster_id",
            activation.Target.ClusterId);
        AddParameter(
            command,
            "@connect_profile_id",
            activation.Target.ConnectProfileId);
        AddParameter(
            command,
            "@connector_name",
            activation.Target.ConnectorName);
        AddParameter(
            command,
            "@task_id",
            activation.Target.TaskId.HasValue
                ? activation.Target.TaskId.Value
                : DBNull.Value);
        AddParameter(
            command,
            "@circuit_state",
            (int)activation.CircuitState);
        AddParameter(
            command,
            "@version",
            activation.Version);
        AddParameter(
            command,
            "@snapshot_json",
            Serialize(activation));
        AddParameter(
            command,
            "@updated_at_utc",
            DateTimeOffset.UtcNow.ToString("O"));
    }

    private static string Serialize(
        ConnectAutoRestartActivation activation)
    {
        var json = JsonSerializer.Serialize(
            activation,
            JsonOptions);

        if (json.Length > 128 * 1024)
        {
            throw new InvalidOperationException(
                "Connect auto-restart snapshot exceeded the durable bound.");
        }

        return json;
    }

    private static ConnectAutoRestartActivation Deserialize(
        string json) =>
        JsonSerializer.Deserialize<
            ConnectAutoRestartActivation>(
                json,
                JsonOptions)
        ?? throw new InvalidOperationException(
            "Connect auto-restart snapshot is invalid.");

    private static void AddParameter(
        DbCommand command,
        string name,
        object value)
    {
        var parameter =
            command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
