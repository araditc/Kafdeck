using System.Diagnostics;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Globalization;
using Kafdeck.Core.Observability;
using Microsoft.Data.Sqlite;
using SQLitePCL;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoHistoricalMetricMaintenanceStore :
    IHistoricalMetricMaintenanceStore
{
    private const int SchemaVersion = 1;
    private const int SingletonId = 1;
    private const string Component =
        "historical-metrics-maintenance";
    private const string RollupSource =
        "kafdeck-rollup";
    internal const int MaxRawSamplesPerRollupBatch = 32;
    internal const int MaxRollupBatchesPerCycle = 24;

    private sealed record RawSampleRow(
        HistoricalMetricIdentity Identity,
        DateTimeOffset ObservedAtUtc,
        double Min,
        double Max,
        double Sum,
        long Count,
        string? State,
        DateTimeOffset FirstObservedAtUtc,
        DateTimeOffset LastObservedAtUtc);
    private const long MaintenanceMigrationLockKey =
        4_839_176_502_110_873_342L;

    private readonly IHistoricalMetricsDbConnectionFactory
        _connectionFactory;
    private readonly TimeProvider _timeProvider;

    public AdoHistoricalMetricMaintenanceStore(
        IHistoricalMetricsDbConnectionFactory connectionFactory,
        TimeProvider? timeProvider = null)
    {
        _connectionFactory =
            connectionFactory ??
            throw new ArgumentNullException(
                nameof(connectionFactory));
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
    }

    public async Task InitializeAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        if (_connectionFactory.SupportsSelectForUpdate)
        {
            await using var lockCommand =
                connection.CreateCommand();
            lockCommand.Transaction = transaction;
            lockCommand.CommandText =
                "SELECT pg_advisory_xact_lock(@lock_key)";
            AddParameter(
                lockCommand,
                "@lock_key",
                MaintenanceMigrationLockKey);
            await lockCommand
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        await ExecuteAsync(
                connection,
                transaction,
                """
                CREATE TABLE IF NOT EXISTS kafdeck_schema_info (
                    component TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                )
                """,
                cancellationToken)
            .ConfigureAwait(false);

        var existingVersion =
            await ReadSchemaVersionAsync(
                    connection,
                    transaction,
                    cancellationToken)
                .ConfigureAwait(false);

        if (existingVersion is not null &&
            existingVersion.Value != SchemaVersion)
        {
            throw new InvalidOperationException(
                $"Historical metric maintenance schema version {existingVersion.Value} is unsupported by this binary (expected {SchemaVersion}).");
        }

        await ExecuteAsync(
                connection,
                transaction,
                """
                CREATE TABLE IF NOT EXISTS kafdeck_historical_metric_maintenance (
                    singleton_id INTEGER PRIMARY KEY,
                    lease_owner TEXT NULL,
                    lease_expires_at_utc TEXT NULL,
                    fencing_token BIGINT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await ExecuteAsync(
                connection,
                transaction,
                """
                CREATE TABLE IF NOT EXISTS kafdeck_historical_metric_raw_identities (
                    metric_name TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    resource_kind TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL,
                    PRIMARY KEY (
                        metric_name,
                        cluster_id,
                        resource_kind,
                        resource_id,
                        observed_at_utc)
                )
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await ExecuteAsync(
                connection,
                transaction,
                """
                CREATE INDEX IF NOT EXISTS ix_kafdeck_history_raw_identity_expiry
                ON kafdeck_historical_metric_raw_identities (
                    expires_at_utc)
                """,
                cancellationToken)
            .ConfigureAwait(false);

        await using (var seed =
                     connection.CreateCommand())
        {
            seed.Transaction = transaction;
            seed.CommandText =
                """
                INSERT INTO kafdeck_historical_metric_maintenance (
                    singleton_id,
                    lease_owner,
                    lease_expires_at_utc,
                    fencing_token,
                    updated_at_utc)
                VALUES (
                    @singleton_id,
                    NULL,
                    NULL,
                    0,
                    @updated_at_utc)
                ON CONFLICT (singleton_id)
                DO NOTHING
                """;
            AddParameter(
                seed,
                "@singleton_id",
                SingletonId);
            AddParameter(
                seed,
                "@updated_at_utc",
                DateTimeOffset.UnixEpoch
                    .ToString("O"));
            await seed
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        if (existingVersion is null)
        {
            await using var versionInsert =
                connection.CreateCommand();
            versionInsert.Transaction = transaction;
            versionInsert.CommandText =
                """
                INSERT INTO kafdeck_schema_info (
                    component,
                    schema_version)
                VALUES (
                    @component,
                    @schema_version)
                """;
            AddParameter(
                versionInsert,
                "@component",
                Component);
            AddParameter(
                versionInsert,
                "@schema_version",
                SchemaVersion);
            await versionInsert
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<HistoricalMetricMaintenanceLease?>
        TryAcquireLeaseAsync(
            string ownerId,
            DateTimeOffset nowUtc,
            TimeSpan leaseDuration,
            CancellationToken cancellationToken = default)
    {
        ValidateOwner(ownerId);

        if (nowUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(nowUtc));
        }

        if (leaseDuration <= TimeSpan.Zero ||
            leaseDuration >
            TimeSpan.FromMinutes(10))
        {
            throw new ArgumentOutOfRangeException(
                nameof(leaseDuration));
        }

        ownerId = ownerId.Trim();
        nowUtc = nowUtc.ToUniversalTime();

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        var current =
            await LockAndReadLeaseAsync(
                    connection,
                    transaction,
                    cancellationToken)
                .ConfigureAwait(false);

        if (current.OwnerId is not null &&
            current.ExpiresAtUtc is not null &&
            current.ExpiresAtUtc.Value > nowUtc &&
            !string.Equals(
                current.OwnerId,
                ownerId,
                StringComparison.Ordinal))
        {
            await transaction
                .CommitAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        var nextToken =
            checked(current.FencingToken + 1);
        var expiresAtUtc =
            nowUtc.Add(leaseDuration);

        await using var update =
            connection.CreateCommand();
        update.Transaction = transaction;
        update.CommandText =
            """
            UPDATE kafdeck_historical_metric_maintenance
            SET
                lease_owner = @lease_owner,
                lease_expires_at_utc = @lease_expires_at_utc,
                fencing_token = @fencing_token,
                updated_at_utc = @updated_at_utc
            WHERE singleton_id = @singleton_id
            """;
        AddParameter(
            update,
            "@lease_owner",
            ownerId);
        AddParameter(
            update,
            "@lease_expires_at_utc",
            expiresAtUtc.ToString("O"));
        AddParameter(
            update,
            "@fencing_token",
            nextToken);
        AddParameter(
            update,
            "@updated_at_utc",
            nowUtc.ToString("O"));
        AddParameter(
            update,
            "@singleton_id",
            SingletonId);

        if (await update
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false) != 1)
        {
            throw new InvalidOperationException(
                "Historical metric maintenance lease row is missing.");
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new HistoricalMetricMaintenanceLease(
            ownerId,
            nextToken,
            expiresAtUtc);
    }

    public async Task<HistoricalMetricMaintenanceResult>
        RunCycleAsync(
            HistoricalMetricMaintenanceLease lease,
            DateTimeOffset nowUtc,
            HistoricalMetricMaintenancePolicy policy,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(policy);
        policy.Validate();
        ValidateOwner(lease.OwnerId);

        if (lease.FencingToken <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(lease));
        }

        if (nowUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(nowUtc));
        }

        nowUtc = nowUtc.ToUniversalTime();
        var rawCutoffUtc =
            nowUtc.Subtract(policy.RawRetention);
        var rollupCutoffUtc =
            nowUtc.Subtract(policy.RollupRetention);

        using var deadline =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        deadline.CancelAfter(
            policy.MaxCycleDuration);
        var cycleToken =
            deadline.Token;

        try
        {
            await using var connection =
                await _connectionFactory
                    .OpenAsync(cycleToken)
                    .ConfigureAwait(false);
            using var providerCancellation =
                RegisterProviderCancellation(
                    connection,
                    cycleToken);

            var rolledWindows = 0;
            var rollupBatches = 0;
            var completedWindows =
                new HashSet<DateTimeOffset>();
            long rawDeleted = 0;

            while (rolledWindows <
                       policy.MaxRollupWindowsPerCycle &&
                   rollupBatches <
                       MaxRollupBatchesPerCycle)
            {
                await using var rollupTransaction =
                    await BeginMaintenanceTransactionAsync(
                            connection,
                            cycleToken)
                        .ConfigureAwait(false);

                var current =
                    await LockAndReadLeaseAsync(
                            connection,
                            rollupTransaction,
                            cycleToken)
                        .ConfigureAwait(false);

                var leaseCheckUtc =
                    _timeProvider.GetUtcNow();

                if (!LeaseIsValid(
                        current,
                        lease,
                        leaseCheckUtc))
                {
                    await rollupTransaction
                        .CommitAsync(cycleToken)
                        .ConfigureAwait(false);

                    return new HistoricalMetricMaintenanceResult(
                        LeaseValid: false,
                        RolledWindows: rolledWindows,
                        RawDeleted: rawDeleted,
                        RollupDeleted: 0);
                }

                var oldestRaw =
                    await ReadOldestEligibleRawTimestampAsync(
                            connection,
                            rollupTransaction,
                            rawCutoffUtc,
                            cycleToken)
                        .ConfigureAwait(false);

                if (oldestRaw is null)
                {
                    await rollupTransaction
                        .CommitAsync(cycleToken)
                        .ConfigureAwait(false);
                    break;
                }

                var windowStartUtc =
                    FloorToWindow(
                        oldestRaw.Value,
                        policy.RollupResolutionSeconds);
                var windowEndUtc =
                    windowStartUtc.AddSeconds(
                        policy.RollupResolutionSeconds);

                if (windowEndUtc > rawCutoffUtc)
                {
                    await rollupTransaction
                        .CommitAsync(cycleToken)
                        .ConfigureAwait(false);
                    break;
                }

                var processed =
                    await ProcessRawRollupBatchAsync(
                            connection,
                            rollupTransaction,
                            windowStartUtc,
                            windowEndUtc,
                            policy.RollupResolutionSeconds,
                            MaxRawSamplesPerRollupBatch,
                            cycleToken)
                        .ConfigureAwait(false);

                var windowHasRemainingRows =
                    await HasRawRowsInWindowAsync(
                            connection,
                            rollupTransaction,
                            windowStartUtc,
                            windowEndUtc,
                            cycleToken)
                        .ConfigureAwait(false);

                await rollupTransaction
                    .CommitAsync(cycleToken)
                    .ConfigureAwait(false);

                rawDeleted += processed;
                rollupBatches++;

                if (!windowHasRemainingRows &&
                    completedWindows.Add(
                        windowStartUtc))
                {
                    rolledWindows =
                        completedWindows.Count;
                }
            }

            long rollupDeleted = 0;
            await using (var retentionTransaction =
                         await BeginMaintenanceTransactionAsync(
                                 connection,
                                 cycleToken)
                             .ConfigureAwait(false))
            {
                var current =
                    await LockAndReadLeaseAsync(
                            connection,
                            retentionTransaction,
                            cycleToken)
                        .ConfigureAwait(false);

                var leaseCheckUtc =
                    _timeProvider.GetUtcNow();

                if (!LeaseIsValid(
                        current,
                        lease,
                        leaseCheckUtc))
                {
                    await retentionTransaction
                        .CommitAsync(cycleToken)
                        .ConfigureAwait(false);

                    return new HistoricalMetricMaintenanceResult(
                        LeaseValid: false,
                        RolledWindows: rolledWindows,
                        RawDeleted: rawDeleted,
                        RollupDeleted: 0);
                }

                rollupDeleted =
                    await DeleteExpiredRollupsAsync(
                            connection,
                            retentionTransaction,
                            rollupCutoffUtc,
                            policy.MaxRollupDeletesPerCycle,
                            cycleToken)
                        .ConfigureAwait(false);

                _ = await DeleteExpiredRawIdentitiesAsync(
                        connection,
                        retentionTransaction,
                        _timeProvider.GetUtcNow(),
                        policy.MaxRollupDeletesPerCycle,
                        cycleToken)
                    .ConfigureAwait(false);

                await retentionTransaction
                    .CommitAsync(cycleToken)
                    .ConfigureAwait(false);
            }

            return new HistoricalMetricMaintenanceResult(
                LeaseValid: true,
                RolledWindows: rolledWindows,
                RawDeleted: rawDeleted,
                RollupDeleted: rollupDeleted);
        }
        catch (SqliteException exception)
            when (exception.SqliteErrorCode ==
                  raw.SQLITE_INTERRUPT &&
                  deadline.IsCancellationRequested)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(
                    "Historical metric maintenance was cancelled.",
                    exception,
                    cancellationToken);
            }

            throw new TimeoutException(
                "Historical metric maintenance exceeded the configured cycle duration.",
                exception);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested &&
                  deadline.IsCancellationRequested)
        {
            throw new TimeoutException(
                "Historical metric maintenance exceeded the configured cycle duration.");
        }
    }

    internal ValueTask<DbTransaction>
        BeginMaintenanceTransactionAsync(
            DbConnection connection,
            CancellationToken cancellationToken) =>
        connection.BeginTransactionAsync(
            _connectionFactory.SupportsSelectForUpdate
                ? IsolationLevel.RepeatableRead
                : IsolationLevel.Serializable,
            cancellationToken);

    private static bool LeaseIsValid(
        LeaseRow current,
        HistoricalMetricMaintenanceLease lease,
        DateTimeOffset nowUtc) =>
        string.Equals(
            current.OwnerId,
            lease.OwnerId,
            StringComparison.Ordinal) &&
        current.FencingToken ==
        lease.FencingToken &&
        current.ExpiresAtUtc is not null &&
        current.ExpiresAtUtc.Value > nowUtc;

    private static CancellationTokenRegistration
        RegisterProviderCancellation(
            DbConnection connection,
            CancellationToken cancellationToken)
    {
        if (connection is not SqliteConnection sqliteConnection ||
            !cancellationToken.CanBeCanceled)
        {
            return default;
        }

        return cancellationToken.UnsafeRegister(
            static state =>
            {
                var sqlite =
                    (SqliteConnection)state!;
                raw.sqlite3_interrupt(
                    sqlite.Handle);
            },
            sqliteConnection);
    }

    private async Task<LeaseRow> LockAndReadLeaseAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        if (!_connectionFactory.SupportsSelectForUpdate)
        {
            await using var lockCommand =
                connection.CreateCommand();
            lockCommand.Transaction = transaction;
            lockCommand.CommandText =
                """
                UPDATE kafdeck_historical_metric_maintenance
                SET updated_at_utc = updated_at_utc
                WHERE singleton_id = @singleton_id
                """;
            AddParameter(
                lockCommand,
                "@singleton_id",
                SingletonId);
            await lockCommand
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            _connectionFactory.SupportsSelectForUpdate
                ? """
                  SELECT
                      lease_owner,
                      lease_expires_at_utc,
                      fencing_token
                  FROM kafdeck_historical_metric_maintenance
                  WHERE singleton_id = @singleton_id
                  FOR UPDATE
                  """
                : """
                  SELECT
                      lease_owner,
                      lease_expires_at_utc,
                      fencing_token
                  FROM kafdeck_historical_metric_maintenance
                  WHERE singleton_id = @singleton_id
                  """;
        AddParameter(
            command,
            "@singleton_id",
            SingletonId);

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        if (!await reader
                .ReadAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                "Historical metric maintenance lease row is missing.");
        }

        var owner =
            reader.IsDBNull(0)
                ? null
                : reader.GetString(0);
        DateTimeOffset? expires = null;
        if (!reader.IsDBNull(1))
        {
            expires =
                DateTimeOffset.Parse(
                    reader.GetString(1),
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind);
        }

        return new LeaseRow(
            owner,
            expires,
            Convert.ToInt64(
                reader.GetValue(2),
                CultureInfo.InvariantCulture));
    }

    private static async Task<DateTimeOffset?>
        ReadOldestEligibleRawTimestampAsync(
            DbConnection connection,
            DbTransaction transaction,
            DateTimeOffset rawCutoffUtc,
            CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT observed_at_utc
            FROM kafdeck_historical_metric_samples
            WHERE resolution_seconds = 0
              AND observed_at_utc < @raw_cutoff_utc
            ORDER BY observed_at_utc
            LIMIT 1
            """;
        AddParameter(
            command,
            "@raw_cutoff_utc",
            rawCutoffUtc.ToString("O"));

        var value =
            await command
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return value is null ||
               value is DBNull
            ? null
            : DateTimeOffset.Parse(
                Convert.ToString(
                    value,
                    CultureInfo.InvariantCulture)!,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind);
    }

    private static async Task<bool> HasRawRowsInWindowAsync(
        DbConnection connection,
        DbTransaction transaction,
        DateTimeOffset windowStartUtc,
        DateTimeOffset windowEndUtc,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT 1
            FROM kafdeck_historical_metric_samples
            WHERE resolution_seconds = 0
              AND observed_at_utc >= @window_start_utc
              AND observed_at_utc < @window_end_utc
            LIMIT 1
            """;
        AddParameter(
            command,
            "@window_start_utc",
            windowStartUtc.ToString("O"));
        AddParameter(
            command,
            "@window_end_utc",
            windowEndUtc.ToString("O"));

        return await command
            .ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false) is not null;
    }

    private static async Task<int> ProcessRawRollupBatchAsync(
        DbConnection connection,
        DbTransaction transaction,
        DateTimeOffset windowStartUtc,
        DateTimeOffset windowEndUtc,
        int resolutionSeconds,
        int maxRawSamples,
        CancellationToken cancellationToken)
    {
        if (maxRawSamples is < 1 or > 10_000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxRawSamples));
        }

        var rows =
            await ReadRawBatchAsync(
                    connection,
                    transaction,
                    windowStartUtc,
                    windowEndUtc,
                    maxRawSamples,
                    cancellationToken)
                .ConfigureAwait(false);

        if (rows.Count == 0)
        {
            return 0;
        }

        foreach (var group in rows.GroupBy(
                     row => row.Identity))
        {
            var min =
                group.Min(row => row.Min);
            var max =
                group.Max(row => row.Max);
            var sum =
                group.Sum(row => row.Sum);
            var count =
                group.Sum(row => row.Count);
            var states =
                group
                    .Select(row => row.State)
                    .Distinct(StringComparer.Ordinal)
                    .ToArray();
            var state =
                states.Length == 1
                    ? states[0]
                    : "Partial";
            var firstObservedAtUtc =
                group.Min(
                    row =>
                        row.FirstObservedAtUtc);
            var lastObservedAtUtc =
                group.Max(
                    row =>
                        row.LastObservedAtUtc);

            await UpsertRollupAsync(
                    connection,
                    transaction,
                    new HistoricalMetricSample(
                        group.Key,
                        windowStartUtc,
                        min,
                        max,
                        sum,
                        count,
                        resolutionSeconds,
                        RollupSource,
                        state,
                        firstObservedAtUtc,
                        lastObservedAtUtc),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var deleted =
            await DeleteRawBatchAsync(
                    connection,
                    transaction,
                    rows,
                    cancellationToken)
                .ConfigureAwait(false);

        if (deleted != rows.Count)
        {
            throw new InvalidOperationException(
                "Historical metric raw rollup rows changed during a fenced maintenance batch.");
        }

        return rows.Count;
    }

    private static async Task<IReadOnlyList<RawSampleRow>>
        ReadRawBatchAsync(
            DbConnection connection,
            DbTransaction transaction,
            DateTimeOffset windowStartUtc,
            DateTimeOffset windowEndUtc,
            int maxRawSamples,
            CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT
                metric_name,
                cluster_id,
                resource_kind,
                resource_id,
                observed_at_utc,
                min_value,
                max_value,
                sum_value,
                sample_count,
                state,
                first_observed_at_utc,
                last_observed_at_utc
            FROM kafdeck_historical_metric_samples
            WHERE resolution_seconds = 0
              AND observed_at_utc >= @window_start_utc
              AND observed_at_utc < @window_end_utc
            ORDER BY
                observed_at_utc,
                metric_name,
                cluster_id,
                resource_kind,
                resource_id
            LIMIT @raw_limit
            """;
        AddParameter(
            command,
            "@window_start_utc",
            windowStartUtc.ToString("O"));
        AddParameter(
            command,
            "@window_end_utc",
            windowEndUtc.ToString("O"));
        AddParameter(
            command,
            "@raw_limit",
            maxRawSamples);

        var rows =
            new List<RawSampleRow>(
                maxRawSamples);
        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        while (await reader
                   .ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            var observedAtUtc =
                DateTimeOffset.Parse(
                    reader.GetString(4),
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind);
            rows.Add(
                new RawSampleRow(
                    new HistoricalMetricIdentity(
                        reader.GetString(0),
                        reader.GetString(1),
                        reader.GetString(2),
                        reader.GetString(3)),
                    observedAtUtc,
                    Convert.ToDouble(
                        reader.GetValue(5),
                        CultureInfo.InvariantCulture),
                    Convert.ToDouble(
                        reader.GetValue(6),
                        CultureInfo.InvariantCulture),
                    Convert.ToDouble(
                        reader.GetValue(7),
                        CultureInfo.InvariantCulture),
                    Convert.ToInt64(
                        reader.GetValue(8),
                        CultureInfo.InvariantCulture),
                    reader.IsDBNull(9)
                        ? null
                        : reader.GetString(9),
                    reader.IsDBNull(10)
                        ? observedAtUtc
                        : DateTimeOffset.Parse(
                            reader.GetString(10),
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind),
                    reader.IsDBNull(11)
                        ? observedAtUtc
                        : DateTimeOffset.Parse(
                            reader.GetString(11),
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind)));
        }

        return rows;
    }

    private static async Task UpsertRollupAsync(
        DbConnection connection,
        DbTransaction transaction,
        HistoricalMetricSample sample,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            INSERT INTO kafdeck_historical_metric_samples (
                metric_name,
                cluster_id,
                resource_kind,
                resource_id,
                observed_at_utc,
                resolution_seconds,
                min_value,
                max_value,
                sum_value,
                sample_count,
                source,
                state,
                first_observed_at_utc,
                last_observed_at_utc)
            VALUES (
                @metric_name,
                @cluster_id,
                @resource_kind,
                @resource_id,
                @observed_at_utc,
                @resolution_seconds,
                @min_value,
                @max_value,
                @sum_value,
                @sample_count,
                @source,
                @state,
                @first_observed_at_utc,
                @last_observed_at_utc)
            ON CONFLICT (
                metric_name,
                cluster_id,
                resource_kind,
                resource_id,
                observed_at_utc,
                resolution_seconds)
            DO UPDATE SET
                min_value = CASE
                    WHEN excluded.min_value <
                         min_value
                    THEN excluded.min_value
                    ELSE min_value
                END,
                max_value = CASE
                    WHEN excluded.max_value >
                         max_value
                    THEN excluded.max_value
                    ELSE max_value
                END,
                sum_value =
                    sum_value +
                    excluded.sum_value,
                sample_count =
                    sample_count +
                    excluded.sample_count,
                source = excluded.source,
                state = CASE
                    WHEN state = excluded.state
                      OR (state IS NULL AND
                          excluded.state IS NULL)
                    THEN state
                    ELSE 'Partial'
                END,
                first_observed_at_utc = CASE
                    WHEN first_observed_at_utc IS NULL
                    THEN NULL
                    WHEN excluded.first_observed_at_utc <
                         first_observed_at_utc
                    THEN excluded.first_observed_at_utc
                    ELSE first_observed_at_utc
                END,
                last_observed_at_utc = CASE
                    WHEN last_observed_at_utc IS NULL
                    THEN NULL
                    WHEN excluded.last_observed_at_utc >
                         last_observed_at_utc
                    THEN excluded.last_observed_at_utc
                    ELSE last_observed_at_utc
                END
            """;
        AddParameter(command, "@metric_name", sample.Identity.MetricName);
        AddParameter(command, "@cluster_id", sample.Identity.ClusterId);
        AddParameter(command, "@resource_kind", sample.Identity.ResourceKind);
        AddParameter(command, "@resource_id", sample.Identity.ResourceId);
        AddParameter(
            command,
            "@observed_at_utc",
            sample.ObservedAtUtc.ToUniversalTime().ToString("O"));
        AddParameter(command, "@resolution_seconds", sample.ResolutionSeconds);
        AddParameter(command, "@min_value", sample.Min);
        AddParameter(command, "@max_value", sample.Max);
        AddParameter(command, "@sum_value", sample.Sum);
        AddParameter(command, "@sample_count", sample.Count);
        AddParameter(command, "@source", sample.Source);
        AddParameter(
            command,
            "@state",
            sample.State is null
                ? DBNull.Value
                : sample.State);
        AddParameter(
            command,
            "@first_observed_at_utc",
            sample.FirstObservedAtUtc!.Value
                .ToUniversalTime()
                .ToString("O"));
        AddParameter(
            command,
            "@last_observed_at_utc",
            sample.LastObservedAtUtc!.Value
                .ToUniversalTime()
                .ToString("O"));

        await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<int> DeleteRawBatchAsync(
        DbConnection connection,
        DbTransaction transaction,
        IReadOnlyList<RawSampleRow> rows,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;

        var predicates =
            new List<string>(
                rows.Count);
        for (var index = 0;
             index < rows.Count;
             index++)
        {
            var row =
                rows[index];
            var suffix =
                index.ToString(
                    CultureInfo.InvariantCulture);
            predicates.Add(
                $"(metric_name = @metric_name_{suffix} AND cluster_id = @cluster_id_{suffix} AND resource_kind = @resource_kind_{suffix} AND resource_id = @resource_id_{suffix} AND observed_at_utc = @observed_at_utc_{suffix})");
            AddParameter(command, $"@metric_name_{suffix}", row.Identity.MetricName);
            AddParameter(command, $"@cluster_id_{suffix}", row.Identity.ClusterId);
            AddParameter(command, $"@resource_kind_{suffix}", row.Identity.ResourceKind);
            AddParameter(command, $"@resource_id_{suffix}", row.Identity.ResourceId);
            AddParameter(
                command,
                $"@observed_at_utc_{suffix}",
                row.ObservedAtUtc.ToUniversalTime().ToString("O"));
        }

        command.CommandText =
            $"""
             DELETE FROM kafdeck_historical_metric_samples
             WHERE resolution_seconds = 0
               AND ({string.Join(" OR ", predicates)})
             """;

        return await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<long> DeleteExpiredRollupsAsync(
        DbConnection connection,
        DbTransaction transaction,
        DateTimeOffset rollupCutoffUtc,
        int maxDeletes,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            DELETE FROM kafdeck_historical_metric_samples
            WHERE (
                metric_name,
                cluster_id,
                resource_kind,
                resource_id,
                observed_at_utc,
                resolution_seconds)
            IN (
                SELECT
                    metric_name,
                    cluster_id,
                    resource_kind,
                    resource_id,
                    observed_at_utc,
                    resolution_seconds
                FROM kafdeck_historical_metric_samples
                WHERE resolution_seconds > 0
                  AND observed_at_utc < @rollup_cutoff_utc
                ORDER BY
                    observed_at_utc,
                    metric_name,
                    cluster_id,
                    resource_kind,
                    resource_id,
                    resolution_seconds
                LIMIT @delete_limit
            )
            """;
        AddParameter(
            command,
            "@rollup_cutoff_utc",
            rollupCutoffUtc.ToString("O"));
        AddParameter(
            command,
            "@delete_limit",
            maxDeletes);

        return await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<long> DeleteExpiredRawIdentitiesAsync(
        DbConnection connection,
        DbTransaction transaction,
        DateTimeOffset nowUtc,
        int maxDeletes,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            DELETE FROM kafdeck_historical_metric_raw_identities
            WHERE (
                metric_name,
                cluster_id,
                resource_kind,
                resource_id,
                observed_at_utc)
            IN (
                SELECT
                    identity.metric_name,
                    identity.cluster_id,
                    identity.resource_kind,
                    identity.resource_id,
                    identity.observed_at_utc
                FROM kafdeck_historical_metric_raw_identities identity
                WHERE identity.expires_at_utc < @now_utc
                  AND NOT EXISTS (
                      SELECT 1
                      FROM kafdeck_historical_metric_samples sample
                      WHERE sample.metric_name = identity.metric_name
                        AND sample.cluster_id = identity.cluster_id
                        AND sample.resource_kind = identity.resource_kind
                        AND sample.resource_id = identity.resource_id
                        AND sample.observed_at_utc = identity.observed_at_utc
                        AND sample.resolution_seconds = 0)
                ORDER BY identity.expires_at_utc
                LIMIT @delete_limit
            )
            """;
        AddParameter(
            command,
            "@now_utc",
            nowUtc.ToUniversalTime().ToString("O"));
        AddParameter(
            command,
            "@delete_limit",
            maxDeletes);

        return await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static DateTimeOffset FloorToWindow(
        DateTimeOffset value,
        int resolutionSeconds)
    {
        var unixSeconds =
            value.ToUniversalTime()
                .ToUnixTimeSeconds();
        var remainder =
            unixSeconds %
            resolutionSeconds;
        if (remainder < 0)
        {
            remainder +=
                resolutionSeconds;
        }

        return DateTimeOffset
            .FromUnixTimeSeconds(
                unixSeconds - remainder);
    }

    private static async Task ExecuteAsync(
        DbConnection connection,
        DbTransaction transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<int?> ReadSchemaVersionAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_schema_info
            WHERE component = @component
            """;
        AddParameter(
            command,
            "@component",
            Component);

        var value =
            await command
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return value is null ||
               value is DBNull
            ? null
            : Convert.ToInt32(
                value,
                CultureInfo.InvariantCulture);
    }

    private static void ValidateOwner(
        string ownerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            ownerId);

        if (!string.Equals(
                ownerId,
                ownerId.Trim(),
                StringComparison.Ordinal) ||
            ownerId.Length > 256 ||
            ownerId.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Historical metric maintenance owner must be trimmed, contain no control characters, and not exceed 256 characters.",
                nameof(ownerId));
        }
    }

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

    private sealed record LeaseRow(
        string? OwnerId,
        DateTimeOffset? ExpiresAtUtc,
        long FencingToken);
}
