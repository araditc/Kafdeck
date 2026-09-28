using System.Data.Common;
using System.Globalization;
using Kafdeck.Core.Observability;
using Microsoft.Data.Sqlite;
using SQLitePCL;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoHistoricalMetricStore :
    IHistoricalMetricStore
{
    private const int SchemaVersion = 3;
    private const string Component =
        "historical-metrics";
    internal static readonly TimeSpan RawIdentityRetention =
        TimeSpan.FromDays(90);

    private readonly record struct RawSampleIdentity(
        string MetricName,
        string ClusterId,
        string ResourceKind,
        string ResourceId,
        string ObservedAtUtc);

    private readonly IHistoricalMetricsDbConnectionFactory
        _connectionFactory;
    private readonly HistoricalMetricStorePolicy _policy;
    private readonly SemaphoreSlim _querySlots;

    public AdoHistoricalMetricStore(
        IHistoricalMetricsDbConnectionFactory connectionFactory,
        HistoricalMetricStorePolicy policy)
    {
        _connectionFactory =
            connectionFactory ??
            throw new ArgumentNullException(
                nameof(connectionFactory));
        _policy =
            policy ??
            throw new ArgumentNullException(
                nameof(policy));
        _policy.Validate();
        _querySlots =
            new SemaphoreSlim(
                _policy.MaxConcurrentQueries,
                _policy.MaxConcurrentQueries);
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
                HistoricalMetricsMigrationLockKey);
            await lockCommand
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        await ExecuteInitializationStatementAsync(
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
            (existingVersion.Value < 1 ||
             existingVersion.Value > SchemaVersion))
        {
            throw new InvalidOperationException(
                $"Historical metrics schema version {existingVersion.Value} is unsupported by this binary (expected 1..{SchemaVersion}).");
        }

        string[] currentSchemaStatements =
        [
            """
            CREATE TABLE IF NOT EXISTS kafdeck_historical_metric_samples (
                metric_name TEXT NOT NULL,
                cluster_id TEXT NOT NULL,
                resource_kind TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL,
                resolution_seconds INTEGER NOT NULL,
                min_value DOUBLE PRECISION NOT NULL,
                max_value DOUBLE PRECISION NOT NULL,
                sum_value DOUBLE PRECISION NOT NULL,
                sample_count BIGINT NOT NULL,
                source TEXT NOT NULL,
                state TEXT NULL,
                first_observed_at_utc TEXT NULL,
                last_observed_at_utc TEXT NULL,
                exact_sum_significand TEXT NULL,
                exact_sum_exponent INTEGER NULL,
                exact_sample_count TEXT NULL,
                exact_state TEXT NULL,
                PRIMARY KEY (
                    metric_name,
                    cluster_id,
                    resource_kind,
                    resource_id,
                    observed_at_utc,
                    resolution_seconds)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_history_lookup
            ON kafdeck_historical_metric_samples (
                metric_name,
                cluster_id,
                resource_kind,
                observed_at_utc,
                resource_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_history_retention
            ON kafdeck_historical_metric_samples (
                resolution_seconds,
                observed_at_utc)
            """,
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
            """
            CREATE INDEX IF NOT EXISTS ix_kafdeck_history_raw_identity_expiry
            ON kafdeck_historical_metric_raw_identities (
                expires_at_utc)
            """,
        ];

        foreach (var statement in currentSchemaStatements)
        {
            await ExecuteInitializationStatementAsync(
                    connection,
                    transaction,
                    statement,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        if (existingVersion == 1)
        {
            string[] versionTwoMigrationStatements =
            [
                """
                ALTER TABLE kafdeck_historical_metric_samples
                ADD COLUMN first_observed_at_utc TEXT NULL
                """,
                """
                ALTER TABLE kafdeck_historical_metric_samples
                ADD COLUMN last_observed_at_utc TEXT NULL
                """,
                """
                UPDATE kafdeck_historical_metric_samples
                SET
                    first_observed_at_utc = observed_at_utc,
                    last_observed_at_utc = observed_at_utc
                WHERE resolution_seconds = 0
                  AND (
                      first_observed_at_utc IS NULL
                      OR last_observed_at_utc IS NULL)
                """,
                """
                UPDATE kafdeck_historical_metric_samples
                SET
                    first_observed_at_utc = NULL,
                    last_observed_at_utc = NULL,
                    state = CASE
                        WHEN state = 'Partial'
                        THEN 'Partial'
                        ELSE 'Unknown'
                    END
                WHERE resolution_seconds > 0
                """,
            ];

            foreach (var statement in versionTwoMigrationStatements)
            {
                await ExecuteInitializationStatementAsync(
                        connection,
                        transaction,
                        statement,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        if (existingVersion is 1 or 2)
        {
            string[] versionThreeMigrationStatements =
            [
                """
                ALTER TABLE kafdeck_historical_metric_samples
                ADD COLUMN exact_sum_significand TEXT NULL
                """,
                """
                ALTER TABLE kafdeck_historical_metric_samples
                ADD COLUMN exact_sum_exponent INTEGER NULL
                """,
                """
                ALTER TABLE kafdeck_historical_metric_samples
                ADD COLUMN exact_sample_count TEXT NULL
                """,
                """
                ALTER TABLE kafdeck_historical_metric_samples
                ADD COLUMN exact_state TEXT NULL
                """,
            ];

            foreach (var statement in versionThreeMigrationStatements)
            {
                await ExecuteInitializationStatementAsync(
                        connection,
                        transaction,
                        statement,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            await using var versionUpdate =
                connection.CreateCommand();
            versionUpdate.Transaction = transaction;
            versionUpdate.CommandText =
                """
                UPDATE kafdeck_schema_info
                SET schema_version = @schema_version
                WHERE component = @component
                """;
            AddParameter(
                versionUpdate,
                "@schema_version",
                SchemaVersion);
            AddParameter(
                versionUpdate,
                "@component",
                Component);
            await versionUpdate
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        else if (existingVersion is null)
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

        await BackfillRawIdentitiesAsync(
                connection,
                transaction,
                DateTimeOffset.UtcNow.Add(
                    RawIdentityRetention),
                cancellationToken)
            .ConfigureAwait(false);

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private const long HistoricalMetricsMigrationLockKey =
        4_839_176_502_110_873_341L;

    private static async Task ExecuteInitializationStatementAsync(
        DbConnection connection,
        DbTransaction transaction,
        string statement,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = statement;
        await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task BackfillRawIdentitiesAsync(
        DbConnection connection,
        DbTransaction transaction,
        DateTimeOffset expiresAtUtc,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            INSERT INTO kafdeck_historical_metric_raw_identities (
                metric_name,
                cluster_id,
                resource_kind,
                resource_id,
                observed_at_utc,
                expires_at_utc)
            SELECT
                metric_name,
                cluster_id,
                resource_kind,
                resource_id,
                observed_at_utc,
                @expires_at_utc
            FROM kafdeck_historical_metric_samples
            WHERE resolution_seconds = 0
            ON CONFLICT (
                metric_name,
                cluster_id,
                resource_kind,
                resource_id,
                observed_at_utc)
            DO NOTHING
            """;
        AddParameter(
            command,
            "@expires_at_utc",
            expiresAtUtc.ToUniversalTime().ToString("O"));
        await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<int?> ReadSchemaVersionAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var versionCommand =
            connection.CreateCommand();
        versionCommand.Transaction = transaction;
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

        var value =
            await versionCommand
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return value is null ||
               value is DBNull
            ? null
            : Convert.ToInt32(
                value,
                CultureInfo.InvariantCulture);
    }

    public async Task AppendAsync(
        IReadOnlyList<HistoricalMetricSample> samples,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(samples);

        if (samples.Count is < 1 or > 1_000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(samples),
                "Historical metric append batch must contain 1 to 1000 samples.");
        }

        foreach (var sample in samples)
        {
            ArgumentNullException.ThrowIfNull(sample);
            sample.Validate();
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(
                    cancellationToken)
                .ConfigureAwait(false);

        var reservedRawIdentities =
            await ReserveRawIdentitiesAsync(
                    connection,
                    transaction,
                    samples,
                    DateTimeOffset.UtcNow.Add(
                        RawIdentityRetention),
                    cancellationToken)
                .ConfigureAwait(false);

        foreach (var sample in samples)
        {
            if (sample.ResolutionSeconds == 0 &&
                !reservedRawIdentities.Contains(
                    ToRawIdentity(sample)))
            {
                continue;
            }

            await using var command =
                connection.CreateCommand();
            command.Transaction =
                transaction;
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
                DO NOTHING
                """;

            BindSample(
                command,
                sample);

            await command
                .ExecuteNonQueryAsync(
                    cancellationToken)
                .ConfigureAwait(false);
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<HashSet<RawSampleIdentity>>
        ReserveRawIdentitiesAsync(
            DbConnection connection,
            DbTransaction transaction,
            IReadOnlyList<HistoricalMetricSample> samples,
            DateTimeOffset expiresAtUtc,
            CancellationToken cancellationToken)
    {
        var rawSamples =
            samples
                .Where(sample =>
                    sample.ResolutionSeconds == 0)
                .ToArray();
        if (rawSamples.Length == 0)
        {
            return [];
        }

        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;

        var values =
            new List<string>(
                rawSamples.Length);
        for (var index = 0;
             index < rawSamples.Length;
             index++)
        {
            var sample =
                rawSamples[index];
            var suffix =
                index.ToString(
                    CultureInfo.InvariantCulture);
            values.Add(
                $"(@metric_name_{suffix}, @cluster_id_{suffix}, @resource_kind_{suffix}, @resource_id_{suffix}, @observed_at_utc_{suffix}, @expires_at_utc)");

            AddParameter(
                command,
                $"@metric_name_{suffix}",
                sample.Identity.MetricName);
            AddParameter(
                command,
                $"@cluster_id_{suffix}",
                sample.Identity.ClusterId);
            AddParameter(
                command,
                $"@resource_kind_{suffix}",
                sample.Identity.ResourceKind);
            AddParameter(
                command,
                $"@resource_id_{suffix}",
                sample.Identity.ResourceId);
            AddParameter(
                command,
                $"@observed_at_utc_{suffix}",
                sample.ObservedAtUtc
                    .ToUniversalTime()
                    .ToString("O"));
        }

        AddParameter(
            command,
            "@expires_at_utc",
            expiresAtUtc
                .ToUniversalTime()
                .ToString("O"));

        command.CommandText =
            $"""
             INSERT INTO kafdeck_historical_metric_raw_identities (
                 metric_name,
                 cluster_id,
                 resource_kind,
                 resource_id,
                 observed_at_utc,
                 expires_at_utc)
             VALUES {string.Join(", ", values)}
             ON CONFLICT (
                 metric_name,
                 cluster_id,
                 resource_kind,
                 resource_id,
                 observed_at_utc)
             DO NOTHING
             RETURNING
                 metric_name,
                 cluster_id,
                 resource_kind,
                 resource_id,
                 observed_at_utc
             """;

        var reserved =
            new HashSet<RawSampleIdentity>();
        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);
        while (await reader
                   .ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            reserved.Add(
                new RawSampleIdentity(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4)));
        }

        return reserved;
    }

    private static RawSampleIdentity ToRawIdentity(
        HistoricalMetricSample sample) =>
        new(
            sample.Identity.MetricName,
            sample.Identity.ClusterId,
            sample.Identity.ResourceKind,
            sample.Identity.ResourceId,
            sample.ObservedAtUtc
                .ToUniversalTime()
                .ToString("O"));

    public async Task<HistoricalMetricQueryResult>
        QueryAsync(
            HistoricalMetricQuery query,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        _policy.ValidateQuery(query);

        using var deadline =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        deadline.CancelAfter(
            _policy.MaxQueryDuration);
        var queryToken =
            deadline.Token;
        var slotAcquired = false;

        try
        {
            await _querySlots
                .WaitAsync(queryToken)
                .ConfigureAwait(false);
            slotAcquired = true;

            await using var connection =
                await _connectionFactory
                    .OpenAsync(queryToken)
                    .ConfigureAwait(false);
            await using var command =
                connection.CreateCommand();

            command.CommandTimeout =
                Math.Max(
                    1,
                    (int)Math.Ceiling(
                        _policy.MaxQueryDuration
                            .TotalSeconds));

            command.CommandText =
                query.ResourceId is null
                    ? """
                      SELECT
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
                          last_observed_at_utc
                      FROM kafdeck_historical_metric_samples
                      WHERE metric_name = @metric_name
                        AND cluster_id = @cluster_id
                        AND resource_kind = @resource_kind
                        AND observed_at_utc >= @from_utc
                        AND observed_at_utc < @to_utc
                      ORDER BY
                          resource_id,
                          observed_at_utc,
                          resolution_seconds
                      LIMIT @row_limit
                      """
                    : """
                      SELECT
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
                          last_observed_at_utc
                      FROM kafdeck_historical_metric_samples
                      WHERE metric_name = @metric_name
                        AND cluster_id = @cluster_id
                        AND resource_kind = @resource_kind
                        AND resource_id = @resource_id
                        AND observed_at_utc >= @from_utc
                        AND observed_at_utc < @to_utc
                      ORDER BY
                          resource_id,
                          observed_at_utc,
                          resolution_seconds
                      LIMIT @row_limit
                      """;

            AddParameter(
                command,
                "@metric_name",
                query.MetricName);
            AddParameter(
                command,
                "@cluster_id",
                query.ClusterId);
            AddParameter(
                command,
                "@resource_kind",
                query.ResourceKind);
            if (query.ResourceId is not null)
            {
                AddParameter(
                    command,
                    "@resource_id",
                    query.ResourceId);
            }

            AddParameter(
                command,
                "@from_utc",
                query.FromUtc.ToUniversalTime()
                    .ToString("O"));
            AddParameter(
                command,
                "@to_utc",
                query.ToUtc.ToUniversalTime()
                    .ToString("O"));
            AddParameter(
                command,
                "@row_limit",
                query.MaxPoints + 1);

            using var providerCancellation =
                RegisterProviderCancellation(
                    connection,
                    queryToken);

            var series =
                new Dictionary<
                    HistoricalMetricIdentity,
                    List<HistoricalMetricSample>>();
            var totalPoints = 0;
            var truncated = false;
            string? limitReason = null;

            await using var reader =
                await command
                    .ExecuteReaderAsync(
                        queryToken)
                    .ConfigureAwait(false);

            while (await reader
                       .ReadAsync(queryToken)
                       .ConfigureAwait(false))
            {
                if (totalPoints >=
                    query.MaxPoints)
                {
                    truncated = true;
                    limitReason =
                        "max_points";
                    break;
                }

                var identity =
                    new HistoricalMetricIdentity(
                        query.MetricName,
                        query.ClusterId,
                        query.ResourceKind,
                        reader.GetString(0));

                if (!series.TryGetValue(
                        identity,
                        out var points))
                {
                    if (series.Count >=
                        query.MaxSeries)
                    {
                        truncated = true;
                        limitReason =
                            "max_series";
                        break;
                    }

                    points = [];
                    series.Add(
                        identity,
                        points);
                }

                points.Add(
                    new HistoricalMetricSample(
                        identity,
                        DateTimeOffset.Parse(
                            reader.GetString(1),
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind),
                        Convert.ToDouble(
                            reader.GetValue(3),
                            CultureInfo.InvariantCulture),
                        Convert.ToDouble(
                            reader.GetValue(4),
                            CultureInfo.InvariantCulture),
                        Convert.ToDouble(
                            reader.GetValue(5),
                            CultureInfo.InvariantCulture),
                        Convert.ToInt64(
                            reader.GetValue(6),
                            CultureInfo.InvariantCulture),
                        Convert.ToInt32(
                            reader.GetValue(2),
                            CultureInfo.InvariantCulture),
                        reader.GetString(7),
                        reader.IsDBNull(8)
                            ? null
                            : reader.GetString(8),
                        reader.IsDBNull(9)
                            ? (Convert.ToInt32(
                                   reader.GetValue(2),
                                   CultureInfo.InvariantCulture) == 0
                                ? DateTimeOffset.Parse(
                                    reader.GetString(1),
                                    CultureInfo.InvariantCulture,
                                    DateTimeStyles.RoundtripKind)
                                : null)
                            : DateTimeOffset.Parse(
                                reader.GetString(9),
                                CultureInfo.InvariantCulture,
                                DateTimeStyles.RoundtripKind),
                        reader.IsDBNull(10)
                            ? (Convert.ToInt32(
                                   reader.GetValue(2),
                                   CultureInfo.InvariantCulture) == 0
                                ? DateTimeOffset.Parse(
                                    reader.GetString(1),
                                    CultureInfo.InvariantCulture,
                                    DateTimeStyles.RoundtripKind)
                                : null)
                            : DateTimeOffset.Parse(
                                reader.GetString(10),
                                CultureInfo.InvariantCulture,
                                DateTimeStyles.RoundtripKind)));

                totalPoints++;
            }

            var resultSeries =
                series
                    .OrderBy(
                        item => item.Key.ResourceId,
                        StringComparer.Ordinal)
                    .Select(
                        item =>
                            new HistoricalMetricSeries(
                                item.Key,
                                item.Value
                                    .OrderBy(
                                        point =>
                                            point.ObservedAtUtc)
                                    .ToArray()))
                    .ToArray();

            return new HistoricalMetricQueryResult(
                resultSeries,
                truncated,
                limitReason,
                query.FromUtc,
                query.ToUtc,
                ProviderName(connection));
        }
        catch (SqliteException exception)
            when (exception.SqliteErrorCode ==
                  raw.SQLITE_INTERRUPT &&
                  deadline.IsCancellationRequested)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(
                    "Historical metric query was cancelled.",
                    exception,
                    cancellationToken);
            }

            throw new TimeoutException(
                "Historical metric query exceeded the configured duration.",
                exception);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested &&
                  deadline.IsCancellationRequested)
        {
            throw new TimeoutException(
                "Historical metric query exceeded the configured duration.");
        }
        finally
        {
            if (slotAcquired)
            {
                _querySlots.Release();
            }
        }
    }

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

    public async Task<HistoricalMetricRetentionResult>
        DeleteExpiredAsync(
            DateTimeOffset rawBeforeUtc,
            DateTimeOffset rollupBeforeUtc,
            CancellationToken cancellationToken = default)
    {
        if (rawBeforeUtc == default ||
            rollupBeforeUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(rawBeforeUtc));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(
                    cancellationToken)
                .ConfigureAwait(false);

        var rawDeleted =
            await DeleteAsync(
                    connection,
                    transaction,
                    resolutionPredicate:
                        "resolution_seconds = 0",
                    rawBeforeUtc,
                    cancellationToken)
                .ConfigureAwait(false);

        var rollupDeleted =
            await DeleteAsync(
                    connection,
                    transaction,
                    resolutionPredicate:
                        "resolution_seconds > 0",
                    rollupBeforeUtc,
                    cancellationToken)
                .ConfigureAwait(false);

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new HistoricalMetricRetentionResult(
            rawDeleted,
            rollupDeleted);
    }

    private static async Task<long> DeleteAsync(
        DbConnection connection,
        DbTransaction transaction,
        string resolutionPredicate,
        DateTimeOffset beforeUtc,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            $"""
             DELETE FROM kafdeck_historical_metric_samples
             WHERE {resolutionPredicate}
               AND observed_at_utc < @before_utc
             """;
        AddParameter(
            command,
            "@before_utc",
            beforeUtc.ToUniversalTime()
                .ToString("O"));

        return await command
            .ExecuteNonQueryAsync(
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static void BindSample(
        DbCommand command,
        HistoricalMetricSample sample)
    {
        AddParameter(
            command,
            "@metric_name",
            sample.Identity.MetricName);
        AddParameter(
            command,
            "@cluster_id",
            sample.Identity.ClusterId);
        AddParameter(
            command,
            "@resource_kind",
            sample.Identity.ResourceKind);
        AddParameter(
            command,
            "@resource_id",
            sample.Identity.ResourceId);
        AddParameter(
            command,
            "@observed_at_utc",
            sample.ObservedAtUtc
                .ToUniversalTime()
                .ToString("O"));
        AddParameter(
            command,
            "@resolution_seconds",
            sample.ResolutionSeconds);
        AddParameter(
            command,
            "@min_value",
            sample.Min);
        AddParameter(
            command,
            "@max_value",
            sample.Max);
        AddParameter(
            command,
            "@sum_value",
            sample.Sum);
        AddParameter(
            command,
            "@sample_count",
            sample.Count);
        AddParameter(
            command,
            "@source",
            sample.Source);
        AddParameter(
            command,
            "@state",
            sample.State is null
                ? DBNull.Value
                : sample.State);
        AddParameter(
            command,
            "@first_observed_at_utc",
            sample.EffectiveFirstObservedAtUtc is { } firstObservedAtUtc
                ? firstObservedAtUtc
                    .ToUniversalTime()
                    .ToString("O")
                : DBNull.Value);
        AddParameter(
            command,
            "@last_observed_at_utc",
            sample.EffectiveLastObservedAtUtc is { } lastObservedAtUtc
                ? lastObservedAtUtc
                    .ToUniversalTime()
                    .ToString("O")
                : DBNull.Value);
    }

    private static string ProviderName(
        DbConnection connection) =>
        connection.GetType().Name.Contains(
            "Sqlite",
            StringComparison.OrdinalIgnoreCase)
            ? "sqlite"
            : "postgresql";

    private static void AddParameter(
        DbCommand command,
        string name,
        object value)
    {
        var parameter =
            command.CreateParameter();
        parameter.ParameterName =
            name;
        parameter.Value =
            value;
        command.Parameters.Add(
            parameter);
    }
}
