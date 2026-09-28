using System.Data;
using System.Data.Common;
using Kafdeck.Core.Observability;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W63HistoricalMetricsPersistenceTests
{
    [Fact]
    public void Historical_metrics_configuration_loads_explicit_sqlite_provider()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-{Guid.NewGuid():N}.db");

        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Observability:History:Enabled"] = "true",
            ["Kafdeck:Observability:History:Provider"] = "Sqlite",
            ["Kafdeck:Observability:History:ExecutionMode"] = "Standalone",
            ["Kafdeck:Observability:History:SqliteDatabasePath"] = path,
            ["Kafdeck:Observability:History:RawRetentionHours"] = "24",
            ["Kafdeck:Observability:History:RollupRetentionDays"] = "7",
            ["Kafdeck:Observability:History:MaxQueryRangeHours"] = "24",
            ["Kafdeck:Observability:History:MaxSeriesPerQuery"] = "1000",
            ["Kafdeck:Observability:History:MaxPointsPerQuery"] = "50000",
            ["Kafdeck:Observability:History:MaxQueryDurationSeconds"] = "10",
            ["Kafdeck:Observability:History:MaxConcurrentQueries"] = "2",
        });

        KafdeckConfigurationValidator.ValidateAndThrow(options);

        var history = Assert.IsType<HistoricalMetricsOptions>(
            options.Observability?.History);
        Assert.True(history.Enabled);
        Assert.Equal(
            HistoricalMetricsProvider.Sqlite,
            history.Provider);
        Assert.Equal(
            HistoricalMetricsExecutionMode.Standalone,
            history.ExecutionMode);
        Assert.Equal(path, history.SqliteDatabasePath);
        Assert.Null(history.ConnectionString);
    }

    [Fact]
    public void Sqlite_history_rejects_high_availability_mode()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-{Guid.NewGuid():N}.db");

        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Observability:History:Enabled"] = "true",
            ["Kafdeck:Observability:History:Provider"] = "Sqlite",
            ["Kafdeck:Observability:History:ExecutionMode"] = "HighAvailability",
            ["Kafdeck:Observability:History:SqliteDatabasePath"] = path,
        });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
    }

    [Fact]
    public void PostgreSql_history_requires_connection_secret()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Observability:History:Enabled"] = "true",
            ["Kafdeck:Observability:History:Provider"] = "PostgreSql",
            ["Kafdeck:Observability:History:ExecutionMode"] = "HighAvailability",
        });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
    }

    [Theory]
    [InlineData("RawRetentionHours", "169")]
    [InlineData("RollupRetentionDays", "91")]
    [InlineData("MaxQueryRangeHours", "745")]
    [InlineData("MaxSeriesPerQuery", "10001")]
    [InlineData("MaxPointsPerQuery", "250001")]
    [InlineData("MaxQueryDurationSeconds", "31")]
    [InlineData("MaxConcurrentQueries", "9")]
    public void Historical_metrics_cap_plus_one_fails_closed(
        string key,
        string value)
    {
        var options = Load(new Dictionary<string, string?>
        {
            [$"Kafdeck:Observability:History:{key}"] = value,
        });

        Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
    }

    [Fact]
    public async Task Newer_sqlite_schema_version_fails_before_v1_ddl()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-{Guid.NewGuid():N}.db");

        try
        {
            await using (var connection =
                         new SqliteConnection(
                             $"Data Source={path}"))
            {
                await connection.OpenAsync();
                await using var command =
                    connection.CreateCommand();
                command.CommandText =
                    """
                    CREATE TABLE kafdeck_schema_info (
                        component TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL
                    );
                    INSERT INTO kafdeck_schema_info (
                        component,
                        schema_version)
                    VALUES (
                        'historical-metrics',
                        3);
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store =
                new AdoHistoricalMetricStore(
                    new SqliteHistoricalMetricsDbConnectionFactory(path),
                    TestPolicy());

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.InitializeAsync());

            await using var verify =
                new SqliteConnection(
                    $"Data Source={path}");
            await verify.OpenAsync();
            await using var verifyCommand =
                verify.CreateCommand();
            verifyCommand.CommandText =
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                  AND name = 'kafdeck_historical_metric_samples'
                """;

            Assert.Equal(
                0L,
                Convert.ToInt64(
                    await verifyCommand.ExecuteScalarAsync()));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Version_one_rollup_migration_preserves_unknown_coverage()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-v1-rollup-{Guid.NewGuid():N}.db");

        try
        {
            var rawObserved =
                new DateTimeOffset(
                    2026,
                    9,
                    28,
                    9,
                    1,
                    0,
                    TimeSpan.Zero);
            var rollupBucket =
                new DateTimeOffset(
                    2026,
                    9,
                    28,
                    8,
                    0,
                    0,
                    TimeSpan.Zero);

            await using (var connection =
                         new SqliteConnection(
                             $"Data Source={path}"))
            {
                await connection.OpenAsync();
                await using var command =
                    connection.CreateCommand();
                command.CommandText =
                    """
                    CREATE TABLE kafdeck_schema_info (
                        component TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL
                    );
                    INSERT INTO kafdeck_schema_info (
                        component,
                        schema_version)
                    VALUES (
                        'historical-metrics',
                        1);

                    CREATE TABLE kafdeck_historical_metric_samples (
                        metric_name TEXT NOT NULL,
                        cluster_id TEXT NOT NULL,
                        resource_kind TEXT NOT NULL,
                        resource_id TEXT NOT NULL,
                        observed_at_utc TEXT NOT NULL,
                        resolution_seconds INTEGER NOT NULL,
                        min_value REAL NOT NULL,
                        max_value REAL NOT NULL,
                        sum_value REAL NOT NULL,
                        sample_count BIGINT NOT NULL,
                        source TEXT NOT NULL,
                        state TEXT NULL,
                        PRIMARY KEY (
                            metric_name,
                            cluster_id,
                            resource_kind,
                            resource_id,
                            observed_at_utc,
                            resolution_seconds)
                    );

                    INSERT INTO kafdeck_historical_metric_samples
                    VALUES (
                        'consumer.lag.total',
                        'prod',
                        'consumer_group',
                        'group-a',
                        @raw_observed,
                        0,
                        10,
                        10,
                        10,
                        1,
                        'legacy',
                        'Stable');

                    INSERT INTO kafdeck_historical_metric_samples
                    VALUES (
                        'consumer.lag.total',
                        'prod',
                        'consumer_group',
                        'group-a',
                        @rollup_bucket,
                        300,
                        5,
                        20,
                        45,
                        3,
                        'legacy-rollup',
                        'Stable');
                    """;
                command.Parameters.AddWithValue(
                    "@raw_observed",
                    rawObserved.ToString("O"));
                command.Parameters.AddWithValue(
                    "@rollup_bucket",
                    rollupBucket.ToString("O"));
                await command.ExecuteNonQueryAsync();
            }

            var store =
                new AdoHistoricalMetricStore(
                    new SqliteHistoricalMetricsDbConnectionFactory(
                        path),
                    TestPolicy());

            await store.InitializeAsync();

            var query =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        rollupBucket.AddHours(-1),
                        rawObserved.AddHours(1),
                        MaxSeries: 1,
                        MaxPoints: 10));

            var points =
                Assert.Single(query.Series).Points;

            var raw =
                Assert.Single(
                    points,
                    point =>
                        point.ResolutionSeconds == 0);
            Assert.True(raw.HasKnownCoverage);
            Assert.Equal(
                rawObserved,
                raw.FirstObservedAtUtc);
            Assert.Equal(
                rawObserved,
                raw.LastObservedAtUtc);

            var rollup =
                Assert.Single(
                    points,
                    point =>
                        point.ResolutionSeconds == 300);
            Assert.False(
                rollup.HasKnownCoverage);
            Assert.Null(
                rollup.FirstObservedAtUtc);
            Assert.Null(
                rollup.LastObservedAtUtc);
            Assert.Equal(
                "Unknown",
                rollup.State);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task PostgreSql_initialization_is_serialized_across_replicas()
    {
        var baseConnectionString =
            Environment.GetEnvironmentVariable(
                "KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(
                baseConnectionString))
        {
            return;
        }

        var schema =
            $"w63_{Guid.NewGuid():N}";
        var adminBuilder =
            new NpgsqlConnectionStringBuilder(
                baseConnectionString);
        await using var admin =
            new NpgsqlConnection(
                adminBuilder.ConnectionString);
        await admin.OpenAsync();

        try
        {
            await using (var create =
                         admin.CreateCommand())
            {
                create.CommandText =
                    $"CREATE SCHEMA \"{schema}\"";
                await create.ExecuteNonQueryAsync();
            }

            var scopedBuilder =
                new NpgsqlConnectionStringBuilder(
                    baseConnectionString)
                {
                    SearchPath = schema,
                    Pooling = false,
                };

            var stores =
                Enumerable.Range(0, 8)
                    .Select(
                        _ => new AdoHistoricalMetricStore(
                            new PostgreSqlHistoricalMetricsDbConnectionFactory(
                                scopedBuilder.ConnectionString),
                            TestPolicy()))
                    .ToArray();

            await Task.WhenAll(
                stores.Select(
                    store =>
                        store.InitializeAsync()));

            await using var verify =
                new NpgsqlConnection(
                    scopedBuilder.ConnectionString);
            await verify.OpenAsync();
            await using var command =
                verify.CreateCommand();
            command.CommandText =
                """
                SELECT
                    (SELECT COUNT(*)
                     FROM kafdeck_schema_info
                     WHERE component = 'historical-metrics'),
                    (SELECT COUNT(*)
                     FROM information_schema.tables
                     WHERE table_schema = current_schema()
                       AND table_name = 'kafdeck_historical_metric_samples')
                """;

            await using var reader =
                await command.ExecuteReaderAsync();
            Assert.True(
                await reader.ReadAsync());
            Assert.Equal(
                1L,
                reader.GetInt64(0));
            Assert.Equal(
                1L,
                reader.GetInt64(1));
        }
        finally
        {
            await using var drop =
                admin.CreateCommand();
            drop.CommandText =
                $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Version_one_partial_rollup_migration_preserves_partial_state()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-v1-partial-{Guid.NewGuid():N}.db");

        try
        {
            var bucket =
                DateTimeOffset.UtcNow.AddHours(-2);

            await using (var connection =
                         new SqliteConnection(
                             $"Data Source={path}"))
            {
                await connection.OpenAsync();
                await using var command =
                    connection.CreateCommand();
                command.CommandText =
                    """
                    CREATE TABLE kafdeck_schema_info (
                        component TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL
                    );
                    INSERT INTO kafdeck_schema_info (
                        component,
                        schema_version)
                    VALUES (
                        'historical-metrics',
                        1);

                    CREATE TABLE kafdeck_historical_metric_samples (
                        metric_name TEXT NOT NULL,
                        cluster_id TEXT NOT NULL,
                        resource_kind TEXT NOT NULL,
                        resource_id TEXT NOT NULL,
                        observed_at_utc TEXT NOT NULL,
                        resolution_seconds INTEGER NOT NULL,
                        min_value REAL NOT NULL,
                        max_value REAL NOT NULL,
                        sum_value REAL NOT NULL,
                        sample_count BIGINT NOT NULL,
                        source TEXT NOT NULL,
                        state TEXT NULL,
                        PRIMARY KEY (
                            metric_name,
                            cluster_id,
                            resource_kind,
                            resource_id,
                            observed_at_utc,
                            resolution_seconds)
                    );

                    INSERT INTO kafdeck_historical_metric_samples
                    VALUES (
                        'consumer.lag.total',
                        'prod',
                        'consumer_group',
                        'group-a',
                        @bucket,
                        300,
                        5,
                        20,
                        45,
                        3,
                        'legacy-rollup',
                        'Partial');
                    """;
                command.Parameters.AddWithValue(
                    "@bucket",
                    bucket.ToString("O"));
                await command.ExecuteNonQueryAsync();
            }

            var store =
                new AdoHistoricalMetricStore(
                    new SqliteHistoricalMetricsDbConnectionFactory(
                        path),
                    TestPolicy());
            await store.InitializeAsync();

            var query =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        bucket.AddHours(-1),
                        bucket.AddHours(1),
                        MaxSeries: 1,
                        MaxPoints: 10));

            var rollup =
                Assert.Single(
                    Assert.Single(query.Series).Points);
            Assert.Equal(
                "Partial",
                rollup.State);
            Assert.False(
                rollup.HasKnownCoverage);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Sqlite_store_append_is_idempotent_and_query_is_bounded()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-{Guid.NewGuid():N}.db");

        try
        {
            var store = new AdoHistoricalMetricStore(
                new SqliteHistoricalMetricsDbConnectionFactory(path),
                TestPolicy());
            await store.InitializeAsync();

            var now = DateTimeOffset.UtcNow;
            var groupA = new HistoricalMetricIdentity(
                "consumer.lag.total",
                "prod",
                "consumer_group",
                "group-a");
            var groupB = new HistoricalMetricIdentity(
                "consumer.lag.total",
                "prod",
                "consumer_group",
                "group-b");

            var first = HistoricalMetricSample.Gauge(
                groupA,
                now.AddMinutes(-2),
                100,
                "consumer_observer",
                "Stable");
            var second = HistoricalMetricSample.Gauge(
                groupA,
                now.AddMinutes(-1),
                90,
                "consumer_observer",
                "Stable");
            var other = HistoricalMetricSample.Gauge(
                groupB,
                now.AddMinutes(-1),
                50,
                "consumer_observer",
                "Stable");

            await store.AppendAsync([first, first, second, other]);

            var exact = await store.QueryAsync(
                new HistoricalMetricQuery(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a",
                    now.AddHours(-1),
                    now.AddMinutes(1),
                    MaxSeries: 1,
                    MaxPoints: 10));

            var exactSeries = Assert.Single(exact.Series);
            Assert.Equal(2, exactSeries.Points.Count);
            Assert.False(exact.Truncated);
            Assert.Equal("sqlite", exact.Provider);

            var bounded = await store.QueryAsync(
                new HistoricalMetricQuery(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    ResourceId: null,
                    now.AddHours(-1),
                    now.AddMinutes(1),
                    MaxSeries: 1,
                    MaxPoints: 10));

            Assert.Single(bounded.Series);
            Assert.True(bounded.Truncated);
            Assert.Equal("max_series", bounded.LimitReason);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public void Raw_sample_rejects_one_sided_explicit_coverage()
    {
        var observedAt =
            DateTimeOffset.UtcNow;
        var sample =
            new HistoricalMetricSample(
                new HistoricalMetricIdentity(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a"),
                observedAt,
                10,
                10,
                10,
                1,
                ResolutionSeconds: 0,
                Source: "consumer_observer",
                State: "Stable",
                FirstObservedAtUtc: observedAt,
                LastObservedAtUtc: null);

        Assert.Throws<ArgumentException>(
            sample.Validate);
    }

    [Fact]
    public async Task Sqlite_store_applies_raw_and_rollup_retention_separately()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-{Guid.NewGuid():N}.db");

        try
        {
            var store = new AdoHistoricalMetricStore(
                new SqliteHistoricalMetricsDbConnectionFactory(path),
                TestPolicy());
            await store.InitializeAsync();

            var now = DateTimeOffset.UtcNow;
            var identity = new HistoricalMetricIdentity(
                "consumer.lag.total",
                "prod",
                "consumer_group",
                "group-a");

            var oldRaw = HistoricalMetricSample.Gauge(
                identity,
                now.AddDays(-2),
                100,
                "consumer_observer",
                "Stable");
            var newRaw = HistoricalMetricSample.Gauge(
                identity,
                now.AddHours(-1),
                90,
                "consumer_observer",
                "Stable");
            var oldRollup = new HistoricalMetricSample(
                identity,
                now.AddDays(-10),
                50,
                100,
                300,
                4,
                ResolutionSeconds: 300,
                Source: "rollup",
                State: "Unknown");
            var newRollup = new HistoricalMetricSample(
                identity,
                now.AddDays(-1),
                40,
                90,
                250,
                4,
                ResolutionSeconds: 300,
                Source: "rollup",
                State: "Unknown");

            await store.AppendAsync(
                [oldRaw, newRaw, oldRollup, newRollup]);

            var deleted = await store.DeleteExpiredAsync(
                rawBeforeUtc: now.AddHours(-24),
                rollupBeforeUtc: now.AddDays(-7));

            Assert.Equal(1, deleted.RawDeleted);
            Assert.Equal(1, deleted.RollupDeleted);

            var remaining = await store.QueryAsync(
                new HistoricalMetricQuery(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a",
                    now.AddDays(-30),
                    now.AddMinutes(1),
                    MaxSeries: 1,
                    MaxPoints: 10));

            var points = Assert.Single(remaining.Series).Points;
            Assert.Equal(2, points.Count);
            Assert.Contains(points, point => point.ResolutionSeconds == 0);
            Assert.Contains(points, point => point.ResolutionSeconds == 300);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Query_owns_a_deadline_and_releases_the_slot_after_timeout()
    {
        var store =
            new AdoHistoricalMetricStore(
                new BlockingHistoricalMetricsConnectionFactory(),
                new HistoricalMetricStorePolicy(
                    HistoricalMetricQuery.HardMaxRange,
                    MaxSeriesPerQuery: 10,
                    MaxPointsPerQuery: 100,
                    MaxQueryDuration:
                        TimeSpan.FromMilliseconds(100),
                    MaxConcurrentQueries: 1));

        var now = DateTimeOffset.UtcNow;
        var query =
            new HistoricalMetricQuery(
                "consumer.lag.total",
                "prod",
                "consumer_group",
                "group-a",
                now.AddMinutes(-5),
                now,
                MaxSeries: 1,
                MaxPoints: 10);

        await Assert.ThrowsAsync<TimeoutException>(
            () => store.QueryAsync(
                query,
                CancellationToken.None));

        await Assert.ThrowsAsync<TimeoutException>(
            () => store.QueryAsync(
                query,
                CancellationToken.None));
    }

    [Fact]
    public async Task Sqlite_query_deadline_interrupts_native_execution_and_releases_slot()
    {
        var databaseName =
            $"w63-timeout-{Guid.NewGuid():N}";
        var connectionString =
            new SqliteConnectionStringBuilder
            {
                DataSource = databaseName,
                Mode = SqliteOpenMode.Memory,
                Cache = SqliteCacheMode.Shared,
                Pooling = false,
            }.ToString();

        await using var anchor =
            new SqliteConnection(
                connectionString);
        await anchor.OpenAsync();

        await using (var schema =
                     anchor.CreateCommand())
        {
            schema.CommandText =
                """
                CREATE VIEW kafdeck_historical_metric_samples AS
                WITH RECURSIVE counter(value) AS (
                    VALUES(0)
                    UNION ALL
                    SELECT value + 1
                    FROM counter
                    WHERE value < 1000000000
                )
                SELECT
                    'consumer.lag.total' AS metric_name,
                    'prod' AS cluster_id,
                    'consumer_group' AS resource_kind,
                    'group-a' AS resource_id,
                    '2026-01-01T00:00:00.0000000+00:00' AS observed_at_utc,
                    0 AS resolution_seconds,
                    CAST(SUM(value) AS REAL) AS min_value,
                    CAST(SUM(value) AS REAL) AS max_value,
                    CAST(SUM(value) AS REAL) AS sum_value,
                    COUNT(*) AS sample_count,
                    'test' AS source,
                    'Stable' AS state,
                    '2026-01-01T00:00:00.0000000+00:00' AS first_observed_at_utc,
                    '2026-01-01T00:00:00.0000000+00:00' AS last_observed_at_utc
                FROM counter
                """;
            await schema.ExecuteNonQueryAsync();
        }

        var store =
            new AdoHistoricalMetricStore(
                new SharedMemoryHistoricalMetricsConnectionFactory(
                    connectionString),
                new HistoricalMetricStorePolicy(
                    HistoricalMetricQuery.HardMaxRange,
                    MaxSeriesPerQuery: 10,
                    MaxPointsPerQuery: 100,
                    MaxQueryDuration:
                        TimeSpan.FromMilliseconds(100),
                    MaxConcurrentQueries: 1));

        var observedAt =
            new DateTimeOffset(
                2026,
                1,
                1,
                0,
                0,
                0,
                TimeSpan.Zero);
        var query =
            new HistoricalMetricQuery(
                "consumer.lag.total",
                "prod",
                "consumer_group",
                "group-a",
                observedAt.AddHours(-1),
                observedAt.AddHours(1),
                MaxSeries: 1,
                MaxPoints: 10);

        await Assert.ThrowsAsync<TimeoutException>(
            () => store.QueryAsync(
                query,
                CancellationToken.None));

        await Assert.ThrowsAsync<TimeoutException>(
            () => store.QueryAsync(
                query,
                CancellationToken.None));
    }

    [Fact]
    public async Task Maintenance_rolls_expired_raw_before_deletion()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-maint-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var rollupWindowStart =
                DateTimeOffset.FromUnixTimeSeconds(
                    now.AddHours(-3)
                        .ToUnixTimeSeconds() /
                    300 *
                    300);
            var identity =
                new HistoricalMetricIdentity(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a");

            await store.AppendAsync(
                [
                    HistoricalMetricSample.Gauge(
                        identity,
                        rollupWindowStart
                            .AddMinutes(1),
                        10,
                        "consumer_observer",
                        "Stable"),
                    HistoricalMetricSample.Gauge(
                        identity,
                        rollupWindowStart
                            .AddMinutes(2),
                        30,
                        "consumer_observer",
                        "Stable"),
                    HistoricalMetricSample.Gauge(
                        identity,
                        now.AddMinutes(-30),
                        40,
                        "consumer_observer",
                        "Stable"),
                    new HistoricalMetricSample(
                        identity,
                        now.AddDays(-8),
                        1,
                        1,
                        1,
                        1,
                        ResolutionSeconds: 300,
                        Source: "rollup",
                        State: "Unknown"),
                ]);

            var policy =
                TestMaintenancePolicy();
            var lease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        policy.LeaseDuration));

            var result =
                await maintenance.RunCycleAsync(
                    lease,
                    now,
                    policy);

            Assert.True(result.LeaseValid);
            Assert.Equal(1, result.RolledWindows);
            Assert.Equal(2, result.RawDeleted);
            Assert.Equal(1, result.RollupDeleted);

            var query =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        now.AddHours(-4),
                        now.AddMinutes(1),
                        MaxSeries: 1,
                        MaxPoints: 10));

            var points =
                Assert.Single(query.Series)
                    .Points;
            Assert.Equal(2, points.Count);

            var rollup =
                Assert.Single(
                    points,
                    point =>
                        point.ResolutionSeconds ==
                        300);
            Assert.Equal(10, rollup.Min);
            Assert.Equal(30, rollup.Max);
            Assert.Equal(40, rollup.Sum);
            Assert.Equal(2, rollup.Count);
            Assert.Equal(20, rollup.Average);

            var recent =
                Assert.Single(
                    points,
                    point =>
                        point.ResolutionSeconds ==
                        0);
            Assert.Equal(40, recent.Sum);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Maintenance_merges_late_raw_into_existing_rollup()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-late-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var rollupWindowStart =
                DateTimeOffset.FromUnixTimeSeconds(
                    now.AddHours(-3)
                        .ToUnixTimeSeconds() /
                    300 *
                    300);
            var identity =
                new HistoricalMetricIdentity(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a");
            var policy =
                TestMaintenancePolicy();

            await store.AppendAsync(
                [
                    HistoricalMetricSample.Gauge(
                        identity,
                        rollupWindowStart
                            .AddMinutes(1),
                        10,
                        "consumer_observer"),
                    HistoricalMetricSample.Gauge(
                        identity,
                        rollupWindowStart
                            .AddMinutes(2),
                        30,
                        "consumer_observer"),
                ]);

            var firstLease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        policy.LeaseDuration));
            _ = await maintenance.RunCycleAsync(
                firstLease,
                now,
                policy);

            await store.AppendAsync(
                [
                    HistoricalMetricSample.Gauge(
                        identity,
                        rollupWindowStart
                            .AddMinutes(3),
                        50,
                        "consumer_observer"),
                ]);

            var secondNow =
                now.AddSeconds(30);
            var secondLease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        secondNow,
                        policy.LeaseDuration));
            _ = await maintenance.RunCycleAsync(
                secondLease,
                secondNow,
                policy);

            var query =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        now.AddHours(-4),
                        now,
                        MaxSeries: 1,
                        MaxPoints: 10));

            var rollup =
                Assert.Single(
                    Assert.Single(query.Series)
                        .Points,
                    point =>
                        point.ResolutionSeconds ==
                        300);

            Assert.Equal(10, rollup.Min);
            Assert.Equal(50, rollup.Max);
            Assert.Equal(90, rollup.Sum);
            Assert.Equal(3, rollup.Count);
            Assert.Equal(30, rollup.Average);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Maintenance_preserves_partial_and_unknown_truth_in_rollup()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-truth-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var rollupWindowStart =
                DateTimeOffset.FromUnixTimeSeconds(
                    now.AddHours(-3)
                        .ToUnixTimeSeconds() /
                    300 *
                    300);
            var identity =
                new HistoricalMetricIdentity(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a");
            var policy =
                TestMaintenancePolicy();

            await store.AppendAsync(
                [
                    HistoricalMetricSample.Gauge(
                        identity,
                        rollupWindowStart
                            .AddMinutes(1),
                        10,
                        "consumer_observer",
                        "Unknown"),
                    HistoricalMetricSample.Gauge(
                        identity,
                        rollupWindowStart
                            .AddMinutes(2),
                        20,
                        "consumer_observer",
                        "Partial"),
                ]);

            var lease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        policy.LeaseDuration));

            var result =
                await maintenance.RunCycleAsync(
                    lease,
                    now,
                    policy);

            Assert.True(result.LeaseValid);

            var query =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        now.AddHours(-4),
                        now,
                        MaxSeries: 1,
                        MaxPoints: 10));

            var rollup =
                Assert.Single(
                    Assert.Single(query.Series)
                        .Points,
                    point =>
                        point.ResolutionSeconds ==
                        policy.RollupResolutionSeconds);

            Assert.Equal(
                "Partial",
                rollup.State);
            Assert.NotNull(
                rollup.FirstObservedAtUtc);
            Assert.NotNull(
                rollup.LastObservedAtUtc);
            Assert.True(
                rollup.FirstObservedAtUtc <=
                rollup.LastObservedAtUtc);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Maintenance_rejects_retry_of_already_rolled_raw_identity()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-retry-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var observedAt =
                now.AddHours(-3)
                    .AddMinutes(1);
            var identity =
                new HistoricalMetricIdentity(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a");
            var sample =
                HistoricalMetricSample.Gauge(
                    identity,
                    observedAt,
                    10,
                    "consumer_observer",
                    "Stable");
            var policy =
                TestMaintenancePolicy();

            await store.AppendAsync([sample]);

            var firstLease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        policy.LeaseDuration));
            _ = await maintenance.RunCycleAsync(
                firstLease,
                now,
                policy);

            await store.AppendAsync([sample]);

            var secondNow =
                DateTimeOffset.UtcNow;
            var secondLease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        secondNow,
                        policy.LeaseDuration));
            _ = await maintenance.RunCycleAsync(
                secondLease,
                secondNow,
                policy);

            var result =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        observedAt.AddHours(-1),
                        secondNow.AddMinutes(1),
                        MaxSeries: 1,
                        MaxPoints: 10));

            var points =
                Assert.Single(result.Series).Points;
            var rollup =
                Assert.Single(
                    points,
                    point =>
                        point.ResolutionSeconds == 300);

            Assert.Equal(1, rollup.Count);
            Assert.Equal(10, rollup.Sum);
            Assert.DoesNotContain(
                points,
                point =>
                    point.ResolutionSeconds == 0 &&
                    point.ObservedAtUtc == observedAt);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Maintenance_keeps_legacy_rollup_coverage_unknown_after_late_merge()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-legacy-late-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var bucket =
                DateTimeOffset.FromUnixTimeSeconds(
                    now.AddHours(-3)
                        .ToUnixTimeSeconds() /
                    300 *
                    300);
            var identity =
                new HistoricalMetricIdentity(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a");

            await store.AppendAsync(
                [
                    new HistoricalMetricSample(
                        identity,
                        bucket,
                        5,
                        20,
                        45,
                        3,
                        300,
                        "legacy-rollup",
                        "Unknown"),
                    HistoricalMetricSample.Gauge(
                        identity,
                        bucket.AddMinutes(1),
                        30,
                        "consumer_observer",
                        "Stable"),
                ]);

            // Simulate migrated legacy coverage: aggregate exists but original bounds are unknowable.
            await using (var connection =
                         new SqliteConnection(
                             $"Data Source={path}"))
            {
                await connection.OpenAsync();
                await using var command =
                    connection.CreateCommand();
                command.CommandText =
                    """
                    UPDATE kafdeck_historical_metric_samples
                    SET
                        first_observed_at_utc = NULL,
                        last_observed_at_utc = NULL
                    WHERE resolution_seconds = 300
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var policy =
                TestMaintenancePolicy();
            var lease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        policy.LeaseDuration));
            _ = await maintenance.RunCycleAsync(
                lease,
                now,
                policy);

            var result =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        bucket.AddHours(-1),
                        now.AddMinutes(1),
                        MaxSeries: 1,
                        MaxPoints: 10));

            var rollup =
                Assert.Single(
                    Assert.Single(result.Series).Points,
                    point =>
                        point.ResolutionSeconds == 300);
            Assert.False(
                rollup.HasKnownCoverage);
            Assert.Null(
                rollup.FirstObservedAtUtc);
            Assert.Null(
                rollup.LastObservedAtUtc);
            Assert.Equal(
                "Partial",
                rollup.State);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Maintenance_rejects_lease_expired_before_cycle_execution()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-expired-lease-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var acquiredAt =
                DateTimeOffset.UtcNow.AddMinutes(-3);
            var lease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        acquiredAt,
                        TimeSpan.FromMinutes(2)));

            var result =
                await maintenance.RunCycleAsync(
                    lease,
                    acquiredAt,
                    TestMaintenancePolicy());

            Assert.False(
                result.LeaseValid);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Maintenance_counts_exact_full_batch_as_completed_window()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-full-batch-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var windowStart =
                DateTimeOffset.FromUnixTimeSeconds(
                    now.AddHours(-3)
                        .ToUnixTimeSeconds() /
                    300 *
                    300);
            var identity =
                new HistoricalMetricIdentity(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a");

            var samples =
                Enumerable.Range(
                        0,
                        AdoHistoricalMetricMaintenanceStore
                            .MaxRawSamplesPerRollupBatch)
                    .Select(
                        index =>
                            HistoricalMetricSample.Gauge(
                                identity,
                                windowStart.AddSeconds(index),
                                index + 1,
                                "consumer_observer",
                                "Stable"))
                    .Append(
                        HistoricalMetricSample.Gauge(
                            identity,
                            windowStart
                                .AddMinutes(5)
                                .AddSeconds(1),
                            999,
                            "consumer_observer",
                            "Stable"))
                    .ToArray();

            await store.AppendAsync(samples);

            var policy =
                TestMaintenancePolicy() with
                {
                    MaxRollupWindowsPerCycle = 1,
                };
            var lease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        policy.LeaseDuration));

            var result =
                await maintenance.RunCycleAsync(
                    lease,
                    now,
                    policy);

            Assert.True(result.LeaseValid);
            Assert.Equal(1, result.RolledWindows);
            Assert.Equal(
                AdoHistoricalMetricMaintenanceStore
                    .MaxRawSamplesPerRollupBatch,
                result.RawDeleted);

            var query =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        windowStart.AddMinutes(-1),
                        now.AddMinutes(1),
                        MaxSeries: 1,
                        MaxPoints: 100));

            var points =
                Assert.Single(query.Series).Points;
            var rollup =
                Assert.Single(
                    points,
                    point =>
                        point.ResolutionSeconds == 300);
            Assert.Equal(
                AdoHistoricalMetricMaintenanceStore
                    .MaxRawSamplesPerRollupBatch,
                rollup.Count);
            Assert.Contains(
                points,
                point =>
                    point.ResolutionSeconds == 0 &&
                    point.ObservedAtUtc ==
                        windowStart
                            .AddMinutes(5)
                            .AddSeconds(1));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Maintenance_lease_fences_stale_worker()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-fence-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var policy =
                TestMaintenancePolicy();

            var first =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        policy.LeaseDuration));

            Assert.Null(
                await maintenance.TryAcquireLeaseAsync(
                    "node-b",
                    now.AddSeconds(30),
                    policy.LeaseDuration));

            var takeoverAt =
                now.AddMinutes(3);
            var second =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-b",
                        takeoverAt,
                        policy.LeaseDuration));

            Assert.True(
                second.FencingToken >
                first.FencingToken);

            var staleResult =
                await maintenance.RunCycleAsync(
                    first,
                    takeoverAt,
                    policy);
            Assert.False(
                staleResult.LeaseValid);

            var activeResult =
                await maintenance.RunCycleAsync(
                    second,
                    takeoverAt,
                    policy);
            Assert.True(
                activeResult.LeaseValid);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task PostgreSql_maintenance_lease_has_single_active_owner()
    {
        var baseConnectionString =
            Environment.GetEnvironmentVariable(
                "KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(
                baseConnectionString))
        {
            return;
        }

        var schema =
            $"w63_maint_{Guid.NewGuid():N}";
        var adminBuilder =
            new NpgsqlConnectionStringBuilder(
                baseConnectionString);
        await using var admin =
            new NpgsqlConnection(
                adminBuilder.ConnectionString);
        await admin.OpenAsync();

        try
        {
            await using (var create =
                         admin.CreateCommand())
            {
                create.CommandText =
                    $"CREATE SCHEMA \"{schema}\"";
                await create.ExecuteNonQueryAsync();
            }

            var scopedBuilder =
                new NpgsqlConnectionStringBuilder(
                    baseConnectionString)
                {
                    SearchPath = schema,
                    Pooling = false,
                };
            var factory =
                new PostgreSqlHistoricalMetricsDbConnectionFactory(
                    scopedBuilder.ConnectionString);
            var history =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var firstStore =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);
            var secondStore =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await history.InitializeAsync();
            await firstStore.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var leaseDuration =
                TimeSpan.FromMinutes(2);

            var leases =
                await Task.WhenAll(
                    firstStore.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        leaseDuration),
                    secondStore.TryAcquireLeaseAsync(
                        "node-b",
                        now,
                        leaseDuration));

            Assert.Equal(
                1,
                leases.Count(
                    lease =>
                        lease is not null));
        }
        finally
        {
            await using var drop =
                admin.CreateCommand();
            drop.CommandText =
                $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Maintenance_batches_expired_rollup_deletion()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-history-retention-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteHistoricalMetricsDbConnectionFactory(
                    path);
            var store =
                new AdoHistoricalMetricStore(
                    factory,
                    TestPolicy());
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await store.InitializeAsync();
            await maintenance.InitializeAsync();

            var now =
                DateTimeOffset.UtcNow;
            var identity =
                new HistoricalMetricIdentity(
                    "consumer.lag.total",
                    "prod",
                    "consumer_group",
                    "group-a");

            await store.AppendAsync(
                [
                    new HistoricalMetricSample(
                        identity,
                        now.AddDays(-10),
                        1,
                        1,
                        1,
                        1,
                        300,
                        "rollup",
                        "Unknown"),
                    new HistoricalMetricSample(
                        identity,
                        now.AddDays(-9),
                        2,
                        2,
                        2,
                        1,
                        300,
                        "rollup",
                        "Unknown"),
                    new HistoricalMetricSample(
                        identity,
                        now.AddDays(-8),
                        3,
                        3,
                        3,
                        1,
                        300,
                        "rollup",
                        "Unknown"),
                ]);

            var policy =
                TestMaintenancePolicy() with
                {
                    MaxRollupDeletesPerCycle = 2,
                };

            var firstLease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        now,
                        policy.LeaseDuration));
            var first =
                await maintenance.RunCycleAsync(
                    firstLease,
                    now,
                    policy);

            Assert.True(first.LeaseValid);
            Assert.Equal(2, first.RollupDeleted);

            var secondNow =
                now.AddSeconds(30);
            var secondLease =
                Assert.IsType<HistoricalMetricMaintenanceLease>(
                    await maintenance.TryAcquireLeaseAsync(
                        "node-a",
                        secondNow,
                        policy.LeaseDuration));
            var second =
                await maintenance.RunCycleAsync(
                    secondLease,
                    secondNow,
                    policy);

            Assert.True(second.LeaseValid);
            Assert.Equal(1, second.RollupDeleted);

            var remaining =
                await store.QueryAsync(
                    new HistoricalMetricQuery(
                        "consumer.lag.total",
                        "prod",
                        "consumer_group",
                        "group-a",
                        now.AddDays(-11),
                        now,
                        MaxSeries: 1,
                        MaxPoints: 10));

            Assert.Empty(remaining.Series);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task PostgreSql_maintenance_uses_repeatable_read_snapshot()
    {
        var baseConnectionString =
            Environment.GetEnvironmentVariable(
                "KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(
                baseConnectionString))
        {
            return;
        }

        var schema =
            $"w63_snapshot_{Guid.NewGuid():N}";
        var adminBuilder =
            new NpgsqlConnectionStringBuilder(
                baseConnectionString);
        await using var admin =
            new NpgsqlConnection(
                adminBuilder.ConnectionString);
        await admin.OpenAsync();

        try
        {
            await using (var create =
                         admin.CreateCommand())
            {
                create.CommandText =
                    $"CREATE SCHEMA \"{schema}\"";
                await create.ExecuteNonQueryAsync();
            }

            var scopedBuilder =
                new NpgsqlConnectionStringBuilder(
                    baseConnectionString)
                {
                    SearchPath = schema,
                    Pooling = false,
                };
            var factory =
                new PostgreSqlHistoricalMetricsDbConnectionFactory(
                    scopedBuilder.ConnectionString);
            var maintenance =
                new AdoHistoricalMetricMaintenanceStore(
                    factory);

            await using var connection =
                await factory.OpenAsync();
            await using var transaction =
                await maintenance.BeginMaintenanceTransactionAsync(
                    connection,
                    CancellationToken.None);

            Assert.Equal(
                IsolationLevel.RepeatableRead,
                transaction.IsolationLevel);

            await transaction.RollbackAsync();
        }
        finally
        {
            await using var drop =
                admin.CreateCommand();
            drop.CommandText =
                $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public void Maintenance_has_hard_raw_batch_and_cycle_batch_caps()
    {
        Assert.InRange(
            AdoHistoricalMetricMaintenanceStore
                .MaxRawSamplesPerRollupBatch,
            1,
            10_000);
        Assert.InRange(
            AdoHistoricalMetricMaintenanceStore
                .MaxRollupBatchesPerCycle,
            1,
            HistoricalMetricMaintenancePolicy
                .HardMaxRollupWindowsPerCycle);

        var root =
            FindRepositoryRoot();
        var source =
            File.ReadAllText(
                Path.Combine(
                    root,
                    "src",
                    "backend",
                    "Infrastructure",
                    "Kafdeck.Infrastructure.Persistence",
                    "AdoHistoricalMetricMaintenanceStore.cs"));

        Assert.Contains(
            "LIMIT @raw_limit",
            source,
            StringComparison.Ordinal);
        Assert.Contains(
            "rollupBatches <",
            source,
            StringComparison.Ordinal);
        Assert.Contains(
            "rollupBatches++;",
            source,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Maintenance_commits_each_rollup_window_in_its_own_transaction()
    {
        var root =
            FindRepositoryRoot();
        var source =
            File.ReadAllText(
                Path.Combine(
                    root,
                    "src",
                    "backend",
                    "Infrastructure",
                    "Kafdeck.Infrastructure.Persistence",
                    "AdoHistoricalMetricMaintenanceStore.cs"));

        var loopIndex =
            source.IndexOf(
                "while (rolledWindows <",
                StringComparison.Ordinal);
        var transactionIndex =
            source.IndexOf(
                "await using var rollupTransaction",
                loopIndex,
                StringComparison.Ordinal);
        var commitIndex =
            source.IndexOf(
                "await rollupTransaction",
                transactionIndex,
                StringComparison.Ordinal);
        var retentionIndex =
            source.IndexOf(
                "long rollupDeleted = 0;",
                commitIndex,
                StringComparison.Ordinal);

        Assert.True(loopIndex >= 0);
        Assert.True(
            transactionIndex > loopIndex);
        Assert.True(
            commitIndex > transactionIndex);
        Assert.True(
            retentionIndex > commitIndex);
    }

    [Fact]
    public void Query_rejects_range_above_hard_cap()
    {
        var now = DateTimeOffset.UtcNow;

        var query = new HistoricalMetricQuery(
            "consumer.lag.total",
            "prod",
            "consumer_group",
            "group-a",
            now.AddDays(-32),
            now,
            MaxSeries: 1,
            MaxPoints: 10);

        Assert.Throws<ArgumentOutOfRangeException>(
            query.Validate);
    }

    private static HistoricalMetricMaintenancePolicy
        TestMaintenancePolicy() =>
        new(
            RawRetention: TimeSpan.FromHours(1),
            RollupRetention: TimeSpan.FromDays(7),
            RollupResolutionSeconds: 300,
            MaxRollupWindowsPerCycle: 24,
            MaxRollupDeletesPerCycle: 1_000,
            LeaseDuration: TimeSpan.FromMinutes(2),
            CycleInterval: TimeSpan.FromMinutes(1),
            MaxCycleDuration: TimeSpan.FromSeconds(30));

    private static HistoricalMetricStorePolicy TestPolicy() =>
        new(
            HistoricalMetricQuery.HardMaxRange,
            MaxSeriesPerQuery: 1_000,
            MaxPointsPerQuery: 50_000,
            MaxQueryDuration: TimeSpan.FromSeconds(10),
            MaxConcurrentQueries: 2);

    private sealed class BlockingHistoricalMetricsConnectionFactory :
        IHistoricalMetricsDbConnectionFactory
    {
        public bool SupportsSelectForUpdate => false;

        public async ValueTask<DbConnection> OpenAsync(
            CancellationToken cancellationToken = default)
        {
            await Task.Delay(
                    Timeout.InfiniteTimeSpan,
                    cancellationToken)
                .ConfigureAwait(false);

            throw new InvalidOperationException(
                "Blocking history factory unexpectedly completed.");
        }
    }

    private sealed class SharedMemoryHistoricalMetricsConnectionFactory :
        IHistoricalMetricsDbConnectionFactory
    {
        private readonly string _connectionString;

        public SharedMemoryHistoricalMetricsConnectionFactory(
            string connectionString)
        {
            _connectionString =
                connectionString;
        }

        public bool SupportsSelectForUpdate => false;

        public async ValueTask<DbConnection> OpenAsync(
            CancellationToken cancellationToken = default)
        {
            var connection =
                new SqliteConnection(
                    _connectionString);
            try
            {
                await connection
                    .OpenAsync(cancellationToken)
                    .ConfigureAwait(false);
                return connection;
            }
            catch
            {
                await connection
                    .DisposeAsync()
                    .ConfigureAwait(false);
                throw;
            }
        }
    }

    private static KafdeckOptions Load(
        IReadOnlyDictionary<string, string?> values) =>
        KafdeckConfigurationLoader.Load(
            new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build());

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current =
            new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(
                    Path.Combine(
                        current.FullName,
                        "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException(
            "Unable to locate Kafdeck repository root.");
    }

    private static void DeleteSqliteFiles(string path)
    {
        foreach (var candidate in new[]
                 {
                     path,
                     path + "-wal",
                     path + "-shm",
                 })
        {
            if (File.Exists(candidate))
            {
                File.Delete(candidate);
            }
        }
    }
}
