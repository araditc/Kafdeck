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
                        2);
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
                State: "Stable");
            var newRollup = new HistoricalMetricSample(
                identity,
                now.AddDays(-1),
                40,
                90,
                250,
                4,
                ResolutionSeconds: 300,
                Source: "rollup",
                State: "Stable");

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
                    'Stable' AS state
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
