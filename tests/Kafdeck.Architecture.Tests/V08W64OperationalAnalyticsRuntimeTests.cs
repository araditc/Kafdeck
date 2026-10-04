using Kafdeck.Api;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.Observability;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Consumers;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W64OperationalAnalyticsRuntimeTests
{
    [Fact]
    public async Task Historical_trend_reads_W63_consumer_lag_series()
    {
        var now =
            DateTimeOffset.UtcNow;
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    [
                        new HistoricalMetricSeries(
                            new HistoricalMetricIdentity(
                                OperationalMetricHistoryNames
                                    .ConsumerLagTotal,
                                "prod",
                                "consumer_group",
                                "group-a"),
                            [
                                HistoricalMetricSample.Gauge(
                                    new HistoricalMetricIdentity(
                                        OperationalMetricHistoryNames
                                            .ConsumerLagTotal,
                                        "prod",
                                        "consumer_group",
                                        "group-a"),
                                    now.AddMinutes(-2),
                                    12,
                                    "test",
                                    "Stable"),
                                HistoricalMetricSample.Gauge(
                                    new HistoricalMetricIdentity(
                                        OperationalMetricHistoryNames
                                            .ConsumerLagTotal,
                                        "prod",
                                        "consumer_group",
                                        "group-a"),
                                    now.AddMinutes(-1),
                                    4,
                                    "test",
                                    "Stable"),
                            ]),
                    ],
                    Truncated: false,
                    LimitReason: null,
                    now.AddHours(-1),
                    now,
                    "test"));

        var service =
            new OperationalTrendService(
                store);
        var query =
            new OperationalTrendQuery(
                OperationalMetricKind.ConsumerLagTotal,
                new OperationalResourceIdentity(
                    "prod",
                    OperationalResourceKind.ConsumerGroup,
                    "group-a"),
                now.AddHours(-1),
                now,
                100);

        var result =
            await service.QueryAsync(
                query,
                CancellationToken.None);

        Assert.Equal(
            OperationalTrendState.Available,
            result.State);
        Assert.Equal(
            2,
            result.Points.Count);
        Assert.Equal(
            12,
            result.Points[0].Average);
        Assert.Equal(
            4,
            result.Points[1].Average);
        Assert.NotNull(
            store.LastQuery);
        Assert.Equal(
            "consumer_group",
            store.LastQuery!.ResourceKind);
    }

    [Fact]
    public async Task Slo_uses_only_complete_available_history()
    {
        var now =
            DateTimeOffset.UtcNow;
        var identity =
            new HistoricalMetricIdentity(
                OperationalMetricHistoryNames
                    .ConsumerLagTotal,
                "prod",
                "consumer_group",
                "group-a");
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    [
                        new HistoricalMetricSeries(
                            identity,
                            [
                                HistoricalMetricSample.Gauge(
                                    identity,
                                    now.AddMinutes(-2),
                                    5,
                                    "test",
                                    "Stable"),
                                HistoricalMetricSample.Gauge(
                                    identity,
                                    now.AddMinutes(-1),
                                    15,
                                    "test",
                                    "Stable"),
                            ]),
                    ],
                    false,
                    null,
                    now.AddHours(-1),
                    now,
                    "test"));
        var service =
            new OperationalTrendService(
                store);

        var result =
            await service.EvaluateSloAsync(
                new OperationalSloDefinition(
                    "consumer-lag",
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    MaximumGoodValue: 10,
                    TargetFraction: 0.9),
                now.AddMinutes(-3),
                now,
                100,
                CancellationToken.None);

        Assert.Equal(
            2,
            result.EvaluatedPoints);
        Assert.Equal(
            1,
            result.GoodPoints);
        Assert.Equal(
            0.5,
            result.ComplianceFraction);
        Assert.NotNull(
            result.BurnRate);
        Assert.Equal(
            5d,
            result.BurnRate!.Value,
            precision: 10);
        Assert.Equal(
            OperationalEvidenceState.Available,
            result.State);
    }

    [Fact]
    public async Task Trend_is_explicitly_unavailable_without_history_provider()
    {
        var now =
            DateTimeOffset.UtcNow;
        var service =
            new OperationalTrendService();

        var result =
            await service.QueryAsync(
                new OperationalTrendQuery(
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    now.AddMinutes(-30),
                    now,
                    100),
                CancellationToken.None);

        Assert.Equal(
            OperationalTrendState.Unavailable,
            result.State);
        Assert.Empty(
            result.Points);
    }

    [Fact]
    public async Task Live_consumer_lag_uses_Kafka_evidence_and_keeps_rate_unavailable()
    {
        var consumerPort =
            new FakeConsumerGroupReadPort();
        var consumers =
            new ConsumerExplorerService(
                consumerPort);
        var service =
            new OperationalAnalyticsRuntimeService(
                consumers,
                new UnavailableMetricsObservationPort());

        var query =
            new OperationalAnalyticsQuery(
                "prod",
                OperationalResourceKind.ConsumerGroup,
                "group-a",
                [
                    OperationalMetricKind.ConsumerLagTotal,
                    OperationalMetricKind.ConsumerConsumeRecordsPerSecond,
                ],
                8);

        var result =
            await service.QueryAsync(
                query,
                new ReadViewOperationContext(
                    DateTimeOffset.UtcNow
                        .AddSeconds(10),
                    8,
                    256 * 1024),
                CancellationToken.None);

        Assert.True(
            result.IsSuccess);
        var value =
            Assert.IsType<OperationalAnalyticsResult>(
                result.Value);
        Assert.Collection(
            value.Items,
            lag =>
            {
                Assert.Equal(
                    OperationalMetricKind.ConsumerLagTotal,
                    lag.Metric);
                Assert.Equal(
                    10d,
                    lag.Value);
                Assert.Equal(
                    OperationalEvidenceState.Available,
                    lag.State);
            },
            rate =>
            {
                Assert.Equal(
                    OperationalMetricKind.ConsumerConsumeRecordsPerSecond,
                    rate.Metric);
                Assert.Null(
                    rate.Value);
                Assert.Equal(
                    OperationalEvidenceState.Unavailable,
                    rate.State);
            });
    }

    [Fact]
    public async Task Live_consumer_lag_marks_imprecise_large_integer_as_partial()
    {
        var consumers =
            new ConsumerExplorerService(
                new FakeConsumerGroupReadPort(
                    9_007_199_254_740_993L));
        var service =
            new OperationalAnalyticsRuntimeService(
                consumers,
                new UnavailableMetricsObservationPort());

        var result =
            await service.QueryAsync(
                new OperationalAnalyticsQuery(
                    "prod",
                    OperationalResourceKind.ConsumerGroup,
                    "group-a",
                    [OperationalMetricKind.ConsumerLagTotal],
                    4),
                new ReadViewOperationContext(
                    DateTimeOffset.UtcNow.AddSeconds(10),
                    4,
                    128 * 1024),
                CancellationToken.None);

        var evidence =
            Assert.Single(
                Assert.IsType<OperationalAnalyticsResult>(
                    result.Value).Items);

        Assert.Equal(
            OperationalEvidenceState.Partial,
            evidence.State);
    }

    [Fact]
    public async Task Live_consumer_rate_marks_stale_provider_evidence_as_stale()
    {
        var now =
            DateTimeOffset.UtcNow;
        var consumers =
            new ConsumerExplorerService(
                new FakeConsumerGroupReadPort());
        var service =
            new OperationalAnalyticsRuntimeService(
                consumers,
                new StaticMetricsObservationPort(
                    new ConsumerRateObservation(
                        ProduceRecordsPerSecond: null,
                        ConsumeRecordsPerSecond: 5,
                        Window: TimeSpan.FromMinutes(1),
                        ObservedAt:
                            now.AddMinutes(-5),
                        Source: "test")),
                new FixedTimeProvider(now));

        var result =
            await service.QueryAsync(
                new OperationalAnalyticsQuery(
                    "prod",
                    OperationalResourceKind.ConsumerGroup,
                    "group-a",
                    [OperationalMetricKind.ConsumerConsumeRecordsPerSecond],
                    4),
                new ReadViewOperationContext(
                    now.AddSeconds(10),
                    4,
                    128 * 1024),
                CancellationToken.None);

        var evidence =
            Assert.Single(
                Assert.IsType<OperationalAnalyticsResult>(
                    result.Value).Items);

        Assert.Equal(
            OperationalEvidenceState.Stale,
            evidence.State);
    }

    [Fact]
    public async Task Trend_with_unknown_points_is_reported_partial()
    {
        var now =
            DateTimeOffset.UtcNow;
        var identity =
            new HistoricalMetricIdentity(
                OperationalMetricHistoryNames.ConsumerLagTotal,
                "prod",
                "consumer_group",
                "group-a");
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    [
                        new HistoricalMetricSeries(
                            identity,
                            [
                                new HistoricalMetricSample(
                                    identity,
                                    now.AddMinutes(-1),
                                    1,
                                    1,
                                    1,
                                    1,
                                    300,
                                    "legacy",
                                    "Unknown"),
                            ]),
                    ],
                    false,
                    null,
                    now.AddHours(-1),
                    now,
                    "test"));
        var service =
            new OperationalTrendService(
                store);

        var result =
            await service.QueryAsync(
                new OperationalTrendQuery(
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    now.AddHours(-1),
                    now,
                    10),
                CancellationToken.None);

        Assert.Equal(
            OperationalTrendState.Partial,
            result.State);
    }

    [Fact]
    public async Task Consumer_history_rejects_rounded_two_to_the_63_before_long_cast()
    {
        var now =
            DateTimeOffset.UtcNow;
        var identity =
            new HistoricalMetricIdentity(
                OperationalMetricHistoryNames.ConsumerLagTotal,
                "prod",
                "consumer_group",
                "group-a");
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    [
                        new HistoricalMetricSeries(
                            identity,
                            [
                                HistoricalMetricSample.Gauge(
                                    identity,
                                    now.AddMinutes(-1),
                                    (double)long.MaxValue,
                                    "test",
                                    "Partial"),
                            ]),
                    ],
                    false,
                    null,
                    now.AddHours(-1),
                    now,
                    "test"));
        var port =
            new HistoricalConsumerHistoryObservationPort(
                store,
                timeProvider:
                    new FixedTimeProvider(now));

        var result =
            await port.GetConsumerHistoryAsync(
                "prod",
                "group-a",
                new ReadViewOperationContext(
                    now.AddSeconds(10),
                    100,
                    128 * 1024),
                CancellationToken.None);

        Assert.True(
            result.IsSuccess);
        var point =
            Assert.Single(
                result.Value!);
        Assert.Null(
            point.TotalLag);
        Assert.Equal(
            "Partial",
            point.State);
    }

    [Fact]
    public async Task Slo_weights_rollups_by_sample_count_and_rejects_mixed_threshold_rollup()
    {
        var now =
            DateTimeOffset.UtcNow;
        var identity =
            new HistoricalMetricIdentity(
                OperationalMetricHistoryNames.ConsumerLagTotal,
                "prod",
                "consumer_group",
                "group-a");
        var goodRollup =
            new HistoricalMetricSample(
                identity,
                now.AddMinutes(-5),
                1,
                5,
                15,
                5,
                300,
                "rollup",
                "Stable",
                now.AddMinutes(-10),
                now.AddMinutes(-5));
        var badRaw =
            HistoricalMetricSample.Gauge(
                identity,
                now.AddMinutes(-1),
                20,
                "raw",
                "Stable");
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    [
                        new HistoricalMetricSeries(
                            identity,
                            [goodRollup, badRaw]),
                    ],
                    false,
                    null,
                    now.AddMinutes(-10),
                    now,
                    "test"));
        var service =
            new OperationalTrendService(
                store);

        var result =
            await service.EvaluateSloAsync(
                new OperationalSloDefinition(
                    "weighted",
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    10,
                    0.9),
                now.AddMinutes(-10),
                now,
                100,
                CancellationToken.None);

        Assert.Equal(
            6,
            result.EvaluatedPoints);
        Assert.Equal(
            5,
            result.GoodPoints);

        var mixed =
            new HistoricalMetricSample(
                identity,
                now.AddMinutes(-5),
                1,
                20,
                50,
                5,
                300,
                "rollup",
                "Stable",
                now.AddMinutes(-10),
                now.AddMinutes(-5));
        var mixedService =
            new OperationalTrendService(
                new FakeHistoricalMetricStore(
                    new HistoricalMetricQueryResult(
                        [
                            new HistoricalMetricSeries(
                                identity,
                                [mixed]),
                        ],
                        false,
                        null,
                        now.AddMinutes(-10),
                        now,
                        "test")));

        var ambiguous =
            await mixedService.EvaluateSloAsync(
                new OperationalSloDefinition(
                    "mixed",
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    10,
                    0.9),
                now.AddMinutes(-10),
                now.AddMinutes(-5),
                100,
                CancellationToken.None);

        Assert.Equal(
            OperationalEvidenceState.Partial,
            ambiguous.State);
        Assert.Equal(
            "aggregate_threshold_ambiguous",
            ambiguous.ReasonCode);
        Assert.Null(
            ambiguous.ComplianceFraction);
    }

    [Fact]
    public async Task Trend_rejects_explicit_query_beyond_configured_history_limits()
    {
        var now =
            DateTimeOffset.UtcNow;
        var policy =
            new HistoricalMetricStorePolicy(
                TimeSpan.FromMinutes(30),
                MaxSeriesPerQuery: 1,
                MaxPointsPerQuery: 50,
                MaxQueryDuration:
                    TimeSpan.FromSeconds(5),
                MaxConcurrentQueries: 1);
        var service =
            new OperationalTrendService(
                new FakeHistoricalMetricStore(
                    new HistoricalMetricQueryResult(
                        Array.Empty<HistoricalMetricSeries>(),
                        false,
                        null,
                        now.AddMinutes(-30),
                        now,
                        "test")),
                policy);

        Assert.Equal(
            50,
            service.DefaultMaxPoints);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => service.QueryAsync(
                new OperationalTrendQuery(
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    now.AddHours(-1),
                    now,
                    50),
                CancellationToken.None));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => service.QueryAsync(
                new OperationalTrendQuery(
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    now.AddMinutes(-20),
                    now,
                    51),
                CancellationToken.None));
    }

    [Fact]
    public async Task Consumer_history_clamps_fixed_query_to_configured_point_limit()
    {
        var now =
            DateTimeOffset.UtcNow;
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    Array.Empty<HistoricalMetricSeries>(),
                    false,
                    null,
                    now.AddMinutes(-30),
                    now,
                    "test"));
        var policy =
            new HistoricalMetricStorePolicy(
                TimeSpan.FromMinutes(30),
                MaxSeriesPerQuery: 1,
                MaxPointsPerQuery: 25,
                MaxQueryDuration:
                    TimeSpan.FromSeconds(5),
                MaxConcurrentQueries: 1);
        var port =
            new HistoricalConsumerHistoryObservationPort(
                store,
                policy,
                new FixedTimeProvider(now));

        var result =
            await port.GetConsumerHistoryAsync(
                "prod",
                "group-a",
                new ReadViewOperationContext(
                    now.AddSeconds(10),
                    500,
                    128 * 1024),
                CancellationToken.None);

        Assert.True(
            result.IsSuccess);
        Assert.NotNull(
            store.LastQuery);
        Assert.Equal(
            25,
            store.LastQuery!.MaxPoints);
        Assert.Equal(
            TimeSpan.FromMinutes(30),
            store.LastQuery.ToUtc -
            store.LastQuery.FromUtc);
    }

    [Fact]
    public async Task Truncated_history_is_partial_and_cannot_produce_available_slo()
    {
        var now =
            DateTimeOffset.UtcNow;
        var identity =
            new HistoricalMetricIdentity(
                OperationalMetricHistoryNames.ConsumerLagTotal,
                "prod",
                "consumer_group",
                "group-a");
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    [
                        new HistoricalMetricSeries(
                            identity,
                            [
                                HistoricalMetricSample.Gauge(
                                    identity,
                                    now.AddMinutes(-2),
                                    5,
                                    "test",
                                    "Stable"),
                                HistoricalMetricSample.Gauge(
                                    identity,
                                    now.AddMinutes(-1),
                                    6,
                                    "test",
                                    "Stable"),
                            ]),
                    ],
                    Truncated: true,
                    LimitReason: "max_points",
                    now.AddMinutes(-3),
                    now,
                    "test"));
        var service =
            new OperationalTrendService(store);

        var trend =
            await service.QueryAsync(
                new OperationalTrendQuery(
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    now.AddMinutes(-3),
                    now,
                    2),
                CancellationToken.None);

        Assert.Equal(
            OperationalTrendState.Partial,
            trend.State);

        var slo =
            await service.EvaluateSloAsync(
                new OperationalSloDefinition(
                    "lag-slo",
                    OperationalMetricKind.ConsumerLagTotal,
                    trend.Resource,
                    10,
                    0.99),
                now.AddMinutes(-3),
                now,
                2,
                CancellationToken.None);

        Assert.Equal(
            OperationalEvidenceState.Partial,
            slo.State);
    }

    [Fact]
    public async Task Sparse_history_window_is_partial_slo_evidence()
    {
        var now =
            DateTimeOffset.UtcNow;
        var identity =
            new HistoricalMetricIdentity(
                OperationalMetricHistoryNames.ConsumerLagTotal,
                "prod",
                "consumer_group",
                "group-a");
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    [
                        new HistoricalMetricSeries(
                            identity,
                            [
                                HistoricalMetricSample.Gauge(
                                    identity,
                                    now.AddMinutes(-30),
                                    5,
                                    "test",
                                    "Stable"),
                            ]),
                    ],
                    false,
                    null,
                    now.AddHours(-1),
                    now,
                    "test"));
        var service =
            new OperationalTrendService(store);

        var slo =
            await service.EvaluateSloAsync(
                new OperationalSloDefinition(
                    "lag-slo",
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    10,
                    0.99),
                now.AddHours(-1),
                now,
                100,
                CancellationToken.None);

        Assert.Equal(
            OperationalEvidenceState.Partial,
            slo.State);
        Assert.Equal(
            "partial_slo_evidence",
            slo.ReasonCode);
    }

    [Fact]
    public async Task Trend_preserves_stale_provider_state()
    {
        var now =
            DateTimeOffset.UtcNow;
        var identity =
            new HistoricalMetricIdentity(
                OperationalMetricHistoryNames.ConsumerLagTotal,
                "prod",
                "consumer_group",
                "group-a");
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    [
                        new HistoricalMetricSeries(
                            identity,
                            [
                                HistoricalMetricSample.Gauge(
                                    identity,
                                    now.AddMinutes(-1),
                                    5,
                                    "test",
                                    "Stale"),
                            ]),
                    ],
                    false,
                    null,
                    now.AddMinutes(-2),
                    now,
                    "test"));
        var service =
            new OperationalTrendService(store);

        var trend =
            await service.QueryAsync(
                new OperationalTrendQuery(
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    now.AddMinutes(-2),
                    now,
                    10),
                CancellationToken.None);

        Assert.Equal(
            OperationalTrendState.Partial,
            trend.State);
        Assert.Equal(
            OperationalEvidenceState.Stale,
            Assert.Single(trend.Points).State);
    }

    [Fact]
    public async Task History_provider_failure_is_explicitly_unavailable()
    {
        var now =
            DateTimeOffset.UtcNow;
        var service =
            new OperationalTrendService(
                new ThrowingHistoricalMetricStore());

        var trend =
            await service.QueryAsync(
                new OperationalTrendQuery(
                    OperationalMetricKind.ConsumerLagTotal,
                    new OperationalResourceIdentity(
                        "prod",
                        OperationalResourceKind.ConsumerGroup,
                        "group-a"),
                    now.AddMinutes(-5),
                    now,
                    10),
                CancellationToken.None);

        Assert.Equal(
            OperationalTrendState.Unavailable,
            trend.State);
        Assert.Empty(
            trend.Points);
    }

    [Fact]
    public async Task Sampler_uses_fenced_lease_rotates_groups_and_marks_imprecise_lag_partial()
    {
        var now =
            new DateTimeOffset(
                2026,
                9,
                28,
                20,
                0,
                0,
                TimeSpan.Zero);
        var time =
            new MutableTimeProvider(now);
        var consumerPort =
            new FakeConsumerGroupReadPort(
                9_007_199_254_740_993L,
                ["group-a", "group-b", "group-c"]);
        var consumers =
            new ConsumerExplorerService(
                consumerPort);
        var store =
            new FakeHistoricalMetricStore(
                new HistoricalMetricQueryResult(
                    Array.Empty<HistoricalMetricSeries>(),
                    false,
                    null,
                    now.AddMinutes(-5),
                    now,
                    "test"));
        var leases =
            new FakeSamplingLeaseStore();
        var options =
            new KafdeckOptions(
                new DeploymentOptions(
                    "http://127.0.0.1:8080",
                    null),
                [
                    new ClusterProfile(
                        "prod",
                        ["localhost:9092"],
                        KafkaSecurityProtocol.Plaintext,
                        null,
                        null),
                ]);
        var service =
            new ConsumerLagHistorySamplingHostedService(
                options,
                consumers,
                consumerPort,
                store,
                leases,
                new ConsumerLagHistorySamplingPolicy(
                    TimeSpan.FromMinutes(1),
                    MaxGroupsPerCluster: 1,
                    MaxConcurrentGroups: 1),
                NullLogger<
                    ConsumerLagHistorySamplingHostedService>.Instance,
                time);

        await service.SampleOnceAsync(
            CancellationToken.None);

        time.Advance(
            TimeSpan.FromMinutes(1));
        await service.SampleOnceAsync(
            CancellationToken.None);

        Assert.Equal(
            2,
            consumerPort.RequestedGroups
                .Distinct(StringComparer.Ordinal)
                .Count());
        Assert.All(
            store.AppendedSamples,
            sample =>
                Assert.Equal(
                    "Partial",
                    sample.State));

        leases.DenyNext = true;
        var readsBefore =
            consumerPort.RequestedGroups.Count;
        time.Advance(
            TimeSpan.FromMinutes(1));
        await service.SampleOnceAsync(
            CancellationToken.None);

        Assert.Equal(
            readsBefore,
            consumerPort.RequestedGroups.Count);
    }

    private sealed class FakeHistoricalMetricStore :
        IHistoricalMetricStore
    {
        private readonly HistoricalMetricQueryResult
            _result;

        public FakeHistoricalMetricStore(
            HistoricalMetricQueryResult result)
        {
            _result = result;
        }

        public HistoricalMetricQuery? LastQuery { get; private set; }

        public List<HistoricalMetricSample> AppendedSamples { get; } =
            new();

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task AppendAsync(
            IReadOnlyList<HistoricalMetricSample> samples,
            CancellationToken cancellationToken = default)
        {
            AppendedSamples.AddRange(
                samples);
            return Task.CompletedTask;
        }

        public Task<HistoricalMetricQueryResult> QueryAsync(
            HistoricalMetricQuery query,
            CancellationToken cancellationToken = default)
        {
            LastQuery = query;
            return Task.FromResult(
                _result);
        }

        public Task<HistoricalMetricRetentionResult> DeleteExpiredAsync(
            DateTimeOffset rawBeforeUtc,
            DateTimeOffset rollupBeforeUtc,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new HistoricalMetricRetentionResult(
                    0,
                    0));
    }

    private sealed class ThrowingHistoricalMetricStore :
        IHistoricalMetricStore
    {
        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task AppendAsync(
            IReadOnlyList<HistoricalMetricSample> samples,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<HistoricalMetricQueryResult> QueryAsync(
            HistoricalMetricQuery query,
            CancellationToken cancellationToken = default) =>
            throw new TimeoutException(
                "test timeout");

        public Task<HistoricalMetricRetentionResult> DeleteExpiredAsync(
            DateTimeOffset rawBeforeUtc,
            DateTimeOffset rollupBeforeUtc,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new HistoricalMetricRetentionResult(
                    0,
                    0));
    }

    private sealed class FakeSamplingLeaseStore :
        IHistoricalMetricSamplingLeaseStore
    {
        public bool DenyNext { get; set; }

        public Task<HistoricalMetricMaintenanceLease?>
            TryAcquireSamplingLeaseAsync(
                string ownerId,
                DateTimeOffset nowUtc,
                TimeSpan leaseDuration,
                CancellationToken cancellationToken = default)
        {
            if (DenyNext)
            {
                DenyNext = false;
                return Task.FromResult<
                    HistoricalMetricMaintenanceLease?>(
                    null);
            }

            return Task.FromResult<
                HistoricalMetricMaintenanceLease?>(
                new HistoricalMetricMaintenanceLease(
                    ownerId,
                    1,
                    nowUtc.Add(leaseDuration)));
        }
    }

    private sealed class StaticMetricsObservationPort :
        IMetricsObservationPort
    {
        private readonly ConsumerRateObservation _value;

        public StaticMetricsObservationPort(
            ConsumerRateObservation value)
        {
            _value = value;
        }

        public Task<ReadViewResult<ConsumerRateObservation>>
            GetConsumerRateAsync(
                string clusterId,
                string groupId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<ConsumerRateObservation>
                    .Success(_value));
    }

    private sealed class MutableTimeProvider :
        TimeProvider
    {
        private DateTimeOffset _utcNow;

        public MutableTimeProvider(
            DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public override DateTimeOffset GetUtcNow() =>
            _utcNow;

        public void Advance(
            TimeSpan value)
        {
            _utcNow =
                _utcNow.Add(value);
        }
    }

    private sealed class FixedTimeProvider :
        TimeProvider
    {
        private readonly DateTimeOffset _utcNow;

        public FixedTimeProvider(
            DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public override DateTimeOffset GetUtcNow() =>
            _utcNow;
    }

    private sealed class FakeConsumerGroupReadPort :
        IConsumerGroupReadPort,
        IConsumerGroupSamplingReadPort
    {
        private readonly long _lag;
        private readonly IReadOnlyList<string> _groups;

        public FakeConsumerGroupReadPort(
            long lag = 10,
            IReadOnlyList<string>? groups = null)
        {
            _lag = lag;
            _groups =
                groups ??
                ["group-a"];
        }

        public List<string> RequestedGroups { get; } =
            new();
        public Task<ReadViewResult<ConsumerGroupPage>>
            ListGroupPageAsync(
                string clusterId,
                string? afterGroupId,
                int maxItems,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken)
        {
            var page =
                _groups
                    .Where(group =>
                        afterGroupId is null ||
                        string.CompareOrdinal(
                            group,
                            afterGroupId) > 0)
                    .OrderBy(group =>
                        group,
                        StringComparer.Ordinal)
                    .Take(maxItems + 1)
                    .ToArray();
            var items =
                page
                    .Take(maxItems)
                    .Select(group =>
                        new ConsumerGroupSummary(
                            group,
                            ConsumerGroupState.Stable,
                            1,
                            false))
                    .ToArray();
            var nextCursor =
                page.Length > maxItems
                    ? items[^1].GroupId
                    : null;

            return Task.FromResult(
                ReadViewResult<ConsumerGroupPage>
                    .Success(
                        new ConsumerGroupPage(
                            items,
                            nextCursor)));
        }

        public Task<ReadViewResult<IReadOnlyList<ConsumerGroupSummary>>>
            ListGroupsAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<IReadOnlyList<ConsumerGroupSummary>>
                    .Success(
                        _groups
                            .Select(group =>
                                new ConsumerGroupSummary(
                                    group,
                                    ConsumerGroupState.Stable,
                                    1,
                                    false))
                            .ToArray()));

        public Task<ReadViewResult<ConsumerGroupDetail>>
            GetGroupAsync(
                string clusterId,
                string groupId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<ConsumerGroupDetail>.Success(
                    new ConsumerGroupDetail(
                        groupId,
                        ConsumerGroupState.Stable,
                        null,
                        null,
                        null,
                        Array.Empty<ConsumerMemberProjection>())));

        public Task<ReadViewResult<IReadOnlyList<ConsumerOffsetProjection>>>
            GetOffsetsAsync(
                string clusterId,
                string groupId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken)
        {
            RequestedGroups.Add(
                groupId);

            return Task.FromResult(
                ReadViewResult<IReadOnlyList<ConsumerOffsetProjection>>
                    .Success(
                        [
                            new ConsumerOffsetProjection(
                                "orders",
                                0,
                                90,
                                100,
                                _lag,
                                ConsumerOffsetState.Observed),
                        ]));
        }
    }
}
