using Kafdeck.Api;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.Observability;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Consumers;
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
                now.AddHours(-1),
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

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task AppendAsync(
            IReadOnlyList<HistoricalMetricSample> samples,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

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
        IConsumerGroupReadPort
    {
        private readonly long _lag;

        public FakeConsumerGroupReadPort(
            long lag = 10)
        {
            _lag = lag;
        }
        public Task<ReadViewResult<IReadOnlyList<ConsumerGroupSummary>>>
            ListGroupsAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<IReadOnlyList<ConsumerGroupSummary>>
                    .Success(
                        [
                            new ConsumerGroupSummary(
                                "group-a",
                                ConsumerGroupState.Stable,
                                1,
                                false),
                        ]));

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
                CancellationToken cancellationToken) =>
            Task.FromResult(
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
