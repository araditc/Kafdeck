using Kafdeck.Core.Consumers;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Modules.Consumers;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ConsumerDiagnosticsServiceTests
{
    [Fact]
    public void Empty_group_is_inactive_without_metrics_or_history()
    {
        var projection = ConsumerDiagnosticsService.Evaluate(
            Group(ConsumerGroupState.Empty),
            Lag(0),
            rates: null,
            history: null,
            TimeSpan.FromMinutes(1),
            metricsAvailable: false,
            historyAvailable: false);

        Assert.Equal(ConsumerDiagnosticState.Inactive, projection.State);
        Assert.Contains(projection.Evidence, item => item.Code == "consumer_group_inactive");
    }

    [Fact]
    public void Complete_zero_lag_is_healthy()
    {
        var projection = ConsumerDiagnosticsService.Evaluate(
            Group(ConsumerGroupState.Stable),
            Lag(0),
            rates: null,
            history: null,
            TimeSpan.FromMinutes(1),
            metricsAvailable: false,
            historyAvailable: false);

        Assert.Equal(ConsumerDiagnosticState.Healthy, projection.State);
        Assert.Contains(projection.Evidence, item => item.Code == "consumer_zero_lag");
    }

    [Fact]
    public void Positive_consume_rate_is_active_with_lag()
    {
        var projection = ConsumerDiagnosticsService.Evaluate(
            Group(ConsumerGroupState.Stable),
            Lag(50),
            new ConsumerRateObservation(
                ProduceRecordsPerSecond: 20,
                ConsumeRecordsPerSecond: 10,
                TimeSpan.FromMinutes(1),
                DateTimeOffset.UtcNow,
                "fixture"),
            history: null,
            TimeSpan.FromMinutes(1),
            metricsAvailable: true,
            historyAvailable: false);

        Assert.Equal(ConsumerDiagnosticState.ActiveWithLag, projection.State);
    }

    [Fact]
    public void Stalled_requires_zero_consume_rate_and_non_decreasing_history_window()
    {
        var now = DateTimeOffset.UtcNow;
        var projection = ConsumerDiagnosticsService.Evaluate(
            Group(ConsumerGroupState.Stable),
            Lag(100),
            new ConsumerRateObservation(
                ProduceRecordsPerSecond: 5,
                ConsumeRecordsPerSecond: 0,
                TimeSpan.FromMinutes(1),
                now,
                "fixture"),
            [
                new ConsumerHistoryObservation(now.AddMinutes(-2), "Stable", 90, "fixture"),
                new ConsumerHistoryObservation(now.AddMinutes(-1), "Stable", 95, "fixture"),
                new ConsumerHistoryObservation(now, "Stable", 100, "fixture"),
            ],
            TimeSpan.FromMinutes(1));

        Assert.Equal(ConsumerDiagnosticState.Stalled, projection.State);
        Assert.Contains(projection.Evidence, item => item.Code == "consumer_zero_consume_rate");
        Assert.Contains(projection.Evidence, item => item.Code == "consumer_non_decreasing_lag_history");
    }

    [Fact]
    public void Missing_metrics_or_history_never_fabricates_stalled_state()
    {
        var projection = ConsumerDiagnosticsService.Evaluate(
            Group(ConsumerGroupState.Stable),
            Lag(100),
            rates: null,
            history: null,
            TimeSpan.FromMinutes(1),
            metricsAvailable: false,
            historyAvailable: false);

        Assert.Equal(ConsumerDiagnosticState.Unknown, projection.State);
        Assert.Contains(projection.Limitations, item => item.Code == "consumer_metrics_unavailable");
        Assert.Contains(projection.Limitations, item => item.Code == "consumer_history_unavailable");
    }

    [Fact]
    public void Decreasing_lag_history_prevents_stalled_classification()
    {
        var now = DateTimeOffset.UtcNow;
        var projection = ConsumerDiagnosticsService.Evaluate(
            Group(ConsumerGroupState.Stable),
            Lag(80),
            new ConsumerRateObservation(
                ProduceRecordsPerSecond: 0,
                ConsumeRecordsPerSecond: 0,
                TimeSpan.FromMinutes(1),
                now,
                "fixture"),
            [
                new ConsumerHistoryObservation(now.AddMinutes(-2), "Stable", 100, "fixture"),
                new ConsumerHistoryObservation(now, "Stable", 80, "fixture"),
            ],
            TimeSpan.FromMinutes(1));

        Assert.Equal(ConsumerDiagnosticState.Unknown, projection.State);
        Assert.DoesNotContain(projection.Evidence, item => item.Code == "consumer_non_decreasing_lag_history");
    }

    [Fact]
    public async Task Default_optional_providers_report_not_configured()
    {
        var operation = new Kafdeck.Core.ReadViews.ReadViewOperationContext(
            DateTimeOffset.UtcNow.AddSeconds(5));

        var metrics = await new UnavailableMetricsObservationPort()
            .GetConsumerRateAsync("prod", "group-a", operation, CancellationToken.None);
        var history = await new UnavailableHistoryObservationPort()
            .GetConsumerHistoryAsync("prod", "group-a", operation, CancellationToken.None);

        Assert.False(metrics.IsSuccess);
        Assert.Equal(Kafdeck.Core.ReadViews.ReadViewFailureCategory.NotConfigured, metrics.Failure!.Category);
        Assert.False(history.IsSuccess);
        Assert.Equal(Kafdeck.Core.ReadViews.ReadViewFailureCategory.NotConfigured, history.Failure!.Category);
    }

    private static ConsumerGroupDetail Group(ConsumerGroupState state) =>
        new(
            "group-a",
            state,
            "consumer",
            "range",
            "broker:9092 (1)",
            Array.Empty<ConsumerMemberProjection>());

    private static ConsumerLagProjection Lag(long? totalLag) =>
        new(
            "group-a",
            totalLag.HasValue
                ? [new ConsumerOffsetProjection("payments", 0, 10, 10 + totalLag.Value, totalLag, ConsumerOffsetState.Observed)]
                : Array.Empty<ConsumerOffsetProjection>(),
            totalLag,
            IsPartial: false,
            Array.Empty<Kafdeck.Core.ReadViews.ReadViewLimitation>());
}
