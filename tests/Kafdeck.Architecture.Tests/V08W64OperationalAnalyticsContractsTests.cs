using Kafdeck.Core.Observability;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W64OperationalAnalyticsContractsTests
{
    [Fact]
    public void Available_zero_is_valid_evidence()
    {
        var evidence =
            new OperationalMetricEvidence(
                OperationalMetricKind.ConsumerLagTotal,
                new OperationalResourceIdentity(
                    "prod",
                    OperationalResourceKind.ConsumerGroup,
                    "group-a"),
                Value: 0,
                ObservedAtUtc:
                    new DateTimeOffset(
                        2026,
                        9,
                        28,
                        12,
                        0,
                        0,
                        TimeSpan.Zero),
                Window: null,
                Source: "kafka-admin",
                State:
                    OperationalEvidenceState.Available);

        evidence.Validate();

        Assert.Equal(0, evidence.Value);
    }

    [Theory]
    [InlineData(OperationalEvidenceState.Unavailable)]
    [InlineData(OperationalEvidenceState.Unknown)]
    public void Missing_evidence_cannot_be_fabricated_as_zero(
        OperationalEvidenceState state)
    {
        var evidence =
            new OperationalMetricEvidence(
                OperationalMetricKind.ConsumerLagTotal,
                new OperationalResourceIdentity(
                    "prod",
                    OperationalResourceKind.ConsumerGroup,
                    "group-a"),
                Value: 0,
                ObservedAtUtc: null,
                Window: null,
                Source: "provider-unavailable",
                State: state);

        Assert.Throws<ArgumentException>(
            evidence.Validate);
    }

    [Fact]
    public void Rate_evidence_requires_bounded_window()
    {
        var evidence =
            new OperationalMetricEvidence(
                OperationalMetricKind.ConsumerConsumeRecordsPerSecond,
                new OperationalResourceIdentity(
                    "prod",
                    OperationalResourceKind.ConsumerGroup,
                    "group-a"),
                Value: 10,
                ObservedAtUtc:
                    DateTimeOffset.UtcNow,
                Window: null,
                Source: "metrics-provider",
                State:
                    OperationalEvidenceState.Available);

        Assert.Throws<ArgumentException>(
            evidence.Validate);
    }

    [Fact]
    public void Partial_evidence_still_requires_real_numeric_evidence()
    {
        var evidence =
            new OperationalMetricEvidence(
                OperationalMetricKind.TopicRecordsInPerSecond,
                new OperationalResourceIdentity(
                    "prod",
                    OperationalResourceKind.Topic,
                    "orders"),
                Value: null,
                ObservedAtUtc:
                    DateTimeOffset.UtcNow,
                Window:
                    TimeSpan.FromMinutes(1),
                Source: "broker-metrics",
                State:
                    OperationalEvidenceState.Partial);

        Assert.Throws<ArgumentException>(
            evidence.Validate);
    }

    [Fact]
    public void Query_rejects_duplicate_metric_selection()
    {
        var query =
            new OperationalAnalyticsQuery(
                "prod",
                OperationalResourceKind.ConsumerGroup,
                "group-a",
                [
                    OperationalMetricKind.ConsumerLagTotal,
                    OperationalMetricKind.ConsumerLagTotal,
                ],
                MaxItems: 100);

        Assert.Throws<ArgumentException>(
            query.Validate);
    }

    [Fact]
    public void Query_rejects_resource_id_without_kind()
    {
        var query =
            new OperationalAnalyticsQuery(
                "prod",
                ResourceKind: null,
                ResourceId: "group-a",
                [
                    OperationalMetricKind.ConsumerLagTotal,
                ],
                MaxItems: 100);

        Assert.Throws<ArgumentException>(
            query.Validate);
    }

    [Fact]
    public void Evidence_rejects_metric_resource_mismatch()
    {
        var evidence =
            new OperationalMetricEvidence(
                OperationalMetricKind.BrokerBytesInPerSecond,
                new OperationalResourceIdentity(
                    "prod",
                    OperationalResourceKind.Topic,
                    "orders"),
                Value: 10,
                ObservedAtUtc:
                    DateTimeOffset.UtcNow,
                Window:
                    TimeSpan.FromMinutes(1),
                Source: "broker-metrics",
                State:
                    OperationalEvidenceState.Available);

        Assert.Throws<ArgumentException>(
            evidence.Validate);
    }

    [Fact]
    public void Query_rejects_metric_resource_mismatch()
    {
        var query =
            new OperationalAnalyticsQuery(
                "prod",
                OperationalResourceKind.Topic,
                "orders",
                [
                    OperationalMetricKind.ConsumerLagTotal,
                ],
                MaxItems: 100);

        Assert.Throws<ArgumentException>(
            query.Validate);
    }

    [Fact]
    public void Complete_targeted_result_requires_explicit_metric_coverage()
    {
        var query =
            new OperationalAnalyticsQuery(
                "prod",
                OperationalResourceKind.ConsumerGroup,
                "group-a",
                [
                    OperationalMetricKind.ConsumerLagTotal,
                ],
                MaxItems: 100);

        var result =
            new OperationalAnalyticsResult(
                Items: Array.Empty<OperationalMetricEvidence>(),
                Truncated: false,
                LimitReason: null,
                RequestedMetrics:
                    query.Metrics);

        Assert.Throws<ArgumentException>(
            () => result.Validate(query));
    }

    [Fact]
    public void Complete_untargeted_result_requires_metric_coverage_per_resource()
    {
        var query =
            new OperationalAnalyticsQuery(
                "prod",
                OperationalResourceKind.ConsumerGroup,
                ResourceId: null,
                [
                    OperationalMetricKind.ConsumerLagTotal,
                    OperationalMetricKind.ConsumerConsumeRecordsPerSecond,
                ],
                MaxItems: 100);

        var resource =
            new OperationalResourceIdentity(
                "prod",
                OperationalResourceKind.ConsumerGroup,
                "group-a");
        var lag =
            new OperationalMetricEvidence(
                OperationalMetricKind.ConsumerLagTotal,
                resource,
                Value: 10,
                ObservedAtUtc:
                    DateTimeOffset.UtcNow,
                Window: null,
                Source: "kafka-admin",
                State:
                    OperationalEvidenceState.Available);

        var result =
            new OperationalAnalyticsResult(
                Items: [lag],
                Truncated: false,
                LimitReason: null,
                RequestedMetrics:
                    query.Metrics);

        Assert.Throws<ArgumentException>(
            () => result.Validate(query));
    }

    [Fact]
    public void Result_rejects_silent_truncation_or_cap_overrun()
    {
        var query =
            new OperationalAnalyticsQuery(
                "prod",
                OperationalResourceKind.ConsumerGroup,
                ResourceId: null,
                [
                    OperationalMetricKind.ConsumerLagTotal,
                ],
                MaxItems: 1);

        var evidence =
            new OperationalMetricEvidence(
                OperationalMetricKind.ConsumerLagTotal,
                new OperationalResourceIdentity(
                    "prod",
                    OperationalResourceKind.ConsumerGroup,
                    "group-a"),
                Value: 1,
                ObservedAtUtc:
                    DateTimeOffset.UtcNow,
                Window: null,
                Source: "kafka-admin",
                State:
                    OperationalEvidenceState.Available);

        var overCap =
            new OperationalAnalyticsResult(
                Items: [evidence, evidence],
                Truncated: true,
                LimitReason: "max_items",
                RequestedMetrics:
                    query.Metrics);

        Assert.Throws<ArgumentException>(
            () => overCap.Validate(query));

        var silentTruncation =
            new OperationalAnalyticsResult(
                Items: [evidence],
                Truncated: true,
                LimitReason: null,
                RequestedMetrics:
                    query.Metrics);

        Assert.Throws<ArgumentException>(
            () => silentTruncation.Validate(query));
    }

    [Fact]
    public void Query_enforces_hard_item_cap()
    {
        var query =
            new OperationalAnalyticsQuery(
                "prod",
                OperationalResourceKind.ConsumerGroup,
                ResourceId: null,
                [
                    OperationalMetricKind.ConsumerLagTotal,
                ],
                MaxItems:
                    OperationalAnalyticsQuery.HardMaxItems + 1);

        Assert.Throws<ArgumentOutOfRangeException>(
            query.Validate);
    }
}
