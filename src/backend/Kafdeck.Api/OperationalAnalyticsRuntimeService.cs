using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.Observability;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Consumers;

namespace Kafdeck.Api;

public sealed class HistoricalConsumerHistoryObservationPort :
    IHistoryObservationPort
{
    private readonly IHistoricalMetricStore _store;
    private readonly TimeProvider _timeProvider;

    public HistoricalConsumerHistoryObservationPort(
        IHistoricalMetricStore store,
        TimeProvider? timeProvider = null)
    {
        _store = store ??
            throw new ArgumentNullException(nameof(store));
        _timeProvider = timeProvider ??
            TimeProvider.System;
    }

    public async Task<ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>>
        GetConsumerHistoryAsync(
            string clusterId,
            string groupId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentNullException.ThrowIfNull(operation);

        var now =
            _timeProvider.GetUtcNow();
        var from =
            now.AddHours(-1);
        var remaining =
            operation.DeadlineUtc -
            now;

        if (remaining <= TimeSpan.Zero)
        {
            return ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>
                .Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Timeout,
                        "consumer_history_deadline_expired",
                        "Consumer history deadline expired before the provider query.",
                        true));
        }

        using var deadline =
            CancellationTokenSource
                .CreateLinkedTokenSource(
                    cancellationToken);
        deadline.CancelAfter(
            remaining);

        try
        {
            var result =
                await _store.QueryAsync(
                        new HistoricalMetricQuery(
                            OperationalMetricHistoryNames
                                .ConsumerLagTotal,
                            clusterId,
                            "consumer_group",
                            groupId,
                            from,
                            now,
                            MaxSeries: 1,
                            MaxPoints:
                                Math.Min(
                                    operation.MaxItems,
                                    512)),
                        deadline.Token)
                    .ConfigureAwait(false);

            var history =
                result.Series
                    .SelectMany(series =>
                        series.Points)
                    .OrderBy(point =>
                        point.ObservedAtUtc)
                    .Take(
                        operation.MaxItems)
                    .Select(point =>
                    {
                        var average =
                            point.Average;
                        long? totalLag = null;
                        var state =
                            point.State ??
                            "Stable";

                        if (double.IsFinite(average) &&
                            average >= 0 &&
                            average <= long.MaxValue)
                        {
                            totalLag =
                                checked(
                                    (long)Math.Round(
                                        average,
                                        MidpointRounding
                                            .AwayFromZero));

                            if (average >
                                    9_007_199_254_740_992d &&
                                string.Equals(
                                    state,
                                    "Stable",
                                    StringComparison.Ordinal))
                            {
                                state =
                                    "Partial";
                            }
                        }
                        else
                        {
                            state =
                                "Partial";
                        }

                        return new ConsumerHistoryObservation(
                            point.ObservedAtUtc,
                            state,
                            totalLag,
                            point.Source);
                    })
                    .ToArray();

            var limitations =
                result.Truncated
                    ? new[]
                    {
                        new ReadViewLimitation(
                            "consumer_history_truncated",
                            result.LimitReason ??
                            "Consumer history was truncated by the configured provider limits."),
                    }
                    : Array.Empty<ReadViewLimitation>();

            return ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>
                .Success(
                    Array.AsReadOnly(
                        history),
                    limitations);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>
                .Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Timeout,
                        "consumer_history_timeout",
                        "Consumer history query timed out.",
                        true));
        }
        catch (TimeoutException)
        {
            return ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>
                .Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Timeout,
                        "consumer_history_timeout",
                        "Consumer history query timed out.",
                        true));
        }
        catch (Exception)
        {
            return ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>
                .Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Unavailable,
                        "consumer_history_unavailable",
                        "Consumer history provider is unavailable.",
                        true));
        }
    }
}

public sealed class OperationalAnalyticsRuntimeService :
    IOperationalAnalyticsObservationPort
{
    private readonly ConsumerExplorerService _consumers;
    private readonly IMetricsObservationPort _metrics;
    private readonly TimeProvider _timeProvider;

    public OperationalAnalyticsRuntimeService(
        ConsumerExplorerService consumers,
        IMetricsObservationPort metrics,
        TimeProvider? timeProvider = null)
    {
        _consumers = consumers ??
            throw new ArgumentNullException(nameof(consumers));
        _metrics = metrics ??
            throw new ArgumentNullException(nameof(metrics));
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
    }

    public async Task<ReadViewResult<OperationalAnalyticsResult>>
        QueryAsync(
            OperationalAnalyticsQuery query,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(operation);
        query.Validate();

        if (query.ResourceKind is null ||
            string.IsNullOrWhiteSpace(
                query.ResourceId))
        {
            return ReadViewResult<OperationalAnalyticsResult>
                .Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.InvalidRequest,
                        "analytics_target_required",
                        "Operational analytics runtime queries currently require one explicit resource.",
                        false));
        }

        var resource =
            new OperationalResourceIdentity(
                query.ClusterId,
                query.ResourceKind.Value,
                query.ResourceId);
        resource.Validate();

        var items =
            new List<OperationalMetricEvidence>(
                query.Metrics.Count);

        foreach (var metric in query.Metrics)
        {
            items.Add(
                await ObserveOneAsync(
                        metric,
                        resource,
                        operation,
                        cancellationToken)
                    .ConfigureAwait(false));
        }

        var result =
            new OperationalAnalyticsResult(
                Array.AsReadOnly(
                    items.ToArray()),
                Truncated: false,
                LimitReason: null,
                query.Metrics.ToArray());
        result.Validate(query);

        return ReadViewResult<OperationalAnalyticsResult>
            .Success(result);
    }

    private async Task<OperationalMetricEvidence>
        ObserveOneAsync(
            OperationalMetricKind metric,
            OperationalResourceIdentity resource,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        if (resource.Kind ==
                OperationalResourceKind.ConsumerGroup &&
            metric ==
                OperationalMetricKind.ConsumerLagTotal)
        {
            var lag =
                await _consumers
                    .GetLagAsync(
                        resource.ClusterId,
                        resource.ResourceId,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (!lag.IsSuccess ||
                lag.Value is null)
            {
                return Unavailable(
                    metric,
                    resource,
                    "consumer_lag_unavailable");
            }

            if (lag.Value.TotalLag is not long total)
            {
                return Unknown(
                    metric,
                    resource,
                    "consumer_lag_unknown");
            }

            var precise =
                total <=
                9_007_199_254_740_992L;

            return new OperationalMetricEvidence(
                metric,
                resource,
                total,
                _timeProvider.GetUtcNow(),
                Window: null,
                "consumer-lag-read-view",
                lag.Value.IsPartial ||
                !precise
                    ? OperationalEvidenceState.Partial
                    : OperationalEvidenceState.Available);
        }

        if (resource.Kind ==
                OperationalResourceKind.ConsumerGroup &&
            metric ==
                OperationalMetricKind
                    .ConsumerConsumeRecordsPerSecond)
        {
            var rates =
                await _metrics
                    .GetConsumerRateAsync(
                        resource.ClusterId,
                        resource.ResourceId,
                        operation,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (!rates.IsSuccess ||
                rates.Value is null ||
                rates.Value.ConsumeRecordsPerSecond is
                    not double value ||
                !double.IsFinite(value) ||
                value < 0 ||
                rates.Value.Window <=
                    TimeSpan.Zero)
            {
                return Unavailable(
                    metric,
                    resource,
                    "consumer_rate_provider_unavailable");
            }

            var age =
                _timeProvider.GetUtcNow() -
                rates.Value.ObservedAt;
            if (age < TimeSpan.FromSeconds(-5) ||
                age > rates.Value.Window)
            {
                return new OperationalMetricEvidence(
                    metric,
                    resource,
                    value,
                    rates.Value.ObservedAt,
                    rates.Value.Window,
                    rates.Value.Source,
                    OperationalEvidenceState.Stale);
            }

            return new OperationalMetricEvidence(
                metric,
                resource,
                value,
                rates.Value.ObservedAt,
                rates.Value.Window,
                rates.Value.Source,
                OperationalEvidenceState.Available);
        }

        return Unavailable(
            metric,
            resource,
            "metric_provider_unavailable");
    }

    private static OperationalMetricEvidence Unavailable(
        OperationalMetricKind metric,
        OperationalResourceIdentity resource,
        string source) =>
        new(
            metric,
            resource,
            Value: null,
            ObservedAtUtc: null,
            Window: null,
            source,
            OperationalEvidenceState.Unavailable);

    private static OperationalMetricEvidence Unknown(
        OperationalMetricKind metric,
        OperationalResourceIdentity resource,
        string source) =>
        new(
            metric,
            resource,
            Value: null,
            ObservedAtUtc: null,
            Window: null,
            source,
            OperationalEvidenceState.Unknown);
}

public sealed class OperationalTrendService
{
    private readonly IHistoricalMetricStore? _history;

    public OperationalTrendService(
        IHistoricalMetricStore? history = null)
    {
        _history = history;
    }

    public async Task<OperationalTrendResult>
        QueryAsync(
            OperationalTrendQuery query,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        query.Validate();

        var metricName =
            OperationalMetricHistoryNames.TryGet(
                query.Metric);

        if (_history is null ||
            metricName is null)
        {
            return Empty(
                query,
                OperationalTrendState.Unavailable,
                "history_provider_unavailable");
        }

        var raw =
            await _history.QueryAsync(
                    new HistoricalMetricQuery(
                        metricName,
                        query.Resource.ClusterId,
                        OperationalMetricHistoryNames
                            .ResourceKind(
                                query.Resource.Kind),
                        query.Resource.ResourceId,
                        query.FromUtc,
                        query.ToUtc,
                        MaxSeries: 1,
                        query.MaxPoints),
                    cancellationToken)
                .ConfigureAwait(false);

        var points =
            raw.Series
                .SelectMany(series =>
                    series.Points)
                .OrderBy(point =>
                    point.ObservedAtUtc)
                .Take(
                    query.MaxPoints)
                .Select(MapPoint)
                .ToArray();

        var state =
            points.Length == 0
                ? OperationalTrendState.Unknown
                : points.Any(point =>
                    point.State is not
                        OperationalEvidenceState.Available)
                    ? OperationalTrendState.Partial
                    : OperationalTrendState.Available;

        var result =
            new OperationalTrendResult(
                query.Metric,
                query.Resource,
                Array.AsReadOnly(points),
                raw.Truncated,
                raw.LimitReason,
                raw.Provider,
                state);
        result.Validate(query);
        return result;
    }

    public async Task<OperationalSloResult>
        EvaluateSloAsync(
            OperationalSloDefinition definition,
            DateTimeOffset fromUtc,
            DateTimeOffset toUtc,
            int maxPoints,
            CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        definition.Validate();

        var trend =
            await QueryAsync(
                    new OperationalTrendQuery(
                        definition.Metric,
                        definition.Resource,
                        fromUtc,
                        toUtc,
                        maxPoints),
                    cancellationToken)
                .ConfigureAwait(false);

        var usable =
            trend.Points
                .Where(point =>
                    point.State ==
                        OperationalEvidenceState.Available &&
                    point.HasKnownCoverage)
                .ToArray();

        if (usable.Length == 0)
        {
            var unavailable =
                new OperationalSloResult(
                    definition,
                    fromUtc,
                    toUtc,
                    0,
                    0,
                    null,
                    null,
                    trend.State ==
                        OperationalTrendState.Unavailable
                        ? OperationalEvidenceState.Unavailable
                        : OperationalEvidenceState.Unknown,
                    trend.State ==
                        OperationalTrendState.Unavailable
                        ? "history_provider_unavailable"
                        : "no_complete_slo_evidence");
            unavailable.Validate();
            return unavailable;
        }

        var good =
            usable.Count(point =>
                point.Max <=
                    definition.MaximumGoodValue);
        var compliance =
            (double)good /
            usable.Length;
        var badFraction =
            1d -
            compliance;
        var errorBudget =
            1d -
            definition.TargetFraction;
        var burn =
            badFraction /
            errorBudget;

        var state =
            usable.Length !=
                trend.Points.Count ||
            trend.State ==
                OperationalTrendState.Partial
                ? OperationalEvidenceState.Partial
                : OperationalEvidenceState.Available;

        var result =
            new OperationalSloResult(
                definition,
                fromUtc,
                toUtc,
                usable.Length,
                good,
                compliance,
                burn,
                state,
                state ==
                    OperationalEvidenceState.Partial
                    ? "partial_slo_evidence"
                    : null);
        result.Validate();
        return result;
    }

    private static OperationalTrendPoint MapPoint(
        HistoricalMetricSample point)
    {
        var state =
            point.State switch
            {
                "Partial" =>
                    OperationalEvidenceState.Partial,
                "Unknown" =>
                    OperationalEvidenceState.Unknown,
                _ =>
                    OperationalEvidenceState.Available,
            };

        return new OperationalTrendPoint(
            point.ObservedAtUtc,
            point.Min,
            point.Max,
            point.Average,
            point.Count,
            point.ResolutionSeconds,
            state,
            point.HasKnownCoverage);
    }

    private static OperationalTrendResult Empty(
        OperationalTrendQuery query,
        OperationalTrendState state,
        string reason)
    {
        var result =
            new OperationalTrendResult(
                query.Metric,
                query.Resource,
                Array.Empty<OperationalTrendPoint>(),
                Truncated: false,
                LimitReason: null,
                reason,
                state);
        result.Validate(query);
        return result;
    }
}
