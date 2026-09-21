using Kafdeck.Core.Consumers;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;

namespace Kafdeck.Modules.Consumers;

public enum ConsumerDiagnosticState
{
    Unknown = 0,
    Healthy = 1,
    ActiveWithLag = 2,
    Inactive = 3,
    Stalled = 4,
}

public sealed record ConsumerDiagnosticEvidence(
    string Code,
    string SafeMessage);

public sealed record ConsumerDiagnosticsProjection(
    string GroupId,
    ConsumerDiagnosticState State,
    ConsumerGroupState GroupState,
    long? TotalLag,
    ConsumerRateObservation? Rates,
    bool MetricsAvailable,
    bool HistoryAvailable,
    IReadOnlyList<ConsumerDiagnosticEvidence> Evidence,
    IReadOnlyList<ReadViewLimitation> Limitations);

public sealed record ConsumerDiagnosticsPolicy(
    TimeSpan OperationTimeout,
    TimeSpan MinimumStallEvidenceWindow,
    int MaxHistoryItems,
    long MaxResponseBytes)
{
    public static ConsumerDiagnosticsPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMinutes(1),
            128,
            2 * 1024 * 1024);
}

public sealed class ConsumerDiagnosticsService
{
    private readonly ConsumerExplorerService _consumers;
    private readonly IMetricsObservationPort _metrics;
    private readonly IHistoryObservationPort _history;
    private readonly ConsumerDiagnosticsPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public ConsumerDiagnosticsService(
        ConsumerExplorerService consumers,
        IMetricsObservationPort metrics,
        IHistoryObservationPort history,
        ConsumerDiagnosticsPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _consumers = consumers ?? throw new ArgumentNullException(nameof(consumers));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _history = history ?? throw new ArgumentNullException(nameof(history));
        _policy = policy ?? ConsumerDiagnosticsPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (_policy.OperationTimeout <= TimeSpan.Zero ||
            _policy.OperationTimeout > TimeSpan.FromMinutes(1))
        {
            throw new ArgumentOutOfRangeException(nameof(policy));
        }

        if (_policy.MinimumStallEvidenceWindow < TimeSpan.FromSeconds(30) ||
            _policy.MinimumStallEvidenceWindow > TimeSpan.FromHours(1))
        {
            throw new ArgumentOutOfRangeException(nameof(policy));
        }

        _ = Operation();
    }

    public async Task<ReadViewResult<ConsumerDiagnosticsProjection>> DiagnoseAsync(
        string clusterId,
        string groupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);

        var groupTask = _consumers.GetGroupAsync(clusterId, groupId, cancellationToken);
        var lagTask = _consumers.GetLagAsync(clusterId, groupId, cancellationToken);
        var metricsTask = _metrics.GetConsumerRateAsync(clusterId, groupId, Operation(), cancellationToken);
        var historyTask = _history.GetConsumerHistoryAsync(clusterId, groupId, Operation(), cancellationToken);

        await Task.WhenAll(groupTask, lagTask, metricsTask, historyTask).ConfigureAwait(false);

        var group = await groupTask.ConfigureAwait(false);
        if (!group.IsSuccess || group.Value is null)
        {
            return ReadViewResult<ConsumerDiagnosticsProjection>.Failed(group.Failure!);
        }

        var lag = await lagTask.ConfigureAwait(false);
        if (!lag.IsSuccess || lag.Value is null)
        {
            return ReadViewResult<ConsumerDiagnosticsProjection>.Failed(lag.Failure!);
        }

        var metrics = await metricsTask.ConfigureAwait(false);
        var history = await historyTask.ConfigureAwait(false);

        var projection = Evaluate(
            group.Value,
            lag.Value,
            metrics.IsSuccess ? metrics.Value : null,
            history.IsSuccess ? history.Value : null,
            _policy.MinimumStallEvidenceWindow,
            metrics.IsSuccess,
            history.IsSuccess);

        var limitations = lag.Limitations
            .Concat(projection.Limitations)
            .Concat(FailureLimitation(metrics.Failure, "consumer_metrics_unavailable"))
            .Concat(FailureLimitation(history.Failure, "consumer_history_unavailable"))
            .GroupBy(item => item.Code, StringComparer.Ordinal)
            .Select(grouping => grouping.First())
            .ToArray();

        return ReadViewResult<ConsumerDiagnosticsProjection>.Success(
            projection with { Limitations = limitations },
            limitations);
    }

    internal static ConsumerDiagnosticsProjection Evaluate(
        ConsumerGroupDetail group,
        ConsumerLagProjection lag,
        ConsumerRateObservation? rates,
        IReadOnlyList<ConsumerHistoryObservation>? history,
        TimeSpan minimumStallEvidenceWindow,
        bool metricsAvailable = true,
        bool historyAvailable = true)
    {
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(lag);

        var evidence = new List<ConsumerDiagnosticEvidence>();
        var limitations = new List<ReadViewLimitation>();

        if (lag.IsPartial)
        {
            limitations.AddRange(lag.Limitations);
        }

        if (group.State is ConsumerGroupState.Empty or ConsumerGroupState.Dead)
        {
            evidence.Add(new ConsumerDiagnosticEvidence(
                "consumer_group_inactive",
                $"Kafka reports the consumer group state as {group.State}."));

            return Projection(
                group,
                lag,
                rates,
                metricsAvailable,
                historyAvailable,
                ConsumerDiagnosticState.Inactive,
                evidence,
                limitations);
        }

        if (!lag.TotalLag.HasValue)
        {
            limitations.Add(new ReadViewLimitation(
                "consumer_lag_unknown",
                "Aggregate lag is not fully observable."));

            return Projection(
                group,
                lag,
                rates,
                metricsAvailable,
                historyAvailable,
                ConsumerDiagnosticState.Unknown,
                evidence,
                limitations);
        }

        if (lag.TotalLag.Value == 0 && !lag.IsPartial)
        {
            evidence.Add(new ConsumerDiagnosticEvidence(
                "consumer_zero_lag",
                "All observed partitions have zero lag."));

            return Projection(
                group,
                lag,
                rates,
                metricsAvailable,
                historyAvailable,
                ConsumerDiagnosticState.Healthy,
                evidence,
                limitations);
        }

        if (rates?.ConsumeRecordsPerSecond is > 0)
        {
            evidence.Add(new ConsumerDiagnosticEvidence(
                "consumer_progress_rate",
                "A positive consume rate is available for the current observation window."));

            return Projection(
                group,
                lag,
                rates,
                metricsAvailable,
                historyAvailable,
                ConsumerDiagnosticState.ActiveWithLag,
                evidence,
                limitations);
        }

        if (CanProveStall(group, lag, rates, history, minimumStallEvidenceWindow, evidence))
        {
            return Projection(
                group,
                lag,
                rates,
                metricsAvailable,
                historyAvailable,
                ConsumerDiagnosticState.Stalled,
                evidence,
                limitations);
        }

        if (!metricsAvailable)
        {
            limitations.Add(new ReadViewLimitation(
                "consumer_metrics_unavailable",
                "Consumer rate metrics are unavailable; Kafdeck will not infer a stalled state."));
        }

        if (!historyAvailable)
        {
            limitations.Add(new ReadViewLimitation(
                "consumer_history_unavailable",
                "Consumer history is unavailable; Kafdeck will not infer a stalled state."));
        }

        return Projection(
            group,
            lag,
            rates,
            metricsAvailable,
            historyAvailable,
            ConsumerDiagnosticState.Unknown,
            evidence,
            limitations);
    }

    private static bool CanProveStall(
        ConsumerGroupDetail group,
        ConsumerLagProjection lag,
        ConsumerRateObservation? rates,
        IReadOnlyList<ConsumerHistoryObservation>? history,
        TimeSpan minimumWindow,
        ICollection<ConsumerDiagnosticEvidence> evidence)
    {
        if (group.State != ConsumerGroupState.Stable ||
            lag.TotalLag is not > 0 ||
            rates?.ConsumeRecordsPerSecond is not double consumeRate ||
            consumeRate > 0 ||
            history is null)
        {
            return false;
        }

        var usable = history
            .Where(item => item.TotalLag.HasValue)
            .OrderBy(item => item.ObservedAt)
            .ToArray();

        if (usable.Length < 2)
        {
            return false;
        }

        var window = usable[^1].ObservedAt - usable[0].ObservedAt;
        if (window < minimumWindow)
        {
            return false;
        }

        var noObservedProgress = true;
        for (var index = 1; index < usable.Length; index++)
        {
            if (usable[index].TotalLag!.Value < usable[index - 1].TotalLag!.Value)
            {
                noObservedProgress = false;
                break;
            }
        }

        if (!noObservedProgress)
        {
            return false;
        }

        evidence.Add(new ConsumerDiagnosticEvidence(
            "consumer_zero_consume_rate",
            "The metrics provider reports zero consume rate for the current observation window."));
        evidence.Add(new ConsumerDiagnosticEvidence(
            "consumer_non_decreasing_lag_history",
            $"Observed lag did not decrease across a history window of at least {minimumWindow.TotalSeconds:0} seconds."));

        return true;
    }

    private static ConsumerDiagnosticsProjection Projection(
        ConsumerGroupDetail group,
        ConsumerLagProjection lag,
        ConsumerRateObservation? rates,
        bool metricsAvailable,
        bool historyAvailable,
        ConsumerDiagnosticState state,
        IReadOnlyList<ConsumerDiagnosticEvidence> evidence,
        IReadOnlyList<ReadViewLimitation> limitations) =>
        new(
            group.GroupId,
            state,
            group.State,
            lag.TotalLag,
            rates,
            metricsAvailable,
            historyAvailable,
            evidence.ToArray(),
            limitations.ToArray());

    private ReadViewOperationContext Operation() =>
        new(
            _timeProvider.GetUtcNow().Add(_policy.OperationTimeout),
            _policy.MaxHistoryItems,
            _policy.MaxResponseBytes);

    private static IEnumerable<ReadViewLimitation> FailureLimitation(
        ReadViewFailure? failure,
        string code)
    {
        if (failure is null)
        {
            return Array.Empty<ReadViewLimitation>();
        }

        return [new ReadViewLimitation(code, failure.SafeMessage)];
    }
}
