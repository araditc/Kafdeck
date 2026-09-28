using Kafdeck.Core.Observability;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Consumers;

namespace Kafdeck.Api;

public sealed record ConsumerLagHistorySamplingPolicy(
    TimeSpan Interval,
    int MaxGroupsPerCluster,
    int MaxConcurrentGroups)
{
    public static ConsumerLagHistorySamplingPolicy Default { get; } =
        new(
            TimeSpan.FromMinutes(1),
            500,
            4);

    public void Validate()
    {
        if (Interval < TimeSpan.FromSeconds(10) ||
            Interval > TimeSpan.FromHours(1) ||
            MaxGroupsPerCluster is < 1 or > 2_000 ||
            MaxConcurrentGroups is < 1 or > 16)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Interval));
        }
    }
}

public sealed class ConsumerLagHistorySamplingHostedService :
    BackgroundService
{
    private const double LargestExactlyRepresentableInteger =
        9_007_199_254_740_992d;

    private readonly KafdeckOptions _options;
    private readonly ConsumerExplorerService _consumers;
    private readonly IHistoricalMetricStore _history;
    private readonly ConsumerLagHistorySamplingPolicy _policy;
    private readonly ILogger<
        ConsumerLagHistorySamplingHostedService> _logger;
    private readonly TimeProvider _timeProvider;

    public ConsumerLagHistorySamplingHostedService(
        KafdeckOptions options,
        ConsumerExplorerService consumers,
        IHistoricalMetricStore history,
        ConsumerLagHistorySamplingPolicy policy,
        ILogger<ConsumerLagHistorySamplingHostedService> logger,
        TimeProvider? timeProvider = null)
    {
        _options = options ??
            throw new ArgumentNullException(nameof(options));
        _consumers = consumers ??
            throw new ArgumentNullException(nameof(consumers));
        _history = history ??
            throw new ArgumentNullException(nameof(history));
        _policy = policy ??
            throw new ArgumentNullException(nameof(policy));
        _policy.Validate();
        _logger = logger ??
            throw new ArgumentNullException(nameof(logger));
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        await SampleOnceAsync(
                stoppingToken)
            .ConfigureAwait(false);

        using var timer =
            new PeriodicTimer(
                _policy.Interval,
                _timeProvider);

        while (await timer
                   .WaitForNextTickAsync(
                       stoppingToken)
                   .ConfigureAwait(false))
        {
            await SampleOnceAsync(
                    stoppingToken)
                .ConfigureAwait(false);
        }
    }

    internal async Task SampleOnceAsync(
        CancellationToken cancellationToken)
    {
        foreach (var cluster in
                 _options.Clusters)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            try
            {
                var groups =
                    await _consumers
                        .ListGroupsAsync(
                            cluster.Id,
                            cancellationToken)
                        .ConfigureAwait(false);

                if (!groups.IsSuccess ||
                    groups.Value is null)
                {
                    continue;
                }

                var selected =
                    groups.Value
                        .OrderBy(group =>
                            group.GroupId,
                            StringComparer.Ordinal)
                        .Take(
                            _policy
                                .MaxGroupsPerCluster)
                        .ToArray();

                using var slots =
                    new SemaphoreSlim(
                        _policy
                            .MaxConcurrentGroups,
                        _policy
                            .MaxConcurrentGroups);
                var samples =
                    new List<HistoricalMetricSample>();
                var gate =
                    new object();

                await Task.WhenAll(
                        selected.Select(
                            async group =>
                            {
                                await slots
                                    .WaitAsync(
                                        cancellationToken)
                                    .ConfigureAwait(false);
                                try
                                {
                                    var lag =
                                        await _consumers
                                            .GetLagAsync(
                                                cluster.Id,
                                                group.GroupId,
                                                cancellationToken)
                                            .ConfigureAwait(false);

                                    if (!lag.IsSuccess ||
                                        lag.Value?.TotalLag is
                                            not long total ||
                                        total < 0)
                                    {
                                        return;
                                    }

                                    var exact =
                                        total <=
                                        LargestExactlyRepresentableInteger;
                                    var sample =
                                        HistoricalMetricSample.Gauge(
                                            new HistoricalMetricIdentity(
                                                OperationalMetricHistoryNames
                                                    .ConsumerLagTotal,
                                                cluster.Id,
                                                "consumer_group",
                                                group.GroupId),
                                            _timeProvider.GetUtcNow(),
                                            total,
                                            "consumer-lag-sampler",
                                            lag.Value.IsPartial ||
                                            !exact
                                                ? "Partial"
                                                : "Stable");

                                    lock (gate)
                                    {
                                        samples.Add(
                                            sample);
                                    }
                                }
                                finally
                                {
                                    slots.Release();
                                }
                            }))
                    .ConfigureAwait(false);

                if (samples.Count > 0)
                {
                    await _history
                        .AppendAsync(
                            samples
                                .OrderBy(sample =>
                                    sample.Identity.ResourceId,
                                    StringComparer.Ordinal)
                                .ToArray(),
                            cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken
                    .IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Consumer lag history sampling failed for cluster {ClusterId}; missing evidence remains missing.",
                    cluster.Id);
            }
        }
    }
}
