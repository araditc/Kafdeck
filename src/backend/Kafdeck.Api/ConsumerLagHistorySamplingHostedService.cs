using Kafdeck.Core.Consumers;
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
    private const long LargestExactlyRepresentableInteger =
        9_007_199_254_740_992L;
    private static readonly TimeSpan SamplingLeaseDuration =
        TimeSpan.FromMinutes(10);
    private static readonly TimeSpan LeaseSafetyMargin =
        TimeSpan.FromSeconds(5);

    private readonly KafdeckOptions _options;
    private readonly ConsumerExplorerService _consumers;
    private readonly IConsumerGroupSamplingReadPort _samplingGroups;
    private readonly IHistoricalMetricStore _history;
    private readonly IHistoricalMetricSamplingLeaseStore
        _samplingLeases;
    private readonly ConsumerLagHistorySamplingPolicy _policy;
    private readonly ILogger<
        ConsumerLagHistorySamplingHostedService> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly string _ownerId;
    private readonly Dictionary<string, string?> _clusterCursors =
        new(StringComparer.Ordinal);

    public ConsumerLagHistorySamplingHostedService(
        KafdeckOptions options,
        ConsumerExplorerService consumers,
        IConsumerGroupSamplingReadPort samplingGroups,
        IHistoricalMetricStore history,
        IHistoricalMetricSamplingLeaseStore samplingLeases,
        ConsumerLagHistorySamplingPolicy policy,
        ILogger<ConsumerLagHistorySamplingHostedService> logger,
        TimeProvider? timeProvider = null)
    {
        _options = options ??
            throw new ArgumentNullException(nameof(options));
        _consumers = consumers ??
            throw new ArgumentNullException(nameof(consumers));
        _samplingGroups = samplingGroups ??
            throw new ArgumentNullException(nameof(samplingGroups));
        _history = history ??
            throw new ArgumentNullException(nameof(history));
        _samplingLeases = samplingLeases ??
            throw new ArgumentNullException(nameof(samplingLeases));
        _policy = policy ??
            throw new ArgumentNullException(nameof(policy));
        _policy.Validate();
        _logger = logger ??
            throw new ArgumentNullException(nameof(logger));
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
        _ownerId =
            BuildOwnerId();
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
        try
        {
            var nowUtc =
                _timeProvider.GetUtcNow();
            var lease =
                await _samplingLeases
                    .TryAcquireSamplingLeaseAsync(
                        _ownerId,
                        nowUtc,
                        SamplingLeaseDuration,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (lease is null)
            {
                return;
            }

            var remaining =
                lease.ExpiresAtUtc -
                _timeProvider.GetUtcNow() -
                LeaseSafetyMargin;
            if (remaining <= TimeSpan.Zero)
            {
                return;
            }

            using var cycleDeadline =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            cycleDeadline.CancelAfter(
                remaining);
            var cycleToken =
                cycleDeadline.Token;

            var observedAtUtc =
                AlignToSamplingWindow(
                    nowUtc,
                    _policy.Interval);

            foreach (var cluster in
                     _options.Clusters)
            {
                cycleToken
                    .ThrowIfCancellationRequested();

                _clusterCursors.TryGetValue(
                    cluster.Id,
                    out var cursor);
                var page =
                    await _samplingGroups
                        .ListGroupPageAsync(
                            cluster.Id,
                            cursor,
                            _policy.MaxGroupsPerCluster,
                            new Kafdeck.Core.ReadViews
                                .ReadViewOperationContext(
                                    _timeProvider.GetUtcNow()
                                        .AddSeconds(10),
                                    _policy.MaxGroupsPerCluster,
                                    4 * 1024 * 1024),
                            cycleToken)
                        .ConfigureAwait(false);

                if (!page.IsSuccess ||
                    page.Value is null)
                {
                    continue;
                }

                var selected =
                    page.Value.Items
                        .ToArray();

                if (selected.Length == 0 &&
                    cursor is not null)
                {
                    page =
                        await _samplingGroups
                            .ListGroupPageAsync(
                                cluster.Id,
                                afterGroupId: null,
                                _policy.MaxGroupsPerCluster,
                                new Kafdeck.Core.ReadViews
                                    .ReadViewOperationContext(
                                        _timeProvider.GetUtcNow()
                                            .AddSeconds(10),
                                        _policy.MaxGroupsPerCluster,
                                        4 * 1024 * 1024),
                                cycleToken)
                            .ConfigureAwait(false);

                    if (!page.IsSuccess ||
                        page.Value is null)
                    {
                        continue;
                    }

                    selected =
                        page.Value.Items
                            .ToArray();
                }

                if (selected.Length == 0)
                {
                    _clusterCursors[cluster.Id] =
                        null;
                    continue;
                }

                _clusterCursors[cluster.Id] =
                    page.Value.NextCursor;

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
                                        cycleToken)
                                    .ConfigureAwait(false);
                                try
                                {
                                    var lag =
                                        await _consumers
                                            .GetLagAsync(
                                                cluster.Id,
                                                group.GroupId,
                                                cycleToken)
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
                                            observedAtUtc,
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
                            cycleToken)
                        .ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken
                .IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(
                "Consumer lag history sampling exceeded its fenced lease budget; incomplete evidence remains missing.");
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Consumer lag history sampling failed; missing evidence remains missing.");
        }
    }

    private static DateTimeOffset AlignToSamplingWindow(
        DateTimeOffset value,
        TimeSpan interval)
    {
        var utcTicks =
            value.UtcDateTime.Ticks;
        var alignedTicks =
            utcTicks -
            utcTicks %
            interval.Ticks;

        return new DateTimeOffset(
            alignedTicks,
            TimeSpan.Zero);
    }

    private static string BuildOwnerId()
    {
        var machine =
            Environment.MachineName;
        var process =
            Environment.ProcessId;
        var suffix =
            Guid.NewGuid()
                .ToString("N");

        return $"{machine}:{process}:{suffix}";
    }
}
