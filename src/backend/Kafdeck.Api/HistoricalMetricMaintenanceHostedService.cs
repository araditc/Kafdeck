using Kafdeck.Core.Observability;

namespace Kafdeck.Api;

public sealed class HistoricalMetricMaintenanceHostedService :
    BackgroundService
{
    private readonly IHistoricalMetricMaintenanceStore
        _maintenanceStore;
    private readonly HistoricalMetricMaintenancePolicy
        _policy;
    private readonly ILogger<
        HistoricalMetricMaintenanceHostedService> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly string _ownerId;

    public HistoricalMetricMaintenanceHostedService(
        IHistoricalMetricMaintenanceStore maintenanceStore,
        HistoricalMetricMaintenancePolicy policy,
        ILogger<HistoricalMetricMaintenanceHostedService> logger,
        TimeProvider? timeProvider = null)
    {
        _maintenanceStore =
            maintenanceStore ??
            throw new ArgumentNullException(
                nameof(maintenanceStore));
        _policy =
            policy ??
            throw new ArgumentNullException(
                nameof(policy));
        _policy.Validate();
        _logger =
            logger ??
            throw new ArgumentNullException(
                nameof(logger));
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
        _ownerId =
            BuildOwnerId();
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        await RunOneCycleAsync(
                stoppingToken)
            .ConfigureAwait(false);

        using var timer =
            new PeriodicTimer(
                _policy.CycleInterval,
                _timeProvider);

        while (await timer
                   .WaitForNextTickAsync(stoppingToken)
                   .ConfigureAwait(false))
        {
            await RunOneCycleAsync(
                    stoppingToken)
                .ConfigureAwait(false);
        }
    }

    private async Task RunOneCycleAsync(
        CancellationToken cancellationToken)
    {
        try
        {
            var nowUtc =
                _timeProvider.GetUtcNow();
            var lease =
                await _maintenanceStore
                    .TryAcquireLeaseAsync(
                        _ownerId,
                        nowUtc,
                        _policy.LeaseDuration,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (lease is null)
            {
                return;
            }

            var result =
                await _maintenanceStore
                    .RunCycleAsync(
                        lease,
                        nowUtc,
                        _policy,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (!result.LeaseValid)
            {
                _logger.LogWarning(
                    "Historical metrics maintenance lease was fenced before execution.");
                return;
            }

            if (result.RolledWindows > 0 ||
                result.RawDeleted > 0 ||
                result.RollupDeleted > 0)
            {
                _logger.LogInformation(
                    "Historical metrics maintenance completed: {RolledWindows} rollup windows, {RawDeleted} raw samples deleted, {RollupDeleted} expired rollups deleted.",
                    result.RolledWindows,
                    result.RawDeleted,
                    result.RollupDeleted);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Historical metrics maintenance cycle failed; retained data remains authoritative and the next bounded cycle will retry.");
        }
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
