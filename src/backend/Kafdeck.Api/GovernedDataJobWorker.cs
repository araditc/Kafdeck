using System.Diagnostics;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

public sealed record GovernedDataJobWorkerPolicy
{
    public GovernedDataJobWorkerPolicy(
        int discoveryLimit = 100,
        TimeSpan? pollInterval = null,
        TimeSpan? leaseTtl = null,
        TimeSpan? sourceReadTimeout = null)
    {
        if (discoveryLimit is < 1 or > 1_000)
        {
            throw new ArgumentOutOfRangeException(nameof(discoveryLimit));
        }

        var poll = pollInterval ?? TimeSpan.FromSeconds(1);
        var lease = leaseTtl ?? TimeSpan.FromSeconds(30);
        var readTimeout = sourceReadTimeout ?? TimeSpan.FromSeconds(10);

        if (poll < TimeSpan.FromMilliseconds(100) ||
            poll > TimeSpan.FromMinutes(1))
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }

        if (lease < TimeSpan.FromSeconds(5) ||
            lease > TimeSpan.FromMinutes(5))
        {
            throw new ArgumentOutOfRangeException(nameof(leaseTtl));
        }

        if (readTimeout < TimeSpan.FromSeconds(1) ||
            readTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(sourceReadTimeout));
        }

        DiscoveryLimit = discoveryLimit;
        PollInterval = poll;
        LeaseTtl = lease;
        SourceReadTimeout = readTimeout;
    }

    public int DiscoveryLimit { get; }
    public TimeSpan PollInterval { get; }
    public TimeSpan LeaseTtl { get; }
    public TimeSpan SourceReadTimeout { get; }

    public static GovernedDataJobWorkerPolicy Default { get; } = new();
}

public sealed class GovernedDataJobWorker
{
    private static readonly TimeSpan LeaseRenewalMargin =
        TimeSpan.FromSeconds(5);

    private readonly IMutationOperationRepository _operations;
    private readonly IFleetMutationStateStore _fleet;
    private readonly GovernedDataJobStateCoordinator _state;
    private readonly GovernedDataJobSourceReader _source;
    private readonly GovernedDataJobDispatchCoordinator _dispatch;
    private readonly IGovernedDataJobEffectGuard _guard;
    private readonly GovernedDataJobWorkerPolicy _policy;
    private readonly TimeProvider _timeProvider;
    private readonly string _workerId;
    private readonly RuntimeTelemetry? _telemetry;

    public GovernedDataJobWorker(
        IMutationOperationRepository operations,
        IFleetMutationStateStore fleet,
        GovernedDataJobStateCoordinator state,
        GovernedDataJobSourceReader source,
        GovernedDataJobDispatchCoordinator dispatch,
        IGovernedDataJobEffectGuard guard,
        GovernedDataJobWorkerPolicy? policy = null,
        TimeProvider? timeProvider = null,
        string? workerId = null,
        RuntimeTelemetry? telemetry = null)
    {
        _operations =
            operations ?? throw new ArgumentNullException(nameof(operations));
        _fleet =
            fleet ?? throw new ArgumentNullException(nameof(fleet));
        _state =
            state ?? throw new ArgumentNullException(nameof(state));
        _source =
            source ?? throw new ArgumentNullException(nameof(source));
        _dispatch =
            dispatch ?? throw new ArgumentNullException(nameof(dispatch));
        _guard =
            guard ?? throw new ArgumentNullException(nameof(guard));
        _policy = policy ?? GovernedDataJobWorkerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;

        _workerId = NormalizeWorkerId(
            workerId ??
            $"data-job:{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}");
        _telemetry = telemetry;
    }

    public async Task<int> RunOnceAsync(
        CancellationToken cancellationToken = default)
    {
        var cycleStarted = Stopwatch.GetTimestamp();
        var active =
            await _fleet.ListActiveDataJobProgressAsync(
                    _policy.DiscoveryLimit,
                    cancellationToken)
                .ConfigureAwait(false);

        var attempted = 0;
        foreach (var progress in active)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var operation =
                await _operations.GetAsync(
                        progress.OperationId,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (!IsActiveDataJob(operation))
            {
                continue;
            }

            attempted++;
            try
            {
                await ProcessOperationOnceAsync(
                        operation!,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // A single malformed/stale job must not terminate the hosted
                // scheduler. All provider effects are still fenced by the
                // per-job guard and durable state machine.
            }
        }

        _telemetry?.RecordWorkerCycle(
            RuntimeTelemetryFamily.DataJobWorker,
            attempted,
            Stopwatch.GetElapsedTime(cycleStarted).TotalMilliseconds);
        return attempted;
    }

    public async Task ProcessOperationOnceAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        if (!IsActiveDataJob(operation))
        {
            return;
        }

        GovernedDataJobPlan plan;
        try
        {
            plan =
                GovernedDataJobPolicy.DeserializePlan(
                    operation.CanonicalIntent);
        }
        catch
        {
            return;
        }

        var loaded =
            await _state.GetAsync(
                    operation.OperationId,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false);

        if (loaded.Outcome != GovernedDataJobStateOutcome.Applied ||
            loaded.Progress is null)
        {
            return;
        }

        var current = loaded.Progress;
        if (!IsRunnable(current.Phase))
        {
            return;
        }

        current =
            await ChargeElapsedAsync(
                    operation,
                    plan,
                    current,
                    cancellationToken)
                .ConfigureAwait(false) ??
            current;

        if (!IsRunnable(current.Phase) ||
            current.ActiveObservationElapsed >=
                plan.Budget.MaxDuration)
        {
            await StopBestEffortAsync(
                    operation,
                    plan,
                    current.WorkerGeneration)
                .ConfigureAwait(false);
            return;
        }

        var now = _timeProvider.GetUtcNow();
        var lease =
            await _state.AcquireLeaseAsync(
                    operation.OperationId,
                    plan,
                    _workerId,
                    now,
                    _policy.LeaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);

        if (lease.Outcome != GovernedDataJobStateOutcome.Applied ||
            lease.Progress is null)
        {
            return;
        }

        current = lease.Progress;
        var generation = current.WorkerGeneration;

        try
        {
            var rangeIndex =
                NextIncompleteRange(
                    plan,
                    current);
            if (rangeIndex < 0)
            {
                return;
            }

            var checkpoint =
                current.Transfer!.Checkpoints[rangeIndex];
            var preReadGuard =
                await _guard.ValidateAsync(
                        operation,
                        plan,
                        rangeIndex,
                        checkpoint.NextSourceOffset,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (preReadGuard.Outcome !=
                MutationPreDispatchGuardOutcome.Allowed)
            {
                if (preReadGuard.Outcome ==
                    MutationPreDispatchGuardOutcome.AuthorizationDenied)
                {
                    _ = await _state.PauseAuthorizationAsync(
                            operation.OperationId,
                            plan,
                            generation,
                            _timeProvider.GetUtcNow(),
                            CancellationToken.None)
                        .ConfigureAwait(false);
                }
                else
                {
                    await StopBestEffortAsync(
                            operation,
                            plan,
                            generation)
                        .ConfigureAwait(false);
                }

                return;
            }

            if (current.Phase ==
                FleetProgressPhase.PausedAuthorization)
            {
                var resumed =
                    await _state.ResumeObservingAsync(
                            operation.OperationId,
                            plan,
                            generation,
                            _timeProvider.GetUtcNow(),
                            cancellationToken)
                        .ConfigureAwait(false);
                if (resumed.Outcome !=
                        GovernedDataJobStateOutcome.Applied ||
                    resumed.Progress is null)
                {
                    return;
                }

                current = resumed.Progress;
            }

            var readNow = _timeProvider.GetUtcNow();
            var remainingDuration =
                plan.Budget.MaxDuration -
                current.ActiveObservationElapsed;
            if (remainingDuration <= TimeSpan.Zero)
            {
                await StopBestEffortAsync(
                        operation,
                        plan,
                        generation)
                    .ConfigureAwait(false);
                return;
            }

            var readTimeout =
                remainingDuration < _policy.SourceReadTimeout
                    ? remainingDuration
                    : _policy.SourceReadTimeout;

            var batch =
                await _source.ReadNextAsync(
                        plan,
                        rangeIndex,
                        current,
                        new KafkaOperationContext(
                            readNow.Add(readTimeout)),
                        cancellationToken)
                    .ConfigureAwait(false);

            if (batch.State ==
                ClusterTransferSourceBatchState.RetryableNoProgress)
            {
                return;
            }

            if (!batch.IsSuccess)
            {
                await StopBestEffortAsync(
                        operation,
                        plan,
                        generation)
                    .ConfigureAwait(false);
                return;
            }

            foreach (var record in batch.Records)
            {
                cancellationToken.ThrowIfCancellationRequested();

                current =
                    await RenewLeaseIfNeededAsync(
                            operation,
                            plan,
                            current,
                            generation,
                            cancellationToken)
                        .ConfigureAwait(false) ??
                    current;

                GovernedDataJobDispatchResult result;
                using var providerTelemetry =
                    _telemetry?.Start(
                        RuntimeTelemetryFamily.DataJobProvider);
                try
                {
                    result =
                        await _dispatch.DispatchRecordAsync(
                                operation,
                                plan,
                                _workerId,
                                generation,
                                rangeIndex,
                                record,
                                cancellationToken)
                            .ConfigureAwait(false);
                    providerTelemetry?.Complete(
                        result.Result.ResultKind);
                }
                catch (GovernedDataJobRateLimitException)
                {
                    providerTelemetry?.Complete(
                        RuntimeTelemetryOutcome.RateLimited);
                    return;
                }
                catch (MutationStateException)
                {
                    providerTelemetry?.Complete(
                        RuntimeTelemetryOutcome.FailedDefinitive);
                    await StopBestEffortAsync(
                            operation,
                            plan,
                            generation)
                        .ConfigureAwait(false);
                    return;
                }

                current = result.Progress;

                if (result.Result.ResultKind ==
                    MutationExecutionResultKind.AppliedVerified)
                {
                    if (current.Phase ==
                        FleetProgressPhase.Completed)
                    {
                        return;
                    }

                    continue;
                }

                if (result.Result.ResultKind ==
                        MutationExecutionResultKind.FailedDefinitive &&
                    result.Result.ResultCode.Contains(
                        "authorization",
                        StringComparison.OrdinalIgnoreCase))
                {
                    _ = await _state.PauseAuthorizationAsync(
                            operation.OperationId,
                            plan,
                            generation,
                            _timeProvider.GetUtcNow(),
                            CancellationToken.None)
                        .ConfigureAwait(false);
                    return;
                }

                if (result.Result.ResultKind ==
                    MutationExecutionResultKind.FailedDefinitive)
                {
                    await StopBestEffortAsync(
                            operation,
                            plan,
                            generation)
                        .ConfigureAwait(false);
                }

                return;
            }
        }
        finally
        {
            await ReleaseLeaseBestEffortAsync(
                    operation,
                    plan,
                    generation)
                .ConfigureAwait(false);
        }
    }

    private async Task<FleetOperationProgressSnapshot?>
        ChargeElapsedAsync(
            MutationOperationSnapshot operation,
            GovernedDataJobPlan plan,
            FleetOperationProgressSnapshot current,
            CancellationToken cancellationToken)
    {
        if (current.Phase ==
            FleetProgressPhase.PausedAuthorization)
        {
            return current;
        }

        var now = _timeProvider.GetUtcNow();
        var elapsed = now - current.UpdatedAtUtc;
        if (elapsed <= TimeSpan.Zero)
        {
            return current;
        }

        var remaining =
            plan.Budget.MaxDuration -
            current.ActiveObservationElapsed;
        if (remaining <= TimeSpan.Zero)
        {
            return current;
        }

        var charge =
            elapsed <= remaining
                ? elapsed
                : remaining;

        var result =
            await _state.ChargeRuntimeAsync(
                    operation.OperationId,
                    plan,
                    current.WorkerGeneration,
                    charge,
                    now,
                    cancellationToken)
                .ConfigureAwait(false);

        return result.Outcome ==
                   GovernedDataJobStateOutcome.Applied
            ? result.Progress
            : null;
    }

    private async Task<FleetOperationProgressSnapshot?>
        RenewLeaseIfNeededAsync(
            MutationOperationSnapshot operation,
            GovernedDataJobPlan plan,
            FleetOperationProgressSnapshot current,
            long generation,
            CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        if (current.WorkerLeaseExpiresAtUtc is { } expiry &&
            expiry > now.Add(LeaseRenewalMargin))
        {
            return current;
        }

        var renewed =
            await _state.RenewLeaseAsync(
                    operation.OperationId,
                    plan,
                    _workerId,
                    generation,
                    now,
                    _policy.LeaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);

        return renewed.Outcome ==
                   GovernedDataJobStateOutcome.Applied
            ? renewed.Progress
            : null;
    }

    private async Task StopBestEffortAsync(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        long generation)
    {
        try
        {
            _ = await _state.StopAsync(
                    operation.OperationId,
                    plan,
                    generation,
                    _timeProvider.GetUtcNow(),
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            // Durable progress remains fail-closed if stop persistence fails.
        }
    }

    private async Task ReleaseLeaseBestEffortAsync(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        long generation)
    {
        try
        {
            _ = await _state.ReleaseLeaseAsync(
                    operation.OperationId,
                    plan,
                    _workerId,
                    generation,
                    _timeProvider.GetUtcNow(),
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            // Lease expiry remains the HA recovery boundary.
        }
    }

    private static int NextIncompleteRange(
        GovernedDataJobPlan plan,
        FleetOperationProgressSnapshot progress)
    {
        var transfer =
            progress.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable.");

        if (transfer.Checkpoints.Count !=
            plan.Ranges.Count)
        {
            throw new MutationStateException(
                "Data-job checkpoint count does not match the immutable plan.");
        }

        for (var index = 0;
             index < plan.Ranges.Count;
             index++)
        {
            if (transfer.Checkpoints[index].NextSourceOffset <
                plan.Ranges[index].EndExclusive)
            {
                return index;
            }
        }

        return -1;
    }

    private static bool IsRunnable(
        FleetProgressPhase phase) =>
        phase is
            FleetProgressPhase.Planned or
            FleetProgressPhase.WaitingForMaterial or
            FleetProgressPhase.Submitting or
            FleetProgressPhase.Observing or
            FleetProgressPhase.PausedAuthorization;

    private static bool IsActiveDataJob(
        MutationOperationSnapshot? operation) =>
        operation is not null &&
        operation.OperationKind ==
            MutationOperationKind.DataJob &&
        operation.State ==
            MutationOperationState.AppliedVerified &&
        string.Equals(
            operation.ResultCode,
            "data_job_activated",
            StringComparison.Ordinal);

    private static string NormalizeWorkerId(
        string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Data-job worker ID is invalid.",
                nameof(value));
        }

        return normalized.Length <= 256
            ? normalized
            : normalized[..256];
    }
}

public sealed class GovernedDataJobHostedService :
    BackgroundService
{
    private readonly GovernedDataJobWorker _worker;
    private readonly GovernedDataJobWorkerPolicy _policy;

    public GovernedDataJobHostedService(
        GovernedDataJobWorker worker,
        GovernedDataJobWorkerPolicy policy)
    {
        _worker =
            worker ??
            throw new ArgumentNullException(nameof(worker));
        _policy =
            policy ??
            throw new ArgumentNullException(nameof(policy));
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _ = await _worker.RunOnceAsync(
                        stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                // The next bounded poll retries discovery. Per-job effects are
                // independently fenced by durable state and current guards.
            }

            await Task.Delay(
                    _policy.PollInterval,
                    stoppingToken)
                .ConfigureAwait(false);
        }
    }
}
