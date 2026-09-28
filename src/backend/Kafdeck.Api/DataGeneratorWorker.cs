using System.Diagnostics;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;
using Kafdeck.Modules.Schemas;

namespace Kafdeck.Api;

public sealed record DataGeneratorWorkerPolicy
{
    public DataGeneratorWorkerPolicy(
        int discoveryLimit = 100,
        TimeSpan? pollInterval = null,
        TimeSpan? leaseTtl = null)
    {
        if (discoveryLimit is < 1 or > 1_000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(discoveryLimit));
        }

        var poll =
            pollInterval ??
            TimeSpan.FromSeconds(1);
        var lease =
            leaseTtl ??
            TimeSpan.FromSeconds(30);

        if (poll < TimeSpan.FromMilliseconds(100) ||
            poll > TimeSpan.FromMinutes(1))
        {
            throw new ArgumentOutOfRangeException(
                nameof(pollInterval));
        }

        if (lease < TimeSpan.FromSeconds(5) ||
            lease > TimeSpan.FromMinutes(5))
        {
            throw new ArgumentOutOfRangeException(
                nameof(leaseTtl));
        }

        DiscoveryLimit = discoveryLimit;
        PollInterval = poll;
        LeaseTtl = lease;
    }

    public int DiscoveryLimit { get; }
    public TimeSpan PollInterval { get; }
    public TimeSpan LeaseTtl { get; }

    public static DataGeneratorWorkerPolicy Default { get; } =
        new();
}

public sealed class DataGeneratorWorker
{
    private static readonly TimeSpan LeaseRenewalMargin =
        TimeSpan.FromSeconds(5);

    private readonly IMutationOperationRepository _operations;
    private readonly IFleetMutationStateStore _fleet;
    private readonly DataGeneratorStateCoordinator _state;
    private readonly DataGeneratorDispatchCoordinator _dispatch;
    private readonly IDataGeneratorEffectGuard _guard;
    private readonly SchemaExplorerService _schemas;
    private readonly DataGeneratorWorkerPolicy _policy;
    private readonly TimeProvider _timeProvider;
    private readonly string _workerId;
    private readonly RuntimeTelemetry? _telemetry;

    public DataGeneratorWorker(
        IMutationOperationRepository operations,
        IFleetMutationStateStore fleet,
        DataGeneratorStateCoordinator state,
        DataGeneratorDispatchCoordinator dispatch,
        IDataGeneratorEffectGuard guard,
        SchemaExplorerService schemas,
        DataGeneratorWorkerPolicy? policy = null,
        TimeProvider? timeProvider = null,
        string? workerId = null,
        RuntimeTelemetry? telemetry = null)
    {
        _operations =
            operations ??
            throw new ArgumentNullException(
                nameof(operations));
        _fleet =
            fleet ??
            throw new ArgumentNullException(
                nameof(fleet));
        _state =
            state ??
            throw new ArgumentNullException(
                nameof(state));
        _dispatch =
            dispatch ??
            throw new ArgumentNullException(
                nameof(dispatch));
        _guard =
            guard ??
            throw new ArgumentNullException(
                nameof(guard));
        _schemas =
            schemas ??
            throw new ArgumentNullException(
                nameof(schemas));
        _policy =
            policy ??
            DataGeneratorWorkerPolicy.Default;
        _timeProvider =
            timeProvider ??
            TimeProvider.System;

        _workerId =
            NormalizeWorkerId(
                workerId ??
                $"data-generator:{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}");
        _telemetry = telemetry;
    }

    public async Task<int> RunOnceAsync(
        CancellationToken cancellationToken = default)
    {
        var cycleStarted = Stopwatch.GetTimestamp();
        var active =
            await _fleet
                .ListActiveDataGeneratorProgressAsync(
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

            if (!IsActive(operation))
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
                // Each provider effect remains fenced by durable state and
                // per-record authorization/precondition guards.
            }
        }

        _telemetry?.RecordWorkerCycle(
            RuntimeTelemetryFamily.DataGeneratorWorker,
            attempted,
            Stopwatch.GetElapsedTime(cycleStarted).TotalMilliseconds);
        return attempted;
    }

    public async Task ProcessOperationOnceAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        if (!IsActive(operation))
        {
            return;
        }

        DataGeneratorPlan plan;
        try
        {
            plan =
                DataGeneratorPolicy.DeserializePlan(
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

        if (loaded.Outcome !=
                DataGeneratorStateOutcome.Applied ||
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

        var lease =
            await _state.AcquireLeaseAsync(
                    operation.OperationId,
                    plan,
                    _workerId,
                    _timeProvider.GetUtcNow(),
                    _policy.LeaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);

        if (lease.Outcome !=
                DataGeneratorStateOutcome.Applied ||
            lease.Progress is null)
        {
            return;
        }

        current = lease.Progress;
        var generation = current.WorkerGeneration;

        try
        {
            var nextIndex =
                current.Generator?.NextRecordIndex ??
                throw new MutationStateException(
                    "Generator durable sequence is unavailable.");

            if (nextIndex >= plan.RecordCount)
            {
                return;
            }

            var preGuard =
                await _guard.ValidateAsync(
                        operation,
                        plan,
                        nextIndex,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (preGuard.Outcome !=
                MutationPreDispatchGuardOutcome.Allowed)
            {
                if (preGuard.Outcome ==
                    MutationPreDispatchGuardOutcome
                        .AuthorizationDenied)
                {
                    _ = await _state
                        .PauseAuthorizationAsync(
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
                        DataGeneratorStateOutcome.Applied ||
                    resumed.Progress is null)
                {
                    return;
                }

                current = resumed.Progress;
            }

            RecordSchemaDocument? schema = null;
            if (plan.Source.Kind ==
                    DataGeneratorSourceKind.Schema &&
                plan.Source.Schema is { } source)
            {
                var observed =
                    await _schemas.GetVersionAsync(
                            plan.Destination.ClusterId,
                            source.Subject,
                            source.Version,
                            cancellationToken)
                        .ConfigureAwait(false);

                if (!observed.IsSuccess ||
                    observed.Value is null)
                {
                    await StopBestEffortAsync(
                            operation,
                            plan,
                            generation)
                        .ConfigureAwait(false);
                    return;
                }

                schema = observed.Value.Schema;
            }

            var remaining =
                checked(
                    plan.RecordCount -
                    (int)Math.Min(
                        int.MaxValue,
                        current.Generator!
                            .NextRecordIndex));
            var cycleLimit =
                Math.Min(
                    plan.Budget.MaxBatchRecords,
                    remaining);

            for (var ordinal = 0;
                 ordinal < cycleLimit;
                 ordinal++)
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

                var index =
                    current.Generator?.NextRecordIndex ??
                    throw new MutationStateException(
                        "Generator durable sequence is unavailable.");

                if (index >= plan.RecordCount)
                {
                    return;
                }

                DataGeneratorDispatchResult result;
                using var providerTelemetry =
                    _telemetry?.Start(
                        RuntimeTelemetryFamily.DataGeneratorProvider);
                try
                {
                    result =
                        await _dispatch.DispatchRecordAsync(
                                operation,
                                plan,
                                _workerId,
                                generation,
                                index,
                                schema,
                                cancellationToken)
                            .ConfigureAwait(false);
                    providerTelemetry?.Complete(
                        result.Result.ResultKind);
                }
                catch (DataGeneratorRateLimitException)
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
                    MutationExecutionResultKind
                        .AppliedVerified)
                {
                    if (current.Phase ==
                        FleetProgressPhase.Completed)
                    {
                        return;
                    }

                    continue;
                }

                if (result.Result.ResultKind ==
                        MutationExecutionResultKind
                            .FailedDefinitive &&
                    result.Result.ResultCode.Contains(
                        "authorization",
                        StringComparison.OrdinalIgnoreCase))
                {
                    _ = await _state
                        .PauseAuthorizationAsync(
                            operation.OperationId,
                            plan,
                            generation,
                            _timeProvider.GetUtcNow(),
                            CancellationToken.None)
                        .ConfigureAwait(false);
                    return;
                }

                if (result.Result.ResultKind is
                    MutationExecutionResultKind
                        .FailedDefinitive or
                    MutationExecutionResultKind
                        .PartiallyApplied)
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
            DataGeneratorPlan plan,
            FleetOperationProgressSnapshot current,
            CancellationToken cancellationToken)
    {
        if (current.Phase ==
            FleetProgressPhase.PausedAuthorization)
        {
            return current;
        }

        var now = _timeProvider.GetUtcNow();
        var elapsed =
            now - current.UpdatedAtUtc;

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
                   DataGeneratorStateOutcome.Applied
            ? result.Progress
            : null;
    }

    private async Task<FleetOperationProgressSnapshot?>
        RenewLeaseIfNeededAsync(
            MutationOperationSnapshot operation,
            DataGeneratorPlan plan,
            FleetOperationProgressSnapshot current,
            long generation,
            CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();

        if (current.WorkerLeaseExpiresAtUtc is { } expiry &&
            expiry >
            now.Add(LeaseRenewalMargin))
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
                   DataGeneratorStateOutcome.Applied
            ? renewed.Progress
            : null;
    }

    private async Task StopBestEffortAsync(
        MutationOperationSnapshot operation,
        DataGeneratorPlan plan,
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
        DataGeneratorPlan plan,
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

    private static bool IsRunnable(
        FleetProgressPhase phase) =>
        phase is
            FleetProgressPhase.Planned or
            FleetProgressPhase.WaitingForMaterial or
            FleetProgressPhase.Submitting or
            FleetProgressPhase.Observing or
            FleetProgressPhase.PausedAuthorization;

    private static bool IsActive(
        MutationOperationSnapshot? operation) =>
        operation is not null &&
        operation.OperationKind ==
            MutationOperationKind.DataGenerator &&
        operation.State ==
            MutationOperationState.AppliedVerified &&
        string.Equals(
            operation.ResultCode,
            "data_generator_activated",
            StringComparison.Ordinal);

    private static string NormalizeWorkerId(
        string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();

        if (normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Generator worker ID is invalid.",
                nameof(value));
        }

        return normalized.Length <= 256
            ? normalized
            : normalized[..256];
    }
}

public sealed class DataGeneratorHostedService :
    BackgroundService
{
    private readonly DataGeneratorWorker _worker;
    private readonly DataGeneratorWorkerPolicy _policy;

    public DataGeneratorHostedService(
        DataGeneratorWorker worker,
        DataGeneratorWorkerPolicy policy)
    {
        _worker =
            worker ??
            throw new ArgumentNullException(
                nameof(worker));
        _policy =
            policy ??
            throw new ArgumentNullException(
                nameof(policy));
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
                // Next bounded poll retries discovery. All provider effects
                // remain independently fenced by durable state and guards.
            }

            await Task.Delay(
                    _policy.PollInterval,
                    stoppingToken)
                .ConfigureAwait(false);
        }
    }
}
