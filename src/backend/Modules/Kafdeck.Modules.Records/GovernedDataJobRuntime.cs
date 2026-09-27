using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public interface IGovernedDataJobEffectGuard
{
    Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        int rangeIndex,
        long sourceOffset,
        CancellationToken cancellationToken = default);
}

public sealed class FailClosedGovernedDataJobEffectGuard :
    IGovernedDataJobEffectGuard
{
    public Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        int rangeIndex,
        long sourceOffset,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(
            new MutationPreDispatchGuardResult(
                MutationPreDispatchGuardOutcome
                    .CapabilityUnsupported,
                "data_job_effect_guard_not_activated"));
    }
}

public sealed class GovernedDataJobSourceReader
{
    private readonly ClusterTransferSourceReader _reader;

    public GovernedDataJobSourceReader(
        ClusterTransferSourceReader reader)
    {
        _reader =
            reader ??
            throw new ArgumentNullException(nameof(reader));
    }

    public Task<ClusterTransferSourceBatchResult> ReadNextAsync(
        GovernedDataJobPlan plan,
        int rangeIndex,
        FleetOperationProgressSnapshot progress,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        GovernedDataJobPolicy.ValidatePlan(plan);
        ArgumentNullException.ThrowIfNull(progress);

        var transfer =
            progress.Transfer ??
            throw new MutationStateException(
                "Data-job transfer progress is unavailable.");
        var transferPlan =
            GovernedDataJobPolicy.ToTransferPlan(plan);

        // Durable W57 progress binds the stronger data-job fingerprint, which
        // includes kind/transform. The reused W47 source reader validates its
        // own transfer-only fingerprint. Bridge only the read projection;
        // never rewrite the durable W57 progress identity.
        var readProjection =
            transfer with
            {
                PlanFingerprint =
                    transferPlan.PlanFingerprint,
            };

        return _reader.ReadNextAsync(
            transferPlan,
            rangeIndex,
            readProjection,
            operation,
            cancellationToken);
    }
}

public sealed record GovernedDataJobDispatchResult(
    MutationProviderResult Result,
    FleetOperationProgressSnapshot Progress);

public sealed class GovernedDataJobDispatchCoordinator
{
    private readonly GovernedDataJobStateCoordinator _state;
    private readonly IClusterTransferProducePort _producer;
    private readonly IGovernedDataJobEffectGuard _guard;
    private readonly TimeProvider _timeProvider;

    public GovernedDataJobDispatchCoordinator(
        GovernedDataJobStateCoordinator state,
        IClusterTransferProducePort producer,
        IGovernedDataJobEffectGuard guard,
        TimeProvider? timeProvider = null)
    {
        _state =
            state ??
            throw new ArgumentNullException(nameof(state));
        _producer =
            producer ??
            throw new ArgumentNullException(nameof(producer));
        _guard =
            guard ??
            throw new ArgumentNullException(nameof(guard));
        _timeProvider =
            timeProvider ?? TimeProvider.System;
    }

    public async Task<GovernedDataJobDispatchResult>
        DispatchRecordAsync(
            MutationOperationSnapshot operation,
            GovernedDataJobPlan plan,
            string workerId,
            long workerGeneration,
            int rangeIndex,
            KafkaRawRecord record,
            CancellationToken cancellationToken =
                default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(record);

        GovernedDataJobPolicy.ValidatePlan(plan);
        ValidateOperationBinding(
            operation,
            plan,
            rangeIndex);

        var range = plan.Ranges[rangeIndex];
        if (record.Offset < range.StartInclusive ||
            record.Offset >= range.EndExclusive)
        {
            throw new MutationStateException(
                "Data-job source offset is outside the immutable planned range.");
        }

        var current =
            await EnsureProgressAsync(
                    operation,
                    plan,
                    workerGeneration,
                    cancellationToken)
                .ConfigureAwait(false);

        EnsureWorkerLease(
            current,
            workerId,
            workerGeneration,
            _timeProvider.GetUtcNow());

        if (current.Phase ==
                FleetProgressPhase.WaitingForExternalAction &&
            current.Transfer?.PendingBatch is
            {
                State:
                    FleetTransferBatchState.DispatchStarted,
            })
        {
            return new(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "data_job_unresolved_dispatch_prevents_replay",
                    ProgressEvidence(current)),
                current);
        }

        GovernedDataJobProgress.EnsureCanDispatch(
            current,
            plan);

        var rawBytes = RawRecordBytes(record);
        if (rawBytes > plan.Budget.MaxBatchBytes)
        {
            return new(
                FailedDefinitive(
                    "data_job_record_exceeds_batch_byte_budget",
                    current),
                current);
        }

        var guard =
            await _guard.ValidateAsync(
                    operation,
                    plan,
                    rangeIndex,
                    record.Offset,
                    cancellationToken)
                .ConfigureAwait(false);

        if (guard.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return new(
                FailedDefinitive(
                    guard.ResultCode,
                    current),
                current);
        }

        var reserved =
            await _state.ReserveBeforeDispatchAsync(
                    operation.OperationId,
                    plan,
                    workerGeneration,
                    rangeIndex,
                    record.Offset,
                    range.DestinationPartition,
                    rawBytes,
                    _timeProvider.GetUtcNow(),
                    cancellationToken)
                .ConfigureAwait(false);
        current = RequireApplied(
            reserved,
            "Data-job batch reservation could not be durably persisted.");

        var batchId =
            current.Transfer!.PendingBatch!.BatchId;

        if (cancellationToken.IsCancellationRequested)
        {
            var released =
                await _state
                    .ReleaseBeforeDispatchAsync(
                        operation.OperationId,
                        plan,
                        workerGeneration,
                        batchId,
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false);
            current = RequireApplied(
                released,
                "Cancelled data-job reservation could not be durably released.");

            return new(
                FailedDefinitive(
                    "data_job_cancelled_before_dispatch",
                    current),
                current);
        }

        var started =
            await _state.MarkDispatchStartedAsync(
                    operation.OperationId,
                    plan,
                    workerGeneration,
                    batchId,
                    _timeProvider.GetUtcNow(),
                    CancellationToken.None)
                .ConfigureAwait(false);
        current = RequireApplied(
            started,
            "Data-job dispatch-start marker could not be durably persisted.");

        EnsureWorkerLease(
            current,
            workerId,
            workerGeneration,
            _timeProvider.GetUtcNow());

        MutationProviderResult provider;
        try
        {
            provider =
                await _producer.ProduceAsync(
                        new ClusterTransferProduceMutation(
                            plan.Destination.ClusterId,
                            range.DestinationTopic,
                            range.DestinationPartition,
                            record.Key,
                            record.Value,
                            record.Headers),
                        CancellationToken.None)
                    .ConfigureAwait(false);
        }
        catch
        {
            provider =
                new MutationProviderResult(
                    MutationExecutionResultKind
                        .ExecutionUnknown,
                    "data_job_provider_exception");
        }

        if (provider.ResultKind ==
            MutationExecutionResultKind.AppliedVerified)
        {
            var completed =
                await _state.CompleteAcknowledgedAsync(
                        operation.OperationId,
                        plan,
                        workerGeneration,
                        batchId,
                        checked(record.Offset + 1),
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false);
            current = RequireApplied(
                completed,
                "Data-job acknowledgement could not be durably checkpointed.");

            return new(
                WithProgressEvidence(
                    provider,
                    current),
                current);
        }

        if (provider.ResultKind ==
            MutationExecutionResultKind.FailedDefinitive)
        {
            var reconciled =
                await _state
                    .ResolveProvenNonApplicationAsync(
                        operation.OperationId,
                        plan,
                        workerGeneration,
                        batchId,
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false);
            current = RequireApplied(
                reconciled,
                "Data-job definitive non-application could not be durably reconciled.");

            return new(
                WithProgressEvidence(
                    provider,
                    current),
                current);
        }

        var ambiguous =
            await _state.MarkAmbiguousAsync(
                    operation.OperationId,
                    plan,
                    workerGeneration,
                    batchId,
                    _timeProvider.GetUtcNow(),
                    CancellationToken.None)
                .ConfigureAwait(false);
        current = RequireApplied(
            ambiguous,
            "Data-job ambiguous dispatch marker could not be durably persisted.");

        return new(
            new MutationProviderResult(
                MutationExecutionResultKind.ExecutionUnknown,
                provider.ResultCode,
                ProgressEvidence(current)),
            current);
    }

    private async Task<FleetOperationProgressSnapshot>
        EnsureProgressAsync(
            MutationOperationSnapshot operation,
            GovernedDataJobPlan plan,
            long workerGeneration,
            CancellationToken cancellationToken)
    {
        var current =
            await _state.GetAsync(
                    operation.OperationId,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false);

        if (current.Outcome ==
            GovernedDataJobStateOutcome.Applied &&
            current.Progress is not null)
        {
            return current.Progress;
        }

        if (current.Outcome !=
            GovernedDataJobStateOutcome.NotFound)
        {
            throw new MutationStateException(
                "Data-job durable progress is invalid.");
        }

        var initialized =
            await _state.InitializeAsync(
                    operation.OperationId,
                    workerGeneration,
                    plan,
                    _timeProvider.GetUtcNow(),
                    cancellationToken)
                .ConfigureAwait(false);

        if ((initialized.Outcome is
                 GovernedDataJobStateOutcome.Applied or
                 GovernedDataJobStateOutcome.Existing) &&
            initialized.Progress is not null)
        {
            return initialized.Progress;
        }

        throw new MutationStateException(
            "Data-job durable progress could not be initialized.");
    }

    private static FleetOperationProgressSnapshot
        RequireApplied(
            GovernedDataJobStateResult result,
            string message) =>
        result.Outcome ==
            GovernedDataJobStateOutcome.Applied &&
        result.Progress is not null
            ? result.Progress
            : throw new MutationStateException(
                $"{message} Outcome: {result.Code}.");

    private static void ValidateOperationBinding(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        int rangeIndex)
    {
        if (operation.OperationKind !=
                MutationOperationKind.DataJob ||
            operation.State !=
                MutationOperationState.AppliedVerified ||
            !string.Equals(
                operation.ResultCode,
                "data_job_activated",
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Data-job dispatch requires one durably activated data-job operation.");
        }

        if (!string.Equals(
                operation.ClusterId,
                plan.Source.ClusterId,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Data-job operation cluster does not match the immutable source.");
        }

        if (rangeIndex < 0 ||
            rangeIndex >= plan.Ranges.Count)
        {
            throw new ArgumentOutOfRangeException(
                nameof(rangeIndex));
        }

        var range = plan.Ranges[rangeIndex];
        var resource =
            $"data-job/{plan.PlanFingerprint}";

        if (!operation.ResourceKeys.Contains(
                resource,
                StringComparer.Ordinal) ||
            operation.Preconditions.Count(
                item =>
                    string.Equals(
                        item.Key,
                        "data-job.plan",
                        StringComparison.Ordinal) &&
                    string.Equals(
                        item.Fingerprint,
                        plan.PlanFingerprint,
                        StringComparison.Ordinal)) != 1)
        {
            throw new MutationStateException(
                "Data-job operation does not bind the immutable plan.");
        }

        RequireAuthorization(
            operation,
            AuthorizationAction.DataJobExecute,
            plan.Source.ClusterId,
            resource);
        RequireAuthorization(
            operation,
            AuthorizationAction.RecordRead,
            plan.Source.ClusterId,
            range.SourceTopic);
        RequireAuthorization(
            operation,
            AuthorizationAction.RecordExport,
            plan.Source.ClusterId,
            range.SourceTopic);
        RequireAuthorization(
            operation,
            AuthorizationAction.RecordProduce,
            plan.Destination.ClusterId,
            range.DestinationTopic);

    }

    private static void RequireAuthorization(
        MutationOperationSnapshot operation,
        AuthorizationAction action,
        string clusterId,
        string resource)
    {
        if (!operation.AuthorizationTargets.Any(
                target =>
                    target.Action == action &&
                    string.Equals(
                        target.ClusterId,
                        clusterId,
                        StringComparison.Ordinal) &&
                    string.Equals(
                        target.ResourceName,
                        resource,
                        StringComparison.Ordinal)))
        {
            throw new MutationStateException(
                $"Data-job operation is missing required authorization '{action}'.");
        }
    }

    private static void EnsureWorkerLease(
        FleetOperationProgressSnapshot progress,
        string workerId,
        long workerGeneration,
        DateTimeOffset nowUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);

        if (workerGeneration <= 0 ||
            progress.WorkerGeneration != workerGeneration ||
            !string.Equals(
                progress.WorkerLeaseOwner,
                workerId.Trim(),
                StringComparison.Ordinal) ||
            !progress.WorkerLeaseExpiresAtUtc.HasValue ||
            progress.WorkerLeaseExpiresAtUtc.Value <= nowUtc)
        {
            throw new MutationStateException(
                "Data-job worker does not hold the current durable lease.");
        }
    }

    private static MutationProviderResult
        FailedDefinitive(
            string code,
            FleetOperationProgressSnapshot progress) =>
        new(
            progress.Transfer?.AcknowledgedRecords > 0
                ? MutationExecutionResultKind
                    .PartiallyApplied
                : MutationExecutionResultKind
                    .FailedDefinitive,
            code,
            ProgressEvidence(progress));

    private static MutationProviderResult
        WithProgressEvidence(
            MutationProviderResult provider,
            FleetOperationProgressSnapshot progress)
    {
        var evidence =
            new Dictionary<string, string>(
                StringComparer.Ordinal);

        if (provider.SafeEvidence is not null)
        {
            foreach (var item in provider.SafeEvidence)
            {
                evidence[item.Key] = item.Value;
            }
        }

        foreach (var item in ProgressEvidence(progress))
        {
            evidence[item.Key] = item.Value;
        }

        return provider with
        {
            SafeEvidence = evidence,
        };
    }

    private static IReadOnlyDictionary<string, string>
        ProgressEvidence(
            FleetOperationProgressSnapshot progress) =>
        new Dictionary<string, string>(
            StringComparer.Ordinal)
        {
            ["data-job.acknowledged.records"] =
                (progress.Transfer?
                    .AcknowledgedRecords ?? 0)
                .ToString(
                    System.Globalization
                        .CultureInfo.InvariantCulture),
            ["data-job.acknowledged.bytes"] =
                (progress.Transfer?
                    .AcknowledgedBytes ?? 0)
                .ToString(
                    System.Globalization
                        .CultureInfo.InvariantCulture),
            ["data-job.pending"] =
                (progress.Transfer?.PendingBatch
                    is not null)
                .ToString()
                .ToLowerInvariant(),
            ["data-job.phase"] =
                progress.Phase.ToString(),
        };

    private static long RawRecordBytes(
        KafkaRawRecord record)
    {
        long total = record.Key?.Length ?? 0;
        total = checked(
            total + (record.Value?.Length ?? 0));

        foreach (var header in record.Headers)
        {
            total = checked(
                total +
                Encoding.UTF8.GetByteCount(
                    header.Name));
            total = checked(
                total + header.Value.Length);
        }

        return total;
    }
}
