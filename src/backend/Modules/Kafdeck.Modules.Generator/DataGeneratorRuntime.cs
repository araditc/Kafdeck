using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Generator;

public interface IDataGeneratorEffectGuard
{
    Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        DataGeneratorPlan plan,
        long recordIndex,
        CancellationToken cancellationToken = default);
}

public sealed class FailClosedDataGeneratorEffectGuard :
    IDataGeneratorEffectGuard
{
    public Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        DataGeneratorPlan plan,
        long recordIndex,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(
            new MutationPreDispatchGuardResult(
                MutationPreDispatchGuardOutcome
                    .CapabilityUnsupported,
                "data_generator_effect_guard_not_activated"));
    }
}

public sealed record DataGeneratorDispatchResult(
    MutationProviderResult Result,
    FleetOperationProgressSnapshot Progress);

public sealed class DataGeneratorDispatchCoordinator
{
    private readonly DataGeneratorStateCoordinator _state;
    private readonly IClusterTransferProducePort _producer;
    private readonly IDataGeneratorEffectGuard _guard;
    private readonly DataGeneratorMaterializer _materializer;
    private readonly TimeProvider _timeProvider;

    public DataGeneratorDispatchCoordinator(
        DataGeneratorStateCoordinator state,
        IClusterTransferProducePort producer,
        IDataGeneratorEffectGuard guard,
        DataGeneratorMaterializer materializer,
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
        _materializer =
            materializer ??
            throw new ArgumentNullException(nameof(materializer));
        _timeProvider =
            timeProvider ?? TimeProvider.System;
    }

    public async Task<DataGeneratorDispatchResult>
        DispatchRecordAsync(
            MutationOperationSnapshot operation,
            DataGeneratorPlan plan,
            string workerId,
            long workerGeneration,
            long recordIndex,
            RecordSchemaDocument? schemaDocument = null,
            CancellationToken cancellationToken =
                default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);

        DataGeneratorPolicy.ValidatePlan(plan);
        ValidateOperationBinding(
            operation,
            plan,
            recordIndex);

        var loaded =
            await _state.GetAsync(
                    operation.OperationId,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false);

        var current =
            loaded.Outcome ==
                DataGeneratorStateOutcome.Applied &&
            loaded.Progress is not null
                ? loaded.Progress
                : throw new MutationStateException(
                    "Generator durable progress is unavailable.");

        EnsureWorkerLease(
            current,
            workerId,
            workerGeneration,
            _timeProvider.GetUtcNow());

        if (current.Generator?.PendingBatch is
            {
                State:
                    FleetGeneratorBatchState
                        .DispatchStarted,
            })
        {
            return new(
                Unknown(
                    "data_generator_unresolved_dispatch_prevents_replay",
                    current),
                current);
        }

        if (current.Generator?.PendingBatch is
            {
                State:
                    FleetGeneratorBatchState
                        .ReservedBeforeDispatch,
            } reserved)
        {
            var released =
                await _state.ReleaseBeforeDispatchAsync(
                        operation.OperationId,
                        plan,
                        workerGeneration,
                        reserved.BatchId,
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false);

            current =
                RequireApplied(
                    released,
                    "Generator pre-dispatch reservation could not be released.");
        }

        DataGeneratorProgress.EnsureCanDispatch(
            current,
            plan);

        DataGeneratorMaterializedRecord materialized;
        try
        {
            materialized =
                _materializer.Materialize(
                    plan,
                    recordIndex,
                    schemaDocument,
                    cancellationToken);
        }
        catch (Exception exception)
            when (exception is
                MutationStateException or
                ArgumentException or
                OverflowException)
        {
            return new(
                FailedDefinitive(
                    "data_generator_materialization_failed",
                    current),
                current);
        }

        var rawBytes =
            RawRecordBytes(materialized);

        if (rawBytes > plan.Budget.MaxBatchBytes)
        {
            return new(
                FailedDefinitive(
                    "data_generator_record_exceeds_batch_byte_budget",
                    current),
                current);
        }

        var guard =
            await _guard.ValidateAsync(
                    operation,
                    plan,
                    recordIndex,
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

        var reservedResult =
            await _state.ReserveBeforeDispatchAsync(
                    operation.OperationId,
                    plan,
                    workerGeneration,
                    recordIndex,
                    rawBytes,
                    _timeProvider.GetUtcNow(),
                    cancellationToken)
                .ConfigureAwait(false);

        current =
            RequireApplied(
                reservedResult,
                "Generator batch reservation could not be durably persisted.");

        var batchId =
            current.Generator!.PendingBatch!.BatchId;

        if (cancellationToken.IsCancellationRequested)
        {
            var released =
                await _state.ReleaseBeforeDispatchAsync(
                        operation.OperationId,
                        plan,
                        workerGeneration,
                        batchId,
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false);

            current =
                RequireApplied(
                    released,
                    "Cancelled generator reservation could not be durably released.");

            return new(
                FailedDefinitive(
                    "data_generator_cancelled_before_dispatch",
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

        current =
            RequireApplied(
                started,
                "Generator dispatch-start marker could not be durably persisted.");

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
                            plan.Destination.TopicName,
                            plan.Destination.Partition,
                            materialized.Key,
                            materialized.Value,
                            materialized.Headers),
                        CancellationToken.None)
                    .ConfigureAwait(false);
        }
        catch
        {
            provider =
                new MutationProviderResult(
                    MutationExecutionResultKind
                        .ExecutionUnknown,
                    "data_generator_provider_exception");
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
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false);

            current =
                RequireApplied(
                    completed,
                    "Generator acknowledgement could not be durably checkpointed.");

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
                await _state.ResolveProvenNonApplicationAsync(
                        operation.OperationId,
                        plan,
                        workerGeneration,
                        batchId,
                        _timeProvider.GetUtcNow(),
                        CancellationToken.None)
                    .ConfigureAwait(false);

            current =
                RequireApplied(
                    reconciled,
                    "Generator definitive non-application could not be durably reconciled.");

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

        current =
            RequireApplied(
                ambiguous,
                "Generator ambiguous dispatch marker could not be durably persisted.");

        return new(
            new MutationProviderResult(
                MutationExecutionResultKind
                    .ExecutionUnknown,
                provider.ResultCode,
                ProgressEvidence(current)),
            current);
    }

    private static void ValidateOperationBinding(
        MutationOperationSnapshot operation,
        DataGeneratorPlan plan,
        long recordIndex)
    {
        if (operation.OperationKind !=
                MutationOperationKind.DataGenerator ||
            operation.State !=
                MutationOperationState.AppliedVerified ||
            !string.Equals(
                operation.ResultCode,
                "data_generator_activated",
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Generator dispatch requires one durably activated generator operation.");
        }

        if (!string.Equals(
                operation.ClusterId,
                plan.Destination.ClusterId,
                StringComparison.Ordinal) ||
            recordIndex < 0 ||
            recordIndex >= plan.RecordCount)
        {
            throw new MutationStateException(
                "Generator operation does not match the immutable destination/index.");
        }

        var resource =
            $"data-generator/{plan.PlanFingerprint}";

        if (!operation.ResourceKeys.Contains(
                resource,
                StringComparer.Ordinal) ||
            operation.Preconditions.Count(
                item =>
                    string.Equals(
                        item.Key,
                        "generator.plan",
                        StringComparison.Ordinal) &&
                    string.Equals(
                        item.Fingerprint,
                        plan.PlanFingerprint,
                        StringComparison.Ordinal)) != 1)
        {
            throw new MutationStateException(
                "Generator operation does not bind the immutable plan.");
        }

        RequireAuthorization(
            operation,
            Kafdeck.Core.Security.AuthorizationAction
                .DataGeneratorExecute,
            plan.Destination.ClusterId,
            resource);

        RequireAuthorization(
            operation,
            Kafdeck.Core.Security.AuthorizationAction
                .RecordProduce,
            plan.Destination.ClusterId,
            plan.Destination.TopicName);
    }

    private static void RequireAuthorization(
        MutationOperationSnapshot operation,
        Kafdeck.Core.Security.AuthorizationAction action,
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
                $"Generator operation is missing required authorization '{action}'.");
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
                "Generator worker does not hold the current durable lease.");
        }
    }

    private static DataGeneratorDispatchResult
        Unused() =>
        throw new NotSupportedException();

    private static FleetOperationProgressSnapshot RequireApplied(
        DataGeneratorStateResult result,
        string message) =>
        result.Outcome ==
            DataGeneratorStateOutcome.Applied &&
        result.Progress is not null
            ? result.Progress
            : throw new MutationStateException(
                $"{message} Outcome: {result.Code}.");

    private static MutationProviderResult FailedDefinitive(
        string code,
        FleetOperationProgressSnapshot progress) =>
        new(
            progress.Generator?.AcknowledgedRecords > 0
                ? MutationExecutionResultKind
                    .PartiallyApplied
                : MutationExecutionResultKind
                    .FailedDefinitive,
            code,
            ProgressEvidence(progress));

    private static MutationProviderResult Unknown(
        string code,
        FleetOperationProgressSnapshot progress) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code,
            ProgressEvidence(progress));

    private static MutationProviderResult WithProgressEvidence(
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
            ["data-generator.acknowledged.records"] =
                (progress.Generator?
                    .AcknowledgedRecords ?? 0)
                .ToString(
                    System.Globalization
                        .CultureInfo.InvariantCulture),
            ["data-generator.acknowledged.bytes"] =
                (progress.Generator?
                    .AcknowledgedBytes ?? 0)
                .ToString(
                    System.Globalization
                        .CultureInfo.InvariantCulture),
            ["data-generator.next-index"] =
                (progress.Generator?
                    .NextRecordIndex ?? 0)
                .ToString(
                    System.Globalization
                        .CultureInfo.InvariantCulture),
            ["data-generator.pending"] =
                (progress.Generator?
                    .PendingBatch is not null)
                .ToString()
                .ToLowerInvariant(),
            ["data-generator.phase"] =
                progress.Phase.ToString(),
        };

    private static long RawRecordBytes(
        DataGeneratorMaterializedRecord record)
    {
        long total =
            record.Key?.Length ?? 0;
        total =
            checked(total + record.Value.Length);

        foreach (var header in record.Headers)
        {
            total =
                checked(
                    total +
                    System.Text.Encoding.UTF8
                        .GetByteCount(header.Name));
            total =
                checked(
                    total +
                    header.Value.Length);
        }

        return total;
    }
}
