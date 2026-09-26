using System.Text;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

/// <summary>
/// Revalidates the still-current authority/policy boundary immediately before
/// one W47 destination record effect. W49 supplies the live identity-aware
/// implementation; until then no runtime activation should register a transfer
/// handler with a permissive substitute.
/// </summary>
public interface IClusterTransferEffectGuard
{
    Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        ClusterTransferPlan plan,
        int mappingIndex,
        long sourceOffset,
        CancellationToken cancellationToken = default);
}

public sealed class FailClosedClusterTransferEffectGuard :
    IClusterTransferEffectGuard
{
    public Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        ClusterTransferPlan plan,
        int mappingIndex,
        long sourceOffset,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);
        cancellationToken.ThrowIfCancellationRequested();

        return Task.FromResult(new MutationPreDispatchGuardResult(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            "cluster_transfer_effect_guard_not_activated"));
    }
}

public sealed record ClusterTransferDispatchResult(
    MutationProviderResult Result,
    FleetOperationProgressSnapshot Progress);

/// <summary>
/// Durable one-record W47 dispatch primitive. It never persists raw Kafka
/// material. A ReservedBeforeDispatch marker is saved before the durable
/// DispatchStarted marker; after DispatchStarted an ambiguous provider outcome
/// remains unresolved and can never be replayed by this coordinator.
/// </summary>
public sealed class ClusterTransferDispatchCoordinator
{
    private readonly IFleetMutationStateStore _state;
    private readonly IClusterTransferProducePort _producer;
    private readonly IClusterTransferEffectGuard _guard;
    private readonly TimeProvider _timeProvider;

    public ClusterTransferDispatchCoordinator(
        IFleetMutationStateStore state,
        IClusterTransferProducePort producer,
        IClusterTransferEffectGuard guard,
        TimeProvider? timeProvider = null)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _guard = guard ?? throw new ArgumentNullException(nameof(guard));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<ClusterTransferDispatchResult> DispatchRecordAsync(
        MutationOperationSnapshot operation,
        ClusterTransferPlan plan,
        int mappingIndex,
        KafkaRawRecord record,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(record);

        if (operation.OperationKind != MutationOperationKind.ClusterTransfer ||
            operation.State != MutationOperationState.Executing ||
            operation.ExecutionClaimGeneration <= 0)
        {
            throw new MutationStateException(
                "Cluster transfer dispatch requires one executing admitted transfer operation.");
        }

        if (mappingIndex < 0 || mappingIndex >= plan.Mappings.Count)
            throw new ArgumentOutOfRangeException(nameof(mappingIndex));

        var mapping = plan.Mappings[mappingIndex];
        if (record.Offset < mapping.StartInclusive ||
            record.Offset >= mapping.EndExclusive)
        {
            throw new MutationStateException(
                "Transfer record offset is outside the immutable planned range.");
        }

        var rawBytes = RawRecordBytes(record);
        if (rawBytes > plan.Budget.MaxBatchBytes)
        {
            var progress = await EnsureProgressAsync(
                    operation,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false);
            return new ClusterTransferDispatchResult(
                DefinitiveResult(
                    progress,
                    "cluster_transfer_record_exceeds_batch_byte_budget"),
                progress);
        }

        var current = await EnsureProgressAsync(
                operation,
                plan,
                cancellationToken)
            .ConfigureAwait(false);

        if (current.Transfer is null ||
            !string.Equals(
                current.Transfer.PlanFingerprint,
                plan.PlanFingerprint,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Durable transfer progress does not match the immutable transfer plan.");
        }

        current = await ReconcileGenerationAsync(
                current,
                operation.ExecutionClaimGeneration,
                cancellationToken)
            .ConfigureAwait(false);

        var transfer = FleetTransferProgress.Restore(current.Transfer!);

        if (transfer.Snapshot.PendingBatch is { } existingPending)
        {
            if (existingPending.State == FleetTransferBatchState.DispatchStarted)
            {
                return new ClusterTransferDispatchResult(
                    Unknown(
                        "cluster_transfer_unresolved_dispatch_prevents_replay",
                        transfer.Snapshot),
                    current);
            }

            transfer.ReleaseBeforeDispatch(existingPending.BatchId);
            current = await SaveTransferAsync(
                    current,
                    transfer.Snapshot,
                    FleetProgressPhase.Submitting,
                    CancellationToken.None)
                .ConfigureAwait(false);
            transfer = FleetTransferProgress.Restore(current.Transfer!);
        }

        var checkpoint = transfer.Snapshot.Checkpoints[mappingIndex];
        if (checkpoint.NextSourceOffset != record.Offset)
        {
            throw new MutationStateException(
                "Transfer record offset does not match the durable next source checkpoint.");
        }

        if (transfer.Snapshot.AcknowledgedRecords >= plan.Budget.MaxTotalRecords ||
            checked(transfer.Snapshot.AcknowledgedBytes + rawBytes) >
            plan.Budget.MaxTotalBytes)
        {
            return new ClusterTransferDispatchResult(
                DefinitiveResult(
                    current,
                    "cluster_transfer_lifetime_budget_exhausted"),
                current);
        }

        var guard = await _guard.ValidateAsync(
                operation,
                plan,
                mappingIndex,
                record.Offset,
                cancellationToken)
            .ConfigureAwait(false);
        if (guard.Outcome != MutationPreDispatchGuardOutcome.Allowed)
        {
            return new ClusterTransferDispatchResult(
                DefinitiveResult(current, guard.ResultCode),
                current);
        }

        var pending = transfer.ReserveBeforeDispatch(
            mappingIndex,
            record.Offset,
            mapping.DestinationPartition,
            rawBytes,
            _timeProvider.GetUtcNow());
        current = await SaveTransferAsync(
                current,
                transfer.Snapshot,
                FleetProgressPhase.Submitting,
                cancellationToken)
            .ConfigureAwait(false);

        if (cancellationToken.IsCancellationRequested)
        {
            transfer = FleetTransferProgress.Restore(current.Transfer!);
            transfer.ReleaseBeforeDispatch(pending.BatchId);
            current = await SaveTransferAsync(
                    current,
                    transfer.Snapshot,
                    FleetProgressPhase.Stopped,
                    CancellationToken.None)
                .ConfigureAwait(false);
            return new ClusterTransferDispatchResult(
                DefinitiveResult(
                    current,
                    "cluster_transfer_cancelled_before_record_dispatch"),
                current);
        }

        transfer = FleetTransferProgress.Restore(current.Transfer!);
        transfer.MarkDispatchStarted(
            pending.BatchId,
            _timeProvider.GetUtcNow());
        current = await SaveTransferAsync(
                current,
                transfer.Snapshot,
                FleetProgressPhase.Submitting,
                CancellationToken.None)
            .ConfigureAwait(false);

        MutationProviderResult provider;
        try
        {
            provider = await _producer
                .ProduceAsync(
                    new ClusterTransferProduceMutation(
                        plan.Destination.ClusterId,
                        mapping.DestinationTopic,
                        mapping.DestinationPartition,
                        record.Key,
                        record.Value,
                        record.Headers),
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            provider = new MutationProviderResult(
                MutationExecutionResultKind.ExecutionUnknown,
                "cluster_transfer_provider_exception");
        }

        transfer = FleetTransferProgress.Restore(current.Transfer!);

        if (provider.ResultKind == MutationExecutionResultKind.AppliedVerified)
        {
            transfer.CompleteAcknowledged(
                pending.BatchId,
                checked(record.Offset + 1));
            current = await SaveTransferAsync(
                    current,
                    transfer.Snapshot,
                    FleetProgressPhase.Submitting,
                    CancellationToken.None)
                .ConfigureAwait(false);
            return new ClusterTransferDispatchResult(
                WithTransferEvidence(provider, current.Transfer!),
                current);
        }

        if (provider.ResultKind == MutationExecutionResultKind.FailedDefinitive)
        {
            transfer.ResolveProvenNonApplication(pending.BatchId);
            current = await SaveTransferAsync(
                    current,
                    transfer.Snapshot,
                    FleetProgressPhase.Stopped,
                    CancellationToken.None)
                .ConfigureAwait(false);
            return new ClusterTransferDispatchResult(
                DefinitiveResult(current, provider.ResultCode),
                current);
        }

        current = await SavePhaseAsync(
                current,
                FleetProgressPhase.Unknown,
                CancellationToken.None)
            .ConfigureAwait(false);
        return new ClusterTransferDispatchResult(
            Unknown(
                provider.ResultCode,
                current.Transfer!),
            current);
    }

    private async Task<FleetOperationProgressSnapshot> EnsureProgressAsync(
        MutationOperationSnapshot operation,
        ClusterTransferPlan plan,
        CancellationToken cancellationToken)
    {
        var existing = await _state.GetProgressAsync(
                operation.OperationId,
                cancellationToken)
            .ConfigureAwait(false);
        if (existing is not null)
            return existing;

        var transfer = FleetTransferProgress.Create(
            plan.PlanFingerprint,
            plan.Mappings
                .Select(mapping => mapping.StartInclusive)
                .ToArray());

        var created = FleetOperationProgress.Create(
                operation.OperationId,
                operation.ExecutionClaimGeneration,
                _timeProvider.GetUtcNow())
            .Snapshot with
        {
            Phase = FleetProgressPhase.Submitting,
            Transfer = transfer.Snapshot,
        };

        var result = await _state.CreateProgressAsync(
                created,
                cancellationToken)
            .ConfigureAwait(false);

        return result.Outcome switch
        {
            FleetProgressCreateOutcome.Created when result.Progress is not null =>
                result.Progress,
            FleetProgressCreateOutcome.Existing when result.Progress is not null =>
                result.Progress,
            FleetProgressCreateOutcome.ParentOperationNotFound =>
                throw new MutationStateException(
                    "Transfer progress cannot exist without its parent mutation operation."),
            _ => throw new MutationStateException(
                "Transfer progress could not be durably initialized."),
        };
    }

    private async Task<FleetOperationProgressSnapshot> ReconcileGenerationAsync(
        FleetOperationProgressSnapshot current,
        long generation,
        CancellationToken cancellationToken)
    {
        if (current.WorkerGeneration == generation)
            return current;

        if (current.WorkerGeneration > generation)
        {
            throw new MutationStateException(
                "A stale transfer worker generation cannot dispatch a provider effect.");
        }

        var progress = FleetOperationProgress.Restore(current);
        progress.FenceToGeneration(
            generation,
            _timeProvider.GetUtcNow());

        var saved = await _state.TrySaveProgressAsync(
                progress.Snapshot,
                current.Version,
                cancellationToken)
            .ConfigureAwait(false);
        if (saved.Outcome != FleetProgressSaveOutcome.Saved ||
            saved.Progress is null)
        {
            throw new MutationStateException(
                "Transfer worker generation could not be durably fenced.");
        }

        return saved.Progress;
    }

    private async Task<FleetOperationProgressSnapshot> SaveTransferAsync(
        FleetOperationProgressSnapshot current,
        FleetTransferProgressSnapshot transfer,
        FleetProgressPhase phase,
        CancellationToken cancellationToken)
    {
        var next = current with
        {
            Transfer = FleetTransferProgress.Validate(transfer),
            Phase = phase,
            Version = checked(current.Version + 1),
            UpdatedAtUtc = _timeProvider.GetUtcNow(),
        };

        var saved = await _state.TrySaveProgressAsync(
                next,
                current.Version,
                cancellationToken)
            .ConfigureAwait(false);
        if (saved.Outcome != FleetProgressSaveOutcome.Saved ||
            saved.Progress is null)
        {
            throw new MutationStateException(
                "Transfer checkpoint transition could not be durably persisted.");
        }

        return saved.Progress;
    }

    private async Task<FleetOperationProgressSnapshot> SavePhaseAsync(
        FleetOperationProgressSnapshot current,
        FleetProgressPhase phase,
        CancellationToken cancellationToken)
    {
        var next = current with
        {
            Phase = phase,
            Version = checked(current.Version + 1),
            UpdatedAtUtc = _timeProvider.GetUtcNow(),
        };

        var saved = await _state.TrySaveProgressAsync(
                next,
                current.Version,
                cancellationToken)
            .ConfigureAwait(false);
        if (saved.Outcome != FleetProgressSaveOutcome.Saved ||
            saved.Progress is null)
        {
            throw new MutationStateException(
                "Transfer phase transition could not be durably persisted.");
        }

        return saved.Progress;
    }

    private static MutationProviderResult DefinitiveResult(
        FleetOperationProgressSnapshot progress,
        string code)
    {
        var acknowledged = progress.Transfer?.AcknowledgedRecords ?? 0;
        return new MutationProviderResult(
            acknowledged > 0
                ? MutationExecutionResultKind.PartiallyApplied
                : MutationExecutionResultKind.FailedDefinitive,
            code,
            SafeProgressEvidence(progress.Transfer));
    }

    private static MutationProviderResult Unknown(
        string code,
        FleetTransferProgressSnapshot transfer) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code,
            SafeProgressEvidence(transfer));

    private static MutationProviderResult WithTransferEvidence(
        MutationProviderResult provider,
        FleetTransferProgressSnapshot transfer)
    {
        var evidence = new Dictionary<string, string>(StringComparer.Ordinal);
        if (provider.SafeEvidence is not null)
        {
            foreach (var item in provider.SafeEvidence)
                evidence[item.Key] = item.Value;
        }

        foreach (var item in SafeProgressEvidence(transfer))
            evidence[item.Key] = item.Value;

        return provider with { SafeEvidence = evidence };
    }

    private static IReadOnlyDictionary<string, string> SafeProgressEvidence(
        FleetTransferProgressSnapshot? transfer) =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["transfer.acknowledged.records"] =
                (transfer?.AcknowledgedRecords ?? 0)
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["transfer.acknowledged.bytes"] =
                (transfer?.AcknowledgedBytes ?? 0)
                .ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["transfer.pending"] =
                (transfer?.PendingBatch is not null)
                .ToString()
                .ToLowerInvariant(),
        };

    private static long RawRecordBytes(KafkaRawRecord record)
    {
        long total = record.Key?.Length ?? 0;
        total = checked(total + (record.Value?.Length ?? 0));

        foreach (var header in record.Headers)
        {
            total = checked(total + Encoding.UTF8.GetByteCount(header.Name));
            total = checked(total + header.Value.Length);
        }

        return total;
    }
}
