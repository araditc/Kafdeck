using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

/// <summary>Releases scheduling conflicts only after durable acknowledged completion. Never resolves an unknown effect.</summary>
internal static class CompletedJobConflictResolution
{
    public static Task<int> ResolveAsync(IFleetMutationStateStore store, MutationOperationSnapshot operation,
        GovernedDataJobPlan plan, FleetOperationProgressSnapshot progress, DateTimeOffset now, CancellationToken token)
    {
        GovernedDataJobPolicy.ValidatePlan(plan);
        var intent = GovernedDataJobPolicy.BuildIntent(plan);
        var transfer = progress.Transfer;
        if (!Completed(operation, progress, intent, "data_job_activated") || transfer is null || transfer.PendingBatch is not null ||
            transfer.PlanFingerprint != plan.PlanFingerprint || transfer.Checkpoints.Count != plan.Ranges.Count ||
            plan.Ranges.Where((range, index) => transfer.Checkpoints[index].MappingIndex != index ||
                transfer.Checkpoints[index].NextSourceOffset != range.EndExclusive).Any()) return Task.FromResult(0);
        var keys = ClusterTransferPolicy.BuildIntent(GovernedDataJobPolicy.ToTransferPlan(plan)).ResourceKeys;
        return ResolveCoreAsync(store, operation, progress, plan.PlanFingerprint, keys, now, token);
    }

    public static Task<int> ResolveAsync(IFleetMutationStateStore store, MutationOperationSnapshot operation,
        DataGeneratorPlan plan, FleetOperationProgressSnapshot progress, DateTimeOffset now, CancellationToken token)
    {
        DataGeneratorPolicy.ValidatePlan(plan);
        var generator = progress.Generator;
        if (!Completed(operation, progress, DataGeneratorPolicy.BuildIntent(plan), "data_generator_activated") ||
            generator is null || generator.PendingBatch is not null || generator.PlanFingerprint != plan.PlanFingerprint ||
            generator.NextRecordIndex != plan.RecordCount || generator.AcknowledgedRecords != plan.RecordCount) return Task.FromResult(0);
        return ResolveCoreAsync(store, operation, progress, plan.PlanFingerprint,
            [FleetConflictKeyCodec.TopicPartition(plan.Destination.KafkaClusterId, plan.Destination.TopicName, plan.Destination.Partition)], now, token);
    }

    private static bool Completed(MutationOperationSnapshot operation, FleetOperationProgressSnapshot progress,
        MutationIntentDescriptor intent, string resultCode) => operation.OperationKind == intent.Kind &&
        operation.State == MutationOperationState.AppliedVerified && operation.ResultCode == resultCode &&
        operation.CanonicalIntent == intent.CanonicalIntent && operation.OperationId == progress.OperationId &&
        progress.Phase == FleetProgressPhase.Completed && progress.Transfer?.PendingBatch is null && progress.Generator?.PendingBatch is null;

    private static async Task<int> ResolveCoreAsync(IFleetMutationStateStore store, MutationOperationSnapshot operation,
        FleetOperationProgressSnapshot completed, string fingerprint, IReadOnlyList<string> expectedKeys,
        DateTimeOffset now, CancellationToken token)
    {
        _ = FleetOperationProgress.Restore(completed);
        var current = await store.GetProgressAsync(operation.OperationId, token).ConfigureAwait(false);
        if (current is null || current.Version != completed.Version || current.WorkerGeneration != completed.WorkerGeneration ||
            current.Phase != FleetProgressPhase.Completed || current.Transfer?.PendingBatch is not null || current.Generator?.PendingBatch is not null)
            return 0;
        var rows = await store.ListConflictObligationsByOperationAsync(operation.OperationId, token).ConfigureAwait(false);
        if (rows.Count > 100) return 0;
        var evidence = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new
            { version = 1, operation.OperationId, operation.OperationKind, planFingerprint = fingerprint, progress = current }))).ToLowerInvariant();
        var resolved = 0;
        foreach (var row in rows)
        {
            if (row.OperationId != operation.OperationId || !row.BlocksConflictingDispatch || row.EffectFingerprint != fingerprint ||
                !expectedKeys.Contains(row.ConflictKey, StringComparer.Ordinal)) continue;
            var obligation = FleetConflictObligation.Restore(row);
            obligation.ApplyObservedResolution(FleetUncertaintyDispositionOutcome.ObservedTerminalEffect, evidence, now);
            var saved = await store.TrySaveConflictObligationAsync(obligation.Snapshot, row.Version, token).ConfigureAwait(false);
            if (saved.Outcome == FleetConflictObligationSaveOutcome.Saved) resolved++;
        }
        return resolved;
    }
}
