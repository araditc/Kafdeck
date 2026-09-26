using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public enum ClusterTransferSourceBatchState
{
    Ready = 1,
    MappingComplete = 2,
    RetryableNoProgress = 3,
    Failed = 4,
}

public sealed record ClusterTransferSourceBatchResult(
    ClusterTransferSourceBatchState State,
    IReadOnlyList<KafkaRawRecord> Records,
    string? FailureCode = null)
{
    public bool IsSuccess =>
        State is ClusterTransferSourceBatchState.Ready or
            ClusterTransferSourceBatchState.MappingComplete or
            ClusterTransferSourceBatchState.RetryableNoProgress;

    public static ClusterTransferSourceBatchResult Ready(
        IReadOnlyList<KafkaRawRecord> records,
        bool mappingComplete) =>
        new(
            mappingComplete
                ? ClusterTransferSourceBatchState.MappingComplete
                : ClusterTransferSourceBatchState.Ready,
            records);

    public static ClusterTransferSourceBatchResult NoProgress(string code) =>
        new(
            ClusterTransferSourceBatchState.RetryableNoProgress,
            Array.Empty<KafkaRawRecord>(),
            code);

    public static ClusterTransferSourceBatchResult Failed(string code) =>
        new(
            ClusterTransferSourceBatchState.Failed,
            Array.Empty<KafkaRawRecord>(),
            code);
}

/// <summary>
/// Reads exactly the next durable W47 source range without consumer-group
/// membership or offset commits. Missing offsets are never silently skipped:
/// retention/compaction gaps stop the transfer before any returned batch is
/// handed to the destination dispatcher.
/// </summary>
public sealed class ClusterTransferSourceReader
{
    private readonly IKafkaRecordReadPort _reader;

    public ClusterTransferSourceReader(IKafkaRecordReadPort reader)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
    }

    public async Task<ClusterTransferSourceBatchResult> ReadNextAsync(
        ClusterTransferPlan plan,
        int mappingIndex,
        FleetTransferProgressSnapshot progress,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(progress);
        if (mappingIndex < 0 || mappingIndex >= plan.Mappings.Count)
            throw new ArgumentOutOfRangeException(nameof(mappingIndex));

        var validated = FleetTransferProgress.Validate(progress);
        if (!string.Equals(
                validated.PlanFingerprint,
                plan.PlanFingerprint,
                StringComparison.Ordinal))
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_progress_plan_mismatch");
        }

        if (validated.PendingBatch is not null)
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_unresolved_dispatch_blocks_source_read");
        }

        if (mappingIndex >= validated.Checkpoints.Count ||
            validated.Checkpoints[mappingIndex].MappingIndex != mappingIndex)
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_checkpoint_mapping_mismatch");
        }

        var mapping = plan.Mappings[mappingIndex];
        var nextOffset = validated.Checkpoints[mappingIndex].NextSourceOffset;
        if (nextOffset < mapping.StartInclusive ||
            nextOffset > mapping.EndExclusive)
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_checkpoint_outside_range");
        }

        if (nextOffset == mapping.EndExclusive)
        {
            return ClusterTransferSourceBatchResult.Ready(
                Array.Empty<KafkaRawRecord>(),
                mappingComplete: true);
        }

        var remainingRecordBudget =
            plan.Budget.MaxTotalRecords - validated.AcknowledgedRecords;
        var remainingByteBudget =
            plan.Budget.MaxTotalBytes - validated.AcknowledgedBytes;
        if (remainingRecordBudget <= 0 || remainingByteBudget <= 0)
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_lifetime_budget_exhausted");
        }

        var remainingRange = mapping.EndExclusive - nextOffset;
        var maxRecords = checked((int)Math.Min(
            (long)RecordOperationBudget.HardMaxRecords,
            Math.Min(
                plan.Budget.MaxBatchRecords,
                Math.Min(remainingRange, remainingRecordBudget))));
        var maxRawBytes = Math.Min(
            RecordOperationBudget.HardMaxRawBytes,
            Math.Min(plan.Budget.MaxBatchBytes, remainingByteBudget));
        if (maxRecords <= 0 || maxRawBytes <= 0)
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_lifetime_budget_exhausted");
        }

        var readDuration = plan.Budget.MaxDuration <= RecordOperationBudget.HardMaxDuration
            ? plan.Budget.MaxDuration
            : RecordOperationBudget.HardMaxDuration;
        var readRate = Math.Min(
            plan.Budget.MaxRecordsPerSecond,
            RecordOperationBudget.HardMaxRecordsPerSecond);

        var result = await _reader.ReadPageAsync(
                new RecordReadRequest(
                    plan.Source.ClusterId,
                    mapping.SourceTopic,
                    mapping.SourcePartition,
                    RecordAnchor.AtOffset(nextOffset),
                    RecordReadDirection.Forward,
                    new RecordOperationBudget(
                        maxRecords,
                        maxRawBytes,
                        maxRawBytes,
                        readDuration,
                        readRate)),
                operation,
                cancellationToken)
            .ConfigureAwait(false);

        if (!result.IsSuccess || result.Value is null)
        {
            var code = result.Failure?.Code ?? "unknown";
            return ClusterTransferSourceBatchResult.Failed(
                $"cluster_transfer_source_{code}");
        }

        var batch = result.Value;
        if (batch.LowWatermark > nextOffset)
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_source_retention_gap");
        }

        // A finite preview cannot wait for a future high watermark. If the
        // frozen end offset is no longer/never observable, execution is stale.
        if (batch.HighWatermark < mapping.EndExclusive)
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_source_range_unavailable");
        }

        if (batch.Records.Count == 0)
        {
            return batch.BudgetOutcome switch
            {
                RecordBudgetOutcome.RawByteLimit =>
                    ClusterTransferSourceBatchResult.Failed(
                        "cluster_transfer_source_record_exceeds_batch_byte_budget"),
                RecordBudgetOutcome.DurationLimit or RecordBudgetOutcome.RateLimit =>
                    ClusterTransferSourceBatchResult.NoProgress(
                        "cluster_transfer_source_read_budget_exhausted"),
                _ when batch.HighWatermark > nextOffset =>
                    ClusterTransferSourceBatchResult.Failed(
                        "cluster_transfer_source_offset_gap"),
                _ =>
                    ClusterTransferSourceBatchResult.Failed(
                        "cluster_transfer_source_range_unavailable"),
            };
        }

        var expected = nextOffset;
        foreach (var record in batch.Records)
        {
            if (record.Offset != expected)
            {
                return ClusterTransferSourceBatchResult.Failed(
                    "cluster_transfer_source_offset_gap");
            }

            if (record.Offset >= mapping.EndExclusive)
            {
                return ClusterTransferSourceBatchResult.Failed(
                    "cluster_transfer_source_range_violation");
            }

            expected = checked(expected + 1);
        }

        var mappingComplete = expected == mapping.EndExclusive;

        // Complete/EOF before the immutable end means one or more planned
        // offsets disappeared; do not return a partial batch that would hide
        // the source gap behind already-applied destination writes.
        if (!mappingComplete &&
            batch.BudgetOutcome == RecordBudgetOutcome.Complete)
        {
            return ClusterTransferSourceBatchResult.Failed(
                "cluster_transfer_source_offset_gap");
        }

        return ClusterTransferSourceBatchResult.Ready(
            Array.AsReadOnly(batch.Records.ToArray()),
            mappingComplete);
    }
}
