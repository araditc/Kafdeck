using System.Text;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Core.Records;

public enum DataQualityEvaluationOutcome
{
    Complete = 1,
    RecordLimit = 2,
    ByteLimit = 3,
    DurationLimit = 4,
    Cancelled = 5,
    SourceUnavailable = 6,
}

public sealed record DataQualityEvaluationCycleBudget
{
    public const int DefaultMaxRecords =
        RecordOperationBudget.DefaultMaxRecords;
    public const int HardMaxRecords =
        RecordOperationBudget.HardMaxRecords;
    public const long DefaultMaxRawBytes =
        RecordOperationBudget.DefaultMaxRawBytes;
    public const long HardMaxRawBytes =
        RecordOperationBudget.HardMaxRawBytes;
    public static readonly TimeSpan DefaultMaxDuration =
        RecordOperationBudget.DefaultMaxDuration;
    public static readonly TimeSpan HardMaxDuration =
        RecordOperationBudget.HardMaxDuration;

    public DataQualityEvaluationCycleBudget(
        int maxRecords = DefaultMaxRecords,
        long maxRawBytes = DefaultMaxRawBytes,
        TimeSpan? maxDuration = null)
    {
        var duration =
            maxDuration ??
            DefaultMaxDuration;

        if (maxRecords is < 1 or >
                HardMaxRecords ||
            maxRawBytes is < 1 or >
                HardMaxRawBytes ||
            duration <= TimeSpan.Zero ||
            duration > HardMaxDuration)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxRecords),
                "Data-quality evaluation cycle budget exceeds the bounded record-read envelope.");
        }

        MaxRecords = maxRecords;
        MaxRawBytes = maxRawBytes;
        MaxDuration = duration;
    }

    public int MaxRecords { get; }

    public long MaxRawBytes { get; }

    public TimeSpan MaxDuration { get; }
}

public sealed record DataQualityEvaluationInput
{
    public const int HardMaxHeadersPerRecord = 1_024;

    public DataQualityEvaluationInput(
        string clusterId,
        string topicName,
        int partition,
        DateTimeOffset windowStartUtc,
        DateTimeOffset windowEndUtc,
        long startOffset,
        long endOffsetExclusive,
        IReadOnlyList<KafkaRawRecord> records,
        DataQualityEvaluationCycleBudget budget)
    {
        ArgumentNullException.ThrowIfNull(
            records);
        ArgumentNullException.ThrowIfNull(
            budget);

        var admittedCount =
            records.Count;
        if (admittedCount >
            budget.MaxRecords)
        {
            throw new ArgumentException(
                "Data-quality evaluation input exceeds the admitted record count.",
                nameof(records));
        }

        var scope =
            new DataQualityPolicyScope(
                clusterId,
                topicName,
                [partition]);

        if (windowStartUtc == default ||
            windowEndUtc <=
                windowStartUtc ||
            windowEndUtc -
                windowStartUtc >
                DataQualityPolicyBudget
                    .HardMaxEvaluationWindow)
        {
            throw new ArgumentException(
                "Data-quality evaluation window is invalid or unbounded.");
        }

        if (startOffset < 0 ||
            endOffsetExclusive <
                startOffset)
        {
            throw new ArgumentOutOfRangeException(
                nameof(startOffset));
        }

        var copy =
            new KafkaRawRecord[
                admittedCount];
        long rawBytes = 0;
        long? previousOffset = null;

        for (var index = 0;
             index < admittedCount;
             index++)
        {
            var record =
                records[index];
            if (record is null)
            {
                throw new ArgumentException(
                    "Data-quality evaluation records cannot contain null entries.",
                    nameof(records));
            }

            if (record.Offset <
                    startOffset ||
                record.Offset >=
                    endOffsetExclusive ||
                previousOffset is not null &&
                record.Offset <=
                    previousOffset.Value)
            {
                throw new ArgumentException(
                    "Data-quality evaluation record offsets must be unique, strictly increasing and inside the admitted range.",
                    nameof(records));
            }

            previousOffset =
                record.Offset;

            rawBytes =
                AddBoundedBytes(
                    rawBytes,
                    record.Key?.Length ?? 0,
                    budget.MaxRawBytes);
            rawBytes =
                AddBoundedBytes(
                    rawBytes,
                    record.Value?.Length ?? 0,
                    budget.MaxRawBytes);

            if (record.Headers is null)
            {
                throw new ArgumentException(
                    "Data-quality evaluation record headers are invalid or unbounded.",
                    nameof(records));
            }

            var admittedHeaderCount =
                record.Headers.Count;
            if (admittedHeaderCount >
                HardMaxHeadersPerRecord)
            {
                throw new ArgumentException(
                    "Data-quality evaluation record headers are invalid or unbounded.",
                    nameof(records));
            }

            var headers =
                new KafkaRecordHeader[
                    admittedHeaderCount];
            for (var headerIndex = 0;
                 headerIndex <
                 admittedHeaderCount;
                 headerIndex++)
            {
                var header =
                    record.Headers[
                        headerIndex];
                if (header is null)
                {
                    throw new ArgumentException(
                        "Data-quality evaluation record headers cannot contain null entries.",
                        nameof(records));
                }

                DataQualityContractInputBounds
                    .RequireRawString(
                        header.Name,
                        RecordHeaderMaskRule
                            .MaxHeaderNameCharacters,
                        nameof(records));

                rawBytes =
                    AddBoundedBytes(
                        rawBytes,
                        Encoding.UTF8.GetByteCount(
                            header.Name),
                        budget.MaxRawBytes);
                rawBytes =
                    AddBoundedBytes(
                        rawBytes,
                        header.Value.Length,
                        budget.MaxRawBytes);

                headers[headerIndex] =
                    new KafkaRecordHeader(
                        header.Name,
                        header.Value.ToArray());
            }

            copy[index] =
                new KafkaRawRecord(
                    record.Offset,
                    record.TimestampUtc,
                    record.Key?.ToArray(),
                    record.Value?.ToArray(),
                    Array.AsReadOnly(
                        headers));
        }

        ClusterId =
            scope.ClusterId;
        TopicName =
            scope.TopicName;
        Partition =
            partition;
        WindowStartUtc =
            windowStartUtc
                .ToUniversalTime();
        WindowEndUtc =
            windowEndUtc
                .ToUniversalTime();
        StartOffset =
            startOffset;
        EndOffsetExclusive =
            endOffsetExclusive;
        Records =
            Array.AsReadOnly(
                copy);
        RawByteCount =
            rawBytes;
        Budget =
            budget;
    }

    public string ClusterId { get; }

    public string TopicName { get; }

    public int Partition { get; }

    public DateTimeOffset WindowStartUtc { get; }

    public DateTimeOffset WindowEndUtc { get; }

    public long StartOffset { get; }

    public long EndOffsetExclusive { get; }

    public IReadOnlyList<KafkaRawRecord> Records { get; }

    public long RawByteCount { get; }

    public DataQualityEvaluationCycleBudget Budget { get; }

    private static long AddBoundedBytes(
        long current,
        int next,
        long hardLimit)
    {
        if (next < 0 ||
            current >
            hardLimit -
            next)
        {
            throw new ArgumentException(
                "Data-quality evaluation input exceeds the admitted raw-byte budget.");
        }

        return current + next;
    }
}

public sealed record DataQualityEvaluationProgress
{
    public DataQualityEvaluationProgress(
        string policyId,
        int policyVersion,
        string clusterId,
        string topicName,
        int partition,
        DateTimeOffset windowStartUtc,
        DateTimeOffset windowEndUtc,
        long startOffset,
        long endOffsetExclusive,
        long nextOffset,
        long evaluatedRecords,
        long evaluatedBytes,
        DataQualityEvidenceState evidenceState,
        DataQualityEvaluationOutcome outcome,
        DateTimeOffset updatedAtUtc)
    {
        DataQualityContractInputBounds
            .RequireRawString(
                policyId,
                DataQualityPolicyDefinition
                    .MaxPolicyIdLength,
                nameof(policyId));
        ArgumentException.ThrowIfNullOrWhiteSpace(
            policyId);
        var normalizedPolicyId =
            policyId.Trim();
        if (normalizedPolicyId.Any(
                char.IsControl))
        {
            throw new ArgumentException(
                "Data-quality progress policy ID is invalid.",
                nameof(policyId));
        }

        var scope =
            new DataQualityPolicyScope(
                clusterId,
                topicName,
                [partition]);

        if (policyVersion < 1 ||
            windowStartUtc == default ||
            windowEndUtc <=
                windowStartUtc ||
            windowEndUtc -
                windowStartUtc >
                DataQualityPolicyBudget
                    .HardMaxEvaluationWindow ||
            startOffset < 0 ||
            endOffsetExclusive <
                startOffset ||
            nextOffset <
                startOffset ||
            nextOffset >
                endOffsetExclusive ||
            evaluatedRecords < 0 ||
            evaluatedRecords >
                nextOffset -
                startOffset ||
            evaluatedBytes < 0 ||
            evaluatedRecords == 0 &&
            evaluatedBytes != 0 ||
            outcome ==
                DataQualityEvaluationOutcome.Complete &&
            nextOffset !=
                endOffsetExclusive ||
            updatedAtUtc == default)
        {
            throw new ArgumentException(
                "Data-quality evaluation progress metadata is invalid.");
        }

        if (!Enum.IsDefined(
                evidenceState) ||
            !Enum.IsDefined(
                outcome))
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome));
        }

        if (evidenceState is
                DataQualityEvidenceState.Unavailable or
                DataQualityEvidenceState.Unknown &&
            (evaluatedRecords != 0 ||
             evaluatedBytes != 0))
        {
            throw new ArgumentException(
                "Unavailable/unknown data-quality progress cannot claim evaluated counters.");
        }

        var windowTicks =
            (windowEndUtc -
             windowStartUtc).Ticks;
        var hardMaxRecords =
            checked(
                windowTicks *
                DataQualityPolicyBudget
                    .HardMaxRecordsPerSecond /
                TimeSpan.TicksPerSecond);
        var hardMaxBytes =
            checked(
                windowTicks *
                DataQualityPolicyBudget
                    .HardMaxBytesPerSecond /
                TimeSpan.TicksPerSecond);

        if (evaluatedRecords >
                hardMaxRecords ||
            evaluatedBytes >
                hardMaxBytes)
        {
            throw new ArgumentException(
                "Data-quality evaluation progress exceeds server-owned hard throughput bounds.");
        }

        PolicyId =
            normalizedPolicyId;
        PolicyVersion =
            policyVersion;
        ClusterId =
            scope.ClusterId;
        TopicName =
            scope.TopicName;
        Partition =
            partition;
        WindowStartUtc =
            windowStartUtc
                .ToUniversalTime();
        WindowEndUtc =
            windowEndUtc
                .ToUniversalTime();
        StartOffset =
            startOffset;
        EndOffsetExclusive =
            endOffsetExclusive;
        NextOffset =
            nextOffset;
        EvaluatedRecords =
            evaluatedRecords;
        EvaluatedBytes =
            evaluatedBytes;
        EvidenceState =
            evidenceState;
        Outcome =
            outcome;
        UpdatedAtUtc =
            updatedAtUtc
                .ToUniversalTime();
    }

    public string PolicyId { get; }

    public int PolicyVersion { get; }

    public string ClusterId { get; }

    public string TopicName { get; }

    public int Partition { get; }

    public DateTimeOffset WindowStartUtc { get; }

    public DateTimeOffset WindowEndUtc { get; }

    public long StartOffset { get; }

    public long EndOffsetExclusive { get; }

    public long NextOffset { get; }

    public long EvaluatedRecords { get; }

    public long EvaluatedBytes { get; }

    public DataQualityEvidenceState EvidenceState { get; }

    public DataQualityEvaluationOutcome Outcome { get; }

    public DateTimeOffset UpdatedAtUtc { get; }
}

public sealed record DataQualityEvaluationResult
{
    public DataQualityEvaluationResult(
        DataQualityAggregateEvidence evidence,
        DataQualityEvaluationProgress progress)
    {
        ArgumentNullException.ThrowIfNull(
            evidence);
        ArgumentNullException.ThrowIfNull(
            progress);

        if (!string.Equals(
                evidence.PolicyId,
                progress.PolicyId,
                StringComparison.Ordinal) ||
            evidence.PolicyVersion !=
                progress.PolicyVersion ||
            evidence.WindowStartUtc !=
                progress.WindowStartUtc ||
            evidence.WindowEndUtc !=
                progress.WindowEndUtc ||
            evidence.EvaluatedRecords !=
                progress.EvaluatedRecords ||
            evidence.EvaluatedBytes !=
                progress.EvaluatedBytes ||
            evidence.State !=
                progress.EvidenceState)
        {
            throw new ArgumentException(
                "Data-quality evaluation evidence and progress must describe the same bounded evaluation.");
        }

        Evidence = evidence;
        Progress = progress;
    }

    public DataQualityAggregateEvidence Evidence { get; }

    public DataQualityEvaluationProgress Progress { get; }
}

public interface IDataQualityBatchEvaluator
{
    Task<DataQualityEvaluationResult> EvaluateAsync(
        DataQualityPolicyDefinition policy,
        DataQualityEvaluationInput input,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);
}
