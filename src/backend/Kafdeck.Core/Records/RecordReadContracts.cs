using Kafdeck.Core.Kafka;

namespace Kafdeck.Core.Records;

public enum RecordReadDirection
{
    Forward = 1,
    Previous = 2,
}

public enum RecordAnchorKind
{
    Earliest = 1,
    Latest = 2,
    Offset = 3,
    Timestamp = 4,
}

public readonly record struct RecordAnchor
{
    private RecordAnchor(RecordAnchorKind kind, long? offset, DateTimeOffset? timestampUtc)
    {
        Kind = kind;
        Offset = offset;
        TimestampUtc = timestampUtc;
    }

    public RecordAnchorKind Kind { get; }

    public long? Offset { get; }

    public DateTimeOffset? TimestampUtc { get; }

    public static RecordAnchor Earliest() => new(RecordAnchorKind.Earliest, null, null);

    public static RecordAnchor Latest() => new(RecordAnchorKind.Latest, null, null);

    public static RecordAnchor AtOffset(long offset)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "Record offset must be non-negative.");
        }

        return new RecordAnchor(RecordAnchorKind.Offset, offset, null);
    }

    public static RecordAnchor AtTimestamp(DateTimeOffset timestampUtc) =>
        new(RecordAnchorKind.Timestamp, null, timestampUtc);
}

public enum RecordBudgetOutcome
{
    Complete = 1,
    RecordLimit = 2,
    RawByteLimit = 3,
    ProjectedByteLimit = 4,
    DurationLimit = 5,
    RateLimit = 6,
}

public sealed record RecordOperationBudget
{
    public const int DefaultMaxRecords = 200;
    public const int HardMaxRecords = 1_000;
    public const long DefaultMaxRawBytes = 2L * 1024 * 1024;
    public const long HardMaxRawBytes = 16L * 1024 * 1024;
    public const long DefaultMaxProjectedBytes = 4L * 1024 * 1024;
    public const long HardMaxProjectedBytes = 32L * 1024 * 1024;
    public static readonly TimeSpan DefaultMaxDuration = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan HardMaxDuration = TimeSpan.FromSeconds(30);
    public const int DefaultMaxRecordsPerSecond = 200;
    public const int HardMaxRecordsPerSecond = 2_000;

    public RecordOperationBudget(
        int maxRecords,
        long maxRawBytes,
        long maxProjectedBytes,
        TimeSpan maxDuration,
        int maxRecordsPerSecond)
    {
        MaxRecords = RequireRange(maxRecords, 1, HardMaxRecords, nameof(maxRecords));
        MaxRawBytes = RequireRange(maxRawBytes, 1, HardMaxRawBytes, nameof(maxRawBytes));
        MaxProjectedBytes = RequireRange(maxProjectedBytes, 1, HardMaxProjectedBytes, nameof(maxProjectedBytes));

        if (maxDuration <= TimeSpan.Zero || maxDuration > HardMaxDuration)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDuration),
                $"Record operation duration must be greater than zero and at most {HardMaxDuration}.");
        }

        MaxDuration = maxDuration;
        MaxRecordsPerSecond = RequireRange(
            maxRecordsPerSecond,
            1,
            HardMaxRecordsPerSecond,
            nameof(maxRecordsPerSecond));
    }

    public int MaxRecords { get; }

    public long MaxRawBytes { get; }

    public long MaxProjectedBytes { get; }

    public TimeSpan MaxDuration { get; }

    public int MaxRecordsPerSecond { get; }

    public static RecordOperationBudget Default { get; } = new(
        DefaultMaxRecords,
        DefaultMaxRawBytes,
        DefaultMaxProjectedBytes,
        DefaultMaxDuration,
        DefaultMaxRecordsPerSecond);

    private static int RequireRange(int value, int min, int max, string parameterName)
    {
        if (value < min || value > max)
        {
            throw new ArgumentOutOfRangeException(parameterName, $"Value must be between {min} and {max}.");
        }

        return value;
    }

    private static long RequireRange(long value, long min, long max, string parameterName)
    {
        if (value < min || value > max)
        {
            throw new ArgumentOutOfRangeException(parameterName, $"Value must be between {min} and {max}.");
        }

        return value;
    }
}

public sealed record RecordReadRequest
{
    public RecordReadRequest(
        string clusterId,
        string topicName,
        int partition,
        RecordAnchor anchor,
        RecordReadDirection direction,
        RecordOperationBudget budget)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentNullException.ThrowIfNull(budget);

        if (partition < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partition), "Partition must be non-negative.");
        }

        ClusterId = clusterId.Trim();
        TopicName = topicName.Trim();
        Partition = partition;
        Anchor = anchor;
        Direction = direction;
        Budget = budget;
    }

    public string ClusterId { get; }

    public string TopicName { get; }

    public int Partition { get; }

    public RecordAnchor Anchor { get; }

    public RecordReadDirection Direction { get; }

    public RecordOperationBudget Budget { get; }
}

public sealed record KafkaRecordHeader(string Name, ReadOnlyMemory<byte> Value);

public sealed record KafkaRawRecord(
    long Offset,
    DateTimeOffset? TimestampUtc,
    ReadOnlyMemory<byte>? Key,
    ReadOnlyMemory<byte>? Value,
    IReadOnlyList<KafkaRecordHeader> Headers);

public sealed record RecordReadBatch(
    IReadOnlyList<KafkaRawRecord> Records,
    long LowWatermark,
    long HighWatermark,
    long? FirstReturnedOffset,
    long? LastReturnedOffset,
    RecordAnchor? NextAnchor,
    RecordAnchor? PreviousAnchor,
    RecordBudgetOutcome BudgetOutcome);

public interface IKafkaRecordReadPort
{
    Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
        RecordReadRequest request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);
}

public interface IRecordMaskingPolicySnapshot
{
    string PolicyId { get; }

    int Version { get; }

    bool RequiresStructuredValue { get; }
}

public interface IRecordMaskingPolicyProvider
{
    IRecordMaskingPolicySnapshot Current { get; }
}
