using System.Text.Json;

namespace Kafdeck.Core.Records;

public enum RecordFilterLanguage
{
    None = 0,
    Cel = 1,
    JqStyle = 2,
}

public enum RecordFilterBudgetOutcome
{
    Complete = 1,
    RecordLimit = 2,
    ByteLimit = 3,
    DurationLimit = 4,
}

public sealed record RecordFilterBudget
{
    public const int DefaultMaxExpressionCharacters = 2_048;
    public const int HardMaxExpressionCharacters = 8_192;
    public const int DefaultMaxAstNodes = 128;
    public const int HardMaxAstNodes = 512;
    public const int DefaultMaxAstDepth = 16;
    public const int HardMaxAstDepth = 32;
    public const int DefaultMaxRegexCount = 4;
    public const int HardMaxRegexCount = 16;
    public const int DefaultMaxEvaluatedRecords = 1_000;
    public const int HardMaxEvaluatedRecords = 10_000;
    public const long DefaultMaxEvaluatedBytes = 8L * 1024 * 1024;
    public const long HardMaxEvaluatedBytes = 64L * 1024 * 1024;
    public static readonly TimeSpan DefaultMaxDuration = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan HardMaxDuration = TimeSpan.FromSeconds(30);

    public RecordFilterBudget(
        int maxExpressionCharacters = DefaultMaxExpressionCharacters,
        int maxAstNodes = DefaultMaxAstNodes,
        int maxAstDepth = DefaultMaxAstDepth,
        int maxRegexCount = DefaultMaxRegexCount,
        int maxEvaluatedRecords = DefaultMaxEvaluatedRecords,
        long maxEvaluatedBytes = DefaultMaxEvaluatedBytes,
        TimeSpan? maxDuration = null)
    {
        MaxExpressionCharacters = RequireRange(
            maxExpressionCharacters,
            1,
            HardMaxExpressionCharacters,
            nameof(maxExpressionCharacters));
        MaxAstNodes = RequireRange(maxAstNodes, 1, HardMaxAstNodes, nameof(maxAstNodes));
        MaxAstDepth = RequireRange(maxAstDepth, 1, HardMaxAstDepth, nameof(maxAstDepth));
        MaxRegexCount = RequireRange(maxRegexCount, 0, HardMaxRegexCount, nameof(maxRegexCount));
        MaxEvaluatedRecords = RequireRange(
            maxEvaluatedRecords,
            1,
            HardMaxEvaluatedRecords,
            nameof(maxEvaluatedRecords));
        MaxEvaluatedBytes = RequireRange(
            maxEvaluatedBytes,
            1,
            HardMaxEvaluatedBytes,
            nameof(maxEvaluatedBytes));

        var actualDuration = maxDuration ?? DefaultMaxDuration;
        if (actualDuration <= TimeSpan.Zero || actualDuration > HardMaxDuration)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxDuration),
                $"Filter duration must be greater than zero and at most {HardMaxDuration}.");
        }

        MaxDuration = actualDuration;
    }

    public int MaxExpressionCharacters { get; }
    public int MaxAstNodes { get; }
    public int MaxAstDepth { get; }
    public int MaxRegexCount { get; }
    public int MaxEvaluatedRecords { get; }
    public long MaxEvaluatedBytes { get; }
    public TimeSpan MaxDuration { get; }

    public static RecordFilterBudget Default { get; } = new();

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

public sealed record RecordHeaderPredicate
{
    public const int MaxNameCharacters = 256;
    public const int MaxValueCharacters = 4_096;

    public RecordHeaderPredicate(
        string name,
        string? equalsUtf8 = null,
        string? prefixUtf8 = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var normalizedName = name.Trim();
        if (normalizedName.Length > MaxNameCharacters)
        {
            throw new ArgumentOutOfRangeException(nameof(name));
        }

        if (equalsUtf8 is not null && equalsUtf8.Length > MaxValueCharacters)
        {
            throw new ArgumentOutOfRangeException(nameof(equalsUtf8));
        }

        if (prefixUtf8 is not null && prefixUtf8.Length > MaxValueCharacters)
        {
            throw new ArgumentOutOfRangeException(nameof(prefixUtf8));
        }

        if (equalsUtf8 is null && prefixUtf8 is null)
        {
            throw new ArgumentException("Header predicate requires an equality or prefix value.");
        }

        if (equalsUtf8 is not null && prefixUtf8 is not null)
        {
            throw new ArgumentException("Header predicate cannot specify equality and prefix simultaneously.");
        }

        Name = normalizedName;
        EqualsUtf8 = equalsUtf8;
        PrefixUtf8 = prefixUtf8;
    }

    public string Name { get; }
    public string? EqualsUtf8 { get; }
    public string? PrefixUtf8 { get; }
}

public sealed record RecordPreFilter
{
    public RecordPreFilter(
        long? minimumOffset = null,
        long? maximumOffset = null,
        DateTimeOffset? minimumTimestampUtc = null,
        DateTimeOffset? maximumTimestampUtc = null,
        string? keyEqualsUtf8 = null,
        string? keyPrefixUtf8 = null,
        IReadOnlyList<RecordHeaderPredicate>? headers = null)
    {
        if (minimumOffset is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(minimumOffset));
        }

        if (maximumOffset is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumOffset));
        }

        if (minimumOffset.HasValue &&
            maximumOffset.HasValue &&
            minimumOffset.Value > maximumOffset.Value)
        {
            throw new ArgumentException("Minimum offset cannot exceed maximum offset.");
        }

        if (minimumTimestampUtc.HasValue &&
            maximumTimestampUtc.HasValue &&
            minimumTimestampUtc.Value > maximumTimestampUtc.Value)
        {
            throw new ArgumentException("Minimum timestamp cannot exceed maximum timestamp.");
        }

        if (keyEqualsUtf8 is not null && keyPrefixUtf8 is not null)
        {
            throw new ArgumentException("Key predicate cannot specify equality and prefix simultaneously.");
        }

        if ((keyEqualsUtf8?.Length ?? 0) > RecordHeaderPredicate.MaxValueCharacters ||
            (keyPrefixUtf8?.Length ?? 0) > RecordHeaderPredicate.MaxValueCharacters)
        {
            throw new ArgumentOutOfRangeException(nameof(keyEqualsUtf8));
        }

        var actualHeaders = headers?.ToArray() ?? [];
        if (actualHeaders.Length > 32)
        {
            throw new ArgumentOutOfRangeException(nameof(headers), "At most 32 header predicates are allowed.");
        }

        MinimumOffset = minimumOffset;
        MaximumOffset = maximumOffset;
        MinimumTimestampUtc = minimumTimestampUtc;
        MaximumTimestampUtc = maximumTimestampUtc;
        KeyEqualsUtf8 = keyEqualsUtf8;
        KeyPrefixUtf8 = keyPrefixUtf8;
        Headers = Array.AsReadOnly(actualHeaders);
    }

    public long? MinimumOffset { get; }
    public long? MaximumOffset { get; }
    public DateTimeOffset? MinimumTimestampUtc { get; }
    public DateTimeOffset? MaximumTimestampUtc { get; }
    public string? KeyEqualsUtf8 { get; }
    public string? KeyPrefixUtf8 { get; }
    public IReadOnlyList<RecordHeaderPredicate> Headers { get; }
}

public sealed record RecordStructuredFilter
{
    public RecordStructuredFilter(RecordFilterLanguage language, string expression)
    {
        if (language is RecordFilterLanguage.None || !Enum.IsDefined(language))
        {
            throw new ArgumentOutOfRangeException(nameof(language));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        Language = language;
        Expression = expression.Trim();
    }

    public RecordFilterLanguage Language { get; }
    public string Expression { get; }
}

public sealed record RecordFilterRequest
{
    public RecordFilterRequest(
        RecordPreFilter? preFilter = null,
        RecordStructuredFilter? structuredFilter = null,
        RecordFilterBudget? budget = null)
    {
        PreFilter = preFilter;
        StructuredFilter = structuredFilter;
        Budget = budget ?? RecordFilterBudget.Default;
    }

    public RecordPreFilter? PreFilter { get; }
    public RecordStructuredFilter? StructuredFilter { get; }
    public RecordFilterBudget Budget { get; }
}

public sealed record RecordFilteredItem(
    KafkaRawRecord RawRecord,
    RecordDecodedValue? DecodedValue);

public enum RecordFilterLimitationCode
{
    DecodeFailed = 1,
    DecodeUnavailable = 2,
    FilterBudgetExhausted = 3,
}

public sealed record RecordFilterLimitation(
    RecordFilterLimitationCode Code,
    int Count);

public sealed record RecordFilterPage(
    IReadOnlyList<RecordFilteredItem> Records,
    long LowWatermark,
    long HighWatermark,
    RecordAnchor? NextAnchor,
    RecordAnchor? PreviousAnchor,
    RecordBudgetOutcome ReadBudgetOutcome,
    RecordFilterBudgetOutcome FilterBudgetOutcome,
    int EvaluatedRecordCount,
    long EvaluatedByteCount,
    IReadOnlyList<RecordFilterLimitation> Limitations);

public sealed record RecordTailBudget
{
    public const int DefaultMaxRecords = 1_000;
    public const int HardMaxRecords = 10_000;
    public const long DefaultMaxRawBytes = 8L * 1024 * 1024;
    public const long HardMaxRawBytes = 64L * 1024 * 1024;
    public static readonly TimeSpan DefaultMaxDuration = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan HardMaxDuration = TimeSpan.FromMinutes(2);
    public const int DefaultMaxRecordsPerSecond = 200;
    public const int HardMaxRecordsPerSecond = 2_000;

    public RecordTailBudget(
        int maxRecords = DefaultMaxRecords,
        long maxRawBytes = DefaultMaxRawBytes,
        TimeSpan? maxDuration = null,
        int maxRecordsPerSecond = DefaultMaxRecordsPerSecond)
    {
        if (maxRecords < 1 || maxRecords > HardMaxRecords)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRecords));
        }

        if (maxRawBytes < 1 || maxRawBytes > HardMaxRawBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRawBytes));
        }

        var actualDuration = maxDuration ?? DefaultMaxDuration;
        if (actualDuration <= TimeSpan.Zero || actualDuration > HardMaxDuration)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDuration));
        }

        if (maxRecordsPerSecond < 1 || maxRecordsPerSecond > HardMaxRecordsPerSecond)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRecordsPerSecond));
        }

        MaxRecords = maxRecords;
        MaxRawBytes = maxRawBytes;
        MaxDuration = actualDuration;
        MaxRecordsPerSecond = maxRecordsPerSecond;
    }

    public int MaxRecords { get; }
    public long MaxRawBytes { get; }
    public TimeSpan MaxDuration { get; }
    public int MaxRecordsPerSecond { get; }
}

public sealed record RecordTailRequest
{
    public RecordTailRequest(
        string admissionIdentity,
        RecordReadRequest initialRead,
        RecordFilterRequest filter,
        RecordTailBudget? budget = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(admissionIdentity);
        ArgumentNullException.ThrowIfNull(initialRead);
        ArgumentNullException.ThrowIfNull(filter);

        AdmissionIdentity = admissionIdentity.Trim();
        InitialRead = initialRead;
        Filter = filter;
        Budget = budget ?? new RecordTailBudget();
    }

    public string AdmissionIdentity { get; }
    public RecordReadRequest InitialRead { get; }
    public RecordFilterRequest Filter { get; }
    public RecordTailBudget Budget { get; }
}

public enum RecordTailFrameKind
{
    Records = 1,
    KafkaFailure = 2,
    AdmissionDenied = 3,
    Completed = 4,
}

public sealed record RecordTailFrame(
    RecordTailFrameKind Kind,
    RecordFilterPage? Page = null,
    Kafdeck.Core.Kafka.KafkaFailure? Failure = null);

public sealed record RecordFilterEvaluationContext(
    KafkaRawRecord RawRecord,
    JsonElement? StructuredValue);
