using System.Text.Json;

namespace Kafdeck.Core.Records;

public sealed record RecordStructuredMaskRule
{
    public const int MaxPathCharacters = 256;
    public const int MaxPathSegments = 16;
    public const int MaxReplacementCharacters = 128;

    public RecordStructuredMaskRule(string jsonPointer, string replacement = "[REDACTED]")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jsonPointer);
        ArgumentNullException.ThrowIfNull(replacement);

        if (!jsonPointer.StartsWith("/", StringComparison.Ordinal) || jsonPointer.Length > MaxPathCharacters)
        {
            throw new ArgumentException("Structured masking paths must be bounded JSON-pointer-style paths starting with '/'.", nameof(jsonPointer));
        }

        if (jsonPointer.Any(char.IsControl))
        {
            throw new ArgumentException("Structured masking paths must not contain control characters.", nameof(jsonPointer));
        }

        var segmentCount = jsonPointer.Split('/', StringSplitOptions.None).Length - 1;
        if (segmentCount is < 1 or > MaxPathSegments)
        {
            throw new ArgumentOutOfRangeException(nameof(jsonPointer), $"Structured masking paths must contain between 1 and {MaxPathSegments} segments.");
        }

        if (replacement.Length > MaxReplacementCharacters || replacement.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(nameof(replacement));
        }

        JsonPointer = jsonPointer;
        Replacement = replacement;
    }

    public string JsonPointer { get; }
    public string Replacement { get; }
}

public sealed record RecordHeaderMaskRule
{
    public const int MaxHeaderNameCharacters = 256;
    public const int MaxReplacementCharacters = 128;

    public RecordHeaderMaskRule(string headerName, string replacement = "[REDACTED]")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(headerName);
        ArgumentNullException.ThrowIfNull(replacement);

        if (headerName.Length > MaxHeaderNameCharacters || headerName.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(nameof(headerName));
        }

        if (replacement.Length > MaxReplacementCharacters || replacement.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(nameof(replacement));
        }

        HeaderName = headerName;
        Replacement = replacement;
    }

    public string HeaderName { get; }
    public string Replacement { get; }
}

public sealed record RecordMaskingPolicyDefinition(
    string PolicyId,
    int Version,
    IReadOnlyList<RecordStructuredMaskRule>? StructuredRules = null,
    IReadOnlyList<RecordHeaderMaskRule>? HeaderRules = null,
    bool MaskKey = false,
    string KeyReplacement = "[REDACTED]");

public sealed class CompiledRecordMaskingPolicy : IRecordMaskingPolicySnapshot
{
    public const int MaxStructuredRules = 128;
    public const int MaxHeaderRules = 64;

    internal CompiledRecordMaskingPolicy(
        string policyId,
        int version,
        IReadOnlyList<RecordStructuredMaskRule> structuredRules,
        IReadOnlyList<RecordHeaderMaskRule> headerRules,
        bool maskKey,
        string keyReplacement)
    {
        PolicyId = policyId;
        Version = version;
        StructuredRules = structuredRules;
        HeaderRules = headerRules;
        MaskKey = maskKey;
        KeyReplacement = keyReplacement;
    }

    public string PolicyId { get; }
    public int Version { get; }
    public bool RequiresStructuredValue => StructuredRules.Count > 0;
    public IReadOnlyList<RecordStructuredMaskRule> StructuredRules { get; }
    public IReadOnlyList<RecordHeaderMaskRule> HeaderRules { get; }
    public bool MaskKey { get; }
    public string KeyReplacement { get; }
}

public static class RecordMaskingPolicyCompiler
{
    public static CompiledRecordMaskingPolicy Compile(RecordMaskingPolicyDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrWhiteSpace(definition.PolicyId);

        var policyId = definition.PolicyId.Trim();
        if (policyId.Length > 128 || policyId.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(nameof(definition.PolicyId));
        }

        if (definition.Version < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(definition.Version));
        }

        var structured = (definition.StructuredRules ?? Array.Empty<RecordStructuredMaskRule>()).ToArray();
        var headers = (definition.HeaderRules ?? Array.Empty<RecordHeaderMaskRule>()).ToArray();

        if (structured.Length > CompiledRecordMaskingPolicy.MaxStructuredRules)
        {
            throw new ArgumentOutOfRangeException(nameof(definition.StructuredRules));
        }

        if (headers.Length > CompiledRecordMaskingPolicy.MaxHeaderRules)
        {
            throw new ArgumentOutOfRangeException(nameof(definition.HeaderRules));
        }

        if (definition.KeyReplacement.Length > RecordStructuredMaskRule.MaxReplacementCharacters ||
            definition.KeyReplacement.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(nameof(definition.KeyReplacement));
        }

        var duplicateStructured = structured
            .GroupBy(rule => rule.JsonPointer, StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicateStructured is not null)
        {
            throw new ArgumentException($"Structured masking path '{duplicateStructured.Key}' is defined more than once.");
        }

        var duplicateHeader = headers
            .GroupBy(rule => rule.HeaderName, StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicateHeader is not null)
        {
            throw new ArgumentException($"Header masking rule '{duplicateHeader.Key}' is defined more than once.");
        }

        return new CompiledRecordMaskingPolicy(
            policyId,
            definition.Version,
            Array.AsReadOnly(structured),
            Array.AsReadOnly(headers),
            definition.MaskKey,
            definition.KeyReplacement);
    }
}

public sealed class StaticRecordMaskingPolicyProvider : IRecordMaskingPolicyProvider
{
    public StaticRecordMaskingPolicyProvider(CompiledRecordMaskingPolicy current)
    {
        Current = current ?? throw new ArgumentNullException(nameof(current));
    }

    public IRecordMaskingPolicySnapshot Current { get; }
}

public enum RecordPayloadProjectionKind
{
    Raw = 1,
    Structured = 2,
    FullyRedacted = 3,
}

public sealed record RecordSafeHeader(
    string Name,
    ReadOnlyMemory<byte> Value,
    bool IsRedacted);

public sealed record RecordSafeProjection(
    int Partition,
    long Offset,
    DateTimeOffset? TimestampUtc,
    ReadOnlyMemory<byte>? Key,
    bool KeyRedacted,
    RecordPayloadProjectionKind ValueKind,
    ReadOnlyMemory<byte>? RawValue,
    JsonElement? StructuredValue,
    IReadOnlyList<RecordSafeHeader> Headers,
    string PolicyId,
    int PolicyVersion,
    IReadOnlyList<string> RedactedPaths);

public sealed record RecordSafePage(
    string ClusterId,
    string TopicName,
    int Partition,
    IReadOnlyList<RecordSafeProjection> Records,
    long LowWatermark,
    long HighWatermark,
    RecordAnchor? NextAnchor,
    RecordAnchor? PreviousAnchor,
    RecordBudgetOutcome ReadBudgetOutcome,
    RecordFilterBudgetOutcome FilterBudgetOutcome,
    IReadOnlyList<RecordFilterLimitation> Limitations,
    string PolicyId,
    int PolicyVersion);

public enum RecordExportFormat
{
    Json = 1,
    Ndjson = 2,
    Csv = 3,
}

public enum RecordExportBudgetOutcome
{
    Complete = 1,
    RowLimit = 2,
    ByteLimit = 3,
    DurationLimit = 4,
}

public sealed record RecordExportBudget
{
    public const int DefaultMaxRows = 1_000;
    public const int HardMaxRows = 10_000;
    public const long DefaultMaxBytes = 8L * 1024 * 1024;
    public const long HardMaxBytes = 64L * 1024 * 1024;
    public static readonly TimeSpan DefaultMaxDuration = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan HardMaxDuration = TimeSpan.FromMinutes(2);

    public RecordExportBudget(
        int maxRows = DefaultMaxRows,
        long maxBytes = DefaultMaxBytes,
        TimeSpan? maxDuration = null)
    {
        if (maxRows is < 1 or > HardMaxRows)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRows));
        }

        if (maxBytes is < 128 or > HardMaxBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBytes));
        }

        var duration = maxDuration ?? DefaultMaxDuration;
        if (duration <= TimeSpan.Zero || duration > HardMaxDuration)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDuration));
        }

        MaxRows = maxRows;
        MaxBytes = maxBytes;
        MaxDuration = duration;
    }

    public int MaxRows { get; }
    public long MaxBytes { get; }
    public TimeSpan MaxDuration { get; }
}

public sealed record RecordExportRequest(
    RecordExportFormat Format,
    RecordExportBudget Budget)
{
    public RecordExportRequest(RecordExportFormat format)
        : this(format, new RecordExportBudget())
    {
    }
}

public sealed record RecordExportSummary(
    RecordExportFormat Format,
    int RowCount,
    long ByteCount,
    RecordExportBudgetOutcome Outcome);

public sealed record RecordAccessAuditMetadata(
    string ClusterId,
    string TopicName,
    int Partition,
    string Operation,
    string PolicyId,
    int PolicyVersion,
    long? FirstOffset,
    long? LastOffset,
    RecordBudgetOutcome ReadBudgetOutcome,
    RecordFilterBudgetOutcome FilterBudgetOutcome,
    RecordExportFormat? ExportFormat = null,
    int? ExportRowCount = null,
    long? ExportByteCount = null,
    RecordExportBudgetOutcome? ExportOutcome = null);
