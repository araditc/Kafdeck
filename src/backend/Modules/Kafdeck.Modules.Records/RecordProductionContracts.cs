using System.Collections.ObjectModel;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public enum RecordProductionPlanningFailureCode
{
    InvalidInput = 1,
    LimitExceeded = 2,
    TopicNotFound = 3,
    InternalTopicUnsupported = 4,
    ProviderUnauthorized = 5,
    ProviderUnsupported = 6,
    ProviderUnavailable = 7,
    ObservationFailed = 8,
    SchemaValidationUnavailable = 9,
    SchemaValidationFailed = 10,
    TemplateInvalid = 11,
    PolicyApprovalUnsupported = 12,
}

public sealed record RecordProductionPlanningFailure(
    RecordProductionPlanningFailureCode Code,
    string SafeMessage,
    int? RecordOrdinal = null);

public sealed record RecordProductionPolicy
{
    public const int HardMaxRecords = MutationLimits.MaxMaterialDigests;
    public const int HardMaxHeadersPerRecord = 64;
    public const int HardMaxHeaderNameCharacters = 256;
    public const int HardMaxKeyBytes = 1024 * 1024;
    public const int HardMaxValueBytes = 4 * 1024 * 1024;
    public const long HardMaxTotalBytes = 16L * 1024 * 1024;

    public RecordProductionPolicy(
        int maxRecords = 16,
        int maxHeadersPerRecord = 32,
        int maxKeyBytes = 256 * 1024,
        int maxValueBytes = 1024 * 1024,
        long maxTotalBytes = 1024 * 1024,
        int highRiskRecordCount = 32,
        long highRiskTotalBytes = 8L * 1024 * 1024,
        bool requireIndependentApprovalForHighRisk = false)
    {
        if (maxRecords is < 1 or > HardMaxRecords)
            throw new ArgumentOutOfRangeException(nameof(maxRecords));
        if (maxHeadersPerRecord is < 0 or > HardMaxHeadersPerRecord)
            throw new ArgumentOutOfRangeException(nameof(maxHeadersPerRecord));
        if (maxKeyBytes is < 0 or > HardMaxKeyBytes)
            throw new ArgumentOutOfRangeException(nameof(maxKeyBytes));
        if (maxValueBytes is < 1 or > HardMaxValueBytes)
            throw new ArgumentOutOfRangeException(nameof(maxValueBytes));
        if (maxTotalBytes is < 1 or > HardMaxTotalBytes)
            throw new ArgumentOutOfRangeException(nameof(maxTotalBytes));
        if (highRiskRecordCount is < 1 or > HardMaxRecords)
            throw new ArgumentOutOfRangeException(nameof(highRiskRecordCount));
        if (highRiskTotalBytes is < 1 or > HardMaxTotalBytes)
            throw new ArgumentOutOfRangeException(nameof(highRiskTotalBytes));

        MaxRecords = maxRecords;
        MaxHeadersPerRecord = maxHeadersPerRecord;
        MaxKeyBytes = maxKeyBytes;
        MaxValueBytes = maxValueBytes;
        MaxTotalBytes = maxTotalBytes;
        HighRiskRecordCount = highRiskRecordCount;
        HighRiskTotalBytes = highRiskTotalBytes;
        RequireIndependentApprovalForHighRisk = requireIndependentApprovalForHighRisk;
    }

    public int MaxRecords { get; }
    public int MaxHeadersPerRecord { get; }
    public int MaxKeyBytes { get; }
    public int MaxValueBytes { get; }
    public long MaxTotalBytes { get; }
    public int HighRiskRecordCount { get; }
    public long HighRiskTotalBytes { get; }
    public bool RequireIndependentApprovalForHighRisk { get; }

    public MutationRiskDecision ClassifyRisk(int recordCount, long totalBytes)
    {
        var baseDecision = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.RecordProduce));

        if (recordCount < HighRiskRecordCount && totalBytes < HighRiskTotalBytes)
            return baseDecision;

        return new MutationRiskDecision(
            MutationRiskClass.High,
            Array.AsReadOnly(
                baseDecision.Reasons
                    .Concat(new[] { "record_production_blast_radius" })
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(value => value, StringComparer.Ordinal)
                    .ToArray()),
            MutationConfirmationMode.TypedTarget,
            RequireIndependentApprovalForHighRisk);
    }
}

public sealed record RecordProductionHeaderInput(
    string Name,
    ReadOnlyMemory<byte> Value);

public sealed record RecordProductionRecordInput(
    ReadOnlyMemory<byte>? Key,
    ReadOnlyMemory<byte> Value,
    IReadOnlyList<RecordProductionHeaderInput> Headers);

public sealed record RecordProductionSchemaRequest(
    string ValidatorId,
    string SchemaIdentity);

public enum RecordProductionSchemaValidationState
{
    Valid = 1,
    Invalid = 2,
    Unavailable = 3,
}

public sealed record RecordProductionSchemaValidationResult(
    RecordProductionSchemaValidationState State,
    string Code,
    string? SchemaFingerprint = null);

public sealed record RecordProductionSchemaValidationContext(
    string ClusterId,
    string TopicName,
    int RecordOrdinal,
    ReadOnlyMemory<byte> Value,
    RecordProductionSchemaRequest Request);

public interface IRecordProductionSchemaValidator
{
    Task<RecordProductionSchemaValidationResult> ValidateAsync(
        RecordProductionSchemaValidationContext context,
        CancellationToken cancellationToken = default);
}

public sealed record RecordProductionTemplateIdentity(
    string TemplateId,
    int Version);

public sealed record RecordProductionRequest(
    string ClusterId,
    string TopicName,
    IReadOnlyList<RecordProductionRecordInput> Records,
    RecordProductionSchemaRequest? SchemaValidation = null,
    RecordProductionTemplateIdentity? Template = null);

public sealed record RecordProductionCanonicalHeader(
    string Name,
    int ValueBytes);

public sealed record RecordProductionCanonicalRecord(
    int Ordinal,
    string MaterialName,
    int? KeyBytes,
    int ValueBytes,
    IReadOnlyList<RecordProductionCanonicalHeader> Headers,
    string? SchemaValidationCode,
    string? SchemaFingerprint);

public sealed record RecordProductionCanonicalSchema(
    string ValidatorId,
    string SchemaIdentity);

public sealed record RecordProductionCanonicalTemplate(
    string TemplateId,
    int Version);

public sealed record RecordProductionCanonicalIntent(
    string ClusterId,
    string TopicName,
    IReadOnlyList<RecordProductionCanonicalRecord> Records,
    long TotalBytes,
    RecordProductionCanonicalSchema? Schema,
    RecordProductionCanonicalTemplate? Template);

public sealed record RecordProductionPlan(
    RecordProductionCanonicalIntent Canonical,
    MutationIntentDescriptor Intent,
    MutationRiskDecision Risk);

public sealed class RecordProductionExecutionMaterial : IDisposable
{
    private readonly Dictionary<string, byte[]> _items;
    private bool _disposed;

    internal RecordProductionExecutionMaterial(Dictionary<string, byte[]> items)
    {
        _items = items;
    }

    [JsonIgnore]
    public IReadOnlyDictionary<string, ReadOnlyMemory<byte>> Items
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return new ReadOnlyDictionary<string, ReadOnlyMemory<byte>>(
                _items.ToDictionary(
                    pair => pair.Key,
                    pair => new ReadOnlyMemory<byte>(pair.Value),
                    StringComparer.Ordinal));
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        foreach (var value in _items.Values)
            CryptographicOperations.ZeroMemory(value);
        _items.Clear();
        _disposed = true;
    }
}

public sealed class RecordProductionPlanningResult : IDisposable
{
    private RecordProductionPlanningResult(
        RecordProductionPlan? plan,
        RecordProductionExecutionMaterial? executionMaterial,
        RecordProductionPlanningFailure? failure)
    {
        Plan = plan;
        ExecutionMaterial = executionMaterial;
        Failure = failure;
    }

    public RecordProductionPlan? Plan { get; }

    [JsonIgnore]
    public RecordProductionExecutionMaterial? ExecutionMaterial { get; }

    public RecordProductionPlanningFailure? Failure { get; }
    public bool IsSuccess => Plan is not null && ExecutionMaterial is not null && Failure is null;

    public static RecordProductionPlanningResult Success(
        RecordProductionPlan plan,
        RecordProductionExecutionMaterial material) =>
        new(plan ?? throw new ArgumentNullException(nameof(plan)),
            material ?? throw new ArgumentNullException(nameof(material)),
            null);

    public static RecordProductionPlanningResult Failed(
        RecordProductionPlanningFailure failure) =>
        new(null, null, failure ?? throw new ArgumentNullException(nameof(failure)));

    public void Dispose() => ExecutionMaterial?.Dispose();
}

public sealed record RecordProductionTemplateDefinition(
    string TemplateId,
    int Version,
    string ValueTemplate,
    IReadOnlyList<string> VariableNames);

public static class RecordProductionTemplateMaterializer
{
    public const int MaxTemplateCharacters = 1024 * 1024;
    public const int MaxVariables = 64;

    public static RecordProductionRequest Materialize(
        string clusterId,
        string topicName,
        RecordProductionTemplateDefinition definition,
        IReadOnlyDictionary<string, string> variables,
        ReadOnlyMemory<byte>? key = null,
        IReadOnlyList<RecordProductionHeaderInput>? headers = null,
        RecordProductionSchemaRequest? schemaValidation = null)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(variables);

        var templateId = RecordProductionValidation.RequireIdentifier(
            definition.TemplateId,
            "Template ID",
            256);
        if (definition.Version <= 0)
            throw new ArgumentOutOfRangeException(nameof(definition.Version));
        if (definition.ValueTemplate.Length > MaxTemplateCharacters)
            throw new ArgumentOutOfRangeException(nameof(definition.ValueTemplate));
        if (definition.VariableNames.Count > MaxVariables)
            throw new ArgumentOutOfRangeException(nameof(definition.VariableNames));

        var expected = definition.VariableNames
            .Select(name => RecordProductionValidation.RequireIdentifier(name, "Template variable", 128))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        if (expected.Length != definition.VariableNames.Count ||
            variables.Count != expected.Length ||
            !variables.Keys.OrderBy(name => name, StringComparer.Ordinal).SequenceEqual(expected, StringComparer.Ordinal))
        {
            throw new ArgumentException(
                "Template variables must exactly match the admitted variable names.",
                nameof(variables));
        }

        var rendered = definition.ValueTemplate;
        foreach (var name in expected)
        {
            var value = variables[name] ?? throw new ArgumentException("Template variable value cannot be null.");
            rendered = rendered.Replace(
                "{{" + name + "}}",
                value,
                StringComparison.Ordinal);
        }

        if (rendered.Contains("{{", StringComparison.Ordinal) ||
            rendered.Contains("}}", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Template contains an unresolved or unadmitted placeholder.",
                nameof(definition));
        }

        var valueBytes = Encoding.UTF8.GetBytes(rendered);
        return new RecordProductionRequest(
            clusterId,
            topicName,
            new[]
            {
                new RecordProductionRecordInput(
                    key,
                    valueBytes,
                    headers ?? Array.Empty<RecordProductionHeaderInput>()),
            },
            schemaValidation,
            new RecordProductionTemplateIdentity(templateId, definition.Version));
    }
}

internal static class RecordProductionValidation
{
    public static string RequireIdentifier(string value, string fieldName, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (!string.Equals(value, normalized, StringComparison.Ordinal) ||
            normalized.Length > maxLength ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"{fieldName} is invalid.",
                fieldName);
        }

        return normalized;
    }

    public static string RequireSafeCode(string value, string fieldName)
    {
        var normalized = RequireIdentifier(value, fieldName, 128);
        if (normalized.Any(character =>
                !(character is >= 'a' and <= 'z' or
                  >= '0' and <= '9' or
                  '.' or '_' or '-')))
        {
            throw new ArgumentException(
                $"{fieldName} must use lowercase safe-code characters.",
                fieldName);
        }

        return normalized;
    }

    public static string? RequireSha256OrNull(string? value, string fieldName)
    {
        if (value is null) return null;
        var normalized = value.Trim().ToLowerInvariant();
        if (normalized.Length != 64 || normalized.Any(c => !char.IsAsciiHexDigit(c)))
            throw new ArgumentException($"{fieldName} must be a SHA-256 hex digest.", fieldName);
        return normalized;
    }
}
