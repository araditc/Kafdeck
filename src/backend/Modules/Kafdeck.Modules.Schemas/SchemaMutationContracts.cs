using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Schemas;

public enum SchemaMutationPlanningFailureCode
{
    InvalidInput = 1,
    LimitExceeded = 2,
    ProviderNotConfigured = 3,
    ProviderUnauthorized = 4,
    ProviderUnsupported = 5,
    ProviderUnavailable = 6,
    ObservationFailed = 7,
    SubjectNotFound = 8,
    VersionNotFound = 9,
    ReferenceNotFound = 10,
    CompatibilityUnavailable = 11,
    SchemaIncompatible = 12,
    NoChange = 13,
    PermanentDeleteRequiresSoftDelete = 14,
}

public sealed record SchemaMutationPlanningFailure(
    SchemaMutationPlanningFailureCode Code,
    string SafeMessage);

public sealed record SchemaRegistrationRequest(
    string ClusterId,
    string Subject,
    RecordSchemaFormat Format,
    string Schema,
    IReadOnlyList<RecordSchemaReference> References);

public sealed record SchemaCompatibilityAlterRequest(
    string ClusterId,
    SchemaCompatibilityScope Scope,
    string? Subject,
    SchemaCompatibilityMode RequestedMode);

public sealed record SchemaDeleteRequest(
    string ClusterId,
    string Subject,
    int? Version,
    bool Permanent);

public sealed record SchemaCanonicalReference(
    string Name,
    string Subject,
    int Version,
    string TargetFingerprint);

public sealed record SchemaCreateCanonicalIntent(
    string ClusterId,
    string Subject,
    RecordSchemaFormat Format,
    string MaterialName,
    int SchemaBytes,
    string SchemaSha256,
    IReadOnlyList<SchemaCanonicalReference> References,
    bool SubjectExists,
    int? LatestVersion,
    string SubjectFingerprint,
    SchemaCompatibilityMode CompatibilityMode,
    bool CompatibilityInherited,
    bool CompatibilityValidated,
    string CompatibilityResultCode);

public sealed record SchemaCompatibilityCanonicalIntent(
    string ClusterId,
    SchemaCompatibilityScope Scope,
    string? Subject,
    SchemaCompatibilityMode CurrentMode,
    bool CurrentInherited,
    SchemaCompatibilityMode RequestedMode,
    string StateFingerprint);

public sealed record SchemaDeleteCanonicalIntent(
    string ClusterId,
    string Subject,
    int? Version,
    bool Permanent,
    bool ExistsActive,
    bool ExistsIncludingDeleted,
    bool IsSoftDeleted,
    string TargetFingerprint);

public sealed record SchemaMutationPlan<TCanonical>(
    TCanonical Canonical,
    MutationIntentDescriptor Intent,
    MutationRiskDecision Risk)
    where TCanonical : class;

public sealed record SchemaMutationPlanningResult<TCanonical>
    where TCanonical : class
{
    private SchemaMutationPlanningResult(
        SchemaMutationPlan<TCanonical>? plan,
        MutationExecutionMaterial? executionMaterial,
        SchemaMutationPlanningFailure? failure)
    {
        Plan = plan;
        ExecutionMaterial = executionMaterial;
        Failure = failure;
    }

    public SchemaMutationPlan<TCanonical>? Plan { get; }
    public MutationExecutionMaterial? ExecutionMaterial { get; }
    public SchemaMutationPlanningFailure? Failure { get; }
    public bool IsSuccess => Plan is not null && Failure is null;

    public static SchemaMutationPlanningResult<TCanonical> Success(
        SchemaMutationPlan<TCanonical> plan,
        MutationExecutionMaterial? executionMaterial = null) =>
        new(
            plan ?? throw new ArgumentNullException(nameof(plan)),
            executionMaterial,
            null);

    public static SchemaMutationPlanningResult<TCanonical> Failed(
        SchemaMutationPlanningFailure failure) =>
        new(
            null,
            null,
            failure ?? throw new ArgumentNullException(nameof(failure)));
}

public sealed record SchemaMutationPolicy
{
    public const int HardMaxSchemaBytes = 4 * 1024 * 1024;
    public const int HardMaxReferences = 64;
    public const int HardMaxSubjectCharacters = 1_024;
    public const int HardMaxReferenceNameCharacters = 1_024;

    public static SchemaMutationPolicy Default { get; } = new();

    public SchemaMutationPolicy(
        int maxSchemaBytes = HardMaxSchemaBytes,
        int maxReferences = HardMaxReferences,
        TimeSpan? observationTimeout = null,
        string policyVersion = "v0.5-schema-mutation-p1")
    {
        var timeout =
            observationTimeout ?? TimeSpan.FromSeconds(10);

        if (maxSchemaBytes is < 1 or > HardMaxSchemaBytes)
            throw new ArgumentOutOfRangeException(nameof(maxSchemaBytes));
        if (maxReferences is < 0 or > HardMaxReferences)
            throw new ArgumentOutOfRangeException(nameof(maxReferences));
        if (timeout < TimeSpan.FromSeconds(1) ||
            timeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(
                nameof(observationTimeout));
        }

        MaxSchemaBytes = maxSchemaBytes;
        MaxReferences = maxReferences;
        ObservationTimeout = timeout;
        PolicyVersion = SchemaMutationCanonicalization.RequireIdentifier(
            policyVersion,
            "Schema mutation policy version",
            256);
    }

    public int MaxSchemaBytes { get; }
    public int MaxReferences { get; }
    public TimeSpan ObservationTimeout { get; }
    public string PolicyVersion { get; }
}

internal static class SchemaMutationCanonicalization
{
    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    public static string RequireIdentifier(
        string value,
        string fieldName,
        int maxLength)
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

    public static string RequireSubject(string value) =>
        RequireIdentifier(
            value,
            "Schema subject",
            SchemaMutationPolicy.HardMaxSubjectCharacters);

    public static string RequireReferenceName(string value) =>
        RequireIdentifier(
            value,
            "Schema reference name",
            SchemaMutationPolicy.HardMaxReferenceNameCharacters);

    public static IReadOnlyList<RecordSchemaReference> NormalizeReferences(
        IReadOnlyList<RecordSchemaReference> references,
        int maxReferences)
    {
        ArgumentNullException.ThrowIfNull(references);
        if (references.Count > maxReferences)
        {
            throw new ArgumentOutOfRangeException(
                nameof(references),
                "Schema references exceed the configured count ceiling.");
        }

        var byName = new Dictionary<string, RecordSchemaReference>(
            StringComparer.Ordinal);

        foreach (var reference in references)
        {
            ArgumentNullException.ThrowIfNull(reference);
            var name = RequireReferenceName(reference.Name);
            var subject = RequireSubject(reference.Subject);
            if (reference.Version <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(references),
                    "Schema reference version must be positive.");
            }

            var normalized = new RecordSchemaReference(
                name,
                subject,
                reference.Version);

            if (byName.TryGetValue(name, out var existing))
            {
                if (existing != normalized)
                {
                    throw new ArgumentException(
                        "Schema reference names must be unique.",
                        nameof(references));
                }

                continue;
            }

            byName.Add(name, normalized);
        }

        return Array.AsReadOnly(
            byName.Values
                .OrderBy(item => item.Name, StringComparer.Ordinal)
                .ThenBy(item => item.Subject, StringComparer.Ordinal)
                .ThenBy(item => item.Version)
                .ToArray());
    }

    public static byte[] EncodeSchema(
        string schema,
        int maxBytes)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        var bytes = StrictUtf8.GetBytes(schema);
        if (bytes.Length > maxBytes)
        {
            CryptographicOperations.ZeroMemory(bytes);
            throw new ArgumentOutOfRangeException(
                nameof(schema),
                "Schema exceeds the configured byte ceiling.");
        }

        return bytes;
    }

    public static string DecodeSchema(
        ReadOnlyMemory<byte> bytes)
    {
        if (bytes.Length is < 1 or > SchemaMutationPolicy.HardMaxSchemaBytes)
        {
            throw new MutationStateException(
                "Schema execution material is outside the admitted byte bounds.");
        }

        try
        {
            return StrictUtf8.GetString(bytes.Span);
        }
        catch (DecoderFallbackException)
        {
            throw new MutationStateException(
                "Schema execution material is not valid UTF-8.");
        }
    }

    public static string Sha256(
        ReadOnlySpan<byte> value) =>
        Convert.ToHexString(
                SHA256.HashData(value))
            .ToLowerInvariant();

    public static string Serialize<T>(T value) =>
        JsonSerializer.Serialize(value, JsonOptions);

    public static T Deserialize<T>(string value)
        where T : class =>
        JsonSerializer.Deserialize<T>(
            value,
            JsonOptions)
        ?? throw new MutationStateException(
            $"Schema mutation canonical intent '{typeof(T).Name}' could not be deserialized.");

    public static string CompatibilityName(
        SchemaCompatibilityMode mode) =>
        mode switch
        {
            SchemaCompatibilityMode.None => "NONE",
            SchemaCompatibilityMode.Backward => "BACKWARD",
            SchemaCompatibilityMode.BackwardTransitive =>
                "BACKWARD_TRANSITIVE",
            SchemaCompatibilityMode.Forward => "FORWARD",
            SchemaCompatibilityMode.ForwardTransitive =>
                "FORWARD_TRANSITIVE",
            SchemaCompatibilityMode.Full => "FULL",
            SchemaCompatibilityMode.FullTransitive =>
                "FULL_TRANSITIVE",
            _ => throw new ArgumentOutOfRangeException(
                nameof(mode),
                "Unknown compatibility mode is not a valid mutation target."),
        };

    public static string ResourceKey(
        string clusterId,
        string subject) =>
        $"cluster/{clusterId}/schema/{subject}";

    public static string AuthorizationResource(
        string subject) =>
        $"schema/{subject}";

    public static string GlobalCompatibilityResource =>
        "schema-compatibility/global";

    public static string FingerprintText(
        IEnumerable<KeyValuePair<string, string>> values)
    {
        var builder = new StringBuilder();
        foreach (var pair in values)
        {
            builder.Append(pair.Key)
                .Append('=')
                .Append(
                    Convert.ToBase64String(
                        Encoding.UTF8.GetBytes(pair.Value)))
                .Append('\n');
        }

        return Sha256(
            Encoding.UTF8.GetBytes(builder.ToString()));
    }
}
