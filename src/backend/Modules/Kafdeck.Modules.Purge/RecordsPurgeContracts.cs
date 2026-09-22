using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Purge;

public enum RecordsPurgeSelectorKind
{
    Absolute = 1,
    Timestamp = 2,
}

public enum RecordsPurgePlanningFailureCode
{
    InvalidInput = 1,
    LimitExceeded = 2,
    TargetNotFound = 3,
    OffsetOutOfRange = 4,
    TimestampUnresolved = 5,
    ProviderUnauthorized = 6,
    ProviderUnsupported = 7,
    ProviderUnavailable = 8,
    ObservationFailed = 9,
}

public sealed record RecordsPurgePlanningFailure(
    RecordsPurgePlanningFailureCode Code,
    string SafeMessage,
    int? TargetOrdinal = null);

public sealed record RecordsPurgeSelector(
    RecordsPurgeSelectorKind Kind,
    long? BeforeOffset = null,
    DateTimeOffset? TimestampUtc = null);

public sealed record RecordsPurgeTargetInput(
    string TopicName,
    int Partition,
    RecordsPurgeSelector Selector);

public sealed record RecordsPurgeRequest(
    string ClusterId,
    IReadOnlyList<RecordsPurgeTargetInput> Targets);

public sealed record RecordsPurgeCanonicalSelector(
    RecordsPurgeSelectorKind Kind,
    long? RequestedBeforeOffset,
    long? TimestampUnixMilliseconds);

public sealed record RecordsPurgeCanonicalTarget(
    int Ordinal,
    string TopicName,
    int Partition,
    RecordsPurgeCanonicalSelector Selector,
    long LowWatermark,
    long HighWatermark,
    long ResolvedBeforeOffset);

public sealed record RecordsPurgeCanonicalIntent(
    string ClusterId,
    bool Irreversible,
    IReadOnlyList<RecordsPurgeCanonicalTarget> Targets);

public sealed record RecordsPurgePlan(
    RecordsPurgeCanonicalIntent Canonical,
    MutationIntentDescriptor Intent,
    MutationRiskDecision Risk);

public sealed record RecordsPurgePlanningResult
{
    private RecordsPurgePlanningResult(
        RecordsPurgePlan? plan,
        RecordsPurgePlanningFailure? failure)
    {
        Plan = plan;
        Failure = failure;
    }

    public RecordsPurgePlan? Plan { get; }
    public RecordsPurgePlanningFailure? Failure { get; }
    public bool IsSuccess => Plan is not null && Failure is null;

    public static RecordsPurgePlanningResult Success(RecordsPurgePlan plan) =>
        new(plan ?? throw new ArgumentNullException(nameof(plan)), null);

    public static RecordsPurgePlanningResult Failed(
        RecordsPurgePlanningFailure failure) =>
        new(null, failure ?? throw new ArgumentNullException(nameof(failure)));
}

public sealed record RecordsPurgePolicy
{
    public const int HardMaxTargets = 256;

    public static RecordsPurgePolicy Default { get; } = new();

    public RecordsPurgePolicy(
        int maxTargets = 64,
        TimeSpan? observationTimeout = null,
        string policyVersion = "v0.5-records-purge-p1")
    {
        var effectiveTimeout = observationTimeout ?? TimeSpan.FromSeconds(10);
        if (maxTargets is < 1 or > HardMaxTargets)
            throw new ArgumentOutOfRangeException(nameof(maxTargets));
        if (effectiveTimeout < TimeSpan.FromSeconds(1) ||
            effectiveTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(observationTimeout));
        }

        MaxTargets = maxTargets;
        ObservationTimeout = effectiveTimeout;
        PolicyVersion = RecordsPurgeCanonicalization.RequireIdentifier(
            policyVersion,
            "Records purge policy version",
            256);
    }

    public int MaxTargets { get; }
    public TimeSpan ObservationTimeout { get; }
    public string PolicyVersion { get; }
}

internal static class RecordsPurgeCanonicalization
{
    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

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
            throw new ArgumentException($"{fieldName} is invalid.", fieldName);
        }

        return normalized;
    }

    public static string RequireTopicName(string value)
    {
        var normalized = RequireIdentifier(value, "Topic name", 249);
        if (normalized is "." or ".." ||
            normalized.Any(character =>
                !(character is >= 'a' and <= 'z' or
                  >= 'A' and <= 'Z' or
                  >= '0' and <= '9' or
                  '.' or '_' or '-')))
        {
            throw new ArgumentException(
                "Topic name is not an admitted exact Kafka topic identifier.",
                nameof(value));
        }

        return normalized;
    }

    public static string PartitionResourceKey(
        string clusterId,
        string topic,
        int partition) =>
        $"cluster/{clusterId}/topic/{topic}/partition/{partition.ToString(CultureInfo.InvariantCulture)}";

    public static string TopicAuthorizationResource(string topic) =>
        $"topic/{topic}";

    public static string PartitionPreconditionFingerprint(
        RecordsPurgePartitionObservation observation)
    {
        var builder = new StringBuilder(256);
        Append(builder, "topic", observation.TopicName);
        Append(
            builder,
            "partition",
            observation.Partition.ToString(CultureInfo.InvariantCulture));
        Append(
            builder,
            "low",
            observation.LowWatermark.ToString(CultureInfo.InvariantCulture));
        Append(
            builder,
            "high",
            observation.HighWatermark.ToString(CultureInfo.InvariantCulture));
        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    public static string Serialize<T>(T value) =>
        JsonSerializer.Serialize(value, JsonOptions);

    public static T Deserialize<T>(string value)
        where T : class =>
        JsonSerializer.Deserialize<T>(value, JsonOptions)
        ?? throw new MutationStateException(
            $"Records purge canonical intent '{typeof(T).Name}' could not be deserialized.");

    private static void Append(StringBuilder builder, string key, string value) =>
        builder.Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}
