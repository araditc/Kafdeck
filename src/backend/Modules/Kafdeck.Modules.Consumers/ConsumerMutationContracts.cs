using System.Collections.ObjectModel;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Consumers;

public enum ConsumerOffsetSelectorKind
{
    Absolute = 1,
    Earliest = 2,
    Latest = 3,
    Timestamp = 4,
    RelativeShift = 5,
}

public enum ConsumerOffsetMovement
{
    MissingCurrent = 1,
    Unchanged = 2,
    BackwardReplay = 3,
    ForwardSkip = 4,
}

public enum ConsumerDeleteMode
{
    Group = 1,
    Offsets = 2,
}

public enum ConsumerMutationPlanningFailureCode
{
    InvalidInput = 1,
    LimitExceeded = 2,
    GroupNotFound = 3,
    GroupStateUnsafe = 4,
    TargetNotFound = 5,
    MissingCommittedOffset = 6,
    OffsetOutOfRange = 7,
    TimestampUnresolved = 8,
    ProviderUnauthorized = 9,
    ProviderUnsupported = 10,
    ProviderUnavailable = 11,
    ObservationFailed = 12,
}

public sealed record ConsumerMutationPlanningFailure(
    ConsumerMutationPlanningFailureCode Code,
    string SafeMessage,
    int? TargetOrdinal = null);

public sealed record ConsumerOffsetSelector(
    ConsumerOffsetSelectorKind Kind,
    long? Value = null,
    DateTimeOffset? TimestampUtc = null);

public sealed record ConsumerOffsetAlterTargetInput(
    string TopicName,
    int Partition,
    ConsumerOffsetSelector Selector);

public sealed record ConsumerOffsetAlterRequest(
    string ClusterId,
    string GroupId,
    IReadOnlyList<ConsumerOffsetAlterTargetInput> Targets);

public sealed record ConsumerOffsetDeleteTargetInput(
    string TopicName,
    int Partition);

public sealed record ConsumerDeleteRequest(
    string ClusterId,
    string GroupId,
    ConsumerDeleteMode Mode,
    IReadOnlyList<ConsumerOffsetDeleteTargetInput>? Targets = null);

public sealed record ConsumerOffsetCanonicalSelector(
    ConsumerOffsetSelectorKind Kind,
    long? RequestedValue,
    long? TimestampUnixMilliseconds);

public sealed record ConsumerOffsetCanonicalTarget(
    int Ordinal,
    string TopicName,
    int Partition,
    ConsumerOffsetCanonicalSelector Selector,
    bool CommittedOffsetMissing,
    long? CommittedOffset,
    long LowWatermark,
    long HighWatermark,
    long ResolvedOffset,
    long? DeltaFromCommitted,
    ConsumerOffsetMovement Movement);

public sealed record ConsumerDeleteCanonicalTarget(
    int Ordinal,
    string TopicName,
    int Partition,
    bool CommittedOffsetMissing,
    long? CommittedOffset,
    long LowWatermark,
    long HighWatermark);

public sealed record ConsumerOffsetAlterCanonicalIntent(
    string ClusterId,
    string GroupId,
    ConsumerGroupState GroupState,
    string GroupFingerprint,
    IReadOnlyList<ConsumerOffsetCanonicalTarget> Targets);

public sealed record ConsumerDeleteCanonicalIntent(
    string ClusterId,
    string GroupId,
    ConsumerDeleteMode Mode,
    ConsumerGroupState GroupState,
    string GroupFingerprint,
    IReadOnlyList<ConsumerDeleteCanonicalTarget> Targets);

public sealed record ConsumerMutationPlan<TCanonical>(
    TCanonical Canonical,
    MutationIntentDescriptor Intent,
    MutationRiskDecision Risk)
    where TCanonical : class;

public sealed record ConsumerMutationPlanningResult<TCanonical>
    where TCanonical : class
{
    private ConsumerMutationPlanningResult(
        ConsumerMutationPlan<TCanonical>? plan,
        ConsumerMutationPlanningFailure? failure)
    {
        Plan = plan;
        Failure = failure;
    }

    public ConsumerMutationPlan<TCanonical>? Plan { get; }
    public ConsumerMutationPlanningFailure? Failure { get; }
    public bool IsSuccess => Plan is not null && Failure is null;

    public static ConsumerMutationPlanningResult<TCanonical> Success(
        ConsumerMutationPlan<TCanonical> plan) =>
        new(plan ?? throw new ArgumentNullException(nameof(plan)), null);

    public static ConsumerMutationPlanningResult<TCanonical> Failed(
        ConsumerMutationPlanningFailure failure) =>
        new(null, failure ?? throw new ArgumentNullException(nameof(failure)));
}

public sealed record ConsumerMutationPolicy
{
    public const int HardMaxTargets = 256;

    public static ConsumerMutationPolicy Default { get; } = new();

    public ConsumerMutationPolicy(
        int maxTargets = 64,
        TimeSpan? observationTimeout = null,
        string policyVersion = "v0.5-consumer-admin-p1",
        bool requireEmptyGroup = true)
    {
        var effectiveObservationTimeout =
            observationTimeout ?? TimeSpan.FromSeconds(10);

        if (maxTargets is < 1 or > HardMaxTargets)
            throw new ArgumentOutOfRangeException(nameof(maxTargets));
        if (effectiveObservationTimeout < TimeSpan.FromSeconds(1) ||
            effectiveObservationTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(observationTimeout));
        }

        MaxTargets = maxTargets;
        ObservationTimeout = effectiveObservationTimeout;
        PolicyVersion = ConsumerMutationCanonicalization.RequireIdentifier(
            policyVersion,
            "Consumer mutation policy version",
            256);
        RequireEmptyGroup = requireEmptyGroup;
    }

    public int MaxTargets { get; }
    public TimeSpan ObservationTimeout { get; }
    public string PolicyVersion { get; }
    public bool RequireEmptyGroup { get; }
}

internal static class ConsumerMutationCanonicalization
{
    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    public static string RequireIdentifier(string value, string fieldName, int maxLength)
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

    public static string GroupResourceKey(string clusterId, string groupId) =>
        $"cluster/{clusterId}/consumer-group/{groupId}";

    public static string PartitionResourceKey(
        string clusterId,
        string topic,
        int partition) =>
        $"cluster/{clusterId}/topic/{topic}/partition/{partition.ToString(CultureInfo.InvariantCulture)}";

    public static string GroupAuthorizationResource(string groupId) =>
        $"consumer-group/{groupId}";

    public static string TopicAuthorizationResource(string topic) =>
        $"topic/{topic}";

    public static string GroupPreconditionFingerprint(
        ConsumerMutationObservation observation)
    {
        var builder = new StringBuilder(1024);
        Append(builder, "exists", observation.Exists ? "1" : "0");
        Append(builder, "group", observation.GroupId);
        Append(builder, "state", ((int)observation.State).ToString(CultureInfo.InvariantCulture));

        foreach (var member in observation.Members
                     .OrderBy(item => item.MemberId, StringComparer.Ordinal))
        {
            Append(builder, "member", member.MemberId);
            Append(builder, "instance", member.GroupInstanceId ?? string.Empty);
            Append(builder, "client", member.ClientId ?? string.Empty);
            Append(builder, "host", member.ClientHost ?? string.Empty);
            foreach (var assignment in member.Assignments
                         .OrderBy(item => item.Topic, StringComparer.Ordinal)
                         .ThenBy(item => item.Partition))
            {
                Append(builder, "assignment-topic", assignment.Topic);
                Append(
                    builder,
                    "assignment-partition",
                    assignment.Partition.ToString(CultureInfo.InvariantCulture));
            }
        }

        return Hash(builder);
    }

    public static string PartitionPreconditionFingerprint(
        ConsumerMutationPartitionObservation observation)
    {
        var builder = new StringBuilder(512);
        Append(builder, "topic", observation.Topic);
        Append(
            builder,
            "partition",
            observation.Partition.ToString(CultureInfo.InvariantCulture));
        Append(
            builder,
            "committed",
            observation.CommittedOffset?.ToString(CultureInfo.InvariantCulture) ?? "missing");
        Append(
            builder,
            "low",
            observation.LowWatermark.ToString(CultureInfo.InvariantCulture));
        Append(
            builder,
            "high",
            observation.HighWatermark.ToString(CultureInfo.InvariantCulture));
        Append(
            builder,
            "timestamp-offset",
            observation.TimestampResolvedOffset?.ToString(CultureInfo.InvariantCulture) ?? "none");
        return Hash(builder);
    }

    public static string Serialize<T>(T value) =>
        JsonSerializer.Serialize(value, JsonOptions);

    public static T Deserialize<T>(string value)
        where T : class =>
        JsonSerializer.Deserialize<T>(value, JsonOptions)
        ?? throw new MutationStateException(
            $"Consumer mutation canonical intent '{typeof(T).Name}' could not be deserialized.");

    private static string Hash(StringBuilder builder) =>
        Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();

    private static void Append(StringBuilder builder, string key, string value) =>
        builder.Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}
