using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

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
    long ObservedLowWatermark,
    long ObservedHighWatermark,
    long BeforeOffset,
    long PurgeDistance);

public sealed record RecordsPurgeCanonicalIntent(
    string ClusterId,
    bool Irreversible,
    bool UndoSupported,
    string WarningCode,
    long TotalPurgeDistance,
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
        PolicyVersion = RecordsPurgeCanonicalization.RequireIdentifier(
            policyVersion,
            "Records purge policy version",
            256);
    }

    public int MaxTargets { get; }
    public TimeSpan ObservationTimeout { get; }
    public string PolicyVersion { get; }
}

internal static class RecordsPurgeCanonicalValidator
{
    public static bool TryBuildProviderTargets(
        RecordsPurgeCanonicalIntent canonical,
        out RecordsPurgeTarget[] targets)
    {
        targets = Array.Empty<RecordsPurgeTarget>();
        if (canonical is null ||
            !canonical.Irreversible ||
            canonical.UndoSupported ||
            !string.Equals(
                canonical.WarningCode,
                RecordsPurgeCanonicalization.WarningCode,
                StringComparison.Ordinal) ||
            canonical.TotalPurgeDistance < 0 ||
            canonical.Targets is null ||
            canonical.Targets.Count is < 1 or > RecordsPurgePolicy.HardMaxTargets ||
            canonical.Targets.Any(static target => target is null))
        {
            return false;
        }

        try
        {
            _ = RecordsPurgeCanonicalization.RequireIdentifier(
                canonical.ClusterId,
                "Cluster ID",
                256);

            var result = new List<RecordsPurgeTarget>(
                canonical.Targets.Count);
            var seen = new HashSet<(string Topic, int Partition)>();
            long totalDistance = 0;

            foreach (var target in canonical.Targets.OrderBy(item => item.Ordinal))
            {
                if (target is null ||
                    target.Ordinal != result.Count ||
                    target.Selector is null ||
                    !Enum.IsDefined(target.Selector.Kind) ||
                    target.Partition < 0 ||
                    target.ObservedLowWatermark < 0 ||
                    target.ObservedHighWatermark < target.ObservedLowWatermark ||
                    target.BeforeOffset < target.ObservedLowWatermark ||
                    target.BeforeOffset > target.ObservedHighWatermark ||
                    target.PurgeDistance < 0 ||
                    target.PurgeDistance !=
                        checked(
                            target.BeforeOffset -
                            target.ObservedLowWatermark) ||
                    !SelectorMatches(target))
                {
                    return false;
                }

                var topic =
                    RecordsPurgeCanonicalization.RequireTopicName(
                        target.TopicName);
                if (!seen.Add((topic, target.Partition)))
                    return false;

                totalDistance = checked(
                    totalDistance + target.PurgeDistance);
                result.Add(
                    new RecordsPurgeTarget(
                        topic,
                        target.Partition,
                        target.BeforeOffset));
            }

            if (totalDistance != canonical.TotalPurgeDistance)
                return false;

            targets = result.ToArray();
            return true;
        }
        catch (Exception exception) when (
            exception is ArgumentException or
            OverflowException)
        {
            return false;
        }
    }

    private static bool SelectorMatches(
        RecordsPurgeCanonicalTarget target)
    {
        return target.Selector.Kind switch
        {
            RecordsPurgeSelectorKind.Absolute =>
                target.Selector.TimestampUnixMilliseconds is null &&
                target.Selector.RequestedBeforeOffset is >= 0 &&
                target.Selector.RequestedBeforeOffset.Value ==
                    target.BeforeOffset,

            RecordsPurgeSelectorKind.Timestamp =>
                target.Selector.RequestedBeforeOffset is null &&
                target.Selector.TimestampUnixMilliseconds.HasValue &&
                TimestampIsSupported(
                    target.Selector.TimestampUnixMilliseconds.Value),

            _ => false,
        };
    }

    private static bool TimestampIsSupported(long unixMilliseconds)
    {
        try
        {
            _ = DateTimeOffset.FromUnixTimeMilliseconds(
                unixMilliseconds);
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
    }
}

internal static class RecordsPurgeCanonicalization
{
    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    internal const string WarningCode = "records_purge_irreversible_no_undo";

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

    public static string ResourceKey(
        string clusterId,
        string topic,
        int partition) =>
        $"cluster/{clusterId}/topic/{topic}/partition/{partition.ToString(CultureInfo.InvariantCulture)}";

    public static string FingerprintPartition(
        RecordsPurgePartitionObservation observation)
    {
        ArgumentNullException.ThrowIfNull(observation);
        var builder = new StringBuilder(256);
        Append(builder, "topic", observation.Topic);
        Append(
            builder,
            "partition",
            observation.Partition.ToString(CultureInfo.InvariantCulture));
        Append(
            builder,
            "low",
            observation.LowWatermark.ToString(CultureInfo.InvariantCulture));

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    public static string Serialize(RecordsPurgeCanonicalIntent value) =>
        JsonSerializer.Serialize(value, JsonOptions);

    public static RecordsPurgeCanonicalIntent Deserialize(string value) =>
        JsonSerializer.Deserialize<RecordsPurgeCanonicalIntent>(
            value,
            JsonOptions)
        ?? throw new MutationStateException(
            "Records purge canonical intent could not be deserialized.");

    private static void Append(
        StringBuilder builder,
        string key,
        string value) =>
        builder.Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}
