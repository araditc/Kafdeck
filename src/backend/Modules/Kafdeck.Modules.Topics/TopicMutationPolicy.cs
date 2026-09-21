using System.Collections.ObjectModel;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Topics;

public static class TopicMutationPolicy
{
    public const int MaxTopicNameLength = 249;
    public const int MaxBulkTopics = 100;
    public const int MaxPartitionCount = 100_000;

    private static readonly HashSet<string> AllowedConfigurationKeys =
        new(StringComparer.Ordinal)
        {
            "cleanup.policy",
            "retention.ms",
            "retention.bytes",
            "segment.ms",
            "segment.bytes",
            "min.insync.replicas",
            "max.message.bytes",
            "delete.retention.ms",
            "min.cleanable.dirty.ratio",
        };

    private static readonly HashSet<string> DurabilitySensitiveConfigurationKeys =
        new(StringComparer.Ordinal)
        {
            "cleanup.policy",
            "retention.ms",
            "retention.bytes",
            "min.insync.replicas",
            "delete.retention.ms",
            "min.cleanable.dirty.ratio",
        };

    private static readonly JsonSerializerOptions CanonicalJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    public static TopicCreateMutation NormalizeCreate(TopicCreateMutation request)
    {
        ArgumentNullException.ThrowIfNull(request);
        var clusterId = RequireIdentifier(request.ClusterId, "Cluster ID", 256);
        var topicName = NormalizeTopicName(request.TopicName);

        if (request.PartitionCount is < 1 or > MaxPartitionCount)
        {
            throw new TopicMutationPolicyException(
                TopicMutationPlanningFailureCode.InvalidPartitionCount,
                $"Partition count must be between 1 and {MaxPartitionCount}.");
        }

        if (request.ReplicationFactor < 1)
        {
            throw new TopicMutationPolicyException(
                TopicMutationPlanningFailureCode.InvalidReplicationFactor,
                "Replication factor must be positive.");
        }

        var createConfigurations = request.Configurations.ToDictionary(
            pair => pair.Key,
            pair => (string?)pair.Value,
            StringComparer.Ordinal);
        var configurations = NormalizeConfigurations(
            createConfigurations,
            allowDelete: false);

        return new TopicCreateMutation(
            clusterId,
            topicName,
            request.PartitionCount,
            request.ReplicationFactor,
            configurations.ToDictionary(
                pair => pair.Key,
                pair => pair.Value!,
                StringComparer.Ordinal));
    }

    public static TopicAlterMutation NormalizeAlter(TopicAlterMutation request)
    {
        ArgumentNullException.ThrowIfNull(request);
        var clusterId = RequireIdentifier(request.ClusterId, "Cluster ID", 256);
        var topicName = NormalizeTopicName(request.TopicName);

        if (request.Changes.Count == 0)
        {
            throw new TopicMutationPolicyException(
                TopicMutationPlanningFailureCode.InvalidConfigurationValue,
                "At least one topic configuration change is required.");
        }

        var changes = NormalizeConfigurations(request.Changes, allowDelete: true);
        return new TopicAlterMutation(clusterId, topicName, changes);
    }

    public static TopicIncreasePartitionsMutation NormalizeIncreasePartitions(
        TopicIncreasePartitionsMutation request)
    {
        ArgumentNullException.ThrowIfNull(request);
        var clusterId = RequireIdentifier(request.ClusterId, "Cluster ID", 256);
        var topicName = NormalizeTopicName(request.TopicName);

        if (request.NewPartitionCount is < 1 or > MaxPartitionCount)
        {
            throw new TopicMutationPolicyException(
                TopicMutationPlanningFailureCode.InvalidPartitionCount,
                $"Target partition count must be between 1 and {MaxPartitionCount}.");
        }

        return new TopicIncreasePartitionsMutation(
            clusterId,
            topicName,
            request.NewPartitionCount);
    }

    public static TopicDeleteMutation NormalizeDelete(TopicDeleteMutation request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return new TopicDeleteMutation(
            RequireIdentifier(request.ClusterId, "Cluster ID", 256),
            NormalizeTopicName(request.TopicName));
    }

    public static string NormalizeTopicName(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > MaxTopicNameLength ||
            value is "." or ".." ||
            value.Any(character => !IsAllowedTopicCharacter(character)))
        {
            throw new TopicMutationPolicyException(
                TopicMutationPlanningFailureCode.InvalidTopicName,
                "Topic name is not an admitted exact Kafka topic identifier.");
        }

        return value;
    }

    public static bool IsDurabilitySensitive(IEnumerable<string> configurationKeys)
    {
        ArgumentNullException.ThrowIfNull(configurationKeys);
        return configurationKeys.Any(
            key => DurabilitySensitiveConfigurationKeys.Contains(key));
    }

    public static string ResourceKey(string clusterId, string topicName) =>
        $"cluster/{clusterId}/topic/{topicName}";

    public static MutationIntentDescriptor BuildIntent(
        TopicCreateMutation mutation,
        IReadOnlyList<MutationPrecondition> preconditions)
    {
        var sensitive = IsDurabilitySensitive(mutation.Configurations.Keys);
        return BuildIntent(
            MutationOperationKind.TopicCreate,
            mutation.ClusterId,
            mutation.TopicName,
            Serialize(mutation),
            preconditions,
            new MutationRiskContext(DurabilitySensitiveChange: sensitive));
    }

    public static MutationIntentDescriptor BuildIntent(
        TopicAlterMutation mutation,
        IReadOnlyList<MutationPrecondition> preconditions)
    {
        var sensitive = IsDurabilitySensitive(mutation.Changes.Keys);
        return BuildIntent(
            MutationOperationKind.TopicAlter,
            mutation.ClusterId,
            mutation.TopicName,
            Serialize(mutation),
            preconditions,
            new MutationRiskContext(DurabilitySensitiveChange: sensitive));
    }

    public static MutationIntentDescriptor BuildIntent(
        TopicIncreasePartitionsMutation mutation,
        IReadOnlyList<MutationPrecondition> preconditions) =>
        BuildIntent(
            MutationOperationKind.TopicIncreasePartitions,
            mutation.ClusterId,
            mutation.TopicName,
            Serialize(mutation),
            preconditions,
            new MutationRiskContext());

    public static MutationIntentDescriptor BuildIntent(
        TopicDeleteMutation mutation,
        IReadOnlyList<MutationPrecondition> preconditions) =>
        BuildIntent(
            MutationOperationKind.TopicDelete,
            mutation.ClusterId,
            mutation.TopicName,
            Serialize(mutation),
            preconditions,
            new MutationRiskContext());

    public static string Serialize<TMutation>(TMutation mutation) =>
        JsonSerializer.Serialize(mutation, CanonicalJsonOptions);

    public static TMutation Deserialize<TMutation>(string canonicalIntent)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(canonicalIntent);
        return JsonSerializer.Deserialize<TMutation>(
                   canonicalIntent,
                   CanonicalJsonOptions) ??
               throw new MutationStateException(
                   $"Canonical topic mutation intent '{typeof(TMutation).Name}' could not be deserialized.");
    }

    public static string FingerprintTopicMetadata(TopicMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        var builder = new StringBuilder(1024);
        Append(builder, "topic", metadata.Name);
        Append(builder, "internal", metadata.IsInternal ? "1" : "0");

        foreach (var partition in metadata.Partitions.OrderBy(item => item.PartitionId))
        {
            Append(
                builder,
                "partition",
                partition.PartitionId.ToString(CultureInfo.InvariantCulture));
            Append(
                builder,
                "leader",
                partition.LeaderBrokerId?.ToString(CultureInfo.InvariantCulture) ?? "none");
            Append(
                builder,
                "replicas",
                string.Join(
                    ",",
                    partition.ReplicaBrokerIds.OrderBy(id => id)));
            Append(
                builder,
                "isr",
                string.Join(
                    ",",
                    partition.InSyncReplicaBrokerIds.OrderBy(id => id)));
        }

        return Hash(builder);
    }

    public static string FingerprintCreateAbsence(
        ClusterMetadata cluster,
        string topicName)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        var builder = new StringBuilder(512);
        Append(builder, "topic", topicName);
        Append(builder, "absent", "1");
        Append(builder, "cluster", cluster.ClusterId);
        Append(builder, "kafka-cluster", cluster.KafkaClusterId ?? "unknown");
        Append(
            builder,
            "controller",
            cluster.ControllerBrokerId?.ToString(CultureInfo.InvariantCulture) ?? "unknown");
        Append(
            builder,
            "brokers",
            string.Join(",", cluster.Brokers.Select(broker => broker.BrokerId).OrderBy(id => id)));
        return Hash(builder);
    }

    public static string FingerprintConfigurations(
        string topicName,
        IReadOnlyList<KafkaConfigurationEntry> entries,
        IEnumerable<string> keys)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(keys);

        var byName = entries.ToDictionary(entry => entry.Name, StringComparer.Ordinal);
        var builder = new StringBuilder(1024);
        Append(builder, "topic", topicName);

        foreach (var key in keys.Distinct(StringComparer.Ordinal).OrderBy(key => key, StringComparer.Ordinal))
        {
            Append(builder, "key", key);
            if (!byName.TryGetValue(key, out var entry))
            {
                Append(builder, "missing", "1");
                continue;
            }

            Append(builder, "value", entry.Value ?? "<null>");
            Append(builder, "sensitive", entry.IsSensitive ? "1" : "0");
            Append(builder, "readonly", entry.IsReadOnly ? "1" : "0");
            Append(builder, "source", entry.Source ?? "unknown");
        }

        return Hash(builder);
    }

    private static MutationIntentDescriptor BuildIntent(
        MutationOperationKind kind,
        string clusterId,
        string topicName,
        string canonicalIntent,
        IReadOnlyList<MutationPrecondition> preconditions,
        MutationRiskContext riskContext)
    {
        var action = MutationAuthorization.ExpectedAction(kind);
        return new MutationIntentDescriptor(
            kind,
            clusterId,
            canonicalIntent,
            new[] { ResourceKey(clusterId, topicName) },
            preconditions,
            AuthorizationTargets: new[]
            {
                new MutationAuthorizationTarget(
                    action,
                    clusterId,
                    topicName),
            },
            RiskContext: riskContext);
    }

    private static IReadOnlyDictionary<string, string?> NormalizeConfigurations(
        IReadOnlyDictionary<string, string?> configurations,
        bool allowDelete)
    {
        ArgumentNullException.ThrowIfNull(configurations);
        if (configurations.Count > 64)
        {
            throw new TopicMutationPolicyException(
                TopicMutationPlanningFailureCode.InvalidConfigurationValue,
                "Topic configuration mutation is limited to 64 entries.");
        }

        var normalized = new SortedDictionary<string, string?>(StringComparer.Ordinal);
        foreach (var pair in configurations)
        {
            var key = pair.Key?.Trim() ?? string.Empty;
            if (!AllowedConfigurationKeys.Contains(key) ||
                !string.Equals(pair.Key, key, StringComparison.Ordinal))
            {
                throw new TopicMutationPolicyException(
                    TopicMutationPlanningFailureCode.ConfigurationKeyUnsupported,
                    $"Topic configuration key '{key}' is not admitted.");
            }

            if (pair.Value is null)
            {
                if (!allowDelete)
                {
                    throw new TopicMutationPolicyException(
                        TopicMutationPlanningFailureCode.InvalidConfigurationValue,
                        $"Topic configuration '{key}' requires a value.");
                }

                normalized[key] = null;
                continue;
            }

            normalized[key] = NormalizeConfigurationValue(key, pair.Value);
        }

        return new ReadOnlyDictionary<string, string?>(normalized);
    }

    private static string NormalizeConfigurationValue(string key, string value)
    {
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Any(char.IsControl))
        {
            throw InvalidConfigValue(key);
        }

        return key switch
        {
            "cleanup.policy" => NormalizeCleanupPolicy(value),
            "retention.ms" or "retention.bytes" => NormalizeNonNegativeOrMinusOne(key, value),
            "segment.ms" or "segment.bytes" or "delete.retention.ms" =>
                NormalizePositiveInt64(key, value),
            "min.insync.replicas" or "max.message.bytes" =>
                NormalizePositiveInt32(key, value),
            "min.cleanable.dirty.ratio" => NormalizeRatio(key, value),
            _ => throw new TopicMutationPolicyException(
                TopicMutationPlanningFailureCode.ConfigurationKeyUnsupported,
                $"Topic configuration key '{key}' is not admitted."),
        };
    }

    private static string NormalizeCleanupPolicy(string value)
    {
        var values = value
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(item => item, StringComparer.Ordinal)
            .ToArray();

        if (values.Length == 1 && values[0] is "compact" or "delete")
        {
            return values[0];
        }

        if (values.Length == 2 &&
            values[0] == "compact" &&
            values[1] == "delete")
        {
            return "compact,delete";
        }

        throw InvalidConfigValue("cleanup.policy");
    }

    private static string NormalizeNonNegativeOrMinusOne(string key, string value)
    {
        if (!long.TryParse(
                value,
                NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed < -1)
        {
            throw InvalidConfigValue(key);
        }

        return parsed.ToString(CultureInfo.InvariantCulture);
    }

    private static string NormalizePositiveInt64(string key, string value)
    {
        if (!long.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed <= 0)
        {
            throw InvalidConfigValue(key);
        }

        return parsed.ToString(CultureInfo.InvariantCulture);
    }

    private static string NormalizePositiveInt32(string key, string value)
    {
        if (!int.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed <= 0)
        {
            throw InvalidConfigValue(key);
        }

        return parsed.ToString(CultureInfo.InvariantCulture);
    }

    private static string NormalizeRatio(string key, string value)
    {
        if (!decimal.TryParse(
                value,
                NumberStyles.AllowDecimalPoint,
                CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed < 0m ||
            parsed > 1m)
        {
            throw InvalidConfigValue(key);
        }

        return parsed.ToString("0.############################", CultureInfo.InvariantCulture);
    }

    private static TopicMutationPolicyException InvalidConfigValue(string key) =>
        new(
            TopicMutationPlanningFailureCode.InvalidConfigurationValue,
            $"Topic configuration '{key}' has an invalid value.");

    private static string RequireIdentifier(string value, string fieldName, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new TopicMutationPolicyException(
                TopicMutationPlanningFailureCode.InvalidInput,
                $"{fieldName} is invalid.");
        }

        return value;
    }

    private static bool IsAllowedTopicCharacter(char character) =>
        character is >= 'a' and <= 'z' or
        >= 'A' and <= 'Z' or
        >= '0' and <= '9' or
        '.' or '_' or '-';

    private static string Hash(StringBuilder builder) =>
        Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();

    private static void Append(StringBuilder builder, string key, string value) =>
        builder
            .Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}

public sealed class TopicMutationPolicyException : ArgumentException
{
    public TopicMutationPolicyException(
        TopicMutationPlanningFailureCode code,
        string message)
        : base(message)
    {
        Code = code;
    }

    public TopicMutationPlanningFailureCode Code { get; }
}
