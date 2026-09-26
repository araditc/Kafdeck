using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed record ClusterTransferEndpoint(
    string ClusterId,
    string ProfileVersion,
    string KafkaClusterId);

public sealed record ClusterTransferMappingRequest(
    string SourceTopic,
    int SourcePartition,
    string DestinationTopic,
    int DestinationPartition,
    long StartInclusive,
    long EndExclusive);

public sealed record ClusterTransferPlanningRequest(
    string SourceClusterId,
    string SourceProfileVersion,
    string DestinationClusterId,
    string DestinationProfileVersion,
    IReadOnlyList<ClusterTransferMappingRequest> Mappings,
    ClusterTransferBudget Budget);

public sealed record ClusterTransferMapping(
    string SourceTopic,
    int SourcePartition,
    string DestinationTopic,
    int DestinationPartition,
    long StartInclusive,
    long EndExclusive,
    string SourceTopicFingerprint,
    string DestinationTopicFingerprint);

public sealed record ClusterTransferDataPolicy(
    string PolicyId,
    int PolicyVersion,
    string Fingerprint);

public sealed record ClusterTransferPlan(
    ClusterTransferEndpoint Source,
    ClusterTransferEndpoint Destination,
    IReadOnlyList<ClusterTransferMapping> Mappings,
    ClusterTransferBudget Budget,
    ClusterTransferDataPolicy DataPolicy,
    string PlanFingerprint);

public sealed record ClusterTransferBudget
{
    public const int DefaultMaxBatchRecords = 100;
    public const int HardMaxBatchRecords = 1_000;
    public const long DefaultMaxBatchBytes = 1L * 1024 * 1024;
    public const long HardMaxBatchBytes = 4L * 1024 * 1024;
    public const long DefaultMaxTotalRecords = 100_000;
    public const long HardMaxTotalRecords = 1_000_000;
    public const long DefaultMaxTotalBytes = 100L * 1024 * 1024;
    public const long HardMaxTotalBytes = 1L * 1024 * 1024 * 1024;
    public static readonly TimeSpan DefaultMaxDuration = TimeSpan.FromMinutes(30);
    public static readonly TimeSpan HardMaxDuration = TimeSpan.FromHours(24);
    public const int DefaultMaxRecordsPerSecond = 1_000;
    public const int HardMaxRecordsPerSecond = 10_000;
    public const long DefaultMaxBytesPerSecond = 1L * 1024 * 1024;
    public const long HardMaxBytesPerSecond = 10L * 1024 * 1024;

    public ClusterTransferBudget(
        int maxBatchRecords = DefaultMaxBatchRecords,
        long maxBatchBytes = DefaultMaxBatchBytes,
        long maxTotalRecords = DefaultMaxTotalRecords,
        long maxTotalBytes = DefaultMaxTotalBytes,
        TimeSpan? maxDuration = null,
        int maxRecordsPerSecond = DefaultMaxRecordsPerSecond,
        long maxBytesPerSecond = DefaultMaxBytesPerSecond)
    {
        if (maxBatchRecords is < 1 or > HardMaxBatchRecords)
            throw new ArgumentOutOfRangeException(nameof(maxBatchRecords));
        if (maxBatchBytes is < 1 or > HardMaxBatchBytes)
            throw new ArgumentOutOfRangeException(nameof(maxBatchBytes));
        if (maxTotalRecords is < 1 or > HardMaxTotalRecords)
            throw new ArgumentOutOfRangeException(nameof(maxTotalRecords));
        if (maxTotalBytes is < 1 or > HardMaxTotalBytes)
            throw new ArgumentOutOfRangeException(nameof(maxTotalBytes));
        if (maxRecordsPerSecond is < 1 or > HardMaxRecordsPerSecond)
            throw new ArgumentOutOfRangeException(nameof(maxRecordsPerSecond));
        if (maxBytesPerSecond is < 1 or > HardMaxBytesPerSecond)
            throw new ArgumentOutOfRangeException(nameof(maxBytesPerSecond));

        var duration = maxDuration ?? DefaultMaxDuration;
        if (duration <= TimeSpan.Zero || duration > HardMaxDuration)
            throw new ArgumentOutOfRangeException(nameof(maxDuration));

        MaxBatchRecords = maxBatchRecords;
        MaxBatchBytes = maxBatchBytes;
        MaxTotalRecords = maxTotalRecords;
        MaxTotalBytes = maxTotalBytes;
        MaxDuration = duration;
        MaxRecordsPerSecond = maxRecordsPerSecond;
        MaxBytesPerSecond = maxBytesPerSecond;
    }

    public int MaxBatchRecords { get; }
    public long MaxBatchBytes { get; }
    public long MaxTotalRecords { get; }
    public long MaxTotalBytes { get; }
    public TimeSpan MaxDuration { get; }
    public int MaxRecordsPerSecond { get; }
    public long MaxBytesPerSecond { get; }
}

public enum ClusterTransferPlanningFailureCode
{
    InvalidInput = 1,
    IdentityUnavailable = 2,
    SamePhysicalCluster = 3,
    TopicUnavailable = 4,
    InternalTopicUnsupported = 5,
    PartitionUnavailable = 6,
    MaskingPolicyUnsupported = 7,
    ProviderUnauthorized = 8,
    ProviderUnsupported = 9,
    ProviderUnavailable = 10,
    ObservationFailed = 11,
}

public sealed record ClusterTransferPlanningFailure(
    ClusterTransferPlanningFailureCode Code,
    string SafeMessage);

public sealed record ClusterTransferPlanResult(
    ClusterTransferPlan? Plan,
    MutationIntentDescriptor? Intent,
    MutationRiskDecision? Risk,
    ClusterTransferPlanningFailure? Failure)
{
    public bool IsSuccess => Plan is not null && Intent is not null && Risk is not null && Failure is null;

    public static ClusterTransferPlanResult Success(
        ClusterTransferPlan plan,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk) =>
        new(plan, intent, risk, null);

    public static ClusterTransferPlanResult Failed(
        ClusterTransferPlanningFailureCode code,
        string message) =>
        new(null, null, null, new ClusterTransferPlanningFailure(code, message));
}

public static class ClusterTransferPolicy
{
    public const int MaxMappings = 64;
    public const int CriticalMappingThreshold = 25;
    public const long CriticalApprovedByteThreshold = 100L * 1024 * 1024;

    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        WriteIndented = false,
    };

    public static ClusterTransferMappingRequest Normalize(
        ClusterTransferMappingRequest mapping)
    {
        ArgumentNullException.ThrowIfNull(mapping);
        var sourceTopic = RequireIdentifier(mapping.SourceTopic, "Source topic", 249);
        var destinationTopic = RequireIdentifier(mapping.DestinationTopic, "Destination topic", 249);
        if (mapping.SourcePartition < 0 || mapping.DestinationPartition < 0)
            throw new ArgumentOutOfRangeException(nameof(mapping), "Transfer partitions must be non-negative.");
        if (mapping.StartInclusive < 0 || mapping.EndExclusive <= mapping.StartInclusive)
            throw new ArgumentOutOfRangeException(nameof(mapping), "Transfer range must be finite [start,end) with 0 <= start < end.");

        return mapping with
        {
            SourceTopic = sourceTopic,
            DestinationTopic = destinationTopic,
        };
    }

    public static string FingerprintTopic(TopicMetadata topic)
    {
        ArgumentNullException.ThrowIfNull(topic);
        var builder = new StringBuilder();
        Append(builder, "name", topic.Name);
        Append(builder, "internal", topic.IsInternal ? "1" : "0");
        foreach (var partition in topic.Partitions.OrderBy(item => item.PartitionId))
        {
            Append(builder, "partition", partition.PartitionId.ToString(CultureInfo.InvariantCulture));
            Append(builder, "leader", partition.LeaderBrokerId?.ToString(CultureInfo.InvariantCulture) ?? "<null>");
            foreach (var broker in partition.ReplicaBrokerIds)
                Append(builder, "replica", broker.ToString(CultureInfo.InvariantCulture));
            foreach (var broker in partition.InSyncReplicaBrokerIds)
                Append(builder, "isr", broker.ToString(CultureInfo.InvariantCulture));
        }

        return Sha256(builder.ToString());
    }

    public static ClusterTransferDataPolicy RequireBytePreservingPolicy(
        IRecordMaskingPolicySnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot is not CompiledRecordMaskingPolicy compiled ||
            compiled.MaskKey ||
            compiled.StructuredRules.Count != 0 ||
            compiled.HeaderRules.Count != 0)
        {
            throw new MutationStateException(
                "Byte-preserving transfer is unavailable while the current masking policy changes key, value or header bytes.");
        }

        var builder = new StringBuilder();
        Append(builder, "policy-id", compiled.PolicyId);
        Append(builder, "version", compiled.Version.ToString(CultureInfo.InvariantCulture));
        Append(builder, "mask-key", "0");
        Append(builder, "structured-rule-count", "0");
        Append(builder, "header-rule-count", "0");
        return new ClusterTransferDataPolicy(
            compiled.PolicyId,
            compiled.Version,
            Sha256(builder.ToString()));
    }

    public static string PlanFingerprint(
        ClusterTransferEndpoint source,
        ClusterTransferEndpoint destination,
        IReadOnlyList<ClusterTransferMapping> mappings,
        ClusterTransferBudget budget,
        ClusterTransferDataPolicy policy)
    {
        var canonical = JsonSerializer.Serialize(
            new
            {
                source,
                destination,
                mappings,
                budget = new
                {
                    budget.MaxBatchRecords,
                    budget.MaxBatchBytes,
                    budget.MaxTotalRecords,
                    budget.MaxTotalBytes,
                    maxDurationTicks = budget.MaxDuration.Ticks,
                    budget.MaxRecordsPerSecond,
                    budget.MaxBytesPerSecond,
                },
                policy,
            },
            CanonicalJson);
        return Sha256(canonical);
    }

    public static MutationIntentDescriptor BuildIntent(ClusterTransferPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var sourceCluster = RequireIdentifier(plan.Source.ClusterId, "Source cluster", 256);
        var destinationCluster = RequireIdentifier(plan.Destination.ClusterId, "Destination cluster", 256);
        if (string.Equals(sourceCluster, destinationCluster, StringComparison.Ordinal))
            throw new MutationStateException("Transfer source and destination cluster profiles must be distinct.");
        if (plan.Mappings.Count is < 1 or > MaxMappings)
            throw new MutationStateException("Transfer mapping count is outside the admitted bound.");

        var sourcePhysicalCluster = RequireIdentifier(
            plan.Source.KafkaClusterId,
            "Source physical Kafka cluster",
            256);
        var destinationPhysicalCluster = RequireIdentifier(
            plan.Destination.KafkaClusterId,
            "Destination physical Kafka cluster",
            256);
        if (string.Equals(
                sourcePhysicalCluster,
                destinationPhysicalCluster,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Transfer source and destination physical Kafka clusters must be distinct.");
        }

        // Conflict identity is physical, not profile-alias based. Authorization
        // targets below intentionally continue to use deployment profile IDs.
        var transferPair = FleetConflictKeyCodec.TransferPair(
            sourcePhysicalCluster,
            destinationPhysicalCluster);
        var resources = new HashSet<string>(StringComparer.Ordinal)
        {
            transferPair,
        };
        var authorization = new List<MutationAuthorizationTarget>
        {
            new(AuthorizationAction.ClusterTransferPlan, sourceCluster, transferPair),
            new(AuthorizationAction.ClusterTransferExecute, sourceCluster, transferPair),
            new(AuthorizationAction.ClusterRead, sourceCluster, sourceCluster),
            new(AuthorizationAction.ClusterRead, destinationCluster, destinationCluster),
        };
        var preconditions = new List<MutationPrecondition>
        {
            new("source.kafka-cluster-id", Sha256(plan.Source.KafkaClusterId)),
            new("destination.kafka-cluster-id", Sha256(plan.Destination.KafkaClusterId)),
            new("record.data-policy", plan.DataPolicy.Fingerprint),
            new("transfer.plan", plan.PlanFingerprint),
        };

        for (var index = 0; index < plan.Mappings.Count; index++)
        {
            var mapping = plan.Mappings[index];
            resources.Add(FleetConflictKeyCodec.TopicPartition(
                sourcePhysicalCluster,
                mapping.SourceTopic,
                mapping.SourcePartition));
            resources.Add(FleetConflictKeyCodec.TopicPartition(
                destinationPhysicalCluster,
                mapping.DestinationTopic,
                mapping.DestinationPartition));

            authorization.Add(new(
                AuthorizationAction.TopicRead,
                sourceCluster,
                mapping.SourceTopic));
            authorization.Add(new(
                AuthorizationAction.RecordRead,
                sourceCluster,
                mapping.SourceTopic));
            authorization.Add(new(
                AuthorizationAction.RecordExport,
                sourceCluster,
                mapping.SourceTopic));
            authorization.Add(new(
                AuthorizationAction.TopicRead,
                destinationCluster,
                mapping.DestinationTopic));
            authorization.Add(new(
                AuthorizationAction.RecordProduce,
                destinationCluster,
                mapping.DestinationTopic));

            preconditions.Add(new(
                $"source.topic.{index:D2}",
                mapping.SourceTopicFingerprint));
            preconditions.Add(new(
                $"destination.topic.{index:D2}",
                mapping.DestinationTopicFingerprint));
        }

        var canonical = JsonSerializer.Serialize(plan, CanonicalJson);
        return new MutationIntentDescriptor(
            MutationOperationKind.ClusterTransfer,
            sourceCluster,
            canonical,
            resources.OrderBy(value => value, StringComparer.Ordinal).ToArray(),
            preconditions.OrderBy(item => item.Key, StringComparer.Ordinal).ToArray(),
            AuthorizationTargets: authorization
                .Distinct()
                .OrderBy(item => item.Action)
                .ThenBy(item => item.ClusterId, StringComparer.Ordinal)
                .ThenBy(item => item.ResourceName, StringComparer.Ordinal)
                .ToArray());
    }

    public static MutationRiskDecision ClassifyRisk(ClusterTransferPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var critical =
            plan.Mappings.Count > CriticalMappingThreshold ||
            plan.Budget.MaxTotalBytes > CriticalApprovedByteThreshold;

        return critical
            ? new MutationRiskDecision(
                MutationRiskClass.Critical,
                Array.AsReadOnly(new[] { "cluster_transfer_blast_radius" }),
                MutationConfirmationMode.TypedTarget,
                RequiresIndependentApproval: true)
            : new MutationRiskDecision(
                MutationRiskClass.High,
                Array.AsReadOnly(new[] { "cluster_transfer" }),
                MutationConfirmationMode.TypedTarget,
                RequiresIndependentApproval: false);
    }

    public static ClusterTransferPlan DeserializePlan(string canonicalIntent)
    {
        try
        {
            var plan = JsonSerializer.Deserialize<ClusterTransferPlan>(
                canonicalIntent,
                CanonicalJson) ??
                throw new MutationStateException("Cluster transfer canonical intent is invalid.");
            _ = BuildIntent(plan);
            return plan;
        }
        catch (JsonException exception)
        {
            throw new MutationStateException(
                $"Cluster transfer canonical intent is invalid: {exception.GetType().Name}.");
        }
    }

    public static string RequireIdentifier(string value, string field, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException($"{field} is invalid or exceeds the admitted bound.", field);
        }

        return value;
    }

    public static string Sha256(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private static void Append(StringBuilder builder, string key, string value) =>
        builder.Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}
