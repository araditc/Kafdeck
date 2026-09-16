namespace Kafdeck.Core.Kafka;

public sealed record ClusterMetadata(
    string ClusterId,
    string? KafkaClusterId,
    int? ControllerBrokerId,
    IReadOnlyList<BrokerMetadata> Brokers);

public sealed record BrokerMetadata(
    int BrokerId,
    string Host,
    int Port,
    string? Rack,
    bool IsController);

public sealed record TopicSummary(
    string Name,
    int PartitionCount,
    bool IsInternal);

public sealed record TopicMetadata(
    string Name,
    bool IsInternal,
    IReadOnlyList<PartitionMetadata> Partitions);

public sealed record PartitionMetadata(
    int PartitionId,
    int? LeaderBrokerId,
    IReadOnlyList<int> ReplicaBrokerIds,
    IReadOnlyList<int> InSyncReplicaBrokerIds);

public sealed record KafkaConfigurationEntry(
    string Name,
    string? Value,
    bool IsSensitive,
    bool IsReadOnly,
    string? Source);
