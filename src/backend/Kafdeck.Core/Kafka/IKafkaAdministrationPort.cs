namespace Kafdeck.Core.Kafka;

public interface IKafkaAdministrationPort
{
    Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);

    Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);

    Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
        string clusterId,
        string topicName,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);

    Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
        string clusterId,
        string topicName,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);

    Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
        string clusterId,
        int brokerId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);

    Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);
}
