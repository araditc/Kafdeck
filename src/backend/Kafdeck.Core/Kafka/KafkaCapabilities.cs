namespace Kafdeck.Core.Kafka;

public enum KafkaCapabilityKind
{
    ClusterMetadata = 1,
    BrokerMetadata = 2,
    ControllerMetadata = 3,
    TopicListing = 4,
    TopicMetadata = 5,
    TopicConfiguration = 6,
    BrokerConfiguration = 7,
}

public enum KafkaCapabilityState
{
    Available = 1,
    Unauthorized = 2,
    Unsupported = 3,
    Unavailable = 4,
    Unknown = 5,
}

public sealed record KafkaCapabilityStatus(
    KafkaCapabilityKind Capability,
    KafkaCapabilityState State,
    string? Reason = null);

public sealed record KafkaCapabilities(IReadOnlyList<KafkaCapabilityStatus> Items);
