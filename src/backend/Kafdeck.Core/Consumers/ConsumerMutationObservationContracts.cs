using Kafdeck.Core.Kafka;

namespace Kafdeck.Core.Consumers;

public sealed record ConsumerMutationObservationTarget(
    string Topic,
    int Partition,
    DateTimeOffset? ResolveTimestampUtc = null,
    bool RequireStableOffset = true);

public sealed record ConsumerMutationPartitionObservation(
    string Topic,
    int Partition,
    long? CommittedOffset,
    long LowWatermark,
    long HighWatermark,
    long? TimestampResolvedOffset);

public sealed record ConsumerMutationObservation(
    string GroupId,
    bool Exists,
    ConsumerGroupState State,
    IReadOnlyList<ConsumerMemberProjection> Members,
    IReadOnlyList<ConsumerMutationPartitionObservation> Partitions);

public interface IConsumerMutationObservationPort
{
    Task<KafkaResult<ConsumerMutationObservation>> ObserveAsync(
        string clusterId,
        string groupId,
        IReadOnlyList<ConsumerMutationObservationTarget> targets,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);
}
