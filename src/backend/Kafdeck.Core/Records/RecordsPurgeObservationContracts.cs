using Kafdeck.Core.Kafka;

namespace Kafdeck.Core.Records;

public sealed record RecordsPurgeObservationTarget(
    string TopicName,
    int Partition,
    DateTimeOffset? ResolveTimestampUtc = null);

public sealed record RecordsPurgePartitionObservation(
    string TopicName,
    int Partition,
    long LowWatermark,
    long HighWatermark,
    long? TimestampResolvedOffset);

public sealed record RecordsPurgeObservation(
    IReadOnlyList<RecordsPurgePartitionObservation> Partitions);

public interface IRecordsPurgeObservationPort
{
    Task<KafkaResult<RecordsPurgeObservation>> ObserveAsync(
        string clusterId,
        IReadOnlyList<RecordsPurgeObservationTarget> targets,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);
}
