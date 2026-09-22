using Kafdeck.Core.Kafka;

namespace Kafdeck.Core.Records;

public sealed record RecordsPurgeObservationTarget(
    string Topic,
    int Partition,
    DateTimeOffset? ResolveTimestampUtc = null);

public sealed record RecordsPurgePartitionObservation(
    string Topic,
    int Partition,
    long LowWatermark,
    long HighWatermark,
    long? TimestampResolvedOffset);

public interface IRecordsPurgeObservationPort
{
    Task<KafkaResult<IReadOnlyList<RecordsPurgePartitionObservation>>> ObserveAsync(
        string clusterId,
        IReadOnlyList<RecordsPurgeObservationTarget> targets,
        KafkaOperationContext operation,
        CancellationToken cancellationToken);
}
