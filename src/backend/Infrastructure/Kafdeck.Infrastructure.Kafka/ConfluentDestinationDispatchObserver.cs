using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

/// <summary>Official typed admin observations only; one finite deadline includes metadata and both offsets.</summary>
public sealed class ConfluentDestinationDispatchObserver(IReadOnlyList<ClusterProfile> profiles, SecretResolver secrets) : IDestinationDispatchObserver, IDisposable
{
    private readonly KafkaAdminClientRegistry _clients = new(profiles, secrets);
    public async Task<DestinationBoundaryObservation> ObserveAsync(string clusterId, string topicName, int partition,
        long workerGeneration, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(clusterId) || string.IsNullOrWhiteSpace(topicName) || partition < 0 || workerGeneration <= 0)
            return new(null, "destination_observation_invalid");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            var client = _clients.GetClient(clusterId);
            var cluster = await client.DescribeClusterAsync(new DescribeClusterOptions { RequestTimeout = TimeSpan.FromSeconds(10) })
                .WaitAsync(deadline.Token).ConfigureAwait(false);
            var topics = await client.DescribeTopicsAsync(TopicCollection.OfTopicNames([topicName]),
                new DescribeTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }).WaitAsync(deadline.Token).ConfigureAwait(false);
            var topic = topics.TopicDescriptions.Single();
            if (topic.Error.IsError || topic.IsInternal || topic.TopicId is null || string.IsNullOrWhiteSpace(cluster.ClusterId) ||
                !topic.Partitions.Any(p => p.Partition == partition)) return new(null, "destination_identity_unavailable");
            var topicId = topic.TopicId.MostSignificantBits.Value.ToString("x16", CultureInfo.InvariantCulture) +
                topic.TopicId.LeastSignificantBits.Value.ToString("x16", CultureInfo.InvariantCulture);
            var target = new TopicPartition(topicName, new Partition(partition));
            async Task<long?> OffsetAsync(OffsetSpec spec)
            {
                var offsets = await client.ListOffsetsAsync([new TopicPartitionOffsetSpec { TopicPartition = target, OffsetSpec = spec }],
                    new ListOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }).WaitAsync(deadline.Token).ConfigureAwait(false);
                var item = offsets.ResultInfos.Single().TopicPartitionOffsetError;
                return item.Error.IsError || item.Offset.Value < 0 ? null : item.Offset.Value;
            }
            var low = await OffsetAsync(OffsetSpec.Earliest()).ConfigureAwait(false);
            var high = await OffsetAsync(OffsetSpec.Latest()).ConfigureAwait(false);
            if (low is null || high is null) return new(null, "destination_offsets_unavailable");
            var boundary = new DestinationDispatchBoundary(clusterId, cluster.ClusterId, topicName, topicId, partition,
                low.Value, high.Value, workerGeneration, DateTimeOffset.UtcNow).Validate();
            return new(boundary, "destination_boundary_observed");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch { return new(null, "destination_boundary_unavailable"); }
    }
    public void Dispose() => _clients.Dispose();
}
