using System.Collections.Concurrent;
using System.Globalization;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Api;

internal sealed record BrokerAgentHealth(bool Healthy, int BrokerCount, int Topics, int AffectedPartitions,
    IReadOnlyList<string> BlockedPartitions);

internal static class BrokerAgentHealthCheck
{
    internal static async Task<BrokerAgentHealth> CheckAsync(IKafkaAdministrationPort kafka,
        BrokerHostBinding binding, int expectedBrokers, bool beforeRestart, CancellationToken cancellation)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        timeout.CancelAfter(TimeSpan.FromSeconds(18));
        var operation = new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(18));
        var cluster = await kafka.GetClusterMetadataAsync(binding.ClusterId, operation, timeout.Token);
        var brokerIds = cluster.Value?.Brokers.Select(item => item.BrokerId).ToHashSet();
        if (!cluster.IsSuccess || cluster.Value?.KafkaClusterId != binding.KafkaClusterId ||
            cluster.Value.ControllerBrokerId is null || brokerIds is null || brokerIds.Count != expectedBrokers ||
            !brokerIds.Contains(binding.BrokerId))
            return new(false, brokerIds?.Count ?? 0, 0, 0, []);

        var topics = await kafka.ListTopicsAsync(binding.ClusterId, operation, timeout.Token);
        if (!topics.IsSuccess || topics.Value is not { Count: <= 256 } visible ||
            visible.Sum(topic => (long)topic.PartitionCount) > 10000)
            return new(false, brokerIds.Count, topics.Value?.Count ?? 0, 0, []);
        var unavailable = 0;
        var affected = 0;
        var blocked = new ConcurrentQueue<string>();
        await Parallel.ForEachAsync(visible, new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = timeout.Token },
            async (topic, token) =>
            {
                var metadata = await kafka.GetTopicMetadataAsync(binding.ClusterId, topic.Name, operation, token);
                if (!metadata.IsSuccess || metadata.Value is null || metadata.Value.Partitions.Count != topic.PartitionCount)
                {
                    Interlocked.Increment(ref unavailable);
                    return;
                }
                var replicas = metadata.Value.Partitions.Where(partition => partition.ReplicaBrokerIds.Contains(binding.BrokerId)).ToArray();
                Interlocked.Add(ref affected, replicas.Length);
                var minimum = 1;
                if (beforeRestart && replicas.Length > 0)
                {
                    var configuration = await kafka.GetTopicConfigurationAsync(binding.ClusterId, topic.Name, operation, token);
                    var value = configuration.Value?.SingleOrDefault(entry => entry.Name == "min.insync.replicas")?.Value;
                    if (!configuration.IsSuccess || !int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out minimum) || minimum < 1)
                    {
                        Interlocked.Increment(ref unavailable);
                        return;
                    }
                }
                foreach (var partition in metadata.Value.Partitions)
                {
                    if (!PartitionSafe(partition, brokerIds, binding.BrokerId, beforeRestart, minimum))
                        blocked.Enqueue(topic.Name + ":" + partition.PartitionId.ToString(CultureInfo.InvariantCulture));
                }
            });
        return new(unavailable == 0 && blocked.IsEmpty, brokerIds.Count, visible.Count, affected,
            blocked.Order(StringComparer.Ordinal).Take(20).ToArray());
    }

    internal static bool PartitionSafe(PartitionMetadata partition, IReadOnlySet<int> brokers, int broker, bool beforeRestart, int minimum)
    {
        var replicas = partition.ReplicaBrokerIds.ToHashSet();
        var isr = partition.InSyncReplicaBrokerIds.ToHashSet();
        return partition.LeaderBrokerId is { } leader && isr.Contains(leader) &&
            isr.Count > 0 && replicas.SetEquals(isr) && isr.IsSubsetOf(brokers) &&
            (!beforeRestart || !replicas.Contains(broker) || isr.Count - 1 >= minimum);
    }
}
