using System.Collections.Concurrent;
using Confluent.Kafka;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Kafka;

internal sealed class KafkaProducerRegistry : IDisposable
{
    private readonly IReadOnlyDictionary<string, ClusterProfile> _profiles;
    private readonly SecretResolver _secretResolver;
    private readonly ConcurrentDictionary<string, Lazy<IProducer<byte[], byte[]>>> _producers =
        new(StringComparer.Ordinal);

    public KafkaProducerRegistry(
        IReadOnlyList<ClusterProfile> profiles,
        SecretResolver secretResolver)
    {
        ArgumentNullException.ThrowIfNull(profiles);
        _secretResolver = secretResolver ?? throw new ArgumentNullException(nameof(secretResolver));
        _profiles = profiles.ToDictionary(profile => profile.Id, StringComparer.Ordinal);
    }

    public bool ContainsCluster(string clusterId) => _profiles.ContainsKey(clusterId);

    public IProducer<byte[], byte[]> GetProducer(string clusterId)
    {
        if (!_profiles.TryGetValue(clusterId, out var profile))
            throw new KeyNotFoundException("Kafka cluster profile is not configured.");

        return _producers.GetOrAdd(
            clusterId,
            _ => new Lazy<IProducer<byte[], byte[]>>(
                () => new ProducerBuilder<byte[], byte[]>(
                        KafkaClientConfigFactory.CreateRecordProducer(profile, _secretResolver))
                    .Build(),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;
    }

    public void Dispose()
    {
        foreach (var producer in _producers.Values)
        {
            if (!producer.IsValueCreated)
                continue;

            try
            {
                producer.Value.Flush(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // Disposal cannot create a new durable mutation outcome.
            }

            producer.Value.Dispose();
        }

        _producers.Clear();
    }
}
