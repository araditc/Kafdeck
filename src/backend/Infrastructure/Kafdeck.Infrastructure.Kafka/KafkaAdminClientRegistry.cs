using System.Collections.Concurrent;
using Confluent.Kafka;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Kafka;

internal sealed class KafkaAdminClientRegistry : IDisposable
{
    private readonly IReadOnlyDictionary<string, ClusterProfile> _profiles;
    private readonly SecretResolver _secretResolver;
    private readonly ConcurrentDictionary<string, Lazy<IAdminClient>> _clients =
        new(StringComparer.Ordinal);

    public KafkaAdminClientRegistry(
        IReadOnlyList<ClusterProfile> profiles,
        SecretResolver secretResolver)
    {
        ArgumentNullException.ThrowIfNull(profiles);
        _secretResolver = secretResolver ?? throw new ArgumentNullException(nameof(secretResolver));

        _profiles = profiles.ToDictionary(profile => profile.Id, StringComparer.Ordinal);
    }

    public bool ContainsCluster(string clusterId) => _profiles.ContainsKey(clusterId);

    public IAdminClient GetClient(string clusterId)
    {
        if (!_profiles.TryGetValue(clusterId, out var profile))
        {
            throw new KeyNotFoundException("Kafka cluster profile is not configured.");
        }

        return _clients.GetOrAdd(
            clusterId,
            _ => new Lazy<IAdminClient>(
                () => BuildClient(profile),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;
    }

    public void Dispose()
    {
        foreach (var client in _clients.Values)
        {
            if (client.IsValueCreated)
            {
                client.Value.Dispose();
            }
        }

        _clients.Clear();
    }

    private IAdminClient BuildClient(ClusterProfile profile)
    {
        var config = KafkaClientConfigFactory.CreateAdminClient(profile, _secretResolver);
        return new AdminClientBuilder(config).Build();
    }
}
