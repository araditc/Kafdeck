using System.Collections.Concurrent;
using Confluent.Kafka;
using Kafdeck.Infrastructure.Configuration;
using ConfigKafkaSecurityProtocol = Kafdeck.Infrastructure.Configuration.KafkaSecurityProtocol;
using ConfigSaslMechanism = Kafdeck.Infrastructure.Configuration.SaslMechanism;

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
        var config = new AdminClientConfig
        {
            BootstrapServers = string.Join(",", profile.BootstrapServers),
            ClientId = $"kafdeck-{profile.Id}",
            SecurityProtocol = MapSecurityProtocol(profile.SecurityProtocol),
        };

        if (profile.SecurityProtocol is ConfigKafkaSecurityProtocol.Ssl or ConfigKafkaSecurityProtocol.SaslSsl)
        {
            config.EnableSslCertificateVerification = true;
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;

            if (profile.Tls?.CaCertificate is not null)
            {
                config.SslCaPem = _secretResolver.Resolve(profile.Tls.CaCertificate).Reveal();
            }

            if (profile.Tls?.ClientCertificate is not null)
            {
                config.SslCertificatePem = _secretResolver.Resolve(profile.Tls.ClientCertificate).Reveal();
            }

            if (profile.Tls?.ClientKey is not null)
            {
                config.SslKeyPem = _secretResolver.Resolve(profile.Tls.ClientKey).Reveal();
            }
        }

        if (profile.SecurityProtocol is ConfigKafkaSecurityProtocol.SaslPlaintext or ConfigKafkaSecurityProtocol.SaslSsl)
        {
            var sasl = profile.Sasl
                ?? throw new KafdeckConfigurationException("SASL profile is required for the selected security protocol.");

            config.SaslMechanism = MapSaslMechanism(sasl.Mechanism);
            config.SaslUsername = _secretResolver.Resolve(sasl.Username).Reveal();
            config.SaslPassword = _secretResolver.Resolve(sasl.Password).Reveal();
        }

        return new AdminClientBuilder(config).Build();
    }

    private static SecurityProtocol MapSecurityProtocol(ConfigKafkaSecurityProtocol value) => value switch
    {
        ConfigKafkaSecurityProtocol.Plaintext => SecurityProtocol.Plaintext,
        ConfigKafkaSecurityProtocol.Ssl => SecurityProtocol.Ssl,
        ConfigKafkaSecurityProtocol.SaslPlaintext => SecurityProtocol.SaslPlaintext,
        ConfigKafkaSecurityProtocol.SaslSsl => SecurityProtocol.SaslSsl,
        _ => throw new KafdeckConfigurationException("Unsupported Kafka security protocol."),
    };

    private static Confluent.Kafka.SaslMechanism MapSaslMechanism(ConfigSaslMechanism value) => value switch
    {
        ConfigSaslMechanism.Plain => Confluent.Kafka.SaslMechanism.Plain,
        ConfigSaslMechanism.ScramSha256 => Confluent.Kafka.SaslMechanism.ScramSha256,
        ConfigSaslMechanism.ScramSha512 => Confluent.Kafka.SaslMechanism.ScramSha512,
        _ => throw new KafdeckConfigurationException("Unsupported Kafka SASL mechanism."),
    };
}
