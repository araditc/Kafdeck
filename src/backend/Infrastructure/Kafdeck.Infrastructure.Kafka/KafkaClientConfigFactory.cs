using Confluent.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;
using ConfigKafkaSecurityProtocol = Kafdeck.Infrastructure.Configuration.KafkaSecurityProtocol;
using ConfigSaslMechanism = Kafdeck.Infrastructure.Configuration.SaslMechanism;

namespace Kafdeck.Infrastructure.Kafka;

internal static class KafkaClientConfigFactory
{
    public static AdminClientConfig CreateAdminClient(
        ClusterProfile profile,
        SecretResolver secretResolver)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(secretResolver);

        var config = new AdminClientConfig
        {
            BootstrapServers = string.Join(",", profile.BootstrapServers),
            ClientId = $"kafdeck-{profile.Id}",
        };

        ApplySecurity(config, profile, secretResolver);
        return config;
    }

    public static ProducerConfig CreateRecordProducer(
        ClusterProfile profile,
        SecretResolver secretResolver)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(secretResolver);

        var config = new ProducerConfig
        {
            BootstrapServers = string.Join(",", profile.BootstrapServers),
            ClientId = $"kafdeck-record-produce-{profile.Id}",
            EnableIdempotence = true,
            Acks = Acks.All,
            AllowAutoCreateTopics = false,
            MessageTimeoutMs = 15_000,
            RequestTimeoutMs = 10_000,
        };

        ApplySecurity(config, profile, secretResolver);
        return config;
    }

    public static ConsumerConfig CreateRecordConsumer(
        ClusterProfile profile,
        SecretResolver secretResolver)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(secretResolver);

        var config = new ConsumerConfig
        {
            BootstrapServers = string.Join(",", profile.BootstrapServers),
            ClientId = $"kafdeck-record-{profile.Id}",
            GroupId = $"kafdeck-record-read-{profile.Id}",
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            EnablePartitionEof = true,
            AllowAutoCreateTopics = false,
            AutoOffsetReset = AutoOffsetReset.Error,
            FetchMaxBytes = checked((int)RecordOperationBudget.HardMaxRawBytes),
            MaxPartitionFetchBytes = checked((int)RecordOperationBudget.HardMaxRawBytes),
        };

        ApplySecurity(config, profile, secretResolver);
        return config;
    }

    private static void ApplySecurity(
        ClientConfig config,
        ClusterProfile profile,
        SecretResolver secretResolver)
    {
        config.SecurityProtocol = MapSecurityProtocol(profile.SecurityProtocol);

        if (profile.SecurityProtocol is ConfigKafkaSecurityProtocol.Ssl or ConfigKafkaSecurityProtocol.SaslSsl)
        {
            config.EnableSslCertificateVerification = true;
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;

            if (profile.Tls?.CaCertificate is not null)
            {
                config.SslCaPem = secretResolver.Resolve(profile.Tls.CaCertificate).Reveal();
            }

            if (profile.Tls?.ClientCertificate is not null)
            {
                config.SslCertificatePem = secretResolver.Resolve(profile.Tls.ClientCertificate).Reveal();
            }

            if (profile.Tls?.ClientKey is not null)
            {
                config.SslKeyPem = secretResolver.Resolve(profile.Tls.ClientKey).Reveal();
            }
        }

        if (profile.SecurityProtocol is ConfigKafkaSecurityProtocol.SaslPlaintext or ConfigKafkaSecurityProtocol.SaslSsl)
        {
            var sasl = profile.Sasl
                ?? throw new KafdeckConfigurationException("SASL profile is required for the selected security protocol.");

            config.SaslMechanism = MapSaslMechanism(sasl.Mechanism);
            config.SaslUsername = secretResolver.Resolve(sasl.Username).Reveal();
            config.SaslPassword = secretResolver.Resolve(sasl.Password).Reveal();
        }
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
