using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaAdapterIntegrationTests
{
    [Fact]
    public async Task Accepted_connection_modes_support_read_only_administration()
    {
        if (!string.Equals(
                Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"),
                "1",
                StringComparison.Ordinal))
        {
            return;
        }

        var secretsDirectory = Environment.GetEnvironmentVariable("KAFDECK_TEST_SECRETS_DIR");
        Assert.False(string.IsNullOrWhiteSpace(secretsDirectory));
        Assert.True(Path.IsPathFullyQualified(secretsDirectory));

        var ca = FileReference(secretsDirectory, "ca.crt");
        var clientCertificate = FileReference(secretsDirectory, "client.crt");
        var clientKey = FileReference(secretsDirectory, "client.key");
        var plainUsername = FileReference(secretsDirectory, "plain.username");
        var plainPassword = FileReference(secretsDirectory, "plain.password");
        var scram256Username = FileReference(secretsDirectory, "scram256.username");
        var scram256Password = FileReference(secretsDirectory, "scram256.password");
        var scram512Username = FileReference(secretsDirectory, "scram512.username");
        var scram512Password = FileReference(secretsDirectory, "scram512.password");

        var serverTls = new TlsProfile(true, ca, null, null);
        var mutualTls = new TlsProfile(true, ca, clientCertificate, clientKey);

        var profiles = new[]
        {
            new ClusterProfile(
                "plaintext",
                ["localhost:9092"],
                KafkaSecurityProtocol.Plaintext,
                null,
                null),
            new ClusterProfile(
                "mtls",
                ["localhost:9093"],
                KafkaSecurityProtocol.Ssl,
                mutualTls,
                null),
            new ClusterProfile(
                "sasl-plaintext-plain",
                ["localhost:9094"],
                KafkaSecurityProtocol.SaslPlaintext,
                null,
                new SaslProfile(SaslMechanism.Plain, plainUsername, plainPassword)),
            new ClusterProfile(
                "sasl-plaintext-scram256",
                ["localhost:9094"],
                KafkaSecurityProtocol.SaslPlaintext,
                null,
                new SaslProfile(SaslMechanism.ScramSha256, scram256Username, scram256Password)),
            new ClusterProfile(
                "sasl-plaintext-scram512",
                ["localhost:9094"],
                KafkaSecurityProtocol.SaslPlaintext,
                null,
                new SaslProfile(SaslMechanism.ScramSha512, scram512Username, scram512Password)),
            new ClusterProfile(
                "sasl-ssl-plain",
                ["localhost:9095"],
                KafkaSecurityProtocol.SaslSsl,
                serverTls,
                new SaslProfile(SaslMechanism.Plain, plainUsername, plainPassword)),
            new ClusterProfile(
                "sasl-ssl-scram256",
                ["localhost:9095"],
                KafkaSecurityProtocol.SaslSsl,
                serverTls,
                new SaslProfile(SaslMechanism.ScramSha256, scram256Username, scram256Password)),
            new ClusterProfile(
                "sasl-ssl-scram512",
                ["localhost:9095"],
                KafkaSecurityProtocol.SaslSsl,
                serverTls,
                new SaslProfile(SaslMechanism.ScramSha512, scram512Username, scram512Password)),
        };

        using var adapter = new ConfluentKafkaAdministrationAdapter(
            profiles,
            new SecretResolver());

        foreach (var profile in profiles)
        {
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var operation = new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(20));

            var cluster = await adapter.GetClusterMetadataAsync(profile.Id, operation, cancellation.Token);
            AssertSuccess(profile.Id, cluster);
            Assert.NotEmpty(cluster.Value!.Brokers);

            var topics = await adapter.ListTopicsAsync(profile.Id, operation, cancellation.Token);
            AssertSuccess(profile.Id, topics);
            Assert.Contains(topics.Value!, topic => topic.Name == "kafdeck-ci-smoke");

            var topic = await adapter.GetTopicMetadataAsync(
                profile.Id,
                "kafdeck-ci-smoke",
                operation,
                cancellation.Token);
            AssertSuccess(profile.Id, topic);
            Assert.Single(topic.Value!.Partitions);

            var configuration = await adapter.GetTopicConfigurationAsync(
                profile.Id,
                "kafdeck-ci-smoke",
                operation,
                cancellation.Token);
            AssertSuccess(profile.Id, configuration);
            Assert.NotEmpty(configuration.Value!);

            var brokerConfiguration = await adapter.GetBrokerConfigurationAsync(
                profile.Id,
                cluster.Value.ControllerBrokerId ?? cluster.Value.Brokers[0].BrokerId,
                operation,
                cancellation.Token);
            AssertSuccess(profile.Id, brokerConfiguration);
            Assert.NotEmpty(brokerConfiguration.Value!);
        }

        var capabilities = await adapter.GetCapabilitiesAsync(
            "plaintext",
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(20)),
            CancellationToken.None);
        AssertSuccess("plaintext", capabilities);
        Assert.Contains(
            capabilities.Value!.Items,
            item => item.Capability == KafkaCapabilityKind.ClusterMetadata &&
                    item.State == KafkaCapabilityState.Available);
        Assert.Contains(
            capabilities.Value.Items,
            item => item.Capability == KafkaCapabilityKind.TopicMetadata &&
                    item.State == KafkaCapabilityState.Available);
    }

    private static SecretReference FileReference(string directory, string fileName) =>
        SecretReference.Parse($"file:{Path.Combine(directory, fileName)}");

    private static void AssertSuccess<T>(string profileId, KafkaResult<T> result) =>
        Assert.True(
            result.IsSuccess,
            $"{profileId}: {result.Failure?.Category} / {result.Failure?.Code} / {result.Failure?.SafeMessage}");
}
