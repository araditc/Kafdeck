using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaAdapterIntegrationTests
{
    [Fact]
    public async Task Plaintext_adapter_reads_cluster_topic_configuration_and_capabilities()
    {
        if (!string.Equals(
                Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"),
                "1",
                StringComparison.Ordinal))
        {
            return;
        }

        var profile = new ClusterProfile(
            "ci",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        using var adapter = new ConfluentKafkaAdministrationAdapter(
            [profile],
            new SecretResolver());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var operation = new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(20));

        var cluster = await adapter.GetClusterMetadataAsync("ci", operation, cancellation.Token);
        Assert.True(cluster.IsSuccess);
        Assert.NotNull(cluster.Value);
        Assert.NotEmpty(cluster.Value.Brokers);

        var topics = await adapter.ListTopicsAsync("ci", operation, cancellation.Token);
        Assert.True(topics.IsSuccess);
        Assert.NotNull(topics.Value);
        Assert.Contains(topics.Value, topic => topic.Name == "kafdeck-ci-smoke");

        var topic = await adapter.GetTopicMetadataAsync(
            "ci",
            "kafdeck-ci-smoke",
            operation,
            cancellation.Token);
        Assert.True(topic.IsSuccess);
        Assert.NotNull(topic.Value);
        Assert.Single(topic.Value.Partitions);

        var configuration = await adapter.GetTopicConfigurationAsync(
            "ci",
            "kafdeck-ci-smoke",
            operation,
            cancellation.Token);
        Assert.True(configuration.IsSuccess);
        Assert.NotNull(configuration.Value);
        Assert.NotEmpty(configuration.Value);

        var capabilities = await adapter.GetCapabilitiesAsync(
            "ci",
            operation,
            cancellation.Token);
        Assert.True(capabilities.IsSuccess);
        Assert.NotNull(capabilities.Value);
        Assert.Contains(
            capabilities.Value.Items,
            item => item.Capability == KafkaCapabilityKind.ClusterMetadata &&
                    item.State == KafkaCapabilityState.Available);
        Assert.Contains(
            capabilities.Value.Items,
            item => item.Capability == KafkaCapabilityKind.TopicMetadata &&
                    item.State == KafkaCapabilityState.Available);
    }
}
