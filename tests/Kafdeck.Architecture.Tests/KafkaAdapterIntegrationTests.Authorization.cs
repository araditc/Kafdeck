using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaAdapterIntegrationTestsAuthorization
{
    [Fact]
    public async Task Restricted_principal_retains_metadata_but_topic_configuration_is_denied()
    {
        if (!string.Equals(Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"), "1", StringComparison.Ordinal)) return;

        var secretsDirectory = Environment.GetEnvironmentVariable("KAFDECK_TEST_SECRETS_DIR");
        Assert.False(string.IsNullOrWhiteSpace(secretsDirectory));
        Assert.True(Path.IsPathFullyQualified(secretsDirectory));

        var username = SecretReference.Parse($"file:{Path.Combine(secretsDirectory!, "restricted.username")}");
        var password = SecretReference.Parse($"file:{Path.Combine(secretsDirectory, "restricted.password")}");
        var profile = new ClusterProfile(
            "restricted",
            ["localhost:9094"],
            KafkaSecurityProtocol.SaslPlaintext,
            null,
            new SaslProfile(SaslMechanism.Plain, username, password));

        using var adapter = new ConfluentKafkaAdministrationAdapter([profile], new SecretResolver());
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var operation = new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(20));

        var topics = await adapter.ListTopicsAsync(profile.Id, operation, cancellation.Token);
        Assert.True(topics.IsSuccess, $"metadata read failed: {topics.Failure?.Category} / {topics.Failure?.Code}");
        Assert.Contains(topics.Value!, topic => topic.Name == "kafdeck-ci-smoke");

        var topic = await adapter.GetTopicMetadataAsync(profile.Id, "kafdeck-ci-smoke", operation, cancellation.Token);
        Assert.True(topic.IsSuccess, $"topic metadata read failed: {topic.Failure?.Category} / {topic.Failure?.Code}");

        var configuration = await adapter.GetTopicConfigurationAsync(profile.Id, "kafdeck-ci-smoke", operation, cancellation.Token);
        Assert.False(configuration.IsSuccess);
        Assert.NotNull(configuration.Failure);
        Assert.Equal(KafkaFailureCategory.Unauthorized, configuration.Failure.Category);
        Assert.False(configuration.Failure.IsRetryable);
        Assert.DoesNotContain(secretsDirectory, configuration.Failure.SafeMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Restricted_principal_cannot_execute_consumer_offset_mutation()
    {
        if (!string.Equals(
                Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"),
                "1",
                StringComparison.Ordinal))
        {
            return;
        }

        var secretsDirectory =
            Environment.GetEnvironmentVariable("KAFDECK_TEST_SECRETS_DIR");
        Assert.False(string.IsNullOrWhiteSpace(secretsDirectory));
        Assert.True(Path.IsPathFullyQualified(secretsDirectory));

        var username = SecretReference.Parse(
            $"file:{Path.Combine(secretsDirectory!, "restricted.username")}");
        var password = SecretReference.Parse(
            $"file:{Path.Combine(secretsDirectory, "restricted.password")}");
        var profile = new ClusterProfile(
            "restricted",
            ["localhost:9094"],
            KafkaSecurityProtocol.SaslPlaintext,
            null,
            new SaslProfile(
                SaslMechanism.Plain,
                username,
                password));

        using var adapter = new ConfluentKafkaConsumerMutationAdapter(
            [profile],
            new SecretResolver());
        using var cancellation =
            new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var result = await adapter.AlterOffsetsAsync(
            new ConsumerOffsetAlterMutation(
                profile.Id,
                "kafdeck-w35-restricted",
                new[]
                {
                    new ConsumerOffsetTarget(
                        "kafdeck-ci-smoke",
                        0,
                        1),
                }),
            cancellation.Token);

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            result.ResultKind);
        Assert.Contains(
            result.ResultCode,
            new[]
            {
                "consumer_offset_alter_kafka_groupauthorizationfailed",
                "consumer_offset_alter_kafka_topicauthorizationfailed",
                "consumer_offset_alter_kafka_clusterauthorizationfailed",
            });
        Assert.DoesNotContain(
            secretsDirectory,
            result.ResultCode,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            secretsDirectory,
            string.Join(
                "\n",
                result.SafeEvidence?.Select(pair => $"{pair.Key}={pair.Value}")
                ?? Array.Empty<string>()),
            StringComparison.Ordinal);
    }

}
