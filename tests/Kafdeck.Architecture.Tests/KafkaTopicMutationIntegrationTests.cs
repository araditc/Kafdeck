using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaTopicMutationIntegrationTests
{
    [Fact]
    public async Task Typed_topic_mutation_adapter_performs_create_alter_partition_increase_and_delete()
    {
        if (!RunKafkaIntegration())
        {
            return;
        }

        var topicName = $"kafdeck-v05-{Guid.NewGuid():N}";
        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        using var mutations = new ConfluentKafkaTopicMutationAdapter(
            new[] { profile },
            new SecretResolver());
        using var reads = new ConfluentKafkaAdministrationAdapter(
            new[] { profile },
            new SecretResolver());
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        try
        {
            var created = await mutations.CreateTopicAsync(
                new TopicCreateMutation(
                    profile.Id,
                    topicName,
                    1,
                    1,
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["retention.ms"] = "60000",
                    }),
                cancellation.Token);

            Assert.Equal(MutationExecutionResultKind.AppliedUnverified, created.ResultKind);
            await EventuallyAsync(
                async () =>
                {
                    var metadata = await reads.GetTopicMetadataAsync(
                        profile.Id,
                        topicName,
                        Operation(),
                        cancellation.Token);
                    return metadata.IsSuccess &&
                           metadata.Value is not null &&
                           metadata.Value.Partitions.Count == 1;
                },
                cancellation.Token);

            var altered = await mutations.AlterTopicAsync(
                new TopicAlterMutation(
                    profile.Id,
                    topicName,
                    new Dictionary<string, string?>(StringComparer.Ordinal)
                    {
                        ["retention.ms"] = "120000",
                    }),
                cancellation.Token);

            Assert.Equal(MutationExecutionResultKind.AppliedUnverified, altered.ResultKind);
            await EventuallyAsync(
                async () =>
                {
                    var configuration = await reads.GetTopicConfigurationAsync(
                        profile.Id,
                        topicName,
                        Operation(),
                        cancellation.Token);
                    return configuration.IsSuccess &&
                           configuration.Value is not null &&
                           configuration.Value.Any(entry =>
                               string.Equals(entry.Name, "retention.ms", StringComparison.Ordinal) &&
                               string.Equals(entry.Value, "120000", StringComparison.Ordinal) &&
                               !entry.IsSensitive);
                },
                cancellation.Token);

            var partitions = await mutations.IncreasePartitionsAsync(
                new TopicIncreasePartitionsMutation(
                    profile.Id,
                    topicName,
                    2),
                cancellation.Token);

            Assert.Equal(MutationExecutionResultKind.AppliedUnverified, partitions.ResultKind);
            await EventuallyAsync(
                async () =>
                {
                    var metadata = await reads.GetTopicMetadataAsync(
                        profile.Id,
                        topicName,
                        Operation(),
                        cancellation.Token);
                    return metadata.IsSuccess &&
                           metadata.Value is not null &&
                           metadata.Value.Partitions.Count == 2;
                },
                cancellation.Token);

            var deleted = await mutations.DeleteTopicAsync(
                new TopicDeleteMutation(profile.Id, topicName),
                cancellation.Token);

            Assert.Equal(MutationExecutionResultKind.AppliedUnverified, deleted.ResultKind);
            await EventuallyAsync(
                async () =>
                {
                    var metadata = await reads.GetTopicMetadataAsync(
                        profile.Id,
                        topicName,
                        Operation(),
                        cancellation.Token);
                    return !metadata.IsSuccess &&
                           metadata.Failure is not null &&
                           IsMissing(metadata.Failure);
                },
                cancellation.Token);
        }
        finally
        {
            try
            {
                await mutations.DeleteTopicAsync(
                    new TopicDeleteMutation(profile.Id, topicName),
                    CancellationToken.None);
            }
            catch
            {
                // Best-effort cleanup for a unique integration-test topic.
            }
        }
    }

    private static async Task EventuallyAsync(
        Func<Task<bool>> condition,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(20);
        while (DateTimeOffset.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await condition().ConfigureAwait(false))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken)
                .ConfigureAwait(false);
        }

        Assert.Fail("Kafka mutation post-condition was not observed before the bounded integration-test deadline.");
    }

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10));

    private static bool IsMissing(KafkaFailure failure) =>
        failure.Code is
            "kafka_unknowntopicorpart" or
            "kafka_local_unknowntopic" or
            "kafka_resourcenotfound";

    private static bool RunKafkaIntegration() =>
        string.Equals(
            Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"),
            "1",
            StringComparison.Ordinal);
}
