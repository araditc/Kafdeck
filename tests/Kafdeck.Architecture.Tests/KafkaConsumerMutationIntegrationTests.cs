using Confluent.Kafka;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Consumers;
using CoreConsumerGroupState = Kafdeck.Core.Consumers.ConsumerGroupState;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaConsumerMutationIntegrationTests
{
    [Fact]
    public async Task Typed_consumer_administration_alters_deletes_offsets_and_deletes_group()
    {
        if (!RunKafkaIntegration())
            return;

        const string topic = "kafdeck-ci-smoke";
        var groupId = $"kafdeck-w35-{Guid.NewGuid():N}";
        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        CreateCommittedOffset(groupId, topic, 1);

        using var observations =
            new ConfluentKafkaConsumerMutationObservationAdapter(
                new[] { profile },
                new SecretResolver());
        using var mutations =
            new ConfluentKafkaConsumerMutationAdapter(
                new[] { profile },
                new SecretResolver());
        using var cancellation =
            new CancellationTokenSource(TimeSpan.FromSeconds(60));

        try
        {
            var initial = await EventuallyObserveAsync(
                observations,
                profile.Id,
                groupId,
                topic,
                cancellation.Token);

            Assert.True(initial.Exists);
            Assert.Equal(CoreConsumerGroupState.Empty, initial.State);
            Assert.Equal(1, Assert.Single(initial.Partitions).CommittedOffset);

            var planner = new ConsumerMutationPlanner(observations);
            var alterPlan = await planner.PlanOffsetAlterAsync(
                new ConsumerOffsetAlterRequest(
                    profile.Id,
                    groupId,
                    new[]
                    {
                        new ConsumerOffsetAlterTargetInput(
                            topic,
                            0,
                            new ConsumerOffsetSelector(
                                ConsumerOffsetSelectorKind.RelativeShift,
                                1)),
                    }),
                cancellation.Token);

            Assert.True(alterPlan.IsSuccess, alterPlan.Failure?.SafeMessage);
            Assert.Equal(2, alterPlan.Plan!.Canonical.Targets[0].ResolvedOffset);

            var service = new ConsumerMutationExecutionService(
                mutations,
                observations);
            var altered = await service.AlterOffsetsAsync(
                alterPlan.Plan.Canonical,
                cancellation.Token);

            Assert.Equal(
                MutationExecutionResultKind.AppliedVerified,
                altered.ResultKind);

            var afterAlter = await EventuallyObserveAsync(
                observations,
                profile.Id,
                groupId,
                topic,
                cancellation.Token);
            Assert.Equal(2, Assert.Single(afterAlter.Partitions).CommittedOffset);

            var offsetDeletePlan = await planner.PlanDeleteAsync(
                new ConsumerDeleteRequest(
                    profile.Id,
                    groupId,
                    ConsumerDeleteMode.Offsets,
                    new[]
                    {
                        new ConsumerOffsetDeleteTargetInput(topic, 0),
                    }),
                cancellation.Token);

            Assert.True(
                offsetDeletePlan.IsSuccess,
                offsetDeletePlan.Failure?.SafeMessage);

            var offsetsDeleted = await service.DeleteAsync(
                offsetDeletePlan.Plan!.Canonical,
                cancellation.Token);

            Assert.Equal(
                MutationExecutionResultKind.AppliedVerified,
                offsetsDeleted.ResultKind);

            var afterOffsetDelete = await EventuallyObserveAsync(
                observations,
                profile.Id,
                groupId,
                topic,
                cancellation.Token);
            Assert.Null(
                Assert.Single(afterOffsetDelete.Partitions).CommittedOffset);

            var groupDeletePlan = await planner.PlanDeleteAsync(
                new ConsumerDeleteRequest(
                    profile.Id,
                    groupId,
                    ConsumerDeleteMode.Group),
                cancellation.Token);

            Assert.True(
                groupDeletePlan.IsSuccess,
                groupDeletePlan.Failure?.SafeMessage);

            var groupDeleted = await service.DeleteAsync(
                groupDeletePlan.Plan!.Canonical,
                cancellation.Token);

            Assert.Equal(
                MutationExecutionResultKind.AppliedVerified,
                groupDeleted.ResultKind);

            var final = await observations.ObserveAsync(
                profile.Id,
                groupId,
                Array.Empty<ConsumerMutationObservationTarget>(),
                Operation(),
                cancellation.Token);

            Assert.True(final.IsSuccess, final.Failure?.SafeMessage);
            Assert.False(final.Value!.Exists);
        }
        finally
        {
            try
            {
                await mutations.DeleteAsync(
                    new ConsumerDeleteMutation(profile.Id, groupId),
                    CancellationToken.None);
            }
            catch
            {
                // Best-effort cleanup for a unique integration-test group.
            }
        }
    }

    [Fact]
    public void Prepare_restricted_consumer_mutation_authorization_fixture()
    {
        if (!RunKafkaIntegration())
            return;

        CreateCommittedOffset(
            "kafdeck-w35-restricted",
            "kafdeck-ci-smoke",
            1);
    }

    private static void CreateCommittedOffset(
        string groupId,
        string topic,
        long offset)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = groupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using var consumer =
            new ConsumerBuilder<Ignore, Ignore>(config).Build();
        consumer.Assign(
            new TopicPartitionOffset(
                topic,
                new Partition(0),
                new Offset(0)));
        consumer.Commit(
            new[]
            {
                new TopicPartitionOffset(
                    topic,
                    new Partition(0),
                    new Offset(offset)),
            });
        consumer.Close();
    }

    private static async Task<ConsumerMutationObservation>
        EventuallyObserveAsync(
            IConsumerMutationObservationPort observations,
            string clusterId,
            string groupId,
            string topic,
            CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(20);
        KafkaResult<ConsumerMutationObservation>? last = null;

        while (DateTimeOffset.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            last = await observations.ObserveAsync(
                clusterId,
                groupId,
                new[]
                {
                    new ConsumerMutationObservationTarget(topic, 0),
                },
                Operation(),
                cancellationToken);

            if (last.IsSuccess &&
                last.Value is not null &&
                last.Value.Exists &&
                last.Value.State == CoreConsumerGroupState.Empty &&
                last.Value.Partitions.Count == 1)
            {
                return last.Value;
            }

            await Task.Delay(
                    TimeSpan.FromMilliseconds(200),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        Assert.Fail(
            $"Consumer mutation observation did not stabilize: {last?.Failure?.Code}");
        throw new InvalidOperationException();
    }

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10));

    private static bool RunKafkaIntegration() =>
        string.Equals(
            Environment.GetEnvironmentVariable(
                "KAFDECK_RUN_KAFKA_INTEGRATION"),
            "1",
            StringComparison.Ordinal);
}
