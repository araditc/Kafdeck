using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaRecordsPurgeIntegrationTests
{
    [Fact]
    public async Task Typed_delete_records_purges_exact_partition_and_verifies_low_watermark()
    {
        if (!RunKafkaIntegration())
            return;

        var topic = $"kafdeck-w38-{Guid.NewGuid():N}";
        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        using var topics =
            new ConfluentKafkaTopicMutationAdapter(
                new[] { profile },
                new SecretResolver());
        using var producer =
            new ConfluentKafkaRecordProduceAdapter(
                new[] { profile },
                new SecretResolver());
        using var observations =
            new ConfluentKafkaRecordsPurgeObservationAdapter(
                new[] { profile },
                new SecretResolver());
        using var purge =
            new ConfluentKafkaRecordsPurgeAdapter(
                new[] { profile },
                new SecretResolver());
        using var cancellation =
            new CancellationTokenSource(
                TimeSpan.FromSeconds(60));

        try
        {
            var created = await topics.CreateTopicAsync(
                new TopicCreateMutation(
                    profile.Id,
                    topic,
                    1,
                    1,
                    new Dictionary<string, string>(
                        StringComparer.Ordinal)),
                cancellation.Token);

            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                created.ResultKind);

            await EventuallyTopicReadyAsync(
                observations,
                profile.Id,
                topic,
                cancellation.Token);

            for (var i = 0; i < 5; i++)
            {
                var produced = await producer.ProduceAsync(
                    new RecordProduceMutation(
                        profile.Id,
                        topic,
                        null,
                        Encoding.UTF8.GetBytes(
                            $"w38-{i}-{Guid.NewGuid():N}"),
                        new Dictionary<
                            string,
                            ReadOnlyMemory<byte>>(
                                StringComparer.Ordinal)),
                    cancellation.Token);

                Assert.Equal(
                    MutationExecutionResultKind.AppliedVerified,
                    produced.ResultKind);
            }

            var before = await EventuallyHighWatermarkAsync(
                observations,
                profile.Id,
                topic,
                5,
                cancellation.Token);

            Assert.Equal(0, before.LowWatermark);
            Assert.True(before.HighWatermark >= 5);

            var planner = new RecordsPurgePlanner(
                observations);

            var planned = await planner.PlanAsync(
                new RecordsPurgeRequest(
                    profile.Id,
                    new[]
                    {
                        new RecordsPurgeTargetInput(
                            topic,
                            0,
                            new RecordsPurgeSelector(
                                RecordsPurgeSelectorKind.Absolute,
                                BeforeOffset: 3)),
                    }),
                cancellation.Token);

            Assert.True(
                planned.IsSuccess,
                planned.Failure?.SafeMessage);
            Assert.Equal(
                MutationRiskClass.Critical,
                planned.Plan!.Risk.RiskClass);
            Assert.True(
                planned.Plan.Risk.RequiresIndependentApproval);

            var operation = MutationOperation.CreatePreview(
                "oidc:https://idp.example|integration",
                planned.Plan.Intent,
                planned.Plan.Risk,
                "w38-integration",
                DateTimeOffset.UtcNow.AddMinutes(5),
                DateTimeOffset.UtcNow,
                $"w38-{Guid.NewGuid():N}");

            var service = new RecordsPurgeExecutionService(
                purge,
                observations);

            using var material =
                new MutationExecutionMaterial();

            var result = await service.ExecuteAsync(
                new MutationExecutionContext(
                    operation.Snapshot,
                    material,
                    DateTimeOffset.UtcNow.AddSeconds(12)),
                cancellation.Token);

            Assert.Equal(
                MutationExecutionResultKind.AppliedVerified,
                result.ResultKind);

            var after = await ObserveAsync(
                observations,
                profile.Id,
                topic,
                cancellation.Token);

            Assert.True(after.LowWatermark >= 3);
        }
        finally
        {
            try
            {
                await topics.DeleteTopicAsync(
                    new TopicDeleteMutation(
                        profile.Id,
                        topic),
                    CancellationToken.None);
            }
            catch
            {
                // Best-effort cleanup for a unique W38 topic.
            }
        }
    }

    private static async Task EventuallyTopicReadyAsync(
        IRecordsPurgeObservationPort observations,
        string clusterId,
        string topic,
        CancellationToken cancellationToken)
    {
        var deadline =
            DateTimeOffset.UtcNow.AddSeconds(20);

        while (DateTimeOffset.UtcNow < deadline)
        {
            var observed = await observations.ObserveAsync(
                clusterId,
                new[]
                {
                    new RecordsPurgeObservationTarget(
                        topic,
                        0),
                },
                Operation(),
                cancellationToken);

            if (observed.IsSuccess &&
                observed.Value is { Count: 1 })
            {
                return;
            }

            await Task.Delay(
                    TimeSpan.FromMilliseconds(200),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        Assert.Fail(
            "W38 integration topic did not become observable.");
    }

    private static async Task<RecordsPurgePartitionObservation>
        EventuallyHighWatermarkAsync(
            IRecordsPurgeObservationPort observations,
            string clusterId,
            string topic,
            long minimumHigh,
            CancellationToken cancellationToken)
    {
        var deadline =
            DateTimeOffset.UtcNow.AddSeconds(20);

        while (DateTimeOffset.UtcNow < deadline)
        {
            var observed = await observations.ObserveAsync(
                clusterId,
                new[]
                {
                    new RecordsPurgeObservationTarget(
                        topic,
                        0),
                },
                Operation(),
                cancellationToken);

            if (observed.IsSuccess &&
                observed.Value is { Count: 1 } &&
                observed.Value[0].HighWatermark >= minimumHigh)
            {
                return observed.Value[0];
            }

            await Task.Delay(
                    TimeSpan.FromMilliseconds(200),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        Assert.Fail(
            "W38 integration high watermark did not reach the expected value.");
        throw new InvalidOperationException();
    }

    private static async Task<RecordsPurgePartitionObservation>
        ObserveAsync(
            IRecordsPurgeObservationPort observations,
            string clusterId,
            string topic,
            CancellationToken cancellationToken)
    {
        var result = await observations.ObserveAsync(
            clusterId,
            new[]
            {
                new RecordsPurgeObservationTarget(
                    topic,
                    0),
            },
            Operation(),
            cancellationToken);

        Assert.True(
            result.IsSuccess,
            result.Failure?.SafeMessage);
        return Assert.Single(result.Value!);
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
