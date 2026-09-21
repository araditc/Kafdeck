using System.Text;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaRecordProduceIntegrationTests
{
    [Fact]
    public async Task Typed_record_producer_returns_broker_ack_for_single_and_bounded_batch()
    {
        if (!RunKafkaIntegration())
            return;

        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        using var producer = new ConfluentKafkaRecordProduceAdapter(
            new[] { profile },
            new SecretResolver());
        using var reads = new ConfluentKafkaAdministrationAdapter(
            new[] { profile },
            new SecretResolver());
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var single = await producer.ProduceAsync(
            new RecordProduceMutation(
                profile.Id,
                "kafdeck-ci-smoke",
                Encoding.UTF8.GetBytes("w34-single-key"),
                Encoding.UTF8.GetBytes($"w34-single-{Guid.NewGuid():N}"),
                new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal)
                {
                    ["x-kafdeck-test"] = Encoding.UTF8.GetBytes("single"),
                }),
            cancellation.Token);

        Assert.Equal(MutationExecutionResultKind.AppliedVerified, single.ResultKind);
        Assert.Equal("true", single.SafeEvidence!["provider.accepted"]);
        Assert.True(long.Parse(single.SafeEvidence["offset"]) >= 0);

        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var planner = new RecordProductionPlanner(reads, digest);

        using var preparation = await planner.PlanAsync(
            new RecordProductionRequest(
                profile.Id,
                "kafdeck-ci-smoke",
                new[]
                {
                    Record($"w34-batch-a-{Guid.NewGuid():N}"),
                    Record($"w34-batch-b-{Guid.NewGuid():N}"),
                }),
            cancellation.Token);

        Assert.True(preparation.IsSuccess, preparation.Failure?.SafeMessage);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|integration",
            preparation.Plan!.Intent,
            preparation.Plan.Risk,
            "w34-integration",
            DateTimeOffset.UtcNow.AddMinutes(5),
            DateTimeOffset.UtcNow,
            $"w34-{Guid.NewGuid():N}");

        using var material = new MutationExecutionMaterial(
            preparation.ExecutionMaterial!.Items);
        var service = new RecordProductionExecutionService(producer);
        var batch = await service.ExecuteAsync(
            new MutationExecutionContext(operation.Snapshot, material),
            cancellation.Token);

        Assert.Equal(MutationExecutionResultKind.AppliedVerified, batch.ResultKind);
        Assert.Equal("2", batch.SafeEvidence!["record.count"]);
        Assert.Equal("2", batch.SafeEvidence["acknowledged.count"]);
        Assert.Equal("true", batch.SafeEvidence["provider.accepted"]);
    }

    private static RecordProductionRecordInput Record(string value) =>
        new(
            null,
            Encoding.UTF8.GetBytes(value),
            Array.Empty<RecordProductionHeaderInput>());

    private static bool RunKafkaIntegration() =>
        string.Equals(
            Environment.GetEnvironmentVariable("KAFDECK_RUN_KAFKA_INTEGRATION"),
            "1",
            StringComparison.Ordinal);
}
