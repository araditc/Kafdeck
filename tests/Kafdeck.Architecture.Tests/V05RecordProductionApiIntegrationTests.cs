using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05RecordProductionApiIntegrationTests
{
    [Fact]
    public async Task Resubmitted_record_material_rebuilds_to_the_admitted_hmac_digest()
    {
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var kafka = new RecordProductionKafkaPort();
        var planner = new RecordProductionPlanner(kafka, digest);
        var request = Request("payload-one");

        using var planning = await planner.PlanAsync(request);
        Assert.True(planning.IsSuccess, planning.Failure?.SafeMessage);

        using var rebuilt = RecordProductionExecutionMaterialBuilder.Build(
            planning.Plan!.Canonical,
            request.Records);

        var expected = Assert.Single(planning.Plan.Intent.MaterialDigests!);
        Assert.Equal(
            expected.Digest,
            digest.ComputeDigest(rebuilt.Items[expected.Name].Span));
    }

    [Fact]
    public async Task Same_shape_but_changed_payload_does_not_match_the_admitted_digest()
    {
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var kafka = new RecordProductionKafkaPort();
        var planner = new RecordProductionPlanner(kafka, digest);
        var previewRequest = Request("payload-one");

        using var planning = await planner.PlanAsync(previewRequest);
        Assert.True(planning.IsSuccess, planning.Failure?.SafeMessage);

        var changed = Request("payload-two");
        using var rebuilt = RecordProductionExecutionMaterialBuilder.Build(
            planning.Plan!.Canonical,
            changed.Records);

        var expected = Assert.Single(planning.Plan.Intent.MaterialDigests!);
        Assert.NotEqual(
            expected.Digest,
            digest.ComputeDigest(rebuilt.Items[expected.Name].Span));
    }

    [Fact]
    public async Task Record_production_pre_dispatch_validation_detects_topic_drift()
    {
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var kafka = new RecordProductionKafkaPort();
        var planner = new RecordProductionPlanner(kafka, digest);

        using var planning = await planner.PlanAsync(Request("payload-one"));
        Assert.True(planning.IsSuccess, planning.Failure?.SafeMessage);

        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planning.Plan!.Intent,
            planning.Plan.Risk,
            "v0.5-w39-record-production",
            now.AddMinutes(5),
            now,
            "record-production-precondition");
        var validator = new RecordProductionPreconditionValidator(kafka);

        var unchanged = await validator.ValidateAsync(operation.Snapshot);
        Assert.Equal(MutationPreDispatchGuardOutcome.Allowed, unchanged.Outcome);

        kafka.PartitionLeader = 2;
        var changed = await validator.ValidateAsync(operation.Snapshot);
        Assert.Equal(MutationPreDispatchGuardOutcome.StalePreview, changed.Outcome);
        Assert.Equal("record_production_precondition_changed", changed.ResultCode);
    }

    private static RecordProductionRequest Request(string payload) =>
        new(
            "prod",
            "orders",
            new[]
            {
                new RecordProductionRecordInput(
                    Encoding.UTF8.GetBytes("key-1"),
                    Encoding.UTF8.GetBytes(payload),
                    new[]
                    {
                        new RecordProductionHeaderInput(
                            "trace",
                            Encoding.UTF8.GetBytes("header-1")),
                    }),
            });

    private sealed class RecordProductionKafkaPort : IKafkaAdministrationPort
    {
        private readonly ObservationMetadata _observation = new(
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow.AddMinutes(1),
            DateTimeOffset.UtcNow.AddMinutes(2),
            ObservationSource.Live);

        public int PartitionLeader { get; set; } = 1;

        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(
                KafkaResult<ClusterMetadata>.Success(
                    new ClusterMetadata(
                        clusterId,
                        "kafka-prod",
                        1,
                        new[]
                        {
                            new BrokerMetadata(1, "broker-1", 9092, null, true),
                            new BrokerMetadata(2, "broker-2", 9092, null, false),
                        }),
                    _observation));

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(
                KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                    Array.Empty<TopicSummary>(),
                    _observation));

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(
                KafkaResult<TopicMetadata>.Success(
                    new TopicMetadata(
                        topicName,
                        false,
                        new[]
                        {
                            new PartitionMetadata(
                                0,
                                PartitionLeader,
                                new[] { 1, 2 },
                                new[] { 1, 2 }),
                        }),
                    _observation));

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
            string clusterId,
            int brokerId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }
}
