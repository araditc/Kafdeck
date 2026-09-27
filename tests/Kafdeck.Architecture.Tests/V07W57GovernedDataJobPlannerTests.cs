using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W57GovernedDataJobPlannerTests
{
    [Fact]
    public async Task Byte_preserving_forward_preview_reuses_physical_identity_and_full_authorization()
    {
        var planner = Planner(
            new StaticRecordMaskingPolicyProvider(
                RecordMaskingPolicyCompiler.Compile(
                    new RecordMaskingPolicyDefinition(
                        "none",
                        1))));

        var result = await planner.PlanAsync(
            Request(
                GovernedDataJobKind.Forward,
                new GovernedDataTransform(
                    GovernedDataTransformKind.BytePreserving)));

        Assert.True(
            result.IsSuccess,
            result.Failure?.SafeMessage);
        Assert.NotNull(result.Plan);
        Assert.NotNull(result.Intent);
        Assert.NotNull(result.Risk);

        Assert.Equal(
            MutationOperationKind.DataJob,
            result.Intent!.Kind);
        Assert.Equal(
            MutationRiskClass.High,
            result.Risk!.RiskClass);

        var actions = result.Intent.AuthorizationTargets!
            .Select(target => target.Action)
            .ToHashSet();

        foreach (var action in new[]
                 {
                     AuthorizationAction.DataJobPlan,
                     AuthorizationAction.DataJobExecute,
                     AuthorizationAction.RecordRead,
                     AuthorizationAction.RecordExport,
                     AuthorizationAction.RecordProduce,
                 })
        {
            Assert.Contains(action, actions);
        }

        Assert.Contains(
            result.Intent.ResourceKeys,
            key =>
                key ==
                $"data-job/{result.Plan!.PlanFingerprint}");
    }

    [Fact]
    public async Task Byte_preserving_preview_denies_masking_policy_instead_of_bypassing_it()
    {
        var planner = Planner(
            new StaticRecordMaskingPolicyProvider(
                RecordMaskingPolicyCompiler.Compile(
                    new RecordMaskingPolicyDefinition(
                        "masked",
                        2,
                        HeaderRules:
                        [
                            new RecordHeaderMaskRule(
                                "authorization"),
                        ]))));

        var result = await planner.PlanAsync(
            Request(
                GovernedDataJobKind.Forward,
                new GovernedDataTransform(
                    GovernedDataTransformKind.BytePreserving)));

        Assert.False(result.IsSuccess);
        Assert.Equal(
            GovernedDataJobPlanningFailureCode.TransferPlanningFailed,
            result.Failure!.Code);
        Assert.Equal(
            ClusterTransferPlanningFailureCode.MaskingPolicyUnsupported,
            result.Failure.TransferFailure);
    }

    [Fact]
    public async Task Structured_reprocess_preview_is_explicitly_unsupported_until_executor_is_active()
    {
        var planner = Planner(
            new StaticRecordMaskingPolicyProvider(
                RecordMaskingPolicyCompiler.Compile(
                    new RecordMaskingPolicyDefinition(
                        "none",
                        1))));

        var result = await planner.PlanAsync(
            Request(
                GovernedDataJobKind.Reprocess,
                new GovernedDataTransform(
                    GovernedDataTransformKind
                        .MaskedStructuredProjection,
                    "json",
                    new[] { "customerId" })));

        Assert.False(result.IsSuccess);
        Assert.Equal(
            GovernedDataJobPlanningFailureCode.TransformUnsupported,
            result.Failure!.Code);
    }

    [Fact]
    public async Task Pre_dispatch_replan_detects_physical_provider_identity_drift()
    {
        var masking =
            new StaticRecordMaskingPolicyProvider(
                RecordMaskingPolicyCompiler.Compile(
                    new RecordMaskingPolicyDefinition(
                        "none",
                        1)));

        var initialPlanner = Planner(masking);
        var request = Request(
            GovernedDataJobKind.Forward,
            new GovernedDataTransform(
                GovernedDataTransformKind.BytePreserving));
        var planned = await initialPlanner.PlanAsync(request);

        Assert.True(
            planned.IsSuccess,
            planned.Failure?.SafeMessage);

        var now = DateTimeOffset.Parse(
            "2026-09-27T08:00:00Z");
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|data-job-user",
            planned.Intent!,
            planned.Risk!,
            "v0.7-w57-provider-drift",
            now.AddMinutes(10),
            now,
            "w57-provider-drift");

        var driftedKafka = new StubKafka(
            new ClusterMetadata(
                "source",
                "physical-source",
                1,
                Array.Empty<BrokerMetadata>()),
            new ClusterMetadata(
                "destination",
                "physical-destination-recreated",
                2,
                Array.Empty<BrokerMetadata>()),
            Topic("orders", 2),
            Topic("orders-copy", 3));

        var driftedPlanner =
            new GovernedDataJobPlanner(
                new ClusterTransferPlanner(
                    driftedKafka,
                    masking));
        var validator =
            new GovernedDataJobPreconditionValidator(
                driftedPlanner);

        var validation =
            await validator.ValidateAsync(
                operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            validation.Outcome);
        Assert.Equal(
            "data_job_plan_drift",
            validation.ResultCode);
    }

    [Fact]
    public async Task Same_physical_cluster_alias_is_rejected_by_preview()
    {
        var masking =
            new StaticRecordMaskingPolicyProvider(
                RecordMaskingPolicyCompiler.Compile(
                    new RecordMaskingPolicyDefinition(
                        "none",
                        1)));
        var kafka = new StubKafka(
            new ClusterMetadata(
                "source",
                "same",
                1,
                Array.Empty<BrokerMetadata>()),
            new ClusterMetadata(
                "destination",
                "same",
                1,
                Array.Empty<BrokerMetadata>()),
            Topic("orders", 1),
            Topic("orders-copy", 1));

        var planner =
            new GovernedDataJobPlanner(
                new ClusterTransferPlanner(
                    kafka,
                    masking));

        var result = await planner.PlanAsync(
            Request(
                GovernedDataJobKind.Forward,
                new GovernedDataTransform(
                    GovernedDataTransformKind.BytePreserving)));

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ClusterTransferPlanningFailureCode.SamePhysicalCluster,
            result.Failure!.TransferFailure);
    }

    private static GovernedDataJobPlanner Planner(
        IRecordMaskingPolicyProvider masking)
    {
        var kafka = new StubKafka(
            new ClusterMetadata(
                "source",
                "physical-source",
                1,
                Array.Empty<BrokerMetadata>()),
            new ClusterMetadata(
                "destination",
                "physical-destination",
                2,
                Array.Empty<BrokerMetadata>()),
            Topic("orders", 2),
            Topic("orders-copy", 3));

        return new GovernedDataJobPlanner(
            new ClusterTransferPlanner(
                kafka,
                masking));
    }

    private static GovernedDataJobPlanningRequest Request(
        GovernedDataJobKind kind,
        GovernedDataTransform transform) =>
        new(
            kind,
            "source",
            "source-v1",
            "destination",
            "destination-v1",
            new[]
            {
                new ClusterTransferMappingRequest(
                    "orders",
                    1,
                    "orders-copy",
                    2,
                    10,
                    20),
            },
            new ClusterTransferBudget(
                maxBatchRecords: 10,
                maxBatchBytes: 1024 * 1024,
                maxTotalRecords: 10,
                maxTotalBytes: 10 * 1024 * 1024,
                maxDuration:
                    TimeSpan.FromMinutes(5),
                maxRecordsPerSecond: 100,
                maxBytesPerSecond: 1024 * 1024),
            transform);

    private static TopicMetadata Topic(
        string name,
        int partitions) =>
        new(
            name,
            false,
            Enumerable.Range(0, partitions)
                .Select(index =>
                    new PartitionMetadata(
                        index,
                        1,
                        new[] { 1, 2, 3 },
                        new[] { 1, 2, 3 }))
                .ToArray());

    private sealed class StubKafka :
        IKafkaAdministrationPort
    {
        private readonly ClusterMetadata _source;
        private readonly ClusterMetadata _destination;
        private readonly TopicMetadata _sourceTopic;
        private readonly TopicMetadata _destinationTopic;

        public StubKafka(
            ClusterMetadata source,
            ClusterMetadata destination,
            TopicMetadata sourceTopic,
            TopicMetadata destinationTopic)
        {
            _source = source;
            _destination = destination;
            _sourceTopic = sourceTopic;
            _destinationTopic = destinationTopic;
        }

        public Task<KafkaResult<ClusterMetadata>>
            GetClusterMetadataAsync(
                string clusterId,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                Success(
                    string.Equals(
                        clusterId,
                        _source.ClusterId,
                        StringComparison.Ordinal)
                        ? _source
                        : _destination));

        public Task<KafkaResult<TopicMetadata>>
            GetTopicMetadataAsync(
                string clusterId,
                string topicName,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                Success(
                    string.Equals(
                        clusterId,
                        _source.ClusterId,
                        StringComparison.Ordinal)
                        ? _sourceTopic
                        : _destinationTopic));

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>>
            ListTopicsAsync(
                string clusterId,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>
            GetTopicConfigurationAsync(
                string clusterId,
                string topicName,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>
            GetBrokerConfigurationAsync(
                string clusterId,
                int brokerId,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<KafkaResult<KafkaCapabilities>>
            GetCapabilitiesAsync(
                string clusterId,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        private static KafkaResult<T> Success<T>(
            T value)
        {
            var now = DateTimeOffset.UtcNow;
            return KafkaResult<T>.Success(
                value,
                new ObservationMetadata(
                    now,
                    now,
                    now,
                    ObservationSource.Live));
        }
    }
}
