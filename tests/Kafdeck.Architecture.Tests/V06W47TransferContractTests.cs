using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W47TransferContractTests
{
    [Fact]
    public async Task Planner_binds_two_physical_clusters_finite_ranges_and_full_authorization_conjunction()
    {
        var masking = new StaticRecordMaskingPolicyProvider(
            RecordMaskingPolicyCompiler.Compile(
                new RecordMaskingPolicyDefinition("none", 1)));
        var kafka = new StubKafka(
            new ClusterMetadata("source", "kafka-source", 1, Array.Empty<BrokerMetadata>()),
            new ClusterMetadata("destination", "kafka-destination", 2, Array.Empty<BrokerMetadata>()),
            Topic("orders", 2),
            Topic("orders-copy", 3));
        var planner = new ClusterTransferPlanner(kafka, masking);

        var result = await planner.PlanAsync(new ClusterTransferPlanningRequest(
            "source",
            "source-config-v1",
            "destination",
            "destination-config-v3",
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
            new ClusterTransferBudget()));

        Assert.True(result.IsSuccess);
        Assert.Equal(MutationRiskClass.High, result.Risk!.RiskClass);
        Assert.False(result.Risk.RequiresIndependentApproval);
        Assert.Equal(MutationOperationKind.ClusterTransfer, result.Intent!.Kind);

        var actions = result.Intent.AuthorizationTargets!
            .Select(item => item.Action)
            .ToHashSet();
        foreach (var action in new[]
                 {
                     AuthorizationAction.ClusterTransferPlan,
                     AuthorizationAction.ClusterTransferExecute,
                     AuthorizationAction.ClusterRead,
                     AuthorizationAction.TopicRead,
                     AuthorizationAction.RecordRead,
                     AuthorizationAction.RecordExport,
                     AuthorizationAction.RecordProduce,
                 })
            Assert.Contains(action, actions);

        var normalized = MutationAuthorizationRequirements.Normalize(
            result.Intent.Kind,
            result.Intent.ClusterId,
            result.Intent.AuthorizationTargets,
            result.Intent.ResourceKeys);
        Assert.Equal(2, normalized.Select(item => item.ClusterId).Distinct().Count());
        Assert.Contains(result.Intent.ResourceKeys, key =>
            FleetConflictKeyCodec.Decode(key).Kind == FleetConflictTargetKind.TransferPair);
        Assert.Contains(result.Intent.ResourceKeys, key =>
            FleetConflictKeyCodec.Decode(key).Kind == FleetConflictTargetKind.TopicPartition);
    }

    [Fact]
    public async Task Planner_rejects_profile_aliases_to_same_physical_kafka_cluster()
    {
        var masking = new StaticRecordMaskingPolicyProvider(
            RecordMaskingPolicyCompiler.Compile(
                new RecordMaskingPolicyDefinition("none", 1)));
        var kafka = new StubKafka(
            new ClusterMetadata("source", "same-kafka-id", 1, Array.Empty<BrokerMetadata>()),
            new ClusterMetadata("destination", "same-kafka-id", 1, Array.Empty<BrokerMetadata>()),
            Topic("orders", 1),
            Topic("orders-copy", 1));
        var planner = new ClusterTransferPlanner(kafka, masking);

        var result = await planner.PlanAsync(new ClusterTransferPlanningRequest(
            "source",
            "v1",
            "destination",
            "v1",
            new[] { new ClusterTransferMappingRequest("orders", 0, "orders-copy", 0, 0, 1) },
            new ClusterTransferBudget()));

        Assert.False(result.IsSuccess);
        Assert.Equal(ClusterTransferPlanningFailureCode.SamePhysicalCluster, result.Failure!.Code);
    }

    [Fact]
    public async Task Planner_denies_masking_policy_instead_of_transferring_unredacted_bytes()
    {
        var masking = new StaticRecordMaskingPolicyProvider(
            RecordMaskingPolicyCompiler.Compile(
                new RecordMaskingPolicyDefinition(
                    "masked",
                    2,
                    HeaderRules: new[] { new RecordHeaderMaskRule("authorization") })));
        var kafka = new StubKafka(
            new ClusterMetadata("source", "a", 1, Array.Empty<BrokerMetadata>()),
            new ClusterMetadata("destination", "b", 2, Array.Empty<BrokerMetadata>()),
            Topic("orders", 1),
            Topic("orders-copy", 1));
        var planner = new ClusterTransferPlanner(kafka, masking);

        var result = await planner.PlanAsync(new ClusterTransferPlanningRequest(
            "source", "v1", "destination", "v1",
            new[] { new ClusterTransferMappingRequest("orders", 0, "orders-copy", 0, 0, 1) },
            new ClusterTransferBudget()));

        Assert.False(result.IsSuccess);
        Assert.Equal(ClusterTransferPlanningFailureCode.MaskingPolicyUnsupported, result.Failure!.Code);
    }

    [Fact]
    public async Task Risk_escalates_above_twenty_five_mappings()
    {
        var mappings = Enumerable.Range(0, 26)
            .Select(index => new ClusterTransferMapping(
                $"s-{index}",
                0,
                $"d-{index}",
                0,
                0,
                1,
                new string('a', 64),
                new string('b', 64)))
            .ToArray();
        var source = new ClusterTransferEndpoint("source", "v1", "physical-a");
        var destination = new ClusterTransferEndpoint("destination", "v1", "physical-b");
        var policy = new ClusterTransferDataPolicy("none", 1, new string('c', 64));
        var budget = new ClusterTransferBudget();
        var plan = new ClusterTransferPlan(
            source,
            destination,
            mappings,
            budget,
            policy,
            ClusterTransferPolicy.PlanFingerprint(source, destination, mappings, budget, policy));

        var risk = ClusterTransferPolicy.ClassifyRisk(plan);

        Assert.Equal(MutationRiskClass.Critical, risk.RiskClass);
        Assert.True(risk.RequiresIndependentApproval);
    }

    [Fact]
    public void Reconciled_risk_classifier_preserves_W44_and_W47_operation_floors()
    {
        Assert.Equal(
            MutationRiskClass.High,
            MutationRiskClassifier.Classify(new MutationRiskInput(MutationOperationKind.QuotaAlter)).RiskClass);
        Assert.Equal(
            MutationRiskClass.High,
            MutationRiskClassifier.Classify(new MutationRiskInput(MutationOperationKind.ClusterConfigAlter)).RiskClass);
        Assert.Equal(
            MutationRiskClass.High,
            MutationRiskClassifier.Classify(new MutationRiskInput(MutationOperationKind.ClusterTransfer)).RiskClass);
    }

    [Fact]
    public void Transfer_conflict_keys_are_bound_to_physical_clusters_not_profile_aliases()
    {
        var mapping = new ClusterTransferMapping(
            "orders",
            0,
            "orders-copy",
            1,
            10,
            20,
            new string('a', 64),
            new string('b', 64));
        var budget = new ClusterTransferBudget();
        var policy = new ClusterTransferDataPolicy(
            "none",
            1,
            new string('c', 64));

        var firstSource = new ClusterTransferEndpoint(
            "source-profile-a",
            "v1",
            "physical-source");
        var firstDestination = new ClusterTransferEndpoint(
            "destination-profile-a",
            "v1",
            "physical-destination");
        var secondSource = new ClusterTransferEndpoint(
            "source-profile-b",
            "v2",
            "physical-source");
        var secondDestination = new ClusterTransferEndpoint(
            "destination-profile-b",
            "v3",
            "physical-destination");

        var first = new ClusterTransferPlan(
            firstSource,
            firstDestination,
            new[] { mapping },
            budget,
            policy,
            ClusterTransferPolicy.PlanFingerprint(
                firstSource,
                firstDestination,
                new[] { mapping },
                budget,
                policy));
        var second = new ClusterTransferPlan(
            secondSource,
            secondDestination,
            new[] { mapping },
            budget,
            policy,
            ClusterTransferPolicy.PlanFingerprint(
                secondSource,
                secondDestination,
                new[] { mapping },
                budget,
                policy));

        var firstIntent = ClusterTransferPolicy.BuildIntent(first);
        var secondIntent = ClusterTransferPolicy.BuildIntent(second);

        Assert.Equal(
            firstIntent.ResourceKeys.OrderBy(value => value, StringComparer.Ordinal),
            secondIntent.ResourceKeys.OrderBy(value => value, StringComparer.Ordinal));

        var decoded = firstIntent.ResourceKeys
            .Select(FleetConflictKeyCodec.Decode)
            .ToArray();
        Assert.Contains(decoded, target =>
            target.Kind == FleetConflictTargetKind.TransferPair &&
            target.PhysicalClusterId == "physical-destination" &&
            target.ResourceId == "physical-source");
        Assert.Contains(decoded, target =>
            target.Kind == FleetConflictTargetKind.TopicPartition &&
            target.PhysicalClusterId == "physical-source" &&
            target.ResourceId == "orders");
        Assert.Contains(decoded, target =>
            target.Kind == FleetConflictTargetKind.TopicPartition &&
            target.PhysicalClusterId == "physical-destination" &&
            target.ResourceId == "orders-copy");

        Assert.NotEqual(
            firstIntent.AuthorizationTargets!
                .Select(target => target.ClusterId)
                .OrderBy(value => value, StringComparer.Ordinal),
            secondIntent.AuthorizationTargets!
                .Select(target => target.ClusterId)
                .OrderBy(value => value, StringComparer.Ordinal));
    }

    [Fact]
    public void Durable_progress_never_stores_payload_and_blocks_replay_after_dispatch_marker()
    {
        var progress = FleetTransferProgress.Create(
            new string('a', 64),
            new long[] { 10 });
        var pending = progress.ReserveBeforeDispatch(
            0,
            10,
            2,
            512,
            DateTimeOffset.UtcNow);

        Assert.Equal(FleetTransferBatchState.ReservedBeforeDispatch, pending.State);
        progress.MarkDispatchStarted(pending.BatchId, DateTimeOffset.UtcNow);

        var restored = FleetTransferProgress.Restore(progress.Snapshot);
        Assert.Equal(FleetTransferBatchState.DispatchStarted, restored.Snapshot.PendingBatch!.State);
        Assert.Throws<MutationStateException>(() =>
            restored.ReserveBeforeDispatch(0, 10, 2, 512, DateTimeOffset.UtcNow));

        restored.CompleteAcknowledged(pending.BatchId, 11);
        Assert.Null(restored.Snapshot.PendingBatch);
        Assert.Equal(11, restored.Snapshot.Checkpoints[0].NextSourceOffset);
        Assert.Equal(1, restored.Snapshot.AcknowledgedRecords);
        Assert.Equal(512, restored.Snapshot.AcknowledgedBytes);
    }

    [Fact]
    public void Transfer_pair_conflict_is_direction_independent()
    {
        Assert.Equal(
            FleetConflictKeyCodec.TransferPair("cluster-a", "cluster-b"),
            FleetConflictKeyCodec.TransferPair("cluster-b", "cluster-a"));
    }

    private static TopicMetadata Topic(string name, int partitions) =>
        new(
            name,
            false,
            Enumerable.Range(0, partitions)
                .Select(index => new PartitionMetadata(
                    index,
                    1,
                    new[] { 1, 2, 3 },
                    new[] { 1, 2, 3 }))
                .ToArray());

    private sealed class StubKafka : IKafkaAdministrationPort
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

        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(Success(
                string.Equals(clusterId, _source.ClusterId, StringComparison.Ordinal)
                    ? _source
                    : _destination));

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(Success(
                string.Equals(clusterId, _source.ClusterId, StringComparison.Ordinal)
                    ? _sourceTopic
                    : _destinationTopic));

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

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

        private static KafkaResult<T> Success<T>(T value)
        {
            var now = DateTimeOffset.UtcNow;
            return KafkaResult<T>.Success(
                value,
                new ObservationMetadata(now, now, now, ObservationSource.Live));
        }
    }
}
