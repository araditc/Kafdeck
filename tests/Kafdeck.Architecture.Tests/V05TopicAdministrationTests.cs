using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Topics;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05TopicAdministrationTests
{
    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(".")]
    [InlineData("..")]
    [InlineData("orders *")]
    [InlineData("orders/*")]
    [InlineData("orders\n")]
    public void Topic_policy_rejects_non_exact_or_unsafe_names(string topicName)
    {
        Assert.ThrowsAny<ArgumentException>(
            () => TopicMutationPolicy.NormalizeDelete(
                new TopicDeleteMutation("prod", topicName)));
    }

    [Fact]
    public void Topic_config_policy_is_allowlisted_canonical_and_risk_sensitive()
    {
        var normalized = TopicMutationPolicy.NormalizeAlter(
            new TopicAlterMutation(
                "prod",
                "payments",
                new Dictionary<string, string?>(StringComparer.Ordinal)
                {
                    ["cleanup.policy"] = "delete,compact",
                    ["retention.ms"] = "60000",
                    ["segment.bytes"] = "1048576",
                }));

        Assert.Equal("compact,delete", normalized.Changes["cleanup.policy"]);
        Assert.Equal("60000", normalized.Changes["retention.ms"]);
        Assert.Equal("1048576", normalized.Changes["segment.bytes"]);
        Assert.True(TopicMutationPolicy.IsDurabilitySensitive(normalized.Changes.Keys));

        Assert.Throws<TopicMutationPolicyException>(
            () => TopicMutationPolicy.NormalizeAlter(
                new TopicAlterMutation(
                    "prod",
                    "payments",
                    new Dictionary<string, string?>(StringComparer.Ordinal)
                    {
                        ["unclean.leader.election.enable"] = "true",
                    })));
    }

    [Fact]
    public async Task Create_planner_binds_absence_broker_context_and_low_risk()
    {
        var reads = new FakeKafkaAdministrationPort
        {
            Cluster = (_, _, _) => Task.FromResult(
                KafkaResult<ClusterMetadata>.Success(
                    Cluster("prod", brokerCount: 3),
                    Observation())),
            Topics = (_, _, _) => Task.FromResult(
                KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                    new[] { new TopicSummary("existing", 1, false) },
                    Observation())),
        };
        var planner = new TopicMutationPlanner(reads);

        var result = await planner.PlanCreateAsync(
            new TopicCreateMutation(
                "prod",
                "orders",
                PartitionCount: 3,
                ReplicationFactor: 3,
                new Dictionary<string, string>(StringComparer.Ordinal)));

        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Plan);
        Assert.Equal(MutationRiskClass.Low, result.Plan!.Risk.RiskClass);
        Assert.NotNull(result.Plan.Intent.Preconditions);
        Assert.Single(result.Plan.Intent.Preconditions!);
        Assert.Equal("topic.absence", result.Plan.Intent.Preconditions![0].Key);
        Assert.NotNull(result.Plan.Intent.AuthorizationTargets);
        Assert.Equal("orders", result.Plan.Intent.AuthorizationTargets!.Single().ResourceName);
        Assert.Equal(
            "cluster/prod/topic/orders",
            result.Plan.Intent.ResourceKeys.Single());
    }

    [Fact]
    public async Task Create_planner_rejects_unsatisfied_replication_factor()
    {
        var reads = new FakeKafkaAdministrationPort
        {
            Cluster = (_, _, _) => Task.FromResult(
                KafkaResult<ClusterMetadata>.Success(
                    Cluster("prod", brokerCount: 2),
                    Observation())),
        };
        var planner = new TopicMutationPlanner(reads);

        var result = await planner.PlanCreateAsync(
            new TopicCreateMutation(
                "prod",
                "orders",
                PartitionCount: 3,
                ReplicationFactor: 3,
                new Dictionary<string, string>(StringComparer.Ordinal)));

        Assert.False(result.IsSuccess);
        Assert.Equal(
            TopicMutationPlanningFailureCode.ReplicationFactorUnsatisfied,
            result.Failure!.Code);
    }

    [Fact]
    public async Task Alter_planner_rejects_internal_topic_and_readonly_config()
    {
        var internalReads = new FakeKafkaAdministrationPort
        {
            Metadata = (_, topic, _, _) => Task.FromResult(
                KafkaResult<TopicMetadata>.Success(
                    Topic(topic, partitions: 1, isInternal: true),
                    Observation())),
        };

        var internalResult = await new TopicMutationPlanner(internalReads)
            .PlanAlterAsync(
                new TopicAlterMutation(
                    "prod",
                    "__consumer_offsets",
                    new Dictionary<string, string?>(StringComparer.Ordinal)
                    {
                        ["retention.ms"] = "60000",
                    }));

        Assert.False(internalResult.IsSuccess);
        Assert.Equal(
            TopicMutationPlanningFailureCode.InternalTopicUnsupported,
            internalResult.Failure!.Code);

        var readOnlyReads = new FakeKafkaAdministrationPort
        {
            Metadata = (_, topic, _, _) => Task.FromResult(
                KafkaResult<TopicMetadata>.Success(
                    Topic(topic, partitions: 1),
                    Observation())),
            Configuration = (_, _, _, _) => Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>.Success(
                    new[]
                    {
                        new KafkaConfigurationEntry(
                            "retention.ms",
                            "604800000",
                            IsSensitive: false,
                            IsReadOnly: true,
                            Source: "DefaultConfig"),
                    },
                    Observation())),
        };

        var readOnlyResult = await new TopicMutationPlanner(readOnlyReads)
            .PlanAlterAsync(
                new TopicAlterMutation(
                    "prod",
                    "orders",
                    new Dictionary<string, string?>(StringComparer.Ordinal)
                    {
                        ["retention.ms"] = "60000",
                    }));

        Assert.False(readOnlyResult.IsSuccess);
        Assert.Equal(
            TopicMutationPlanningFailureCode.ConfigurationReadOnly,
            readOnlyResult.Failure!.Code);
    }

    [Fact]
    public async Task Partition_planner_requires_strict_increase_and_high_risk()
    {
        var reads = new FakeKafkaAdministrationPort
        {
            Metadata = (_, topic, _, _) => Task.FromResult(
                KafkaResult<TopicMetadata>.Success(
                    Topic(topic, partitions: 3),
                    Observation())),
        };
        var planner = new TopicMutationPlanner(reads);

        var invalid = await planner.PlanIncreasePartitionsAsync(
            new TopicIncreasePartitionsMutation("prod", "orders", 3));
        Assert.False(invalid.IsSuccess);
        Assert.Equal(
            TopicMutationPlanningFailureCode.InvalidPartitionCount,
            invalid.Failure!.Code);

        var valid = await planner.PlanIncreasePartitionsAsync(
            new TopicIncreasePartitionsMutation("prod", "orders", 6));
        Assert.True(valid.IsSuccess);
        Assert.Equal(MutationRiskClass.High, valid.Plan!.Risk.RiskClass);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            valid.Plan.Risk.ConfirmationMode);
    }

    [Fact]
    public async Task Topic_create_execution_is_verified_only_after_readback()
    {
        var mutations = new FakeTopicMutationPort
        {
            Create = (_, _) => Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.AppliedUnverified,
                    "topic_create_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                    })),
        };
        var reads = new FakeKafkaAdministrationPort
        {
            Metadata = (_, topic, _, _) => Task.FromResult(
                KafkaResult<TopicMetadata>.Success(
                    Topic(topic, partitions: 3),
                    Observation())),
            Configuration = (_, _, _, _) => Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>.Success(
                    new[]
                    {
                        new KafkaConfigurationEntry(
                            "retention.ms",
                            "60000",
                            IsSensitive: false,
                            IsReadOnly: false,
                            Source: "DynamicTopicConfig"),
                    },
                    Observation())),
        };

        var service = new TopicMutationExecutionService(
            mutations,
            reads,
            new TopicMutationVerificationPolicy(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(50)));

        var result = await service.CreateAsync(
            new TopicCreateMutation(
                "prod",
                "orders",
                3,
                1,
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["retention.ms"] = "60000",
                }));

        Assert.Equal(MutationExecutionResultKind.AppliedVerified, result.ResultKind);
        Assert.Equal("topic_create_verified", result.ResultCode);
        Assert.Equal("true", result.SafeEvidence!["provider.accepted"]);
        Assert.Equal("true", result.SafeEvidence["resource.exists"]);
        Assert.Equal("observed", result.SafeEvidence["verification.state"]);
    }

    [Fact]
    public async Task Topic_delete_verification_waits_until_topic_absence_is_observed()
    {
        var calls = 0;
        var mutations = new FakeTopicMutationPort
        {
            Delete = (_, _) => Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.AppliedUnverified,
                    "topic_delete_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                    })),
        };
        var reads = new FakeKafkaAdministrationPort
        {
            Topics = (_, _, _) =>
            {
                var call = Interlocked.Increment(ref calls);
                IReadOnlyList<TopicSummary> topics =
                    call == 1
                        ? new[] { new TopicSummary("orders", 1, false) }
                        : Array.Empty<TopicSummary>();

                return Task.FromResult(
                    KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                        topics,
                        Observation()));
            },
        };

        var service = new TopicMutationExecutionService(
            mutations,
            reads,
            new TopicMutationVerificationPolicy(
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(50)));

        var result = await service.DeleteAsync(
            new TopicDeleteMutation("prod", "orders"));

        Assert.Equal(MutationExecutionResultKind.AppliedVerified, result.ResultKind);
        Assert.Equal("false", result.SafeEvidence!["resource.exists"]);
        Assert.Equal("observed", result.SafeEvidence["verification.state"]);
        Assert.True(calls >= 2);
    }

    private static ClusterMetadata Cluster(string clusterId, int brokerCount) =>
        new(
            clusterId,
            "kafka-cluster-id",
            ControllerBrokerId: 1,
            Enumerable.Range(1, brokerCount)
                .Select(id => new BrokerMetadata(id, $"broker-{id}", 9092, null, id == 1))
                .ToArray());

    private static TopicMetadata Topic(
        string topicName,
        int partitions,
        bool isInternal = false) =>
        new(
            topicName,
            isInternal,
            Enumerable.Range(0, partitions)
                .Select(id => new PartitionMetadata(
                    id,
                    LeaderBrokerId: 1,
                    ReplicaBrokerIds: new[] { 1 },
                    InSyncReplicaBrokerIds: new[] { 1 }))
                .ToArray());

    private static ObservationMetadata Observation()
    {
        var now = DateTimeOffset.UtcNow;
        return new ObservationMetadata(
            now,
            now.AddSeconds(5),
            now.AddSeconds(10),
            ObservationSource.Live);
    }

    private sealed class FakeKafkaAdministrationPort : IKafkaAdministrationPort
    {
        public Func<string, KafkaOperationContext, CancellationToken, Task<KafkaResult<ClusterMetadata>>>? Cluster { get; init; }
        public Func<string, KafkaOperationContext, CancellationToken, Task<KafkaResult<IReadOnlyList<TopicSummary>>>>? Topics { get; init; }
        public Func<string, string, KafkaOperationContext, CancellationToken, Task<KafkaResult<TopicMetadata>>>? Metadata { get; init; }
        public Func<string, string, KafkaOperationContext, CancellationToken, Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>>? Configuration { get; init; }

        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Cluster?.Invoke(clusterId, operation, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Topics?.Invoke(clusterId, operation, cancellationToken)
            ?? Task.FromResult(
                KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                    Array.Empty<TopicSummary>(),
                    Observation()));

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Metadata?.Invoke(clusterId, topicName, operation, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Configuration?.Invoke(clusterId, topicName, operation, cancellationToken)
            ?? throw new NotSupportedException();

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

    private sealed class FakeTopicMutationPort : ITopicMutationPort
    {
        public Func<TopicCreateMutation, CancellationToken, Task<MutationProviderResult>>? Create { get; init; }
        public Func<TopicAlterMutation, CancellationToken, Task<MutationProviderResult>>? Alter { get; init; }
        public Func<TopicIncreasePartitionsMutation, CancellationToken, Task<MutationProviderResult>>? Increase { get; init; }
        public Func<TopicDeleteMutation, CancellationToken, Task<MutationProviderResult>>? Delete { get; init; }

        public Task<MutationProviderResult> CreateTopicAsync(
            TopicCreateMutation request,
            CancellationToken cancellationToken = default) =>
            Create?.Invoke(request, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<MutationProviderResult> AlterTopicAsync(
            TopicAlterMutation request,
            CancellationToken cancellationToken = default) =>
            Alter?.Invoke(request, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<MutationProviderResult> IncreasePartitionsAsync(
            TopicIncreasePartitionsMutation request,
            CancellationToken cancellationToken = default) =>
            Increase?.Invoke(request, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<MutationProviderResult> DeleteTopicAsync(
            TopicDeleteMutation request,
            CancellationToken cancellationToken = default) =>
            Delete?.Invoke(request, cancellationToken)
            ?? throw new NotSupportedException();
    }
}
