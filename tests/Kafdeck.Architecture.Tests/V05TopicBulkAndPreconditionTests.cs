using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Topics;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05TopicBulkAndPreconditionTests
{
    [Fact]
    public async Task Bulk_create_materialization_is_finite_deduplicated_sorted_and_all_or_fail()
    {
        var reads = new FakeKafkaAdministrationPort
        {
            Cluster = (_, _, _) => Task.FromResult(
                KafkaResult<ClusterMetadata>.Success(
                    Cluster(),
                    Observation())),
            Topics = (_, _, _) => Task.FromResult(
                KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                    Array.Empty<TopicSummary>(),
                    Observation())),
        };
        var planner = new TopicMutationPlanner(reads);
        var materializer = new TopicBulkMutationMaterializer(planner);

        var result = await materializer.PlanCreatesAsync(
            new[]
            {
                Create("zeta"),
                Create("alpha"),
                Create("alpha"),
            });

        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Plan);
        Assert.Equal(
            new[] { "alpha", "zeta" },
            result.Plan!.Items.Select(item => item.Mutation.TopicName).ToArray());
        Assert.Equal(2, result.Plan.ResourceKeys.Count);
        Assert.Equal(2, result.Plan.AuthorizationTargets.Count);
        Assert.Equal(MutationRiskClass.Moderate, result.Plan.Risk.RiskClass);

        var conflictingDuplicate = await materializer.PlanCreatesAsync(
            new[]
            {
                Create("alpha", partitions: 1),
                Create("alpha", partitions: 2),
            });

        Assert.False(conflictingDuplicate.IsSuccess);
        Assert.Null(conflictingDuplicate.Plan);
        Assert.Equal(
            TopicMutationPlanningFailureCode.InvalidInput,
            conflictingDuplicate.Failure!.Code);
    }

    [Fact]
    public async Task Bulk_materialization_returns_no_partial_plan_when_any_target_is_stale()
    {
        var reads = new FakeKafkaAdministrationPort
        {
            Cluster = (_, _, _) => Task.FromResult(
                KafkaResult<ClusterMetadata>.Success(
                    Cluster(),
                    Observation())),
            Topics = (_, _, _) => Task.FromResult(
                KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                    new[] { new TopicSummary("blocked", false, 1, 1) },
                    Observation())),
        };

        var result = await new TopicBulkMutationMaterializer(
                new TopicMutationPlanner(reads))
            .PlanCreatesAsync(new[] { Create("allowed"), Create("blocked") });

        Assert.False(result.IsSuccess);
        Assert.Null(result.Plan);
        Assert.Equal("blocked", result.Failure!.TopicName);
        Assert.Equal(
            TopicMutationPlanningFailureCode.TopicAlreadyExists,
            result.Failure.Code);
    }

    [Fact]
    public async Task Create_precondition_validator_detects_state_change_without_becoming_authorization_guard()
    {
        var topicExists = false;
        var reads = new FakeKafkaAdministrationPort
        {
            Cluster = (_, _, _) => Task.FromResult(
                KafkaResult<ClusterMetadata>.Success(
                    Cluster(),
                    Observation())),
            Topics = (_, _, _) => Task.FromResult(
                KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                    topicExists
                        ? new[] { new TopicSummary("orders", false, 1, 1) }
                        : Array.Empty<TopicSummary>(),
                    Observation())),
        };

        var planner = new TopicMutationPlanner(reads);
        var planned = await planner.PlanCreateAsync(Create("orders"));
        Assert.True(planned.IsSuccess);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Plan!.Intent,
            planned.Plan.Risk,
            "v0.5-w33",
            DateTimeOffset.UtcNow.AddMinutes(5),
            DateTimeOffset.UtcNow,
            "w33-precondition");
        var validator = new TopicMutationPreconditionValidator(reads);

        var allowed = await validator.ValidateAsync(operation.Snapshot);
        Assert.Equal(MutationPreDispatchGuardOutcome.Allowed, allowed.Outcome);

        topicExists = true;
        var stale = await validator.ValidateAsync(operation.Snapshot);
        Assert.Equal(MutationPreDispatchGuardOutcome.StalePreview, stale.Outcome);
        Assert.Equal("topic_precondition_topic_now_exists", stale.ResultCode);

        Assert.DoesNotContain(
            typeof(IMutationPreDispatchGuard),
            typeof(TopicMutationPreconditionValidator).GetInterfaces());
    }

    [Fact]
    public void Topic_mutation_port_exposes_only_the_four_admitted_typed_operations()
    {
        var methods = typeof(ITopicMutationPort)
            .GetMethods()
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[]
            {
                "AlterTopicAsync",
                "CreateTopicAsync",
                "DeleteTopicAsync",
                "IncreasePartitionsAsync",
            },
            methods);

        var adapterMethods = typeof(Kafdeck.Infrastructure.Kafka.ConfluentKafkaTopicMutationAdapter)
            .GetMethods()
            .Where(method => method.DeclaringType ==
                             typeof(Kafdeck.Infrastructure.Kafka.ConfluentKafkaTopicMutationAdapter))
            .ToArray();

        Assert.DoesNotContain(
            adapterMethods,
            method =>
                method.Name.Contains("Command", StringComparison.OrdinalIgnoreCase) ||
                method.Name.Contains("Generic", StringComparison.OrdinalIgnoreCase) ||
                method.GetParameters().Any(parameter =>
                    parameter.ParameterType.FullName?.Contains(
                        "Confluent.Kafka.IAdminClient",
                        StringComparison.Ordinal) == true));
    }

    private static TopicCreateMutation Create(string topic, int partitions = 1) =>
        new(
            "prod",
            topic,
            partitions,
            1,
            new Dictionary<string, string>(StringComparer.Ordinal));

    private static ClusterMetadata Cluster() =>
        new(
            "prod",
            "test-cluster",
            1,
            new[]
            {
                new BrokerMetadata(1, "localhost", 9092, null),
                new BrokerMetadata(2, "localhost", 9093, null),
            });

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
            ?? throw new NotSupportedException();

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
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
    }
}
