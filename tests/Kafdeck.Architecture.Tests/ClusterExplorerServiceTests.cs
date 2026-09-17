using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Clusters;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ClusterExplorerServiceTests
{
    [Fact]
    public async Task Cluster_metadata_authorization_denial_is_unknown_not_unavailable()
    {
        var port = new FakeKafkaAdministrationPort
        {
            ClusterMetadata = (_, _, _) => Task.FromResult(KafkaResult<ClusterMetadata>.Failed(
                Failure(KafkaFailureCategory.Unauthorized, "metadata_denied", false),
                Observation())),
        };
        var policy = Policy();
        var service = new ClusterExplorerService(port, new KafkaSnapshotCoordinator(policy), policy);

        var projection = await service.GetClusterAsync("cluster-a");

        Assert.Equal(ClusterHealthState.Unknown, projection.Health);
        Assert.Equal(KafkaFailureCategory.Unauthorized, projection.Failure?.Category);
        Assert.Contains(projection.Limitations, limitation =>
            limitation.Capability == KafkaCapabilityKind.ClusterMetadata &&
            limitation.State == KafkaCapabilityState.Unauthorized);
        Assert.DoesNotContain(projection.HealthReasons, reason =>
            reason.Code == ClusterHealthReasonCode.KafkaUnavailable);
    }

    [Fact]
    public async Task Capability_denial_is_a_limitation_without_changing_healthy_metadata_to_outage()
    {
        var port = new FakeKafkaAdministrationPort
        {
            ClusterMetadata = (_, _, _) => Task.FromResult(KafkaResult<ClusterMetadata>.Success(
                Metadata("cluster-a"),
                Observation())),
            Capabilities = (_, _, _) => Task.FromResult(KafkaResult<KafkaCapabilities>.Success(
                new KafkaCapabilities(
                [
                    new KafkaCapabilityStatus(KafkaCapabilityKind.ClusterMetadata, KafkaCapabilityState.Available),
                    new KafkaCapabilityStatus(
                        KafkaCapabilityKind.BrokerConfiguration,
                        KafkaCapabilityState.Unauthorized,
                        "Broker configuration ACL denied."),
                ]),
                Observation())),
        };
        var policy = Policy();
        var service = new ClusterExplorerService(port, new KafkaSnapshotCoordinator(policy), policy);

        var projection = await service.GetClusterAsync("cluster-a");

        Assert.Equal(ClusterHealthState.Healthy, projection.Health);
        Assert.Null(projection.Failure);
        Assert.Single(projection.Brokers);
        Assert.Contains(projection.Limitations, limitation =>
            limitation.Capability == KafkaCapabilityKind.BrokerConfiguration &&
            limitation.State == KafkaCapabilityState.Unauthorized);
    }

    [Fact]
    public async Task Retryable_refresh_failure_serves_stale_metadata_as_degraded()
    {
        var calls = 0;
        var port = new FakeKafkaAdministrationPort
        {
            ClusterMetadata = (_, _, _) =>
            {
                var call = Interlocked.Increment(ref calls);
                if (call == 1)
                {
                    var oldObservation = Observation(DateTimeOffset.UtcNow - TimeSpan.FromMilliseconds(1500));
                    return Task.FromResult(KafkaResult<ClusterMetadata>.Success(Metadata("cluster-a"), oldObservation));
                }

                return Task.FromResult(KafkaResult<ClusterMetadata>.Failed(
                    Failure(KafkaFailureCategory.Unavailable, "temporarily_unavailable", true),
                    Observation()));
            },
            Capabilities = (_, _, _) => Task.FromResult(KafkaResult<KafkaCapabilities>.Success(
                new KafkaCapabilities(Array.Empty<KafkaCapabilityStatus>()),
                Observation())),
        };
        var policy = Policy(clusterMetadataTtl: TimeSpan.FromSeconds(1));
        var service = new ClusterExplorerService(port, new KafkaSnapshotCoordinator(policy), policy);

        var initial = await service.GetClusterAsync("cluster-a");
        var stale = await service.GetClusterAsync("cluster-a");

        Assert.Equal(ClusterHealthState.Healthy, initial.Health);
        Assert.Equal(ClusterHealthState.Degraded, stale.Health);
        Assert.Equal(ObservationSource.StaleSnapshot, stale.Observation.Source);
        Assert.Contains(stale.HealthReasons, reason => reason.Code == ClusterHealthReasonCode.StaleObservation);
        Assert.Equal(initial.Observation.ObservedAtUtc, stale.Observation.ObservedAtUtc);
        Assert.Equal(4, calls);
    }

    [Fact]
    public async Task Broker_configuration_authorization_denial_remains_an_explicit_failure()
    {
        var port = new FakeKafkaAdministrationPort
        {
            BrokerConfiguration = (_, _, _, _) => Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>.Failed(
                    Failure(KafkaFailureCategory.Unauthorized, "broker_config_denied", false),
                    Observation())),
        };
        var policy = Policy();
        var service = new ClusterExplorerService(port, new KafkaSnapshotCoordinator(policy), policy);

        var result = await service.GetBrokerConfigurationAsync("cluster-a", 1);

        Assert.False(result.IsSuccess);
        Assert.Null(result.Value);
        Assert.Equal(KafkaFailureCategory.Unauthorized, result.Failure?.Category);
    }

    private static KafkaSnapshotPolicy Policy(TimeSpan? clusterMetadataTtl = null) =>
        new(
            perClusterConcurrency: 2,
            globalConcurrency: 4,
            operationDeadline: TimeSpan.FromSeconds(2),
            clusterMetadataTtl: clusterMetadataTtl ?? TimeSpan.FromSeconds(5),
            topicMetadataTtl: TimeSpan.FromSeconds(10));

    private static ClusterMetadata Metadata(string clusterId) =>
        new(
            clusterId,
            "kafka-id",
            1,
            [new BrokerMetadata(1, "broker-1", 9092, null, true)]);

    private static ObservationMetadata Observation(DateTimeOffset? observedAt = null)
    {
        var observed = observedAt ?? DateTimeOffset.UtcNow;
        return new ObservationMetadata(
            observed,
            observed + TimeSpan.FromSeconds(5),
            observed + TimeSpan.FromSeconds(10),
            ObservationSource.Live);
    }

    private static KafkaFailure Failure(
        KafkaFailureCategory category,
        string code,
        bool retryable) =>
        new(category, code, "Safe failure.", retryable);

    private sealed class FakeKafkaAdministrationPort : IKafkaAdministrationPort
    {
        public Func<string, KafkaOperationContext, CancellationToken, Task<KafkaResult<ClusterMetadata>>>? ClusterMetadata { get; init; }
        public Func<string, KafkaOperationContext, CancellationToken, Task<KafkaResult<KafkaCapabilities>>>? Capabilities { get; init; }
        public Func<string, int, KafkaOperationContext, CancellationToken, Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>>? BrokerConfiguration { get; init; }

        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            ClusterMetadata?.Invoke(clusterId, operation, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
            string clusterId,
            int brokerId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            BrokerConfiguration?.Invoke(clusterId, brokerId, operation, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Capabilities?.Invoke(clusterId, operation, cancellationToken)
            ?? Task.FromResult(KafkaResult<KafkaCapabilities>.Success(
                new KafkaCapabilities(Array.Empty<KafkaCapabilityStatus>()),
                Observation()));
    }
}
