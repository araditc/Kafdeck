using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Topics;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class TopicExplorerServiceTests
{
    [Fact]
    public async Task High_topic_count_pages_over_one_cached_snapshot_without_n_plus_one_reads()
    {
        var listCalls = 0;
        var metadataCalls = 0;
        var topics = Enumerable.Range(0, 1000)
            .Select(index => new TopicSummary(
                $"topic-{index:D4}",
                partitionCount: 12,
                isInternal: false,
                OfflinePartitionCount: index % 100 == 0 ? 1 : 0,
                UnderReplicatedPartitionCount: index % 50 == 0 ? 1 : 0))
            .ToArray();
        var port = new FakeKafkaAdministrationPort
        {
            Topics = (_, _, _) =>
            {
                Interlocked.Increment(ref listCalls);
                return Task.FromResult(KafkaResult<IReadOnlyList<TopicSummary>>.Success(topics, Observation()));
            },
            TopicMetadata = (_, _, _, _) =>
            {
                Interlocked.Increment(ref metadataCalls);
                throw new InvalidOperationException("List pagination must not issue per-row topic metadata reads.");
            },
        };
        var service = Service(port);

        var first = await service.ListTopicsAsync(
            "cluster-a",
            new TopicListRequest(PageSize: 200));
        var second = await service.ListTopicsAsync(
            "cluster-a",
            new TopicListRequest(PageSize: 200, Cursor: first.Value!.NextCursor));

        Assert.True(first.IsSuccess);
        Assert.True(second.IsSuccess);
        Assert.Equal(200, first.Value!.Items.Count);
        Assert.Equal(200, second.Value!.Items.Count);
        Assert.NotNull(first.Value.NextCursor);
        Assert.Equal(1, listCalls);
        Assert.Equal(0, metadataCalls);
        Assert.Equal("topic-0000", first.Value.Items[0].Name);
        Assert.Equal("topic-0200", second.Value.Items[0].Name);
    }

    [Fact]
    public async Task Search_is_applied_in_memory_to_the_cached_topic_snapshot()
    {
        var listCalls = 0;
        var topics = new[]
        {
            new TopicSummary("orders-eu", 3, false, 0, 0),
            new TopicSummary("orders-us", 3, false, 0, 1),
            new TopicSummary("payments", 6, false, 0, 0),
        };
        var port = new FakeKafkaAdministrationPort
        {
            Topics = (_, _, _) =>
            {
                Interlocked.Increment(ref listCalls);
                return Task.FromResult(KafkaResult<IReadOnlyList<TopicSummary>>.Success(topics, Observation()));
            },
        };
        var service = Service(port);

        var result = await service.ListTopicsAsync(
            "cluster-a",
            new TopicListRequest(Search: "ORDERS", PageSize: 50));

        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.Value!.Items.Count);
        Assert.Equal("orders-eu", result.Value.Items[0].Name);
        Assert.Equal(TopicAnomalyState.Healthy, result.Value.Items[0].AnomalyState);
        Assert.Equal(TopicAnomalyState.Degraded, result.Value.Items[1].AnomalyState);
        Assert.Equal(1, listCalls);
    }

    [Fact]
    public async Task Missing_bulk_anomaly_evidence_is_unknown_not_fabricated_as_healthy()
    {
        var port = new FakeKafkaAdministrationPort
        {
            Topics = (_, _, _) => Task.FromResult(KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                [new TopicSummary("orders", 3, false)],
                Observation())),
        };
        var service = Service(port);

        var result = await service.ListTopicsAsync("cluster-a");

        Assert.True(result.IsSuccess);
        Assert.Null(result.Value!.Items[0].OfflinePartitionCount);
        Assert.Null(result.Value.Items[0].UnderReplicatedPartitionCount);
        Assert.Equal(TopicAnomalyState.Unknown, result.Value.Items[0].AnomalyState);
    }

    [Fact]
    public async Task Topic_detail_projects_no_leader_and_under_replicated_partitions()
    {
        var metadata = new TopicMetadata(
            "orders",
            false,
            [
                new PartitionMetadata(0, null, [1, 2, 3], [1, 2]),
                new PartitionMetadata(1, 2, [1, 2, 3], [1, 2]),
                new PartitionMetadata(2, 3, [1, 2, 3], [1, 2, 3]),
            ]);
        var port = new FakeKafkaAdministrationPort
        {
            TopicMetadata = (_, _, _, _) => Task.FromResult(KafkaResult<TopicMetadata>.Success(
                metadata,
                Observation())),
        };
        var service = Service(port);

        var result = await service.GetTopicAsync("cluster-a", "orders");

        Assert.True(result.IsSuccess);
        Assert.Equal(1, result.Value!.OfflinePartitionCount);
        Assert.Equal(2, result.Value.UnderReplicatedPartitionCount);
        Assert.Equal(TopicAnomalyState.Degraded, result.Value.AnomalyState);
        Assert.Equal(PartitionHealthState.NoLeader, result.Value.Partitions[0].Health);
        Assert.Equal(PartitionHealthState.UnderReplicated, result.Value.Partitions[1].Health);
        Assert.Equal(PartitionHealthState.Healthy, result.Value.Partitions[2].Health);
        Assert.Equal(new[] { 3 }, result.Value.Partitions[1].OutOfSyncReplicaBrokerIds);
    }

    [Fact]
    public async Task Topic_configuration_authorization_denial_remains_explicit()
    {
        var port = new FakeKafkaAdministrationPort
        {
            TopicConfiguration = (_, _, _, _) => Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>.Failed(
                    new KafkaFailure(
                        KafkaFailureCategory.Unauthorized,
                        "topic_config_denied",
                        "Topic configuration access denied.",
                        false),
                    Observation())),
        };
        var service = Service(port);

        var result = await service.GetTopicConfigurationAsync("cluster-a", "orders");

        Assert.False(result.IsSuccess);
        Assert.Null(result.Value);
        Assert.Equal(KafkaFailureCategory.Unauthorized, result.Failure?.Category);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(201)]
    public async Task Topic_page_size_is_bounded(int pageSize)
    {
        var service = Service(new FakeKafkaAdministrationPort());

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            service.ListTopicsAsync("cluster-a", new TopicListRequest(PageSize: pageSize)));
    }

    private static TopicExplorerService Service(FakeKafkaAdministrationPort port)
    {
        var policy = new KafkaSnapshotPolicy(
            perClusterConcurrency: 2,
            globalConcurrency: 4,
            operationDeadline: TimeSpan.FromSeconds(2),
            clusterMetadataTtl: TimeSpan.FromSeconds(5),
            topicMetadataTtl: TimeSpan.FromSeconds(10));
        return new TopicExplorerService(port, new KafkaSnapshotCoordinator(policy), policy);
    }

    private static ObservationMetadata Observation()
    {
        var now = DateTimeOffset.UtcNow;
        return new ObservationMetadata(
            now,
            now + TimeSpan.FromSeconds(10),
            now + TimeSpan.FromSeconds(20),
            ObservationSource.Live);
    }

    private sealed class FakeKafkaAdministrationPort : IKafkaAdministrationPort
    {
        public Func<string, KafkaOperationContext, CancellationToken, Task<KafkaResult<IReadOnlyList<TopicSummary>>>>? Topics { get; init; }
        public Func<string, string, KafkaOperationContext, CancellationToken, Task<KafkaResult<TopicMetadata>>>? TopicMetadata { get; init; }
        public Func<string, string, KafkaOperationContext, CancellationToken, Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>>? TopicConfiguration { get; init; }

        public Task<KafkaResult<ClusterMetadata>> GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Topics?.Invoke(clusterId, operation, cancellationToken)
            ?? Task.FromResult(KafkaResult<IReadOnlyList<TopicSummary>>.Success(
                Array.Empty<TopicSummary>(),
                Observation()));

        public Task<KafkaResult<TopicMetadata>> GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            TopicMetadata?.Invoke(clusterId, topicName, operation, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            TopicConfiguration?.Invoke(clusterId, topicName, operation, cancellationToken)
            ?? throw new NotSupportedException();

        public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
            string clusterId,
            int brokerId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
