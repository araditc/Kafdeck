using Confluent.Kafka;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaRecordReadAdapterTests
{
    [Fact]
    public void Record_consumer_configuration_disables_offset_mutation_and_topic_creation()
    {
        var profile = PlaintextProfile("records");
        var config = KafkaClientConfigFactory.CreateRecordConsumer(profile, new SecretResolver());

        Assert.False(config.EnableAutoCommit ?? true);
        Assert.False(config.EnableAutoOffsetStore ?? true);
        Assert.False(config.AllowAutoCreateTopics ?? true);
        Assert.True(config.EnablePartitionEof ?? false);
        Assert.Equal(AutoOffsetReset.Error, config.AutoOffsetReset);
        Assert.Equal((int)RecordOperationBudget.HardMaxRawBytes, config.FetchMaxBytes.GetValueOrDefault());
        Assert.Equal((int)RecordOperationBudget.HardMaxRawBytes, config.MaxPartitionFetchBytes.GetValueOrDefault());
    }

    [Fact]
    public async Task Unknown_cluster_fails_without_creating_consumer()
    {
        using var adapter = new ConfluentKafkaRecordReadAdapter([], new SecretResolver());
        var result = await adapter.ReadPageAsync(
            Request("missing"),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)),
            CancellationToken.None);

        AssertFailure(result, KafkaFailureCategory.InvalidConfiguration, "cluster_not_configured");
    }

    [Fact]
    public async Task Cancelled_request_fails_before_consumer_creation()
    {
        using var adapter = new ConfluentKafkaRecordReadAdapter(
            [PlaintextProfile("records")],
            new SecretResolver());

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var result = await adapter.ReadPageAsync(
            Request("records"),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)),
            cancellation.Token);

        AssertFailure(result, KafkaFailureCategory.Cancelled, "operation_cancelled");
    }

    [Fact]
    public async Task Expired_operation_fails_before_consumer_creation()
    {
        using var adapter = new ConfluentKafkaRecordReadAdapter(
            [PlaintextProfile("records")],
            new SecretResolver());

        var result = await adapter.ReadPageAsync(
            Request("records"),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddMilliseconds(-1)),
            CancellationToken.None);

        AssertFailure(result, KafkaFailureCategory.Timeout, "deadline_exceeded");
    }

    [Fact]
    public void Record_reader_source_has_no_commit_subscribe_or_producer_path()
    {
        var root = FindRepositoryRoot();
        var source = File.ReadAllText(Path.Combine(
            root,
            "src",
            "backend",
            "Infrastructure",
            "Kafdeck.Infrastructure.Kafka",
            "ConfluentKafkaRecordReadAdapter.cs"));

        Assert.DoesNotContain(".Commit(", source, StringComparison.Ordinal);
        Assert.DoesNotContain(".StoreOffset(", source, StringComparison.Ordinal);
        Assert.DoesNotContain(".Subscribe(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ProducerBuilder<", source, StringComparison.Ordinal);
    }

    private static RecordReadRequest Request(string clusterId) =>
        new(
            clusterId,
            "topic",
            0,
            RecordAnchor.Earliest(),
            RecordReadDirection.Forward,
            RecordOperationBudget.Default);

    private static ClusterProfile PlaintextProfile(string id) =>
        new(id, ["localhost:1"], KafkaSecurityProtocol.Plaintext, null, null);

    private static void AssertFailure(
        KafkaResult<RecordReadBatch> result,
        KafkaFailureCategory category,
        string code)
    {
        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(category, result.Failure.Category);
        Assert.Equal(code, result.Failure.Code);
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Unable to locate Kafdeck repository root.");
    }
}
