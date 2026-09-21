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


    [Fact]
    public async Task Slow_cluster_waiters_do_not_consume_global_capacity()
    {
        using var releaseSlow = new ManualResetEventSlim(false);
        var slowStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = new TestRecordConsumerFactory(
            slowClusterId: "slow",
            slowStarted,
            releaseSlow);

        using var adapter = new ConfluentKafkaRecordReadAdapter(
            [PlaintextProfile("slow"), PlaintextProfile("fast")],
            factory,
            new KafkaRecordReadConcurrencyOptions(perClusterLimit: 1, globalLimit: 2));

        var slowFirst = adapter.ReadPageAsync(
            Request("slow"),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)),
            CancellationToken.None);

        await slowStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));

        var slowSecond = adapter.ReadPageAsync(
            Request("slow"),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)),
            CancellationToken.None);

        var fast = adapter.ReadPageAsync(
            Request("fast"),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)),
            CancellationToken.None);

        var fastResult = await fast.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(fastResult.IsSuccess, fastResult.Failure?.SafeMessage);

        releaseSlow.Set();

        var firstResult = await slowFirst.WaitAsync(TimeSpan.FromSeconds(3));
        var secondResult = await slowSecond.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.True(firstResult.IsSuccess, firstResult.Failure?.SafeMessage);
        Assert.True(secondResult.IsSuccess, secondResult.Failure?.SafeMessage);
    }

    [Fact]
    public async Task Setup_polling_observes_request_cancellation()
    {
        using var neverRelease = new ManualResetEventSlim(false);
        var slowStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = new TestRecordConsumerFactory(
            slowClusterId: "slow",
            slowStarted,
            neverRelease,
            keepEverySlowSessionBlocked: true);

        using var adapter = new ConfluentKafkaRecordReadAdapter(
            [PlaintextProfile("slow")],
            factory,
            new KafkaRecordReadConcurrencyOptions(perClusterLimit: 1, globalLimit: 1));

        using var cancellation = new CancellationTokenSource();

        var read = adapter.ReadPageAsync(
            Request("slow"),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)),
            cancellation.Token);

        await slowStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));
        cancellation.Cancel();

        var result = await read.WaitAsync(TimeSpan.FromSeconds(3));

        AssertFailure(result, KafkaFailureCategory.Cancelled, "operation_cancelled");
    }

    [Fact]
    public async Task Budget_expiry_after_watermark_setup_does_not_replace_requested_anchor()
    {
        var time = new AdvancingTimeProvider(
            new DateTimeOffset(2026, 9, 20, 20, 0, 0, TimeSpan.Zero));

        var factory = new AdvancingSetupConsumerFactory(
            () => time.Advance(TimeSpan.FromSeconds(2)));

        var budget = new RecordOperationBudget(
            maxRecords: 10,
            maxRawBytes: 1024,
            maxProjectedBytes: 1024,
            maxDuration: TimeSpan.FromSeconds(1),
            maxRecordsPerSecond: 10);

        using var adapter = new ConfluentKafkaRecordReadAdapter(
            [PlaintextProfile("records")],
            factory,
            new KafkaRecordReadConcurrencyOptions(perClusterLimit: 1, globalLimit: 1),
            time);

        var result = await adapter.ReadPageAsync(
            new RecordReadRequest(
                "records",
                "topic",
                0,
                RecordAnchor.Earliest(),
                RecordReadDirection.Forward,
                budget),
            new KafkaOperationContext(time.GetUtcNow().AddSeconds(10)),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);
        Assert.Equal(RecordBudgetOutcome.DurationLimit, result.Value.BudgetOutcome);
        Assert.Empty(result.Value.Records);
        Assert.Equal(0, result.Value.LowWatermark);
        Assert.Equal(10, result.Value.HighWatermark);
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


    private sealed class TestRecordConsumerFactory : IKafkaRecordConsumerFactory
    {
        private readonly string _slowClusterId;
        private readonly TaskCompletionSource _slowStarted;
        private readonly ManualResetEventSlim _releaseSlow;
        private readonly bool _keepEverySlowSessionBlocked;
        private int _slowSessionCount;

        public TestRecordConsumerFactory(
            string slowClusterId,
            TaskCompletionSource slowStarted,
            ManualResetEventSlim releaseSlow,
            bool keepEverySlowSessionBlocked = false)
        {
            _slowClusterId = slowClusterId;
            _slowStarted = slowStarted;
            _releaseSlow = releaseSlow;
            _keepEverySlowSessionBlocked = keepEverySlowSessionBlocked;
        }

        public IKafkaRecordConsumerSession Create(ClusterProfile profile)
        {
            if (!string.Equals(profile.Id, _slowClusterId, StringComparison.Ordinal))
            {
                return new ImmediateEmptySession();
            }

            var ordinal = Interlocked.Increment(ref _slowSessionCount);
            return ordinal == 1 || _keepEverySlowSessionBlocked
                ? new BlockingSetupSession(_slowStarted, _releaseSlow)
                : new ImmediateEmptySession();
        }
    }

    private sealed class BlockingSetupSession : IKafkaRecordConsumerSession
    {
        private readonly TaskCompletionSource _started;
        private readonly ManualResetEventSlim _release;

        public BlockingSetupSession(
            TaskCompletionSource started,
            ManualResetEventSlim release)
        {
            _started = started;
            _release = release;
        }

        public WatermarkOffsets QueryWatermarkOffsets(
            TopicPartition topicPartition,
            TimeSpan timeout)
        {
            _started.TrySetResult();

            if (!_release.Wait(timeout))
            {
                throw new KafkaException(new Error(ErrorCode.Local_TimedOut));
            }

            return new WatermarkOffsets(new Offset(0), new Offset(0));
        }

        public Offset OffsetForTimestamp(
            TopicPartition topicPartition,
            DateTimeOffset timestampUtc,
            TimeSpan timeout) => Offset.Unset;

        public void Assign(TopicPartitionOffset offset)
        {
        }

        public ConsumeResult<byte[], byte[]>? Consume(TimeSpan timeout) => null;

        public void Dispose()
        {
        }
    }

    private sealed class ImmediateEmptySession : IKafkaRecordConsumerSession
    {
        public WatermarkOffsets QueryWatermarkOffsets(
            TopicPartition topicPartition,
            TimeSpan timeout) =>
            new(new Offset(0), new Offset(0));

        public Offset OffsetForTimestamp(
            TopicPartition topicPartition,
            DateTimeOffset timestampUtc,
            TimeSpan timeout) => Offset.Unset;

        public void Assign(TopicPartitionOffset offset)
        {
        }

        public ConsumeResult<byte[], byte[]>? Consume(TimeSpan timeout) => null;

        public void Dispose()
        {
        }
    }

    private sealed class AdvancingSetupConsumerFactory : IKafkaRecordConsumerFactory
    {
        private readonly Action _onWatermarkQuery;

        public AdvancingSetupConsumerFactory(Action onWatermarkQuery)
        {
            _onWatermarkQuery = onWatermarkQuery;
        }

        public IKafkaRecordConsumerSession Create(ClusterProfile profile) =>
            new AdvancingSetupSession(_onWatermarkQuery);
    }

    private sealed class AdvancingSetupSession : IKafkaRecordConsumerSession
    {
        private readonly Action _onWatermarkQuery;

        public AdvancingSetupSession(Action onWatermarkQuery)
        {
            _onWatermarkQuery = onWatermarkQuery;
        }

        public WatermarkOffsets QueryWatermarkOffsets(
            TopicPartition topicPartition,
            TimeSpan timeout)
        {
            _onWatermarkQuery();
            return new WatermarkOffsets(new Offset(0), new Offset(10));
        }

        public Offset OffsetForTimestamp(
            TopicPartition topicPartition,
            DateTimeOffset timestampUtc,
            TimeSpan timeout) => new(0);

        public void Assign(TopicPartitionOffset offset)
        {
        }

        public ConsumeResult<byte[], byte[]>? Consume(TimeSpan timeout) => null;

        public void Dispose()
        {
        }
    }

    private sealed class AdvancingTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;

        public AdvancingTimeProvider(DateTimeOffset initial)
        {
            _now = initial;
        }

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan duration) => _now += duration;
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
