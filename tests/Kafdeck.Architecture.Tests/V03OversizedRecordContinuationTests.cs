using Confluent.Kafka;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V03OversizedRecordContinuationTests
{
    [Fact]
    public async Task Oversized_first_record_returns_continuation_past_rejected_offset()
    {
        var profile = new ClusterProfile(
            "records",
            ["localhost:1"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);
        var session = new OversizedRecordSession();
        using var adapter = new ConfluentKafkaRecordReadAdapter(
            [profile],
            new SingleSessionFactory(session),
            new KafkaRecordReadConcurrencyOptions(perClusterLimit: 1, globalLimit: 1));
        var budget = new RecordOperationBudget(
            maxRecords: 10,
            maxRawBytes: 4,
            maxProjectedBytes: 1024,
            maxDuration: TimeSpan.FromSeconds(2),
            maxRecordsPerSecond: 10);

        var result = await adapter.ReadPageAsync(
            new RecordReadRequest(
                "records",
                "topic",
                0,
                RecordAnchor.AtOffset(0),
                RecordReadDirection.Forward,
                budget),
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(5)),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);
        Assert.Empty(result.Value.Records);
        Assert.Equal(RecordBudgetOutcome.RawByteLimit, result.Value.BudgetOutcome);
        Assert.True(result.Value.NextAnchor.HasValue);
        Assert.Equal(RecordAnchorKind.Offset, result.Value.NextAnchor.Value.Kind);
        Assert.Equal(1, result.Value.NextAnchor.Value.Offset);
        Assert.Equal(0, result.Value.LowWatermark);
        Assert.Equal(2, result.Value.HighWatermark);
    }

    private sealed class SingleSessionFactory : IKafkaRecordConsumerFactory
    {
        private readonly IKafkaRecordConsumerSession _session;

        public SingleSessionFactory(IKafkaRecordConsumerSession session)
        {
            _session = session;
        }

        public IKafkaRecordConsumerSession Create(ClusterProfile profile) => _session;
    }

    private sealed class OversizedRecordSession : IKafkaRecordConsumerSession
    {
        private bool _consumed;

        public WatermarkOffsets QueryWatermarkOffsets(
            TopicPartition topicPartition,
            TimeSpan timeout) =>
            new(new Offset(0), new Offset(2));

        public Offset OffsetForTimestamp(
            TopicPartition topicPartition,
            DateTimeOffset timestampUtc,
            TimeSpan timeout) => new(0);

        public void Assign(TopicPartitionOffset offset)
        {
        }

        public ConsumeResult<byte[], byte[]>? Consume(TimeSpan timeout)
        {
            if (_consumed)
            {
                return new ConsumeResult<byte[], byte[]>
                {
                    TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 2),
                    IsPartitionEOF = true,
                    Message = new Message<byte[], byte[]>(),
                };
            }

            _consumed = true;
            return new ConsumeResult<byte[], byte[]>
            {
                TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 0),
                Message = new Message<byte[], byte[]>
                {
                    Key = [],
                    Value = new byte[32],
                    Timestamp = new Timestamp(DateTime.UtcNow),
                },
            };
        }

        public void Dispose()
        {
        }
    }
}
