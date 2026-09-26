using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W47TransferSourceReaderTests
{
    [Fact]
    public async Task Reads_exact_next_checkpoint_with_frozen_range_and_batch_budget()
    {
        var plan = Plan(start: 10, end: 13, maxBatchRecords: 2);
        var progress = FleetTransferProgress
            .Create(plan.PlanFingerprint, new long[] { 10 })
            .Snapshot;
        var port = new StubReadPort(request =>
        {
            Assert.Equal("source", request.ClusterId);
            Assert.Equal("orders", request.TopicName);
            Assert.Equal(1, request.Partition);
            Assert.Equal(RecordAnchorKind.Offset, request.Anchor.Kind);
            Assert.Equal(10, request.Anchor.Offset);
            Assert.Equal(2, request.Budget.MaxRecords);

            return Success(new RecordReadBatch(
                new[]
                {
                    Record(10),
                    Record(11),
                },
                LowWatermark: 0,
                HighWatermark: 13,
                FirstReturnedOffset: 10,
                LastReturnedOffset: 11,
                NextAnchor: RecordAnchor.AtOffset(12),
                PreviousAnchor: null,
                BudgetOutcome: RecordBudgetOutcome.RecordLimit));
        });
        var reader = new ClusterTransferSourceReader(port);

        var result = await reader.ReadNextAsync(
            plan,
            0,
            progress,
            Operation());

        Assert.True(result.IsSuccess);
        Assert.Equal(ClusterTransferSourceBatchState.Ready, result.State);
        Assert.Equal(new long[] { 10, 11 }, result.Records.Select(record => record.Offset));
        Assert.Equal(1, port.CallCount);
    }

    [Fact]
    public async Task Final_contiguous_batch_reports_mapping_complete()
    {
        var plan = Plan(start: 10, end: 12, maxBatchRecords: 10);
        var progress = FleetTransferProgress
            .Create(plan.PlanFingerprint, new long[] { 10 })
            .Snapshot;
        var port = new StubReadPort(_ => Success(new RecordReadBatch(
            new[]
            {
                Record(10),
                Record(11),
            },
            0,
            12,
            10,
            11,
            null,
            null,
            RecordBudgetOutcome.Complete)));
        var reader = new ClusterTransferSourceReader(port);

        var result = await reader.ReadNextAsync(
            plan,
            0,
            progress,
            Operation());

        Assert.Equal(ClusterTransferSourceBatchState.MappingComplete, result.State);
        Assert.Equal(2, result.Records.Count);
    }

    [Fact]
    public async Task Retention_gap_fails_before_returning_records()
    {
        var plan = Plan(start: 10, end: 12);
        var progress = FleetTransferProgress
            .Create(plan.PlanFingerprint, new long[] { 10 })
            .Snapshot;
        var port = new StubReadPort(_ => Success(new RecordReadBatch(
            new[] { Record(11) },
            LowWatermark: 11,
            HighWatermark: 12,
            FirstReturnedOffset: 11,
            LastReturnedOffset: 11,
            NextAnchor: null,
            PreviousAnchor: null,
            BudgetOutcome: RecordBudgetOutcome.Complete)));
        var reader = new ClusterTransferSourceReader(port);

        var result = await reader.ReadNextAsync(
            plan,
            0,
            progress,
            Operation());

        Assert.Equal(ClusterTransferSourceBatchState.Failed, result.State);
        Assert.Equal("cluster_transfer_source_retention_gap", result.FailureCode);
        Assert.Empty(result.Records);
    }

    [Fact]
    public async Task Missing_offset_inside_live_range_is_never_silently_skipped()
    {
        var plan = Plan(start: 10, end: 13);
        var progress = FleetTransferProgress
            .Create(plan.PlanFingerprint, new long[] { 10 })
            .Snapshot;
        var port = new StubReadPort(_ => Success(new RecordReadBatch(
            new[]
            {
                Record(10),
                Record(12),
            },
            LowWatermark: 0,
            HighWatermark: 13,
            FirstReturnedOffset: 10,
            LastReturnedOffset: 12,
            NextAnchor: null,
            PreviousAnchor: null,
            BudgetOutcome: RecordBudgetOutcome.Complete)));
        var reader = new ClusterTransferSourceReader(port);

        var result = await reader.ReadNextAsync(
            plan,
            0,
            progress,
            Operation());

        Assert.Equal(ClusterTransferSourceBatchState.Failed, result.State);
        Assert.Equal("cluster_transfer_source_offset_gap", result.FailureCode);
        Assert.Empty(result.Records);
    }

    [Fact]
    public async Task Unresolved_destination_dispatch_blocks_new_source_read()
    {
        var plan = Plan(start: 10, end: 12);
        var transfer = FleetTransferProgress.Create(
            plan.PlanFingerprint,
            new long[] { 10 });
        var pending = transfer.ReserveBeforeDispatch(
            0,
            10,
            3,
            4,
            DateTimeOffset.Parse("2026-09-26T08:00:00Z"));
        transfer.MarkDispatchStarted(
            pending.BatchId,
            DateTimeOffset.Parse("2026-09-26T08:00:01Z"));

        var port = new StubReadPort(_ => throw new InvalidOperationException(
            "Source read must not occur while one destination dispatch is unresolved."));
        var reader = new ClusterTransferSourceReader(port);

        var result = await reader.ReadNextAsync(
            plan,
            0,
            transfer.Snapshot,
            Operation());

        Assert.Equal(ClusterTransferSourceBatchState.Failed, result.State);
        Assert.Equal(
            "cluster_transfer_unresolved_dispatch_blocks_source_read",
            result.FailureCode);
        Assert.Equal(0, port.CallCount);
    }

    [Fact]
    public async Task Frozen_end_beyond_current_high_watermark_is_stale_not_future_wait()
    {
        var plan = Plan(start: 10, end: 20);
        var progress = FleetTransferProgress
            .Create(plan.PlanFingerprint, new long[] { 10 })
            .Snapshot;
        var port = new StubReadPort(_ => Success(new RecordReadBatch(
            new[] { Record(10) },
            LowWatermark: 0,
            HighWatermark: 15,
            FirstReturnedOffset: 10,
            LastReturnedOffset: 10,
            NextAnchor: RecordAnchor.AtOffset(11),
            PreviousAnchor: null,
            BudgetOutcome: RecordBudgetOutcome.RecordLimit)));
        var reader = new ClusterTransferSourceReader(port);

        var result = await reader.ReadNextAsync(
            plan,
            0,
            progress,
            Operation());

        Assert.Equal(ClusterTransferSourceBatchState.Failed, result.State);
        Assert.Equal("cluster_transfer_source_range_unavailable", result.FailureCode);
        Assert.Empty(result.Records);
    }

    private static ClusterTransferPlan Plan(
        long start,
        long end,
        int maxBatchRecords = 10)
    {
        var source = new ClusterTransferEndpoint(
            "source",
            "source-v1",
            "physical-source");
        var destination = new ClusterTransferEndpoint(
            "destination",
            "destination-v1",
            "physical-destination");
        var mappings = new[]
        {
            new ClusterTransferMapping(
                "orders",
                1,
                "orders-copy",
                3,
                start,
                end,
                new string('a', 64),
                new string('b', 64)),
        };
        var budget = new ClusterTransferBudget(
            maxBatchRecords: maxBatchRecords,
            maxBatchBytes: 1024 * 1024,
            maxTotalRecords: 100,
            maxTotalBytes: 8 * 1024 * 1024,
            maxDuration: TimeSpan.FromMinutes(5),
            maxRecordsPerSecond: 100,
            maxBytesPerSecond: 1024 * 1024);
        var policy = new ClusterTransferDataPolicy(
            "none",
            1,
            new string('c', 64));

        return new ClusterTransferPlan(
            source,
            destination,
            mappings,
            budget,
            policy,
            ClusterTransferPolicy.PlanFingerprint(
                source,
                destination,
                mappings,
                budget,
                policy));
    }

    private static KafkaRawRecord Record(long offset) =>
        new(
            offset,
            null,
            (ReadOnlyMemory<byte>?)null,
            new ReadOnlyMemory<byte>(new byte[] { 1, 2 }),
            Array.Empty<KafkaRecordHeader>());

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10));

    private static KafkaResult<RecordReadBatch> Success(RecordReadBatch batch)
    {
        var now = DateTimeOffset.UtcNow;
        return KafkaResult<RecordReadBatch>.Success(
            batch,
            new ObservationMetadata(
                now,
                now.AddSeconds(1),
                now.AddSeconds(2),
                ObservationSource.Live));
    }

    private sealed class StubReadPort : IKafkaRecordReadPort
    {
        private readonly Func<RecordReadRequest, KafkaResult<RecordReadBatch>> _result;

        public StubReadPort(
            Func<RecordReadRequest, KafkaResult<RecordReadBatch>> result)
        {
            _result = result;
        }

        public int CallCount { get; private set; }

        public Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
            RecordReadRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(_result(request));
        }
    }
}
