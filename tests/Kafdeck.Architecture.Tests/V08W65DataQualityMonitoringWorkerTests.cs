using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W65DataQualityMonitoringWorkerTests
{
    [Fact]
    public async Task Worker_requires_authorization_before_record_read()
    {
        var store =
            new FakeStore(
                [Snapshot()]);
        var read =
            new FakeReadPort();
        var evaluator =
            new FakeEvaluator();
        var authorization =
            new FakeAuthorization(
                authorized: false);

        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                evaluator,
                authorization,
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 4));

        var result =
            await worker.RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        Assert.Equal(1, result.AuthorizationDenied);
        Assert.Equal(0, result.EvaluationsAttempted);
        Assert.Equal(0, read.Calls);
        Assert.Equal(0, evaluator.Calls);
        Assert.Empty(store.Appended);
    }

    [Fact]
    public async Task Worker_reads_bounded_tail_and_persists_only_aggregate_result()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var read =
            new FakeReadPort(
                new RecordReadBatch(
                    [
                        new KafkaRawRecord(
                            10,
                            now.AddSeconds(-1),
                            null,
                            new byte[] { 123, 125 },
                            []),
                    ],
                    0,
                    11,
                    10,
                    10,
                    null,
                    null,
                    RecordBudgetOutcome.Complete));
        var evaluator =
            new FakeEvaluator(
                now);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                evaluator,
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 4),
                new FixedTimeProvider(now));

        var result =
            await worker.RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        Assert.Equal(1, read.Calls);
        Assert.Equal(1, evaluator.Calls);
        Assert.Equal(1, result.EvaluationsPersisted);
        Assert.Single(store.Appended);
        Assert.Equal(
            DataQualityEvidencePoint
                .BoundedEvaluatorSource,
            store.Appended[0]
                .Evidence.Source);
        Assert.Equal(
            RecordReadDirection.Forward,
            read.LastRequest!.Direction);
        Assert.Equal(
            RecordAnchorKind.Timestamp,
            read.LastRequest.Anchor.Kind);
        Assert.True(
            read.LastRequest.Budget.MaxRecords <=
            RecordOperationBudget.HardMaxRecords);
        Assert.True(
            read.LastRequest.Budget.MaxRawBytes <=
            RecordOperationBudget.HardMaxRawBytes);
    }

    [Fact]
    public async Task Worker_stops_at_cycle_evaluation_ceiling()
    {
        var snapshot =
            Snapshot(
                partitions: [0, 1, 2, 3]);
        var store =
            new FakeStore(
                [snapshot]);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(
                    EmptyBatch()),
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 2));

        var result =
            await worker.RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        Assert.True(
            result.EvaluationLimitReached);
        Assert.Equal(
            2,
            result.EvaluationsAttempted);
    }

    [Fact]
    public async Task Worker_marks_truncated_source_batch_partial_and_preserves_unread_range()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var batch =
            new RecordReadBatch(
                [
                    new KafkaRawRecord(
                        10,
                        now.AddSeconds(-1),
                        null,
                        new byte[] { 123, 125 },
                        []),
                ],
                0,
                20,
                10,
                10,
                RecordAnchor.AtOffset(11),
                null,
                RecordBudgetOutcome.RawByteLimit);

        var worker =
            new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(batch),
                new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now)),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(
            DataQualityEvidenceState.Partial,
            point.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.ByteLimit,
            point.Progress.Outcome);
        Assert.Equal(20, point.Progress.EndOffsetExclusive);
        Assert.Equal(11, point.Progress.NextOffset);
    }

    [Fact]
    public async Task Worker_rotates_shared_policy_budget_across_partitions()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                        new RecordReadBatch(
                            [
                                new KafkaRawRecord(
                                    0,
                                    now.AddSeconds(-1),
                                    null,
                                    new byte[] { 123, 125 },
                                    []),
                            ],
                            0,
                            1,
                            0,
                            0,
                            null,
                            null,
                            RecordBudgetOutcome.Complete));
        var store =
            new FakeStore(
                [
                    Snapshot(
                        partitions: [0, 1, 2, 3],
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 1,
                                bytesPerSecond: 1024 * 1024)),
                ]);
        var clock =
            new MutableTimeProvider(now);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                new FakeEvaluator(now),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 8,
                    maxRecordsPerEvaluation: 10,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                clock);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(1, read.Calls);

        clock.Advance(
            TimeSpan.FromSeconds(1));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(2, read.Calls);
        Assert.Equal(
            [0, 1],
            read.Requests
                .Select(request => request.Partition)
                .ToArray());
        Assert.All(
            read.Requests,
            request =>
                Assert.Equal(
                    1,
                    request.Budget.MaxRecords));
    }

    [Fact]
    public async Task Worker_gives_rotating_partition_full_remaining_byte_capacity()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var twoMiB =
            new byte[
                2 * 1024 * 1024];
        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                        new RecordReadBatch(
                            [
                                new KafkaRawRecord(
                                    0,
                                    now.AddSeconds(-1),
                                    null,
                                    twoMiB,
                                    []),
                            ],
                            0,
                            1,
                            0,
                            0,
                            null,
                            null,
                            RecordBudgetOutcome.Complete));
        var store =
            new FakeStore(
                [
                    Snapshot(
                        partitions: [0, 1, 2, 3],
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 100,
                                bytesPerSecond:
                                    4L * 1024 * 1024)),
                ]);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                new FakeEvaluator(now),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 8,
                    maxRecordsPerEvaluation: 100,
                    maxRawBytesPerEvaluation:
                        4L * 1024 * 1024,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                new FixedTimeProvider(now));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(2, read.Calls);
        Assert.Equal(
            4L * 1024 * 1024,
            read.Requests[0]
                .Budget.MaxRawBytes);
        Assert.Equal(
            2L * 1024 * 1024,
            read.Requests[1]
                .Budget.MaxRawBytes);
        Assert.Equal(
            [0, 1],
            read.Requests
                .Select(request => request.Partition)
                .ToArray());
    }

    [Fact]
    public async Task Worker_retries_underfunded_partition_first_with_full_allowance_next_cycle()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var threeMiB =
            new byte[
                3 * 1024 * 1024];

        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                    {
                        if (request.Budget.MaxRawBytes >=
                            threeMiB.Length)
                        {
                            return new RecordReadBatch(
                                [
                                    new KafkaRawRecord(
                                        0,
                                        now.AddSeconds(-1),
                                        null,
                                        threeMiB,
                                        []),
                                ],
                                0,
                                1,
                                0,
                                0,
                                null,
                                null,
                                RecordBudgetOutcome.Complete);
                        }

                        return new RecordReadBatch(
                            [],
                            0,
                            1,
                            null,
                            null,
                            RecordAnchor.AtOffset(0),
                            null,
                            RecordBudgetOutcome.RawByteLimit);
                    });

        var store =
            new FakeStore(
                [
                    Snapshot(
                        partitions: [0, 1, 2, 3],
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 100,
                                bytesPerSecond:
                                    4L * 1024 * 1024)),
                ]);

        var clock =
            new MutableTimeProvider(now);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                new FakeEvaluator(now),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 8,
                    maxRecordsPerEvaluation: 100,
                    maxRawBytesPerEvaluation:
                        4L * 1024 * 1024,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                clock);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        clock.Advance(
            TimeSpan.FromSeconds(1));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            [0, 1, 1, 2],
            read.Requests
                .Select(
                    request =>
                        request.Partition)
                .ToArray());

        Assert.Equal(
            4L * 1024 * 1024,
            read.Requests[2]
                .Budget.MaxRawBytes);
    }

    [Fact]
    public async Task Worker_rotates_past_full_ceiling_oversized_partition()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);

        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                    {
                        if (request.Partition == 0)
                        {
                            return new RecordReadBatch(
                                [],
                                0,
                                1,
                                null,
                                null,
                                RecordAnchor.AtOffset(0),
                                null,
                                RecordBudgetOutcome.RawByteLimit);
                        }

                        return new RecordReadBatch(
                            [
                                new KafkaRawRecord(
                                    0,
                                    now.AddSeconds(-1),
                                    null,
                                    new byte[] { 123, 125 },
                                    []),
                            ],
                            0,
                            1,
                            0,
                            0,
                            null,
                            null,
                            RecordBudgetOutcome.Complete);
                    });

        var store =
            new FakeStore(
                [
                    Snapshot(
                        partitions: [0, 1],
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 100,
                                bytesPerSecond:
                                    4L * 1024 * 1024)),
                ]);

        var clock =
            new MutableTimeProvider(now);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                new FakeEvaluator(now),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 1,
                    maxRecordsPerEvaluation: 100,
                    maxRawBytesPerEvaluation:
                        4L * 1024 * 1024,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                clock);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(
            [0],
            read.Requests
                .Select(
                    request =>
                        request.Partition)
                .ToArray());

        clock.Advance(
            TimeSpan.FromSeconds(1));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            [0, 1],
            read.Requests
                .Select(
                    request =>
                        request.Partition)
                .ToArray());
        Assert.Equal(
            2,
            store.Appended.Count);
        Assert.Contains(
            store.Appended,
            point =>
                point.Progress.Partition == 0 &&
                point.Evidence.State ==
                    DataQualityEvidenceState.Unknown);
        Assert.Contains(
            store.Appended,
            point =>
                point.Progress.Partition == 1 &&
                point.Evidence.EvaluatedRecords == 1);
    }

    [Fact]
    public async Task Worker_continues_after_uncertain_timestamp_within_bounded_batch()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var batch =
            new RecordReadBatch(
                [
                    new KafkaRawRecord(
                        10,
                        now.AddSeconds(1),
                        null,
                        new byte[] { 123, 125 },
                        []),
                    new KafkaRawRecord(
                        11,
                        now.AddSeconds(-1),
                        null,
                        new byte[] { 123, 125 },
                        []),
                ],
                0,
                12,
                10,
                11,
                null,
                null,
                RecordBudgetOutcome.Complete);

        await new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(batch),
                new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now)),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now))
            .RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(
            1,
            point.Evidence.EvaluatedRecords);
        Assert.Equal(
            DataQualityEvidenceState.Partial,
            point.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.RecordLimit,
            point.Progress.Outcome);
        Assert.Equal(
            11,
            point.Progress.StartOffset);
    }

    [Fact]
    public async Task Worker_advances_and_clears_resume_after_oversized_record()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);

        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                    {
                        if (request.Anchor.Kind ==
                            RecordAnchorKind.Timestamp)
                        {
                            return new RecordReadBatch(
                                [],
                                0,
                                3,
                                null,
                                null,
                                RecordAnchor.AtOffset(1),
                                null,
                                RecordBudgetOutcome.RawByteLimit);
                        }

                        Assert.Equal(
                            RecordAnchorKind.Offset,
                            request.Anchor.Kind);

                        if (request.Anchor.Offset == 1)
                        {
                            return new RecordReadBatch(
                                [
                                    new KafkaRawRecord(
                                        1,
                                        now.AddSeconds(-1),
                                        null,
                                        new byte[] { 123, 125 },
                                        []),
                                ],
                                0,
                                2,
                                1,
                                1,
                                null,
                                RecordAnchor.AtOffset(1),
                                RecordBudgetOutcome.RecordLimit);
                        }

                        Assert.Equal(
                            2,
                            request.Anchor.Offset);

                        return new RecordReadBatch(
                            [
                                new KafkaRawRecord(
                                    2,
                                    now.AddSeconds(-1),
                                    null,
                                    new byte[] { 123, 125 },
                                    []),
                            ],
                            0,
                            3,
                            2,
                            2,
                            null,
                            RecordAnchor.AtOffset(2),
                            RecordBudgetOutcome.Complete);
                    });

        var store =
            new FakeStore(
                [
                    Snapshot(
                        partitions: [0],
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 100,
                                bytesPerSecond:
                                    4L * 1024 * 1024)),
                ]);

        var clock =
            new MutableTimeProvider(now);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                new FakeEvaluator(now),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 8,
                    maxRecordsPerEvaluation: 100,
                    maxRawBytesPerEvaluation:
                        4L * 1024 * 1024,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                clock);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        clock.Advance(
            TimeSpan.FromSeconds(1));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            [
                RecordAnchorKind.Timestamp,
                RecordAnchorKind.Offset,
                RecordAnchorKind.Offset,
                RecordAnchorKind.Timestamp,
            ],
            read.Requests
                .Select(request => request.Anchor.Kind)
                .ToArray());
        Assert.Equal(
            1,
            read.Requests[1].Anchor.Offset);
        Assert.Equal(
            2,
            read.Requests[2].Anchor.Offset);
    }

    [Fact]
    public async Task Worker_enforces_rolling_record_rate_across_partitions_and_cycles()
    {
        var clock =
            new MutableTimeProvider(
                new DateTimeOffset(
                    2026,
                    10,
                    4,
                    12,
                    0,
                    0,
                    TimeSpan.Zero));
        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                        new RecordReadBatch(
                            [
                                new KafkaRawRecord(
                                    0,
                                    clock.GetUtcNow(),
                                    null,
                                    new byte[] { 123, 125 },
                                    []),
                            ],
                            0,
                            1,
                            0,
                            0,
                            null,
                            null,
                            RecordBudgetOutcome.Complete));
        var worker =
            new DataQualityMonitoringWorker(
                new FakeStore(
                    [
                        Snapshot(
                            partitions: [0, 1, 2],
                            budget:
                                new DataQualityPolicyBudget(
                                    recordsPerSecond: 2,
                                    bytesPerSecond:
                                        1024 * 1024)),
                    ]),
                read,
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 10,
                    maxRecordsPerEvaluation: 10,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                clock);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(2, read.Calls);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(2, read.Calls);

        clock.Advance(
            TimeSpan.FromMilliseconds(999));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(2, read.Calls);

        clock.Advance(
            TimeSpan.FromMilliseconds(1));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(4, read.Calls);
    }

    [Fact]
    public async Task Worker_enforces_rolling_byte_rate_across_immediate_cycles()
    {
        var clock =
            new MutableTimeProvider(
                new DateTimeOffset(
                    2026,
                    10,
                    4,
                    12,
                    0,
                    0,
                    TimeSpan.Zero));
        var payload =
            new byte[3];

        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                        request.Budget.MaxRawBytes >=
                            payload.Length
                            ? new RecordReadBatch(
                                [
                                    new KafkaRawRecord(
                                        0,
                                        clock.GetUtcNow(),
                                        null,
                                        payload,
                                        []),
                                ],
                                0,
                                1,
                                0,
                                0,
                                null,
                                null,
                                RecordBudgetOutcome.Complete)
                            : new RecordReadBatch(
                                [],
                                0,
                                1,
                                null,
                                null,
                                RecordAnchor.AtOffset(0),
                                null,
                                RecordBudgetOutcome.RawByteLimit));

        var worker =
            new DataQualityMonitoringWorker(
                new FakeStore(
                    [
                        Snapshot(
                            partitions: [0, 1],
                            budget:
                                new DataQualityPolicyBudget(
                                    recordsPerSecond: 100,
                                    bytesPerSecond: 4)),
                    ]),
                read,
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 10,
                    maxRecordsPerEvaluation: 10,
                    maxRawBytesPerEvaluation: 4,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                clock);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(2, read.Calls);
        Assert.Equal(
            4,
            read.Requests[0].Budget.MaxRawBytes);
        Assert.Equal(
            1,
            read.Requests[1].Budget.MaxRawBytes);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(2, read.Calls);

        clock.Advance(
            TimeSpan.FromSeconds(1));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.True(
            read.Requests.Count >= 3);
        Assert.Equal(
            4,
            read.Requests[2].Budget.MaxRawBytes);
    }

    [Fact]
    public async Task Worker_clears_stale_resume_offset_without_counting_source_failure()
    {
        var call = 0;
        var read =
            new FakeReadPort(
                failureFactory:
                    request =>
                    {
                        call++;
                        if (call == 1)
                        {
                            return null;
                        }

                        return request.Anchor.Kind ==
                            RecordAnchorKind.Offset
                            ? new KafkaFailure(
                                KafkaFailureCategory.ProtocolError,
                                "offset_out_of_range",
                                "The requested offset is outside the retained range.",
                                false)
                            : null;
                    },
                batchFactory:
                    request =>
                        new RecordReadBatch(
                            [],
                            0,
                            2,
                            null,
                            null,
                            RecordAnchor.AtOffset(1),
                            null,
                            RecordBudgetOutcome.RawByteLimit));

        var store =
            new FakeStore(
                [
                    Snapshot(
                        partitions: [0],
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 100,
                                bytesPerSecond:
                                    4L * 1024 * 1024)),
                ]);

        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 4,
                    maxRecordsPerEvaluation: 100,
                    maxRawBytesPerEvaluation:
                        4L * 1024 * 1024,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        var second =
            await worker.RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        Assert.Equal(
            0,
            second.SourceFailures);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            RecordAnchorKind.Timestamp,
            read.Requests[^1].Anchor.Kind);
    }

    [Fact]
    public async Task Worker_rotates_through_truncated_policy_pages()
    {
        var store =
            new FakeStore(
                [
                    Snapshot(policyId: "a-policy"),
                    Snapshot(policyId: "b-policy"),
                    Snapshot(policyId: "c-policy"),
                ]);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(
                    EmptyBatch()),
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 2,
                    maxEvaluationsPerCycle: 10));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            ["a-policy", "b-policy", "c-policy"],
            store.Appended
                .Select(point => point.Evidence.PolicyId)
                .Distinct(StringComparer.Ordinal)
                .ToArray());
        Assert.Contains(
            store.ListQueries,
            query =>
                string.Equals(
                    query.AfterPolicyId,
                    "b-policy",
                    StringComparison.Ordinal));
    }

    [Fact]
    public async Task Worker_does_not_advance_past_untouched_policy_when_global_limit_is_reached()
    {
        var store =
            new FakeStore(
                [
                    Snapshot(policyId: "a-policy"),
                    Snapshot(policyId: "b-policy"),
                    Snapshot(policyId: "c-policy"),
                ]);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(
                    EmptyBatch()),
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 3,
                    maxEvaluationsPerCycle: 1));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            ["a-policy", "b-policy"],
            store.Appended
                .Select(
                    point =>
                        point.Evidence.PolicyId)
                .ToArray());
        Assert.Contains(
            store.ListQueries,
            query =>
                string.Equals(
                    query.AfterPolicyId,
                    "a-policy",
                    StringComparison.Ordinal));
        Assert.DoesNotContain(
            store.ListQueries,
            query =>
                string.Equals(
                    query.AfterPolicyId,
                    "b-policy",
                    StringComparison.Ordinal));
    }

    [Fact]
    public async Task Worker_rotates_clusters_when_global_evaluation_limit_is_reached()
    {
        var store =
            new FakeStore(
                [
                    Snapshot(
                        policyId: "a-policy",
                        clusterId: "cluster-a"),
                    Snapshot(
                        policyId: "b-policy",
                        clusterId: "cluster-b"),
                ]);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(
                    EmptyBatch()),
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 2,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 1));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["cluster-a", "cluster-b"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["cluster-a", "cluster-b"]));

        Assert.Equal(
            ["cluster-a", "cluster-b"],
            store.Appended
                .Select(
                    point =>
                        point.Progress.ClusterId)
                .ToArray());
    }

    [Fact]
    public async Task Worker_admits_returned_offsets_beyond_stale_high_watermark()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var batch =
            new RecordReadBatch(
                [
                    new KafkaRawRecord(
                        5,
                        now.AddSeconds(-1),
                        null,
                        new byte[] { 123, 125 },
                        []),
                ],
                0,
                5,
                5,
                5,
                RecordAnchor.AtOffset(6),
                null,
                RecordBudgetOutcome.RecordLimit);

        var result =
            await new DataQualityMonitoringWorker(
                    store,
                    new FakeReadPort(batch),
                    new BoundedDataQualityBatchEvaluator(
                        new FixedTimeProvider(now)),
                    new FakeAuthorization(
                        authorized: true),
                    timeProvider:
                        new FixedTimeProvider(now))
                .RunCycleAsync(
                    new DataQualityMonitoringCycleRequest(
                        ["prod"]));

        Assert.Equal(0, result.EvaluationFailures);
        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(6, point.Progress.EndOffsetExclusive);
        Assert.Equal(1, point.Evidence.EvaluatedRecords);
    }

    [Fact]
    public async Task Worker_preserves_resolved_anchor_for_complete_empty_read()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var batch =
            new RecordReadBatch(
                [],
                0,
                10,
                null,
                null,
                null,
                RecordAnchor.AtOffset(10),
                RecordBudgetOutcome.Complete);

        await new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(batch),
                new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now)),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now))
            .RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(10, point.Progress.StartOffset);
        Assert.Equal(10, point.Progress.EndOffsetExclusive);
        Assert.Equal(
            DataQualityEvaluationOutcome.Complete,
            point.Progress.Outcome);
        Assert.Equal(
            DataQualityEvidenceState.Available,
            point.Evidence.State);
    }

    [Fact]
    public async Task Worker_excludes_future_records_at_window_end()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var batch =
            new RecordReadBatch(
                [
                    new KafkaRawRecord(
                        10,
                        now.AddSeconds(-1),
                        null,
                        new byte[] { 123, 125 },
                        []),
                    new KafkaRawRecord(
                        11,
                        now.AddSeconds(1),
                        null,
                        new byte[] { 123, 125 },
                        []),
                ],
                0,
                12,
                10,
                11,
                null,
                null,
                RecordBudgetOutcome.Complete);

        await new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(batch),
                new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now)),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now))
            .RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(1, point.Evidence.EvaluatedRecords);
        Assert.Equal(
            DataQualityEvidenceState.Partial,
            point.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.RecordLimit,
            point.Progress.Outcome);
        Assert.Equal(12, point.Progress.EndOffsetExclusive);
        Assert.Equal(11, point.Progress.NextOffset);
    }

    [Fact]
    public async Task Worker_preserves_start_range_for_empty_truncated_source_batch()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var batch =
            new RecordReadBatch(
                [],
                5,
                20,
                null,
                null,
                RecordAnchor.AtOffset(8),
                null,
                RecordBudgetOutcome.RawByteLimit);

        await new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(batch),
                new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now)),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now))
            .RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(
            DataQualityEvidenceState.Unknown,
            point.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.ByteLimit,
            point.Progress.Outcome);
        Assert.Equal(7, point.Progress.StartOffset);
        Assert.Equal(20, point.Progress.EndOffsetExclusive);
        Assert.Equal(7, point.Progress.NextOffset);
    }

    [Fact]
    public async Task Worker_treats_missing_timestamp_as_partial_window_uncertainty()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var batch =
            new RecordReadBatch(
                [
                    new KafkaRawRecord(
                        10,
                        now.AddSeconds(-1),
                        null,
                        new byte[] { 123, 125 },
                        []),
                    new KafkaRawRecord(
                        11,
                        null,
                        null,
                        new byte[] { 123, 125 },
                        []),
                ],
                0,
                12,
                10,
                11,
                null,
                null,
                RecordBudgetOutcome.Complete);

        await new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(batch),
                new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now)),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now))
            .RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(1, point.Evidence.EvaluatedRecords);
        Assert.Equal(
            DataQualityEvidenceState.Partial,
            point.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.RecordLimit,
            point.Progress.Outcome);
        Assert.Equal(12, point.Progress.EndOffsetExclusive);
        Assert.Equal(11, point.Progress.NextOffset);
    }

    [Fact]
    public async Task Worker_propagates_cancelled_source_result_as_cancellation()
    {
        var store =
            new FakeStore(
                [Snapshot()]);
        var read =
            new FakeReadPort(
                failure:
                    new KafkaFailure(
                        KafkaFailureCategory.Cancelled,
                        "cancelled",
                        "Cancelled.",
                        true));

        await Assert.ThrowsAsync<OperationCanceledException>(
            () =>
                new DataQualityMonitoringWorker(
                        store,
                        read,
                        new FakeEvaluator(),
                        new FakeAuthorization(
                            authorized: true))
                    .RunCycleAsync(
                        new DataQualityMonitoringCycleRequest(
                            ["prod"])));

        Assert.Empty(store.Appended);
    }

    [Fact]
    public async Task Worker_persists_worker_deadline_as_unknown_duration_evidence()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var read =
            new FakeReadPort(
                failure:
                    new KafkaFailure(
                        KafkaFailureCategory.Timeout,
                        "deadline_exceeded",
                        "Kafka operation exceeded its deadline.",
                        true));

        var result =
            await new DataQualityMonitoringWorker(
                    store,
                    read,
                    new FakeEvaluator(now),
                    new FakeAuthorization(
                        authorized: true),
                    timeProvider:
                        new FixedTimeProvider(now))
                .RunCycleAsync(
                    new DataQualityMonitoringCycleRequest(
                        ["prod"]));

        Assert.Equal(0, result.SourceFailures);
        Assert.Equal(1, result.EvaluationsPersisted);

        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(
            DataQualityEvidenceState.Unknown,
            point.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.DurationLimit,
            point.Progress.Outcome);
        Assert.Equal(0, point.Evidence.EvaluatedRecords);
        Assert.Equal(0, point.Evidence.EvaluatedBytes);
        Assert.Equal(
            DataQualityEvidencePoint.BoundedEvaluatorSource,
            point.Evidence.Source);
    }

    [Fact]
    public async Task Worker_charges_worker_deadline_against_policy_rate_allowance()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                5,
                12,
                0,
                0,
                TimeSpan.Zero);
        var read =
            new FakeReadPort(
                failureFactory:
                    request =>
                        request.Partition == 0
                            ? new KafkaFailure(
                                KafkaFailureCategory.Timeout,
                                "deadline_exceeded",
                                "Kafka operation exceeded its deadline.",
                                true)
                            : null,
                batchFactory:
                    request =>
                        new RecordReadBatch(
                            [
                                new KafkaRawRecord(
                                    0,
                                    now.AddSeconds(-1),
                                    null,
                                    new byte[] { 123, 125 },
                                    []),
                            ],
                            0,
                            1,
                            0,
                            0,
                            null,
                            null,
                            RecordBudgetOutcome.Complete));

        var store =
            new FakeStore(
                [
                    Snapshot(
                        partitions: [0, 1],
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 1,
                                bytesPerSecond:
                                    1024 * 1024)),
                ]);

        var result =
            await new DataQualityMonitoringWorker(
                    store,
                    read,
                    new FakeEvaluator(now),
                    new FakeAuthorization(
                        authorized: true),
                    new DataQualityMonitoringWorkerPolicy(
                        maxClustersPerCycle: 1,
                        maxPoliciesPerCluster: 1,
                        maxEvaluationsPerCycle: 4,
                        maxRecordsPerEvaluation: 1,
                        maxDurationPerEvaluation:
                            TimeSpan.FromSeconds(1)),
                    new FixedTimeProvider(now))
                .RunCycleAsync(
                    new DataQualityMonitoringCycleRequest(
                        ["prod"]));

        Assert.Equal(1, read.Calls);
        Assert.Equal(1, result.EvaluationsPersisted);
        Assert.Equal(
            DataQualityEvaluationOutcome.DurationLimit,
            Assert.Single(store.Appended)
                .Progress.Outcome);
    }

    [Fact]
    public async Task Worker_preserves_evaluator_limit_when_source_is_also_truncated()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                5,
                12,
                0,
                0,
                TimeSpan.Zero);
        var store =
            new FakeStore(
                [Snapshot()]);
        var read =
            new FakeReadPort(
                new RecordReadBatch(
                    [
                        new KafkaRawRecord(
                            10,
                            now.AddSeconds(-2),
                            null,
                            new byte[] { 123, 125 },
                            []),
                        new KafkaRawRecord(
                            11,
                            now.AddSeconds(-1),
                            null,
                            new byte[] { 123, 125 },
                            []),
                    ],
                    0,
                    20,
                    10,
                    11,
                    RecordAnchor.AtOffset(12),
                    null,
                    RecordBudgetOutcome.RecordLimit));

        await new DataQualityMonitoringWorker(
                store,
                read,
                new PartialProgressEvaluator(
                    nextOffset: 11,
                    evaluatedRecords: 1,
                    now),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now))
            .RunCycleAsync(
                new DataQualityMonitoringCycleRequest(
                    ["prod"]));

        var point =
            Assert.Single(
                store.Appended);
        Assert.Equal(
            DataQualityEvaluationOutcome.DurationLimit,
            point.Progress.Outcome);
        Assert.Equal(
            11,
            point.Progress.NextOffset);
    }

    [Fact]
    public async Task Worker_keeps_rolling_rate_history_across_policy_version_replacement()
    {
        var clock =
            new MutableTimeProvider(
                new DateTimeOffset(
                    2026,
                    10,
                    4,
                    12,
                    0,
                    0,
                    TimeSpan.Zero));
        var store =
            new FakeStore(
                [
                    Snapshot(
                        version: 1,
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 1,
                                bytesPerSecond:
                                    1024 * 1024)),
                ]);
        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                        new RecordReadBatch(
                            [
                                new KafkaRawRecord(
                                    0,
                                    clock.GetUtcNow()
                                        .AddMilliseconds(-1),
                                    null,
                                    new byte[] { 123, 125 },
                                    []),
                            ],
                            0,
                            1,
                            0,
                            0,
                            null,
                            null,
                            RecordBudgetOutcome.Complete));
        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 4,
                    maxRecordsPerEvaluation: 4,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                clock);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(1, read.Calls);

        store.ReplaceItems(
            [
                Snapshot(
                    version: 2,
                    budget:
                        new DataQualityPolicyBudget(
                            recordsPerSecond: 1,
                            bytesPerSecond:
                                1024 * 1024)),
            ]);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        Assert.Equal(1, read.Calls);

        clock.Advance(
            TimeSpan.FromSeconds(1));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(2, read.Calls);
    }

    [Fact]
    public async Task Worker_rebases_resume_when_retained_offset_is_behind_evaluation_window()
    {
        var clock =
            new MutableTimeProvider(
                new DateTimeOffset(
                    2026,
                    10,
                    4,
                    12,
                    0,
                    0,
                    TimeSpan.Zero));
        var timestampReads = 0;
        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                    {
                        if (request.Anchor.Kind ==
                            RecordAnchorKind.Timestamp)
                        {
                            timestampReads++;
                            if (timestampReads == 1)
                            {
                                return new RecordReadBatch(
                                    [],
                                    0,
                                    2,
                                    null,
                                    null,
                                    RecordAnchor.AtOffset(1),
                                    null,
                                    RecordBudgetOutcome.RawByteLimit);
                            }

                            return new RecordReadBatch(
                                [],
                                0,
                                2,
                                null,
                                null,
                                null,
                                null,
                                RecordBudgetOutcome.Complete);
                        }

                        Assert.Equal(
                            RecordAnchorKind.Offset,
                            request.Anchor.Kind);
                        Assert.Equal(
                            1,
                            request.Anchor.Offset);

                        return new RecordReadBatch(
                            [
                                new KafkaRawRecord(
                                    1,
                                    clock.GetUtcNow()
                                        .AddSeconds(-3),
                                    null,
                                    new byte[] { 123, 125 },
                                    []),
                            ],
                            0,
                            2,
                            1,
                            1,
                            RecordAnchor.AtOffset(2),
                            RecordAnchor.AtOffset(1),
                            RecordBudgetOutcome.RecordLimit);
                    });
        var store =
            new FakeStore(
                [
                    Snapshot(
                        budget:
                            new DataQualityPolicyBudget(
                                recordsPerSecond: 100,
                                bytesPerSecond: 4,
                                evaluationWindow:
                                    TimeSpan.FromSeconds(2))),
                ]);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                read,
                new FakeEvaluator(),
                new FakeAuthorization(
                    authorized: true),
                new DataQualityMonitoringWorkerPolicy(
                    maxClustersPerCycle: 1,
                    maxPoliciesPerCluster: 1,
                    maxEvaluationsPerCycle: 4,
                    maxRecordsPerEvaluation: 4,
                    maxRawBytesPerEvaluation: 4,
                    maxDurationPerEvaluation:
                        TimeSpan.FromSeconds(1)),
                clock);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        clock.Advance(
            TimeSpan.FromSeconds(3));
        var beforeStaleAttempt =
            store.Appended.Count;
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            beforeStaleAttempt,
            store.Appended.Count);

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            [
                RecordAnchorKind.Timestamp,
                RecordAnchorKind.Offset,
                RecordAnchorKind.Timestamp,
            ],
            read.Requests
                .Select(
                    request =>
                        request.Anchor.Kind)
                .ToArray());
    }

    [Fact]
    public async Task Worker_retains_continuation_from_initial_truncated_read()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                5,
                6,
                0,
                0,
                TimeSpan.Zero);

        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                    {
                        if (request.Anchor.Kind ==
                            RecordAnchorKind.Timestamp)
                        {
                            return new RecordReadBatch(
                                [
                                    new KafkaRawRecord(
                                        10,
                                        now.AddSeconds(-2),
                                        null,
                                        new byte[] { 123, 125 },
                                        []),
                                    new KafkaRawRecord(
                                        11,
                                        now.AddSeconds(-1),
                                        null,
                                        new byte[] { 123, 125 },
                                        []),
                                ],
                                0,
                                20,
                                10,
                                11,
                                RecordAnchor.AtOffset(12),
                                null,
                                RecordBudgetOutcome.RecordLimit);
                        }

                        Assert.Equal(
                            12,
                            request.Anchor.Offset);

                        return new RecordReadBatch(
                            [],
                            0,
                            20,
                            null,
                            null,
                            null,
                            null,
                            RecordBudgetOutcome.Complete);
                    });

        var worker =
            new DataQualityMonitoringWorker(
                new FakeStore(
                    [Snapshot()]),
                read,
                new FakeEvaluator(now),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            2,
            read.Requests.Count);
        Assert.Equal(
            RecordAnchorKind.Offset,
            read.Requests[1].Anchor.Kind);
        Assert.Equal(
            12,
            read.Requests[1].Anchor.Offset);
    }

    [Fact]
    public async Task Worker_advances_continuation_only_through_evaluated_records()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                5,
                6,
                0,
                0,
                TimeSpan.Zero);

        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                    {
                        if (request.Anchor.Kind ==
                            RecordAnchorKind.Timestamp)
                        {
                            return new RecordReadBatch(
                                Enumerable
                                    .Range(10, 5)
                                    .Select(
                                        offset =>
                                            new KafkaRawRecord(
                                                offset,
                                                now.AddSeconds(-1),
                                                null,
                                                new byte[] { 123, 125 },
                                                []))
                                    .ToArray(),
                                0,
                                30,
                                10,
                                14,
                                RecordAnchor.AtOffset(15),
                                null,
                                RecordBudgetOutcome.RecordLimit);
                        }

                        Assert.Equal(
                            12,
                            request.Anchor.Offset);

                        return new RecordReadBatch(
                            [],
                            0,
                            30,
                            null,
                            null,
                            null,
                            null,
                            RecordBudgetOutcome.Complete);
                    });

        var worker =
            new DataQualityMonitoringWorker(
                new FakeStore(
                    [Snapshot()]),
                read,
                new PartialProgressEvaluator(
                    nextOffset: 12,
                    evaluatedRecords: 2,
                    now),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            12,
            read.Requests[1].Anchor.Offset);
    }

    [Fact]
    public async Task Worker_propagates_evaluator_cancelled_result_without_persistence()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                5,
                6,
                0,
                0,
                TimeSpan.Zero);
        using var cancellation =
            new CancellationTokenSource();
        var store =
            new FakeStore(
                [Snapshot()]);
        var worker =
            new DataQualityMonitoringWorker(
                store,
                new FakeReadPort(
                    new RecordReadBatch(
                        [
                            new KafkaRawRecord(
                                0,
                                now.AddSeconds(-1),
                                null,
                                new byte[] { 123, 125 },
                                []),
                        ],
                        0,
                        1,
                        0,
                        0,
                        null,
                        null,
                        RecordBudgetOutcome.Complete)),
                new CancellingEvaluator(
                    cancellation,
                    now),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () =>
                worker.RunCycleAsync(
                    new DataQualityMonitoringCycleRequest(
                        ["prod"]),
                    cancellation.Token));

        Assert.Empty(
            store.Appended);
    }

    [Fact]
    public async Task Worker_advances_past_fully_inspected_timestamp_uncertain_page()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                5,
                9,
                0,
                0,
                TimeSpan.Zero);

        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                    {
                        if (request.Anchor.Kind ==
                            RecordAnchorKind.Timestamp)
                        {
                            return new RecordReadBatch(
                                [
                                    new KafkaRawRecord(
                                        10,
                                        now.AddSeconds(1),
                                        null,
                                        new byte[] { 123, 125 },
                                        []),
                                    new KafkaRawRecord(
                                        11,
                                        null,
                                        null,
                                        new byte[] { 123, 125 },
                                        []),
                                ],
                                0,
                                20,
                                10,
                                11,
                                RecordAnchor.AtOffset(12),
                                null,
                                RecordBudgetOutcome.RecordLimit);
                        }

                        Assert.Equal(
                            12,
                            request.Anchor.Offset);

                        return new RecordReadBatch(
                            [],
                            0,
                            20,
                            null,
                            null,
                            null,
                            null,
                            RecordBudgetOutcome.Complete);
                    });

        var worker =
            new DataQualityMonitoringWorker(
                new FakeStore(
                    [Snapshot()]),
                read,
                new FakeEvaluator(now),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            12,
            read.Requests[1].Anchor.Offset);
    }

    [Fact]
    public async Task Worker_does_not_advance_safe_prefix_past_first_accepted_record()
    {
        var now =
            new DateTimeOffset(
                2026,
                10,
                5,
                9,
                0,
                0,
                TimeSpan.Zero);

        var read =
            new FakeReadPort(
                batchFactory:
                    request =>
                    {
                        if (request.Anchor.Kind ==
                            RecordAnchorKind.Timestamp)
                        {
                            return new RecordReadBatch(
                                [
                                    new KafkaRawRecord(
                                        10,
                                        null,
                                        null,
                                        new byte[] { 123, 125 },
                                        []),
                                    new KafkaRawRecord(
                                        11,
                                        now.AddSeconds(-1),
                                        null,
                                        new byte[] { 123, 125 },
                                        []),
                                    new KafkaRawRecord(
                                        12,
                                        now.AddSeconds(1),
                                        null,
                                        new byte[] { 123, 125 },
                                        []),
                                ],
                                0,
                                20,
                                10,
                                12,
                                RecordAnchor.AtOffset(13),
                                null,
                                RecordBudgetOutcome.RecordLimit);
                        }

                        Assert.Equal(
                            11,
                            request.Anchor.Offset);

                        return new RecordReadBatch(
                            [],
                            0,
                            20,
                            null,
                            null,
                            null,
                            null,
                            RecordBudgetOutcome.Complete);
                    });

        var worker =
            new DataQualityMonitoringWorker(
                new FakeStore(
                    [Snapshot()]),
                read,
                new PartialProgressEvaluator(
                    nextOffset: 11,
                    evaluatedRecords: 0,
                    now),
                new FakeAuthorization(
                    authorized: true),
                timeProvider:
                    new FixedTimeProvider(now));

        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));
        await worker.RunCycleAsync(
            new DataQualityMonitoringCycleRequest(
                ["prod"]));

        Assert.Equal(
            11,
            read.Requests[1].Anchor.Offset);
    }

    [Fact]
    public async Task Worker_does_not_fabricate_evidence_on_source_failure()
    {
        var store =
            new FakeStore(
                [Snapshot()]);
        var read =
            new FakeReadPort(
                failure:
                    new KafkaFailure(
                        KafkaFailureCategory.Unavailable,
                        "source-unavailable",
                        "Source unavailable.",
                        true));

        var result =
            await new DataQualityMonitoringWorker(
                    store,
                    read,
                    new FakeEvaluator(),
                    new FakeAuthorization(
                        authorized: true))
                .RunCycleAsync(
                    new DataQualityMonitoringCycleRequest(
                        ["prod"]));

        Assert.Equal(1, result.SourceFailures);
        Assert.Empty(store.Appended);
    }

    private static DataQualityPolicyLifecycleSnapshot
        Snapshot(
            IReadOnlyList<int>? partitions = null,
            DataQualityPolicyBudget? budget = null,
            string policyId = "orders-quality",
            string clusterId = "prod",
            int version = 1) =>
        new(
            new DataQualityPolicyDefinition(
                policyId,
                version,
                new DataQualityPolicyScope(
                    clusterId,
                    "orders",
                    partitions ?? [0]),
                [
                    new DataQualityRule(
                        "id-required",
                        DataQualityRuleKind.RequiredPath,
                        "/id"),
                ],
                budget),
            DataQualityPolicyLifecycleState.Active,
            1,
            DateTimeOffset.UtcNow);

    private static RecordReadBatch EmptyBatch() =>
        new(
            [],
            0,
            0,
            null,
            null,
            null,
            null,
            RecordBudgetOutcome.Complete);

    private sealed class FakeAuthorization(
        bool authorized) :
        IDataQualityMonitoringAuthorizationPort
    {
        public Task<bool> IsAuthorizedAsync(
            DataQualityPolicyDefinition policy,
            int partition,
            CancellationToken cancellationToken) =>
            Task.FromResult(authorized);
    }

    private sealed class FakeReadPort :
        IKafkaRecordReadPort
    {
        private readonly RecordReadBatch?
            _batch;
        private readonly KafkaFailure?
            _failure;
        private readonly Func<RecordReadRequest, RecordReadBatch>?
            _batchFactory;
        private readonly Func<RecordReadRequest, KafkaFailure?>?
            _failureFactory;

        public FakeReadPort(
            RecordReadBatch? batch = null,
            KafkaFailure? failure = null,
            Func<RecordReadRequest, RecordReadBatch>? batchFactory = null,
            Func<RecordReadRequest, KafkaFailure?>? failureFactory = null)
        {
            _batch =
                batch ??
                EmptyBatch();
            _failure =
                failure;
            _batchFactory =
                batchFactory;
            _failureFactory =
                failureFactory;
        }

        public int Calls { get; private set; }
        public RecordReadRequest? LastRequest { get; private set; }
        public List<RecordReadRequest> Requests { get; } = [];

        public Task<KafkaResult<RecordReadBatch>>
            ReadPageAsync(
                RecordReadRequest request,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            Calls++;
            LastRequest = request;
            Requests.Add(request);

            var observedAt =
                DateTimeOffset.UtcNow;
            var observation =
                new ObservationMetadata(
                    observedAt,
                    observedAt,
                    observedAt,
                    ObservationSource.Live);

            var failure =
                _failureFactory?.Invoke(request) ??
                _failure;

            return Task.FromResult(
                failure is null
                    ? KafkaResult<RecordReadBatch>
                        .Success(
                            _batchFactory?.Invoke(request) ??
                            _batch!,
                            observation)
                    : KafkaResult<RecordReadBatch>
                        .Failed(
                            failure,
                            observation));
        }
    }

    private sealed class PartialProgressEvaluator :
        IDataQualityBatchEvaluator
    {
        private readonly long _nextOffset;
        private readonly long _evaluatedRecords;
        private readonly DateTimeOffset _now;

        public PartialProgressEvaluator(
            long nextOffset,
            long evaluatedRecords,
            DateTimeOffset now)
        {
            _nextOffset =
                nextOffset;
            _evaluatedRecords =
                evaluatedRecords;
            _now =
                now;
        }

        public Task<DataQualityEvaluationResult>
            EvaluateAsync(
                DataQualityPolicyDefinition policy,
                DataQualityEvaluationInput input,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            var evidence =
                new DataQualityAggregateEvidence(
                    policy.PolicyId,
                    policy.Version,
                    input.WindowStartUtc,
                    input.WindowEndUtc,
                    _evaluatedRecords,
                    _evaluatedRecords * 2,
                    0,
                    [
                        new DataQualityRuleViolationCount(
                            "id-required",
                            0),
                    ],
                    DataQualityEvidenceState.Partial,
                    DataQualityEvidencePoint
                        .BoundedEvaluatorSource);

            var progress =
                new DataQualityEvaluationProgress(
                    policy.PolicyId,
                    policy.Version,
                    input.ClusterId,
                    input.TopicName,
                    input.Partition,
                    input.WindowStartUtc,
                    input.WindowEndUtc,
                    input.StartOffset,
                    input.EndOffsetExclusive,
                    _nextOffset,
                    _evaluatedRecords,
                    _evaluatedRecords * 2,
                    DataQualityEvidenceState.Partial,
                    DataQualityEvaluationOutcome.DurationLimit,
                    _now);

            return Task.FromResult(
                new DataQualityEvaluationResult(
                    evidence,
                    progress));
        }
    }

    private sealed class CancellingEvaluator :
        IDataQualityBatchEvaluator
    {
        private readonly CancellationTokenSource
            _cancellation;
        private readonly DateTimeOffset
            _now;

        public CancellingEvaluator(
            CancellationTokenSource cancellation,
            DateTimeOffset now)
        {
            _cancellation =
                cancellation;
            _now =
                now;
        }

        public Task<DataQualityEvaluationResult>
            EvaluateAsync(
                DataQualityPolicyDefinition policy,
                DataQualityEvaluationInput input,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            _cancellation.Cancel();

            var evidence =
                new DataQualityAggregateEvidence(
                    policy.PolicyId,
                    policy.Version,
                    input.WindowStartUtc,
                    input.WindowEndUtc,
                    0,
                    0,
                    0,
                    [
                        new DataQualityRuleViolationCount(
                            "id-required",
                            0),
                    ],
                    DataQualityEvidenceState.Unknown,
                    DataQualityEvidencePoint
                        .BoundedEvaluatorSource);

            var progress =
                new DataQualityEvaluationProgress(
                    policy.PolicyId,
                    policy.Version,
                    input.ClusterId,
                    input.TopicName,
                    input.Partition,
                    input.WindowStartUtc,
                    input.WindowEndUtc,
                    input.StartOffset,
                    input.EndOffsetExclusive,
                    input.StartOffset,
                    0,
                    0,
                    DataQualityEvidenceState.Unknown,
                    DataQualityEvaluationOutcome.Cancelled,
                    _now);

            return Task.FromResult(
                new DataQualityEvaluationResult(
                    evidence,
                    progress));
        }
    }

    private sealed class FakeEvaluator :
        IDataQualityBatchEvaluator
    {
        private readonly DateTimeOffset?
            _now;

        public FakeEvaluator(
            DateTimeOffset? now = null)
        {
            _now = now;
        }

        public int Calls { get; private set; }

        public Task<DataQualityEvaluationResult>
            EvaluateAsync(
                DataQualityPolicyDefinition policy,
                DataQualityEvaluationInput input,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            Calls++;

            var evidence =
                new DataQualityAggregateEvidence(
                    policy.PolicyId,
                    policy.Version,
                    input.WindowStartUtc,
                    input.WindowEndUtc,
                    input.Records.Count,
                    input.RawByteCount,
                    0,
                    [
                        new DataQualityRuleViolationCount(
                            "id-required",
                            0),
                    ],
                    DataQualityEvidenceState.Available,
                    DataQualityEvidencePoint
                        .BoundedEvaluatorSource);

            var progress =
                new DataQualityEvaluationProgress(
                    policy.PolicyId,
                    policy.Version,
                    input.ClusterId,
                    input.TopicName,
                    input.Partition,
                    input.WindowStartUtc,
                    input.WindowEndUtc,
                    input.StartOffset,
                    input.EndOffsetExclusive,
                    input.EndOffsetExclusive,
                    input.Records.Count,
                    input.RawByteCount,
                    DataQualityEvidenceState.Available,
                    DataQualityEvaluationOutcome.Complete,
                    _now ??
                    input.WindowEndUtc);

            return Task.FromResult(
                new DataQualityEvaluationResult(
                    evidence,
                    progress));
        }
    }

    private sealed class FakeStore :
        IDataQualityLifecycleStore
    {
        private IReadOnlyList<
            DataQualityPolicyLifecycleSnapshot>
            _items;

        public FakeStore(
            IReadOnlyList<
                DataQualityPolicyLifecycleSnapshot>
                items)
        {
            _items = items;
        }

        public List<DataQualityEvidencePoint> Appended { get; } = [];
        public List<DataQualityPolicyListQuery> ListQueries { get; } = [];

        public void ReplaceItems(
            IReadOnlyList<
                DataQualityPolicyLifecycleSnapshot>
                items)
        {
            ArgumentNullException.ThrowIfNull(
                items);
            _items =
                items;
        }

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<DataQualityPolicyLifecycleSnapshot?>
            GetPolicyAsync(
                string policyId,
                CancellationToken cancellationToken = default) =>
            Task.FromResult<
                DataQualityPolicyLifecycleSnapshot?>(
                _items.FirstOrDefault(
                    item =>
                        item.Definition.PolicyId ==
                        policyId));

        public Task<DataQualityPolicyPage>
            ListPoliciesAsync(
                DataQualityPolicyListQuery query,
                CancellationToken cancellationToken = default)
        {
            ListQueries.Add(query);

            var candidates =
                _items
                    .Where(
                        item =>
                            item.State ==
                                DataQualityPolicyLifecycleState.Active &&
                            item.Definition.Scope.ClusterId ==
                                query.ClusterId &&
                            (query.AfterPolicyId is null ||
                             string.CompareOrdinal(
                                 item.Definition.PolicyId,
                                 query.AfterPolicyId) > 0))
                    .OrderBy(
                        item =>
                            item.Definition.PolicyId,
                        StringComparer.Ordinal)
                    .Take(query.MaxResults + 1)
                    .ToArray();
            var truncated =
                candidates.Length >
                query.MaxResults;
            var items =
                candidates
                    .Take(query.MaxResults)
                    .ToArray();

            return Task.FromResult(
                new DataQualityPolicyPage(
                    items,
                    truncated,
                    truncated
                        ? items[^1].Definition.PolicyId
                        : null));
        }

        public Task<DataQualityPolicyLifecycleSnapshot>
            CreatePolicyAsync(
                DataQualityPolicyDefinition definition,
                DataQualityPolicyLifecycleState state,
                DateTimeOffset updatedAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<DataQualityPolicyLifecycleSnapshot?>
            ReplacePolicyAsync(
                DataQualityPolicyDefinition definition,
                DataQualityPolicyLifecycleState state,
                long expectedRevision,
                DateTimeOffset updatedAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<DataQualityPolicyLifecycleSnapshot?>
            SetPolicyStateAsync(
                string policyId,
                DataQualityPolicyLifecycleState state,
                long expectedRevision,
                DateTimeOffset updatedAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task AppendEvidenceAsync(
            DataQualityEvidencePoint point,
            CancellationToken cancellationToken = default)
        {
            Appended.Add(point);
            return Task.CompletedTask;
        }

        public Task<DataQualityEvidencePage>
            QueryEvidenceAsync(
                DataQualityEvidenceQuery query,
                CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new DataQualityEvidencePage(
                    [],
                    truncated: false));
    }

    private sealed class MutableTimeProvider :
        TimeProvider
    {
        private DateTimeOffset _now;

        public MutableTimeProvider(
            DateTimeOffset now)
        {
            _now =
                now;
        }

        public override DateTimeOffset
            GetUtcNow() =>
            _now;

        public void Advance(
            TimeSpan delta)
        {
            if (delta < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(delta));
            }

            _now +=
                delta;
        }
    }

    private sealed class FixedTimeProvider(
        DateTimeOffset now) :
        TimeProvider
    {
        public override DateTimeOffset
            GetUtcNow() =>
            now;
    }
}
