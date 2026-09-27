using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W57GovernedDataJobProgressTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 8, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Initial_progress_binds_exact_plan_and_range_starts_without_payloads()
    {
        var plan = Plan();
        var operationId = Guid.NewGuid();

        var snapshot =
            GovernedDataJobProgress.CreateInitial(
                operationId,
                workerGeneration: 1,
                plan,
                Now);

        Assert.Equal(operationId, snapshot.OperationId);
        Assert.Equal(1, snapshot.WorkerGeneration);
        Assert.Equal(
            FleetProgressPhase.Observing,
            snapshot.Phase);
        Assert.NotNull(snapshot.Transfer);
        Assert.Equal(
            plan.PlanFingerprint,
            snapshot.Transfer!.PlanFingerprint);
        Assert.Equal(
            plan.Ranges
                .Select(range => range.StartInclusive)
                .ToArray(),
            snapshot.Transfer.Checkpoints
                .Select(item => item.NextSourceOffset)
                .ToArray());
        Assert.Null(snapshot.Transfer.PendingBatch);

        var json =
            System.Text.Json.JsonSerializer.Serialize(
                snapshot);
        Assert.DoesNotContain(
            "payload",
            json,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "recordValue",
            json,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Only_one_unresolved_batch_can_exist()
    {
        var plan = Plan();
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot =
            GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                rangeIndex: 0,
                sourceOffset:
                    plan.Ranges[0].StartInclusive,
                destinationPartition: 0,
                rawBytes: 128,
                Now);

        Assert.NotNull(snapshot.Transfer!.PendingBatch);

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobProgress.ReserveBeforeDispatch(
                    snapshot,
                    plan,
                    rangeIndex: 0,
                    sourceOffset:
                        plan.Ranges[0].StartInclusive,
                    destinationPartition: 0,
                    rawBytes: 64,
                    Now.AddSeconds(1)));
    }

    [Fact]
    public void Crash_before_dispatch_can_release_without_advancing_checkpoint()
    {
        var plan = Plan();
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot =
            GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                plan.Ranges[0].StartInclusive,
                0,
                128,
                Now);

        var batchId =
            snapshot.Transfer!.PendingBatch!.BatchId;
        snapshot =
            GovernedDataJobProgress.ReleaseBeforeDispatch(
                snapshot,
                batchId,
                Now.AddSeconds(1));

        Assert.Null(snapshot.Transfer!.PendingBatch);
        Assert.Equal(
            plan.Ranges[0].StartInclusive,
            snapshot.Transfer.Checkpoints[0]
                .NextSourceOffset);
        Assert.Equal(
            0,
            snapshot.Transfer.AcknowledgedRecords);
    }

    [Fact]
    public void Ambiguous_dispatch_retains_pending_batch_and_blocks_new_dispatch()
    {
        var plan = Plan();
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot =
            GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                plan.Ranges[0].StartInclusive,
                0,
                128,
                Now);

        var batchId =
            snapshot.Transfer!.PendingBatch!.BatchId;
        snapshot =
            GovernedDataJobProgress.MarkDispatchStarted(
                snapshot,
                batchId,
                Now.AddSeconds(1));
        snapshot =
            GovernedDataJobProgress.MarkAmbiguous(
                snapshot,
                batchId,
                Now.AddSeconds(2));

        Assert.Equal(
            FleetProgressPhase.WaitingForExternalAction,
            snapshot.Phase);
        Assert.NotNull(snapshot.Transfer!.PendingBatch);
        Assert.Equal(
            FleetTransferBatchState.DispatchStarted,
            snapshot.Transfer.PendingBatch!.State);

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobProgress.EnsureCanDispatch(
                    snapshot,
                    plan));

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobProgress.ReserveBeforeDispatch(
                    snapshot,
                    plan,
                    0,
                    plan.Ranges[0].StartInclusive,
                    0,
                    64,
                    Now.AddSeconds(3)));
    }

    [Fact]
    public void Proven_non_application_allows_same_checkpoint_to_be_retried_only_after_reconciliation()
    {
        var plan = Plan();
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot =
            GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                plan.Ranges[0].StartInclusive,
                0,
                128,
                Now);
        var batchId =
            snapshot.Transfer!.PendingBatch!.BatchId;

        snapshot =
            GovernedDataJobProgress.MarkDispatchStarted(
                snapshot,
                batchId,
                Now.AddSeconds(1));
        snapshot =
            GovernedDataJobProgress.ResolveProvenNonApplication(
                snapshot,
                batchId,
                Now.AddSeconds(2));

        GovernedDataJobProgress.EnsureCanDispatch(
            snapshot,
            plan);

        var retried =
            GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                plan.Ranges[0].StartInclusive,
                0,
                64,
                Now.AddSeconds(3));

        Assert.NotNull(retried.Transfer!.PendingBatch);
        Assert.Equal(
            plan.Ranges[0].StartInclusive,
            retried.Transfer.PendingBatch!.SourceOffset);
    }

    [Fact]
    public void Acknowledgement_advances_exactly_one_source_offset_and_charges_budget()
    {
        var plan = Plan();
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot =
            GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                plan.Ranges[0].StartInclusive,
                0,
                128,
                Now);
        var batchId =
            snapshot.Transfer!.PendingBatch!.BatchId;

        snapshot =
            GovernedDataJobProgress.MarkDispatchStarted(
                snapshot,
                batchId,
                Now.AddSeconds(1));
        snapshot =
            GovernedDataJobProgress.CompleteAcknowledged(
                snapshot,
                plan,
                batchId,
                plan.Ranges[0].StartInclusive + 1,
                Now.AddSeconds(2));

        Assert.Equal(
            plan.Ranges[0].StartInclusive + 1,
            snapshot.Transfer!.Checkpoints[0]
                .NextSourceOffset);
        Assert.Equal(
            1,
            snapshot.Transfer.AcknowledgedRecords);
        Assert.Equal(
            128,
            snapshot.Transfer.AcknowledgedBytes);
        Assert.Null(snapshot.Transfer.PendingBatch);
    }

    [Fact]
    public void Record_and_byte_budgets_fail_closed_at_cap_plus_one()
    {
        var plan = Plan(
            maxTotalRecords: 1,
            maxTotalBytes: 128);
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot =
            GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                plan.Ranges[0].StartInclusive,
                0,
                128,
                Now);
        var batchId =
            snapshot.Transfer!.PendingBatch!.BatchId;
        snapshot =
            GovernedDataJobProgress.MarkDispatchStarted(
                snapshot,
                batchId,
                Now.AddSeconds(1));
        snapshot =
            GovernedDataJobProgress.CompleteAcknowledged(
                snapshot,
                plan,
                batchId,
                plan.Ranges[0].StartInclusive + 1,
                Now.AddSeconds(2));

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobProgress.EnsureCanDispatch(
                    snapshot,
                    plan));

        var fresh = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobProgress.ReserveBeforeDispatch(
                    fresh,
                    plan,
                    0,
                    plan.Ranges[0].StartInclusive,
                    0,
                    129,
                    Now));
    }

    [Fact]
    public void Record_rate_budget_fails_closed_at_cap_plus_one_and_uses_durable_elapsed()
    {
        var plan = Plan(
            maxTotalRecords: 10,
            maxTotalBytes: 4096,
            maxRecordsPerSecond: 1,
            maxBytesPerSecond: 4096);
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot = GovernedDataJobProgress.ReserveBeforeDispatch(
            snapshot,
            plan,
            0,
            plan.Ranges[0].StartInclusive,
            0,
            64,
            Now);
        var batchId = snapshot.Transfer!.PendingBatch!.BatchId;
        snapshot = GovernedDataJobProgress.MarkDispatchStarted(
            snapshot,
            batchId,
            Now);
        snapshot = GovernedDataJobProgress.CompleteAcknowledged(
            snapshot,
            plan,
            batchId,
            plan.Ranges[0].StartInclusive + 1,
            Now);

        Assert.Throws<MutationStateException>(
            () => GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                plan.Ranges[0].StartInclusive + 1,
                0,
                64,
                Now));

        snapshot = GovernedDataJobProgress.ChargeRuntime(
            snapshot,
            plan,
            TimeSpan.FromSeconds(2),
            Now.AddSeconds(2));

        var allowed = GovernedDataJobProgress.ReserveBeforeDispatch(
            snapshot,
            plan,
            0,
            plan.Ranges[0].StartInclusive + 1,
            0,
            64,
            Now.AddSeconds(2));

        Assert.NotNull(allowed.Transfer!.PendingBatch);
    }

    [Fact]
    public void Byte_rate_budget_fails_closed_at_cap_plus_one_and_uses_durable_elapsed()
    {
        var plan = Plan(
            maxTotalRecords: 10,
            maxTotalBytes: 4096,
            maxRecordsPerSecond: 10,
            maxBytesPerSecond: 100);
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot = GovernedDataJobProgress.ReserveBeforeDispatch(
            snapshot,
            plan,
            0,
            plan.Ranges[0].StartInclusive,
            0,
            60,
            Now);
        var batchId = snapshot.Transfer!.PendingBatch!.BatchId;
        snapshot = GovernedDataJobProgress.MarkDispatchStarted(
            snapshot,
            batchId,
            Now);
        snapshot = GovernedDataJobProgress.CompleteAcknowledged(
            snapshot,
            plan,
            batchId,
            plan.Ranges[0].StartInclusive + 1,
            Now);

        Assert.Throws<MutationStateException>(
            () => GovernedDataJobProgress.ReserveBeforeDispatch(
                snapshot,
                plan,
                0,
                plan.Ranges[0].StartInclusive + 1,
                0,
                41,
                Now));

        snapshot = GovernedDataJobProgress.ChargeRuntime(
            snapshot,
            plan,
            TimeSpan.FromSeconds(2),
            Now.AddSeconds(2));

        var allowed = GovernedDataJobProgress.ReserveBeforeDispatch(
            snapshot,
            plan,
            0,
            plan.Ranges[0].StartInclusive + 1,
            0,
            41,
            Now.AddSeconds(2));

        Assert.NotNull(allowed.Transfer!.PendingBatch);
    }

    [Fact]
    public void Failover_requires_monotonic_worker_generation()
    {
        var plan = Plan();
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            5,
            plan,
            Now);

        Assert.Throws<MutationStateException>(
            () => GovernedDataJobProgress.Fence(
                snapshot,
                5,
                Now.AddSeconds(1)));

        var fenced = GovernedDataJobProgress.Fence(
            snapshot,
            6,
            Now.AddSeconds(1));

        Assert.Equal(6, fenced.WorkerGeneration);
        Assert.Equal(
            snapshot.Version + 1,
            fenced.Version);
    }

    [Fact]
    public void Duration_budget_is_cumulative_and_does_not_reset_on_fence()
    {
        var plan = Plan(
            maxDuration: TimeSpan.FromSeconds(10));
        var snapshot = GovernedDataJobProgress.CreateInitial(
            Guid.NewGuid(),
            1,
            plan,
            Now);

        snapshot =
            GovernedDataJobProgress.ChargeRuntime(
                snapshot,
                plan,
                TimeSpan.FromSeconds(6),
                Now.AddSeconds(6));

        snapshot =
            GovernedDataJobProgress.Fence(
                snapshot,
                2,
                Now.AddSeconds(7));

        Assert.Equal(
            TimeSpan.FromSeconds(6),
            snapshot.ActiveObservationElapsed);

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobProgress.ChargeRuntime(
                    snapshot,
                    plan,
                    TimeSpan.FromSeconds(5),
                    Now.AddSeconds(12)));
    }

    private static GovernedDataJobPlan Plan(
        long maxTotalRecords = 10,
        long maxTotalBytes = 1024,
        TimeSpan? maxDuration = null,
        int maxRecordsPerSecond = 10,
        long maxBytesPerSecond = 1024)
    {
        var source =
            new ClusterTransferEndpoint(
                "source",
                "v1",
                "physical-source");
        var destination =
            new ClusterTransferEndpoint(
                "destination",
                "v1",
                "physical-destination");
        var mappings =
            new[]
            {
                new ClusterTransferMapping(
                    "orders.dlq",
                    0,
                    "orders.replay",
                    0,
                    100,
                    110,
                    Sha('a'),
                    Sha('b')),
            };
        var budget =
            new ClusterTransferBudget(
                maxBatchRecords: 1,
                maxBatchBytes: 512,
                maxTotalRecords: maxTotalRecords,
                maxTotalBytes: maxTotalBytes,
                maxDuration:
                    maxDuration ??
                    TimeSpan.FromMinutes(5),
                maxRecordsPerSecond: maxRecordsPerSecond,
                maxBytesPerSecond: maxBytesPerSecond);
        var policy =
            new ClusterTransferDataPolicy(
                "default",
                1,
                Sha('c'));
        var transfer =
            new ClusterTransferPlan(
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

        return GovernedDataJobPolicy.FromTransfer(
            GovernedDataJobKind.Forward,
            transfer);
    }

    private static string Sha(char value) =>
        new(value, 64);
}
