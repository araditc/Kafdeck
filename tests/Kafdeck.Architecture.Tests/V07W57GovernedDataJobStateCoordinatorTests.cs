using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W57GovernedDataJobStateCoordinatorTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 9, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Initialization_is_idempotent_only_for_same_plan()
    {
        var store = new FakeFleetStore();
        var coordinator =
            new GovernedDataJobStateCoordinator(store);
        var plan = Plan();
        var operationId = Guid.NewGuid();

        var first = await coordinator.InitializeAsync(
            operationId,
            1,
            plan,
            Now);
        var second = await coordinator.InitializeAsync(
            operationId,
            1,
            plan,
            Now);

        Assert.Equal(
            GovernedDataJobStateOutcome.Applied,
            first.Outcome);
        Assert.Equal(
            GovernedDataJobStateOutcome.Existing,
            second.Outcome);

        var other = plan with
        {
            PlanFingerprint =
                new string('f', 64),
        };

        var conflict = await coordinator.InitializeAsync(
            operationId,
            1,
            other,
            Now);

        Assert.Equal(
            GovernedDataJobStateOutcome.InvalidState,
            conflict.Outcome);
        Assert.Equal(
            "data_job_existing_plan_conflict",
            conflict.Code);
    }

    [Fact]
    public async Task Stale_worker_cannot_mutate_persisted_progress()
    {
        var store = new FakeFleetStore();
        var coordinator =
            new GovernedDataJobStateCoordinator(store);
        var plan = Plan();
        var operationId = Guid.NewGuid();

        _ = await coordinator.InitializeAsync(
            operationId,
            5,
            plan,
            Now);

        var result =
            await coordinator.ReserveBeforeDispatchAsync(
                operationId,
                plan,
                workerGeneration: 4,
                rangeIndex: 0,
                sourceOffset:
                    plan.Ranges[0].StartInclusive,
                destinationPartition: 0,
                rawBytes: 64,
                Now.AddSeconds(1));

        Assert.Equal(
            GovernedDataJobStateOutcome.StaleWorker,
            result.Outcome);
        Assert.Equal(0, store.SaveCalls);
        Assert.Null(
            store.Progress!.Transfer!.PendingBatch);
    }

    [Fact]
    public async Task Optimistic_version_conflict_fails_closed()
    {
        var store = new FakeFleetStore();
        var coordinator =
            new GovernedDataJobStateCoordinator(store);
        var plan = Plan();
        var operationId = Guid.NewGuid();

        _ = await coordinator.InitializeAsync(
            operationId,
            1,
            plan,
            Now);

        store.ForceVersionConflict = true;

        var result =
            await coordinator.ReserveBeforeDispatchAsync(
                operationId,
                plan,
                1,
                0,
                plan.Ranges[0].StartInclusive,
                0,
                64,
                Now.AddSeconds(1));

        Assert.Equal(
            GovernedDataJobStateOutcome.VersionConflict,
            result.Outcome);
        Assert.Equal(
            "data_job_progress_version_conflict",
            result.Code);
        Assert.Null(
            store.Progress!.Transfer!.PendingBatch);
    }

    [Fact]
    public async Task Ambiguous_batch_survives_reload_and_blocks_followup_dispatch()
    {
        var store = new FakeFleetStore();
        var coordinator =
            new GovernedDataJobStateCoordinator(store);
        var plan = Plan();
        var operationId = Guid.NewGuid();

        _ = await coordinator.InitializeAsync(
            operationId,
            1,
            plan,
            Now);

        var reserved =
            await coordinator.ReserveBeforeDispatchAsync(
                operationId,
                plan,
                1,
                0,
                plan.Ranges[0].StartInclusive,
                0,
                64,
                Now.AddSeconds(1));

        var batchId =
            reserved.Progress!.Transfer!
                .PendingBatch!.BatchId;

        _ = await coordinator.MarkDispatchStartedAsync(
            operationId,
            plan,
            1,
            batchId,
            Now.AddSeconds(2));

        var ambiguous =
            await coordinator.MarkAmbiguousAsync(
                operationId,
                plan,
                1,
                batchId,
                Now.AddSeconds(3));

        Assert.Equal(
            GovernedDataJobStateOutcome.Applied,
            ambiguous.Outcome);

        var reloaded =
            await coordinator.GetAsync(
                operationId,
                plan);

        Assert.NotNull(
            reloaded.Progress!.Transfer!.PendingBatch);
        Assert.Equal(
            FleetProgressPhase.WaitingForExternalAction,
            reloaded.Progress.Phase);

        Assert.Throws<MutationStateException>(
            () =>
                GovernedDataJobProgress.EnsureCanDispatch(
                    reloaded.Progress,
                    plan));
    }

    [Fact]
    public async Task Fence_advances_generation_without_resetting_counters()
    {
        var store = new FakeFleetStore();
        var coordinator =
            new GovernedDataJobStateCoordinator(store);
        var plan = Plan();
        var operationId = Guid.NewGuid();

        _ = await coordinator.InitializeAsync(
            operationId,
            1,
            plan,
            Now);

        var charged =
            await coordinator.ChargeRuntimeAsync(
                operationId,
                plan,
                1,
                TimeSpan.FromSeconds(5),
                Now.AddSeconds(5));

        var fenced =
            await coordinator.FenceAsync(
                operationId,
                plan,
                2,
                Now.AddSeconds(6));

        Assert.Equal(
            GovernedDataJobStateOutcome.Applied,
            fenced.Outcome);
        Assert.Equal(
            2,
            fenced.Progress!.WorkerGeneration);
        Assert.Equal(
            charged.Progress!.ActiveObservationElapsed,
            fenced.Progress.ActiveObservationElapsed);
        Assert.Equal(
            charged.Progress.Transfer!
                .AcknowledgedRecords,
            fenced.Progress.Transfer!
                .AcknowledgedRecords);
    }

    private static GovernedDataJobPlan Plan()
    {
        var source = new ClusterTransferEndpoint(
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
                    10,
                    20,
                    Sha('a'),
                    Sha('b')),
            };
        var budget =
            new ClusterTransferBudget(
                maxBatchRecords: 1,
                maxBatchBytes: 512,
                maxTotalRecords: 10,
                maxTotalBytes: 1024,
                maxDuration:
                    TimeSpan.FromMinutes(5),
                maxRecordsPerSecond: 10,
                maxBytesPerSecond: 1024);
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

    private sealed class FakeFleetStore :
        IFleetMutationStateStore
    {
        public FleetOperationProgressSnapshot?
            Progress { get; private set; }

        public int SaveCalls { get; private set; }

        public bool ForceVersionConflict
        {
            get;
            set;
        }

        public Task InitializeAsync(
            CancellationToken cancellationToken =
                default) =>
            Task.CompletedTask;

        public Task<FleetProgressCreateResult>
            CreateProgressAsync(
                FleetOperationProgressSnapshot progress,
                CancellationToken cancellationToken =
                    default)
        {
            if (Progress is null)
            {
                Progress = progress;
                return Task.FromResult(
                    new FleetProgressCreateResult(
                        FleetProgressCreateOutcome.Created,
                        Progress));
            }

            return Task.FromResult(
                new FleetProgressCreateResult(
                    FleetProgressCreateOutcome.Existing,
                    Progress));
        }

        public Task<FleetOperationProgressSnapshot?>
            GetProgressAsync(
                Guid operationId,
                CancellationToken cancellationToken =
                    default) =>
            Task.FromResult(
                Progress is not null &&
                Progress.OperationId == operationId
                    ? Progress
                    : null);

        public Task<FleetProgressSaveResult>
            TrySaveProgressAsync(
                FleetOperationProgressSnapshot progress,
                long expectedVersion,
                CancellationToken cancellationToken =
                    default)
        {
            SaveCalls++;

            if (Progress is null)
            {
                return Task.FromResult(
                    new FleetProgressSaveResult(
                        FleetProgressSaveOutcome.NotFound,
                        null));
            }

            if (ForceVersionConflict ||
                Progress.Version != expectedVersion)
            {
                return Task.FromResult(
                    new FleetProgressSaveResult(
                        FleetProgressSaveOutcome.VersionConflict,
                        Progress));
            }

            Progress = progress;
            return Task.FromResult(
                new FleetProgressSaveResult(
                    FleetProgressSaveOutcome.Saved,
                    Progress));
        }

        public Task<FleetConflictObligationCreateResult>
            CreateConflictObligationAsync(
                FleetConflictObligationSnapshot obligation,
                CancellationToken cancellationToken =
                    default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationBatchCreateResult>
            CreateConflictObligationsAsync(
                IReadOnlyList<
                    FleetConflictObligationSnapshot>
                    obligations,
                CancellationToken cancellationToken =
                    default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSnapshot?>
            GetConflictObligationAsync(
                Guid obligationId,
                CancellationToken cancellationToken =
                    default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSnapshot?>
            FindBlockingConflictObligationAsync(
                string conflictKey,
                CancellationToken cancellationToken =
                    default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSaveResult>
            TrySaveConflictObligationAsync(
                FleetConflictObligationSnapshot obligation,
                long expectedVersion,
                CancellationToken cancellationToken =
                    default) =>
            throw new NotSupportedException();
    }
}
