using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W57GovernedDataJobActivationTests
{
    private static readonly DateTimeOffset Now =
        DateTimeOffset.Parse("2026-09-27T09:00:00Z");

    [Fact]
    public async Task Activation_persists_progress_and_atomic_conflict_set_before_reporting_verified()
    {
        var plan = Plan();
        var operation = ExecutingOperation(plan);
        var store = new FakeFleetStore();
        var handler = Handler(store);

        var result = await handler.ExecuteAsync(
            new MutationExecutionContext(
                operation,
                new MutationExecutionMaterial()));

        Assert.Equal(
            MutationExecutionResultKind.AppliedVerified,
            result.ResultKind);
        Assert.Equal(
            "data_job_activated",
            result.ResultCode);
        Assert.NotNull(store.Progress);
        Assert.Equal(
            FleetProgressPhase.Observing,
            store.Progress!.Phase);
        Assert.NotEmpty(store.Obligations);
        Assert.All(
            store.Obligations,
            obligation =>
            {
                Assert.Equal(
                    operation.OperationId,
                    obligation.OperationId);
                Assert.Equal(
                    plan.PlanFingerprint,
                    obligation.EffectFingerprint);
                Assert.True(
                    obligation.BlocksConflictingDispatch);
            });
    }

    [Fact]
    public async Task Conflict_admission_fails_before_external_effect_and_stops_subordinate_progress()
    {
        var plan = Plan();
        var operation = ExecutingOperation(plan);
        var store = new FakeFleetStore
        {
            ObligationOutcome =
                FleetConflictObligationBatchCreateOutcome
                    .FleetConflictScopeConflict,
        };
        var handler = Handler(store);

        var result = await handler.ExecuteAsync(
            new MutationExecutionContext(
                operation,
                new MutationExecutionMaterial()));

        Assert.Equal(
            MutationExecutionResultKind.FailedBeforeDispatch,
            result.ResultKind);
        Assert.Equal(
            "data_job_fleet_conflict",
            result.ResultCode);
        Assert.NotNull(store.Progress);
        Assert.Equal(
            FleetProgressPhase.Stopped,
            store.Progress!.Phase);
        Assert.Null(
            store.Progress.Transfer!.PendingBatch);
    }

    [Fact]
    public async Task Same_plan_activation_retry_is_idempotent()
    {
        var plan = Plan();
        var operation = ExecutingOperation(plan);
        var store = new FakeFleetStore();
        var handler = Handler(store);

        var first = await handler.ExecuteAsync(
            new MutationExecutionContext(
                operation,
                new MutationExecutionMaterial()));

        store.ObligationOutcome =
            FleetConflictObligationBatchCreateOutcome
                .ExistingSameEffects;

        var second = await handler.ExecuteAsync(
            new MutationExecutionContext(
                operation,
                new MutationExecutionMaterial()));

        Assert.Equal(
            MutationExecutionResultKind.AppliedVerified,
            first.ResultKind);
        Assert.Equal(
            MutationExecutionResultKind.AppliedVerified,
            second.ResultKind);
        Assert.Equal(
            "data_job_activated",
            second.ResultCode);
    }

    [Fact]
    public async Task Atomic_conflict_set_never_exceeds_store_batch_ceiling()
    {
        var plan = Plan(rangeCount: GovernedDataJobPolicy.MaxRanges);
        var operation = ExecutingOperation(plan);
        var store = new FakeFleetStore();
        var handler = Handler(store);

        var result = await handler.ExecuteAsync(
            new MutationExecutionContext(
                operation,
                new MutationExecutionMaterial()));

        Assert.Equal(
            MutationExecutionResultKind.AppliedVerified,
            result.ResultKind);
        Assert.InRange(
            store.Obligations.Count,
            1,
            100);
    }

    private static GovernedDataJobActivationHandler Handler(
        FakeFleetStore store) =>
        new(
            store,
            new GovernedDataJobStateCoordinator(store),
            new FixedTimeProvider());

    private static GovernedDataJobPlan Plan(
        int rangeCount = 1)
    {
        var source = new ClusterTransferEndpoint(
            "source",
            "source-v1",
            "physical-source");
        var destination = new ClusterTransferEndpoint(
            "destination",
            "destination-v1",
            "physical-destination");

        var mappings = Enumerable
            .Range(0, rangeCount)
            .Select(index =>
                new ClusterTransferMapping(
                    $"source-{index}",
                    0,
                    $"destination-{index}",
                    0,
                    10,
                    20,
                    Sha((char)('a' + index % 6)),
                    Sha((char)('a' + (index + 1) % 6))))
            .ToArray();

        var budget = new ClusterTransferBudget(
            maxBatchRecords: 10,
            maxBatchBytes: 1024,
            maxTotalRecords: 1000,
            maxTotalBytes: 1024 * 1024,
            maxDuration: TimeSpan.FromMinutes(10),
            maxRecordsPerSecond: 100,
            maxBytesPerSecond: 1024 * 1024);
        var policy = new ClusterTransferDataPolicy(
            "none",
            1,
            Sha('f'));

        var transfer = new ClusterTransferPlan(
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

    private static MutationOperationSnapshot ExecutingOperation(
        GovernedDataJobPlan plan)
    {
        var requester =
            "oidc:https://idp.example|data-job-user";
        var operation = MutationOperation.CreatePreview(
            requester,
            GovernedDataJobPolicy.BuildIntent(plan),
            GovernedDataJobPolicy.ClassifyRisk(plan),
            "v0.7-w57-activation-test",
            Now.AddMinutes(10),
            Now,
            "w57-activation");

        operation.OpenForConfirmation(
            Now.AddSeconds(1));
        operation.Confirm(
            requester,
            operation.Snapshot.PreviewHash,
            Now.AddSeconds(2),
            operation.Snapshot.ConfirmationChallenge);

        if (operation.Snapshot.State ==
            MutationOperationState.AwaitingApproval)
        {
            operation.Approve(
                new MutationApprovalAuthorizationEvidence(
                    operation.Snapshot.OperationId,
                    "oidc:https://idp.example|approver",
                    operation.Snapshot.PreviewHash,
                    Sha('e')),
                operation.Snapshot.PreviewHash,
                Now.AddMilliseconds(2500));
        }

        _ = operation.ClaimExecution(
            Now.AddSeconds(3),
            Now.AddMinutes(5));
        return operation.Snapshot;
    }

    private static string Sha(char value) =>
        new(value, 64);

    private sealed class FixedTimeProvider :
        TimeProvider
    {
        public override DateTimeOffset GetUtcNow() =>
            Now;
    }

    private sealed class FakeFleetStore :
        IFleetMutationStateStore
    {
        public FleetOperationProgressSnapshot? Progress
        {
            get;
            private set;
        }

        public List<FleetConflictObligationSnapshot>
            Obligations { get; } = [];

        public FleetConflictObligationBatchCreateOutcome
            ObligationOutcome { get; set; } =
            FleetConflictObligationBatchCreateOutcome.Created;

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<FleetProgressCreateResult>
            CreateProgressAsync(
                FleetOperationProgressSnapshot progress,
                CancellationToken cancellationToken = default)
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
                CancellationToken cancellationToken = default) =>
            Task.FromResult(
                Progress?.OperationId == operationId
                    ? Progress
                    : null);

        public Task<FleetProgressSaveResult>
            TrySaveProgressAsync(
                FleetOperationProgressSnapshot progress,
                long expectedVersion,
                CancellationToken cancellationToken = default)
        {
            if (Progress is null)
            {
                return Task.FromResult(
                    new FleetProgressSaveResult(
                        FleetProgressSaveOutcome.NotFound,
                        null));
            }

            if (Progress.Version != expectedVersion)
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

        public Task<FleetConflictObligationBatchCreateResult>
            CreateConflictObligationsAsync(
                IReadOnlyList<FleetConflictObligationSnapshot> obligations,
                CancellationToken cancellationToken = default)
        {
            Obligations.Clear();
            Obligations.AddRange(obligations);

            return Task.FromResult(
                new FleetConflictObligationBatchCreateResult(
                    ObligationOutcome,
                    ObligationOutcome is
                        FleetConflictObligationBatchCreateOutcome.Created or
                        FleetConflictObligationBatchCreateOutcome.ExistingSameEffects
                            ? obligations
                            : Array.Empty<FleetConflictObligationSnapshot>()));
        }

        public Task<FleetConflictObligationCreateResult>
            CreateConflictObligationAsync(
                FleetConflictObligationSnapshot obligation,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSnapshot?>
            GetConflictObligationAsync(
                Guid obligationId,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSnapshot?>
            FindBlockingConflictObligationAsync(
                string conflictKey,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSaveResult>
            TrySaveConflictObligationAsync(
                FleetConflictObligationSnapshot obligation,
                long expectedVersion,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
