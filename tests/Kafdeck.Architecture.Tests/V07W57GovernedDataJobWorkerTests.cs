using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W57GovernedDataJobWorkerTests
{
    private static readonly DateTimeOffset Now =
        DateTimeOffset.Parse("2026-09-27T08:00:00Z");

    [Fact]
    public async Task Worker_resumes_from_durable_checkpoint_and_completes_finite_range()
    {
        var plan = Plan(start: 10, end: 12);
        var operation = ActivatedOperation(plan);
        var store = new FakeFleetStore();
        var state = new GovernedDataJobStateCoordinator(store);

        _ = await state.InitializeAsync(
            operation.OperationId,
            operation.ExecutionClaimGeneration,
            plan,
            Now);

        var readerPort = new CheckpointReadPort(endExclusive: 12);
        var source = new GovernedDataJobSourceReader(
            new ClusterTransferSourceReader(readerPort));
        var producer = new SequencedProducer(
            MutationExecutionResultKind.AppliedVerified);
        var guard = new AllowGuard();
        var dispatch = new GovernedDataJobDispatchCoordinator(
            state,
            producer,
            guard,
            new FixedTimeProvider(Now));

        var worker = new GovernedDataJobWorker(
            new UnusedOperationRepository(),
            store,
            state,
            source,
            dispatch,
            guard,
            new GovernedDataJobWorkerPolicy(
                discoveryLimit: 10,
                pollInterval: TimeSpan.FromSeconds(1),
                leaseTtl: TimeSpan.FromSeconds(30),
                sourceReadTimeout: TimeSpan.FromSeconds(5)),
            new FixedTimeProvider(Now),
            "worker-a");

        await worker.ProcessOperationOnceAsync(operation);
        var first = await state.GetAsync(
            operation.OperationId,
            plan);

        Assert.Equal(
            11,
            first.Progress!.Transfer!.Checkpoints[0]
                .NextSourceOffset);
        Assert.Equal(
            FleetProgressPhase.Observing,
            first.Progress.Phase);

        await worker.ProcessOperationOnceAsync(operation);
        var second = await state.GetAsync(
            operation.OperationId,
            plan);

        Assert.Equal(
            12,
            second.Progress!.Transfer!.Checkpoints[0]
                .NextSourceOffset);
        Assert.Equal(
            FleetProgressPhase.Completed,
            second.Progress.Phase);
        Assert.Equal(
            new long[] { 10, 11 },
            readerPort.RequestedOffsets);
        Assert.Equal(2, producer.CallCount);
    }

    [Fact]
    public async Task Worker_never_replays_ambiguous_destination_batch()
    {
        var plan = Plan(start: 10, end: 12);
        var operation = ActivatedOperation(plan);
        var store = new FakeFleetStore();
        var state = new GovernedDataJobStateCoordinator(store);

        _ = await state.InitializeAsync(
            operation.OperationId,
            operation.ExecutionClaimGeneration,
            plan,
            Now);

        var readerPort = new CheckpointReadPort(endExclusive: 12);
        var source = new GovernedDataJobSourceReader(
            new ClusterTransferSourceReader(readerPort));
        var producer = new SequencedProducer(
            MutationExecutionResultKind.ExecutionUnknown);
        var guard = new AllowGuard();
        var dispatch = new GovernedDataJobDispatchCoordinator(
            state,
            producer,
            guard,
            new FixedTimeProvider(Now));

        var worker = new GovernedDataJobWorker(
            new UnusedOperationRepository(),
            store,
            state,
            source,
            dispatch,
            guard,
            timeProvider: new FixedTimeProvider(Now),
            workerId: "worker-a");

        await worker.ProcessOperationOnceAsync(operation);
        var ambiguous = await state.GetAsync(
            operation.OperationId,
            plan);

        Assert.Equal(
            FleetProgressPhase.WaitingForExternalAction,
            ambiguous.Progress!.Phase);
        Assert.NotNull(
            ambiguous.Progress.Transfer!.PendingBatch);
        Assert.Equal(1, producer.CallCount);
        Assert.Equal(1, readerPort.CallCount);

        await worker.ProcessOperationOnceAsync(operation);

        Assert.Equal(1, producer.CallCount);
        Assert.Equal(1, readerPort.CallCount);
    }

    private static GovernedDataJobPlan Plan(
        long start,
        long end)
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
                0,
                "orders-copy",
                0,
                start,
                end,
                new string('a', 64),
                new string('b', 64)),
        };
        var budget = new ClusterTransferBudget(
            maxBatchRecords: 1,
            maxBatchBytes: 1024,
            maxTotalRecords: end - start,
            maxTotalBytes: 16 * 1024,
            maxDuration: TimeSpan.FromMinutes(5),
            maxRecordsPerSecond: 100,
            maxBytesPerSecond: 16 * 1024);
        var policy = new ClusterTransferDataPolicy(
            "none",
            1,
            new string('c', 64));
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

    private static MutationOperationSnapshot ActivatedOperation(
        GovernedDataJobPlan plan)
    {
        var requester =
            "oidc:https://idp.example|data-job-user";
        var operation = MutationOperation.CreatePreview(
            requester,
            GovernedDataJobPolicy.BuildIntent(plan),
            GovernedDataJobPolicy.ClassifyRisk(plan),
            "v0.7-w57-worker",
            Now.AddMinutes(10),
            Now,
            "w57-worker");

        operation.OpenForConfirmation(
            Now.AddMilliseconds(100));
        operation.Confirm(
            requester,
            operation.Snapshot.PreviewHash,
            Now.AddMilliseconds(200),
            operation.Snapshot.ConfirmationChallenge);

        if (operation.Snapshot.State ==
            MutationOperationState.AwaitingApproval)
        {
            operation.Approve(
                new MutationApprovalAuthorizationEvidence(
                    operation.Snapshot.OperationId,
                    "oidc:https://idp.example|approver",
                    operation.Snapshot.PreviewHash,
                    new string('d', 64)),
                operation.Snapshot.PreviewHash,
                Now.AddMilliseconds(300));
        }

        _ = operation.ClaimExecution(
            Now.AddMilliseconds(400),
            Now.AddMinutes(5));
        operation.MarkDispatchStarted(
            Now.AddMilliseconds(500));
        operation.Complete(
            MutationExecutionResultKind.AppliedVerified,
            "data_job_activated",
            Now.AddMilliseconds(600));

        return operation.Snapshot;
    }

    private sealed class CheckpointReadPort :
        IKafkaRecordReadPort
    {
        private readonly long _endExclusive;

        public CheckpointReadPort(
            long endExclusive)
        {
            _endExclusive = endExclusive;
        }

        public int CallCount { get; private set; }
        public List<long> RequestedOffsets { get; } = [];

        public Task<KafkaResult<RecordReadBatch>>
            ReadPageAsync(
                RecordReadRequest request,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            CallCount++;
            var offset =
                request.Anchor.Offset ??
                throw new InvalidOperationException(
                    "Worker source read must use an exact offset anchor.");
            RequestedOffsets.Add(offset);

            var record = new KafkaRawRecord(
                offset,
                null,
                null,
                new byte[] { 1, 2, 3 },
                Array.Empty<KafkaRecordHeader>());
            var next = checked(offset + 1);
            var batch = new RecordReadBatch(
                new[] { record },
                LowWatermark: 0,
                HighWatermark: _endExclusive,
                FirstReturnedOffset: offset,
                LastReturnedOffset: offset,
                NextAnchor: next < _endExclusive
                    ? RecordAnchor.AtOffset(next)
                    : null,
                PreviousAnchor: null,
                BudgetOutcome: next == _endExclusive
                    ? RecordBudgetOutcome.Complete
                    : RecordBudgetOutcome.RecordLimit);

            var now = Now;
            return Task.FromResult(
                KafkaResult<RecordReadBatch>.Success(
                    batch,
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }
    }

    private sealed class SequencedProducer :
        IClusterTransferProducePort
    {
        private readonly MutationExecutionResultKind _kind;

        public SequencedProducer(
            MutationExecutionResultKind kind)
        {
            _kind = kind;
        }

        public int CallCount { get; private set; }

        public Task<MutationProviderResult> ProduceAsync(
            ClusterTransferProduceMutation request,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return Task.FromResult(
                new MutationProviderResult(
                    _kind,
                    _kind ==
                        MutationExecutionResultKind.AppliedVerified
                        ? "ack"
                        : "ambiguous"));
        }
    }

    private sealed class AllowGuard :
        IGovernedDataJobEffectGuard
    {
        public Task<MutationPreDispatchGuardResult>
            ValidateAsync(
                MutationOperationSnapshot operation,
                GovernedDataJobPlan plan,
                int rangeIndex,
                long sourceOffset,
                CancellationToken cancellationToken = default) =>
            Task.FromResult(
                MutationPreDispatchGuardResult.Allowed);
    }

    private sealed class FixedTimeProvider :
        TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(
            DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() =>
            _now;
    }

    private sealed class FakeFleetStore :
        IFleetMutationStateStore
    {
        private FleetOperationProgressSnapshot? _progress;

        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<FleetProgressCreateResult>
            CreateProgressAsync(
                FleetOperationProgressSnapshot progress,
                CancellationToken cancellationToken = default)
        {
            if (_progress is null)
            {
                _progress = progress;
                return Task.FromResult(
                    new FleetProgressCreateResult(
                        FleetProgressCreateOutcome.Created,
                        progress));
            }

            return Task.FromResult(
                new FleetProgressCreateResult(
                    FleetProgressCreateOutcome.Existing,
                    _progress));
        }

        public Task<FleetOperationProgressSnapshot?>
            GetProgressAsync(
                Guid operationId,
                CancellationToken cancellationToken = default) =>
            Task.FromResult(
                _progress?.OperationId == operationId
                    ? _progress
                    : null);

        public Task<FleetProgressSaveResult>
            TrySaveProgressAsync(
                FleetOperationProgressSnapshot progress,
                long expectedVersion,
                CancellationToken cancellationToken = default)
        {
            if (_progress is null)
            {
                return Task.FromResult(
                    new FleetProgressSaveResult(
                        FleetProgressSaveOutcome.NotFound,
                        null));
            }

            if (_progress.Version != expectedVersion)
            {
                return Task.FromResult(
                    new FleetProgressSaveResult(
                        FleetProgressSaveOutcome.VersionConflict,
                        _progress));
            }

            _progress = progress;
            return Task.FromResult(
                new FleetProgressSaveResult(
                    FleetProgressSaveOutcome.Saved,
                    progress));
        }

        public Task<FleetConflictObligationCreateResult>
            CreateConflictObligationAsync(
                FleetConflictObligationSnapshot obligation,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationBatchCreateResult>
            CreateConflictObligationsAsync(
                IReadOnlyList<FleetConflictObligationSnapshot> obligations,
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

    private sealed class UnusedOperationRepository :
        IMutationOperationRepository
    {
        public Task InitializeAsync(
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationCreateResult> CreateAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationOperationSnapshot?> GetAsync(
            Guid operationId,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<MutationOperationSnapshot>>
            ListByStateAsync(
                MutationOperationState state,
                int limit,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<MutationOperationSnapshot>>
            ListRecoverableExecutionsAsync(
                DateTimeOffset nowUtc,
                bool includeActiveLeases,
                int limit,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationSaveResult> TrySaveAsync(
            MutationOperationSnapshot operation,
            long expectedVersion,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationClusterSlotResult>
            TryAcquireClusterExecutionSlotAsync(
                Guid operationId,
                long executionClaimGeneration,
                string clusterId,
                int maxConcurrentPerCluster,
                DateTimeOffset expiresAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationClusterSlotRenewOutcome>
            TryRenewClusterExecutionSlotAsync(
                Guid operationId,
                long executionClaimGeneration,
                string clusterId,
                DateTimeOffset expiresAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationResourceClaimResult>
            TryAcquireResourceClaimsAsync(
                Guid operationId,
                long executionClaimGeneration,
                IReadOnlyList<string> resourceKeys,
                DateTimeOffset expiresAtUtc,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MutationLeaseRenewResult>
            TryRenewExecutionLeaseAsync(
                MutationOperationSnapshot operation,
                long expectedVersion,
                CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task ReleaseClusterExecutionSlotAsync(
            Guid operationId,
            long executionClaimGeneration,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task ReleaseResourceClaimsAsync(
            Guid operationId,
            long executionClaimGeneration,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
