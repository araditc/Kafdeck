using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W47TransferDispatchTests
{
    [Fact]
    public async Task Acknowledged_record_advances_durable_checkpoint_and_preserves_exact_destination_bytes()
    {
        var plan = Plan();
        var operation = ExecutingOperation(plan);
        var state = new InMemoryFleetStateStore();
        var producer = new CapturingProducer(
            new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "ack",
                new Dictionary<string, string>
                {
                    ["partition"] = "3",
                    ["offset"] = "44",
                }));
        var guard = new AllowGuard();
        var coordinator = new ClusterTransferDispatchCoordinator(
            state,
            producer,
            guard,
            new FixedTimeProvider());

        var record = new KafkaRawRecord(
            10,
            null,
            new ReadOnlyMemory<byte>(Array.Empty<byte>()),
            (ReadOnlyMemory<byte>?)null,
            new[]
            {
                new KafkaRecordHeader("x-dup", new byte[] { 1 }),
                new KafkaRecordHeader("x-dup", new byte[] { 2 }),
            });

        Assert.False(record.Value.HasValue);

        var result = await coordinator.DispatchRecordAsync(
            operation,
            plan,
            0,
            record);

        Assert.Equal(MutationExecutionResultKind.AppliedVerified, result.Result.ResultKind);
        Assert.Equal(1, producer.CallCount);
        var produced = Assert.IsType<ClusterTransferProduceMutation>(producer.Last);
        Assert.Equal("destination", produced.ClusterId);
        Assert.Equal("orders-copy", produced.TopicName);
        Assert.Equal(3, produced.Partition);
        Assert.True(produced.Key.HasValue);
        Assert.Empty(produced.Key.Value.ToArray());
        Assert.False(produced.Value.HasValue);
        Assert.Equal(new[] { "x-dup", "x-dup" }, produced.Headers.Select(item => item.Name));
        Assert.Equal(new byte[] { 1 }, produced.Headers[0].Value.ToArray());
        Assert.Equal(new byte[] { 2 }, produced.Headers[1].Value.ToArray());

        Assert.NotNull(result.Progress.Transfer);
        Assert.Null(result.Progress.Transfer!.PendingBatch);
        Assert.Equal(11, result.Progress.Transfer.Checkpoints[0].NextSourceOffset);
        Assert.Equal(1, result.Progress.Transfer.AcknowledgedRecords);
        Assert.Equal(12, result.Progress.Transfer.AcknowledgedBytes);
        Assert.Equal(1, guard.CallCount);
    }

    [Fact]
    public async Task Ambiguous_destination_outcome_is_durable_and_never_replayed()
    {
        var plan = Plan();
        var operation = ExecutingOperation(plan);
        var state = new InMemoryFleetStateStore();
        var producer = new CapturingProducer(
            new MutationProviderResult(
                MutationExecutionResultKind.ExecutionUnknown,
                "ambiguous"));
        var coordinator = new ClusterTransferDispatchCoordinator(
            state,
            producer,
            new AllowGuard(),
            new FixedTimeProvider());

        var record = Record(10);

        var first = await coordinator.DispatchRecordAsync(
            operation,
            plan,
            0,
            record);
        var second = await coordinator.DispatchRecordAsync(
            operation,
            plan,
            0,
            record);

        Assert.Equal(MutationExecutionResultKind.ExecutionUnknown, first.Result.ResultKind);
        Assert.Equal(MutationExecutionResultKind.ExecutionUnknown, second.Result.ResultKind);
        Assert.Equal("cluster_transfer_unresolved_dispatch_prevents_replay", second.Result.ResultCode);
        Assert.Equal(1, producer.CallCount);
        Assert.NotNull(second.Progress.Transfer!.PendingBatch);
        Assert.Equal(
            FleetTransferBatchState.DispatchStarted,
            second.Progress.Transfer.PendingBatch!.State);
        Assert.Equal(10, second.Progress.Transfer.Checkpoints[0].NextSourceOffset);
    }

    [Fact]
    public async Task Definitive_non_application_releases_pending_marker_without_advancing_checkpoint()
    {
        var plan = Plan();
        var operation = ExecutingOperation(plan);
        var state = new InMemoryFleetStateStore();
        var producer = new CapturingProducer(
            new MutationProviderResult(
                MutationExecutionResultKind.FailedDefinitive,
                "denied"));
        var coordinator = new ClusterTransferDispatchCoordinator(
            state,
            producer,
            new AllowGuard(),
            new FixedTimeProvider());

        var result = await coordinator.DispatchRecordAsync(
            operation,
            plan,
            0,
            Record(10));

        Assert.Equal(MutationExecutionResultKind.FailedDefinitive, result.Result.ResultKind);
        Assert.Null(result.Progress.Transfer!.PendingBatch);
        Assert.Equal(10, result.Progress.Transfer.Checkpoints[0].NextSourceOffset);
        Assert.Equal(0, result.Progress.Transfer.AcknowledgedRecords);
    }

    [Fact]
    public async Task Current_effect_guard_denial_prevents_destination_dispatch()
    {
        var plan = Plan();
        var operation = ExecutingOperation(plan);
        var state = new InMemoryFleetStateStore();
        var producer = new CapturingProducer(
            new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "unexpected"));
        var coordinator = new ClusterTransferDispatchCoordinator(
            state,
            producer,
            new DenyGuard(),
            new FixedTimeProvider());

        var result = await coordinator.DispatchRecordAsync(
            operation,
            plan,
            0,
            Record(10));

        Assert.Equal(MutationExecutionResultKind.FailedDefinitive, result.Result.ResultKind);
        Assert.Equal("current_transfer_authorization_denied", result.Result.ResultCode);
        Assert.Equal(0, producer.CallCount);
        Assert.Null(result.Progress.Transfer!.PendingBatch);
    }

    [Fact]
    public async Task Byte_budget_is_checked_before_any_destination_effect()
    {
        var plan = Plan(maxBatchBytes: 4);
        var operation = ExecutingOperation(plan);
        var state = new InMemoryFleetStateStore();
        var producer = new CapturingProducer(
            new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "unexpected"));
        var coordinator = new ClusterTransferDispatchCoordinator(
            state,
            producer,
            new AllowGuard(),
            new FixedTimeProvider());

        var result = await coordinator.DispatchRecordAsync(
            operation,
            plan,
            0,
            new KafkaRawRecord(
                10,
                null,
                null,
                new byte[] { 1, 2, 3, 4, 5 },
                Array.Empty<KafkaRecordHeader>()));

        Assert.Equal(MutationExecutionResultKind.FailedDefinitive, result.Result.ResultKind);
        Assert.Equal("cluster_transfer_record_exceeds_batch_byte_budget", result.Result.ResultCode);
        Assert.Equal(0, producer.CallCount);
    }

    private static ClusterTransferPlan Plan(long maxBatchBytes = 1024)
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
                10,
                20,
                new string('a', 64),
                new string('b', 64)),
        };
        var budget = new ClusterTransferBudget(
            maxBatchRecords: 10,
            maxBatchBytes: maxBatchBytes,
            maxTotalRecords: 10,
            maxTotalBytes: 1024 * 1024,
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

    private static MutationOperationSnapshot ExecutingOperation(
        ClusterTransferPlan plan)
    {
        var now = DateTimeOffset.Parse("2026-09-26T08:00:00Z");
        var requester = "oidc:https://idp.example|transfer-user";
        var operation = MutationOperation.CreatePreview(
            requester,
            ClusterTransferPolicy.BuildIntent(plan),
            ClusterTransferPolicy.ClassifyRisk(plan),
            "v0.6-w47",
            now.AddMinutes(10),
            now,
            "w47-dispatch-test");

        operation.OpenForConfirmation(now.AddSeconds(1));
        operation.Confirm(
            requester,
            operation.Snapshot.PreviewHash,
            now.AddSeconds(2),
            operation.Snapshot.ConfirmationChallenge);

        if (operation.Snapshot.State == MutationOperationState.AwaitingApproval)
        {
            operation.Approve(
                new MutationApprovalAuthorizationEvidence(
                    operation.Snapshot.OperationId,
                    "oidc:https://idp.example|transfer-approver",
                    operation.Snapshot.PreviewHash,
                    new string('d', 64)),
                operation.Snapshot.PreviewHash,
                now.AddMilliseconds(2500));
        }

        _ = operation.ClaimExecution(
            now.AddSeconds(3),
            now.AddMinutes(5));
        return operation.Snapshot;
    }

    private static KafkaRawRecord Record(long offset) =>
        new(
            offset,
            null,
            new byte[] { 1, 2 },
            new byte[] { 3, 4 },
            new[]
            {
                new KafkaRecordHeader("h", new byte[] { 5 }),
            });

    private sealed class AllowGuard : IClusterTransferEffectGuard
    {
        public int CallCount { get; private set; }

        public Task<MutationPreDispatchGuardResult> ValidateAsync(
            MutationOperationSnapshot operation,
            ClusterTransferPlan plan,
            int mappingIndex,
            long sourceOffset,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            return Task.FromResult(MutationPreDispatchGuardResult.Allowed);
        }
    }

    private sealed class DenyGuard : IClusterTransferEffectGuard
    {
        public Task<MutationPreDispatchGuardResult> ValidateAsync(
            MutationOperationSnapshot operation,
            ClusterTransferPlan plan,
            int mappingIndex,
            long sourceOffset,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new MutationPreDispatchGuardResult(
                MutationPreDispatchGuardOutcome.AuthorizationDenied,
                "current_transfer_authorization_denied"));
    }

    private sealed class CapturingProducer : IClusterTransferProducePort
    {
        private readonly MutationProviderResult _result;

        public CapturingProducer(MutationProviderResult result)
        {
            _result = result;
        }

        public int CallCount { get; private set; }

        public ClusterTransferProduceMutation? Last { get; private set; }

        public Task<MutationProviderResult> ProduceAsync(
            ClusterTransferProduceMutation request,
            CancellationToken cancellationToken = default)
        {
            CallCount++;

            ReadOnlyMemory<byte>? copiedKey = request.Key.HasValue
                ? new ReadOnlyMemory<byte>(request.Key.Value.ToArray())
                : (ReadOnlyMemory<byte>?)null;
            ReadOnlyMemory<byte>? copiedValue = request.Value.HasValue
                ? new ReadOnlyMemory<byte>(request.Value.Value.ToArray())
                : (ReadOnlyMemory<byte>?)null;

            Last = new ClusterTransferProduceMutation(
                request.ClusterId,
                request.TopicName,
                request.Partition,
                copiedKey,
                copiedValue,
                request.Headers
                    .Select(header => new KafkaRecordHeader(
                        header.Name,
                        header.Value.ToArray()))
                    .ToArray());
            return Task.FromResult(_result);
        }
    }

    private sealed class InMemoryFleetStateStore : IFleetMutationStateStore
    {
        private FleetOperationProgressSnapshot? _progress;

        public Task InitializeAsync(CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<FleetProgressCreateResult> CreateProgressAsync(
            FleetOperationProgressSnapshot progress,
            CancellationToken cancellationToken = default)
        {
            if (_progress is null)
            {
                _progress = FleetOperationProgress.Restore(progress).Snapshot;
                return Task.FromResult(new FleetProgressCreateResult(
                    FleetProgressCreateOutcome.Created,
                    _progress));
            }

            return Task.FromResult(new FleetProgressCreateResult(
                FleetProgressCreateOutcome.Existing,
                _progress));
        }

        public Task<FleetOperationProgressSnapshot?> GetProgressAsync(
            Guid operationId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                _progress?.OperationId == operationId
                    ? _progress
                    : null);

        public Task<FleetProgressSaveResult> TrySaveProgressAsync(
            FleetOperationProgressSnapshot progress,
            long expectedVersion,
            CancellationToken cancellationToken = default)
        {
            if (_progress is null)
            {
                return Task.FromResult(new FleetProgressSaveResult(
                    FleetProgressSaveOutcome.NotFound,
                    null));
            }

            if (_progress.Version != expectedVersion)
            {
                return Task.FromResult(new FleetProgressSaveResult(
                    FleetProgressSaveOutcome.VersionConflict,
                    _progress));
            }

            _progress = FleetOperationProgress.Restore(progress).Snapshot;
            return Task.FromResult(new FleetProgressSaveResult(
                FleetProgressSaveOutcome.Saved,
                _progress));
        }

        public Task<FleetConflictObligationCreateResult> CreateConflictObligationAsync(
            FleetConflictObligationSnapshot obligation,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationBatchCreateResult> CreateConflictObligationsAsync(
            IReadOnlyList<FleetConflictObligationSnapshot> obligations,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSnapshot?> GetConflictObligationAsync(
            Guid obligationId,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSnapshot?> FindBlockingConflictObligationAsync(
            string conflictKey,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<FleetConflictObligationSaveResult> TrySaveConflictObligationAsync(
            FleetConflictObligationSnapshot obligation,
            long expectedVersion,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now =
            DateTimeOffset.Parse("2026-09-26T08:00:00Z");

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
