using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05MutationPersistenceTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 21, 16, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Sqlite_repository_enforces_idempotency_cas_and_resource_claims()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-mutation-{Guid.NewGuid():N}.db");
        try
        {
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                new FixedTimeProvider(Now));
            await repository.InitializeAsync();

            var first = CreateOperation("idem-1", "{\"value\":1}");
            var created = await repository.CreateAsync(first.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var replay = CreateOperation("idem-1", "{\"value\":1}");
            var same = await repository.CreateAsync(replay.Snapshot);
            Assert.Equal(MutationCreateOutcome.ExistingSameIntent, same.Outcome);
            Assert.Equal(first.Snapshot.OperationId, same.Operation.OperationId);

            var conflict = CreateOperation("idem-1", "{\"value\":2}");
            var different = await repository.CreateAsync(conflict.Snapshot);
            Assert.Equal(MutationCreateOutcome.IdempotencyConflict, different.Outcome);

            var original = created.Operation;
            var confirmed = MutationOperation.Restore(original);
            confirmed.Confirm(
                confirmed.Snapshot.RequesterPrincipalId,
                confirmed.Snapshot.PreviewHash,
                Now.AddSeconds(1));

            var saved = await repository.TrySaveAsync(
                confirmed.Snapshot,
                original.Version);
            Assert.Equal(MutationSaveOutcome.Saved, saved.Outcome);

            var stale = MutationOperation.Restore(original);
            stale.Cancel(Now.AddSeconds(2));
            var staleSave = await repository.TrySaveAsync(
                stale.Snapshot,
                original.Version);
            Assert.Equal(MutationSaveOutcome.VersionConflict, staleSave.Outcome);
            Assert.Equal(
                confirmed.Snapshot.State,
                staleSave.Operation!.State);

            var executing = MutationOperation.Restore(saved.Operation!);
            var executingGeneration = executing.ClaimExecution(Now.AddSeconds(3), Now.AddMinutes(2));
            var executingSaved = await repository.TrySaveAsync(
                executing.Snapshot,
                saved.Operation!.Version);
            Assert.Equal(MutationSaveOutcome.Saved, executingSaved.Outcome);

            var claim = await repository.TryAcquireResourceClaimsAsync(
                executing.Snapshot.OperationId,
                executingGeneration,
                executing.Snapshot.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(MutationResourceClaimOutcome.Acquired, claim.Outcome);

            var other = CreateReadyOperation("idem-2");
            var otherCreated = await repository.CreateAsync(other.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, otherCreated.Outcome);

            var otherExecuting = MutationOperation.Restore(otherCreated.Operation);
            var otherGeneration = otherExecuting.ClaimExecution(Now.AddSeconds(3), Now.AddMinutes(2));
            var otherSaved = await repository.TrySaveAsync(
                otherExecuting.Snapshot,
                otherCreated.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, otherSaved.Outcome);

            var blocked = await repository.TryAcquireResourceClaimsAsync(
                otherExecuting.Snapshot.OperationId,
                otherGeneration,
                otherExecuting.Snapshot.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(MutationResourceClaimOutcome.Conflict, blocked.Outcome);

            await repository.ReleaseResourceClaimsAsync(
                executing.Snapshot.OperationId,
                executingGeneration);

            var acquiredAfterRelease = await repository.TryAcquireResourceClaimsAsync(
                otherExecuting.Snapshot.OperationId,
                otherGeneration,
                otherExecuting.Snapshot.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(MutationResourceClaimOutcome.Acquired, acquiredAfterRelease.Outcome);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Sqlite_cluster_execution_slots_enforce_configured_cap_across_distinct_resources()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-cluster-slots-{Guid.NewGuid():N}.db");
        try
        {
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                new FixedTimeProvider(Now));
            await repository.InitializeAsync();

            var first = CreateReadyOperation("slot-first", "cluster/prod/topic/first");
            var second = CreateReadyOperation("slot-second", "cluster/prod/topic/second");
            var firstCreated = await repository.CreateAsync(first.Snapshot);
            var secondCreated = await repository.CreateAsync(second.Snapshot);

            var firstAggregate = MutationOperation.Restore(firstCreated.Operation);
            var secondAggregate = MutationOperation.Restore(secondCreated.Operation);
            var firstGeneration = firstAggregate.ClaimExecution(Now, Now.AddMinutes(2));
            var secondGeneration = secondAggregate.ClaimExecution(Now, Now.AddMinutes(2));

            Assert.Equal(
                MutationSaveOutcome.Saved,
                (await repository.TrySaveAsync(
                    firstAggregate.Snapshot,
                    firstCreated.Operation.Version)).Outcome);
            Assert.Equal(
                MutationSaveOutcome.Saved,
                (await repository.TrySaveAsync(
                    secondAggregate.Snapshot,
                    secondCreated.Operation.Version)).Outcome);

            var firstSlot = await repository.TryAcquireClusterExecutionSlotAsync(
                firstAggregate.Snapshot.OperationId,
                firstGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddMinutes(2));
            var secondSlot = await repository.TryAcquireClusterExecutionSlotAsync(
                secondAggregate.Snapshot.OperationId,
                secondGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddMinutes(2));

            Assert.Equal(MutationClusterSlotOutcome.Acquired, firstSlot.Outcome);
            Assert.Equal(MutationClusterSlotOutcome.Saturated, secondSlot.Outcome);

            await repository.ReleaseClusterExecutionSlotAsync(
                firstAggregate.Snapshot.OperationId,
                firstGeneration);

            var afterRelease = await repository.TryAcquireClusterExecutionSlotAsync(
                secondAggregate.Snapshot.OperationId,
                secondGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddMinutes(2));
            Assert.Equal(MutationClusterSlotOutcome.Acquired, afterRelease.Outcome);

            await repository.ReleaseClusterExecutionSlotAsync(
                secondAggregate.Snapshot.OperationId,
                secondGeneration);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Expired_execution_unknown_cluster_slot_is_reclaimed_after_process_heartbeat_stops()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-unknown-slot-reclaim-{Guid.NewGuid():N}.db");
        try
        {
            var time = new MutableTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var holder = CreateReadyOperation(
                "unknown-slot-holder",
                "cluster/prod/topic/holder");
            var competitor = CreateReadyOperation(
                "unknown-slot-competitor",
                "cluster/prod/topic/competitor");

            var holderCreated = await repository.CreateAsync(holder.Snapshot);
            var competitorCreated = await repository.CreateAsync(competitor.Snapshot);

            var holderAggregate = MutationOperation.Restore(holderCreated.Operation);
            var holderGeneration = holderAggregate.ClaimExecution(
                Now,
                Now.AddSeconds(20));
            var holderClaimed = await repository.TrySaveAsync(
                holderAggregate.Snapshot,
                holderCreated.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, holderClaimed.Outcome);

            var holderSlot = await repository.TryAcquireClusterExecutionSlotAsync(
                holderAggregate.Snapshot.OperationId,
                holderGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddSeconds(20));
            Assert.Equal(MutationClusterSlotOutcome.Acquired, holderSlot.Outcome);

            holderAggregate.MarkDispatchStarted(Now.AddSeconds(1));
            var holderDispatched = await repository.TrySaveAsync(
                holderAggregate.Snapshot,
                holderClaimed.Operation!.Version);
            Assert.Equal(MutationSaveOutcome.Saved, holderDispatched.Outcome);

            holderAggregate.Complete(
                MutationExecutionResultKind.ExecutionUnknown,
                "execution_timeout",
                Now.AddSeconds(2));
            var holderUnknown = await repository.TrySaveAsync(
                holderAggregate.Snapshot,
                holderDispatched.Operation!.Version);
            Assert.Equal(MutationSaveOutcome.Saved, holderUnknown.Outcome);
            Assert.Equal(MutationOperationState.ExecutionUnknown, holderUnknown.Operation!.State);

            var competitorAggregate = MutationOperation.Restore(competitorCreated.Operation);
            var competitorGeneration = competitorAggregate.ClaimExecution(
                Now,
                Now.AddSeconds(60));
            var competitorClaimed = await repository.TrySaveAsync(
                competitorAggregate.Snapshot,
                competitorCreated.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, competitorClaimed.Outcome);

            time.SetUtcNow(Now.AddSeconds(21));

            var reclaimed = await repository.TryAcquireClusterExecutionSlotAsync(
                competitorAggregate.Snapshot.OperationId,
                competitorGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddSeconds(60));

            Assert.Equal(MutationClusterSlotOutcome.Acquired, reclaimed.Outcome);
            Assert.Equal(0, reclaimed.SlotNumber);

            await repository.ReleaseClusterExecutionSlotAsync(
                competitorAggregate.Snapshot.OperationId,
                competitorGeneration);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Execution_unknown_cluster_slot_heartbeat_prevents_reclaim_until_renewed_lease_expires()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-unknown-slot-heartbeat-{Guid.NewGuid():N}.db");
        try
        {
            var time = new MutableTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var holder = CreateReadyOperation(
                "unknown-heartbeat-holder",
                "cluster/prod/topic/holder");
            var competitor = CreateReadyOperation(
                "unknown-heartbeat-competitor",
                "cluster/prod/topic/competitor");

            var holderCreated = await repository.CreateAsync(holder.Snapshot);
            var competitorCreated = await repository.CreateAsync(competitor.Snapshot);

            var holderAggregate = MutationOperation.Restore(holderCreated.Operation);
            var holderGeneration = holderAggregate.ClaimExecution(
                Now,
                Now.AddSeconds(20));
            var holderClaimed = await repository.TrySaveAsync(
                holderAggregate.Snapshot,
                holderCreated.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, holderClaimed.Outcome);

            Assert.Equal(
                MutationClusterSlotOutcome.Acquired,
                (await repository.TryAcquireClusterExecutionSlotAsync(
                    holderAggregate.Snapshot.OperationId,
                    holderGeneration,
                    "prod",
                    maxConcurrentPerCluster: 1,
                    Now.AddSeconds(20))).Outcome);

            holderAggregate.MarkDispatchStarted(Now.AddSeconds(1));
            var holderDispatched = await repository.TrySaveAsync(
                holderAggregate.Snapshot,
                holderClaimed.Operation!.Version);
            Assert.Equal(MutationSaveOutcome.Saved, holderDispatched.Outcome);

            holderAggregate.Complete(
                MutationExecutionResultKind.ExecutionUnknown,
                "execution_timeout",
                Now.AddSeconds(2));
            var holderUnknown = await repository.TrySaveAsync(
                holderAggregate.Snapshot,
                holderDispatched.Operation!.Version);
            Assert.Equal(MutationSaveOutcome.Saved, holderUnknown.Outcome);

            var competitorAggregate = MutationOperation.Restore(competitorCreated.Operation);
            var competitorGeneration = competitorAggregate.ClaimExecution(
                Now,
                Now.AddSeconds(60));
            var competitorClaimed = await repository.TrySaveAsync(
                competitorAggregate.Snapshot,
                competitorCreated.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, competitorClaimed.Outcome);

            time.SetUtcNow(Now.AddSeconds(10));
            var renewed = await repository.TryRenewClusterExecutionSlotAsync(
                holderAggregate.Snapshot.OperationId,
                holderGeneration,
                "prod",
                Now.AddSeconds(30));
            Assert.Equal(MutationClusterSlotRenewOutcome.Renewed, renewed);

            time.SetUtcNow(Now.AddSeconds(21));
            var stillHeld = await repository.TryAcquireClusterExecutionSlotAsync(
                competitorAggregate.Snapshot.OperationId,
                competitorGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddSeconds(60));
            Assert.Equal(MutationClusterSlotOutcome.Saturated, stillHeld.Outcome);

            time.SetUtcNow(Now.AddSeconds(31));
            var reclaimed = await repository.TryAcquireClusterExecutionSlotAsync(
                competitorAggregate.Snapshot.OperationId,
                competitorGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddSeconds(60));
            Assert.Equal(MutationClusterSlotOutcome.Acquired, reclaimed.Outcome);

            await repository.ReleaseClusterExecutionSlotAsync(
                competitorAggregate.Snapshot.OperationId,
                competitorGeneration);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Bounded_executor_dispatches_once_and_persists_verified_outcome()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-executor-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("executor-1");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var handler = new RecordingHandler(MutationOperationKind.TopicCreate);
            var audit = new CapturingMutationAuditSink();
            var executor = new MutationExecutor(
                repository,
                audit,
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(new[] { handler }),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var result = await executor.ExecuteAsync(operation.Snapshot.OperationId);

            Assert.Equal(MutationOperationState.AppliedVerified, result.State);
            Assert.Equal(1, handler.CallCount);
            Assert.Equal("3", result.SafeProviderEvidence["partition"]);
            Assert.Equal("42", result.SafeProviderEvidence["offset"]);
            Assert.Contains(audit.Events, item => item.EventType == MutationAuditEventType.DispatchStarted);
            Assert.Contains(audit.Events, item => item.EventType == MutationAuditEventType.Completed);

            var reloaded = await repository.GetAsync(operation.Snapshot.OperationId);
            Assert.NotNull(reloaded);
            Assert.Equal("3", reloaded!.SafeProviderEvidence["partition"]);
            Assert.Equal("42", reloaded.SafeProviderEvidence["offset"]);

            var replay = await executor.ExecuteAsync(operation.Snapshot.OperationId);
            Assert.Equal(MutationOperationState.AppliedVerified, replay.State);
            Assert.Equal(1, handler.CallCount);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }



    [Fact]
    public async Task Resource_claim_requires_matching_persisted_execution_generation()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-claim-fence-{Guid.NewGuid():N}.db");
        try
        {
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                new FixedTimeProvider(Now));
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("claim-fence");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var invalidBeforeClaim = await repository.TryAcquireResourceClaimsAsync(
                created.Operation.OperationId,
                1,
                created.Operation.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(
                MutationResourceClaimOutcome.InvalidExecutionClaim,
                invalidBeforeClaim.Outcome);

            var aggregate = MutationOperation.Restore(created.Operation);
            var generation = aggregate.ClaimExecution(Now.AddSeconds(1), Now.AddMinutes(2));
            var saved = await repository.TrySaveAsync(
                aggregate.Snapshot,
                created.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, saved.Outcome);

            var wrongGeneration = await repository.TryAcquireResourceClaimsAsync(
                aggregate.Snapshot.OperationId,
                generation + 1,
                aggregate.Snapshot.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(
                MutationResourceClaimOutcome.InvalidExecutionClaim,
                wrongGeneration.Outcome);

            var valid = await repository.TryAcquireResourceClaimsAsync(
                aggregate.Snapshot.OperationId,
                generation,
                aggregate.Snapshot.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(MutationResourceClaimOutcome.Acquired, valid.Outcome);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Caller_cancellation_after_dispatch_does_not_cancel_server_owned_provider_execution()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-dispatch-owner-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("dispatch-owner");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var handler = new BlockingRecordingHandler(MutationOperationKind.TopicCreate);
            var executor = new MutationExecutor(
                repository,
                new CapturingMutationAuditSink(),
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(new[] { handler }),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(5),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            using var caller = new CancellationTokenSource();
            var execution = executor.ExecuteAsync(operation.Snapshot.OperationId, caller.Token);

            await handler.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            caller.Cancel();
            handler.Complete.TrySetResult(true);

            var result = await execution.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(MutationOperationState.AppliedVerified, result.State);
            Assert.False(handler.ProviderCancellationObserved);
            Assert.Equal(1, handler.CallCount);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Executor_renews_execution_and_resource_lease_immediately_before_dispatch()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-pre-dispatch-renew-{Guid.NewGuid():N}.db");
        try
        {
            var time = new MutableTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("pre-dispatch-renew");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var handler = new RecordingHandler(MutationOperationKind.TopicCreate);
            var guard = new AdvancingAllowedGuard(
                time,
                Now.AddSeconds(19));
            var executor = new MutationExecutor(
                repository,
                new CapturingMutationAuditSink(),
                guard,
                new MutationExecutionHandlerRegistry(new[] { handler }),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var result = await executor.ExecuteAsync(operation.Snapshot.OperationId);

            Assert.Equal(MutationOperationState.AppliedVerified, result.State);
            Assert.Equal(Now.AddSeconds(39), result.ExecutionClaimExpiresAtUtc);
            Assert.NotNull(result.DispatchStartedAtUtc);
            Assert.True(result.DispatchStartedAtUtc >= Now.AddSeconds(19));
            Assert.Equal(1, handler.CallCount);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Provider_ignoring_cancellation_times_out_to_unknown_and_keeps_conflict_claim()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-provider-timeout-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("timeout-holder");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var handler = new IgnoringCancellationHandler(MutationOperationKind.TopicCreate);
            var executor = new MutationExecutor(
                repository,
                new CapturingMutationAuditSink(),
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(new[] { handler }),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var result = await executor
                .ExecuteAsync(operation.Snapshot.OperationId)
                .WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(MutationOperationState.ExecutionUnknown, result.State);
            Assert.Equal("execution_timeout", result.ResultCode);

            var competitor = CreateReadyOperation("timeout-competitor");
            var competitorCreated = await repository.CreateAsync(competitor.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, competitorCreated.Outcome);

            var competitorAggregate = MutationOperation.Restore(competitorCreated.Operation);
            var competitorGeneration = competitorAggregate.ClaimExecution(
                Now.AddSeconds(2),
                Now.AddSeconds(20));
            var competitorSaved = await repository.TrySaveAsync(
                competitorAggregate.Snapshot,
                competitorCreated.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, competitorSaved.Outcome);

            var blocked = await repository.TryAcquireResourceClaimsAsync(
                competitorAggregate.Snapshot.OperationId,
                competitorGeneration,
                competitorAggregate.Snapshot.ResourceKeys,
                Now.AddSeconds(20));
            Assert.Equal(MutationResourceClaimOutcome.Conflict, blocked.Outcome);

            handler.Complete.TrySetResult(true);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Synchronously_blocking_handler_invocation_is_bounded_by_operation_timeout()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-sync-handler-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("sync-handler-timeout");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            using var handler = new SynchronouslyBlockingHandler(
                MutationOperationKind.TopicCreate);
            var executor = new MutationExecutor(
                repository,
                new CapturingMutationAuditSink(),
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(new[] { handler }),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var execution = executor.ExecuteAsync(operation.Snapshot.OperationId);

            await handler.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var result = await execution.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(MutationOperationState.ExecutionUnknown, result.State);
            Assert.Equal("execution_timeout", result.ResultCode);
            Assert.Equal(1, handler.CallCount);

            handler.Release();
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Post_dispatch_audit_failure_does_not_abort_provider_or_retain_known_outcome_claim()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-audit-after-dispatch-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("post-dispatch-audit");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var handler = new RecordingHandler(MutationOperationKind.TopicCreate);
            var audit = new ThrowingPostDispatchAuditSink();
            var executor = new MutationExecutor(
                repository,
                audit,
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(new[] { handler }),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var result = await executor.ExecuteAsync(operation.Snapshot.OperationId);

            Assert.Equal(MutationOperationState.AppliedVerified, result.State);
            Assert.Equal(1, handler.CallCount);
            Assert.True(audit.DispatchAuditAttempted);
            Assert.True(audit.CompletedAuditAttempted);

            var competitor = CreateReadyOperation("post-dispatch-audit-competitor");
            var competitorCreated = await repository.CreateAsync(competitor.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, competitorCreated.Outcome);

            var competitorAggregate = MutationOperation.Restore(competitorCreated.Operation);
            var generation = competitorAggregate.ClaimExecution(
                Now.AddSeconds(2),
                Now.AddSeconds(20));
            Assert.Equal(
                MutationSaveOutcome.Saved,
                (await repository.TrySaveAsync(
                    competitorAggregate.Snapshot,
                    competitorCreated.Operation.Version)).Outcome);

            var claim = await repository.TryAcquireResourceClaimsAsync(
                competitorAggregate.Snapshot.OperationId,
                generation,
                competitorAggregate.Snapshot.ResourceKeys,
                Now.AddSeconds(20));

            Assert.Equal(MutationResourceClaimOutcome.Acquired, claim.Outcome);
            await repository.ReleaseResourceClaimsAsync(
                competitorAggregate.Snapshot.OperationId,
                generation);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Malformed_provider_result_is_durably_classified_as_execution_unknown()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-malformed-provider-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("malformed-provider-result");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var executor = new MutationExecutor(
                repository,
                new CapturingMutationAuditSink(),
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(
                    new[] { new MalformedResultHandler(MutationOperationKind.TopicCreate) }),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var result = await executor.ExecuteAsync(operation.Snapshot.OperationId);

            Assert.Equal(MutationOperationState.ExecutionUnknown, result.State);
            Assert.Equal("malformed_provider_result", result.ResultCode);
            Assert.Empty(result.SafeProviderEvidence);

            var reloaded = await repository.GetAsync(operation.Snapshot.OperationId);
            Assert.NotNull(reloaded);
            Assert.Equal(MutationOperationState.ExecutionUnknown, reloaded!.State);
            Assert.Equal("malformed_provider_result", reloaded.ResultCode);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Pre_dispatch_terminal_audit_failure_still_releases_resource_claim()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-predispatch-audit-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("predispatch-audit");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var executor = new MutationExecutor(
                repository,
                new ThrowingPreDispatchTerminalAuditSink(),
                new DenyingGuard(),
                new MutationExecutionHandlerRegistry(Array.Empty<IMutationExecutionHandler>()),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => executor.ExecuteAsync(operation.Snapshot.OperationId));

            var persisted = await repository.GetAsync(operation.Snapshot.OperationId);
            Assert.NotNull(persisted);
            Assert.Equal(MutationOperationState.FailedBeforeDispatch, persisted!.State);
            Assert.Equal("authorization_denied", persisted.ResultCode);

            var competitor = CreateReadyOperation("predispatch-audit-competitor");
            var competitorCreated = await repository.CreateAsync(competitor.Snapshot);
            var aggregate = MutationOperation.Restore(competitorCreated.Operation);
            var generation = aggregate.ClaimExecution(
                Now.AddSeconds(2),
                Now.AddSeconds(20));
            Assert.Equal(
                MutationSaveOutcome.Saved,
                (await repository.TrySaveAsync(
                    aggregate.Snapshot,
                    competitorCreated.Operation.Version)).Outcome);

            var claim = await repository.TryAcquireResourceClaimsAsync(
                aggregate.Snapshot.OperationId,
                generation,
                aggregate.Snapshot.ResourceKeys,
                Now.AddSeconds(20));

            Assert.Equal(MutationResourceClaimOutcome.Acquired, claim.Outcome);
            await repository.ReleaseResourceClaimsAsync(
                aggregate.Snapshot.OperationId,
                generation);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Timed_out_provider_retains_cluster_concurrency_slot_until_late_task_finishes()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-late-permit-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var first = CreateReadyOperation(
                "late-permit-first",
                "cluster/prod/topic/first");
            var second = CreateReadyOperation(
                "late-permit-second",
                "cluster/prod/topic/second");
            Assert.Equal(MutationCreateOutcome.Created, (await repository.CreateAsync(first.Snapshot)).Outcome);
            Assert.Equal(MutationCreateOutcome.Created, (await repository.CreateAsync(second.Snapshot)).Outcome);

            var handler = new FirstCallIgnoresCancellationHandler(MutationOperationKind.TopicCreate);
            var executor = new MutationExecutor(
                repository,
                new CapturingMutationAuditSink(),
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(new[] { handler }),
                new TestMaterialDigestService(),
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var firstResult = await executor
                .ExecuteAsync(first.Snapshot.OperationId)
                .WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(MutationOperationState.ExecutionUnknown, firstResult.State);
            Assert.Equal(1, handler.CallCount);

            var secondExecution = executor.ExecuteAsync(second.Snapshot.OperationId);
            await Task.Delay(150);
            Assert.False(secondExecution.IsCompleted);
            Assert.Equal(1, handler.CallCount);

            handler.ReleaseFirst.TrySetResult(true);

            var secondResult = await secondExecution.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(MutationOperationState.AppliedVerified, secondResult.State);
            Assert.Equal(2, handler.CallCount);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Ephemeral_execution_material_is_digest_validated_and_never_persisted()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-material-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var digestService = new TestMaterialDigestService();
            var secret = Encoding.UTF8.GetBytes("record-secret-payload");
            var digest = digestService.ComputeDigest(secret);
            var risk = MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.RecordProduce));

            var operation = MutationOperation.CreatePreview(
                "oidc:https://idp.example|alice",
                new MutationIntentDescriptor(
                    MutationOperationKind.RecordProduce,
                    "prod",
                    "{\"topic\":\"payments\"}",
                    new[] { "cluster/prod/topic/payments" },
                    MaterialDigests: new[]
                    {
                        new MutationMaterialDigest("value", digest),
                    }),
                risk,
                "v0.5-p1",
                Now.AddMinutes(5),
                Now,
                "material-ok");
            operation.OpenForConfirmation(Now);
            operation.Confirm(
                operation.Snapshot.RequesterPrincipalId,
                operation.Snapshot.PreviewHash,
                Now);

            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var persistedJson = JsonSerializer.Serialize(created.Operation);
            Assert.DoesNotContain(
                "record-secret-payload",
                persistedJson,
                StringComparison.Ordinal);

            var handler = new MaterialRecordingHandler(
                MutationOperationKind.RecordProduce,
                "value",
                secret);
            var executor = new MutationExecutor(
                repository,
                new CapturingMutationAuditSink(),
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(new[] { handler }),
                digestService,
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var result = await executor.ExecuteAsync(
                operation.Snapshot.OperationId,
                new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal)
                {
                    ["value"] = secret,
                });

            Assert.Equal(MutationOperationState.AppliedVerified, result.State);
            Assert.Equal(1, handler.CallCount);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Ephemeral_execution_material_mismatch_fails_before_dispatch()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-material-mismatch-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var digestService = new TestMaterialDigestService();
            var expected = Encoding.UTF8.GetBytes("expected-secret");
            var wrong = Encoding.UTF8.GetBytes("wrong-secret");
            var risk = MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.RecordProduce));

            var operation = MutationOperation.CreatePreview(
                "oidc:https://idp.example|alice",
                new MutationIntentDescriptor(
                    MutationOperationKind.RecordProduce,
                    "prod",
                    "{\"topic\":\"payments\"}",
                    new[] { "cluster/prod/topic/payments" },
                    MaterialDigests: new[]
                    {
                        new MutationMaterialDigest(
                            "value",
                            digestService.ComputeDigest(expected)),
                    }),
                risk,
                "v0.5-p1",
                Now.AddMinutes(5),
                Now,
                "material-mismatch");
            operation.OpenForConfirmation(Now);
            operation.Confirm(
                operation.Snapshot.RequesterPrincipalId,
                operation.Snapshot.PreviewHash,
                Now);

            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var handler = new MaterialRecordingHandler(
                MutationOperationKind.RecordProduce,
                "value",
                expected);
            var executor = new MutationExecutor(
                repository,
                new CapturingMutationAuditSink(),
                new AllowedGuard(),
                new MutationExecutionHandlerRegistry(new[] { handler }),
                digestService,
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var result = await executor.ExecuteAsync(
                operation.Snapshot.OperationId,
                new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal)
                {
                    ["value"] = wrong,
                });

            Assert.Equal(MutationOperationState.FailedBeforeDispatch, result.State);
            Assert.Equal("execution_material_mismatch", result.ResultCode);
            Assert.Equal(0, handler.CallCount);
            Assert.Null(result.DispatchStartedAtUtc);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Recoverable_query_filters_active_leases_before_applying_limit()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-recovery-filter-{Guid.NewGuid():N}.db");
        try
        {
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                new FixedTimeProvider(Now));
            await repository.InitializeAsync();

            var expired = CreateReadyOperation("recover-expired");
            var expiredCreated = await repository.CreateAsync(expired.Snapshot);
            var expiredAggregate = MutationOperation.Restore(expiredCreated.Operation);
            expiredAggregate.ClaimExecution(Now, Now.AddMinutes(1));
            Assert.Equal(
                MutationSaveOutcome.Saved,
                (await repository.TrySaveAsync(
                    expiredAggregate.Snapshot,
                    expiredCreated.Operation.Version)).Outcome);

            var active = CreateReadyOperation("recover-active");
            var activeCreated = await repository.CreateAsync(active.Snapshot);
            var activeAggregate = MutationOperation.Restore(activeCreated.Operation);
            activeAggregate.ClaimExecution(Now, Now.AddMinutes(5));
            Assert.Equal(
                MutationSaveOutcome.Saved,
                (await repository.TrySaveAsync(
                    activeAggregate.Snapshot,
                    activeCreated.Operation.Version)).Outcome);

            var recoverable = await repository.ListRecoverableExecutionsAsync(
                Now.AddMinutes(2),
                includeActiveLeases: false,
                limit: 1);

            Assert.Single(recoverable);
            Assert.Equal(
                expiredAggregate.Snapshot.OperationId,
                recoverable[0].OperationId);

            var includingActive = await repository.ListRecoverableExecutionsAsync(
                Now.AddMinutes(2),
                includeActiveLeases: true,
                limit: 3);
            Assert.Equal(2, includingActive.Count);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Recovery_marks_interrupted_pre_dispatch_execution_as_failed_before_dispatch()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-recovery-pre-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("recovery-pre");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var aggregate = MutationOperation.Restore(created.Operation);
            aggregate.ClaimExecution(Now.AddSeconds(1), Now.AddMinutes(2));
            var claimed = await repository.TrySaveAsync(
                aggregate.Snapshot,
                created.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, claimed.Outcome);

            var audit = new CapturingMutationAuditSink();
            var recovery = new MutationRecoveryCoordinator(repository, audit, time);

            var recovered = await recovery.RecoverInterruptedExecutionsAsync(
                ignoreActiveExecutionLeases: true);

            Assert.Equal(1, recovered);
            var persisted = await repository.GetAsync(created.Operation.OperationId);
            Assert.NotNull(persisted);
            Assert.Equal(MutationOperationState.FailedBeforeDispatch, persisted!.State);
            Assert.Equal("process_interrupted_before_dispatch", persisted.ResultCode);
            Assert.Contains(audit.Events, item => item.OutcomeCode == "process_interrupted_before_dispatch");
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Recovery_marks_interrupted_post_dispatch_execution_as_unknown()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-recovery-post-{Guid.NewGuid():N}.db");
        try
        {
            var time = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("recovery-post");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var aggregate = MutationOperation.Restore(created.Operation);
            aggregate.ClaimExecution(Now.AddSeconds(1), Now.AddMinutes(2));
            var claimed = await repository.TrySaveAsync(
                aggregate.Snapshot,
                created.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, claimed.Outcome);

            aggregate.MarkDispatchStarted(Now.AddSeconds(2));
            var dispatched = await repository.TrySaveAsync(
                aggregate.Snapshot,
                claimed.Operation!.Version);
            Assert.Equal(MutationSaveOutcome.Saved, dispatched.Outcome);

            var audit = new CapturingMutationAuditSink();
            var recovery = new MutationRecoveryCoordinator(repository, audit, time);

            var recovered = await recovery.RecoverInterruptedExecutionsAsync(
                ignoreActiveExecutionLeases: true);

            Assert.Equal(1, recovered);
            var persisted = await repository.GetAsync(created.Operation.OperationId);
            Assert.NotNull(persisted);
            Assert.Equal(MutationOperationState.ExecutionUnknown, persisted!.State);
            Assert.Equal("process_interrupted_after_dispatch", persisted.ResultCode);
            Assert.Contains(audit.Events, item => item.OutcomeCode == "process_interrupted_after_dispatch");
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Ha_recovery_respects_active_execution_lease_then_recovers_after_expiry()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-ha-recovery-{Guid.NewGuid():N}.db");
        try
        {
            var repositoryTime = new FixedTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                repositoryTime);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("ha-recovery");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var aggregate = MutationOperation.Restore(created.Operation);
            aggregate.ClaimExecution(Now.AddSeconds(1), Now.AddMinutes(2));
            var saved = await repository.TrySaveAsync(
                aggregate.Snapshot,
                created.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, saved.Outcome);

            var audit = new CapturingMutationAuditSink();
            var activeLeaseRecovery = new MutationRecoveryCoordinator(
                repository,
                audit,
                new FixedTimeProvider(Now.AddMinutes(1)));

            var skipped = await activeLeaseRecovery.RecoverInterruptedExecutionsAsync();
            Assert.Equal(0, skipped);
            Assert.Equal(
                MutationOperationState.Executing,
                (await repository.GetAsync(created.Operation.OperationId))!.State);

            var expiredLeaseRecovery = new MutationRecoveryCoordinator(
                repository,
                audit,
                new FixedTimeProvider(Now.AddMinutes(3)));

            var recovered = await expiredLeaseRecovery.RecoverInterruptedExecutionsAsync();
            Assert.Equal(1, recovered);
            Assert.Equal(
                MutationOperationState.FailedBeforeDispatch,
                (await repository.GetAsync(created.Operation.OperationId))!.State);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Unknown_execution_retains_resource_claim_after_lease_expiry()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-unknown-claim-{Guid.NewGuid():N}.db");
        try
        {
            var time = new MutableTimeProvider(Now);
            var repository = new AdoMutationOperationRepository(
                new SqliteMutationDbConnectionFactory(path),
                time);
            await repository.InitializeAsync();

            var operation = CreateReadyOperation("unknown-holder");
            var created = await repository.CreateAsync(operation.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

            var aggregate = MutationOperation.Restore(created.Operation);
            var generation = aggregate.ClaimExecution(Now.AddSeconds(1), Now.AddMinutes(2));
            var claimed = await repository.TrySaveAsync(
                aggregate.Snapshot,
                created.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, claimed.Outcome);

            var resourceClaim = await repository.TryAcquireResourceClaimsAsync(
                aggregate.Snapshot.OperationId,
                generation,
                aggregate.Snapshot.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(MutationResourceClaimOutcome.Acquired, resourceClaim.Outcome);

            aggregate.MarkDispatchStarted(Now.AddSeconds(2));
            var dispatched = await repository.TrySaveAsync(
                aggregate.Snapshot,
                claimed.Operation!.Version);
            Assert.Equal(MutationSaveOutcome.Saved, dispatched.Outcome);

            var recovery = new MutationRecoveryCoordinator(
                repository,
                new CapturingMutationAuditSink(),
                new FixedTimeProvider(Now.AddMinutes(3)));
            var recovered = await recovery.RecoverInterruptedExecutionsAsync();
            Assert.Equal(1, recovered);

            time.SetUtcNow(Now.AddMinutes(3));

            var competitor = CreateReadyOperation("unknown-competitor");
            var competitorCreated = await repository.CreateAsync(competitor.Snapshot);
            Assert.Equal(MutationCreateOutcome.Created, competitorCreated.Outcome);

            var competitorAggregate = MutationOperation.Restore(competitorCreated.Operation);
            var competitorGeneration = competitorAggregate.ClaimExecution(
                Now.AddMinutes(3),
                Now.AddMinutes(5));
            var competitorSaved = await repository.TrySaveAsync(
                competitorAggregate.Snapshot,
                competitorCreated.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, competitorSaved.Outcome);

            var blocked = await repository.TryAcquireResourceClaimsAsync(
                competitorAggregate.Snapshot.OperationId,
                competitorGeneration,
                competitorAggregate.Snapshot.ResourceKeys,
                Now.AddMinutes(5));
            Assert.Equal(MutationResourceClaimOutcome.Conflict, blocked.Outcome);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task PostgreSql_long_identity_idempotency_scope_is_fixed_size_and_index_safe_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var repository = new AdoMutationOperationRepository(
            new PostgreSqlMutationDbConnectionFactory(connectionString),
            new FixedTimeProvider(Now));
        await repository.InitializeAsync();

        var principal = "oidc:" + new string('p', 3_500);
        var clusterId = new string('c', 240);
        var idempotencyKey = $"pg-long-scope-{Guid.NewGuid():N}";
        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicCreate));
        var intent = new MutationIntentDescriptor(
            MutationOperationKind.TopicCreate,
            clusterId,
            "{\"operation\":\"create-topic\",\"name\":\"long-scope-test\"}",
            new[] { $"cluster/{clusterId}/topic/long-scope-test" },
            new[] { new MutationPrecondition("topic", "absent") });

        MutationOperation Build() =>
            MutationOperation.CreatePreview(
                principal,
                intent,
                risk,
                "v0.5-p1",
                Now.AddMinutes(5),
                Now,
                idempotencyKey);

        var first = Build();
        var second = Build();

        Assert.Equal(64, first.Snapshot.IdempotencyScope.Length);
        Assert.DoesNotContain(principal, first.Snapshot.IdempotencyScope, StringComparison.Ordinal);
        Assert.DoesNotContain(clusterId, first.Snapshot.IdempotencyScope, StringComparison.Ordinal);

        var created = await repository.CreateAsync(first.Snapshot);
        var replay = await repository.CreateAsync(second.Snapshot);

        Assert.Equal(MutationCreateOutcome.Created, created.Outcome);
        Assert.Equal(MutationCreateOutcome.ExistingSameIntent, replay.Outcome);
        Assert.Equal(created.Operation.OperationId, replay.Operation.OperationId);
        Assert.Equal(64, replay.Operation.IdempotencyScope.Length);
    }

    [Fact]
    public async Task PostgreSql_multibyte_resource_claim_uses_fixed_size_index_key_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var repository = new AdoMutationOperationRepository(
            new PostgreSqlMutationDbConnectionFactory(connectionString),
            new FixedTimeProvider(Now));
        await repository.InitializeAsync();

        var longResourceKey = new string('界', 1_024);
        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicCreate));

        MutationOperation Build(string idempotencyKey)
        {
            var operation = MutationOperation.CreatePreview(
                "oidc:https://idp.example|alice",
                new MutationIntentDescriptor(
                    MutationOperationKind.TopicCreate,
                    "prod",
                    "{\"operation\":\"long-resource-claim\"}",
                    new[] { longResourceKey },
                    new[] { new MutationPrecondition("topic", "absent") },
                    AuthorizationTargets: new[]
                    {
                        new MutationAuthorizationTarget(
                            Kafdeck.Core.Security.AuthorizationAction.TopicCreate,
                            "prod",
                            "long-resource-claim"),
                    }),
                risk,
                "v0.5-p1",
                Now.AddMinutes(5),
                Now,
                idempotencyKey);
            operation.OpenForConfirmation(Now);
            operation.Confirm(
                operation.Snapshot.RequesterPrincipalId,
                operation.Snapshot.PreviewHash,
                Now);
            return operation;
        }

        var first = Build($"pg-long-resource-{Guid.NewGuid():N}");
        var firstCreated = await repository.CreateAsync(first.Snapshot);
        var firstExecuting = MutationOperation.Restore(firstCreated.Operation);
        var firstGeneration = firstExecuting.ClaimExecution(
            Now.AddSeconds(1),
            Now.AddMinutes(2));
        Assert.Equal(
            MutationSaveOutcome.Saved,
            (await repository.TrySaveAsync(
                firstExecuting.Snapshot,
                firstCreated.Operation.Version)).Outcome);

        var firstClaim = await repository.TryAcquireResourceClaimsAsync(
            firstExecuting.Snapshot.OperationId,
            firstGeneration,
            firstExecuting.Snapshot.ResourceKeys,
            Now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Acquired, firstClaim.Outcome);

        var competitor = Build($"pg-long-resource-competitor-{Guid.NewGuid():N}");
        var competitorCreated = await repository.CreateAsync(competitor.Snapshot);
        var competitorExecuting = MutationOperation.Restore(competitorCreated.Operation);
        var competitorGeneration = competitorExecuting.ClaimExecution(
            Now.AddSeconds(1),
            Now.AddMinutes(2));
        Assert.Equal(
            MutationSaveOutcome.Saved,
            (await repository.TrySaveAsync(
                competitorExecuting.Snapshot,
                competitorCreated.Operation.Version)).Outcome);

        var blocked = await repository.TryAcquireResourceClaimsAsync(
            competitorExecuting.Snapshot.OperationId,
            competitorGeneration,
            competitorExecuting.Snapshot.ResourceKeys,
            Now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Conflict, blocked.Outcome);

        await repository.ReleaseResourceClaimsAsync(
            firstExecuting.Snapshot.OperationId,
            firstGeneration);
    }

    [Fact]
    public async Task PostgreSql_repository_coordinates_concurrent_idempotency_cas_and_resource_claims_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var repository = new AdoMutationOperationRepository(
            new PostgreSqlMutationDbConnectionFactory(connectionString),
            new FixedTimeProvider(Now));
        await repository.InitializeAsync();

        var idempotencyKey = $"pg-{Guid.NewGuid():N}";
        var firstCandidate = CreateOperation(idempotencyKey, "{\"provider\":\"postgres\"}");
        var secondCandidate = CreateOperation(idempotencyKey, "{\"provider\":\"postgres\"}");

        var createResults = await Task.WhenAll(
            repository.CreateAsync(firstCandidate.Snapshot),
            repository.CreateAsync(secondCandidate.Snapshot));

        Assert.Single(createResults, item => item.Outcome == MutationCreateOutcome.Created);
        Assert.Single(createResults, item => item.Outcome == MutationCreateOutcome.ExistingSameIntent);
        Assert.Equal(createResults[0].Operation.OperationId, createResults[1].Operation.OperationId);

        var persisted = await repository.GetAsync(createResults[0].Operation.OperationId);
        Assert.NotNull(persisted);

        var left = MutationOperation.Restore(persisted!);
        var right = MutationOperation.Restore(persisted!);
        left.Confirm(left.Snapshot.RequesterPrincipalId, left.Snapshot.PreviewHash, Now.AddSeconds(1));
        right.Cancel(Now.AddSeconds(1));

        var casResults = await Task.WhenAll(
            repository.TrySaveAsync(left.Snapshot, persisted!.Version),
            repository.TrySaveAsync(right.Snapshot, persisted.Version));

        Assert.Single(casResults, item => item.Outcome == MutationSaveOutcome.Saved);
        Assert.Single(casResults, item => item.Outcome == MutationSaveOutcome.VersionConflict);

        var holder = CreateReadyOperation($"pg-holder-{Guid.NewGuid():N}");
        var holderCreated = await repository.CreateAsync(holder.Snapshot);
        Assert.Equal(MutationCreateOutcome.Created, holderCreated.Outcome);

        var holderExecuting = MutationOperation.Restore(holderCreated.Operation);
        var holderGeneration = holderExecuting.ClaimExecution(Now.AddSeconds(2), Now.AddMinutes(2));
        var holderSaved = await repository.TrySaveAsync(
            holderExecuting.Snapshot,
            holderCreated.Operation.Version);
        Assert.Equal(MutationSaveOutcome.Saved, holderSaved.Outcome);

        var competingOperation = CreateReadyOperation($"pg-claim-{Guid.NewGuid():N}");
        var competingCreated = await repository.CreateAsync(competingOperation.Snapshot);
        Assert.Equal(MutationCreateOutcome.Created, competingCreated.Outcome);

        var competingExecuting = MutationOperation.Restore(competingCreated.Operation);
        var competingGeneration = competingExecuting.ClaimExecution(Now.AddSeconds(2), Now.AddMinutes(2));
        var competingSaved = await repository.TrySaveAsync(
            competingExecuting.Snapshot,
            competingCreated.Operation.Version);
        Assert.Equal(MutationSaveOutcome.Saved, competingSaved.Outcome);

        var firstClaim = await repository.TryAcquireResourceClaimsAsync(
            holderExecuting.Snapshot.OperationId,
            holderGeneration,
            holderExecuting.Snapshot.ResourceKeys,
            Now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Acquired, firstClaim.Outcome);

        var blockedClaim = await repository.TryAcquireResourceClaimsAsync(
            competingExecuting.Snapshot.OperationId,
            competingGeneration,
            competingExecuting.Snapshot.ResourceKeys,
            Now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Conflict, blockedClaim.Outcome);

        await repository.ReleaseResourceClaimsAsync(
            holderExecuting.Snapshot.OperationId,
            holderGeneration);

        var secondClaim = await repository.TryAcquireResourceClaimsAsync(
            competingExecuting.Snapshot.OperationId,
            competingGeneration,
            competingExecuting.Snapshot.ResourceKeys,
            Now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Acquired, secondClaim.Outcome);

        await repository.ReleaseResourceClaimsAsync(
            competingExecuting.Snapshot.OperationId,
            competingGeneration);
    }

    [Fact]
    public async Task PostgreSql_atomic_lease_renewal_prevents_recovery_after_previous_expiry_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var time = new MutableTimeProvider(Now);
        var repository = new AdoMutationOperationRepository(
            new PostgreSqlMutationDbConnectionFactory(connectionString),
            time);
        await repository.InitializeAsync();

        var holder = CreateReadyOperation($"pg-renew-{Guid.NewGuid():N}");
        var created = await repository.CreateAsync(holder.Snapshot);
        Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

        var aggregate = MutationOperation.Restore(created.Operation);
        var generation = aggregate.ClaimExecution(
            Now,
            Now.AddSeconds(20));
        var saved = await repository.TrySaveAsync(
            aggregate.Snapshot,
            created.Operation.Version);
        Assert.Equal(MutationSaveOutcome.Saved, saved.Outcome);

        var clusterSlot = await repository.TryAcquireClusterExecutionSlotAsync(
            aggregate.Snapshot.OperationId,
            generation,
            aggregate.Snapshot.ClusterId,
            maxConcurrentPerCluster: 1,
            Now.AddSeconds(20));
        Assert.Equal(MutationClusterSlotOutcome.Acquired, clusterSlot.Outcome);

        var claimed = await repository.TryAcquireResourceClaimsAsync(
            aggregate.Snapshot.OperationId,
            generation,
            aggregate.Snapshot.ResourceKeys,
            Now.AddSeconds(20));
        Assert.Equal(MutationResourceClaimOutcome.Acquired, claimed.Outcome);

        time.SetUtcNow(Now.AddSeconds(19));
        var renewalExpectedVersion = aggregate.Snapshot.Version;
        aggregate.RenewExecutionLease(
            time.GetUtcNow(),
            Now.AddSeconds(39));

        var renewed = await repository.TryRenewExecutionLeaseAsync(
            aggregate.Snapshot,
            renewalExpectedVersion);
        Assert.Equal(MutationLeaseRenewOutcome.Renewed, renewed.Outcome);
        Assert.Equal(Now.AddSeconds(39), renewed.Operation!.ExecutionClaimExpiresAtUtc);

        time.SetUtcNow(Now.AddSeconds(21));
        var recovery = new MutationRecoveryCoordinator(
            repository,
            new CapturingMutationAuditSink(),
            time);

        var recovered = await recovery.RecoverInterruptedExecutionsAsync();
        Assert.Equal(0, recovered);

        var persisted = await repository.GetAsync(aggregate.Snapshot.OperationId);
        Assert.NotNull(persisted);
        Assert.Equal(MutationOperationState.Executing, persisted!.State);
        Assert.Equal(Now.AddSeconds(39), persisted.ExecutionClaimExpiresAtUtc);

        await repository.ReleaseResourceClaimsAsync(
            aggregate.Snapshot.OperationId,
            generation);
        await repository.ReleaseClusterExecutionSlotAsync(
            aggregate.Snapshot.OperationId,
            generation);
    }

    [Fact]
    public async Task PostgreSql_cluster_concurrency_cap_is_global_across_repository_instances_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var leftRepository = new AdoMutationOperationRepository(
            new PostgreSqlMutationDbConnectionFactory(connectionString),
            new FixedTimeProvider(Now));
        var rightRepository = new AdoMutationOperationRepository(
            new PostgreSqlMutationDbConnectionFactory(connectionString),
            new FixedTimeProvider(Now));
        await leftRepository.InitializeAsync();
        await rightRepository.InitializeAsync();

        var left = CreateReadyOperation(
            $"pg-cluster-slot-left-{Guid.NewGuid():N}",
            "cluster/prod/topic/left");
        var right = CreateReadyOperation(
            $"pg-cluster-slot-right-{Guid.NewGuid():N}",
            "cluster/prod/topic/right");

        var leftCreated = await leftRepository.CreateAsync(left.Snapshot);
        var rightCreated = await rightRepository.CreateAsync(right.Snapshot);
        Assert.Equal(MutationCreateOutcome.Created, leftCreated.Outcome);
        Assert.Equal(MutationCreateOutcome.Created, rightCreated.Outcome);

        var leftAggregate = MutationOperation.Restore(leftCreated.Operation);
        var rightAggregate = MutationOperation.Restore(rightCreated.Operation);
        var leftGeneration = leftAggregate.ClaimExecution(Now, Now.AddMinutes(2));
        var rightGeneration = rightAggregate.ClaimExecution(Now, Now.AddMinutes(2));

        Assert.Equal(
            MutationSaveOutcome.Saved,
            (await leftRepository.TrySaveAsync(
                leftAggregate.Snapshot,
                leftCreated.Operation.Version)).Outcome);
        Assert.Equal(
            MutationSaveOutcome.Saved,
            (await rightRepository.TrySaveAsync(
                rightAggregate.Snapshot,
                rightCreated.Operation.Version)).Outcome);

        var acquisitions = await Task.WhenAll(
            leftRepository.TryAcquireClusterExecutionSlotAsync(
                leftAggregate.Snapshot.OperationId,
                leftGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddMinutes(2)),
            rightRepository.TryAcquireClusterExecutionSlotAsync(
                rightAggregate.Snapshot.OperationId,
                rightGeneration,
                "prod",
                maxConcurrentPerCluster: 1,
                Now.AddMinutes(2)));

        Assert.Single(
            acquisitions,
            result => result.Outcome == MutationClusterSlotOutcome.Acquired);
        Assert.Single(
            acquisitions,
            result => result.Outcome == MutationClusterSlotOutcome.Saturated);

        var leftWon = acquisitions[0].Outcome == MutationClusterSlotOutcome.Acquired;
        var winnerRepository = leftWon ? leftRepository : rightRepository;
        var loserRepository = leftWon ? rightRepository : leftRepository;
        var winnerOperation = leftWon ? leftAggregate.Snapshot : rightAggregate.Snapshot;
        var loserOperation = leftWon ? rightAggregate.Snapshot : leftAggregate.Snapshot;
        var winnerGeneration = leftWon ? leftGeneration : rightGeneration;
        var loserGeneration = leftWon ? rightGeneration : leftGeneration;

        await winnerRepository.ReleaseClusterExecutionSlotAsync(
            winnerOperation.OperationId,
            winnerGeneration);

        var afterRelease = await loserRepository.TryAcquireClusterExecutionSlotAsync(
            loserOperation.OperationId,
            loserGeneration,
            "prod",
            maxConcurrentPerCluster: 1,
            Now.AddMinutes(2));

        Assert.Equal(MutationClusterSlotOutcome.Acquired, afterRelease.Outcome);
        Assert.Equal(0, afterRelease.SlotNumber);

        await loserRepository.ReleaseClusterExecutionSlotAsync(
            loserOperation.OperationId,
            loserGeneration);
    }

    [Fact]
    public async Task PostgreSql_claim_recovery_race_leaves_no_orphan_resource_claim_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var repository = new AdoMutationOperationRepository(
            new PostgreSqlMutationDbConnectionFactory(connectionString),
            new FixedTimeProvider(Now));
        await repository.InitializeAsync();

        var holder = CreateReadyOperation($"pg-race-holder-{Guid.NewGuid():N}");
        var created = await repository.CreateAsync(holder.Snapshot);
        Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

        var aggregate = MutationOperation.Restore(created.Operation);
        var generation = aggregate.ClaimExecution(
            Now.AddSeconds(1),
            Now.AddMinutes(2));
        var saved = await repository.TrySaveAsync(
            aggregate.Snapshot,
            created.Operation.Version);
        Assert.Equal(MutationSaveOutcome.Saved, saved.Outcome);

        var recovery = new MutationRecoveryCoordinator(
            repository,
            new CapturingMutationAuditSink(),
            new FixedTimeProvider(Now.AddMinutes(3)));

        var claimTask = repository.TryAcquireResourceClaimsAsync(
            aggregate.Snapshot.OperationId,
            generation,
            aggregate.Snapshot.ResourceKeys,
            Now.AddMinutes(2));
        var recoveryTask = recovery.RecoverInterruptedExecutionsAsync();

        await Task.WhenAll(claimTask, recoveryTask);
        var claimResult = await claimTask;
        _ = await recoveryTask;

        Assert.Contains(
            claimResult.Outcome,
            new[]
            {
                MutationResourceClaimOutcome.Acquired,
                MutationResourceClaimOutcome.InvalidExecutionClaim,
            });

        var persisted = await repository.GetAsync(aggregate.Snapshot.OperationId);
        Assert.NotNull(persisted);
        Assert.Equal(MutationOperationState.FailedBeforeDispatch, persisted!.State);

        var competitor = CreateReadyOperation($"pg-race-competitor-{Guid.NewGuid():N}");
        var competitorCreated = await repository.CreateAsync(competitor.Snapshot);
        var competitorAggregate = MutationOperation.Restore(competitorCreated.Operation);
        var competitorGeneration = competitorAggregate.ClaimExecution(
            Now.AddSeconds(2),
            Now.AddMinutes(5));
        Assert.Equal(
            MutationSaveOutcome.Saved,
            (await repository.TrySaveAsync(
                competitorAggregate.Snapshot,
                competitorCreated.Operation.Version)).Outcome);

        var competitorClaim = await repository.TryAcquireResourceClaimsAsync(
            competitorAggregate.Snapshot.OperationId,
            competitorGeneration,
            competitorAggregate.Snapshot.ResourceKeys,
            Now.AddMinutes(5));

        Assert.Equal(MutationResourceClaimOutcome.Acquired, competitorClaim.Outcome);
        await repository.ReleaseResourceClaimsAsync(
            competitorAggregate.Snapshot.OperationId,
            competitorGeneration);
    }

    private static MutationOperation CreateOperation(
        string idempotencyKey,
        string canonicalIntent,
        string resourceKey = "cluster/prod/topic/payments")
    {
        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicCreate));
        var intent = new MutationIntentDescriptor(
            MutationOperationKind.TopicCreate,
            "prod",
            canonicalIntent,
            new[] { resourceKey },
            new[] { new MutationPrecondition("topic", "absent") });

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            intent,
            risk,
            "v0.5-p1",
            Now.AddMinutes(5),
            Now,
            idempotencyKey);
        operation.OpenForConfirmation(Now);
        return operation;
    }

    private static MutationOperation CreateReadyOperation(
        string idempotencyKey,
        string resourceKey = "cluster/prod/topic/payments")
    {
        var operation = CreateOperation(
            idempotencyKey,
            "{\"operation\":\"create-topic\"}",
            resourceKey);
        operation.Confirm(
            operation.Snapshot.RequesterPrincipalId,
            operation.Snapshot.PreviewHash,
            Now);
        return operation;
    }

    private static void DeleteSqliteFiles(string databasePath)
    {
        foreach (var path in new[] { databasePath, databasePath + "-wal", databasePath + "-shm" })
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort test cleanup; all connections are scoped and already disposed.
            }
        }
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() => _now;
    }

    private sealed class MutableTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;

        public MutableTimeProvider(DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() => _now;

        public void SetUtcNow(DateTimeOffset now)
        {
            _now = now;
        }
    }

    private sealed class AdvancingAllowedGuard : IMutationPreDispatchGuard
    {
        private readonly MutableTimeProvider _timeProvider;
        private readonly DateTimeOffset _advanceTo;

        public AdvancingAllowedGuard(
            MutableTimeProvider timeProvider,
            DateTimeOffset advanceTo)
        {
            _timeProvider = timeProvider;
            _advanceTo = advanceTo;
        }

        public Task<MutationPreDispatchGuardResult> ValidateAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _timeProvider.SetUtcNow(_advanceTo);
            return Task.FromResult(MutationPreDispatchGuardResult.Allowed);
        }
    }

    private sealed class AllowedGuard : IMutationPreDispatchGuard
    {
        public Task<MutationPreDispatchGuardResult> ValidateAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(MutationPreDispatchGuardResult.Allowed);
        }
    }

    private sealed class DenyingGuard : IMutationPreDispatchGuard
    {
        public Task<MutationPreDispatchGuardResult> ValidateAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(
                new MutationPreDispatchGuardResult(
                    MutationPreDispatchGuardOutcome.AuthorizationDenied,
                    "authorization_denied"));
        }
    }

    private sealed class MalformedResultHandler : IMutationExecutionHandler
    {
        public MalformedResultHandler(MutationOperationKind operationKind)
        {
            OperationKind = operationKind;
        }

        public MutationOperationKind OperationKind { get; }

        public Task<MutationProviderResult> ExecuteAsync(
            MutationExecutionContext context,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.AppliedVerified,
                    "verified",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["error.detail"] = "this free-form evidence is not admitted",
                    }));
        }
    }

    private sealed class RecordingHandler : IMutationExecutionHandler
    {
        public RecordingHandler(MutationOperationKind operationKind)
        {
            OperationKind = operationKind;
        }

        public MutationOperationKind OperationKind { get; }
        public int CallCount { get; private set; }

        public Task<MutationProviderResult> ExecuteAsync(
            MutationExecutionContext context,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CallCount++;
            return Task.FromResult(new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "verified",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["partition"] = "3",
                    ["offset"] = "42",
                }));
        }
    }


    private sealed class IgnoringCancellationHandler : IMutationExecutionHandler
    {
        public IgnoringCancellationHandler(MutationOperationKind operationKind)
        {
            OperationKind = operationKind;
        }

        public MutationOperationKind OperationKind { get; }
        public TaskCompletionSource<bool> Complete { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<MutationProviderResult> ExecuteAsync(
            MutationExecutionContext context,
            CancellationToken cancellationToken = default)
        {
            await Complete.Task.ConfigureAwait(false);
            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "late_verified");
        }
    }

    private sealed class FirstCallIgnoresCancellationHandler : IMutationExecutionHandler
    {
        public FirstCallIgnoresCancellationHandler(MutationOperationKind operationKind)
        {
            OperationKind = operationKind;
        }

        public MutationOperationKind OperationKind { get; }
        public int CallCount { get; private set; }
        public TaskCompletionSource<bool> ReleaseFirst { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<MutationProviderResult> ExecuteAsync(
            MutationExecutionContext context,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            if (CallCount == 1)
            {
                await ReleaseFirst.Task.ConfigureAwait(false);
                return new MutationProviderResult(
                    MutationExecutionResultKind.AppliedVerified,
                    "late_first");
            }

            cancellationToken.ThrowIfCancellationRequested();
            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "verified_second");
        }
    }

    private sealed class SynchronouslyBlockingHandler :
        IMutationExecutionHandler,
        IDisposable
    {
        private readonly ManualResetEventSlim _release = new(false);

        public SynchronouslyBlockingHandler(MutationOperationKind operationKind)
        {
            OperationKind = operationKind;
        }

        public MutationOperationKind OperationKind { get; }
        public int CallCount { get; private set; }
        public TaskCompletionSource<bool> Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<MutationProviderResult> ExecuteAsync(
            MutationExecutionContext context,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            Started.TrySetResult(true);

            // Deliberately blocks before returning Task and ignores cancellation.
            _release.Wait();

            return Task.FromResult(new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "late_sync_verified"));
        }

        public void Release() => _release.Set();

        public void Dispose()
        {
            _release.Set();
            _release.Dispose();
        }
    }

    private sealed class BlockingRecordingHandler : IMutationExecutionHandler
    {
        public BlockingRecordingHandler(MutationOperationKind operationKind)
        {
            OperationKind = operationKind;
        }

        public MutationOperationKind OperationKind { get; }
        public int CallCount { get; private set; }
        public bool ProviderCancellationObserved { get; private set; }
        public TaskCompletionSource<bool> Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> Complete { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<MutationProviderResult> ExecuteAsync(
            MutationExecutionContext context,
            CancellationToken cancellationToken = default)
        {
            CallCount++;
            Started.TrySetResult(true);
            try
            {
                await Complete.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                ProviderCancellationObserved = true;
                throw;
            }

            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "verified");
        }
    }

    private sealed class TestMaterialDigestService : IMutationMaterialDigestService
    {
        public string ComputeDigest(ReadOnlySpan<byte> material) =>
            Convert.ToHexString(SHA256.HashData(material)).ToLowerInvariant();
    }

    private sealed class MaterialRecordingHandler : IMutationExecutionHandler
    {
        private readonly string _materialName;
        private readonly byte[] _expected;

        public MaterialRecordingHandler(
            MutationOperationKind operationKind,
            string materialName,
            byte[] expected)
        {
            OperationKind = operationKind;
            _materialName = materialName;
            _expected = expected;
        }

        public MutationOperationKind OperationKind { get; }
        public int CallCount { get; private set; }

        public Task<MutationProviderResult> ExecuteAsync(
            MutationExecutionContext context,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CallCount++;
            Assert.Equal(
                _expected,
                context.Material.GetRequired(_materialName).ToArray());

            return Task.FromResult(new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "material_verified"));
        }
    }

    private sealed class ThrowingPreDispatchTerminalAuditSink : IMutationAuditSink
    {
        public ValueTask WriteAsync(
            MutationAuditEvent auditEvent,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (auditEvent.EventType == MutationAuditEventType.Completed &&
                auditEvent.State == MutationOperationState.FailedBeforeDispatch)
            {
                throw new InvalidOperationException(
                    "simulated pre-dispatch terminal audit failure");
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingPostDispatchAuditSink : IMutationAuditSink
    {
        public bool DispatchAuditAttempted { get; private set; }
        public bool CompletedAuditAttempted { get; private set; }

        public ValueTask WriteAsync(
            MutationAuditEvent auditEvent,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (auditEvent.EventType == MutationAuditEventType.DispatchStarted)
            {
                DispatchAuditAttempted = true;
                throw new InvalidOperationException("simulated dispatch audit sink failure");
            }

            if (auditEvent.EventType == MutationAuditEventType.Completed &&
                auditEvent.State == MutationOperationState.AppliedVerified)
            {
                CompletedAuditAttempted = true;
                throw new InvalidOperationException("simulated terminal audit sink failure");
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class CapturingMutationAuditSink : IMutationAuditSink
    {
        public List<MutationAuditEvent> Events { get; } = new();

        public ValueTask WriteAsync(
            MutationAuditEvent auditEvent,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Events.Add(auditEvent);
            return ValueTask.CompletedTask;
        }
    }
}
