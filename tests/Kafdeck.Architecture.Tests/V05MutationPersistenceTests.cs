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
                new MutationExecutorPolicy(
                    maxConcurrentPerCluster: 1,
                    operationTimeout: TimeSpan.FromSeconds(1),
                    resourceClaimTtl: TimeSpan.FromSeconds(20)),
                time);

            var result = await executor.ExecuteAsync(operation.Snapshot.OperationId);

            Assert.Equal(MutationOperationState.AppliedVerified, result.State);
            Assert.Equal(1, handler.CallCount);
            Assert.Contains(audit.Events, item => item.EventType == MutationAuditEventType.DispatchStarted);
            Assert.Contains(audit.Events, item => item.EventType == MutationAuditEventType.Completed);

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

    private static MutationOperation CreateOperation(string idempotencyKey, string canonicalIntent)
    {
        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.TopicCreate));
        var intent = new MutationIntentDescriptor(
            MutationOperationKind.TopicCreate,
            "prod",
            canonicalIntent,
            new[] { "cluster/prod/topic/payments" },
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

    private static MutationOperation CreateReadyOperation(string idempotencyKey)
    {
        var operation = CreateOperation(idempotencyKey, "{\"operation\":\"create-topic\"}");
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

    private sealed class RecordingHandler : IMutationExecutionHandler
    {
        public RecordingHandler(MutationOperationKind operationKind)
        {
            OperationKind = operationKind;
        }

        public MutationOperationKind OperationKind { get; }
        public int CallCount { get; private set; }

        public Task<MutationProviderResult> ExecuteAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CallCount++;
            return Task.FromResult(new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "verified"));
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
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken = default)
        {
            await Complete.Task.ConfigureAwait(false);
            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "late_verified");
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
            MutationOperationSnapshot operation,
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
