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

            var claim = await repository.TryAcquireResourceClaimsAsync(
                confirmed.Snapshot.OperationId,
                1,
                confirmed.Snapshot.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(MutationResourceClaimOutcome.Acquired, claim.Outcome);

            var other = CreateOperation("idem-2", "{\"value\":3}");
            var blocked = await repository.TryAcquireResourceClaimsAsync(
                other.Snapshot.OperationId,
                1,
                other.Snapshot.ResourceKeys,
                Now.AddMinutes(2));
            Assert.Equal(MutationResourceClaimOutcome.Conflict, blocked.Outcome);

            await repository.ReleaseResourceClaimsAsync(
                confirmed.Snapshot.OperationId,
                1);

            var acquiredAfterRelease = await repository.TryAcquireResourceClaimsAsync(
                other.Snapshot.OperationId,
                1,
                other.Snapshot.ResourceKeys,
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
            aggregate.ClaimExecution(Now.AddSeconds(1));
            var claimed = await repository.TrySaveAsync(
                aggregate.Snapshot,
                created.Operation.Version);
            Assert.Equal(MutationSaveOutcome.Saved, claimed.Outcome);

            var audit = new CapturingMutationAuditSink();
            var recovery = new MutationRecoveryCoordinator(repository, audit, time);

            var recovered = await recovery.RecoverInterruptedExecutionsAsync();

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
            aggregate.ClaimExecution(Now.AddSeconds(1));
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

            var recovered = await recovery.RecoverInterruptedExecutionsAsync();

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

        var winner = casResults.Single(item => item.Outcome == MutationSaveOutcome.Saved).Operation!;
        var competingOperation = CreateOperation(
            $"pg-claim-{Guid.NewGuid():N}",
            "{\"provider\":\"postgres\",\"claim\":true}");

        var firstClaim = await repository.TryAcquireResourceClaimsAsync(
            winner.OperationId,
            1,
            winner.ResourceKeys,
            Now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Acquired, firstClaim.Outcome);

        var blockedClaim = await repository.TryAcquireResourceClaimsAsync(
            competingOperation.Snapshot.OperationId,
            1,
            competingOperation.Snapshot.ResourceKeys,
            Now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Conflict, blockedClaim.Outcome);

        await repository.ReleaseResourceClaimsAsync(winner.OperationId, 1);

        var secondClaim = await repository.TryAcquireResourceClaimsAsync(
            competingOperation.Snapshot.OperationId,
            1,
            competingOperation.Snapshot.ResourceKeys,
            Now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Acquired, secondClaim.Outcome);

        await repository.ReleaseResourceClaimsAsync(
            competingOperation.Snapshot.OperationId,
            1);
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
