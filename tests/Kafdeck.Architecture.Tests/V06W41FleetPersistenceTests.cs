using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W41FleetPersistenceTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 24, 10, 30, 0, TimeSpan.Zero);

    [Fact]
    public async Task Sqlite_fleet_progress_and_conflict_obligations_are_durable_and_cas_guarded()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-v06-fleet-{Guid.NewGuid():N}.db");
        try
        {
            await ExerciseDurableStoreAsync(
                new SqliteMutationDbConnectionFactory(path));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task PostgreSql_fleet_progress_and_conflict_obligations_are_durable_and_cas_guarded_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        // The persistence suite runs PostgreSQL facts in parallel. Give this
        // W41 schema its own namespace so concurrent CREATE TABLE IF NOT EXISTS
        // statements from unrelated tests cannot contend on pg_type creation.
        var adminFactory = new PostgreSqlMutationDbConnectionFactory(connectionString);
        var schema = $"kafdeck_v06_{Guid.NewGuid():N}";
        await using (var connection = await adminFactory.OpenAsync())
        await using (var create = connection.CreateCommand())
        {
            create.CommandText = $"CREATE SCHEMA {schema}";
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            var isolatedConnectionString =
                $"{connectionString.TrimEnd(';')};Search Path={schema}";
            await ExerciseDurableStoreAsync(
                new PostgreSqlMutationDbConnectionFactory(isolatedConnectionString));
        }
        finally
        {
            await using var connection = await adminFactory.OpenAsync();
            await using var drop = connection.CreateCommand();
            drop.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }


    [Fact]
    public async Task Sqlite_shared_conflict_guard_is_bidirectional_and_race_safe()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-v06-shared-conflict-{Guid.NewGuid():N}.db");
        try
        {
            await ExerciseSharedConflictGuardAsync(
                new SqliteMutationDbConnectionFactory(path));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task PostgreSql_shared_conflict_guard_is_bidirectional_and_race_safe_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var adminFactory = new PostgreSqlMutationDbConnectionFactory(connectionString);
        var schema = $"kafdeck_v06_guard_{Guid.NewGuid():N}";
        await using (var connection = await adminFactory.OpenAsync())
        await using (var create = connection.CreateCommand())
        {
            create.CommandText = $"CREATE SCHEMA {schema}";
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            var isolatedConnectionString =
                $"{connectionString.TrimEnd(';')};Search Path={schema}";
            await ExerciseSharedConflictGuardAsync(
                new PostgreSqlMutationDbConnectionFactory(isolatedConnectionString));
        }
        finally
        {
            await using var connection = await adminFactory.OpenAsync();
            await using var drop = connection.CreateCommand();
            drop.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }


    [Fact]
    public async Task Sqlite_mixed_version_execution_fence_requires_drain_and_upgrades_schema()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-v06-version-fence-{Guid.NewGuid():N}.db");
        try
        {
            await ExerciseMixedVersionFenceAsync(
                new SqliteMutationDbConnectionFactory(path));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task PostgreSql_mixed_version_execution_fence_requires_drain_and_upgrades_schema_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var adminFactory = new PostgreSqlMutationDbConnectionFactory(connectionString);
        var schema = $"kafdeck_v06_version_{Guid.NewGuid():N}";
        await using (var connection = await adminFactory.OpenAsync())
        await using (var create = connection.CreateCommand())
        {
            create.CommandText = $"CREATE SCHEMA {schema}";
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            var isolatedConnectionString =
                $"{connectionString.TrimEnd(';')};Search Path={schema}";
            await ExerciseMixedVersionFenceAsync(
                new PostgreSqlMutationDbConnectionFactory(isolatedConnectionString));
        }
        finally
        {
            await using var connection = await adminFactory.OpenAsync();
            await using var drop = connection.CreateCommand();
            drop.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }


    [Fact]
    public async Task Sqlite_recovery_preserves_sticky_obligation_after_lease_expiry()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-v06-recovery-obligation-{Guid.NewGuid():N}.db");
        try
        {
            await ExerciseRecoveryPreservesStickyObligationAsync(
                new SqliteMutationDbConnectionFactory(path));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task PostgreSql_recovery_preserves_sticky_obligation_after_lease_expiry_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var adminFactory = new PostgreSqlMutationDbConnectionFactory(connectionString);
        var schema = $"kafdeck_v06_recovery_{Guid.NewGuid():N}";
        await using (var connection = await adminFactory.OpenAsync())
        await using (var create = connection.CreateCommand())
        {
            create.CommandText = $"CREATE SCHEMA {schema}";
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            var isolatedConnectionString =
                $"{connectionString.TrimEnd(';')};Search Path={schema}";
            await ExerciseRecoveryPreservesStickyObligationAsync(
                new PostgreSqlMutationDbConnectionFactory(isolatedConnectionString));
        }
        finally
        {
            await using var connection = await adminFactory.OpenAsync();
            await using var drop = connection.CreateCommand();
            drop.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Fleet_state_refuses_orphan_parent_identity()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-v06-orphan-{Guid.NewGuid():N}.db");
        try
        {
            var factory = new SqliteMutationDbConnectionFactory(path);
            var parentRepository = new AdoMutationOperationRepository(factory);
            await parentRepository.InitializeAsync();

            var store = new AdoFleetMutationStateStore(factory);
            await store.InitializeAsync();

            var missingOperationId = Guid.NewGuid();
            var progress = FleetOperationProgress.Create(
                missingOperationId,
                workerGeneration: 1,
                Now);
            var progressCreate = await store.CreateProgressAsync(progress.Snapshot);
            Assert.Equal(
                FleetProgressCreateOutcome.ParentOperationNotFound,
                progressCreate.Outcome);

            var obligation = FleetConflictObligation.Create(
                missingOperationId,
                "step-1",
                FleetConflictKeyCodec.Topic("prod", "orphan"),
                "sha256:orphan-effect",
                Now);
            var obligationCreate = await store.CreateConflictObligationAsync(
                obligation.Snapshot);
            Assert.Equal(
                FleetConflictObligationCreateOutcome.ParentOperationNotFound,
                obligationCreate.Outcome);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    private static async Task ExerciseDurableStoreAsync(
        IMutationDbConnectionFactory factory)
    {
        var parentRepository = new AdoMutationOperationRepository(factory);
        await parentRepository.InitializeAsync();

        var store = new AdoFleetMutationStateStore(factory);
        await store.InitializeAsync();

        var parent = CreateParentOperation();
        var parentCreated = await parentRepository.CreateAsync(parent.Snapshot);
        Assert.Equal(MutationCreateOutcome.Created, parentCreated.Outcome);

        var createdProgress = FleetOperationProgress.Create(
            parent.Snapshot.OperationId,
            workerGeneration: 1,
            Now);
        var progressCreate = await store.CreateProgressAsync(createdProgress.Snapshot);
        Assert.Equal(FleetProgressCreateOutcome.Created, progressCreate.Outcome);

        var winningProgress = FleetOperationProgress.Restore(progressCreate.Progress!);
        Assert.Equal(
            TimeSpan.FromHours(3),
            winningProgress.ChargeActiveObservation(
                TimeSpan.FromHours(3),
                Now.AddHours(3)));
        var winningSave = await store.TrySaveProgressAsync(
            winningProgress.Snapshot,
            expectedVersion: 0);
        Assert.Equal(FleetProgressSaveOutcome.Saved, winningSave.Outcome);

        var staleProgress = FleetOperationProgress.Restore(progressCreate.Progress!);
        staleProgress.ChargeActiveObservation(
            TimeSpan.FromMinutes(30),
            Now.AddMinutes(30));
        var staleSave = await store.TrySaveProgressAsync(
            staleProgress.Snapshot,
            expectedVersion: 0);
        Assert.Equal(FleetProgressSaveOutcome.VersionConflict, staleSave.Outcome);
        Assert.Equal(
            TimeSpan.FromHours(3),
            staleSave.Progress!.ActiveObservationElapsed);

        // Reconstruct the store to prove accounting is persisted rather than
        // process-local and remains available after a worker/process restart.
        var restartedStore = new AdoFleetMutationStateStore(factory);
        await restartedStore.InitializeAsync();
        var reloadedProgress = await restartedStore.GetProgressAsync(
            parent.Snapshot.OperationId);
        Assert.NotNull(reloadedProgress);
        Assert.Equal(TimeSpan.FromHours(3), reloadedProgress!.ActiveObservationElapsed);
        Assert.Equal(1, reloadedProgress.Version);

        var conflictKey = FleetConflictKeyCodec.Topic(
            "prod",
            $"fleet-{Guid.NewGuid():N}");
        var obligation = FleetConflictObligation.Create(
            parent.Snapshot.OperationId,
            "submit-1",
            conflictKey,
            "sha256:effect-a",
            Now.AddMinutes(1));
        var obligationCreate = await restartedStore.CreateConflictObligationAsync(
            obligation.Snapshot);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.Created,
            obligationCreate.Outcome);

        var duplicateSameEffect = FleetConflictObligation.Create(
            parent.Snapshot.OperationId,
            "submit-1",
            conflictKey,
            "sha256:effect-a",
            Now.AddMinutes(2));
        var sameEffect = await restartedStore.CreateConflictObligationAsync(
            duplicateSameEffect.Snapshot);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.ExistingSameEffect,
            sameEffect.Outcome);
        Assert.Equal(
            obligation.Snapshot.ObligationId,
            sameEffect.Obligation!.ObligationId);

        var duplicateDifferentEffect = FleetConflictObligation.Create(
            parent.Snapshot.OperationId,
            "submit-1",
            conflictKey,
            "sha256:effect-b",
            Now.AddMinutes(3));
        var differentEffect = await restartedStore.CreateConflictObligationAsync(
            duplicateDifferentEffect.Snapshot);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.ExistingDifferentEffect,
            differentEffect.Outcome);

        var blocking = await restartedStore.FindBlockingConflictObligationAsync(conflictKey);
        Assert.NotNull(blocking);
        Assert.True(blocking!.BlocksConflictingDispatch);

        // The durable admission path must apply the same conservative relation
        // as FleetConflictScope, not only exact typed-key equality.
        var conflictTarget = FleetConflictKeyCodec.Decode(conflictKey);
        var partitionBlocking = await restartedStore.FindBlockingConflictObligationAsync(
            FleetConflictKeyCodec.TopicPartition(
                conflictTarget.PhysicalClusterId,
                conflictTarget.ResourceId,
                partitionId: 0));
        Assert.NotNull(partitionBlocking);
        Assert.Equal(
            blocking.ObligationId,
            partitionBlocking!.ObligationId);

        var configurationBlocking = await restartedStore.FindBlockingConflictObligationAsync(
            FleetConflictKeyCodec.TopicConfiguration(
                conflictTarget.PhysicalClusterId,
                conflictTarget.ResourceId,
                "retention.ms"));
        Assert.NotNull(configurationBlocking);
        Assert.Equal(
            blocking.ObligationId,
            configurationBlocking!.ObligationId);

        var siblingParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(siblingParent.Snapshot)).Outcome);
        var siblingObligation = FleetConflictObligation.Create(
            siblingParent.Snapshot.OperationId,
            "submit-sibling",
            FleetConflictKeyCodec.TopicConfiguration(
                conflictTarget.PhysicalClusterId,
                conflictTarget.ResourceId,
                "cleanup.policy"),
            "sha256:sibling-effect",
            Now.AddMinutes(4));
        var siblingCreate = await restartedStore.CreateConflictObligationAsync(
            siblingObligation.Snapshot);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.FleetConflictScopeConflict,
            siblingCreate.Outcome);
        Assert.NotNull(siblingCreate.Obligation);
        Assert.Equal(
            blocking.ObligationId,
            siblingCreate.Obligation!.ObligationId);

        // Two different typed targets in the same physical-topic scope racing
        // from independent operations must have exactly one admission winner.
        var raceTopic = $"fleet-scope-race-{Guid.NewGuid():N}";
        var raceTopicParent = CreateParentOperation();
        var racePartitionParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(raceTopicParent.Snapshot)).Outcome);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(racePartitionParent.Snapshot)).Outcome);

        var raceTopicObligation = FleetConflictObligation.Create(
            raceTopicParent.Snapshot.OperationId,
            "scope-race-topic",
            FleetConflictKeyCodec.Topic("prod", raceTopic),
            "sha256:scope-race-topic",
            Now.AddMinutes(5));
        var racePartitionObligation = FleetConflictObligation.Create(
            racePartitionParent.Snapshot.OperationId,
            "scope-race-partition",
            FleetConflictKeyCodec.TopicPartition("prod", raceTopic, 1),
            "sha256:scope-race-partition",
            Now.AddMinutes(5));

        var raceTopicTask = restartedStore.CreateConflictObligationAsync(
            raceTopicObligation.Snapshot);
        var racePartitionTask = restartedStore.CreateConflictObligationAsync(
            racePartitionObligation.Snapshot);
        await Task.WhenAll(raceTopicTask, racePartitionTask);

        var raceResults = new[] { await raceTopicTask, await racePartitionTask };
        Assert.Single(
            raceResults,
            result => result.Outcome == FleetConflictObligationCreateOutcome.Created);
        Assert.Single(
            raceResults,
            result => result.Outcome ==
                FleetConflictObligationCreateOutcome.FleetConflictScopeConflict);

        var raceBlocker = await restartedStore.FindBlockingConflictObligationAsync(
            FleetConflictKeyCodec.TopicConfiguration(
                "prod",
                raceTopic,
                "retention.ms"));
        Assert.NotNull(raceBlocker);
        Assert.Contains(
            raceBlocker!.ObligationId,
            new[]
            {
                raceTopicObligation.Snapshot.ObligationId,
                racePartitionObligation.Snapshot.ObligationId,
            });

        var quarantined = FleetConflictObligation.Restore(blocking);
        quarantined.ApplyDisposition(
            FleetUncertaintyDispositionOutcome.QuarantineUnknown,
            Guid.NewGuid(),
            "sha256:bounded-readback-a",
            Now.AddHours(1));
        var quarantineSave = await restartedStore.TrySaveConflictObligationAsync(
            quarantined.Snapshot,
            expectedVersion: blocking.Version);
        Assert.Equal(
            FleetConflictObligationSaveOutcome.Saved,
            quarantineSave.Outcome);
        Assert.NotNull(await restartedStore.FindBlockingConflictObligationAsync(conflictKey));

        var superseded = FleetConflictObligation.Restore(quarantineSave.Obligation!);
        superseded.ApplyDisposition(
            FleetUncertaintyDispositionOutcome.SupersedeUnknownForNewIntent,
            Guid.NewGuid(),
            "sha256:bounded-readback-b",
            Now.AddHours(2));
        var supersedeSave = await restartedStore.TrySaveConflictObligationAsync(
            superseded.Snapshot,
            expectedVersion: quarantineSave.Obligation!.Version);
        Assert.Equal(
            FleetConflictObligationSaveOutcome.Saved,
            supersedeSave.Outcome);
        Assert.Null(await restartedStore.FindBlockingConflictObligationAsync(conflictKey));

        var tombstone = await restartedStore.GetConflictObligationAsync(
            superseded.Snapshot.ObligationId);
        Assert.NotNull(tombstone);
        Assert.True(tombstone!.NoRedispatchTombstone);
        Assert.True(tombstone.RequiresUnresolvedPredecessorBinding);

        var staleObligation = FleetConflictObligation.Restore(quarantineSave.Obligation!);
        staleObligation.ApplyDisposition(
            FleetUncertaintyDispositionOutcome.ObservedNonApplication,
            Guid.NewGuid(),
            "sha256:stale-readback",
            Now.AddHours(3));
        var staleObligationSave = await restartedStore.TrySaveConflictObligationAsync(
            staleObligation.Snapshot,
            expectedVersion: quarantineSave.Obligation!.Version);
        Assert.Equal(
            FleetConflictObligationSaveOutcome.VersionConflict,
            staleObligationSave.Outcome);
        Assert.True(staleObligationSave.Obligation!.NoRedispatchTombstone);
    }




    private static async Task ExerciseRecoveryPreservesStickyObligationAsync(
        IMutationDbConnectionFactory factory)
    {
        var now = new DateTimeOffset(2026, 9, 24, 13, 0, 0, TimeSpan.Zero);
        var time = new MutableTimeProvider(now);
        var repository = new AdoMutationOperationRepository(factory, time);
        await repository.InitializeAsync();

        var store = new AdoFleetMutationStateStore(factory);
        await store.InitializeAsync();

        var topic = $"recovery-{Guid.NewGuid():N}";
        var legacyKey = $"cluster/prod/topic/{topic}";
        var requester = "oidc:https://idp.example|w41-recovery";
        var operation = MutationOperation.CreatePreview(
            requester,
            new MutationIntentDescriptor(
                MutationOperationKind.TopicCreate,
                "prod",
                $"{{\"operation\":\"recovery\",\"topic\":\"{topic}\"}}",
                new[] { legacyKey },
                Preconditions: new[]
                {
                    new MutationPrecondition("topic", "absent"),
                }),
            MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.TopicCreate)),
            "v0.6-w41-recovery",
            now.AddHours(2),
            now,
            $"recovery-{Guid.NewGuid():N}");

        operation.OpenForConfirmation(now.AddMilliseconds(1));
        operation.Confirm(
            requester,
            operation.Snapshot.PreviewHash,
            now.AddMilliseconds(2),
            operation.Snapshot.ConfirmationChallenge);

        var created = await repository.CreateAsync(operation.Snapshot);
        Assert.Equal(MutationCreateOutcome.Created, created.Outcome);

        var executing = MutationOperation.Restore(created.Operation);
        var generation = executing.ClaimExecution(
            now.AddSeconds(1),
            now.AddMinutes(2));
        var executingSave = await repository.TrySaveAsync(
            executing.Snapshot,
            created.Operation.Version);
        Assert.Equal(MutationSaveOutcome.Saved, executingSave.Outcome);

        var claim = await repository.TryAcquireResourceClaimsAsync(
            executing.Snapshot.OperationId,
            generation,
            new[] { legacyKey },
            now.AddMinutes(2));
        Assert.Equal(MutationResourceClaimOutcome.Acquired, claim.Outcome);

        // Same-operation claim + obligation coexistence is intentional: the
        // durable obligation represents outstanding effect uncertainty while
        // the renewable claim is only a worker/execution lease boundary.
        var obligation = FleetConflictObligation.Create(
            executing.Snapshot.OperationId,
            "dispatch-intent",
            FleetConflictKeyCodec.Topic("prod", topic),
            "sha256:recovery-effect",
            now.AddSeconds(2));
        var obligationCreate = await store.CreateConflictObligationAsync(
            obligation.Snapshot);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.Created,
            obligationCreate.Outcome);

        var ready = MutationOperation.CreatePreview(
            "oidc:https://idp.example|ready-restored",
            new MutationIntentDescriptor(
                MutationOperationKind.TopicCreate,
                "prod",
                $"{{\"operation\":\"ready\",\"topic\":\"ready-{topic}\"}}",
                new[] { $"cluster/prod/topic/ready-{topic}" }),
            MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.TopicCreate)),
            "v0.6-w41-recovery",
            now.AddHours(2),
            now,
            $"ready-{Guid.NewGuid():N}");
        ready.OpenForConfirmation(now.AddMilliseconds(1));
        ready.Confirm(
            ready.Snapshot.RequesterPrincipalId,
            ready.Snapshot.PreviewHash,
            now.AddMilliseconds(2),
            ready.Snapshot.ConfirmationChallenge);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(ready.Snapshot)).Outcome);

        time.SetUtcNow(now.AddMinutes(3));
        var recovery = new MutationRecoveryCoordinator(
            repository,
            new NoopMutationAuditSink(),
            time);
        var recovered = await recovery.RecoverInterruptedExecutionsAsync();
        Assert.Equal(1, recovered);

        var recoveredParent = await repository.GetAsync(executing.Snapshot.OperationId);
        Assert.NotNull(recoveredParent);
        Assert.Equal(
            MutationOperationState.FailedBeforeDispatch,
            recoveredParent!.State);

        var readyAfterRecovery = await repository.GetAsync(ready.Snapshot.OperationId);
        Assert.NotNull(readyAfterRecovery);
        Assert.Equal(MutationOperationState.Ready, readyAfterRecovery!.State);

        var blocking = await store.FindBlockingConflictObligationAsync(
            FleetConflictKeyCodec.Topic("prod", topic));
        Assert.NotNull(blocking);
        Assert.True(blocking!.BlocksConflictingDispatch);

        var competitor = MutationOperation.CreatePreview(
            "oidc:https://idp.example|competitor",
            new MutationIntentDescriptor(
                MutationOperationKind.TopicCreate,
                "prod",
                $"{{\"operation\":\"competitor\",\"topic\":\"{topic}\"}}",
                new[] { legacyKey }),
            MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.TopicCreate)),
            "v0.6-w41-recovery",
            now.AddHours(3),
            now.AddMinutes(3),
            $"competitor-{Guid.NewGuid():N}");
        competitor.OpenForConfirmation(now.AddMinutes(3).AddMilliseconds(1));
        competitor.Confirm(
            competitor.Snapshot.RequesterPrincipalId,
            competitor.Snapshot.PreviewHash,
            now.AddMinutes(3).AddMilliseconds(2),
            competitor.Snapshot.ConfirmationChallenge);
        var competitorCreated = await repository.CreateAsync(competitor.Snapshot);
        var competitorExecuting = MutationOperation.Restore(competitorCreated.Operation);
        var competitorGeneration = competitorExecuting.ClaimExecution(
            now.AddMinutes(3).AddSeconds(1),
            now.AddMinutes(5));
        Assert.Equal(
            MutationSaveOutcome.Saved,
            (await repository.TrySaveAsync(
                competitorExecuting.Snapshot,
                competitorCreated.Operation.Version)).Outcome);

        var blocked = await repository.TryAcquireResourceClaimsAsync(
            competitorExecuting.Snapshot.OperationId,
            competitorGeneration,
            new[] { legacyKey },
            now.AddMinutes(5));
        Assert.Equal(MutationResourceClaimOutcome.Conflict, blocked.Outcome);
        Assert.Equal(legacyKey, blocked.ConflictingResourceKey);
    }

    private static async Task ExerciseMixedVersionFenceAsync(
        IMutationDbConnectionFactory factory)
    {
        var repository = new AdoMutationOperationRepository(factory);
        await repository.InitializeAsync();
        Assert.Equal(5, await ReadMutationSchemaVersionAsync(factory));

        var executing = CreateExecutingLegacyTopicOperation(
            $"mixed-version-{Guid.NewGuid():N}");
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(executing.Operation.Snapshot)).Outcome);

        var slot = await repository.TryAcquireClusterExecutionSlotAsync(
            executing.Operation.Snapshot.OperationId,
            executing.Generation,
            "prod",
            maxConcurrentPerCluster: 4,
            executing.ClaimExpiresAtUtc);
        Assert.Equal(MutationClusterSlotOutcome.Acquired, slot.Outcome);

        await SetMutationSchemaVersionAsync(factory, 4);

        var blockedUpgrade = new AdoMutationOperationRepository(factory);
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => blockedUpgrade.InitializeAsync());
        Assert.Contains("drained", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(4, await ReadMutationSchemaVersionAsync(factory));

        var aggregate = MutationOperation.Restore(executing.Operation.Snapshot);
        aggregate.Complete(
            MutationExecutionResultKind.FailedBeforeDispatch,
            "maintenance_window_drain",
            DateTimeOffset.UtcNow);
        Assert.Equal(
            MutationSaveOutcome.Saved,
            (await repository.TrySaveAsync(
                aggregate.Snapshot,
                executing.Operation.Snapshot.Version)).Outcome);
        await repository.ReleaseClusterExecutionSlotAsync(
            executing.Operation.Snapshot.OperationId,
            executing.Generation);

        var upgraded = new AdoMutationOperationRepository(factory);
        await upgraded.InitializeAsync();
        Assert.Equal(5, await ReadMutationSchemaVersionAsync(factory));

        // Model an already-running v0.5 process: it initialized successfully
        // while the durable marker was v4, stays alive and does not re-run
        // startup after another process activates v5.
        await SetMutationSchemaVersionAsync(factory, 4);
        var legacyExecutor = new LegacyV05AdmissionProbe(factory);
        await legacyExecutor.InitializeAsync();

        var activatingV06 = new AdoMutationOperationRepository(factory);
        await activatingV06.InitializeAsync();
        Assert.Equal(5, await ReadMutationSchemaVersionAsync(factory));

        await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(
            () => legacyExecutor.TryAcquireClusterSlotWithLegacySqlAsync());
        await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(
            () => legacyExecutor.TryAcquireResourceClaimWithLegacySqlAsync());

        // Current v0.6 SQL carries the durable execution epoch and remains
        // admissible after the old statement shape has been fenced out.
        var current = CreateExecutingLegacyTopicOperation(
            $"current-v5-{Guid.NewGuid():N}");
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await activatingV06.CreateAsync(current.Operation.Snapshot)).Outcome);
        Assert.Equal(
            MutationClusterSlotOutcome.Acquired,
            (await activatingV06.TryAcquireClusterExecutionSlotAsync(
                current.Operation.Snapshot.OperationId,
                current.Generation,
                "prod",
                maxConcurrentPerCluster: 4,
                current.ClaimExpiresAtUtc)).Outcome);
        Assert.Equal(
            MutationResourceClaimOutcome.Acquired,
            (await activatingV06.TryAcquireResourceClaimsAsync(
                current.Operation.Snapshot.OperationId,
                current.Generation,
                current.Operation.Snapshot.ResourceKeys,
                current.ClaimExpiresAtUtc)).Outcome);
        await activatingV06.ReleaseResourceClaimsAsync(
            current.Operation.Snapshot.OperationId,
            current.Generation);
        await activatingV06.ReleaseClusterExecutionSlotAsync(
            current.Operation.Snapshot.OperationId,
            current.Generation);

        await SetMutationSchemaVersionAsync(factory, 6);
        var futureVersion = new AdoMutationOperationRepository(factory);
        var unsupported = await Assert.ThrowsAsync<InvalidOperationException>(
            () => futureVersion.InitializeAsync());
        Assert.Contains("unsupported", unsupported.Message, StringComparison.OrdinalIgnoreCase);

        await SetMutationSchemaVersionAsync(factory, 5);
    }

    private static async Task<int> ReadMutationSchemaVersionAsync(
        IMutationDbConnectionFactory factory)
    {
        await using var connection = await factory.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_schema_info
            WHERE component = 'mutation-operations'
            """;
        return Convert.ToInt32(
            await command.ExecuteScalarAsync(),
            System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task SetMutationSchemaVersionAsync(
        IMutationDbConnectionFactory factory,
        int version)
    {
        await using var connection = await factory.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            UPDATE kafdeck_schema_info
            SET schema_version = @schema_version
            WHERE component = 'mutation-operations'
            """;
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@schema_version";
        parameter.Value = version;
        command.Parameters.Add(parameter);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

    private static async Task ExerciseSharedConflictGuardAsync(
        IMutationDbConnectionFactory factory)
    {
        var parentRepository = new AdoMutationOperationRepository(factory);
        await parentRepository.InitializeAsync();

        var store = new AdoFleetMutationStateStore(factory);
        await store.InitializeAsync();

        // Force a post-schema connection so the connection factory installs the
        // cross-generation database guard before either admission path executes.
        await using (var guardConnection = await factory.OpenAsync())
        {
        }

        var firstTopic = $"guard-claim-first-{Guid.NewGuid():N}";
        var firstLegacy = CreateExecutingLegacyTopicOperation(firstTopic);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(firstLegacy.Operation.Snapshot)).Outcome);

        var firstFleetParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(firstFleetParent.Snapshot)).Outcome);

        var firstLegacyKey = $"cluster/prod/topic/{firstTopic}";
        var firstClaim = await parentRepository.TryAcquireResourceClaimsAsync(
            firstLegacy.Operation.Snapshot.OperationId,
            firstLegacy.Generation,
            new[] { firstLegacyKey },
            firstLegacy.ClaimExpiresAtUtc);
        Assert.Equal(MutationResourceClaimOutcome.Acquired, firstClaim.Outcome);

        var blockedObligation = FleetConflictObligation.Create(
            firstFleetParent.Snapshot.OperationId,
            "claim-first",
            FleetConflictKeyCodec.Topic("prod", firstTopic),
            "sha256:claim-first",
            DateTimeOffset.UtcNow);
        var blockedCreate = await store.CreateConflictObligationAsync(
            blockedObligation.Snapshot);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.LegacyResourceClaimConflict,
            blockedCreate.Outcome);
        Assert.Null(blockedCreate.Obligation);

        await parentRepository.ReleaseResourceClaimsAsync(
            firstLegacy.Operation.Snapshot.OperationId,
            firstLegacy.Generation);

        var secondTopic = $"guard-obligation-first-{Guid.NewGuid():N}";
        var secondFleetParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(secondFleetParent.Snapshot)).Outcome);

        var blockingObligation = FleetConflictObligation.Create(
            secondFleetParent.Snapshot.OperationId,
            "obligation-first",
            FleetConflictKeyCodec.Topic("prod", secondTopic),
            "sha256:obligation-first",
            DateTimeOffset.UtcNow);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.Created,
            (await store.CreateConflictObligationAsync(blockingObligation.Snapshot)).Outcome);

        var secondLegacy = CreateExecutingLegacyTopicOperation(secondTopic);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(secondLegacy.Operation.Snapshot)).Outcome);

        var secondLegacyKey = $"cluster/prod/topic/{secondTopic}";
        var blockedClaim = await parentRepository.TryAcquireResourceClaimsAsync(
            secondLegacy.Operation.Snapshot.OperationId,
            secondLegacy.Generation,
            new[] { secondLegacyKey },
            secondLegacy.ClaimExpiresAtUtc);
        Assert.Equal(MutationResourceClaimOutcome.Conflict, blockedClaim.Outcome);
        Assert.Equal(secondLegacyKey, blockedClaim.ConflictingResourceKey);

        var raceTopic = $"guard-race-{Guid.NewGuid():N}";
        var raceLegacy = CreateExecutingLegacyTopicOperation(raceTopic);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(raceLegacy.Operation.Snapshot)).Outcome);
        var raceFleetParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await parentRepository.CreateAsync(raceFleetParent.Snapshot)).Outcome);

        var raceLegacyKey = $"cluster/prod/topic/{raceTopic}";
        var raceObligation = FleetConflictObligation.Create(
            raceFleetParent.Snapshot.OperationId,
            "race",
            FleetConflictKeyCodec.Topic("prod", raceTopic),
            "sha256:race",
            DateTimeOffset.UtcNow);

        var claimTask = parentRepository.TryAcquireResourceClaimsAsync(
            raceLegacy.Operation.Snapshot.OperationId,
            raceLegacy.Generation,
            new[] { raceLegacyKey },
            raceLegacy.ClaimExpiresAtUtc);
        var obligationTask = store.CreateConflictObligationAsync(
            raceObligation.Snapshot);

        await Task.WhenAll(claimTask, obligationTask);
        var raceClaim = await claimTask;
        var raceCreate = await obligationTask;

        var claimWon = raceClaim.Outcome == MutationResourceClaimOutcome.Acquired;
        var obligationWon =
            raceCreate.Outcome == FleetConflictObligationCreateOutcome.Created;
        Assert.NotEqual(claimWon, obligationWon);

        if (claimWon)
        {
            Assert.Equal(
                FleetConflictObligationCreateOutcome.LegacyResourceClaimConflict,
                raceCreate.Outcome);
        }
        else
        {
            Assert.Equal(MutationResourceClaimOutcome.Conflict, raceClaim.Outcome);
            Assert.Equal(raceLegacyKey, raceClaim.ConflictingResourceKey);
        }
    }

    private static (MutationOperation Operation, long Generation, DateTimeOffset ClaimExpiresAtUtc)
        CreateExecutingLegacyTopicOperation(string topic)
    {
        var now = DateTimeOffset.UtcNow;
        var requester = "oidc:https://idp.example|legacy-guard";
        var operation = MutationOperation.CreatePreview(
            requester,
            new MutationIntentDescriptor(
                MutationOperationKind.TopicCreate,
                "prod",
                $"{{\"operation\":\"guard\",\"topic\":\"{topic}\"}}",
                new[] { $"cluster/prod/topic/{topic}" },
                Preconditions: new[]
                {
                    new MutationPrecondition("topic", "absent"),
                }),
            MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.TopicCreate)),
            "v0.6-w41-guard",
            now.AddHours(2),
            now,
            $"guard-{Guid.NewGuid():N}");

        operation.OpenForConfirmation(now.AddMilliseconds(1));
        operation.Confirm(
            requester,
            operation.Snapshot.PreviewHash,
            now.AddMilliseconds(2),
            operation.Snapshot.ConfirmationChallenge);
        var claimExpiresAtUtc = now.AddMinutes(30);
        var generation = operation.ClaimExecution(
            now.AddMilliseconds(3),
            claimExpiresAtUtc);

        return (operation, generation, claimExpiresAtUtc);
    }

    private static MutationOperation CreateParentOperation()
    {
        var suffix = Guid.NewGuid().ToString("N");
        return MutationOperation.CreatePreview(
            "oidc:https://idp.example|fleet-persistence",
            new MutationIntentDescriptor(
                MutationOperationKind.TopicCreate,
                "prod",
                $"{{\"operation\":\"fleet-parent\",\"id\":\"{suffix}\"}}",
                new[] { $"cluster/prod/topic/fleet-{suffix}" },
                Preconditions: new[]
                {
                    new MutationPrecondition("topic", "absent"),
                }),
            MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.TopicCreate)),
            "v0.6-w41",
            Now.AddHours(8),
            Now,
            $"fleet-parent-{suffix}");
    }


    private sealed class LegacyV05AdmissionProbe
    {
        private readonly IMutationDbConnectionFactory _factory;
        private bool _initialized;

        public LegacyV05AdmissionProbe(IMutationDbConnectionFactory factory)
        {
            _factory = factory;
        }

        public async Task InitializeAsync()
        {
            var version = await ReadMutationSchemaVersionAsync(_factory);
            if (version != 4)
            {
                throw new InvalidOperationException(
                    "Legacy v0.5 executor accepts only mutation schema v4.");
            }

            _initialized = true;
        }

        public async Task TryAcquireClusterSlotWithLegacySqlAsync()
        {
            RequireInitialized();
            await using var connection = await _factory.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                INSERT INTO kafdeck_mutation_cluster_slots (
                    cluster_id,
                    slot_number,
                    operation_id,
                    execution_generation,
                    expires_at_utc)
                VALUES (
                    @cluster_id,
                    @slot_number,
                    @operation_id,
                    @execution_generation,
                    @expires_at_utc)
                """;
            AddLegacyParameter(command, "@cluster_id", "legacy-v05");
            AddLegacyParameter(command, "@slot_number", 0);
            AddLegacyParameter(command, "@operation_id", Guid.NewGuid().ToString("D"));
            AddLegacyParameter(command, "@execution_generation", 1L);
            AddLegacyParameter(
                command,
                "@expires_at_utc",
                DateTimeOffset.UtcNow.AddMinutes(5).ToString("O"));
            await command.ExecuteNonQueryAsync();
        }

        public async Task TryAcquireResourceClaimWithLegacySqlAsync()
        {
            RequireInitialized();
            await using var connection = await _factory.OpenAsync();
            await using var command = connection.CreateCommand();
            var suffix = Guid.NewGuid().ToString("N");
            command.CommandText =
                """
                INSERT INTO kafdeck_mutation_resource_claims (
                    resource_key_hash,
                    resource_key,
                    operation_id,
                    execution_generation,
                    expires_at_utc)
                VALUES (
                    @resource_key_hash,
                    @resource_key,
                    @operation_id,
                    @execution_generation,
                    @expires_at_utc)
                """;
            AddLegacyParameter(command, "@resource_key_hash", $"legacy-hash-{suffix}");
            AddLegacyParameter(command, "@resource_key", $"cluster/prod/topic/legacy-{suffix}");
            AddLegacyParameter(command, "@operation_id", Guid.NewGuid().ToString("D"));
            AddLegacyParameter(command, "@execution_generation", 1L);
            AddLegacyParameter(
                command,
                "@expires_at_utc",
                DateTimeOffset.UtcNow.AddMinutes(5).ToString("O"));
            await command.ExecuteNonQueryAsync();
        }

        private void RequireInitialized()
        {
            if (!_initialized)
            {
                throw new InvalidOperationException("Legacy executor was not initialized.");
            }
        }

        private static void AddLegacyParameter(
            System.Data.Common.DbCommand command,
            string name,
            object value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            command.Parameters.Add(parameter);
        }
    }

    private sealed class MutableTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;

        public MutableTimeProvider(DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() => _now;

        public void SetUtcNow(DateTimeOffset now) => _now = now;
    }

    private sealed class NoopMutationAuditSink : IMutationAuditSink
    {
        public ValueTask WriteAsync(
            MutationAuditEvent auditEvent,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }
    }

    private static void DeleteSqliteFiles(string path)
    {
        foreach (var suffix in new[] { string.Empty, "-wal", "-shm" })
        {
            var candidate = path + suffix;
            if (File.Exists(candidate))
            {
                File.Delete(candidate);
            }
        }
    }
}
