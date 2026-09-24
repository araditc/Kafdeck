using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
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
            var factory = new SqliteMutationDbConnectionFactory(path);
            await ExerciseLegacyPartitionGuardAndBackfillAsync(factory);
            await ExerciseSharedConflictGuardAsync(factory);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Sqlite_v2_conflict_guard_is_reinstalled_before_terminal_partition_admission()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-v06-guard-upgrade-{Guid.NewGuid():N}.db");
        try
        {
            var seedFactory = new SqliteMutationDbConnectionFactory(path);
            var repository = new AdoMutationOperationRepository(seedFactory);
            await repository.InitializeAsync();

            var store = new AdoFleetMutationStateStore(seedFactory);
            await store.InitializeAsync();

            await InstallLegacySqliteConflictGuardV2Async(seedFactory);
            Assert.Equal(
                2,
                await ReadSqliteConflictGuardSchemaVersionAsync(seedFactory));

            var clusterId = "prod/partition/eu";
            var topic = $"guard-upgrade-{Guid.NewGuid():N}";
            var parent = CreateParentOperation(clusterId);
            Assert.Equal(
                MutationCreateOutcome.Created,
                (await repository.CreateAsync(parent.Snapshot)).Outcome);

            var obligation = FleetConflictObligation.Create(
                parent.Snapshot.OperationId,
                "guard-upgrade",
                FleetConflictKeyCodec.Topic(clusterId, topic),
                "sha256:guard-upgrade",
                DateTimeOffset.UtcNow);
            Assert.Equal(
                FleetConflictObligationCreateOutcome.Created,
                (await store.CreateConflictObligationAsync(obligation.Snapshot)).Outcome);

            var preUpgrade = CreateExecutingLegacyTopicOperation(topic, clusterId);
            Assert.Equal(
                MutationCreateOutcome.Created,
                (await repository.CreateAsync(preUpgrade.Operation.Snapshot)).Outcome);
            var partitionKey =
                $"cluster/{clusterId}/topic/{topic}/partition/4";
            var admittedByV2 = await repository.TryAcquireResourceClaimsAsync(
                preUpgrade.Operation.Snapshot.OperationId,
                preUpgrade.Generation,
                new[] { partitionKey },
                preUpgrade.ClaimExpiresAtUtc);
            Assert.Equal(MutationResourceClaimOutcome.Acquired, admittedByV2.Outcome);
            await repository.ReleaseResourceClaimsAsync(
                preUpgrade.Operation.Snapshot.OperationId,
                preUpgrade.Generation);

            // A new process must reject the stale v2 marker, reinstall the
            // corrected terminal-suffix triggers transactionally, and advance
            // the durable marker before any new admission can use the guard.
            var upgradedFactory = new SqliteMutationDbConnectionFactory(path);
            await using (var connection = await upgradedFactory.OpenAsync())
            {
            }

            Assert.Equal(
                3,
                await ReadSqliteConflictGuardSchemaVersionAsync(upgradedFactory));

            var upgradedRepository = new AdoMutationOperationRepository(upgradedFactory);
            await upgradedRepository.InitializeAsync();

            var postUpgrade = CreateExecutingLegacyTopicOperation(topic, clusterId);
            Assert.Equal(
                MutationCreateOutcome.Created,
                (await upgradedRepository.CreateAsync(postUpgrade.Operation.Snapshot)).Outcome);
            var blocked = await upgradedRepository.TryAcquireResourceClaimsAsync(
                postUpgrade.Operation.Snapshot.OperationId,
                postUpgrade.Generation,
                new[] { partitionKey },
                postUpgrade.ClaimExpiresAtUtc);
            Assert.Equal(MutationResourceClaimOutcome.Conflict, blocked.Outcome);
            Assert.Equal(partitionKey, blocked.ConflictingResourceKey);
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
            var factory =
                new PostgreSqlMutationDbConnectionFactory(isolatedConnectionString);
            await ExerciseLegacyPartitionGuardAndBackfillAsync(factory);
            await ExerciseSharedConflictGuardAsync(factory);
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
    public async Task PostgreSql_execution_version_fence_validation_is_schema_local_when_available()
    {
        var connectionString = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var adminFactory = new PostgreSqlMutationDbConnectionFactory(connectionString);
        var firstSchema = $"kafdeck_v06_fence_a_{Guid.NewGuid():N}";
        var secondSchema = $"kafdeck_v06_fence_b_{Guid.NewGuid():N}";

        await using (var connection = await adminFactory.OpenAsync())
        {
            foreach (var schema in new[] { firstSchema, secondSchema })
            {
                await using var create = connection.CreateCommand();
                create.CommandText = $"CREATE SCHEMA {schema}";
                await create.ExecuteNonQueryAsync();
            }
        }

        try
        {
            var firstFactory = new PostgreSqlMutationDbConnectionFactory(
                $"{connectionString.TrimEnd(';')};Search Path={firstSchema}");
            var secondFactory = new PostgreSqlMutationDbConnectionFactory(
                $"{connectionString.TrimEnd(';')};Search Path={secondSchema}");

            var first = new AdoMutationOperationRepository(firstFactory);
            var second = new AdoMutationOperationRepository(secondFactory);
            await first.InitializeAsync();
            await second.InitializeAsync();

            Assert.Equal(5, await ReadMutationSchemaVersionAsync(firstFactory));
            Assert.Equal(5, await ReadMutationSchemaVersionAsync(secondFactory));

            // Both schemas now contain triggers with the same PostgreSQL names.
            // Re-validating either schema must inspect only its own trigger
            // relations rather than counting same-named triggers database-wide.
            var firstReloaded = new AdoMutationOperationRepository(
                new PostgreSqlMutationDbConnectionFactory(
                    $"{connectionString.TrimEnd(';')};Search Path={firstSchema}"));
            var secondReloaded = new AdoMutationOperationRepository(
                new PostgreSqlMutationDbConnectionFactory(
                    $"{connectionString.TrimEnd(';')};Search Path={secondSchema}"));

            await firstReloaded.InitializeAsync();
            await secondReloaded.InitializeAsync();
        }
        finally
        {
            await using var connection = await adminFactory.OpenAsync();
            foreach (var schema in new[] { firstSchema, secondSchema })
            {
                await using var drop = connection.CreateCommand();
                drop.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
                await drop.ExecuteNonQueryAsync();
            }
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

    private static async Task ExerciseLegacyPartitionGuardAndBackfillAsync(
        IMutationDbConnectionFactory factory)
    {
        var repository = new AdoMutationOperationRepository(factory);
        await repository.InitializeAsync();

        // Seed one pre-fence fleet obligation using the legacy table/statement
        // shape before the current fleet store initializes. This models a
        // durable row created by an older fleet-state binary before rolling
        // upgrade/backfill installs the writer fence.
        var backfillTopic = $"legacy-backfill-{Guid.NewGuid():N}";
        var backfillParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(backfillParent.Snapshot)).Outcome);
        var backfillObligation = FleetConflictObligation.Create(
            backfillParent.Snapshot.OperationId,
            "legacy-backfill",
            FleetConflictKeyCodec.Topic("prod", backfillTopic),
            "sha256:legacy-backfill",
            DateTimeOffset.UtcNow);
        await SeedLegacyObligationBeforeFenceAsync(
            factory,
            backfillObligation.Snapshot);

        var store = new AdoFleetMutationStateStore(factory);
        await store.InitializeAsync();

        var durableLegacyKey = await ReadPersistedLegacyResourceKeyAsync(
            factory,
            backfillObligation.Snapshot.ObligationId);
        Assert.Equal(
            $"cluster/prod/topic/{backfillTopic}",
            durableLegacyKey);

        var backfillLegacy = CreateExecutingLegacyTopicOperation(backfillTopic);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(backfillLegacy.Operation.Snapshot)).Outcome);
        var backfillPartitionKey =
            $"cluster/prod/topic/{backfillTopic}/partition/9";
        var backfillBlocked = await repository.TryAcquireResourceClaimsAsync(
            backfillLegacy.Operation.Snapshot.OperationId,
            backfillLegacy.Generation,
            new[] { backfillPartitionKey },
            backfillLegacy.ClaimExpiresAtUtc);
        Assert.Equal(MutationResourceClaimOutcome.Conflict, backfillBlocked.Outcome);
        Assert.Equal(backfillPartitionKey, backfillBlocked.ConflictingResourceKey);

        // Ensure the cross-generation trigger set is installed/upgraded before
        // exercising the remaining legacy claim admission races.
        await using (var guardConnection = await factory.OpenAsync())
        {
        }

        var claimFirstTopic = $"partition-claim-first-{Guid.NewGuid():N}";
        var claimFirst = CreateExecutingLegacyTopicOperation(claimFirstTopic);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(claimFirst.Operation.Snapshot)).Outcome);
        var partitionKey =
            $"cluster/prod/topic/{claimFirstTopic}/partition/0";
        var acquired = await repository.TryAcquireResourceClaimsAsync(
            claimFirst.Operation.Snapshot.OperationId,
            claimFirst.Generation,
            new[] { partitionKey },
            claimFirst.ClaimExpiresAtUtc);
        Assert.Equal(MutationResourceClaimOutcome.Acquired, acquired.Outcome);

        var persistedClaim = await ReadPersistedResourceClaimAsync(
            factory,
            claimFirst.Operation.Snapshot.OperationId,
            claimFirst.Generation);
        Assert.Equal(partitionKey, persistedClaim.ResourceKey);
        Assert.Equal(
            Convert.ToHexString(
                    SHA256.HashData(Encoding.UTF8.GetBytes(partitionKey)))
                .ToLowerInvariant(),
            persistedClaim.ResourceKeyHash);

        var claimFirstFleetParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(claimFirstFleetParent.Snapshot)).Outcome);
        var blockedObligation = FleetConflictObligation.Create(
            claimFirstFleetParent.Snapshot.OperationId,
            "partition-claim-first",
            FleetConflictKeyCodec.TopicConfiguration(
                "prod",
                claimFirstTopic,
                "retention.ms"),
            "sha256:partition-claim-first",
            DateTimeOffset.UtcNow);
        var blockedCreate = await store.CreateConflictObligationAsync(
            blockedObligation.Snapshot);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.LegacyResourceClaimConflict,
            blockedCreate.Outcome);

        await repository.ReleaseResourceClaimsAsync(
            claimFirst.Operation.Snapshot.OperationId,
            claimFirst.Generation);

        var obligationFirstTopic =
            $"partition-obligation-first-{Guid.NewGuid():N}";
        var obligationFirstParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(obligationFirstParent.Snapshot)).Outcome);
        var firstObligation = FleetConflictObligation.Create(
            obligationFirstParent.Snapshot.OperationId,
            "partition-obligation-first",
            FleetConflictKeyCodec.TopicPartition(
                "prod",
                obligationFirstTopic,
                3),
            "sha256:partition-obligation-first",
            DateTimeOffset.UtcNow);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.Created,
            (await store.CreateConflictObligationAsync(firstObligation.Snapshot)).Outcome);

        var obligationFirstLegacy =
            CreateExecutingLegacyTopicOperation(obligationFirstTopic);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(obligationFirstLegacy.Operation.Snapshot)).Outcome);
        var obligationFirstPartitionKey =
            $"cluster/prod/topic/{obligationFirstTopic}/partition/3";
        var blockedClaim = await repository.TryAcquireResourceClaimsAsync(
            obligationFirstLegacy.Operation.Snapshot.OperationId,
            obligationFirstLegacy.Generation,
            new[] { obligationFirstPartitionKey },
            obligationFirstLegacy.ClaimExpiresAtUtc);
        Assert.Equal(MutationResourceClaimOutcome.Conflict, blockedClaim.Outcome);
        Assert.Equal(obligationFirstPartitionKey, blockedClaim.ConflictingResourceKey);

        var raceTopic = $"partition-race-{Guid.NewGuid():N}";
        var raceLegacy = CreateExecutingLegacyTopicOperation(raceTopic);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(raceLegacy.Operation.Snapshot)).Outcome);
        var raceFleetParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(raceFleetParent.Snapshot)).Outcome);

        var racePartitionKey = $"cluster/prod/topic/{raceTopic}/partition/1";
        var raceObligation = FleetConflictObligation.Create(
            raceFleetParent.Snapshot.OperationId,
            "partition-race",
            FleetConflictKeyCodec.Topic("prod", raceTopic),
            "sha256:partition-race",
            DateTimeOffset.UtcNow);

        var claimTask = repository.TryAcquireResourceClaimsAsync(
            raceLegacy.Operation.Snapshot.OperationId,
            raceLegacy.Generation,
            new[] { racePartitionKey },
            raceLegacy.ClaimExpiresAtUtc);
        var obligationTask = store.CreateConflictObligationAsync(
            raceObligation.Snapshot);
        await Task.WhenAll(claimTask, obligationTask);

        var raceClaim = await claimTask;
        var raceCreate = await obligationTask;
        Assert.NotEqual(
            raceClaim.Outcome == MutationResourceClaimOutcome.Acquired,
            raceCreate.Outcome == FleetConflictObligationCreateOutcome.Created);
        if (raceClaim.Outcome == MutationResourceClaimOutcome.Acquired)
        {
            Assert.Equal(
                FleetConflictObligationCreateOutcome.LegacyResourceClaimConflict,
                raceCreate.Outcome);
            await repository.ReleaseResourceClaimsAsync(
                raceLegacy.Operation.Snapshot.OperationId,
                raceLegacy.Generation);
        }
        else
        {
            Assert.Equal(MutationResourceClaimOutcome.Conflict, raceClaim.Outcome);
            Assert.Equal(racePartitionKey, raceClaim.ConflictingResourceKey);
        }

        // A cluster ID may itself contain "/partition/". Only the terminal
        // numeric partition suffix belongs to the legacy resource child; the
        // cluster portion must remain intact when deriving the guard scope.
        var markerCluster = "prod/partition/eu";
        var markerTopic = $"marker-topic-{Guid.NewGuid():N}";
        var markerLegacy = CreateExecutingLegacyTopicOperation(
            markerTopic,
            markerCluster);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(markerLegacy.Operation.Snapshot)).Outcome);
        var markerPartitionKey =
            $"cluster/{markerCluster}/topic/{markerTopic}/partition/4";
        var markerClaim = await repository.TryAcquireResourceClaimsAsync(
            markerLegacy.Operation.Snapshot.OperationId,
            markerLegacy.Generation,
            new[] { markerPartitionKey },
            markerLegacy.ClaimExpiresAtUtc);
        Assert.Equal(MutationResourceClaimOutcome.Acquired, markerClaim.Outcome);

        var markerFleetParent = CreateParentOperation(markerCluster);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(markerFleetParent.Snapshot)).Outcome);
        var markerBlockedObligation = FleetConflictObligation.Create(
            markerFleetParent.Snapshot.OperationId,
            "terminal-partition-marker",
            FleetConflictKeyCodec.TopicConfiguration(
                markerCluster,
                markerTopic,
                "retention.ms"),
            "sha256:terminal-partition-marker",
            DateTimeOffset.UtcNow);
        var markerBlockedCreate = await store.CreateConflictObligationAsync(
            markerBlockedObligation.Snapshot);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.LegacyResourceClaimConflict,
            markerBlockedCreate.Outcome);

        await repository.ReleaseResourceClaimsAsync(
            markerLegacy.Operation.Snapshot.OperationId,
            markerLegacy.Generation);

        var markerObligationParent = CreateParentOperation(markerCluster);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(markerObligationParent.Snapshot)).Outcome);
        var markerObligationFirst = FleetConflictObligation.Create(
            markerObligationParent.Snapshot.OperationId,
            "terminal-partition-marker-obligation-first",
            FleetConflictKeyCodec.Topic(
                markerCluster,
                markerTopic),
            "sha256:terminal-partition-marker-obligation-first",
            DateTimeOffset.UtcNow);
        Assert.Equal(
            FleetConflictObligationCreateOutcome.Created,
            (await store.CreateConflictObligationAsync(
                markerObligationFirst.Snapshot)).Outcome);

        var markerSecondLegacy = CreateExecutingLegacyTopicOperation(
            markerTopic,
            markerCluster);
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(markerSecondLegacy.Operation.Snapshot)).Outcome);
        var markerBlockedClaim = await repository.TryAcquireResourceClaimsAsync(
            markerSecondLegacy.Operation.Snapshot.OperationId,
            markerSecondLegacy.Generation,
            new[] { markerPartitionKey },
            markerSecondLegacy.ClaimExpiresAtUtc);
        Assert.Equal(
            MutationResourceClaimOutcome.Conflict,
            markerBlockedClaim.Outcome);
        Assert.Equal(markerPartitionKey, markerBlockedClaim.ConflictingResourceKey);

        // Simulate an already-running pre-fence fleet-state binary after the
        // current process has completed backfill/fence installation. Its old
        // INSERT/UPDATE statement shapes do not carry writer fence fields and
        // must be rejected by the database rather than recreating an obligation
        // with no durable legacy guard identity or canonical binding.
        var legacyFleetWriter = new LegacyFleetStateWriterProbe(factory);
        var legacyWriteParent = CreateParentOperation();
        Assert.Equal(
            MutationCreateOutcome.Created,
            (await repository.CreateAsync(legacyWriteParent.Snapshot)).Outcome);
        var legacyWriteObligation = FleetConflictObligation.Create(
            legacyWriteParent.Snapshot.OperationId,
            "legacy-writer-insert",
            FleetConflictKeyCodec.Topic(
                "prod",
                $"legacy-writer-{Guid.NewGuid():N}"),
            "sha256:legacy-writer-insert",
            DateTimeOffset.UtcNow);

        await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(
            () => legacyFleetWriter.TryInsertWithLegacySqlAsync(
                legacyWriteObligation.Snapshot));

        await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(
            () => legacyFleetWriter.TryUpdateWithLegacySqlAsync(
                backfillObligation.Snapshot.ObligationId));
    }

    private static async Task InstallLegacySqliteConflictGuardV2Async(
        IMutationDbConnectionFactory factory)
    {
        const string sql =
            """
                CREATE TABLE IF NOT EXISTS kafdeck_mutation_conflict_guards (
                    legacy_resource_key TEXT PRIMARY KEY
                );
        
                CREATE TABLE IF NOT EXISTS kafdeck_mutation_conflict_guard_schema (
                    component TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                );
        
                DROP TRIGGER IF EXISTS kafdeck_claim_fleet_conflict_guard;
                DROP TRIGGER IF EXISTS kafdeck_obligation_claim_conflict_guard_insert;
                DROP TRIGGER IF EXISTS kafdeck_obligation_claim_conflict_guard_update;
        
                CREATE TRIGGER kafdeck_claim_fleet_conflict_guard
                BEFORE INSERT ON kafdeck_mutation_resource_claims
                BEGIN
                    INSERT OR IGNORE INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
                    VALUES (
                        CASE
                            WHEN instr(NEW.resource_key, '/topic/') > 0
                             AND instr(NEW.resource_key, '/partition/') >
                                 instr(NEW.resource_key, '/topic/') + length('/topic/')
                             AND substr(
                                    NEW.resource_key,
                                    instr(NEW.resource_key, '/partition/') + length('/partition/')) <> ''
                             AND substr(
                                    NEW.resource_key,
                                    instr(NEW.resource_key, '/partition/') + length('/partition/'))
                                 NOT GLOB '*[^0-9]*'
                            THEN substr(
                                    NEW.resource_key,
                                    1,
                                    instr(NEW.resource_key, '/partition/') - 1)
                            ELSE NEW.resource_key
                        END
                    );
        
                    SELECT RAISE(IGNORE)
                    WHERE EXISTS (
                        SELECT 1
                        FROM kafdeck_fleet_conflict_obligations AS obligation
                        WHERE obligation.blocks_conflicting_dispatch = 1
                          AND json_extract(obligation.snapshot_json, '$.legacyResourceKey') =
                              CASE
                                  WHEN instr(NEW.resource_key, '/topic/') > 0
                                   AND instr(NEW.resource_key, '/partition/') >
                                       instr(NEW.resource_key, '/topic/') + length('/topic/')
                                   AND substr(
                                          NEW.resource_key,
                                          instr(NEW.resource_key, '/partition/') + length('/partition/')) <> ''
                                   AND substr(
                                          NEW.resource_key,
                                          instr(NEW.resource_key, '/partition/') + length('/partition/'))
                                       NOT GLOB '*[^0-9]*'
                                  THEN substr(
                                          NEW.resource_key,
                                          1,
                                          instr(NEW.resource_key, '/partition/') - 1)
                                  ELSE NEW.resource_key
                              END
                          AND obligation.operation_id <> NEW.operation_id
                    );
                END;
        
                CREATE TRIGGER kafdeck_obligation_claim_conflict_guard_insert
                BEFORE INSERT ON kafdeck_fleet_conflict_obligations
                WHEN NEW.blocks_conflicting_dispatch = 1
                 AND json_extract(NEW.snapshot_json, '$.legacyResourceKey') IS NOT NULL
                BEGIN
                    INSERT OR IGNORE INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
                    VALUES (json_extract(NEW.snapshot_json, '$.legacyResourceKey'));
        
                    SELECT RAISE(IGNORE)
                    WHERE EXISTS (
                        SELECT 1
                        FROM kafdeck_mutation_resource_claims AS claim
                        WHERE (
                            CASE
                                WHEN instr(claim.resource_key, '/topic/') > 0
                                 AND instr(claim.resource_key, '/partition/') >
                                     instr(claim.resource_key, '/topic/') + length('/topic/')
                                 AND substr(
                                        claim.resource_key,
                                        instr(claim.resource_key, '/partition/') + length('/partition/')) <> ''
                                 AND substr(
                                        claim.resource_key,
                                        instr(claim.resource_key, '/partition/') + length('/partition/'))
                                     NOT GLOB '*[^0-9]*'
                                THEN substr(
                                        claim.resource_key,
                                        1,
                                        instr(claim.resource_key, '/partition/') - 1)
                                ELSE claim.resource_key
                            END
                        ) = json_extract(NEW.snapshot_json, '$.legacyResourceKey')
                          AND claim.operation_id <> NEW.operation_id
                    );
                END;
        
                CREATE TRIGGER kafdeck_obligation_claim_conflict_guard_update
                BEFORE UPDATE OF blocks_conflicting_dispatch, snapshot_json
                ON kafdeck_fleet_conflict_obligations
                WHEN NEW.blocks_conflicting_dispatch = 1
                 AND json_extract(NEW.snapshot_json, '$.legacyResourceKey') IS NOT NULL
                BEGIN
                    INSERT OR IGNORE INTO kafdeck_mutation_conflict_guards (legacy_resource_key)
                    VALUES (json_extract(NEW.snapshot_json, '$.legacyResourceKey'));
        
                    SELECT RAISE(IGNORE)
                    WHERE EXISTS (
                        SELECT 1
                        FROM kafdeck_mutation_resource_claims AS claim
                        WHERE (
                            CASE
                                WHEN instr(claim.resource_key, '/topic/') > 0
                                 AND instr(claim.resource_key, '/partition/') >
                                     instr(claim.resource_key, '/topic/') + length('/topic/')
                                 AND substr(
                                        claim.resource_key,
                                        instr(claim.resource_key, '/partition/') + length('/partition/')) <> ''
                                 AND substr(
                                        claim.resource_key,
                                        instr(claim.resource_key, '/partition/') + length('/partition/'))
                                     NOT GLOB '*[^0-9]*'
                                THEN substr(
                                        claim.resource_key,
                                        1,
                                        instr(claim.resource_key, '/partition/') - 1)
                                ELSE claim.resource_key
                            END
                        ) = json_extract(NEW.snapshot_json, '$.legacyResourceKey')
                          AND claim.operation_id <> NEW.operation_id
                    );
                END;
        
                INSERT INTO kafdeck_mutation_conflict_guard_schema (component, schema_version)
                VALUES ('legacy-topic-guard-scope', 2)
                ON CONFLICT (component) DO UPDATE
                SET schema_version = excluded.schema_version;
        
            """;

        await using var connection = await factory.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
    }

    private static async Task<int> ReadSqliteConflictGuardSchemaVersionAsync(
        IMutationDbConnectionFactory factory)
    {
        await using var connection = await factory.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_mutation_conflict_guard_schema
            WHERE component = 'legacy-topic-guard-scope'
            """;
        return Convert.ToInt32(
            await command.ExecuteScalarAsync(),
            System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<(string ResourceKeyHash, string ResourceKey)>
        ReadPersistedResourceClaimAsync(
            IMutationDbConnectionFactory factory,
            Guid operationId,
            long generation)
    {
        await using var connection = await factory.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT resource_key_hash, resource_key
            FROM kafdeck_mutation_resource_claims
            WHERE operation_id = @operation_id
              AND execution_generation = @execution_generation
            """;
        AddTestParameter(command, "@operation_id", operationId.ToString("D"));
        AddTestParameter(command, "@execution_generation", generation);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        var result = (reader.GetString(0), reader.GetString(1));
        Assert.False(await reader.ReadAsync());
        return result;
    }

    private static async Task SeedLegacyObligationBeforeFenceAsync(
        IMutationDbConnectionFactory factory,
        FleetConflictObligationSnapshot snapshot)
    {
        var node = JsonSerializer.SerializeToNode(
                snapshot,
                new JsonSerializerOptions(JsonSerializerDefaults.Web))?.AsObject()
            ?? throw new InvalidOperationException(
                "Expected fleet conflict obligation JSON object.");
        Assert.True(node.Remove("legacyResourceKey"));

        await using var connection = await factory.OpenAsync();
        await using (var create = connection.CreateCommand())
        {
            create.CommandText =
                """
                CREATE TABLE IF NOT EXISTS kafdeck_fleet_conflict_obligations (
                    obligation_id TEXT PRIMARY KEY,
                    operation_id TEXT NOT NULL,
                    step_id TEXT NOT NULL,
                    conflict_key_hash TEXT NOT NULL,
                    conflict_key TEXT NOT NULL,
                    effect_fingerprint TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    state INTEGER NOT NULL,
                    blocks_conflicting_dispatch INTEGER NOT NULL,
                    version BIGINT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    FOREIGN KEY (operation_id)
                        REFERENCES kafdeck_mutation_operations(operation_id)
                        ON DELETE RESTRICT,
                    UNIQUE (operation_id, step_id, conflict_key_hash, conflict_key)
                )
                """;
            await create.ExecuteNonQueryAsync();
        }

        await using var insert = connection.CreateCommand();
        insert.CommandText =
            """
            INSERT INTO kafdeck_fleet_conflict_obligations (
                obligation_id,
                operation_id,
                step_id,
                conflict_key_hash,
                conflict_key,
                effect_fingerprint,
                schema_version,
                state,
                blocks_conflicting_dispatch,
                version,
                snapshot_json,
                created_at_utc,
                updated_at_utc)
            VALUES (
                @obligation_id,
                @operation_id,
                @step_id,
                @conflict_key_hash,
                @conflict_key,
                @effect_fingerprint,
                @schema_version,
                @state,
                @blocks_conflicting_dispatch,
                @version,
                @snapshot_json,
                @created_at_utc,
                @updated_at_utc)
            """;
        AddTestParameter(
            insert,
            "@obligation_id",
            snapshot.ObligationId.ToString("D"));
        AddTestParameter(
            insert,
            "@operation_id",
            snapshot.OperationId.ToString("D"));
        AddTestParameter(insert, "@step_id", snapshot.StepId);
        AddTestParameter(
            insert,
            "@conflict_key_hash",
            Convert.ToHexString(
                    SHA256.HashData(
                        Encoding.UTF8.GetBytes(snapshot.ConflictKey)))
                .ToLowerInvariant());
        AddTestParameter(insert, "@conflict_key", snapshot.ConflictKey);
        AddTestParameter(
            insert,
            "@effect_fingerprint",
            snapshot.EffectFingerprint);
        AddTestParameter(insert, "@schema_version", snapshot.SchemaVersion);
        AddTestParameter(insert, "@state", (int)snapshot.State);
        AddTestParameter(
            insert,
            "@blocks_conflicting_dispatch",
            snapshot.BlocksConflictingDispatch ? 1 : 0);
        AddTestParameter(insert, "@version", snapshot.Version);
        AddTestParameter(insert, "@snapshot_json", node.ToJsonString());
        AddTestParameter(
            insert,
            "@created_at_utc",
            snapshot.CreatedAtUtc.ToUniversalTime().ToString("O"));
        AddTestParameter(
            insert,
            "@updated_at_utc",
            snapshot.UpdatedAtUtc.ToUniversalTime().ToString("O"));
        Assert.Equal(1, await insert.ExecuteNonQueryAsync());
    }

    private static async Task<string?> ReadPersistedLegacyResourceKeyAsync(
        IMutationDbConnectionFactory factory,
        Guid obligationId)
    {
        await using var connection = await factory.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT snapshot_json
            FROM kafdeck_fleet_conflict_obligations
            WHERE obligation_id = @obligation_id
            """;
        AddTestParameter(command, "@obligation_id", obligationId.ToString("D"));
        var json = Convert.ToString(await command.ExecuteScalarAsync());
        Assert.False(string.IsNullOrWhiteSpace(json));

        using var document = JsonDocument.Parse(json!);
        Assert.True(document.RootElement.TryGetProperty(
            "legacyResourceKey",
            out var property));
        return property.ValueKind == JsonValueKind.Null
            ? null
            : property.GetString();
    }

    private static void AddTestParameter(
        System.Data.Common.DbCommand command,
        string name,
        object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    private static (MutationOperation Operation, long Generation, DateTimeOffset ClaimExpiresAtUtc)
        CreateExecutingLegacyTopicOperation(
            string topic,
            string clusterId = "prod")
    {
        var now = DateTimeOffset.UtcNow;
        var requester = "oidc:https://idp.example|legacy-guard";
        var operation = MutationOperation.CreatePreview(
            requester,
            new MutationIntentDescriptor(
                MutationOperationKind.TopicCreate,
                clusterId,
                $"{{\"operation\":\"guard\",\"topic\":\"{topic}\"}}",
                new[] { $"cluster/{clusterId}/topic/{topic}" },
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

    private static MutationOperation CreateParentOperation(
        string clusterId = "prod")
    {
        var suffix = Guid.NewGuid().ToString("N");
        return MutationOperation.CreatePreview(
            "oidc:https://idp.example|fleet-persistence",
            new MutationIntentDescriptor(
                MutationOperationKind.TopicCreate,
                clusterId,
                $"{{\"operation\":\"fleet-parent\",\"id\":\"{suffix}\"}}",
                new[] { $"cluster/{clusterId}/topic/fleet-{suffix}" },
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


    private sealed class LegacyFleetStateWriterProbe
    {
        private readonly IMutationDbConnectionFactory _factory;

        public LegacyFleetStateWriterProbe(IMutationDbConnectionFactory factory)
        {
            _factory = factory;
        }

        public async Task TryInsertWithLegacySqlAsync(
            FleetConflictObligationSnapshot snapshot)
        {
            var node = JsonSerializer.SerializeToNode(
                    snapshot,
                    new JsonSerializerOptions(JsonSerializerDefaults.Web))?.AsObject()
                ?? throw new InvalidOperationException(
                    "Expected fleet conflict obligation JSON object.");
            node.Remove("legacyResourceKey");

            await using var connection = await _factory.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                INSERT INTO kafdeck_fleet_conflict_obligations (
                    obligation_id,
                    operation_id,
                    step_id,
                    conflict_key_hash,
                    conflict_key,
                    effect_fingerprint,
                    schema_version,
                    state,
                    blocks_conflicting_dispatch,
                    version,
                    snapshot_json,
                    created_at_utc,
                    updated_at_utc)
                VALUES (
                    @obligation_id,
                    @operation_id,
                    @step_id,
                    @conflict_key_hash,
                    @conflict_key,
                    @effect_fingerprint,
                    @schema_version,
                    @state,
                    @blocks_conflicting_dispatch,
                    @version,
                    @snapshot_json,
                    @created_at_utc,
                    @updated_at_utc)
                """;
            AddTestParameter(
                command,
                "@obligation_id",
                snapshot.ObligationId.ToString("D"));
            AddTestParameter(
                command,
                "@operation_id",
                snapshot.OperationId.ToString("D"));
            AddTestParameter(command, "@step_id", snapshot.StepId);
            AddTestParameter(
                command,
                "@conflict_key_hash",
                Convert.ToHexString(
                        SHA256.HashData(
                            Encoding.UTF8.GetBytes(snapshot.ConflictKey)))
                    .ToLowerInvariant());
            AddTestParameter(command, "@conflict_key", snapshot.ConflictKey);
            AddTestParameter(
                command,
                "@effect_fingerprint",
                snapshot.EffectFingerprint);
            AddTestParameter(command, "@schema_version", snapshot.SchemaVersion);
            AddTestParameter(command, "@state", (int)snapshot.State);
            AddTestParameter(
                command,
                "@blocks_conflicting_dispatch",
                snapshot.BlocksConflictingDispatch ? 1 : 0);
            AddTestParameter(command, "@version", snapshot.Version);
            AddTestParameter(command, "@snapshot_json", node.ToJsonString());
            AddTestParameter(
                command,
                "@created_at_utc",
                snapshot.CreatedAtUtc.ToUniversalTime().ToString("O"));
            AddTestParameter(
                command,
                "@updated_at_utc",
                snapshot.UpdatedAtUtc.ToUniversalTime().ToString("O"));

            await command.ExecuteNonQueryAsync();
        }

        public async Task TryUpdateWithLegacySqlAsync(Guid obligationId)
        {
            await using var connection = await _factory.OpenAsync();
            await using var read = connection.CreateCommand();
            read.CommandText =
                """
                SELECT snapshot_json
                FROM kafdeck_fleet_conflict_obligations
                WHERE obligation_id = @obligation_id
                """;
            AddTestParameter(
                read,
                "@obligation_id",
                obligationId.ToString("D"));
            var current = Convert.ToString(await read.ExecuteScalarAsync());
            Assert.False(string.IsNullOrWhiteSpace(current));

            var node = JsonNode.Parse(current!)?.AsObject()
                ?? throw new InvalidOperationException(
                    "Expected fleet conflict obligation JSON object.");
            node.Remove("legacyResourceKey");

            await using var update = connection.CreateCommand();
            update.CommandText =
                """
                UPDATE kafdeck_fleet_conflict_obligations
                SET snapshot_json = @snapshot_json,
                    updated_at_utc = @updated_at_utc
                WHERE obligation_id = @obligation_id
                """;
            AddTestParameter(update, "@snapshot_json", node.ToJsonString());
            AddTestParameter(
                update,
                "@updated_at_utc",
                DateTimeOffset.UtcNow.ToString("O"));
            AddTestParameter(
                update,
                "@obligation_id",
                obligationId.ToString("D"));
            await update.ExecuteNonQueryAsync();
        }
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
