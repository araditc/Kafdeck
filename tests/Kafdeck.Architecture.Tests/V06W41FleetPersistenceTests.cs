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
