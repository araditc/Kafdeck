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
                "cluster/prod/topic/orphan",
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

        var conflictKey = $"cluster/prod/topic/fleet-{Guid.NewGuid():N}";
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
