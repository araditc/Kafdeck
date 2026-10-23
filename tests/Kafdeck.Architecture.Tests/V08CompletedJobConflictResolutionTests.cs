using Kafdeck.Api;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08CompletedJobConflictResolutionTests
{
    [Theory]
    [InlineData(FleetProgressPhase.Completed, 1, true)]
    [InlineData(FleetProgressPhase.Unknown, 1, false)]
    [InlineData(FleetProgressPhase.Stopped, 1, false)]
    [InlineData(FleetProgressPhase.Completed, 0, false)]
    public async Task Only_exact_durable_ack_completion_releases_destination_and_restart_discovery_is_idempotent(
        FleetProgressPhase phase, int acknowledged, bool releases)
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-completed-conflict-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            var factory = new SqliteMutationDbConnectionFactory(path);
            var operations = new AdoMutationOperationRepository(factory); await operations.InitializeAsync();
            var store = new AdoFleetMutationStateStore(factory); await store.InitializeAsync();
            var now = DateTimeOffset.UtcNow;
            var plan = DataGeneratorPolicy.CreatePlan(new("cluster", "profile-v1", "physical", "destination", 0, new string('a', 64)),
                new(DataGeneratorSourceKind.BuiltInTemplate, Template: new(DataGeneratorBuiltInTemplate.BasicJsonV1, 1,
                    DataGeneratorPolicy.BuiltInTemplateFingerprint(DataGeneratorBuiltInTemplate.BasicJsonV1, 1))), 1, 7, new());
            var parent = Activated(plan, now);
            Assert.Equal(MutationCreateOutcome.Created, (await operations.CreateAsync(parent)).Outcome);
            var progress = new FleetOperationProgressSnapshot
            {
                OperationId = parent.OperationId, WorkerGeneration = 1, Phase = phase, UpdatedAtUtc = now,
                Generator = new(plan.PlanFingerprint, acknowledged, acknowledged, acknowledged * 4, 0),
            };
            Assert.Equal(FleetProgressCreateOutcome.Created, (await store.CreateProgressAsync(progress)).Outcome);
            var key = FleetConflictKeyCodec.TopicPartition("physical", "destination", 0);
            var obligation = FleetConflictObligation.Create(parent.OperationId, "data-generator-destination", key, plan.PlanFingerprint, now).Snapshot;
            Assert.Equal(FleetConflictObligationCreateOutcome.Created, (await store.CreateConflictObligationAsync(obligation)).Outcome);
            // Simulate a restart after acknowledgement persistence but before cleanup.
            var reopened = new AdoFleetMutationStateStore(new SqliteMutationDbConnectionFactory(path));
            var discovered = await reopened.ListActiveDataGeneratorProgressAsync(10);
            if (phase == FleetProgressPhase.Completed) Assert.Contains(discovered, row => row.OperationId == parent.OperationId);
            Assert.Equal(releases ? 1 : 0, await CompletedJobConflictResolution.ResolveAsync(reopened, parent, plan, progress, now, CancellationToken.None));
            Assert.Equal(0, await CompletedJobConflictResolution.ResolveAsync(reopened, parent, plan, progress, now, CancellationToken.None));
            var saved = await reopened.GetConflictObligationAsync(obligation.ObligationId); Assert.NotNull(saved);
            Assert.Equal(!releases, saved.BlocksConflictingDispatch);
            if (releases)
            {
                Assert.Equal(FleetConflictObligationState.ObservedTerminalEffect, saved.State);
                Assert.Equal(64, saved.SafeResolutionEvidenceHash!.Length);
                Assert.DoesNotContain(await reopened.ListActiveDataGeneratorProgressAsync(10), row => row.OperationId == parent.OperationId);
            }
            var next = Activated(plan, now); Assert.Equal(MutationCreateOutcome.Created, (await operations.CreateAsync(next)).Outcome);
            var nextObligation = FleetConflictObligation.Create(next.OperationId, "data-generator-destination", key, plan.PlanFingerprint, now).Snapshot;
            var nextAdmission = await reopened.CreateConflictObligationAsync(nextObligation);
            Assert.Equal(releases, nextAdmission.Outcome == FleetConflictObligationCreateOutcome.Created);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-wal", "-shm" }) if (File.Exists(path + suffix)) File.Delete(path + suffix);
        }
    }

    private static MutationOperationSnapshot Activated(DataGeneratorPlan plan, DateTimeOffset now)
    {
        const string actor = "oidc:https://idp.example|requester";
        var operation = MutationOperation.CreatePreview(actor, DataGeneratorPolicy.BuildIntent(plan),
            MutationRiskClassifier.Classify(new(MutationOperationKind.DataGenerator)), "completion-test", now.AddMinutes(10), now, Guid.NewGuid().ToString("N"));
        operation.OpenForConfirmation(now); operation.Confirm(actor, operation.Snapshot.PreviewHash, now, operation.Snapshot.ConfirmationChallenge);
        if (operation.Snapshot.State == MutationOperationState.AwaitingApproval)
            operation.Approve(new(operation.Snapshot.OperationId, "oidc:https://idp.example|approver", operation.Snapshot.PreviewHash, new string('b', 64)), operation.Snapshot.PreviewHash, now);
        operation.ClaimExecution(now, now.AddMinutes(1)); operation.MarkDispatchStarted(now);
        operation.Complete(MutationExecutionResultKind.AppliedVerified, "data_generator_activated", now);
        return operation.Snapshot;
    }
}
