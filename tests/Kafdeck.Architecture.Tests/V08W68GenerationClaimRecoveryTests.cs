using Kafdeck.Api;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68GenerationClaimRecoveryTests
{
    [Fact]
    public async Task Explicitly_fenced_new_generation_can_claim_same_resource_without_releasing_or_replaying_old_unknown()
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-w68-generation-claims-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            var connections = new SqliteNotificationDeliveryDbConnectionFactory(path);
            var repository = new AdoMutationOperationRepository(new SqliteMutationDbConnectionFactory(path));
            await repository.InitializeAsync();
            await new AdoNotificationRoutingStore(connections).InitializeAsync();
            await W68RuntimeSchema.InstallOrUpgradeAsync(connections, default(CancellationToken));
            var e1 = Guid.NewGuid(); var e2 = Guid.NewGuid();
            var template = V08W68GovernedApplyBoundaryTests.Plan(); var sourceGeneration = Guid.NewGuid();
            await W68RuntimeSchema.AdmitGenerationAsync(connections, "deployment", null, e1, new string('a', 64));
            var oldStore = new W68RuntimeStore(connections, e1); await oldStore.InitializeAsync();
            foreach (var registration in new[]
            {
                new W68RuntimeRegistration(W68RuntimeRegistrationKind.Owner, "deployment", "owner", Guid.NewGuid(), 0, "owner", "", "", "", "", true),
                new W68RuntimeRegistration(W68RuntimeRegistrationKind.Source, "deployment", "source", sourceGeneration, 0, "owner", "", "source", "subscription", template.DesiredFingerprint, true),
                new W68RuntimeRegistration(W68RuntimeRegistrationKind.Controller, "deployment", "controller", Guid.NewGuid(), 0, "owner", template.RequesterPrincipalId, "", "", "", true),
            }) Assert.NotNull(await oldStore.UpsertRegistrationAsync(registration, 0, _ => Task.FromResult(true)));
            var firstPlan = await Plan(oldStore, e1);
            var first = Executing(firstPlan);
            Assert.Equal(MutationCreateOutcome.Created, (await repository.CreateAsync(first.Snapshot)).Outcome);
            Assert.Equal(MutationResourceClaimOutcome.Acquired, (await repository.TryAcquireResourceClaimsAsync(firstPlan.PlanId,
                first.Snapshot.ExecutionClaimGeneration, first.Snapshot.ResourceKeys, first.Snapshot.ExecutionClaimExpiresAtUtc!.Value)).Outcome);
            var executingVersion = first.Snapshot.Version; var now = DateTimeOffset.UtcNow;
            first.MarkDispatchStarted(now);
            Assert.Equal(MutationSaveOutcome.Saved, (await repository.TrySaveAsync(first.Snapshot, executingVersion)).Outcome);
            var dispatchedVersion = first.Snapshot.Version;
            first.Complete(MutationExecutionResultKind.ExecutionUnknown, "declarative_commit_unknown", now);
            Assert.Equal(MutationSaveOutcome.Saved, (await repository.TrySaveAsync(first.Snapshot, dispatchedVersion)).Outcome);
            Assert.Null(await oldStore.GetCommitReceiptAsync(firstPlan.PlanId));
            Assert.Throws<MutationStateException>(() => first.ClaimExecution(now, now.AddMinutes(1)));
            Assert.Throws<MutationStateException>(() => first.Cancel(now));
            Assert.Throws<MutationStateException>(() => first.Complete(MutationExecutionResultKind.AppliedVerified, "unsupported-inference", now));

            // Another intent in E1 still collides: neither a new operation nor a
            // source revision may release the unresolved physical scope.
            var competitorPlan = Seal(firstPlan with { PlanId = Guid.NewGuid(), SourceRevisionHash = new string('b',64) });
            var competitor = Executing(competitorPlan);
            Assert.Equal(first.Snapshot.ResourceKeys, competitor.Snapshot.ResourceKeys);
            Assert.Equal(MutationCreateOutcome.Created, (await repository.CreateAsync(competitor.Snapshot)).Outcome);
            Assert.Equal(MutationResourceClaimOutcome.Conflict, (await repository.TryAcquireResourceClaimsAsync(competitor.Snapshot.OperationId,
                competitor.Snapshot.ExecutionClaimGeneration, competitor.Snapshot.ResourceKeys, competitor.Snapshot.ExecutionClaimExpiresAtUtc!.Value)).Outcome);

            // This is the explicit offline operator fence/admission seam; the
            // runtime cannot invent absence proof or perform this transition.
            await W68RuntimeSchema.AdmitGenerationAsync(connections, "deployment", e1, e2, new string('c',64));
            var newStore = new W68RuntimeStore(connections, e2); await newStore.InitializeAsync();
            Assert.Null(await oldStore.ApplySubscriptionAsync(firstPlan.PlanId, firstPlan.PlanFingerprint,
                firstPlan.RequesterPrincipalId, (_, _) => Task.FromResult(true)));
            Assert.Null(await newStore.GetVerifiedCommitReceiptAsync(firstPlan.PlanId, firstPlan.PlanFingerprint,
                firstPlan.RequesterPrincipalId, (_, _) => Task.FromResult(true)));
            var nextPlan = await Plan(newStore, e2); var next = Executing(nextPlan);
            Assert.Equal(firstPlan.Key, nextPlan.Key);
            Assert.Equal(first.Snapshot.AuthorizationTargets, next.Snapshot.AuthorizationTargets);
            Assert.NotEqual(Assert.Single(first.Snapshot.ResourceKeys), Assert.Single(next.Snapshot.ResourceKeys));
            Assert.Contains(e1.ToString("D"), first.Snapshot.ResourceKeys[0], StringComparison.Ordinal);
            Assert.Contains(e2.ToString("D"), next.Snapshot.ResourceKeys[0], StringComparison.Ordinal);
            Assert.True(next.Snapshot.Risk.RequiresIndependentApproval);
            Assert.NotEqual(next.Snapshot.RequesterPrincipalId, next.Snapshot.ApprovedByPrincipalId);
            Assert.Equal(MutationCreateOutcome.Created, (await repository.CreateAsync(next.Snapshot)).Outcome);
            Assert.Equal(MutationResourceClaimOutcome.Acquired, (await repository.TryAcquireResourceClaimsAsync(nextPlan.PlanId,
                next.Snapshot.ExecutionClaimGeneration, next.Snapshot.ResourceKeys, next.Snapshot.ExecutionClaimExpiresAtUtc!.Value)).Outcome);
            Assert.Equal(MutationResourceClaimOutcome.InvalidExecutionClaim, (await repository.TryAcquireResourceClaimsAsync(firstPlan.PlanId,
                first.Snapshot.ExecutionClaimGeneration, first.Snapshot.ResourceKeys, DateTimeOffset.UtcNow.AddSeconds(30))).Outcome);
            var oldDurable = await repository.GetAsync(firstPlan.PlanId);
            Assert.Equal(MutationOperationState.ExecutionUnknown, oldDurable!.State);
            Assert.Null(await newStore.GetCommitReceiptAsync(firstPlan.PlanId));
            await using var connection = await connections.OpenAsync();
            await using var query = connection.CreateCommand();
            query.CommandText = "SELECT COUNT(*) FROM kafdeck_mutation_resource_claims WHERE operation_id=@old OR operation_id=@new";
            var oldId = query.CreateParameter(); oldId.ParameterName="@old"; oldId.Value=firstPlan.PlanId.ToString("D"); query.Parameters.Add(oldId);
            var newId = query.CreateParameter(); newId.ParameterName="@new"; newId.Value=nextPlan.PlanId.ToString("D"); query.Parameters.Add(newId);
            Assert.Equal(2L, Convert.ToInt64(await query.ExecuteScalarAsync()));

            async Task<W68StoredRuntimePlan> Plan(W68RuntimeStore store, Guid generation)
            {
                var id = Guid.NewGuid(); var scope = W68RuntimeFingerprints.LeaseScope("subscription");
                var lease = await store.AcquireLeaseAsync("deployment", scope, "controller", "owner", template.RequesterPrincipalId,
                    id.ToString("N"), 120, _ => Task.FromResult(true)); Assert.NotNull(lease);
                var plan = Seal(template with { PlanId=id, DeploymentGeneration=generation, SourceGeneration=sourceGeneration,
                    SourceRevisionHash=template.DesiredFingerprint, SourceRegistrationRevision=1, ExpectedOwnershipRevision=0,
                    ExpectedOwnershipGeneration=null, ExpectedSubscriptionRevision=null, ObservedSubscriptionFingerprint="missing",
                    ObservedDestinationId=null, ObservedSubscription=null, LeaseScopeId=scope, LeaseHolderId=id.ToString("N"),
                    LeaseEpoch=lease!.Epoch, ExpiresAtUtc=(await store.ReadDatabaseUtcNowAsync()).AddSeconds(100) });
                Assert.NotNull(await store.SavePlanAsync(plan, (_, _) => Task.FromResult(true))); return plan;
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach(var suffix in new[]{"","-wal","-shm"}) if(File.Exists(path+suffix)) File.Delete(path+suffix);
        }
    }
    private static W68StoredRuntimePlan Seal(W68StoredRuntimePlan plan) => plan with { PlanFingerprint=W68RuntimeFingerprints.Plan(plan) };
    private static MutationOperation Executing(W68StoredRuntimePlan plan)
    {
        var now=DateTimeOffset.UtcNow;
        var operation=MutationOperation.CreatePreview(plan.PlanId,plan.RequesterPrincipalId,DeclarativeSubscriptionMutation.Intent(plan),
            MutationRiskClassifier.Classify(new(MutationOperationKind.DeclarativeNotificationSubscription)),"v0.8-bearer:"+plan.PolicyFingerprint,
            now.AddMinutes(2),now,Guid.NewGuid().ToString("N"));
        operation.OpenForConfirmation(now); operation.Confirm(plan.RequesterPrincipalId,operation.Snapshot.PreviewHash,now,operation.Snapshot.ConfirmationChallenge);
        operation.Approve(new(operation.Snapshot.OperationId,"oidc:https://idp.example|distinct-approver",operation.Snapshot.PreviewHash,new string('b',64)),operation.Snapshot.PreviewHash,now);
        operation.ClaimExecution(now,now.AddMinutes(2)); return operation;
    }
}
