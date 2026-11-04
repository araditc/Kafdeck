using System.Data.Common;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G5CompensationSourceOutcomeTests
{
    [Fact]
    public void Stored_source_body_must_match_its_receipt_bound_fingerprint_before_inverse_eligibility()
    {
        var source=V08W68GovernedApplyBoundaryTests.Plan();
        var before=new NotificationSubscriptionSnapshot(source.DesiredSubscription,NotificationSubscriptionState.Paused,1,DateTimeOffset.UtcNow);
        source=source with {ObservedSubscription=before,ObservedDestinationId=before.Definition.DestinationId,ObservedSubscriptionFingerprint=W68RuntimeFingerprints.Subscription(before)};
        source=source with {PlanFingerprint=W68RuntimeFingerprints.Plan(source)};
        Assert.True(W71CompensationProof.IsToggle(source));
        // Both changes preserve the apparent toggle, but alter sealed source
        // evidence. The original hash/receipt must never authorize either body.
        Assert.False(W71CompensationProof.IsToggle(source with {ObservedSubscription=new NotificationSubscriptionSnapshot(before.Definition,before.State,before.Revision,before.UpdatedAtUtc.AddSeconds(1))}));
        Assert.False(W71CompensationProof.IsToggle(source with {ExpiresAtUtc=source.ExpiresAtUtc.AddMinutes(1)}));
        Assert.Equal(source.PlanFingerprint,W68RuntimeFingerprints.Plan(source));
    }
    [Theory]
    [InlineData(MutationOperationState.Previewed)]
    [InlineData(MutationOperationState.AwaitingApproval)]
    [InlineData(MutationOperationState.Ready)]
    [InlineData(MutationOperationState.ExecutionUnknown)]
    [InlineData(MutationOperationState.AppliedVerified)]
    public async Task Positive_storage_receipt_is_not_permission_to_compensate_an_unresolved_original(MutationOperationState outcome)
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(60));var token=timeout.Token;
        await using var db=await W68G3PersistenceFixture.CreateAsync(false,token);
        var repository=new AdoMutationOperationRepository(new MutationConnections(db.Connections));
        await repository.InitializeAsync(token);
        var (create,_)=await db.PlanAsync(W68RuntimePlanCommand.Apply,token:token);
        Assert.NotNull(await db.Store.ApplySubscriptionAsync(create.PlanId,create.PlanFingerprint,create.RequesterPrincipalId,W68G3PersistenceFixture.Yes,token));
        var before=await db.Routing.GetSubscriptionAsync(create.Key.ResourceId,token);Assert.NotNull(before);
        var manifest=DeclarativeSubscriptionManifest.Parse(JsonSerializer.SerializeToUtf8Bytes(new
        {
            apiVersion="kafdeck.io/v0.8",kind="NotificationSubscription",
            metadata=new {deploymentRef=create.Key.DeploymentId,name=create.Key.ResourceId,ownerRef=create.OwnerId},
            spec=new {destinationRef=before.Definition.DestinationId,state="Active",eventClasses=before.Definition.EventClasses.Select(x=>x.ToString()).ToArray(),eventTypes=before.Definition.EventTypes}
        }),"json");
        var sourceRegistration=await db.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,create.Key.DeploymentId,create.SourceId,token);
        Assert.NotNull(await db.Store.UpsertRegistrationAsync(sourceRegistration! with {PinnedRevision=manifest.Fingerprint()},sourceRegistration!.Revision,W68G3PersistenceFixture.Yes,token));
        var (template,_)=await db.PlanAsync(W68RuntimePlanCommand.Apply,sourceRevision:manifest.Fingerprint(),save:false,token:token);
        var source=template with {ObservedSubscription=before,DesiredSubscription=manifest.Definition,DesiredState=NotificationSubscriptionState.Active,
            DesiredCanonicalJson=Encoding.UTF8.GetString(manifest.ToCanonicalUtf8()),DesiredFingerprint=manifest.Fingerprint()};
        source=source with {PlanFingerprint=W68RuntimeFingerprints.Plan(source)};
        Assert.NotNull(await db.Store.SavePlanAsync(source,W68G3PersistenceFixture.Yes,token));
        var receipt=await db.Store.ApplySubscriptionAsync(source.PlanId,source.PlanFingerprint,source.RequesterPrincipalId,W68G3PersistenceFixture.Yes,token);
        Assert.NotNull(receipt);
        Assert.True(W71CompensationProof.CurrentMatches(source,receipt,await db.Store.GetOwnershipAsync(source.Key,token),await db.Routing.GetSubscriptionAsync(source.Key.ResourceId,token)));

        // Trusted repository fault/restore fixture, deliberately independent of
        // the positive product commit. This is not an OIDC or approval proof.
        var now=DateTimeOffset.UtcNow;
        var operation=MutationOperation.CreatePreview(source.PlanId,source.RequesterPrincipalId,DeclarativeSubscriptionMutation.Intent(source),
            MutationRiskClassifier.Classify(new(MutationOperationKind.DeclarativeNotificationSubscription)),"fixture-policy",now.AddMinutes(5),now,Guid.NewGuid().ToString("N"));
        if(outcome!=MutationOperationState.Previewed)
        {
            operation.OpenForConfirmation(now);
            operation.Confirm(source.RequesterPrincipalId,operation.Snapshot.PreviewHash,now,operation.Snapshot.ConfirmationChallenge);
            if(outcome!=MutationOperationState.AwaitingApproval)
            {
                operation.Approve(new(source.PlanId,"oidc:https://idp.example|distinct-approver",operation.Snapshot.PreviewHash,new string('b',64)),operation.Snapshot.PreviewHash,now);
                if(outcome is MutationOperationState.ExecutionUnknown or MutationOperationState.AppliedVerified)
                {
                    operation.ClaimExecution(now,now.AddMinutes(1));operation.MarkDispatchStarted(now);
                    operation.Complete(outcome==MutationOperationState.ExecutionUnknown?MutationExecutionResultKind.ExecutionUnknown:MutationExecutionResultKind.AppliedVerified,"fixture_commit_outcome",now);
                }
            }
        }
        Assert.Equal(outcome,operation.Snapshot.State);
        Assert.Equal(MutationCreateOutcome.Created,(await repository.CreateAsync(operation.Snapshot,token)).Outcome);
        var durable=await repository.GetAsync(source.PlanId,token);Assert.NotNull(durable);
        Assert.Equal(outcome==MutationOperationState.AppliedVerified,await DeclarativeCompensationAuthority.IsVerifiedAsync(source.PlanId,repository,db.Store,token));
        // Cache only immutable source evidence before opening the write
        // transaction. Live operation state is re-read on every callback from
        // the actual repository sharing this same SQLite database. Reading the
        // source plan on a second connection after INSERT would table-lock.
        var cachedSource=await db.Store.GetPlanAsync(source.PlanId,token);Assert.NotNull(cachedSource);
        var inverseManifest=DeclarativeSubscriptionManifest.Parse(JsonSerializer.SerializeToUtf8Bytes(new
        {
            apiVersion="kafdeck.io/v0.8",kind="NotificationSubscription",
            metadata=new{deploymentRef=source.Key.DeploymentId,name=source.Key.ResourceId,ownerRef=source.OwnerId},
            spec=new{destinationRef=before.Definition.DestinationId,state="Paused",eventClasses=before.Definition.EventClasses.Select(x=>x.ToString()).ToArray(),eventTypes=before.Definition.EventTypes}
        }),"json");
        var pin=await db.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,source.Key.DeploymentId,source.SourceId,token);
        Assert.NotNull(await db.Store.UpsertRegistrationAsync(pin! with {PinnedRevision=inverseManifest.Fingerprint()},pin!.Revision,W68G3PersistenceFixture.Yes,token));
        var (inverseTemplate,_)=await db.PlanAsync(W68RuntimePlanCommand.Apply,sourceRevision:inverseManifest.Fingerprint(),save:false,token:token);
        var inverse=inverseTemplate with {CompensatesPlanId=source.PlanId,ObservedSubscription=await db.Routing.GetSubscriptionAsync(source.Key.ResourceId,token),
            DesiredSubscription=inverseManifest.Definition,DesiredState=inverseManifest.State,DesiredCanonicalJson=Encoding.UTF8.GetString(inverseManifest.ToCanonicalUtf8()),DesiredFingerprint=inverseManifest.Fingerprint()};
        inverse=inverse with {PlanFingerprint=W68RuntimeFingerprints.Plan(inverse)};
        var saved=await db.Store.SavePlanAsync(inverse,(_,ct)=>DeclarativeCompensationAuthority.IsVerifiedPlanAsync(cachedSource,repository,ct),token);
        Assert.Equal(outcome==MutationOperationState.AppliedVerified,saved is not null);
        var after=await repository.GetAsync(source.PlanId,token);Assert.NotNull(after);
        Assert.Equal(outcome,after.State);Assert.Equal(durable.Version,after.Version);
        Assert.Equal(receipt,await db.Store.GetCommitReceiptAsync(source.PlanId,token));
        Assert.Equal(2,(await db.Routing.GetSubscriptionAsync(source.Key.ResourceId,token))!.Revision);
    }

    private sealed class MutationConnections(INotificationDeliveryDbConnectionFactory inner):IMutationDbConnectionFactory
    {
        public bool SupportsSelectForUpdate=>inner.SupportsSelectForUpdate;
        public ValueTask<DbConnection> OpenAsync(CancellationToken token=default)=>inner.OpenAsync(token);
    }
}
