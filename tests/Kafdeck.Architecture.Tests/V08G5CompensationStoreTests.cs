using System.Text;
using System.Text.Json;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Real store atomicity/fencing proof. Actual OIDC/governance is tested in the hosted G5 suite.
public sealed class V08G5CompensationStoreTests
{
    [Theory]
    [InlineData("valid")] [InlineData("drift-before-plan")] [InlineData("drift-before-effect")]
    [InlineData("wrong-state")] [InlineData("wrong-definition")] [InlineData("missing-source")]
    [InlineData("create")] [InlineData("uncommitted")]
    public Task SQLite_inverse_is_exact_new_fenced_effect(string scenario) => Run(false,scenario);

    [W68G3PostgresTheory]
    [InlineData("valid")] [InlineData("drift-before-plan")] [InlineData("drift-before-effect")]
    [InlineData("wrong-state")] [InlineData("wrong-definition")] [InlineData("missing-source")]
    [InlineData("create")] [InlineData("uncommitted")]
    public Task PostgreSQL_inverse_is_exact_new_fenced_effect(string scenario) => Run(true,scenario);

    private static async Task Run(bool postgres,string scenario)
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(60));var token=timeout.Token;
        await using var db=await W68G3PersistenceFixture.CreateAsync(postgres,token);
        var (created,_)=await db.PlanAsync(W68RuntimePlanCommand.Apply,token:token);
        Assert.NotNull(await db.Store.ApplySubscriptionAsync(created.PlanId,created.PlanFingerprint,W68G3PersistenceFixture.Principal,W68G3PersistenceFixture.Yes,token));
        var source=await Plan(NotificationSubscriptionState.Active,null);
        Assert.True(W71CompensationProof.IsToggle(source));
        var storedSource=await db.Store.SavePlanAsync(source,W68G3PersistenceFixture.Yes,token);Assert.NotNull(storedSource);
        if(scenario!="uncommitted")
            Assert.NotNull(await db.Store.ApplySubscriptionAsync(source.PlanId,source.PlanFingerprint,W68G3PersistenceFixture.Principal,W68G3PersistenceFixture.Yes,token));
        else
            await db.Store.ReleaseLeaseAsync(new(source.Key.DeploymentId,source.LeaseScopeId,source.LeaseHolderId,source.LeaseEpoch,source.ExpiresAtUtc),source.ControllerId,source.OwnerId,source.RequesterPrincipalId,W68G3PersistenceFixture.Yes,token);
        if(scenario=="drift-before-plan") await Drift();
        var sourceId=scenario=="create"?created.PlanId:scenario=="missing-source"?Guid.NewGuid():source.PlanId;
        var inverse=await Plan(scenario=="wrong-state"?NotificationSubscriptionState.Active:NotificationSubscriptionState.Paused,sourceId);
        if(scenario=="wrong-definition")
        {
            inverse=inverse with {DesiredSubscription=new(inverse.Key.ResourceId,"foreign-destination",[NotificationEventClass.Operational],["fixture.changed"])};
            inverse=Canonical(inverse);
        }
        var saved=await db.Store.SavePlanAsync(inverse,W68G3PersistenceFixture.Yes,token);
        if(scenario is not ("valid" or "drift-before-effect")) {Assert.Null(saved);Assert.Null(await db.Store.GetCommitReceiptAsync(inverse.PlanId,token));return;}
        Assert.NotNull(saved);
        if(scenario=="drift-before-effect") await Drift();
        var result=await db.Store.ApplySubscriptionAsync(inverse.PlanId,inverse.PlanFingerprint,W68G3PersistenceFixture.Principal,W68G3PersistenceFixture.Yes,token);
        if(scenario=="drift-before-effect") {Assert.Null(result);Assert.Null(await db.Store.GetCommitReceiptAsync(inverse.PlanId,token));return;}
        Assert.NotNull(result);Assert.Equal(3,result.SubscriptionRevision);Assert.Equal(3,result.OwnershipRevision);
        var after=await db.Routing.GetSubscriptionAsync(inverse.Key.ResourceId,token);Assert.Equal(NotificationSubscriptionState.Paused,after!.State);Assert.Equal(3,after.Revision);
        Assert.Equal(result,await db.Store.ApplySubscriptionAsync(inverse.PlanId,inverse.PlanFingerprint,W68G3PersistenceFixture.Principal,W68G3PersistenceFixture.Yes,token));
        Assert.Equal(3,(await db.Routing.GetSubscriptionAsync(inverse.Key.ResourceId,token))!.Revision);
        Assert.Equal(source.PlanId,(await db.Store.LoadPlanAsync(inverse.PlanId,token))!.CompensatesPlanId);

        async Task<W68StoredRuntimePlan> Plan(NotificationSubscriptionState state,Guid? link)
        {
            var current=await db.Routing.GetSubscriptionAsync("subscription-a",token);Assert.NotNull(current);
            var provisional=new {apiVersion="kafdeck.io/v0.8",kind="NotificationSubscription",metadata=new{deploymentRef=W68G3PersistenceFixture.Deployment,name="subscription-a",ownerRef=W68G3PersistenceFixture.Owner},
                spec=new{destinationRef=current.Definition.DestinationId,state=state.ToString(),eventClasses=current.Definition.EventClasses.Select(x=>x.ToString()).ToArray(),eventTypes=current.Definition.EventTypes}};
            var desired=DeclarativeSubscriptionManifest.Parse(JsonSerializer.SerializeToUtf8Bytes(provisional),"json");
            var registration=await db.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Source,token);
            Assert.NotNull(await db.Store.UpsertRegistrationAsync(registration! with {PinnedRevision=desired.Fingerprint()},registration!.Revision,W68G3PersistenceFixture.Yes,token));
            var (plan,_)=await db.PlanAsync(W68RuntimePlanCommand.Apply,sourceRevision:desired.Fingerprint(),save:false,token:token);
            return Canonical(plan with {DesiredSubscription=desired.Definition,DesiredState=state,ObservedSubscription=current,CompensatesPlanId=link});
        }
        async Task Drift()
        {
            var current=await db.Routing.GetSubscriptionAsync("subscription-a",token);Assert.NotNull(current);
            Assert.NotNull(await db.Routing.ReplaceSubscriptionAsync(current.Definition,NotificationSubscriptionState.Paused,current.Revision,DateTimeOffset.UtcNow,token));
        }
    }
    private static W68StoredRuntimePlan Canonical(W68StoredRuntimePlan plan)
    {
        var d=plan.DesiredSubscription;
        var desired=DeclarativeSubscriptionManifest.Parse(JsonSerializer.SerializeToUtf8Bytes(new {apiVersion="kafdeck.io/v0.8",kind="NotificationSubscription",metadata=new{deploymentRef=plan.Key.DeploymentId,name=d.SubscriptionId,ownerRef=plan.OwnerId},
            spec=new{destinationRef=d.DestinationId,state=plan.DesiredState.ToString(),eventClasses=d.EventClasses.Select(x=>x.ToString()).ToArray(),eventTypes=d.EventTypes}}),"json");
        plan=plan with {DesiredCanonicalJson=Encoding.UTF8.GetString(desired.ToCanonicalUtf8()),DesiredFingerprint=desired.Fingerprint()};
        return plan with {PlanFingerprint=W68RuntimeFingerprints.Plan(plan)};
    }

    [Fact]
    public async Task Ordinary_legacy_plan_JSON_and_fingerprint_omit_null_compensation_link()
    {
        await using var db=await W68G3PersistenceFixture.CreateAsync(false);
        var (plan,_)=await db.PlanAsync(W68RuntimePlanCommand.Apply);
        var json=JsonSerializer.Serialize(plan);
        Assert.DoesNotContain("CompensatesPlanId",json,StringComparison.Ordinal);
        var restored=JsonSerializer.Deserialize<W68StoredRuntimePlan>(json);Assert.NotNull(restored);
        Assert.Null(restored.CompensatesPlanId);Assert.Equal(plan.PlanFingerprint,W68RuntimeFingerprints.Plan(restored));
        Assert.Equal(json,JsonSerializer.Serialize(await db.Store.LoadPlanAsync(plan.PlanId)));
    }
}
