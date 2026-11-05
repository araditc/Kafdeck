using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

/// <summary>Real repository transactions with explicitly trusted identity/policy fixtures;
/// genuine authentication and actual broker/client effects are separate live producers.</summary>
public sealed class V09W74RequestStoreTests
{
    [Fact] public Task SQLite_request_decisions_bind_exact_operation_current_authority_and_cancel_without_granting_execution()=>Sqlite(false);
    [G2PostgreSqlFact] public Task PostgreSQL_request_decisions_bind_exact_operation_current_authority_and_cancel_without_granting_execution()=>Postgres(false);
    [Fact] public Task SQLite_expired_and_dispatched_requests_cannot_be_rewritten_or_replayed()=>Sqlite(true);
    [G2PostgreSqlFact] public Task PostgreSQL_expired_and_dispatched_requests_cannot_be_rewritten_or_replayed()=>Postgres(true);
    private static async Task Sqlite(bool expiry)
    {
        var file=Path.Combine(Path.GetTempPath(),"w74-requests-"+Guid.NewGuid().ToString("N")+".db");
        try{await Scenario(new SqliteMutationDbConnectionFactory(file),expiry);}
        finally{SqliteConnection.ClearAllPools();foreach(var suffix in new[]{"","-wal","-shm"})File.Delete(file+suffix);}
    }
    private static async Task Postgres(bool expiry)
    {await using var database=await G2CliPostgreSqlDatabase.CreateAsync();await Scenario(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString),expiry);}
    private static async Task Scenario(IMutationDbConnectionFactory factory,bool expiry)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(90));var token=deadline.Token;
        var generation=new AdoGovernanceGenerationGate(factory,Guid.NewGuid());await generation.InitializeAsync(token);
        var operations=new AdoMutationOperationRepository(factory);await operations.InitializeAsync(token);
        var authority=new CurrentAuthority();var store=new AdoServiceRequestStore(factory,operations,authority,generation);await store.InitializeAsync(token);
        var policies=new AdoGovernancePolicyStore(factory,generationGate:generation,approvedRequests:store);await policies.InitializeAsync(token);
        var document=GovernancePolicyCompiler.Compile(JsonSerializer.Serialize(new{schemaVersion=1,rules=new[]{new{id="require-reviewed-request",priority=1,
            scope=new{clusterId="permitted",resourceKind="topic",resourceIds=new[]{"requested-topic"},action="TopicCreate"},conditions=Array.Empty<object>(),outcome="RequireApprovedRequest"}}}));
        var before=await policies.GetStateAsync("permitted",token);var markers=before.Digest is null?Array.Empty<MutationPrecondition>():new[]{new MutationPrecondition("v09.governance:permitted",before.Digest)};
        var version=await policies.StageAsync("permitted",document,"trusted-policy-fixture",token);
        await policies.CommitVersionAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"permitted",version.VersionId,markers),version.VersionId,version.ContentHash,()=>true,token);
        await policies.ActivateAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"permitted","active",markers),version.VersionId,version.ContentHash,before.Revision,true,()=>true,token);
        var state=await policies.GetStateAsync("permitted",token);var now=DateTimeOffset.UtcNow;
        var owner=SecurityAuditPrincipal.FromOperatorKey(new("https://idp.example","requester"));
        var reviewer=SecurityAuditPrincipal.FromOperatorKey(new("https://idp.example","reviewer"));
        var target=MutationOperation.CreatePreview(Guid.NewGuid(),owner,new(MutationOperationKind.TopicCreate,"permitted","{\"topic\":\"requested-topic\"}",["topic:requested-topic"],
            [new("v09.governance:permitted",state.Digest!)],AuthorizationTargets:[new(AuthorizationAction.TopicCreate,"permitted","requested-topic")]),
            new(MutationRiskClass.Moderate,["trusted-fixture"],MutationConfirmationMode.Explicit,false),"v0.8-bearer:"+new string('b',64),now.AddMinutes(5),now,Guid.NewGuid().ToString("N")).Snapshot;
        await operations.CreateAsync(target,token);var id=Guid.NewGuid().ToString("N");
        // A finite setup budget allows real PostgreSQL contention; expiry is still
        // observed on the actual database clock after a successfully approved request.
        var expires=expiry?DateTimeOffset.UtcNow.AddSeconds(20):target.PreviewExpiresAtUtc;
        async Task<ServiceRequestRecord> Change(ServiceRequestCommand command,long expected,string actor,string client="Browser")
        {
            var at=DateTimeOffset.UtcNow;var plan=new ServiceRequestPlan(Guid.NewGuid(),"permitted","",id,command,expected,actor,client,new string('a',64),store.DeploymentGeneration,
                AdoServiceRequestStore.Bind(target),"bounded fixture request",command==ServiceRequestCommand.Cancel?target.PreviewExpiresAtUtc:expires,at,at.AddMinutes(1));
            plan=plan with{ContentHash=ServiceRequestContract.PlanHash(plan)};await store.StageAsync(plan,token);
            var kind=ServiceRequestContract.IsDecision(command)?MutationOperationKind.ServiceRequestDecision:MutationOperationKind.ServiceRequestChange;
            var action=ServiceRequestContract.IsDecision(command)?AuthorizationAction.RequestDecide:AuthorizationAction.RequestSubmit;
            var operation=MutationOperation.CreatePreview(plan.PlanId,actor,new(kind,"permitted",JsonSerializer.Serialize(new{schema="request-v1",planId=plan.PlanId,contentHash=plan.ContentHash,deploymentGeneration=plan.DeploymentGeneration}),
                [ServiceRequestContract.Resource(id)],[new("request.material",plan.ContentHash)],AuthorizationTargets:[new(action,"permitted",ServiceRequestContract.Resource(id))]),
                new(MutationRiskClass.Moderate,["fixture"],MutationConfirmationMode.Explicit,false),"v0.8-bearer:"+new string('a',64),at.AddMinutes(1),at,Guid.NewGuid().ToString("N")).Snapshot;
            await operations.CreateAsync(operation,token);
            return (await store.CommitAsync(plan,operation,()=>true,policies.ValidateBindingDigestsAsync,token)).Request;
        }
        async Task Required()=>Assert.Equal("RequestApprovalRequired",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>policies.ValidateOperationAsync(target,token))).Code);
        await Required();var draft=await Change(ServiceRequestCommand.Draft,0,owner);Assert.Equal(ServiceRequestState.Draft,draft.BusinessState);await Required();
        var submitted=await Change(ServiceRequestCommand.Submit,1,owner);Assert.Equal(ServiceRequestState.Submitted,submitted.BusinessState);await Required();
        if(!expiry)
        {
            Assert.Equal("RequestEligibleDistinctHumanRequired",(await Assert.ThrowsAsync<ServiceRequestConflictException>(()=>Change(ServiceRequestCommand.Approve,2,owner))).Code);
            var sameHuman="oidc-cli-human:"+Uri.EscapeDataString("https://idp.example")+"|requester|cli|human-registration";
            Assert.Equal("RequestEligibleDistinctHumanRequired",(await Assert.ThrowsAsync<ServiceRequestConflictException>(()=>Change(ServiceRequestCommand.Approve,2,sameHuman,"CliHuman"))).Code);
            Assert.Equal("RequestEligibleDistinctHumanRequired",(await Assert.ThrowsAsync<ServiceRequestConflictException>(()=>Change(ServiceRequestCommand.Approve,2,"oidc-service:fixture","Automation"))).Code);
        }
        var approved=await Change(ServiceRequestCommand.Approve,2,reviewer);Assert.Equal(ServiceRequestState.Approved,approved.BusinessState);
        Assert.True(approved.ExpiresAtUtc>DateTimeOffset.UtcNow,"Request fixture exhausted its finite setup budget before the intended expiry phase.");
        await policies.ValidateOperationAsync(target,token);
        if(!expiry)
        {
            authority.DecisionAllowed=false;await Required();authority.DecisionAllowed=true;await policies.ValidateOperationAsync(target,token);
            authority.RequesterAllowed=false;await Required();authority.RequesterAllowed=true;
            var changed=target with{CanonicalIntentHash=new string('c',64)};
            Assert.Equal("RequestApprovalRequired",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>policies.ValidateOperationAsync(changed,token))).Code);
            Assert.Equal("RequestApprovalRequired",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>policies.ValidateOperationAsync(target with{OperationId=Guid.NewGuid()},token))).Code);
            Assert.Equal("RequestApprovalRequired",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>policies.ValidateOperationAsync(target with{PreviewHash=new string('c',64)},token))).Code);
            Assert.True((await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>policies.ValidateOperationAsync(target with{Preconditions=[new("v09.governance:permitted",new string('c',64))]},token))).Stale);
        }
        if(expiry)
        {
            var delay=expires-DateTimeOffset.UtcNow+TimeSpan.FromMilliseconds(150);if(delay>TimeSpan.Zero)await Task.Delay(delay,token);await Required();
            Assert.Equal(ServiceRequestState.Expired,(await store.GetAsync("permitted",id,token))!.BusinessState);
            // A dispatched/unknown effect is never cleared by business cancellation or a replacement revision.
            var dispatched=target with{State=MutationOperationState.ExecutionUnknown,DispatchStartedAtUtc=DateTimeOffset.UtcNow,Version=target.Version+1};
            Assert.Equal(MutationSaveOutcome.Saved,(await operations.TrySaveAsync(dispatched,target.Version,token)).Outcome);target=dispatched;
            Assert.Equal("RequestEffectAlreadyDispatched",(await Assert.ThrowsAsync<ServiceRequestConflictException>(()=>Change(ServiceRequestCommand.Cancel,3,owner))).Code);
            Assert.Equal(MutationOperationState.ExecutionUnknown,(await operations.GetAsync(target.OperationId,token))!.State);
        }
        else
        {
            var cancelled=await Change(ServiceRequestCommand.Cancel,3,owner);Assert.Equal(ServiceRequestState.Cancelled,cancelled.BusinessState);await Required();
            await Assert.ThrowsAsync<ServiceRequestConflictException>(()=>Change(ServiceRequestCommand.Approve,3,reviewer));
            var history=await store.HistoryAuthorizedAsync("permitted",id,0,200,_=>true,token);Assert.Equal(4,history.Count);
            Assert.Empty(await store.ListAuthorizedAsync("permitted",null,1,_=>false,token));
            Assert.Null(await store.GetAsync("hidden",id,token));
            var restarted=new AdoServiceRequestStore(factory,operations,authority,generation);await restarted.InitializeAsync(token);
            Assert.Equal(ServiceRequestState.Cancelled,(await restarted.GetAsync("permitted",id,token))!.BusinessState);
            var revision=await Change(ServiceRequestCommand.Revise,4,owner);Assert.Equal(2,revision.Revision);Assert.Empty(revision.Decisions);
            await Change(ServiceRequestCommand.Submit,5,owner);var rejected=await Change(ServiceRequestCommand.Reject,6,reviewer);
            Assert.Equal(ServiceRequestState.Rejected,rejected.BusinessState);await Required();
            await Change(ServiceRequestCommand.Revise,7,owner);await Change(ServiceRequestCommand.Submit,8,owner);
            async Task<bool> Race(ServiceRequestCommand command,string actor)
            {try{await Change(command,9,actor);return true;}catch(ServiceRequestConflictException){return false;}}
            var race=await Task.WhenAll(Race(ServiceRequestCommand.Approve,reviewer),Race(ServiceRequestCommand.Cancel,owner));Assert.Single(race,x=>x);
            var concurrent=await store.GetAsync("permitted",id,token);Assert.Equal(10,concurrent!.Version);
            Assert.True(concurrent.BusinessState is ServiceRequestState.Approved or ServiceRequestState.Cancelled);
            if(concurrent.BusinessState==ServiceRequestState.Cancelled)await Required();else await policies.ValidateOperationAsync(target,token);
        }
    }
    private sealed class CurrentAuthority:IServiceRequestCurrentAuthority
    {
        public bool DecisionAllowed=true;public bool RequesterAllowed=true;
        public bool IsCurrentRequesterAllowed(string principal,AuthorizationRequest request)=>!principal.Contains("requester",StringComparison.Ordinal)||RequesterAllowed;
        public bool IsCurrentHumanDecisionAllowed(string principal,AuthorizationRequest request)=>DecisionAllowed;
    }
}
