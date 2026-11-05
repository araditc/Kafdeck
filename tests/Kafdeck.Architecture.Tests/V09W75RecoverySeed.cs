using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

internal sealed record W75RecoverySeed(AdoGovernancePolicyStore Store,GovernancePolicyVersion Version,GovernancePolicyState Active,MutationOperationSnapshot Pending);

internal static class V09W75RecoverySeed
{
    // Fixture seeding through owning ports; this is not product approval/effect evidence.
    internal static async Task<W75RecoverySeed> SeedAsync(IMutationDbConnectionFactory factory,IGovernanceGenerationTransactionGate? generation,
        IPolicyMetadataTransactionReader? metadata,string clusterId,string topicName,CancellationToken token=default)
    {
        var store=new AdoGovernancePolicyStore(factory,metadata,generation);
        await store.InitializeAsync(token);
        var policy=GovernancePolicyCompiler.Compile(JsonSerializer.Serialize(new
        {
            schemaVersion=1,rules=new[]{new{id="deny-delete",priority=1,scope=new{clusterId,resourceKind="topic",resourceIds=new[]{topicName},action="TopicDelete"},conditions=Array.Empty<object>(),outcome="Deny"}}
        }));
        var version=await store.StageAsync(clusterId,policy,"oidc:https://recovery.invalid|owner",token);
        var initial=await store.GetStateAsync(clusterId,token);
        IReadOnlyList<MutationPrecondition> binding=initial.Digest is null?[]:[new("v09.governance:"+clusterId,initial.Digest)];
        var commit=Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,clusterId,version.VersionId,binding);
        Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await store.CommitVersionAsync(commit,version.VersionId,version.ContentHash,()=>true,token)).ResultKind);
        var activation=Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,clusterId,"active",binding);
        Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await store.ActivateAsync(activation,version.VersionId,version.ContentHash,0,true,()=>true,token)).ResultKind);
        var active=await store.GetStateAsync(clusterId,token);
        var pending=Operation(MutationOperationKind.TopicDelete,AuthorizationAction.TopicDelete,clusterId,topicName,[new("v09.governance:"+clusterId,active.Digest!)]);
        return new(store,version,active,pending);
    }

    internal static MutationOperationSnapshot Operation(MutationOperationKind kind,AuthorizationAction action,string cluster,string resource,IReadOnlyList<MutationPrecondition> preconditions)
    {
        var critical=kind==MutationOperationKind.GovernancePolicyActivation;
        var risk=new MutationRiskDecision(critical?MutationRiskClass.Critical:MutationRiskClass.High,["test_fixture"],critical?MutationConfirmationMode.TypedTarget:MutationConfirmationMode.Explicit,critical);
        var intent=new MutationIntentDescriptor(kind,cluster,JsonSerializer.Serialize(new{resource}),[$"test:{cluster}:{resource}"],preconditions,AuthorizationTargets:[new(action,cluster,resource)]);
        return MutationOperation.CreatePreview(Guid.NewGuid(),"oidc:https://recovery.invalid|owner",intent,risk,"v0.8-bearer:"+new string('b',64),DateTimeOffset.UtcNow.AddMinutes(5),DateTimeOffset.UtcNow,Guid.NewGuid().ToString("N")).Snapshot;
    }
}
