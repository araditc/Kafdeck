using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W75GlobalNotificationScopeTests
{
    [Fact]
    public void Policy_projection_preserves_raw_authorization_and_adds_only_equivalent_global_controls()
    {
        MutationAuthorizationTarget[] raw=[new(AuthorizationAction.NotificationManage,"g3-deployment","subscription"),new(AuthorizationAction.DeclarativeApply,"g3-deployment","subscription")];
        var copy=raw.ToArray();var projected=MutationGovernanceTargets.Project(raw);
        Assert.Equal(copy,raw);Assert.Equal(3,projected.Count);Assert.All(raw,t=>Assert.Contains(t,projected));
        Assert.Contains(new(AuthorizationAction.NotificationManage,"$deployment","subscription"),projected);
        Assert.DoesNotContain(new(AuthorizationAction.DeclarativeApply,"$deployment","subscription"),projected);
        Assert.Equal(projected,MutationGovernanceTargets.Project(projected));
        Assert.Null(DeclarativeMutationAuthorization.Request(raw[0]).ClusterId);
    }
    [Fact]
    public async Task SQLite_global_and_existing_deployment_policy_are_both_required_for_declarative_effects()
    {
        var path=Path.Combine(Path.GetTempPath(),"w75-global-scope-"+Guid.NewGuid().ToString("N")+".db");
        try{await Scenario(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }
    [G2PostgreSqlFact]
    public async Task PostgreSQL_global_and_existing_deployment_policy_are_both_required_for_declarative_effects()
    {
        await using var db=await G2CliPostgreSqlDatabase.CreateAsync();await Scenario(new PostgreSqlMutationDbConnectionFactory(db.ConnectionString));
    }
    private static async Task Scenario(IMutationDbConnectionFactory factory)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(30));var token=deadline.Token;
        var store=new AdoGovernancePolicyStore(factory);await store.InitializeAsync(token);
        MutationOperationSnapshot Operation(params MutationPrecondition[] markers)=>MutationOperation.CreatePreview("oidc:https://idp.example|requester",
            new(MutationOperationKind.DeclarativeNotificationSubscription,"g3-deployment","{}",["declarative-subscription"],markers,AuthorizationTargets:[
                new(AuthorizationAction.DeclarativeRead,"g3-deployment","subscription"),new(AuthorizationAction.DeclarativePlan,"g3-deployment","subscription"),new(AuthorizationAction.DeclarativeApply,"g3-deployment","subscription"),
                new(AuthorizationAction.NotificationRead,"g3-deployment","subscription"),new(AuthorizationAction.NotificationRead,"g3-deployment","destination"),
                new(AuthorizationAction.NotificationManage,"g3-deployment","subscription"),new(AuthorizationAction.NotificationManage,"g3-deployment","destination")]),
            new(MutationRiskClass.Critical,["fixture"],MutationConfirmationMode.TypedTarget,true),"v0.8-bearer:"+new string('a',64),DateTimeOffset.UtcNow.AddMinutes(5),DateTimeOffset.UtcNow,Guid.NewGuid().ToString("N")).Snapshot;
        async Task<GovernancePolicyVersion> Install(string scope)
        {
            var document=GovernancePolicyCompiler.Compile(JsonSerializer.Serialize(new{schemaVersion=1,rules=new[]{new{id="deny-subscription",priority=1,
                scope=new{clusterId=scope,resourceKind="notification",resourceIds=new[]{"subscription"},action="NotificationManage"},conditions=Array.Empty<object>(),outcome="Deny"}}}));
            var before=await store.GetStateAsync(scope,token);var markers=before.Digest is null?Array.Empty<MutationPrecondition>():new[]{new MutationPrecondition("v09.governance:"+scope,before.Digest)};
            var version=await store.StageAsync(scope,document,"trusted-policy-fixture",token);
            await store.CommitVersionAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,scope,version.VersionId,markers),version.VersionId,version.ContentHash,()=>true,token);
            await store.ActivateAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,scope,"active",markers),version.VersionId,version.ContentHash,before.Revision,true,()=>true,token);
            return version;
        }
        var legacy=Operation();await store.ValidateOperationAsync(legacy,token);var raw=legacy.AuthorizationTargets.ToArray();
        var globalVersion=await Install("$deployment");var global=await store.GetStateAsync("$deployment",token);
        Assert.True((await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.ValidateOperationAsync(legacy,token))).Stale);
        var fresh=Operation(new MutationPrecondition("v09.governance:$deployment",global.Digest!));
        Assert.Equal("GovernanceDenied",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.ValidateOperationAsync(fresh,token))).Code);
        Assert.Equal(raw,legacy.AuthorizationTargets);
        var disable=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"$deployment","active",[new("v09.governance:$deployment",global.Digest!)]);
        await store.ActivateAsync(disable,globalVersion.VersionId,globalVersion.ContentHash,global.Revision,false,()=>true,token);
        var disabled=await store.GetStateAsync("$deployment",token);
        Assert.True((await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.ValidateOperationAsync(fresh,token))).Stale);
        await store.ValidateOperationAsync(Operation(new MutationPrecondition("v09.governance:$deployment",disabled.Digest!)),token);
        _=await Install("g3-deployment");var local=await store.GetStateAsync("g3-deployment",token);
        Assert.Equal("GovernanceDenied",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.ValidateOperationAsync(Operation(new MutationPrecondition("v09.governance:$deployment",disabled.Digest!),new MutationPrecondition("v09.governance:g3-deployment",local.Digest!)),token))).Code);
    }
}
