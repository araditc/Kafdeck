using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W75PolicyTests
{
    private const string Rule="""{"schemaVersion":1,"rules":[{"id":"delete","priority":1,"scope":{"clusterId":"alpha","resourceKind":"topic","resourceIds":["orders"],"action":"TopicDelete"},"conditions":[],"outcome":"Deny"}]}""";

    [Theory]
    [InlineData("unknown")]
    [InlineData("duplicate")]
    [InlineData("schema")]
    [InlineData("allow")]
    [InlineData("operator")]
    [InlineData("field")]
    [InlineData("action")]
    [InlineData("kind")]
    [InlineData("empty-selector")]
    [InlineData("duplicate-rule")]
    [InlineData("numeric-action")]
    [InlineData("wrong-type")]
    public void Closed_policy_rejects_ambiguous_or_executable_material(string mode)
    {
        var text=mode switch
        {
            "unknown"=>Rule.Replace("\"priority\":1","\"priority\":1,\"script\":\"return true\""),
            "duplicate"=>Rule.Replace("\"priority\":1","\"priority\":1,\"priority\":2"),
            "schema"=>Rule.Replace("\"schemaVersion\":1","\"schemaVersion\":2"),
            "allow"=>Rule.Replace("\"Deny\"","\"Allow\""),
            "operator"=>Rule.Replace("\"conditions\":[]","\"conditions\":[{\"field\":\"risk\",\"operator\":\"eval\",\"value\":\"high\"}]"),
            "field"=>Rule.Replace("\"conditions\":[]","\"conditions\":[{\"field\":\"shell\",\"operator\":\"equals\",\"value\":\"x\"}]"),
            "action"=>Rule.Replace("TopicDelete","ArbitraryProviderExecute"),
            "kind"=>Rule.Replace("\"topic\"","\"proxy\""),
            "empty-selector"=>Rule.Replace("[\"orders\"]","[]"),
            "numeric-action"=>Rule.Replace("TopicDelete","17"),
            "wrong-type"=>Rule.Replace("\"priority\":1","\"priority\":\"shell\""),
            _=>Rule.Replace("\"rules\":[","\"rules\":["+JsonDocument.Parse(Rule).RootElement.GetProperty("rules")[0].GetRawText()+",")
        };
        Assert.ThrowsAny<ArgumentException>(()=>GovernancePolicyCompiler.Compile(text));
    }

    [Fact]
    public void Canonical_hash_is_stable_and_missing_facts_cannot_be_caller_approved()
    {
        var policy=GovernancePolicyCompiler.Compile(Rule);
        Assert.Equal(policy.ContentHash,GovernancePolicyCompiler.Compile(" \n"+Rule.Replace("\"priority\":1","\"priority\": 1")+"\n").ContentHash);
        var fact=new GovernanceFact("alpha","topic","orders","TopicDelete","high");
        Assert.False(GovernancePolicyCompiler.Evaluate(policy,fact,null).Allowed);
        var request=GovernancePolicyCompiler.Compile(Rule.Replace("\"Deny\"","\"RequireApprovedRequest\""));
        Assert.Equal("RequestApprovalRequired",GovernancePolicyCompiler.Evaluate(request,fact,new Dictionary<string,string>{{"requestApproved","true"}}).Code);
        var classification=GovernancePolicyCompiler.Compile(Rule.Replace("\"conditions\":[]","\"conditions\":[{\"field\":\"classification\",\"operator\":\"equals\",\"value\":\"sensitive\"}]"));
        Assert.Equal("GovernanceEvidenceUnavailable",GovernancePolicyCompiler.Evaluate(classification,fact,null).Code);
        Assert.True(GovernancePolicyCompiler.Evaluate(classification,fact,new Dictionary<string,string>{{"classification","public"}}).Allowed);
        Assert.False(GovernancePolicyCompiler.Evaluate(classification,fact,new Dictionary<string,string>{{"classification","sensitive"}}).Allowed);
        Assert.True(GovernancePolicyCompiler.Evaluate(policy,fact with{ClusterId="other"},null).Allowed);
    }

    [Fact]
    public void Policy_input_depth_rule_selector_and_byte_limits_are_enforced()
    {
        Assert.ThrowsAny<Exception>(()=>GovernancePolicyCompiler.Compile(new string(' ',GovernancePolicyCompiler.MaximumBytes)+Rule));
        var rule=JsonDocument.Parse(Rule).RootElement.GetProperty("rules")[0].GetRawText();
        var many="{\"schemaVersion\":1,\"rules\":["+string.Join(',',Enumerable.Range(0,101).Select(i=>rule.Replace("\"delete\"","\"r"+i+"\"")))+"]}";
        Assert.Throws<ArgumentException>(()=>GovernancePolicyCompiler.Compile(many));
        var selectors=Rule.Replace("[\"orders\"]","["+string.Join(',',Enumerable.Range(0,21).Select(i=>"\"r"+i+"\""))+"]");
        Assert.Throws<ArgumentException>(()=>GovernancePolicyCompiler.Compile(selectors));
        var nested="{\"schemaVersion\":1,\"rules\":"+new string('[',13)+new string(']',13)+"}";
        Assert.ThrowsAny<JsonException>(()=>GovernancePolicyCompiler.Compile(nested));
    }

    [Fact]
    public async Task SQLite_restart_activation_disable_and_concurrent_CAS_preserve_authority()
    {
        var path=Path.Combine(Path.GetTempPath(),"kafdeck-w75-"+Guid.NewGuid().ToString("N")+".db");
        try{await StoreScenarios(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }

    [G2PostgreSqlFact]
    public async Task PostgreSQL_two_nodes_restart_activation_disable_and_concurrent_CAS_preserve_authority()
    {
        await using var db=await G2CliPostgreSqlDatabase.CreateAsync();
        await StoreScenarios(new PostgreSqlMutationDbConnectionFactory(db.ConnectionString));
    }

    [Fact]
    public async Task Same_SQLite_database_global_control_and_policy_activation_serialize_without_nested_read_deadlock()
    {
        var path=Path.Combine(Path.GetTempPath(),"kafdeck-w75-global-"+Guid.NewGuid().ToString("N")+".db");
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(20));var token=deadline.Token;
        try
        {
            var policy=new AdoGovernancePolicyStore(new SqliteMutationDbConnectionFactory(path));await policy.InitializeAsync(token);
            var routing=new AdoNotificationGovernanceStore(new SqliteNotificationDeliveryDbConnectionFactory(path),policy);await routing.InitializeAsync(token);
            var empty=await policy.StageAsync("$deployment",GovernancePolicyCompiler.Compile("{\"schemaVersion\":1,\"rules\":[]}"),"owner",token);
            var create=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"$deployment",empty.VersionId,[]);
            await policy.CommitVersionAsync(create,empty.VersionId,empty.ContentHash,()=>true,token);
            var activate=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"$deployment","active",[]);
            await policy.ActivateAsync(activate,empty.VersionId,empty.ContentHash,0,true,()=>true,token);
            var first=await policy.GetStateAsync("$deployment",token);
            var doc=Rule.Replace("\"alpha\"","\"$deployment\"").Replace("\"topic\"","\"notification\"").Replace("\"orders\"","\"ops\"").Replace("\"TopicDelete\"","\"NotificationManage\"");
            var deny=await policy.StageAsync("$deployment",GovernancePolicyCompiler.Compile(doc),"owner",token);
            var createDeny=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"$deployment",deny.VersionId,[new("v09.governance:$deployment",first.Digest!)]);
            await policy.CommitVersionAsync(createDeny,deny.VersionId,deny.ContentHash,()=>true,token);
            var activation=activate with{OperationId=Guid.NewGuid(),Preconditions=[new("v09.governance:$deployment",first.Digest!)]};
            var inside=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var release=new ManualResetEventSlim();var callbacks=0;
            bool Authority(){if(Interlocked.Increment(ref callbacks)==1){inside.SetResult();Assert.True(release.Wait(TimeSpan.FromSeconds(10),token));}return true;}
            var change=new NotificationGovernanceChange("ops","candidate",new string('a',64),0,true,true,"owner",new string('b',64));
            var metadata=Task.Run(()=>routing.ChangeAsync(change,Authority,token),token);
            await inside.Task.WaitAsync(TimeSpan.FromSeconds(5),token);
            var policyChange=Task.Run(()=>policy.ActivateAsync(activation,deny.VersionId,deny.ContentHash,1,true,()=>true,token),token);
            await Task.Delay(50,token);release.Set();
            Assert.NotNull(await metadata);Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await policyChange).ResultKind);
            Assert.Equal("GovernanceDenied",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>routing.ChangeAsync(new("ops","candidate",new string('a',64),1,false,false,"owner",new string('b',64)),()=>true,token))).Code);
            Assert.Equal(1,(await routing.GetAsync("ops",token))!.Revision);
        }
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }

    private static async Task StoreScenarios(IMutationDbConnectionFactory factory)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(30));var token=deadline.Token;
        var store=new AdoGovernancePolicyStore(factory);await store.InitializeAsync(token);
        var plain=V09W75RecoverySeed.Operation(MutationOperationKind.TopicDelete,AuthorizationAction.TopicDelete,"alpha","orders",[]);
        Assert.Null((await store.GetStateAsync("alpha",token)).Digest);
        await store.ValidateOperationAsync(plain,token);
        var empty=GovernancePolicyCompiler.Compile("{\"schemaVersion\":1,\"rules\":[]}");
        var version=await store.StageAsync("alpha",empty,plain.RequesterPrincipalId,token);
        Assert.Empty(await store.ListVersionsAsync("alpha",null,50,token));
        Assert.Null(await store.GetVersionAsync("other",version.VersionId,true,token));
        var commit=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"alpha",version.VersionId,[]);
        Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await store.CommitVersionAsync(commit,version.VersionId,version.ContentHash,()=>true,token)).ResultKind);
        var activate=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"alpha","active",[]);
        Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await store.ActivateAsync(activate,version.VersionId,version.ContentHash,0,true,()=>true,token)).ResultKind);
        var first=await store.GetStateAsync("alpha",token);
        Assert.True((await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.ValidateOperationAsync(plain,token))).Stale);
        MutationOperationSnapshot Bind(MutationOperationSnapshot op,GovernancePolicyState state)=>op with{Preconditions=[new("v09.governance:alpha",state.Digest!)]};
        await store.ValidateOperationAsync(Bind(plain,first),token);
        var disable=Bind(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"alpha","active",[]),first);
        Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await store.ActivateAsync(disable,version.VersionId,version.ContentHash,1,false,()=>true,token)).ResultKind);
        var second=await store.GetStateAsync("alpha",token);Assert.False(second.Enabled);Assert.NotEqual(first.Digest,second.Digest);
        Assert.True((await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.ValidateOperationAsync(Bind(plain,first),token))).Stale);
        var restarted=new AdoGovernancePolicyStore(factory);await restarted.InitializeAsync(token);
        Assert.Equal(second,await restarted.GetStateAsync("alpha",token));await restarted.ValidateOperationAsync(Bind(plain,second),token);
        var deny=await restarted.StageAsync("alpha",GovernancePolicyCompiler.Compile(Rule),plain.RequesterPrincipalId,token);
        var denyCommit=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"alpha",deny.VersionId,[]);
        await restarted.CommitVersionAsync(Bind(denyCommit,second),deny.VersionId,deny.ContentHash,()=>true,token);
        var activateDeny=Bind(activate with{OperationId=Guid.NewGuid()},second);
        Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await restarted.ActivateAsync(activateDeny,deny.VersionId,deny.ContentHash,2,true,()=>true,token)).ResultKind);
        var third=await restarted.GetStateAsync("alpha",token);
        Assert.Equal("GovernanceDenied",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>restarted.ValidateOperationAsync(Bind(plain,third),token))).Code);
        var candidates=Enumerable.Range(0,2).Select(_=>Bind(activate with{OperationId=Guid.NewGuid()},third)).ToArray();
        async Task<bool> Race(MutationOperationSnapshot op,AdoGovernancePolicyStore node)
        {
            try{return (await node.ActivateAsync(op,deny.VersionId,deny.ContentHash,3,false,()=>true,token)).ResultKind==MutationExecutionResultKind.AppliedVerified;}
            catch(GovernanceEffectRejectedException){return false;}
            catch(System.Data.Common.DbException){return false;}
        }
        var winners=await Task.WhenAll(Race(candidates[0],store),Race(candidates[1],restarted));Assert.Single(winners,x=>x);
        var final=await store.GetStateAsync("alpha",token);Assert.Equal(4,final.Revision);Assert.False(final.Enabled);
    }
}
