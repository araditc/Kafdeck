using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W75PolicyPaginationTests
{
    [Fact]
    public async Task SQLite_authorization_precedes_page_and_hidden_rows_never_create_continuation()
    {
        var path=Path.Combine(Path.GetTempPath(),"w75-page-"+Guid.NewGuid().ToString("N")+".db");
        try{await Scenario(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }
    [G2PostgreSqlFact]
    public async Task PostgreSQL_authorization_precedes_page_and_hidden_rows_never_create_continuation()
    {
        await using var db=await G2CliPostgreSqlDatabase.CreateAsync();await Scenario(new PostgreSqlMutationDbConnectionFactory(db.ConnectionString));
    }
    private static async Task Scenario(IMutationDbConnectionFactory factory)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(30));var token=deadline.Token;
        var store=new AdoGovernancePolicyStore(factory);await store.InitializeAsync(token);
        for(var i=0;i<7;i++)
        {
            var document=JsonSerializer.Serialize(new{schemaVersion=1,rules=new[]{new{id="r"+i,priority=i,
                scope=new{clusterId="alpha",resourceKind="topic",resourceIds=new[]{"orders"+i},action="TopicDelete"},conditions=Array.Empty<object>(),outcome="Deny"}}});
            var version=await store.StageAsync("alpha",GovernancePolicyCompiler.Compile(document),"owner",token);
            var source=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"alpha",version.VersionId,[]);
            Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await store.CommitVersionAsync(source,version.VersionId,version.ContentHash,()=>true,token)).ResultKind);
        }
        var all=await store.ListVersionsAsync("alpha",null,50,token);Assert.Equal(7,all.Count);
        // The actual store order deliberately contains hidden rows before, between and after visible rows.
        var visible=new[]{all[1].VersionId,all[3].VersionId,all[5].VersionId};var allowed=visible.ToHashSet(StringComparer.Ordinal);
        var first=await store.ListAuthorizedVersionsAsync("alpha",null,2,allowed.Contains,token);
        Assert.Equal(visible,first.Select(x=>x.VersionId));
        var firstPage=first.Take(2).ToArray();var continuation=first.Count>2?firstPage[^1].VersionId:null;
        Assert.Equal(visible[1],continuation);
        var second=await store.ListAuthorizedVersionsAsync("alpha",continuation,2,allowed.Contains,token);
        Assert.Equal(visible[2],Assert.Single(second).VersionId);
        Assert.False(second.Count>2); // A hidden row after the final visible record must not produce another cursor.
        Assert.Empty(await store.ListAuthorizedVersionsAsync("alpha",visible[2],2,allowed.Contains,token));
        Assert.Empty(await store.ListAuthorizedVersionsAsync("alpha",null,2,_=>false,token));
        var authorizationCalls=0;
        Assert.Empty(await store.ListAuthorizedVersionsAsync("hidden",null,2,_=>{authorizationCalls++;return true;},token));
        Assert.Equal(0,authorizationCalls);
    }
}
