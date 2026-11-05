using Kafdeck.Core.Attribution;
using Kafdeck.Core.Catalog;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W76RateCardReferenceTests
{
    [Fact]
    public async Task SQLite_archive_after_preview_blocks_new_rate_commits_atomically_and_preserves_prior_allocations()
    {
        var path=Path.Combine(Path.GetTempPath(),"w76-rate-reference-"+Guid.NewGuid().ToString("N")+".db");
        try{await Scenario(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }
    [G2PostgreSqlFact]
    public async Task PostgreSQL_archive_after_preview_blocks_new_rate_commits_atomically_and_preserves_prior_allocations()
    {await using var database=await G2CliPostgreSqlDatabase.CreateAsync();await Scenario(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString));}
    private static async Task Scenario(IMutationDbConnectionFactory factory)
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(60));var token=timeout.Token;
        var catalog=new AdoCatalogStore(factory,"trusted-reference-fixture");await catalog.InitializeAsync(token);
        var rates=new AdoAttributionStore(factory,catalogReferences:catalog);await rates.InitializeAsync(token);
        async Task<CatalogRecord> Change(CatalogEntityKind kind,string? id,long expected,bool archived=false,string? teamId=null)
        {
            var change=CatalogContract.Validate(new(kind,id,expected,"Fixture owner",Archived:archived,TeamId:teamId));var entity=id??Guid.NewGuid().ToString("N");
            var changes=new[]{new CatalogPlannedChange(change,entity,null)};var now=DateTimeOffset.UtcNow;
            var plan=new CatalogPlan(Guid.NewGuid(),"permitted","trusted-catalog-fixture",new string('b',64),catalog.DeploymentGeneration,CatalogContract.MaterialHash("permitted",changes),changes,now,now.AddMinutes(5));
            await catalog.StageAsync(plan,token);Assert.NotNull(await catalog.CommitAsync(plan,()=>true,token));return(await catalog.GetAsync("permitted",kind,entity,token))!;
        }
        async Task<RateCardVersion> Draft(IReadOnlyList<AttributionWeight> weights)=>await rates.StageRateAsync("permitted",V09W76AttributionTests.Card("0.25",weights).Document,"trusted-rate-fixture",token);
        async Task CommitVersion(RateCardVersion version)=>Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await rates.CommitRateAsync(V09W76AttributionTests.Operation(MutationOperationKind.RateCardVersion,version),version.VersionId,version.ContentHash,0,false,()=>true,token)).ResultKind);
        var team=await Change(CatalogEntityKind.Team,null,0);var staged=await Draft([new(team.Id,null,"1")]);
        var versionOp=V09W76AttributionTests.Operation(MutationOperationKind.RateCardVersion,staged);
        await Change(CatalogEntityKind.Team,team.Id,team.Revision,true);
        Assert.Equal("RateCardReferenceChanged",(await Assert.ThrowsAsync<AttributionConflictException>(()=>rates.CommitRateAsync(versionOp,staged.VersionId,staged.ContentHash,0,false,()=>true,token))).Code);
        Assert.False((await rates.GetRateAsync("permitted",staged.VersionId,true,token))!.Committed);Assert.Null(await rates.ReadReceiptAsync(versionOp.OperationId,token));Assert.Equal(0,(await rates.GetStateAsync("permitted",token)).Revision);

        var team2=await Change(CatalogEntityKind.Team,null,0);var app=await Change(CatalogEntityKind.Application,null,0,teamId:team2.Id);var appRate=await Draft([new(null,app.Id,"1")]);await CommitVersion(appRate);
        var activation=V09W76AttributionTests.Operation(MutationOperationKind.RateCardActivation,appRate,0,true);
        await Change(CatalogEntityKind.Application,app.Id,app.Revision,true,team2.Id);
        Assert.Equal("RateCardReferenceChanged",(await Assert.ThrowsAsync<AttributionConflictException>(()=>rates.CommitRateAsync(activation,appRate.VersionId,appRate.ContentHash,0,true,()=>true,token))).Code);
        Assert.Null(await rates.ReadReceiptAsync(activation.OperationId,token));Assert.Equal(0,(await rates.GetStateAsync("permitted",token)).Revision);

        var team3=await Change(CatalogEntityKind.Team,null,0);var historical=await Draft([new(team3.Id,null,"1")]);await CommitVersion(historical);
        var first=V09W76AttributionTests.Operation(MutationOperationKind.RateCardActivation,historical,0,true);await rates.CommitRateAsync(first,historical.VersionId,historical.ContentHash,0,true,()=>true,token);
        var window=V09W76AttributionTests.Window("4") with{Ownership=new(null,null,null,null,"Missing")};var before=AttributionCalculator.Allocate(window,historical,DateTimeOffset.UtcNow);
        await Change(CatalogEntityKind.Team,team3.Id,team3.Revision,true);
        var retained=await rates.FindActiveRateAsync("permitted","trafficBytes",window.WindowStartUtc,window.WindowEndUtc,token);Assert.NotNull(retained);
        var after=AttributionCalculator.Allocate(window,retained,DateTimeOffset.UtcNow);Assert.Equal(before.RateCardHash,after.RateCardHash);Assert.Equal(team3.Id,Assert.Single(after.Allocations).TeamId);Assert.Equal(before.UnroundedAmount,after.UnroundedAmount);
        var repeat=V09W76AttributionTests.Operation(MutationOperationKind.RateCardActivation,historical,1,true);
        await Assert.ThrowsAsync<AttributionConflictException>(()=>rates.CommitRateAsync(repeat,historical.VersionId,historical.ContentHash,1,true,()=>true,token));
        Assert.Equal(1,(await rates.GetStateAsync("permitted",token)).Revision);Assert.Null(await rates.ReadReceiptAsync(repeat.OperationId,token));
        var noAuthority=new AdoAttributionStore(factory);var unverified=await noAuthority.StageRateAsync("permitted",historical.Document with{Amount="0.5"},"trusted-rate-fixture",token);var denied=V09W76AttributionTests.Operation(MutationOperationKind.RateCardVersion,unverified);
        Assert.Equal("RateCardReferenceAuthorityUnavailable",(await Assert.ThrowsAsync<AttributionConflictException>(()=>noAuthority.CommitRateAsync(denied,unverified.VersionId,unverified.ContentHash,0,false,()=>true,token))).Code);
        Assert.Null(await noAuthority.ReadReceiptAsync(denied.OperationId,token));
    }
}
