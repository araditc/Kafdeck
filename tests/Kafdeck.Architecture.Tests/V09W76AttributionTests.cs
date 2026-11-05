using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Attribution;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W76AttributionTests
{
    [Fact]
    public void Explicit_weights_conserve_decimal_measured_units_and_keep_residual()
    {
        var team=Guid.NewGuid().ToString("N");var app=Guid.NewGuid().ToString("N");var w=Window("10") with{Ownership=new(null,null,null,null,"Missing")};
        var card=Card("0.00001",[new(team,null,"0.3333333333333333333333333333"),new(null,app,"0.25")]);
        var result=AttributionCalculator.Allocate(w,card,DateTimeOffset.UtcNow);
        Assert.Equal("Estimated",result.Quality);Assert.All(result.Allocations,a=>Assert.Equal("Estimated",a.Quality));
        Assert.Equal(10,result.Allocations.Sum(a=>AttributionContract.Decimal(a.Quantity))+AttributionContract.Decimal(result.ResidualQuantity!));
        Assert.Equal("0.0001",result.UnroundedAmount);Assert.NotEqual("0",result.ResidualQuantity);
    }
    [Fact]
    public void Direct_owner_is_observed_historical_ownership_is_not_rewritten_by_weights()
    {
        var team=Guid.NewGuid().ToString("N");var other=Guid.NewGuid().ToString("N");var w=Window("8") with{Ownership=new(3,team,null,new string('a',64),"Observed")};
        var result=AttributionCalculator.Allocate(w,Card("2",[new(other,null,"1")]),DateTimeOffset.UtcNow);
        Assert.Equal("Observed",result.Quality);Assert.Equal(team,Assert.Single(result.Allocations).TeamId);Assert.Equal("0",result.ResidualQuantity);Assert.Equal("16",result.UnroundedAmount);
    }
    [Fact]
    public void Unknown_reset_stale_and_missing_evidence_never_become_zero_estimates()
    {
        var now=DateTimeOffset.UtcNow;var source=new AttributionSourceIdentity("meter","permitted","KafdeckMeasuredCountersV1");
        foreach(var sample in new[]{new MeasuredCounterWindow("orders","trafficBytes","bytes",now.AddMinutes(-2),now.AddMinutes(-1),"10","1","Complete","1"),
            new("orders","trafficBytes","bytes",now.AddMinutes(-2),now.AddMinutes(-1),"0","10","Partial","1"),new("orders","trafficBytes","bytes",now.AddMinutes(-12),now.AddMinutes(-10),"0","10","Complete","1")})
        {
            var w=AttributionContract.Counter(source,sample,now,"known-profile");Assert.Null(w.Quantity);
            var result=AttributionCalculator.Allocate(w,Card("1",[]),now);Assert.Null(result.UnroundedAmount);Assert.Null(result.ResidualQuantity);
        }
        Assert.Null(AttributionCalculator.Totals([],true).UnroundedAmount);
        Assert.Null(AttributionCalculator.Allocate(Window("1"),null,now).UnroundedAmount);
    }
    [Fact]
    public void Rounds_once_at_total_and_rejects_overflow_negative_rates_or_mixed_currencies()
    {
        var first=AttributionCalculator.Allocate(Window("1"),Card("1.005",[]),DateTimeOffset.UtcNow);
        var second=first with{Measurement=first.Measurement with{WindowStartUtc=first.Measurement.WindowStartUtc.AddMinutes(1),WindowEndUtc=first.Measurement.WindowEndUtc.AddMinutes(1)}};
        var total=AttributionCalculator.Totals([first,second],true);Assert.Equal("2.01",total.RoundedAmount);Assert.Equal("2.01",total.UnroundedAmount);
        Assert.Throws<ArgumentException>(()=>AttributionContract.Rate(Card("-1",[]).Document));
        Assert.Throws<ArgumentException>(()=>AttributionCalculator.Totals([first,first with{Currency="EUR"}],true));
        Assert.Throws<OverflowException>(()=>AttributionCalculator.Allocate(Window(decimal.MaxValue.ToString(System.Globalization.CultureInfo.InvariantCulture)),Card("2",[]),DateTimeOffset.UtcNow));
        Assert.Throws<ArgumentException>(()=>AttributionContract.Rate(Card("1",[new(Guid.NewGuid().ToString("N"),null,"0.7"),new(Guid.NewGuid().ToString("N"),null,"0.7")]).Document));
    }
    [Fact]
    public async Task SQLite_rate_CAS_overlap_multiple_units_scope_paging_restart_and_restore_generation()
    {
        var path=Path.Combine(Path.GetTempPath(),"w76-"+Guid.NewGuid().ToString("N")+".db");
        try{await StoreScenario(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }
    [G2PostgreSqlFact]
    public async Task PostgreSQL_rate_CAS_overlap_multiple_units_scope_paging_restart_and_restore_generation()
    {await using var db=await G2CliPostgreSqlDatabase.CreateAsync();await StoreScenario(new PostgreSqlMutationDbConnectionFactory(db.ConnectionString));}

    private static async Task StoreScenario(IMutationDbConnectionFactory factory)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(60));var token=deadline.Token;var gen=Guid.NewGuid();
        var gate=new AdoGovernanceGenerationGate(factory,gen);await gate.InitializeAsync(token);
        var store=new AdoAttributionStore(factory,gate);await store.InitializeAsync(token);await store.InitializeAsync(token);
        var now=DateTimeOffset.UtcNow;var seen=Window("98304") with{WindowStartUtc=now.AddMinutes(-2),WindowEndUtc=now.AddMinutes(-1),CollectedAtUtc=now};
        var hidden=seen with{WindowId=new string('0',64),ResourceId="hidden"};var last=seen with{WindowId=new string('f',64),WindowStartUtc=now.AddMinutes(-1),WindowEndUtc=now};
        await store.AppendAsync([seen,hidden,last],token);await store.AppendAsync([seen with{CollectedAtUtc=now.AddMinutes(1)}],token);
        var query=new MeasurementQuery("permitted",now.AddMinutes(-3),now, null,1);
        var page=await store.QueryAuthorizedAsync(query,w=>w.ResourceId!="hidden",token);Assert.Equal(2,page.Count);Assert.DoesNotContain(page,w=>w.ResourceId=="hidden");
        var tail=await store.QueryAuthorizedAsync(query with{AfterId=page[1].WindowId},w=>w.ResourceId!="hidden",token);Assert.Empty(tail);
        var first=await store.StageRateAsync("permitted",Card("0.00001",[]).Document,"trusted-owner",token);
        var committed=Operation(MutationOperationKind.RateCardVersion,first);await store.CommitRateAsync(committed,first.VersionId,first.ContentHash,0,false,()=>true,token);
        var activation=Operation(MutationOperationKind.RateCardActivation,first,0,true);await store.CommitRateAsync(activation,first.VersionId,first.ContentHash,0,true,()=>true,token);
        Assert.Equal(first.VersionId,(await store.FindActiveRateAsync("permitted","trafficBytes",seen.WindowStartUtc,seen.WindowEndUtc,token))!.VersionId);
        Assert.Equal(1,(await store.GetStateAsync("permitted",token)).Revision);
        var overlap=await store.StageRateAsync("permitted",first.Document with{Amount="2"},"trusted-owner",token);
        await store.CommitRateAsync(Operation(MutationOperationKind.RateCardVersion,overlap),overlap.VersionId,overlap.ContentHash,0,false,()=>true,token);
        await Assert.ThrowsAsync<AttributionConflictException>(()=>store.CommitRateAsync(Operation(MutationOperationKind.RateCardActivation,overlap,1,true),overlap.VersionId,overlap.ContentHash,1,true,()=>true,token));
        var storage=await store.StageRateAsync("permitted",first.Document with{MetricKind="storageByteSeconds",Unit="byteSeconds"},"trusted-owner",token);
        await store.CommitRateAsync(Operation(MutationOperationKind.RateCardVersion,storage),storage.VersionId,storage.ContentHash,0,false,()=>true,token);
        await store.CommitRateAsync(Operation(MutationOperationKind.RateCardActivation,storage,1,true),storage.VersionId,storage.ContentHash,1,true,()=>true,token);
        Assert.NotNull(await store.FindActiveRateAsync("permitted","trafficBytes",seen.WindowStartUtc,seen.WindowEndUtc,token));
        Assert.NotNull(await store.FindActiveRateAsync("permitted","storageByteSeconds",seen.WindowStartUtc,seen.WindowEndUtc,token));
        var foreign=await store.StageRateAsync("permitted",first.Document with{Currency="EUR"},"trusted-owner",token);
        await store.CommitRateAsync(Operation(MutationOperationKind.RateCardVersion,foreign),foreign.VersionId,foreign.ContentHash,0,false,()=>true,token);
        await store.CommitRateAsync(Operation(MutationOperationKind.RateCardActivation,foreign,2,true),foreign.VersionId,foreign.ContentHash,2,true,()=>true,token);
        await Assert.ThrowsAsync<AttributionConflictException>(()=>store.FindActiveRateAsync("permitted","trafficBytes",seen.WindowStartUtc,seen.WindowEndUtc,token));
        Assert.Equal(first.VersionId,(await store.FindActiveRateAuthorizedAsync("permitted","trafficBytes",seen.WindowStartUtc,seen.WindowEndUtc,v=>v.VersionId==first.VersionId,token))!.VersionId);
        var receipt=await store.ReadReceiptAsync(activation.OperationId,token);Assert.True(AdoAttributionStore.Matches(receipt,activation));
        var versionReceipt=await store.ReadReceiptAsync(committed.OperationId,token);Assert.NotNull(versionReceipt);
        await store.VerifyPositiveReceiptAsync(committed,versionReceipt,()=>true,token);await store.VerifyPositiveReceiptAsync(committed,versionReceipt,()=>true,token);
        await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.VerifyPositiveReceiptAsync(committed,versionReceipt,()=>false,token));
        await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.CommitRateAsync(Operation(MutationOperationKind.RateCardActivation,storage,3,false),storage.VersionId,storage.ContentHash,3,false,()=>false,token));
        var restarted=new AdoAttributionStore(factory,new AdoGovernanceGenerationGate(factory,gen));await restarted.InitializeAsync(token);Assert.Equal(3,(await restarted.GetStateAsync("permitted",token)).Revision);
        var wrong=new AdoAttributionStore(factory,new AdoGovernanceGenerationGate(factory,Guid.NewGuid()));await Assert.ThrowsAsync<GovernanceGenerationException>(()=>wrong.GetStateAsync("permitted",token));
        await Assert.ThrowsAsync<GovernanceGenerationException>(()=>wrong.VerifyPositiveReceiptAsync(committed,versionReceipt,()=>true,token));
        Assert.Equal(0,await restarted.DeleteExpiredMeasurementsAsync(90,now,token));Assert.Empty(await restarted.QueryAuthorizedAsync(query,w=>false,token));
        Assert.Equal(1,(await restarted.GetRateAsync("permitted",first.VersionId,false,token))!.Committed?1:0);
    }
    internal static MutationOperationSnapshot Operation(MutationOperationKind kind,RateCardVersion card,long expected=0,bool enabled=false)
    {
        var material=new RateCardMutationMaterial(card.VersionId,card.ContentHash,expected,enabled);var resource=kind==MutationOperationKind.RateCardActivation?"active":card.VersionId;
        return MutationOperation.CreatePreview(Guid.NewGuid(),"oidc:https://fixture.invalid|owner",new(kind,card.ClusterId,JsonSerializer.Serialize(material,AttributionContract.Json),["rate-card:"+resource],AuthorizationTargets:[new(AuthorizationAction.RateCardManage,card.ClusterId,resource)]),
            new(MutationRiskClass.High,["trusted-store-fixture"],MutationConfirmationMode.TypedTarget,false),"v0.8-bearer:"+new string('a',64),DateTimeOffset.UtcNow.AddMinutes(5),DateTimeOffset.UtcNow,Guid.NewGuid().ToString("N")).Snapshot;
    }
    internal static MeasurementWindow Window(string quantity)
    {
        var now=DateTimeOffset.UtcNow;return new(new string('a',64),"meter","permitted","KafdeckMeasuredCountersV1","trafficBytes","bytes","orders",now.AddMinutes(-2),now.AddMinutes(-1),now,quantity,"Observed",[],new(1,Guid.NewGuid().ToString("N"),null,new string('b',64),"Observed"),new string('c',64));
    }
    internal static RateCardVersion Card(string amount,IReadOnlyList<AttributionWeight> weights)
    {var now=DateTimeOffset.UtcNow;var doc=new RateCardDocument("trafficBytes","bytes","USD",amount,now.AddDays(-1),now.AddDays(1),weights);return new(new string('d',64),"permitted",AttributionContract.Hash(JsonSerializer.Serialize(doc,AttributionContract.Json)),doc,"fixture",now,true);}
}
