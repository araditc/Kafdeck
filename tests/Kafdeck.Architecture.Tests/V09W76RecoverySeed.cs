using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Attribution;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

internal sealed record W76RecoveryBundle(MeasurementWindow Window,RateCardVersion Version,MutationOperationSnapshot Pending,Guid ActivationId);
internal static class V09W76RecoverySeed
{
    private const string Cluster="w76-restore-scope";
    // This is trusted persistence fixture seeding for physical backup verification, not measurement acquisition or human approval proof.
    internal static async Task<W76RecoveryBundle> SeedAsync(IMutationDbConnectionFactory factory,IGovernanceGenerationTransactionGate gate,CancellationToken token)
    {
        var policy=new AdoGovernancePolicyStore(factory,generationGate:gate);await policy.InitializeAsync(token);
        var store=new AdoAttributionStore(factory,gate,policy);await store.InitializeAsync(token);
        var now=DateTimeOffset.UtcNow;
        var window=AttributionContract.Counter(new("physical-restore-fixture",Cluster,"KafdeckMeasuredCountersV1"),new("restore-orders","trafficBytes","bytes",now.AddMinutes(-2),now.AddMinutes(-1),"0","128","Complete","trusted-restore-seed",new string('a',64)),now,"trusted-fixture-not-acquisition") with{Ownership=new(7,Guid.NewGuid().ToString("N"),null,new string('a',64),"Observed")};
        await store.AppendAsync([window],token);
        var version=await store.StageRateAsync(Cluster,new("trafficBytes","bytes","USD","0.25",now.AddDays(-1),now.AddDays(1),[]),"trusted-restore-owner",token);
        async Task<MutationOperationSnapshot> Operation(MutationOperationKind kind,long expected,bool enabled)
        {
            var current=await policy.GetStateAsync(Cluster,token);return Create(kind,version,expected,enabled,current.Digest);
        }
        var commit=await Operation(MutationOperationKind.RateCardVersion,0,false);await store.CommitRateAsync(commit,version.VersionId,version.ContentHash,0,false,()=>true,token);
        var activation=await Operation(MutationOperationKind.RateCardActivation,0,true);await store.CommitRateAsync(activation,version.VersionId,version.ContentHash,0,true,()=>true,token);
        var pending=await Operation(MutationOperationKind.RateCardActivation,1,false);
        var operations=new AdoMutationOperationRepository(factory);await operations.InitializeAsync(token);Assert.Equal(MutationCreateOutcome.Created,(await operations.CreateAsync(pending,token)).Outcome);
        return new(window,version with{Committed=true},pending,activation.OperationId);
    }
    internal static async Task VerifyAsync(IMutationDbConnectionFactory factory,IGovernanceGenerationTransactionGate gate,W76RecoveryBundle bundle,bool postgres,CancellationToken token)
    {
        var policy=new AdoGovernancePolicyStore(factory,generationGate:gate);await policy.InitializeAsync(token);
        var store=new AdoAttributionStore(factory,gate,policy);await store.InitializeAsync(token);
        var row=Assert.Single(await store.QueryAuthorizedAsync(new(Cluster,bundle.Window.WindowStartUtc,bundle.Window.WindowEndUtc,bundle.Window.ResourceId,50),_=>true,token));
        Assert.Equal(JsonSerializer.Serialize(bundle.Window,AttributionContract.Json),JsonSerializer.Serialize(row,AttributionContract.Json));
        var active=await store.GetStateAsync(Cluster,token);Assert.True(active.Enabled);Assert.Equal(1,active.Revision);Assert.Equal(bundle.Version.VersionId,active.VersionId);
        var version=await store.GetRateAsync(Cluster,bundle.Version.VersionId,false,token);Assert.NotNull(version);Assert.Equal(bundle.Version.ContentHash,version.ContentHash);
        var estimate=AttributionCalculator.Allocate(row,version,DateTimeOffset.UtcNow);Assert.Equal("32",estimate.UnroundedAmount);Assert.Equal(bundle.Window.Ownership.TeamId,Assert.Single(estimate.Allocations).TeamId);
        var original=await store.ReadReceiptAsync(bundle.ActivationId,token);Assert.NotNull(original);
        var operations=new AdoMutationOperationRepository(factory);var pending=await operations.GetAsync(bundle.Pending.OperationId,token);Assert.NotNull(pending);Assert.Equal(bundle.Pending.PreviewHash,pending.PreviewHash);
        var stale=await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.CommitRateAsync(pending,version.VersionId,version.ContentHash,1,false,()=>true,token));Assert.True(stale.Stale);Assert.Equal("GovernancePolicyChanged",stale.Code);
        Assert.Equal(1,(await store.GetStateAsync(Cluster,token)).Revision);Assert.Null(await store.ReadReceiptAsync(pending.OperationId,token));
        var current=await policy.GetStateAsync(Cluster,token);var fresh=Create(MutationOperationKind.RateCardActivation,version,1,false,current.Digest);
        Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await store.CommitRateAsync(fresh,version.VersionId,version.ContentHash,1,false,()=>true,token)).ResultKind);
        Assert.Equal(2,(await store.GetStateAsync(Cluster,token)).Revision);
        var directory=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY")??Path.GetTempPath();Directory.CreateDirectory(directory);
        await File.WriteAllBytesAsync(Path.Combine(directory,(postgres?"postgres":"sqlite")+"-w76-attribution-restore-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(new{
            provider=postgres?"PostgreSQL":"SQLite",trustedFixtureNotMeasurementAcquisition=true,physicalBackupProofProvidedByParentCase=true,
            historicalWindowAndOwnershipPreserved=true,activeRateVersionHashAndEpochPreserved=true,pendingRateOperationRestored=true,positiveRateReceiptPreserved=true,
            oldPlanRefusedWithoutEffect=true,freshExplicitGenerationBoundPlanCommitted=true,unroundedRestoredEstimate="32",sourceWindowId=row.WindowId,rateContentHash=version.ContentHash}),token);
    }
    private static MutationOperationSnapshot Create(MutationOperationKind kind,RateCardVersion v,long expected,bool enabled,string? policyDigest)
    {
        var material=new RateCardMutationMaterial(v.VersionId,v.ContentHash,expected,enabled);var target=kind==MutationOperationKind.RateCardVersion?v.VersionId:"active";
        var preconditions=policyDigest is null?Array.Empty<MutationPrecondition>():new[]{new MutationPrecondition("v09.governance:"+Cluster,policyDigest)};
        return MutationOperation.CreatePreview(Guid.NewGuid(),"oidc:https://restore-fixture.invalid|owner",new(kind,Cluster,JsonSerializer.Serialize(material,AttributionContract.Json),["fixture-rate:"+target],preconditions,AuthorizationTargets:[new(AuthorizationAction.RateCardManage,Cluster,target)]),
            new(MutationRiskClass.High,["trusted-physical-restore-fixture"],MutationConfirmationMode.TypedTarget,false),"v0.8-bearer:"+new string('b',64),DateTimeOffset.UtcNow.AddMinutes(10),DateTimeOffset.UtcNow,Guid.NewGuid().ToString("N")).Snapshot;
    }
}
