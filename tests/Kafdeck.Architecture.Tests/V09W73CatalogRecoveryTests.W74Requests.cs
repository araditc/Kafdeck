using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed partial class V09W73CatalogRecoveryTests
{
    [Fact]
    public async Task Actual_SQLite_request_backup_restore_preserves_immutable_decision_and_requires_fresh_generation_reapproval()
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromMinutes(2));var token=timeout.Token;
        var directory=Path.Combine(Path.GetTempPath(),"w74-request-restore-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        try
        {
            var original=Path.Combine(directory,"source.db");var backup=Path.Combine(directory,"backup.db");var restored=Path.Combine(directory,"restored.db");
            var generation=Guid.NewGuid();var factory=new SqliteMutationDbConnectionFactory(original);var gate=new AdoGovernanceGenerationGate(factory,generation);await gate.InitializeAsync(token);
            var seeded=await SeedW74Requests(factory,gate,token);var attribution=await V09W76RecoverySeed.SeedAsync(factory,gate,token);await CopySqlite(original,backup,token);
            await CommitW74Request(seeded.Store,seeded.Policy,seeded.Target,seeded.RequestId,ServiceRequestCommand.Cancel,3,token);
            Assert.Equal(ServiceRequestState.Cancelled,(await seeded.Store.GetAsync("alpha",seeded.RequestId,token))!.BusinessState);
            await CopySqlite(backup,restored,token);
            var restoredFactory=new SqliteMutationDbConnectionFactory(restored);var next=Guid.NewGuid();var restoredGate=new AdoGovernanceGenerationGate(restoredFactory,next);
            await Assert.ThrowsAsync<GovernanceGenerationException>(()=>restoredGate.InitializeAsync(token));
            var backupHash=Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(backup,token))).ToLowerInvariant();
            await restoredGate.AdmitRestoredGenerationAsync(generation,next,GovernancePolicyCompiler.Hash("isolated SQLite restore; no workers; distinct database paths"),backupHash,token);
            await VerifyW74FreshApproval(restoredFactory,restoredGate,seeded,generation,token);
            await V09W76RecoverySeed.VerifyAsync(restoredFactory,restoredGate,attribution,false,token);
            await WriteW74Evidence("sqlite",new{actualSqliteBackup=true,approvedRequestAndDecisionRestored=true,pendingRequestPlanRestored=true,
                oldGenerationRefused=true,oldApprovalCannotAuthorizeFreshOperation=true,freshRevisionRequiresNewHumanDecision=true,freshReapprovedOperationAllowed=true,
                trustedPortSeedingNotAuthenticationEvidence=true},token);
        }
        finally{SqliteConnection.ClearAllPools();Directory.Delete(directory,true);}
    }
    [PostgreSqlRecoveryFact]
    public async Task Actual_PostgreSQL_request_physical_backup_promotion_restore_fences_old_approval_and_requires_reapproval()
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromMinutes(6));var token=timeout.Token;
        await using var lab=await PostgreSqlRecoveryScenario.CreateAsync(token);
        var factory=new PostgreSqlMutationDbConnectionFactory(await lab.ConnectionStringAsync("primary",token));var generation=Guid.NewGuid();
        var gate=new AdoGovernanceGenerationGate(factory,generation);await gate.InitializeAsync(token);var seeded=await SeedW74Requests(factory,gate,token);var attribution=await V09W76RecoverySeed.SeedAsync(factory,gate,token);
        await lab.ActionAsync("Backup",token);await CommitW74Request(seeded.Store,seeded.Policy,seeded.Target,seeded.RequestId,ServiceRequestCommand.Cancel,3,token);
        await lab.ActionAsync("Sync",token);await WaitW74RequestVersion(await lab.ConnectionStringAsync("standby",token),seeded.RequestId,4,token);
        await lab.ActionAsync("Failover",token);using var promotion=await lab.ReadEvidenceAsync("promotion-evidence.json",token);Assert.True(promotion.RootElement.GetProperty("oldPrimaryFenced").GetBoolean());
        var survivorFactory=new PostgreSqlMutationDbConnectionFactory(await lab.ConnectionStringAsync("standby",token));
        var survivor=new AdoServiceRequestStore(survivorFactory,new AdoMutationOperationRepository(survivorFactory),new W74RestoreAuthority(),new AdoGovernanceGenerationGate(survivorFactory,generation));
        Assert.Equal(ServiceRequestState.Cancelled,(await survivor.GetAsync("alpha",seeded.RequestId,token))!.BusinessState);
        await lab.ActionAsync("Restore",token);using var restore=await lab.ReadEvidenceAsync("restore-evidence.json",token);
        Assert.True(restore.RootElement.GetProperty("physicalBackupVerified").GetBoolean());Assert.True(restore.RootElement.GetProperty("quarantined").GetBoolean());
        var restoredFactory=new PostgreSqlMutationDbConnectionFactory(await lab.ConnectionStringAsync("restore",token));await WaitW74RequestVersion(await lab.ConnectionStringAsync("restore",token),seeded.RequestId,3,token);
        var oldStore=new AdoServiceRequestStore(restoredFactory,new AdoMutationOperationRepository(restoredFactory),new W74RestoreAuthority(),new AdoGovernanceGenerationGate(restoredFactory,generation));
        Assert.Equal("25006",(await Assert.ThrowsAsync<PostgresException>(()=>oldStore.StageAsync(seeded.Pending,token))).SqlState);
        await lab.ActionAsync("FenceOriginalNodes",token);var next=Guid.NewGuid();var anchor=await lab.WriteAnchorAsync(next,"quarantined",token);await lab.ActionAsync("AdmitRestoreMaintenance",token,next,anchor);
        var fenceHash=Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(lab.DirectoryPath,"original-nodes-fenced.json"),token))).ToLowerInvariant();
        var restoreHash=Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(lab.DirectoryPath,"restore-evidence.json"),token))).ToLowerInvariant();
        var freshGate=new AdoGovernanceGenerationGate(restoredFactory,next);await freshGate.AdmitRestoredGenerationAsync(generation,next,fenceHash,restoreHash,token);
        await VerifyW74FreshApproval(restoredFactory,freshGate,seeded,generation,token);
        await V09W76RecoverySeed.VerifyAsync(restoredFactory,freshGate,attribution,true,token);
        await lab.WriteEvidenceAsync("postgres-w74-requests-restore-evidence.json",new{actualPhysicalBackup=true,pgVerifyBackup=true,actualPromotion=true,originalPrimaryFenced=true,
            approvedRequestAndDecisionRestored=true,pendingRequestPlanRestored=true,quarantineRefusedRequestWrite=true,bothOriginalNodesFenced=true,
            oldGenerationRefused=true,oldApprovalCannotAuthorizeFreshOperation=true,freshRevisionRequiresNewHumanDecision=true,freshReapprovedOperationAllowed=true,
            trustedPortSeedingNotAuthenticationEvidence=true},token);
    }
    private sealed record W74RestoreSeed(AdoServiceRequestStore Store,AdoGovernancePolicyStore Policy,MutationOperationSnapshot Target,string RequestId,ServiceRequestPlan Pending);
    private sealed class W74RestoreAuthority:IServiceRequestCurrentAuthority
    {public bool IsCurrentRequesterAllowed(string principal,AuthorizationRequest request)=>true;public bool IsCurrentHumanDecisionAllowed(string principal,AuthorizationRequest request)=>principal.EndsWith("reviewer",StringComparison.Ordinal);}
    private static async Task<W74RestoreSeed> SeedW74Requests(IMutationDbConnectionFactory factory,IGovernanceGenerationTransactionGate gate,CancellationToken token)
    {
        var operations=new AdoMutationOperationRepository(factory);await operations.InitializeAsync(token);var store=new AdoServiceRequestStore(factory,operations,new W74RestoreAuthority(),gate);await store.InitializeAsync(token);
        var policy=new AdoGovernancePolicyStore(factory,generationGate:gate,approvedRequests:store);await policy.InitializeAsync(token);
        var compiled=GovernancePolicyCompiler.Compile(JsonSerializer.Serialize(new{schemaVersion=1,rules=new[]{new{id="require-restored-request",priority=1,
            scope=new{clusterId="alpha",resourceKind="topic",resourceIds=new[]{"orders"},action="TopicCreate"},conditions=Array.Empty<object>(),outcome="RequireApprovedRequest"}}}));
        var before=await policy.GetStateAsync("alpha",token);var markers=before.Digest is null?Array.Empty<MutationPrecondition>():new[]{new MutationPrecondition("v09.governance:alpha",before.Digest)};
        var version=await policy.StageAsync("alpha",compiled,"trusted-restore-owner",token);
        await policy.CommitVersionAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"alpha",version.VersionId,markers),version.VersionId,version.ContentHash,()=>true,token);
        await policy.ActivateAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"alpha","active",markers),version.VersionId,version.ContentHash,before.Revision,true,()=>true,token);
        var target=await NewW74Target(operations,policy,token);var id=Guid.NewGuid().ToString("N");
        await CommitW74Request(store,policy,target,id,ServiceRequestCommand.Draft,0,token);await CommitW74Request(store,policy,target,id,ServiceRequestCommand.Submit,1,token);await CommitW74Request(store,policy,target,id,ServiceRequestCommand.Approve,2,token);
        await policy.ValidateOperationAsync(target,token);var pending=await StageW74Request(store,target,id,ServiceRequestCommand.Cancel,3,token);return new(store,policy,target,id,pending);
    }
    private static async Task VerifyW74FreshApproval(IMutationDbConnectionFactory factory,AdoGovernanceGenerationGate gate,W74RestoreSeed seed,Guid oldGeneration,CancellationToken token)
    {
        var operations=new AdoMutationOperationRepository(factory);var store=new AdoServiceRequestStore(factory,operations,new W74RestoreAuthority(),gate);await store.InitializeAsync(token);
        var policy=new AdoGovernancePolicyStore(factory,generationGate:gate,approvedRequests:store);await policy.InitializeAsync(token);
        var restored=await store.GetAsync("alpha",seed.RequestId,token);Assert.NotNull(restored);Assert.Equal(ServiceRequestState.Approved,restored.BusinessState);Assert.Single(restored.Decisions);Assert.Equal(3,restored.Version);
        Assert.Equal(seed.Pending.ContentHash,(await store.GetPlanAsync(seed.Pending.PlanId,token))!.ContentHash);
        var old=new AdoServiceRequestStore(factory,operations,new W74RestoreAuthority(),new AdoGovernanceGenerationGate(factory,oldGeneration));
        await Assert.ThrowsAsync<GovernanceGenerationException>(()=>old.StageAsync(seed.Pending,token));
        Assert.Equal("RequestRestoreGenerationChanged",(await Assert.ThrowsAsync<ServiceRequestConflictException>(()=>store.StageAsync(seed.Pending,token))).Code);
        Assert.Equal("GovernancePolicyChanged",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>policy.ValidateOperationAsync(seed.Target,token))).Code);
        var fresh=await NewW74Target(operations,policy,token);Assert.Equal("RequestApprovalRequired",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>policy.ValidateOperationAsync(fresh,token))).Code);
        var revised=await CommitW74Request(store,policy,fresh,seed.RequestId,ServiceRequestCommand.Revise,3,token);Assert.Equal(2,revised.Revision);Assert.Empty(revised.Decisions);
        await CommitW74Request(store,policy,fresh,seed.RequestId,ServiceRequestCommand.Submit,4,token);
        Assert.Equal("RequestApprovalRequired",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>policy.ValidateOperationAsync(fresh,token))).Code);
        await CommitW74Request(store,policy,fresh,seed.RequestId,ServiceRequestCommand.Approve,5,token);await policy.ValidateOperationAsync(fresh,token);
    }
    private static async Task<MutationOperationSnapshot> NewW74Target(AdoMutationOperationRepository operations,AdoGovernancePolicyStore policy,CancellationToken token)
    {
        var state=await policy.GetStateAsync("alpha",token);var now=DateTimeOffset.UtcNow;var target=MutationOperation.CreatePreview(Guid.NewGuid(),"oidc:https://restore.invalid|owner",
            new(MutationOperationKind.TopicCreate,"alpha","{\"topic\":\"orders\"}",["topic:orders"],[new("v09.governance:alpha",state.Digest!)],AuthorizationTargets:[new(AuthorizationAction.TopicCreate,"alpha","orders")]),
            new(MutationRiskClass.Moderate,["trusted-restore-fixture"],MutationConfirmationMode.Explicit,false),"v0.8-bearer:"+new string('a',64),now.AddMinutes(15),now,Guid.NewGuid().ToString("N")).Snapshot;
        await operations.CreateAsync(target,token);return target;
    }
    private static async Task<ServiceRequestPlan> StageW74Request(AdoServiceRequestStore store,MutationOperationSnapshot target,string id,ServiceRequestCommand command,long version,CancellationToken token)
    {
        var now=DateTimeOffset.UtcNow;var actor=ServiceRequestContract.IsDecision(command)?"oidc:https://restore.invalid|reviewer":target.RequesterPrincipalId;
        var plan=new ServiceRequestPlan(Guid.NewGuid(),"alpha","",id,command,version,actor,"Browser",new string('b',64),store.DeploymentGeneration,AdoServiceRequestStore.Bind(target),
            "trusted physical request recovery fixture",target.PreviewExpiresAtUtc,now,now.AddMinutes(10));plan=plan with{ContentHash=ServiceRequestContract.PlanHash(plan)};return await store.StageAsync(plan,token);
    }
    private static async Task<ServiceRequestRecord> CommitW74Request(AdoServiceRequestStore store,AdoGovernancePolicyStore policy,MutationOperationSnapshot target,string id,ServiceRequestCommand command,long version,CancellationToken token)
    {
        var plan=await StageW74Request(store,target,id,command,version,token);var now=DateTimeOffset.UtcNow;
        var operation=MutationOperation.CreatePreview(plan.PlanId,plan.RequesterPrincipalId,Kafdeck.Api.ServiceRequestMutation.Intent(plan),
            new(MutationRiskClass.Moderate,["trusted-restore-fixture"],MutationConfirmationMode.Explicit,false),"v0.8-bearer:"+new string('b',64),now.AddMinutes(10),now,Guid.NewGuid().ToString("N")).Snapshot;
        return(await store.CommitAsync(plan,operation,()=>true,policy.ValidateBindingDigestsAsync,token)).Request;
    }
    private static async Task WaitW74RequestVersion(string connection,string id,long expected,CancellationToken token)
    {
        await using var db=new NpgsqlConnection(connection);await db.OpenAsync(token);
        for(var attempt=0;attempt<60;attempt++){await using var cmd=new NpgsqlCommand("SELECT version FROM service_request_heads WHERE request_id=@id",db);cmd.Parameters.AddWithValue("id",id);
            if(Convert.ToInt64(await cmd.ExecuteScalarAsync(token),CultureInfo.InvariantCulture)==expected)return;await Task.Delay(100,token);}
        throw new InvalidOperationException("Physical request replica/restore version did not converge.");
    }
    private static async Task WriteW74Evidence(string provider,object value,CancellationToken token)
    {var path=Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY");if(path is null)return;Directory.CreateDirectory(path);await File.WriteAllBytesAsync(Path.Combine(path,provider+"-w74-requests-restore-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(value),token);}
}
