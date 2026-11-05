using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

/// <summary>Real store transactions; fixture-seeded policy is not product approval or broker effect evidence.</summary>
public sealed class V09W75FleetDispatchGateTests
{
    [Fact]
    public Task SQLite_policy_activation_after_reservation_blocks_both_new_dispatch_markers() => Sqlite(false);
    [Fact]
    public Task SQLite_already_dispatched_batches_remain_observable_after_policy_changes() => Sqlite(true);
    [G2PostgreSqlFact]
    public Task PostgreSql_policy_activation_after_reservation_blocks_both_new_dispatch_markers() => PostgreSql(false);
    [G2PostgreSqlFact]
    public Task PostgreSql_already_dispatched_batches_remain_observable_after_policy_changes() => PostgreSql(true);

    private static async Task Sqlite(bool alreadyStarted)
    {
        foreach (var generator in new[] {false,true})
        {
            var path=Path.Combine(Path.GetTempPath(),"kafdeck-w75-dispatch-"+Guid.NewGuid().ToString("N")+".db");
            try{await Prove(new SqliteMutationDbConnectionFactory(path),generator,alreadyStarted);}
            finally{Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();foreach(var suffix in new[]{"","-wal","-shm"})if(File.Exists(path+suffix))File.Delete(path+suffix);}
        }
    }
    private static async Task PostgreSql(bool alreadyStarted)
    {
        foreach(var generator in new[]{false,true})
        {
            await using var database=await G2CliPostgreSqlDatabase.CreateAsync();
            await Prove(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString),generator,alreadyStarted);
        }
    }
    private static async Task Prove(IMutationDbConnectionFactory factory,bool generator,bool alreadyStarted)
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(30));var token=timeout.Token;
        var repository=new AdoMutationOperationRepository(factory);await repository.InitializeAsync(token);
        var policy=new AdoGovernancePolicyStore(factory);await policy.InitializeAsync(token);
        var store=new AdoFleetMutationStateStore(factory,repository,policy);await store.InitializeAsync(token);
        var kind=generator?MutationOperationKind.DataGenerator:MutationOperationKind.DataJob;
        var targets=generator
            ? new[]{AuthorizationAction.DataGeneratorPlan,AuthorizationAction.DataGeneratorExecute,AuthorizationAction.ClusterRead,AuthorizationAction.TopicRead,AuthorizationAction.RecordProduce}
                .Select(action=>new MutationAuthorizationTarget(action,"permitted","orders")).ToArray()
            : new[]{new MutationAuthorizationTarget(AuthorizationAction.DataJobPlan,"source","source-orders"),new(AuthorizationAction.DataJobExecute,"permitted","orders"),
                new(AuthorizationAction.ClusterRead,"source","source-orders"),new(AuthorizationAction.ClusterRead,"permitted","orders"),
                new(AuthorizationAction.TopicRead,"source","source-orders"),new(AuthorizationAction.TopicRead,"permitted","orders"),
                new(AuthorizationAction.RecordRead,"source","source-orders"),new(AuthorizationAction.RecordExport,"source","source-orders"),new(AuthorizationAction.RecordProduce,"permitted","orders")};
        var intent=new MutationIntentDescriptor(kind,"permitted","{\"fixture\":\"bounded-dispatch\"}",["fixture:orders"],AuthorizationTargets:targets);
        var risk=new MutationRiskDecision(MutationRiskClass.High,["bounded_fixture"],MutationConfirmationMode.Explicit,false);
        var now=DateTimeOffset.UtcNow;
        var parent=MutationOperation.CreatePreview(Guid.NewGuid(),"oidc:https://recovery.invalid|owner",intent,risk,"v0.8-bearer:"+new string('b',64),now.AddMinutes(5),now,Guid.NewGuid().ToString("N")).Snapshot;
        Assert.Equal(MutationCreateOutcome.Created,(await repository.CreateAsync(parent,token)).Outcome);
        var baseProgress=FleetOperationProgress.Create(parent.OperationId,1,now).Snapshot;
        var generate=FleetGeneratorProgress.Create(parent.PreviewHash);
        var transfer=FleetTransferProgress.Create(parent.PreviewHash,[0]);
        if(generator)generate.ReserveBeforeDispatch(0,0,4,now);else transfer.ReserveBeforeDispatch(0,0,0,4,now);
        var reserved=baseProgress with{Phase=FleetProgressPhase.Submitting,Generator=generator?generate.Snapshot:null,Transfer=generator?null:transfer.Snapshot};
        Assert.Equal(FleetProgressCreateOutcome.Created,(await store.CreateProgressAsync(reserved,token)).Outcome);
        if(generator)generate.MarkDispatchStarted(generate.Snapshot.PendingBatch!.BatchId,now.AddSeconds(1));else transfer.MarkDispatchStarted(transfer.Snapshot.PendingBatch!.BatchId,now.AddSeconds(1));
        var started=reserved with{Version=1,UpdatedAtUtc=now.AddSeconds(1),Generator=generator?generate.Snapshot:null,Transfer=generator?null:transfer.Snapshot};
        if(alreadyStarted)Assert.Equal(FleetProgressSaveOutcome.Saved,(await store.TrySaveProgressAsync(started,0,token)).Outcome);
        // The policy commits after reservation; its new epoch must be checked at
        // the durable per-record marker, not only at the original operation dispatch.
        await V09W75RecoverySeed.SeedAsync(factory,null,null,"permitted","orders",token);
        var candidate=alreadyStarted?started with{Version=2,Phase=FleetProgressPhase.Observing,UpdatedAtUtc=now.AddSeconds(2)}:started;
        var result=await store.TrySaveProgressAsync(candidate,alreadyStarted?1:0,token);
        var expected=alreadyStarted?FleetProgressSaveOutcome.Saved:FleetProgressSaveOutcome.VersionConflict;
        Assert.Equal(expected,result.Outcome);
        var restarted=new AdoFleetMutationStateStore(factory,repository,policy);
        var durable=await restarted.GetProgressAsync(parent.OperationId,token);Assert.NotNull(durable);
        Assert.Equal(alreadyStarted?2:0,durable.Version);
        if(generator)Assert.Equal(alreadyStarted?FleetGeneratorBatchState.DispatchStarted:FleetGeneratorBatchState.ReservedBeforeDispatch,durable.Generator!.PendingBatch!.State);
        else Assert.Equal(alreadyStarted?FleetTransferBatchState.DispatchStarted:FleetTransferBatchState.ReservedBeforeDispatch,durable.Transfer!.PendingBatch!.State);
        Assert.Equal(parent.PreviewHash,(await repository.GetAsync(parent.OperationId,token))!.PreviewHash);
    }
}
