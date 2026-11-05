using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W75PolicyCommitRecoveryTests
{
    [Fact]
    public async Task SQLite_committed_version_ack_loss_requires_positive_immutable_receipt_and_current_authority()
    {
        var path=Path.Combine(Path.GetTempPath(),"w75-receipt-"+Guid.NewGuid().ToString("N")+".db");
        try{await Scenario(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }
    [G2PostgreSqlFact]
    public async Task PostgreSQL_committed_version_ack_loss_requires_positive_immutable_receipt_and_current_authority()
    {
        await using var db=await G2CliPostgreSqlDatabase.CreateAsync();await Scenario(new PostgreSqlMutationDbConnectionFactory(db.ConnectionString));
    }
    private static async Task Scenario(IMutationDbConnectionFactory factory)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(30));var token=deadline.Token;
        var original=Guid.NewGuid();var gate=new AdoGovernanceGenerationGate(factory,original);await gate.InitializeAsync(token);
        var policy=new AdoGovernancePolicyStore(factory,generationGate:gate);await policy.InitializeAsync(token);
        var repository=new AdoMutationOperationRepository(factory,governance:policy);await repository.InitializeAsync(token);
        var version=await policy.StageAsync("alpha",GovernancePolicyCompiler.Compile("{\"schemaVersion\":1,\"rules\":[]}"),"owner",token);
        var initial=await policy.GetStateAsync("alpha",token);
        var preview=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"alpha",version.VersionId,[new("v09.governance:alpha",initial.Digest!)]);
        var aggregate=MutationOperation.Restore(preview);
        aggregate.OpenForConfirmation(DateTimeOffset.UtcNow);aggregate.Confirm(preview.RequesterPrincipalId,preview.PreviewHash,DateTimeOffset.UtcNow,aggregate.Snapshot.ConfirmationChallenge);
        aggregate.ClaimExecution(DateTimeOffset.UtcNow,DateTimeOffset.UtcNow.AddMinutes(2));aggregate.MarkDispatchStarted(DateTimeOffset.UtcNow);
        Assert.Equal(MutationCreateOutcome.Created,(await repository.CreateAsync(aggregate.Snapshot,token)).Outcome);
        var executingVersion=aggregate.Snapshot.Version;
        Assert.Null(await policy.ReadReceiptAsync(preview.OperationId,token));
        Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await policy.CommitVersionAsync(aggregate.Snapshot,version.VersionId,version.ContentHash,()=>true,token)).ResultKind);
        // Controlled acknowledgement-loss fixture after an actual committed SQL effect; no second effect is invoked.
        aggregate.Complete(MutationExecutionResultKind.ExecutionUnknown,"simulated_commit_ack_loss",DateTimeOffset.UtcNow);
        Assert.Equal(MutationSaveOutcome.Saved,(await repository.TrySaveAsync(aggregate.Snapshot,executingVersion,token)).Outcome);
        var source=aggregate.Snapshot;var receipt=await policy.ReadReceiptAsync(source.OperationId,token);
        Assert.True(AdoGovernancePolicyStore.Matches(receipt,source));
        Assert.False(AdoGovernancePolicyStore.Matches(receipt,source with{RequesterPrincipalId="different"}));
        Assert.Throws<MutationStateException>(()=>aggregate.ReconcileVerifiedGovernanceCommit(receipt!.OperationId,32,receipt.MaterialHash,receipt.PreviewHash,receipt.VersionId,receipt.ContentHash,receipt.Revision,receipt.CommittedAtUtc,DateTimeOffset.UtcNow));
        var recovered=MutationOperation.Restore(source);
        recovered.ReconcileVerifiedGovernanceCommit(receipt!.OperationId,receipt.OperationKind,receipt.MaterialHash,receipt.PreviewHash,receipt.VersionId,receipt.ContentHash,receipt.Revision,receipt.CommittedAtUtc,DateTimeOffset.UtcNow);
        await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>repository.TrySaveGuardedAsync(recovered.Snapshot,source.Version,
            (c,t,ct)=>policy.ValidateRecoveryAsync(c,t,source,receipt,()=>false,ct),token));
        Assert.Equal(MutationOperationState.ExecutionUnknown,(await repository.GetAsync(source.OperationId,token))!.State);
        await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>repository.TrySaveGuardedAsync(recovered.Snapshot,source.Version,
            (c,t,ct)=>policy.ValidateRecoveryAsync(c,t,source,receipt with{MaterialHash=new string('c',64)},()=>true,ct),token));
        // Real outside-generation admission races the already-observed receipt, before the owning CAS transaction.
        var next=new AdoGovernanceGenerationGate(factory,Guid.NewGuid());
        await next.AdmitRestoredGenerationAsync(original,Guid.Parse(next.DeploymentGeneration!),new string('a',64),new string('b',64),token);
        await Assert.ThrowsAsync<GovernanceGenerationException>(()=>repository.TrySaveGuardedAsync(recovered.Snapshot,source.Version,
            (c,t,ct)=>policy.ValidateRecoveryAsync(c,t,source,receipt,()=>true,ct),token));
        Assert.Equal(MutationOperationState.ExecutionUnknown,(await repository.GetAsync(source.OperationId,token))!.State);
        var admitted=new AdoGovernancePolicyStore(factory,generationGate:next);await admitted.InitializeAsync(token);
        Assert.NotEqual(initial.Digest,(await admitted.GetStateAsync("alpha",token)).Digest);
        Assert.True((await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>admitted.ValidateOperationAsync(source,token))).Stale);
        Assert.Equal(MutationSaveOutcome.Saved,(await repository.TrySaveGuardedAsync(recovered.Snapshot,source.Version,
            (c,t,ct)=>admitted.ValidateRecoveryAsync(c,t,source,receipt,()=>true,ct),token)).Outcome);
        Assert.Equal(MutationOperationState.AppliedVerified,(await repository.GetAsync(source.OperationId,token))!.State);
        Assert.Single(await admitted.ListVersionsAsync("alpha",null,50,token));
        Assert.Equal(receipt,await admitted.ReadReceiptAsync(source.OperationId,token));
    }
}
