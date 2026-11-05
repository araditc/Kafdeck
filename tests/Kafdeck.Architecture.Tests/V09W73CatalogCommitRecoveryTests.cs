using Kafdeck.Api;
using Kafdeck.Core.Catalog;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W73CatalogCommitRecoveryTests
{
    [Fact]
    public async Task SQLite_positive_receipt_recovery_is_atomic_with_current_authority_and_generation()
    {
        var directory = Path.Combine(Path.GetTempPath(), "w73-receipt-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try { await ExerciseAsync(new SqliteMutationDbConnectionFactory(Path.Combine(directory, "data.db"))); }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory, true); }
    }
    [CatalogPostgreSqlFact]
    public async Task PostgreSQL_positive_receipt_recovery_is_atomic_with_current_authority_and_generation()
    { await using var database = await G2CliPostgreSqlDatabase.CreateAsync(); await ExerciseAsync(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString)); }

    private static async Task ExerciseAsync(IMutationDbConnectionFactory factory)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(1)); var token = deadline.Token;
        var generation = Guid.NewGuid(); var gate = new AdoGovernanceGenerationGate(factory, generation); await gate.InitializeAsync(token);
        var store = new AdoCatalogStore(factory, generation.ToString("D"), gate); await store.InitializeAsync(token);
        var operations = new AdoMutationOperationRepository(factory); await operations.InitializeAsync(token);
        var id = Guid.NewGuid().ToString("N");
        async Task<(CatalogPlan Plan, CatalogCommitReceipt Receipt, MutationOperation Unknown)> CommitWithLostAcknowledgment(long revision)
        {
            var plan = Plan(generation, id, revision); await store.StageAsync(plan, token);
            var operation = Executing(plan); var receipt = await store.CommitAsync(plan, () => true, token); Assert.NotNull(receipt);
            // Explicit fault fixture: the DB effect really committed; the process cannot trust the lost acknowledgment.
            operation.Complete(MutationExecutionResultKind.ExecutionUnknown, "injected_commit_ack_loss", DateTimeOffset.UtcNow);
            await operations.CreateAsync(operation.Snapshot, token); return (plan, receipt, operation);
        }
        var first = await CommitWithLostAcknowledgment(0); var before = first.Unknown.Snapshot;
        var recovered = MutationOperation.Restore(before);
        recovered.ReconcileVerifiedCatalogCommit(first.Receipt.OperationId, first.Receipt.ContentHash, first.Receipt.Records.Count, first.Receipt.AppliedAtUtc, DateTimeOffset.UtcNow);
        var calls = 0;
        await Assert.ThrowsAsync<CatalogConflictException>(() => operations.TrySaveGuardedAsync(recovered.Snapshot, before.Version,
            (db, tx, ct) => store.VerifyReceiptOnTransactionAsync(db, tx, first.Plan, first.Receipt, () => ++calls <= 2, ct), token));
        Assert.Equal(MutationOperationState.ExecutionUnknown, (await operations.GetAsync(before.OperationId, token))!.State);
        Assert.Single(await store.HistoryAsync("alpha", CatalogEntityKind.Team, id, 0, 200, token));

        var saved = await operations.TrySaveGuardedAsync(recovered.Snapshot, before.Version,
            (db, tx, ct) => store.VerifyReceiptOnTransactionAsync(db, tx, first.Plan, first.Receipt, () => true, ct), token);
        Assert.Equal(MutationSaveOutcome.Saved, saved.Outcome); Assert.Equal(MutationOperationState.AppliedVerified, saved.Operation!.State);
        Assert.Equal(before.DispatchStartedAtUtc, saved.Operation.DispatchStartedAtUtc); Assert.Equal(before.ExecutionClaimGeneration, saved.Operation.ExecutionClaimGeneration);
        Assert.Equal(MutationSaveOutcome.VersionConflict, (await operations.TrySaveGuardedAsync(recovered.Snapshot, before.Version,
            (db, tx, ct) => store.VerifyReceiptOnTransactionAsync(db, tx, first.Plan, first.Receipt, () => true, ct), token)).Outcome);
        Assert.Single(await store.HistoryAsync("alpha", CatalogEntityKind.Team, id, 0, 200, token));

        var second = await CommitWithLostAcknowledgment(1); var secondRecovered = MutationOperation.Restore(second.Unknown.Snapshot);
        secondRecovered.ReconcileVerifiedCatalogCommit(second.Receipt.OperationId, second.Receipt.ContentHash, second.Receipt.Records.Count, second.Receipt.AppliedAtUtc, DateTimeOffset.UtcNow);
        // Real generation transition occurs AFTER the initial positive receipt read and BEFORE recovery CAS.
        var nextId = Guid.NewGuid(); var next = new AdoGovernanceGenerationGate(factory, nextId);
        await next.AdmitRestoredGenerationAsync(generation, nextId, new string('a', 64), new string('b', 64), token);
        await Assert.ThrowsAsync<GovernanceGenerationException>(() => operations.TrySaveGuardedAsync(secondRecovered.Snapshot, second.Unknown.Snapshot.Version,
            (db, tx, ct) => store.VerifyReceiptOnTransactionAsync(db, tx, second.Plan, second.Receipt, () => true, ct), token));
        Assert.Equal(MutationOperationState.ExecutionUnknown, (await operations.GetAsync(second.Plan.PlanId, token))!.State);
    }

    private static CatalogPlan Plan(Guid generation, string id, long revision)
    {
        CatalogPlannedChange[] changes = [new(CatalogContract.Validate(new(CatalogEntityKind.Team, revision == 0 ? null : id, revision, "Revision " + revision)), id, null)];
        var now = DateTimeOffset.UtcNow.AddMilliseconds(-100);
        return new(Guid.NewGuid(), "alpha", "oidc:https://idp.example|requester", new string('a', 64), generation.ToString("D"), CatalogContract.MaterialHash("alpha", changes), changes, now, now.AddMinutes(5));
    }
    private static MutationOperation Executing(CatalogPlan plan)
    {
        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(plan.PlanId, plan.RequesterPrincipalId, CatalogMutation.Intent(plan), CatalogMutation.RiskFor(plan), "test-policy", plan.ExpiresAtUtc, plan.CreatedAtUtc, Guid.NewGuid().ToString("N"));
        operation.OpenForConfirmation(now); operation.Confirm(plan.RequesterPrincipalId, operation.Snapshot.PreviewHash, now, operation.Snapshot.ConfirmationChallenge);
        operation.ClaimExecution(now, now.AddMinutes(1)); operation.MarkDispatchStarted(now); return operation;
    }
    [Theory]
    [InlineData("wrong-kind")]
    [InlineData("wrong-operation")]
    [InlineData("wrong-hash")]
    [InlineData("missing-hash")]
    [InlineData("no-dispatch")]
    [InlineData("zero-records")]
    [InlineData("future-time")]
    public void Nonmatching_catalog_receipt_cannot_clear_unknown(string failure)
    {
        var plan = Plan(Guid.NewGuid(), Guid.NewGuid().ToString("N"), 0); var operation = Executing(plan);
        operation.Complete(MutationExecutionResultKind.ExecutionUnknown, "ack_loss", DateTimeOffset.UtcNow);
        var snapshot = operation.Snapshot;
        if (failure == "wrong-kind") snapshot = snapshot with { OperationKind = MutationOperationKind.TopicCreate };
        if (failure == "missing-hash") snapshot = snapshot with { Preconditions = [] };
        if (failure == "no-dispatch") snapshot = snapshot with { DispatchStartedAtUtc = null };
        operation = MutationOperation.Restore(snapshot);
        Assert.Throws<MutationStateException>(() => operation.ReconcileVerifiedCatalogCommit(failure == "wrong-operation" ? Guid.NewGuid() : plan.PlanId,
            failure == "wrong-hash" ? new string('0', 64) : plan.ContentHash, failure == "zero-records" ? 0 : 1,
            failure == "future-time" ? DateTimeOffset.UtcNow.AddMinutes(1) : DateTimeOffset.UtcNow, DateTimeOffset.UtcNow));
        Assert.Equal(snapshot, operation.Snapshot);
    }
}
