using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

[Collection("Native notification recovery")]
public sealed class V09W73CatalogRecoveryTests(ITestOutputHelper output)
{
    [Fact]
    public async Task Actual_SQLite_backup_restore_preserves_catalog_active_policy_and_pending_plan_then_fences_old_generation()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2)); var token = deadline.Token;
        var directory = Path.Combine(Path.GetTempPath(), "w73-restore-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try
        {
            var original = Path.Combine(directory, "source.db"); var backup = Path.Combine(directory, "backup.db"); var restored = Path.Combine(directory, "restored.db");
            var generation = Guid.NewGuid(); var factory = new SqliteMutationDbConnectionFactory(original);
            var gate = new AdoGovernanceGenerationGate(factory, generation); await gate.InitializeAsync(token);
            var store = new AdoCatalogStore(factory, generation.ToString("D"), gate); await store.InitializeAsync(token);
            var (id, pending) = await SeedCatalog(store, token);
            var policy = await V09W75RecoverySeed.SeedAsync(factory, gate, store, "alpha", "orders", token);
            var operations = new AdoMutationOperationRepository(factory); await operations.InitializeAsync(token); await operations.CreateAsync(policy.Pending, token);
            await CopySqlite(original, backup, token);
            Assert.NotNull(await store.CommitAsync(pending, () => true, token));
            Assert.Equal(2, (await store.GetAsync("alpha", CatalogEntityKind.Team, id, token))!.Revision);
            await CopySqlite(backup, restored, token);
            var restoredFactory = new SqliteMutationDbConnectionFactory(restored); var next = Guid.NewGuid();
            var newGate = new AdoGovernanceGenerationGate(restoredFactory, next);
            await Assert.ThrowsAsync<GovernanceGenerationException>(() => newGate.InitializeAsync(token));
            var restoreHash = Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(backup, token))).ToLowerInvariant();
            var fenceHash = CatalogContract.Hash("isolated SQLite restore fixture; no background workers; different source/restore paths; " + generation.ToString("D"));
            await newGate.AdmitRestoredGenerationAsync(generation, next, fenceHash, restoreHash, token);
            var current = new AdoCatalogStore(restoredFactory, next.ToString("D"), newGate); await current.InitializeAsync(token);
            Assert.Equal(1, (await current.GetAsync("alpha", CatalogEntityKind.Team, id, token))!.Revision);
            Assert.Equal(pending.ContentHash, (await current.GetPlanAsync(pending.PlanId, token))!.ContentHash);
            var old = new AdoCatalogStore(restoredFactory, generation.ToString("D"), new AdoGovernanceGenerationGate(restoredFactory, generation));
            await Assert.ThrowsAsync<GovernanceGenerationException>(() => old.CommitAsync(pending, () => true, token));
            await VerifyPolicy(restoredFactory, newGate, current, policy, token, generationChanged: true);
            var fresh = Plan(current, id, 1, "Fresh after restored generation"); await current.StageAsync(fresh, token);
            Assert.NotNull(await current.CommitAsync(fresh, () => true, token));
            output.WriteLine("W73/W75 actual SQLite BackupDatabase restore: baseline revision restored; active policy/version and durable pending operation preserved; old generation refused; fresh explicit plan committed.");
        }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory, true); }
    }

    [PostgreSqlRecoveryFact]
    public async Task Actual_PostgreSQL_physical_backup_promotion_restore_and_generation_admission_preserve_catalog_and_policy()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(6)); var token = deadline.Token;
        await using var lab = await PostgreSqlRecoveryScenario.CreateAsync(token);
        var source = new PostgreSqlMutationDbConnectionFactory(await lab.ConnectionStringAsync("primary", token));
        var generation = Guid.NewGuid(); var gate = new AdoGovernanceGenerationGate(source, generation); await gate.InitializeAsync(token);
        var store = new AdoCatalogStore(source, generation.ToString("D"), gate); await store.InitializeAsync(token);
        var (id, pending) = await SeedCatalog(store, token);
        var policy = await V09W75RecoverySeed.SeedAsync(source, gate, store, "alpha", "orders", token);
        var operations = new AdoMutationOperationRepository(source); await operations.InitializeAsync(token); await operations.CreateAsync(policy.Pending, token);
        await lab.ActionAsync("Backup", token);
        Assert.NotNull(await store.CommitAsync(pending, () => true, token));
        await lab.ActionAsync("Sync", token);
        await WaitRevision(await lab.ConnectionStringAsync("standby", token), id, 2, token);
        await lab.ActionAsync("Failover", token);
        using var promotion = await lab.ReadEvidenceAsync("promotion-evidence.json", token);
        Assert.True(promotion.RootElement.GetProperty("oldPrimaryFenced").GetBoolean()); Assert.True(promotion.RootElement.GetProperty("oldPrimaryPortClosed").GetBoolean());
        var promotedFactory = new PostgreSqlMutationDbConnectionFactory(await lab.ConnectionStringAsync("standby", token));
        var promotedGate = new AdoGovernanceGenerationGate(promotedFactory, generation);
        var survivor = new AdoCatalogStore(promotedFactory, generation.ToString("D"), promotedGate);
        Assert.Equal(2, (await survivor.GetAsync("alpha", CatalogEntityKind.Team, id, token))!.Revision);
        Assert.NotNull(await survivor.GetReceiptAsync(pending.PlanId, token));
        await VerifyPolicy(promotedFactory, promotedGate, survivor, policy, token);

        await lab.ActionAsync("Restore", token);
        using var restoreProof = await lab.ReadEvidenceAsync("restore-evidence.json", token);
        Assert.True(restoreProof.RootElement.GetProperty("physicalBackupVerified").GetBoolean()); Assert.True(restoreProof.RootElement.GetProperty("quarantined").GetBoolean());
        await WaitRevision(await lab.ConnectionStringAsync("restore", token), id, 1, token);
        var restoredFactory = new PostgreSqlMutationDbConnectionFactory(await lab.ConnectionStringAsync("restore", token));
        var next = Guid.NewGuid(); var restoredGate = new AdoGovernanceGenerationGate(restoredFactory, next);
        var quarantined = new AdoCatalogStore(restoredFactory, generation.ToString("D"), new AdoGovernanceGenerationGate(restoredFactory, generation));
        Assert.Equal("25006", (await Assert.ThrowsAsync<PostgresException>(() => quarantined.CommitAsync(pending, () => true, token))).SqlState);
        await lab.ActionAsync("FenceOriginalNodes", token);
        var anchor = await lab.WriteAnchorAsync(next, "quarantined", token);
        await lab.ActionAsync("AdmitRestoreMaintenance", token, next, anchor);
        var fenceHash = Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(lab.DirectoryPath, "original-nodes-fenced.json"), token))).ToLowerInvariant();
        var restoreHash = Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(lab.DirectoryPath, "restore-evidence.json"), token))).ToLowerInvariant();
        await restoredGate.AdmitRestoredGenerationAsync(generation, next, fenceHash, restoreHash, token);
        var restoredStore = new AdoCatalogStore(restoredFactory, next.ToString("D"), restoredGate); await restoredStore.InitializeAsync(token);
        await Assert.ThrowsAsync<GovernanceGenerationException>(() => quarantined.CommitAsync(pending, () => true, token));
        Assert.Null(await restoredStore.GetReceiptAsync(pending.PlanId, token));
        Assert.Equal(pending.ContentHash, (await restoredStore.GetPlanAsync(pending.PlanId, token))!.ContentHash);
        await VerifyPolicy(restoredFactory, restoredGate, restoredStore, policy, token, generationChanged: true);
        var fresh = Plan(restoredStore, id, 1, "Fresh explicit restored generation"); await restoredStore.StageAsync(fresh, token);
        Assert.NotNull(await restoredStore.CommitAsync(fresh, () => true, token));
        await lab.WriteEvidenceAsync("postgres-w73-w75-catalog-policy-restore-evidence.json", new
        {
            actualPhysicalBackup = true, pgVerifyBackup = true, actualStandbyPromotion = true, oldPrimaryPhysicallyFenced = true,
            catalogRevisionBeforeRestore = 2, restoredCatalogRevision = 1, pendingCatalogPlanRestored = true,
            activePolicyVersionAndDigestRestored = true, durablePendingPolicyOperationRestored = true, restoredPolicyDenialPreserved = true,
            readOnlyQuarantineRefusedEffect = true, bothOriginalNodesFencedBeforeMaintenance = true, newGenerationExplicitlyAdmitted = true,
            oldConfigurationFenced = true, oldPlanNotReplayed = true, freshExplicitPlanCommitted = true,
            fixtureSeedingNotProductApprovalEvidence = true,
        }, token);
        output.WriteLine("W73/W75 actual PostgreSQL verified physical backup/standby promotion/restore: catalog+policy+pending operation preserved; quarantined old plan refused; physical fencing/new generation/fresh plan proven.");
    }

    private static async Task<(string Id, CatalogPlan Pending)> SeedCatalog(AdoCatalogStore store, CancellationToken token)
    {
        var id = Guid.NewGuid().ToString("N"); var original = Plan(store, id, 0, "Original snapshot");
        await store.StageAsync(original, token); Assert.NotNull(await store.CommitAsync(original, () => true, token));
        var pending = Plan(store, id, 1, "After backup"); await store.StageAsync(pending, token); return (id, pending);
    }
    private static CatalogPlan Plan(ICatalogStore store, string id, long revision, string name)
    {
        CatalogPlannedChange[] changes = [new(CatalogContract.Validate(new(CatalogEntityKind.Team, revision == 0 ? null : id, revision, name)), id, null)]; var now = DateTimeOffset.UtcNow;
        return new(Guid.NewGuid(), "alpha", "oidc:https://recovery.invalid|owner", new string('a', 64), store.DeploymentGeneration,
            CatalogContract.MaterialHash("alpha", changes), changes, now, now.AddMinutes(10));
    }
    private static async Task VerifyPolicy(IMutationDbConnectionFactory factory, IGovernanceGenerationTransactionGate gate, AdoCatalogStore catalog, W75RecoverySeed seed, CancellationToken token, bool generationChanged = false)
    {
        var store = new AdoGovernancePolicyStore(factory, catalog, gate);
        var active = await store.GetStateAsync("alpha", token);
        if (generationChanged) Assert.NotEqual(seed.Active.Digest, active.Digest); else Assert.Equal(seed.Active.Digest, active.Digest);
        Assert.Equal(seed.Active.Revision, active.Revision);
        var version = await store.GetVersionAsync("alpha", seed.Version.VersionId, token: token); Assert.NotNull(version);
        Assert.Equal(seed.Version.ContentHash, version.ContentHash); Assert.Equal(seed.Version.Document, version.Document);
        var pending = await new AdoMutationOperationRepository(factory).GetAsync(seed.Pending.OperationId, token); Assert.NotNull(pending);
        Assert.Equal(seed.Pending.CanonicalIntent, pending.CanonicalIntent);
        var denied = await Assert.ThrowsAsync<GovernanceEffectRejectedException>(() => store.ValidateOperationAsync(pending, token));
        Assert.Equal(generationChanged ? "GovernancePolicyChanged" : "GovernanceDenied", denied.Code);
        if (generationChanged)
        {
            var fresh = V09W75RecoverySeed.Operation(MutationOperationKind.TopicDelete, AuthorizationAction.TopicDelete, "alpha", "orders", [new("v09.governance:alpha", active.Digest!)]);
            Assert.Equal("GovernanceDenied", (await Assert.ThrowsAsync<GovernanceEffectRejectedException>(() => store.ValidateOperationAsync(fresh, token))).Code);
        }
    }
    private static async Task CopySqlite(string source, string target, CancellationToken token)
    {
        await using var from = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = source, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString);
        await using var to = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = target, Pooling = false }.ConnectionString);
        await from.OpenAsync(token); await to.OpenAsync(token); from.BackupDatabase(to);
    }
    private static async Task WaitRevision(string connectionString, string id, long revision, CancellationToken token)
    {
        while (true)
        {
            await using var db = new NpgsqlConnection(connectionString); await db.OpenAsync(token);
            await using var command = db.CreateCommand(); command.CommandText = "SELECT revision FROM catalog_owned_entities WHERE cluster_id='alpha' AND kind=1 AND id=@id"; command.Parameters.AddWithValue("id", id);
            var value = await command.ExecuteScalarAsync(token);
            if (value is long observed && observed == revision) return;
            await Task.Delay(50, token);
        }
    }
}
