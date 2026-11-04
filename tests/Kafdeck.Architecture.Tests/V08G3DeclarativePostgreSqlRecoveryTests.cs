using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Native storage/anchor proof. OIDC, independent current RBAC and separate API
// process authority are exercised by the genuine hosted G3 acceptance suite.
[Collection("Native notification recovery")]
public sealed class V08G3DeclarativePostgreSqlRecoveryTests
{
    [PostgreSqlRecoveryFact]
    public async Task Real_synchronous_replica_promotion_preserves_receipt_and_fences_expired_controller_epoch()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(6)); var token = deadline.Token;
        await using var lab = await PostgreSqlRecoveryScenario.CreateAsync(token);
        var generation = Guid.NewGuid();
        var authority = await Anchor(lab, generation, "active", token);
        var primary = await Context.Create(lab, "primary", generation, authority, initialize: true, token);
        var committedPlan = await Plan(primary, "committed-before-failover", 120, token);
        var committed = await Apply(primary, committedPlan, token); Assert.NotNull(committed);
        var pending = await Plan(primary, "pending-before-failover", 5, token);
        await lab.ActionAsync("Sync", token);
        var replica = await Context.Create(lab, "standby", generation, authority, initialize: false, token);
        Assert.Equal(committed, await replica.Store.GetCommitReceiptAsync(committedPlan.PlanId, token));
        Assert.Equal(pending.PlanFingerprint, (await replica.Store.GetPlanAsync(pending.PlanId, token))!.PlanFingerprint);
        Assert.Equal(pending.LeaseEpoch, await Epoch(replica, pending.LeaseScopeId, token));
        await lab.ActionAsync("Failover", token);
        using var promotion = await lab.ReadEvidenceAsync("promotion-evidence.json", token);
        Assert.True(promotion.RootElement.GetProperty("oldPrimaryFenced").GetBoolean());
        Assert.True(promotion.RootElement.GetProperty("oldPrimaryPortClosed").GetBoolean());
        Assert.False(promotion.RootElement.GetProperty("inRecovery").GetBoolean());
        var survivor = await Context.Create(lab, "standby", generation, authority, initialize: false, token);
        Assert.Equal(committed, await Apply(survivor, committedPlan, token));
        // Actual database time, not a synthetic TimeProvider or timestamp edit.
        var elapsedUntil = DateTimeOffset.UtcNow.AddSeconds(15);
        DateTimeOffset observed;
        while ((observed = await survivor.Store.ReadDatabaseUtcNowAsync(token)) <= pending.ExpiresAtUtc.AddMilliseconds(100))
        {
            Assert.True(DateTimeOffset.UtcNow < elapsedUntil, "Real database lease expiry did not advance within the finite bound.");
            await Task.Delay(100, token);
        }
        Assert.Null(await Apply(survivor, pending, token));
        var fresh = await Plan(survivor, pending.Key.ResourceId, 120, token);
        Assert.True(fresh.LeaseEpoch > pending.LeaseEpoch);
        var result = await Apply(survivor, fresh, token); Assert.NotNull(result);
        Assert.Equal(result, await Apply(survivor, fresh, token));
        Assert.Null(await survivor.Store.GetCommitReceiptAsync(pending.PlanId, token));
        Assert.Equal(2L, await Count(survivor, "kafdeck_declarative_commit_receipts", token));
        Assert.Equal(2L, await Count(survivor, "kafdeck_declarative_ownership_audit", token));
        await lab.WriteEvidenceAsync("g3-postgresql-lease-failover-evidence.json", new
        {
            actualPhysicalSynchronousReplica = true, committedReceiptReadOnStandbyBeforePromotion = true,
            immutablePendingPlanAndEpochReadOnStandby = true, oldPrimaryPhysicallyFenced = true,
            physicalStandbyPromoted = true, replicaTimeline = promotion.RootElement.GetProperty("timeline").GetInt32(),
            realDatabaseExpiryObserved = observed, syntheticClock = false,
            expiredOldPlanRefused = true, newEpochGreater = true, committedReceiptRecoveredWithoutSecondEffect = true,
            successfulSubscriptionWrites = 2, durableOwnershipAuditRows = 2, durableCommitReceipts = 2,
            duplicateEffectWrites = 0, oidcAssertionsInThisStorageScenario = false,
        }, token);
    }

    [PostgreSqlRecoveryFact]
    public async Task Real_stale_physical_backup_requires_outside_quarantine_generation_admission_and_refuses_old_plan_replay()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(6)); var token = deadline.Token;
        await using var lab = await PostgreSqlRecoveryScenario.CreateAsync(token);
        var e1 = Guid.NewGuid(); var e2 = Guid.NewGuid();
        var originalAuthority = await Anchor(lab, e1, "active", token);
        var primary = await Context.Create(lab, "primary", e1, originalAuthority, initialize: true, token);
        var historical = await Plan(primary, "committed-before-backup", 300, token);
        var historicalReceipt = await Apply(primary, historical, token); Assert.NotNull(historicalReceipt);
        var stale = await Plan(primary, "accepted-after-backup", 300, token);
        await lab.ActionAsync("Backup", token);
        using (var backup = await lab.ReadEvidenceAsync("latest-backup.json", token))
            Assert.False(File.Exists(Path.Combine(backup.RootElement.GetProperty("backupDirectory").GetString()!, "w68-outside-authority.json")));
        Assert.NotNull(await Apply(primary, stale, token));
        Assert.Equal(2L, await Count(primary, "kafdeck_declarative_commit_receipts", token));
        var quarantined = await Anchor(lab, e2, "quarantined", token);
        Assert.False(DeclarativeRecoveryAnchor.Allows(originalAuthority));
        Assert.False(DeclarativeRecoveryAnchor.Allows(quarantined));
        Assert.True(DeclarativeRecoveryAnchor.Allows(quarantined, maintenance: true));
        // G2's native writable-maintenance fence has its own exact closed
        // three-field anchor. Both files are outside physical backup data.
        var nativeAnchor = await lab.WriteAnchorAsync(e2, "quarantined", token);
        await lab.ActionAsync("Restore", token);
        await lab.ActionAsync("FenceOriginalNodes", token);
        using var fence = await lab.ReadEvidenceAsync("original-nodes-fenced.json", token);
        Assert.True(fence.RootElement.GetProperty("bothOriginalPortsClosed").GetBoolean());
        var restored = await Context.Create(lab, "restore", e2, quarantined, initialize: false, token);
        Assert.Equal(e1, await restored.Store.GetDeploymentGenerationAsync(W68G3PersistenceFixture.Deployment, token));
        Assert.NotNull(await restored.Store.GetPlanAsync(stale.PlanId, token));
        Assert.Null(await restored.Store.GetCommitReceiptAsync(stale.PlanId, token));
        Assert.Equal(historicalReceipt, await restored.Store.GetCommitReceiptAsync(historical.PlanId, token));
        Assert.Null(await restored.Routing.GetSubscriptionAsync(stale.Key.ResourceId, token));
        Assert.Null(await Apply(restored, stale, token));
        await lab.ActionAsync("AdmitRestoreMaintenance", token, e2, nativeAnchor);
        await AssertWritable(restored, token); // refusal cannot be a read-only-DB false positive
        var prematurelyActive = await Anchor(lab, e2, "active", token);
        var mismatch = restored with { Authority = prematurelyActive };
        Assert.True(DeclarativeRecoveryAnchor.Allows(prematurelyActive));
        Assert.Null(await mismatch.Store.AcquireLeaseAsync(W68G3PersistenceFixture.Deployment,
            W68RuntimeFingerprints.LeaseScope("fresh-after-restore"), W68G3PersistenceFixture.Controller, W68G3PersistenceFixture.Owner,
            W68G3PersistenceFixture.Principal, Guid.NewGuid().ToString("N"), 30,
            _ => Task.FromResult(DeclarativeRecoveryAnchor.Allows(prematurelyActive)), token));
        Assert.Null(await Apply(mismatch, stale, token));
        await Assert.ThrowsAsync<InvalidOperationException>(() => mismatch.Store.InitializeAsync(token));
        var maintenanceAuthority = await Anchor(lab, e2, "quarantined", token);
        Assert.True(DeclarativeRecoveryAnchor.Allows(maintenanceAuthority, maintenance: true));
        var fenceHash = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(fence.RootElement))).ToLowerInvariant();
        await W68RuntimeSchema.AdmitGenerationAsync(restored.Connections, W68G3PersistenceFixture.Deployment, e1, e2, fenceHash, token);
        var active = await Anchor(lab, e2, "active", token);
        var resumed = restored with { Authority = active };
        Assert.Null(await Apply(resumed, stale, token)); // stored E1 plan stays immutable and cannot become E2
        Assert.True(historical.ExpiresAtUtc > await resumed.Store.ReadDatabaseUtcNowAsync(token));
        Assert.Null(await Apply(resumed, historical, token));
        Assert.Equal(historicalReceipt, await resumed.Store.GetCommitReceiptAsync(historical.PlanId, token));
        var oldConfiguration = new W68RuntimeStore(restored.Connections, e1);
        Assert.Null(await oldConfiguration.AcquireLeaseAsync(W68G3PersistenceFixture.Deployment,
            W68RuntimeFingerprints.LeaseScope("old-configuration"), W68G3PersistenceFixture.Controller, W68G3PersistenceFixture.Owner,
            W68G3PersistenceFixture.Principal, Guid.NewGuid().ToString("N"), 30, W68G3PersistenceFixture.Yes, token));
        Assert.Null(await new W68RuntimeStore(restored.Connections).AcquireLeaseAsync(W68G3PersistenceFixture.Deployment,
            W68RuntimeFingerprints.LeaseScope("unprotected-configuration"), W68G3PersistenceFixture.Controller, W68G3PersistenceFixture.Owner,
            W68G3PersistenceFixture.Principal, Guid.NewGuid().ToString("N"), 30, W68G3PersistenceFixture.Yes, token));
        var fresh = await Plan(resumed, "fresh-after-restore", 120, token);
        Assert.NotNull(await Apply(resumed, fresh, token));
        var reopened = await Context.Create(lab, "restore", e2, active, initialize: false, token);
        Assert.NotNull(await reopened.Store.GetCommitReceiptAsync(fresh.PlanId, token));
        Assert.Null(await reopened.Store.GetCommitReceiptAsync(stale.PlanId, token));
        Assert.Null(await reopened.Routing.GetSubscriptionAsync(stale.Key.ResourceId, token));
        Assert.Equal(2L, await Count(reopened, "kafdeck_declarative_commit_receipts", token));
        Assert.Equal(2L, await Count(reopened, "kafdeck_declarative_ownership_audit", token));
        await lab.WriteEvidenceAsync("g3-postgresql-physical-restore-fence-evidence.json", new
        {
            actualPhysicalBackupRestore = true, originallyCommittedAfterBackup = true,
            restoredOldPlanHasNoCommitReceipt = true, bothOriginalNodesPhysicallyFenced = true,
            outsideAnchorExcludedFromBackup = true, quarantineReadByProductionAnchorVerifier = true,
            writableRestoreVerified = true, prematurelyActiveAnchorStillDeniedByDurableGeneration = true,
            explicitOfflineGenerationAdmission = true, oldImmutablePlanRefusedAfterAdmission = true,
            oldConfigurationRefused = true, unprotectedConfigurationRefused = true,
            unexpiredOldGenerationCommitReceiptDeniedByApply = true, historicalReceiptRemainsReadOnlyAvailable = true,
            freshNewGenerationSubscriptionWrites = 1, oldPlanReplayWrites = 0, durableFreshCommitReceipts = 1,
            originalAcceptedOutcomeNotInventedFromRestoredMissingReceipt = true,
            restartedStoreRetainsFence = true, nativeFenceEvidenceSha256 = fenceHash,
            oidcAssertionsInThisStorageScenario = false,
        }, token);
    }

    private sealed record Context(INotificationDeliveryDbConnectionFactory Connections, Guid Generation, DeclarativeRuntimeOptions Authority)
    {
        internal W68RuntimeStore Store => new(Connections, Generation);
        internal AdoNotificationRoutingStore Routing => new(Connections);
        internal static async Task<Context> Create(PostgreSqlRecoveryScenario lab, string endpoint, Guid generation,
            DeclarativeRuntimeOptions authority, bool initialize, CancellationToken token)
        {
            var c = new Context(new PostgreSqlNotificationDeliveryDbConnectionFactory(await lab.ConnectionStringAsync(endpoint, token)), generation, authority);
            if (initialize)
            {
                await W68RuntimeSchema.InstallOrUpgradeAsync(c.Connections, token);
                await c.Routing.InitializeAsync(token);
                await W68RuntimeSchema.AdmitGenerationAsync(c.Connections, W68G3PersistenceFixture.Deployment, null, generation, W68G3PersistenceFixture.R1, token);
                foreach (var row in new[]
                {
                    new W68RuntimeRegistration(W68RuntimeRegistrationKind.Owner,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Owner,Guid.NewGuid(),0,W68G3PersistenceFixture.Owner,"","","","",true),
                    new W68RuntimeRegistration(W68RuntimeRegistrationKind.Source,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Source,Guid.NewGuid(),0,W68G3PersistenceFixture.Owner,"","reviewed-repository","notifications.yaml",W68G3PersistenceFixture.R1,true),
                    new W68RuntimeRegistration(W68RuntimeRegistrationKind.Controller,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Controller,Guid.NewGuid(),0,W68G3PersistenceFixture.Owner,W68G3PersistenceFixture.Principal,"","","",true),
                }) Assert.NotNull(await c.Store.UpsertRegistrationAsync(row,0,_=>Task.FromResult(DeclarativeRecoveryAnchor.Allows(authority)),token));
            }
            await W68RuntimeSchema.VerifyAsync(c.Connections, token);
            if (initialize) await c.Store.InitializeAsync(token);
            return c;
        }
    }
    private static async Task<W68StoredRuntimePlan> Plan(Context c, string id, int ttl, CancellationToken token)
    {
        var operation = Guid.NewGuid();
        var lease = await c.Store.AcquireLeaseAsync(W68G3PersistenceFixture.Deployment,W68RuntimeFingerprints.LeaseScope(id),
            W68G3PersistenceFixture.Controller,W68G3PersistenceFixture.Owner,W68G3PersistenceFixture.Principal,operation.ToString("N"),ttl,
            _=>Task.FromResult(DeclarativeRecoveryAnchor.Allows(c.Authority)),token); Assert.NotNull(lease);
        var source = await c.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Source,token);Assert.NotNull(source);
        var desired = new NotificationSubscriptionDefinition(id,"reviewed-destination",[NotificationEventClass.Operational]);
        var canonical = JsonSerializer.Serialize(new{id,destination="reviewed-destination",state="Active"});
        var plan = new W68StoredRuntimePlan(operation,W68RuntimePlanCommand.Apply,
            new(W68G3PersistenceFixture.Deployment,"Deployment",W68G3PersistenceFixture.Deployment,"NotificationSubscription",id),
            W68G3PersistenceFixture.Owner,W68G3PersistenceFixture.Source,source.GenerationId,source.PinnedRevision,W68G3PersistenceFixture.Principal,
            0,null,null,"missing",null,desired,NotificationSubscriptionState.Active,canonical,W68RuntimeFingerprints.Desired(canonical),
            W68G3PersistenceFixture.R1,W68G3PersistenceFixture.Controller,lease.LeaseScopeId,lease.HolderId,lease.Epoch,lease.ExpiresAtUtc.AddMilliseconds(-100),"",
            null,c.Generation,source.Revision);
        plan=plan with{PlanFingerprint=W68RuntimeFingerprints.Plan(plan)};
        Assert.NotNull(await c.Store.SavePlanAsync(plan,(_,_)=>Task.FromResult(DeclarativeRecoveryAnchor.Allows(c.Authority)),token));
        return plan;
    }
    private static Task<W68RuntimeCommitReceipt?> Apply(Context c,W68StoredRuntimePlan p,CancellationToken token)=>
        c.Store.ApplySubscriptionAsync(p.PlanId,p.PlanFingerprint,W68G3PersistenceFixture.Principal,(_,_)=>Task.FromResult(DeclarativeRecoveryAnchor.Allows(c.Authority)),token);
    private static async Task<DeclarativeRuntimeOptions> Anchor(PostgreSqlRecoveryScenario lab,Guid generation,string state,CancellationToken token)
    {
        var path=Path.Combine(lab.DirectoryPath,"w68-outside-authority.json");
        var bytes=JsonSerializer.SerializeToUtf8Bytes(new{version=1,deploymentId=W68G3PersistenceFixture.Deployment,generation=generation.ToString("D"),state});
        await File.WriteAllBytesAsync(path,bytes,token);
        return new(){Enabled=true,DeploymentId=W68G3PersistenceFixture.Deployment,DeploymentGeneration=generation,
            RecoveryAnchorFile=path,RecoveryAnchorSha256=Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant()};
    }
    private static async Task<long> Count(Context c,string table,CancellationToken token)
    {
        await using var db=await c.Connections.OpenAsync(token);await using var cmd=db.CreateCommand();cmd.CommandText="SELECT COUNT(*) FROM "+table;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(token),CultureInfo.InvariantCulture);
    }
    private static async Task<long> Epoch(Context c,string scope,CancellationToken token)
    {
        await using var db=await c.Connections.OpenAsync(token);await using var cmd=db.CreateCommand();
        cmd.CommandText="SELECT epoch FROM kafdeck_declarative_controller_leases WHERE deployment_id=@d AND lease_scope_id=@s";
        var d=cmd.CreateParameter();d.ParameterName="@d";d.Value=W68G3PersistenceFixture.Deployment;cmd.Parameters.Add(d);
        var s=cmd.CreateParameter();s.ParameterName="@s";s.Value=scope;cmd.Parameters.Add(s);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(token),CultureInfo.InvariantCulture);
    }
    private static async Task AssertWritable(Context c,CancellationToken token)
    {
        await using var db=await c.Connections.OpenAsync(token);await using var tx=await db.BeginTransactionAsync(token);await using var cmd=db.CreateCommand();cmd.Transaction=tx;
        cmd.CommandText="UPDATE kafdeck_declarative_schema_migrations SET manifest_id=manifest_id WHERE version=2";
        Assert.Equal(1,await cmd.ExecuteNonQueryAsync(token));await tx.RollbackAsync(token);
    }
}
