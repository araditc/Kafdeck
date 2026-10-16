using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

public sealed class PostgreSqlRecoveryFactAttribute : FactAttribute
{
    public PostgreSqlRecoveryFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_G2_RECOVERY_ROOT")))
            Skip = "Requires real primary/standby/restore PostgreSQL tools and an explicit isolated KAFDECK_G2_RECOVERY_ROOT; no synthetic crash/failover proof.";
    }
}

[CollectionDefinition("Native notification recovery", DisableParallelization = true)]
public sealed class NativeNotificationRecoveryCollection { }

[Collection("Native notification recovery")]
public sealed class V08G2PostgreSqlRecoveryTests(ITestOutputHelper output)
{
    [PostgreSqlRecoveryFact]
    public async Task Abrupt_sender_crash_after_replicated_claim_and_complete_POST_survives_real_primary_promotion_without_resend()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var lab = await PostgreSqlRecoveryScenario.CreateAsync(token);
        await using var peer = new NotificationRecoveryTlsPeer(holdReceipt: true);
        peer.Start();
        var id = await lab.SeedAsync(peer, token);
        var anchor = await lab.AnchorHashAsync(token);
        var crashed = await lab.StartSenderAsync("primary", peer, anchor, token);
        await crashed.WaitReadyAsync(token);
        Assert.Equal(id, await peer.CompletePost.Task.WaitAsync(token));
        var replicated = await WaitAsync(lab, "standby", id, NotificationDeliveryState.InFlight, token);
        Assert.Equal(1, replicated.Snapshot.AttemptCount);
        await crashed.KillAbruptlyAsync(token);
        await lab.ActionAsync("Failover", token);
        using var promotion = await lab.ReadEvidenceAsync("promotion-evidence.json", token);
        Assert.True(promotion.RootElement.GetProperty("oldPrimaryFenced").GetBoolean());
        Assert.True(promotion.RootElement.GetProperty("oldPrimaryPortClosed").GetBoolean());
        Assert.False(promotion.RootElement.GetProperty("inRecovery").GetBoolean());
        var survivor = await lab.StartSenderAsync("standby", peer, anchor, token);
        await survivor.WaitReadyAsync(token);
        var unknown = await WaitAsync(lab, "standby", id, NotificationDeliveryState.UnknownExternalEffect, token);
        var elapsed = unknown.UpdatedAtUtc - replicated.UpdatedAtUtc;
        Assert.True(elapsed >= TimeSpan.FromSeconds(30), "Recovery must wait for actual PostgreSQL stale time.");
        await Task.Delay(1200, token);
        var durable = await (await lab.StoreAsync("standby", token)).GetAsync(id, "recovery-webhook", token);
        Assert.NotNull(durable);
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, durable.Snapshot.State);
        Assert.Equal(1, durable.Snapshot.AttemptCount);
        Assert.Equal(1, peer.PhysicalPosts);
        Assert.Equal(1, peer.AcceptedConnections);
        Assert.False(peer.InvalidWire);
        await lab.WriteEvidenceAsync("postgres-crash-after-post-failover-evidence.json", new
        {
            actualPhysicalTls = true, independentReleaseSenderProcess = true, abruptProcessKill = true,
            inFlightClaimReadOnPhysicalStandbyBeforeKill = true, completePostReadBeforeKill = true,
            oldPrimaryPhysicallyFenced = true, physicalStandbyPromoted = true, syntheticClockAging = false,
            realDatabaseStaleMilliseconds = (long)elapsed.TotalMilliseconds, physicalPosts = peer.PhysicalPosts,
            acceptedTlsConnections = peer.AcceptedConnections, attempts = durable.Snapshot.AttemptCount,
            terminalState = durable.Snapshot.State.ToString(), automaticResends = 0, durableReopen = true,
            executionMode = "HighAvailability", explicitInterlockRequired = true, automaticFailover = false,
            defaultEnabled = false, coversStaleBackupRestore = false,
        }, token);
        output.WriteLine($"REAL_PG_ABRUPT_CRASH_PROMOTION replicatedInFlight=true; senderProcessKilled=true; oldPrimaryFenced=true; standbyPromoted=true; physicalPOSTs=1; attempts=1; ledger=UnknownExternalEffect; realDBStaleMs={(long)elapsed.TotalMilliseconds}; blindResends=0");
    }

    [PostgreSqlRecoveryFact]
    public async Task Real_primary_failover_before_any_claim_delivers_exactly_once_on_promoted_node()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var lab = await PostgreSqlRecoveryScenario.CreateAsync(token);
        await using var peer = new NotificationRecoveryTlsPeer(holdReceipt: false);
        peer.Start();
        var id = await lab.SeedAsync(peer, token);
        var pending = await WaitAsync(lab, "standby", id, NotificationDeliveryState.Pending, token);
        Assert.Equal(0, pending.Snapshot.AttemptCount);
        Assert.Equal(0, peer.PhysicalPosts);
        await lab.ActionAsync("Failover", token);
        using var promotion = await lab.ReadEvidenceAsync("promotion-evidence.json", token);
        Assert.True(promotion.RootElement.GetProperty("oldPrimaryFenced").GetBoolean());
        Assert.True(promotion.RootElement.GetProperty("oldPrimaryPortClosed").GetBoolean());
        var sender = await lab.StartSenderAsync("standby", peer, await lab.AnchorHashAsync(token), token);
        await sender.WaitReadyAsync(token);
        var delivered = await WaitAsync(lab, "standby", id, NotificationDeliveryState.Delivered, token);
        Assert.Equal(id, await peer.CompletePost.Task.WaitAsync(token));
        await Task.Delay(1200, token);
        var durable = await (await lab.StoreAsync("standby", token)).GetAsync(id, "recovery-webhook", token);
        Assert.NotNull(durable);
        Assert.Equal(delivered.Revision, durable.Revision);
        Assert.Equal(NotificationDeliveryState.Delivered, durable.Snapshot.State);
        Assert.Equal(1, durable.Snapshot.AttemptCount);
        Assert.Equal(1, peer.PhysicalPosts);
        Assert.Equal(1, peer.AcceptedConnections);
        Assert.False(peer.InvalidWire);
        await lab.WriteEvidenceAsync("postgres-before-claim-failover-evidence.json", new
        {
            actualPhysicalTls = true, independentReleaseSenderProcess = true, failoverBeforeClaim = true,
            replicatedPendingAttempts = 0, oldPrimaryPhysicallyFenced = true, physicalStandbyPromoted = true,
            physicalPosts = peer.PhysicalPosts, attempts = durable.Snapshot.AttemptCount,
            terminalState = durable.Snapshot.State.ToString(), durableReopen = true, automaticResends = 0,
            executionMode = "HighAvailability", explicitInterlockRequired = true, automaticFailover = false, defaultEnabled = false,
        }, token);
        output.WriteLine("REAL_PG_BEFORE_CLAIM_PROMOTION replicatedPending=true; oldPrimaryFenced=true; standbyPromoted=true; physicalPOSTs=1; HTTP204=1; attempts=1; ledger=Delivered; blindResends=0");
    }

    [PostgreSqlRecoveryFact]
    public async Task Physical_stale_Pending_backup_restore_cannot_replay_with_new_active_anchor_or_old_configuration()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var lab = await PostgreSqlRecoveryScenario.CreateAsync(token);
        await using var peer = new NotificationRecoveryTlsPeer(holdReceipt: false);
        peer.Start();
        var id = await lab.SeedAsync(peer, token);
        var originalHash = await lab.AnchorHashAsync(token);
        await lab.ActionAsync("Backup", token);
        var sender = await lab.StartSenderAsync("primary", peer, originalHash, token);
        await sender.WaitReadyAsync(token);
        await WaitAsync(lab, "primary", id, NotificationDeliveryState.Delivered, token, peer);
        Assert.Equal(id, await peer.CompletePost.Task.WaitAsync(token));
        await sender.KillAbruptlyAsync(token);
        var nextGeneration = Guid.NewGuid();
        var quarantinedHash = await lab.WriteAnchorAsync(nextGeneration, "quarantined", token);
        await lab.ActionAsync("Restore", token);
        var restored = await WaitAsync(lab, "restore", id, NotificationDeliveryState.Pending, token);
        Assert.Equal(0, restored.Snapshot.AttemptCount);
        var reader = await lab.GenerationReaderAsync("restore", token);
        Assert.Equal(lab.InitialGeneration, await reader.ReadAsync(token));
        using var restoreEvidence = await lab.ReadEvidenceAsync("restore-evidence.json", token);
        Assert.True(restoreEvidence.RootElement.GetProperty("physicalBackupVerified").GetBoolean());
        Assert.True(restoreEvidence.RootElement.GetProperty("quarantined").GetBoolean());

        // Explicit gate calls establish the refusal reason independently of the
        // restored server's read-only DDL defense. No status read advances epoch.
        await Assert.ThrowsAsync<KafdeckConfigurationException>(() =>
            new ConfiguredNotificationDeliveryInterlock(lab.AnchorPath, originalHash, reader).VerifyAsync(token).AsTask());
        var oldConfiguration = await lab.StartSenderAsync("restore", peer, originalHash, token);
        await oldConfiguration.AssertRefusedAsync(token);
        await Assert.ThrowsAsync<KafdeckConfigurationException>(() =>
            new ConfiguredNotificationDeliveryInterlock(lab.AnchorPath, quarantinedHash, reader).VerifyAsync(token).AsTask());
        var quarantined = await lab.StartSenderAsync("restore", peer, quarantinedHash, token);
        await quarantined.AssertRefusedAsync(token);
        var activeHash = await lab.WriteAnchorAsync(nextGeneration, "active", token);
        await Assert.ThrowsAsync<KafdeckConfigurationException>(() =>
            new ConfiguredNotificationDeliveryInterlock(lab.AnchorPath, activeHash, reader).VerifyAsync(token).AsTask());
        var attemptedBypass = await lab.StartSenderAsync("restore", peer, activeHash, token);
        await attemptedBypass.AssertRefusedAsync(token);
        Assert.Equal(lab.InitialGeneration, await reader.ReadAsync(token));
        var unchanged = await (await lab.StoreAsync("restore", token)).GetAsync(id, "recovery-webhook", token);
        Assert.NotNull(unchanged);
        Assert.Equal(restored.Revision, unchanged.Revision);
        Assert.Equal(NotificationDeliveryState.Pending, unchanged.Snapshot.State);
        Assert.Equal(0, unchanged.Snapshot.AttemptCount);
        Assert.Equal(1, peer.PhysicalPosts);
        Assert.Equal(1, peer.AcceptedConnections);
        Assert.False(peer.InvalidWire);
        await lab.WriteEvidenceAsync("postgres-stale-backup-quarantine-evidence.json", new
        {
            actualVerifiedPhysicalBackupRestore = true, originalProviderAccepted = true,
            backupState = "Pending", originalLaterState = "Delivered", restoredState = unchanged.Snapshot.State.ToString(),
            restoredAttempts = unchanged.Snapshot.AttemptCount, externalAnchorOutsideDatabaseBackup = true,
            oldConfigurationHashRefused = true, quarantinedGenerationRefused = true,
            activeNewHashStillRefusedByDatabaseEpoch = true, directProductionGateVerifiedBeforeStartup = true,
            databaseEpochUnchangedByReads = true, refusedReleaseSenderProcesses = 3,
            physicalPosts = peer.PhysicalPosts, automaticResends = 0,
            safeResumeExercisedByThisCase = false, offlineReconciliationExercisedByThisCase = false,
            executionMode = "HighAvailability", explicitInterlockRequired = true, automaticFailover = false, defaultEnabled = false,
        }, token);
        output.WriteLine("REAL_PG_STALE_BACKUP_QUARANTINE physicalBackupVerified=true; originalDelivered=true; restoredPending=true; restoredAttempts=0; oldHashRefused=true; activeNewHashEpochMismatch=true; senderProcessesRefused=3; physicalPOSTs=1; blindResends=0; safeResumeAccepted=false");
    }

    private async Task<NotificationDeliveryRecord> WaitAsync(PostgreSqlRecoveryScenario lab, string endpoint,
        Guid id, NotificationDeliveryState state, CancellationToken token, NotificationRecoveryTlsPeer? peer = null)
    {
        var store = await lab.StoreAsync(endpoint, token);
        while (true)
        {
            var row = await store.GetAsync(id, "recovery-webhook", token);
            if (row?.Snapshot.State == state) return row;
            if (row?.Snapshot.State is NotificationDeliveryState.UnknownExternalEffect or NotificationDeliveryState.Exhausted)
            {
                if (peer is not null) output.WriteLine($"SAFE_RECOVERY_FAILURE state={row.Snapshot.State}; attempts={row.Snapshot.AttemptCount}; TLS={peer.AcceptedConnections}; POSTs={peer.PhysicalPosts}; invalidWire={peer.InvalidWire}");
                throw new InvalidOperationException("Recovery sender reached unexpected terminal state: " + row.Snapshot.State);
            }
            await Task.Delay(25, token);
        }
    }
}
