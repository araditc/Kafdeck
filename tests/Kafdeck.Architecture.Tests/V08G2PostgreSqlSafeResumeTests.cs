using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

[Collection("Native notification recovery")]
public sealed class V08G2PostgreSqlSafeResumeTests(ITestOutputHelper output)
{
    [PostgreSqlRecoveryFact]
    public async Task Actual_stale_restore_is_governed_then_fresh_product_source_resumes_without_old_ledger_or_absent_ledger_replay()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(6));
        var token = deadline.Token;
        await using var lab = await PostgreSqlRecoveryScenario.CreateAsync(token);
        await using var peer = new NotificationRecoveryTlsPeer(holdReceipt: false);
        peer.Start();
        var oldLedgerId = await lab.SeedAsync(peer, token);
        var oldSourceId = await CaptureProductAsync(await lab.ConnectionStringAsync("primary", token), token);
        var futureOldSourceId = await CaptureProductAsync(await lab.ConnectionStringAsync("primary", token), token, futureClock: true);
        var originalHash = await lab.AnchorHashAsync(token);
        // Genuine product outbox row exists in the backup, but never had a delivery row.
        Assert.Null(await (await lab.StoreAsync("primary", token)).GetAsync(oldSourceId, "recovery-webhook", token));
        await lab.ActionAsync("Backup", token);
        var original = await lab.StartSenderAsync("primary", peer, originalHash, token);
        await original.WaitReadyAsync(token);
        await WaitStateAsync(lab, oldLedgerId, "primary", NotificationDeliveryState.Delivered, token);
        Assert.Equal(oldLedgerId, await peer.CompletePost.Task.WaitAsync(token));
        await original.KillAbruptlyAsync(token);
        var e2 = Guid.NewGuid();
        var quarantine = await lab.WriteAnchorAsync(e2, "quarantined", token);
        await lab.ActionAsync("Restore", token);
        await lab.ActionAsync("FenceOriginalNodes", token);
        await lab.ActionAsync("AdmitRestoreMaintenance", token, e2, quarantine);
        using var fence = await lab.ReadEvidenceAsync("original-nodes-fenced.json", token);
        using var maintenance = await lab.ReadEvidenceAsync("restore-maintenance-evidence.json", token);
        Assert.True(fence.RootElement.GetProperty("bothOriginalPortsClosed").GetBoolean());
        Assert.Equal("quarantined", maintenance.RootElement.GetProperty("outsideAuthorityState").GetString());
        Assert.False(maintenance.RootElement.GetProperty("automaticSenderActivationAllowed").GetBoolean());
        var connections = new PostgreSqlNotificationDeliveryDbConnectionFactory(await lab.ConnectionStringAsync("restore", token));
        var recovery = new AdoNotificationRecoveryStore(connections, new NotificationRecoveryAuthority(new(e2, lab.AnchorPath, quarantine)),
            AdoNotificationRecoverySourceInventory.ForPostgreSql(await lab.ConnectionStringAsync("restore", token)));
        await recovery.InitializeAsync(token);
        var restored = Assert.IsType<NotificationDeliveryRecord>(await (await lab.StoreAsync("restore", token)).GetAsync(oldLedgerId, "recovery-webhook", token));
        Assert.Equal(NotificationDeliveryState.Pending, restored.Snapshot.State);
        Assert.Equal(0, restored.Snapshot.AttemptCount);
        var proof = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new { providerAcceptedEvent = oldLedgerId, physicalPosts = 1 }))).ToLowerInvariant();
        var fenceProof = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(fence.RootElement))).ToLowerInvariant();
        NotificationRecoveryRequest Request(NotificationRecoveryCommand command) => new(command,
            command == NotificationRecoveryCommand.FenceBatch ? NotificationRecoveryDisposition.ExternalAcceptanceObserved :
                command == NotificationRecoveryCommand.FenceSourceBatch ? NotificationRecoveryDisposition.UnresolvedRiskAccepted : null,
            proof, fenceProof, lab.InitialGeneration, e2, quarantine);
        await Assert.ThrowsAsync<InvalidOperationException>(() => recovery.PreviewAsync(Request(NotificationRecoveryCommand.AdvanceGeneration), "requester", A, B, token));
        await ApplyAsync(recovery, Request(NotificationRecoveryCommand.FenceBatch), token);
        var fenced = Assert.IsType<NotificationDeliveryRecord>(await (await lab.StoreAsync("restore", token)).GetAsync(oldLedgerId, "recovery-webhook", token));
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, fenced.Snapshot.State);
        Assert.Equal(0, fenced.Snapshot.AttemptCount);
        Assert.Null(fenced.Snapshot.ProviderReceiptFingerprint);
        await ApplyAsync(recovery, Request(NotificationRecoveryCommand.FenceSourceBatch), token);
        var advance = await ApplyAsync(recovery, Request(NotificationRecoveryCommand.AdvanceGeneration), token);
        Assert.True(advance.ReadyForOperatorActivation);
        Assert.Equal(e2, await (await lab.GenerationReaderAsync("restore", token)).ReadAsync(token));
        var blocked = await lab.StartSenderAsync("restore", peer, quarantine, token);
        await blocked.AssertRefusedAsync(token);
        var oldConfig = await lab.StartSenderAsync("restore", peer, originalHash, token);
        await oldConfig.AssertRefusedAsync(token);
        var activeHash = await lab.WriteAnchorAsync(e2, "active", token);
        // Recreate a sender only after the outside authority is explicitly activated.
        var resumed = await lab.StartSenderAsync("restore", peer, activeHash, token);
        await resumed.WaitReadyAsync(token);
        // The real restored source contains an application-clock future event.
        // Its explicit reviewed ID fence refuses even without capture provenance.
        Assert.False(await recovery.CheckAsync(futureOldSourceId, "recovery-webhook", advance.HistoricalCutoffUtc.AddYears(1), token));
        await RouteProductAsync(await lab.ConnectionStringAsync("restore", token), connections, peer, token);
        var oldSource = await WaitStateAsync(lab, oldSourceId, "restore", NotificationDeliveryState.UnknownExternalEffect, token);
        Assert.Equal(1, oldSource.Snapshot.AttemptCount);
        Assert.Equal(1, peer.PhysicalPosts);
        await resumed.KillAbruptlyAsync(token);
        // Independent process restart must retain both cutoff and suppression.
        var restarted = await lab.StartSenderAsync("restore", peer, activeHash, token);
        await restarted.WaitReadyAsync(token);
        var freshId = await CaptureProductAsync(await lab.ConnectionStringAsync("restore", token), token);
        await RouteProductAsync(await lab.ConnectionStringAsync("restore", token), connections, peer, token);
        var fresh = await WaitStateAsync(lab, freshId, "restore", NotificationDeliveryState.Delivered, token);
        await Task.Delay(1200, token);
        Assert.Equal(1, fresh.Snapshot.AttemptCount);
        Assert.Null(fresh.Snapshot.ProviderReceiptFingerprint); // HTTP204 has no typed provider receipt identifier.
        Assert.Equal(2, peer.PhysicalPosts);
        Assert.Equal(2, peer.AcceptedConnections);
        Assert.DoesNotContain(oldSourceId, peer.PostedIds);
        Assert.DoesNotContain(futureOldSourceId, peer.PostedIds);
        Assert.Single(peer.PostedIds, id => id == oldLedgerId);
        Assert.Single(peer.PostedIds, id => id == freshId);
        Assert.False(peer.InvalidWire);
        await using var c = await connections.OpenAsync(token);
        await using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM notification_recovery_source_suppressions WHERE disposition='SuppressedHistoricalSource'";
        Assert.Equal(2, Convert.ToInt64(await cmd.ExecuteScalarAsync(token)));
        var reopened = await new AdoNotificationRecoveryStore(connections).StatusAsync(token);
        Assert.Equal(advance.HistoricalCutoffUtc, reopened.HistoricalCutoffUtc);
        await lab.WriteEvidenceAsync("postgres-governed-restore-safe-resume-evidence.json", new
        {
            actualPhysicalBackupRestore = true, independentHaSenderProcesses = true, originalNodesPhysicallyFenced = true,
            restoredPendingOriginallyAccepted = true, originalRestoredIntentState = fenced.Snapshot.State.ToString(),
            originalRestoredAttemptCount = fenced.Snapshot.AttemptCount, fabricatedDeliveredReceipts = 0,
            distinctHumanProtocolExercisedAtStore = true, realOidcApprovalCoveredSeparately = false,
            externalAuthorityQuarantinedDuringReview = true, databaseEpochAdvancedAtomically = true,
            reviewedHistoricalCutoffUtc = reopened.HistoricalCutoffUtc, oldConfigurationRefused = true,
            quarantinedConfigurationRefused = true, absentLedgerHistoricalProductSourceSuppressed = true,
            futureApplicationClockHistoricalSourceExplicitlyFenced = true, sourceInventoryWasReadOnly = true,
            newPostCutoffDurableProductTransitionDelivered = true, persistedCutoffAfterProcessRestart = true,
            freshLedgerOutcome = "Delivered", acknowledgement = "HTTP204", providerReceiptIdProvided = false,
            originalPhysicalPosts = 1, freshPhysicalPosts = 1, oldSourcePhysicalPosts = 0,
            totalPhysicalPosts = peer.PhysicalPosts, totalTlsConnections = peer.AcceptedConnections,
            automaticResends = 0, nativeFenceEvidenceSha256 = fenceProof,
        }, token);
        output.WriteLine("REAL_PG_GOVERNED_RESTORE_RESUME originalPOST=1; restoredPendingSuppressed=true; noLedgerHistoricalSourcePOST=0; freshDurableProductPOST=1; restartCutoff=true; outsideAuthority=E2; oldNodesFenced=true");
    }

    internal static async Task<Guid> CaptureProductAsync(string connectionString, CancellationToken token, bool futureClock = false)
        => await CaptureProductAsync(new PostgreSqlMutationDbConnectionFactory(connectionString), token, futureClock);

    internal static async Task<Guid> CaptureProductAsync(IMutationDbConnectionFactory factory, CancellationToken token, bool futureClock = false)
    {
        var repository = new AdoMutationOperationRepository(factory, captureProductNotifications: true);
        await repository.InitializeAsync(token);
        var before = (await new MutationNotificationOutbox(factory).ReadPendingAsync(50, token)).Select(e => e.EventId).ToHashSet();
        var now = DateTimeOffset.UtcNow;
        var intent = new MutationIntentDescriptor(MutationOperationKind.TopicCreate, "recovery-lab", "{\"topicName\":\"reviewed-product\"}",
            ["cluster/recovery-lab/topic/reviewed-product"], [new MutationPrecondition("topic", "absent")]);
        var operation = MutationOperation.CreatePreview("oidc:https://idp.example|operator", intent,
            MutationRiskClassifier.Classify(new(MutationOperationKind.TopicCreate)), "recovery-product-fixture", now.AddMinutes(5), now, Guid.NewGuid().ToString("N"));
        operation.OpenForConfirmation(now); operation.Confirm(operation.Snapshot.RequesterPrincipalId, operation.Snapshot.PreviewHash, now);
        operation.ClaimExecution(now, now.AddMinutes(2)); operation.MarkDispatchStarted(now);
        await repository.CreateAsync(operation.Snapshot, token);
        var version = operation.Snapshot.Version;
        operation.Complete(MutationExecutionResultKind.AppliedVerified, "verified-fixture-product-transition", DateTimeOffset.UtcNow.AddHours(futureClock ? 1 : 0));
        Assert.Equal(MutationSaveOutcome.Saved, (await repository.TrySaveAsync(operation.Snapshot, version, token)).Outcome);
        var events = await new MutationNotificationOutbox(factory).ReadPendingAsync(50, token);
        return events.Single(e => !before.Contains(e.EventId)).EventId;
    }

    private static async Task RouteProductAsync(string connectionString, INotificationDeliveryDbConnectionFactory connections,
        NotificationRecoveryTlsPeer peer, CancellationToken token)
    {
        var routing = new AdoNotificationRoutingStore(connections); await routing.InitializeAsync(token);
        var deliveries = new AdoNotificationDeliveryStore(connections);
        var profile = new NotificationDestinationProfile("recovery-webhook", NotificationProviderKind.Webhook, "Recovery fixture",
            [NotificationEventClass.Operational, NotificationEventClass.Mutation], new Uri(peer.Endpoint));
        if (await routing.GetSubscriptionAsync("resumed-product", token) is null)
            await routing.CreateSubscriptionAsync(new("resumed-product", profile.DestinationId, [NotificationEventClass.Mutation]),
                NotificationSubscriptionState.Active, DateTimeOffset.UtcNow, token);
        var coordinator = new NotificationRoutingCoordinator(routing, deliveries, new ConfiguredNotificationDestinationProfileCatalog([profile]));
        var policy = new NotificationProductEventPolicy();
        var pump = new NotificationProductEventPump(new MutationNotificationOutbox(new PostgreSqlMutationDbConnectionFactory(connectionString)),
            coordinator, policy, new NotificationProductEventBackpressure(connections, policy),
            recoverySourceEvidence: new AdoNotificationRecoveryStore(connections));
        Assert.Equal(1, (await pump.RunCycleAsync(token)).Acknowledged);
    }
    private static async Task<NotificationDeliveryRecord> WaitStateAsync(PostgreSqlRecoveryScenario lab, Guid id, string endpoint,
        NotificationDeliveryState state, CancellationToken token)
    {
        var store = await lab.StoreAsync(endpoint, token);
        while (true)
        {
            var row = await store.GetAsync(id, "recovery-webhook", token);
            if (row?.Snapshot.State == state) return row;
            if (row?.Snapshot.State is NotificationDeliveryState.UnknownExternalEffect or NotificationDeliveryState.Exhausted)
                throw new InvalidOperationException("Recovery sender reached unexpected terminal state: " + row.Snapshot.State);
            await Task.Delay(25, token);
        }
    }
    internal static async Task<NotificationRecoveryPlan> ApplyAsync(INotificationRecoveryStore store, NotificationRecoveryRequest request, CancellationToken token)
    {
        var plan = await store.PreviewAsync(request, "reviewed-requester", A, B, token);
        Assert.Null(await store.ApproveAsync(plan.OperationId, plan.Revision, plan.PreviewHash, "requester-new-session", A, B, token));
        plan = Assert.IsType<NotificationRecoveryPlan>(await store.ApproveAsync(plan.OperationId, plan.Revision, plan.PreviewHash, "independent-approver", C, B, token));
        return Assert.IsType<NotificationRecoveryPlan>(await store.ApplyAsync(plan.OperationId, plan.Revision, plan.PreviewHash, A, B, () => true, token));
    }
    private const string A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private const string B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    private const string C = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
}
