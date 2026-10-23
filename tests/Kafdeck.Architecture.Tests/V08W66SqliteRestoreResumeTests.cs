using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66SqliteRestoreResumeTests
{
    [Fact]
    public async Task Actual_separate_SQLite_backups_restore_then_governed_resume_suppresses_old_source_and_only_fresh_product_reaches_TLS_after_host_restart()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = timeout.Token;
        var directory = Path.Combine(Path.GetTempPath(), "kafdeck-sqlite-safe-resume-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            await using var peer = new NotificationRecoveryTlsPeer(false); peer.Start();
            var originalPath = Path.Combine(directory, "original.db");
            var originalSourcePath = Path.Combine(directory, "original-source.db");
            var restoredPath = Path.Combine(directory, "restored.db");
            var restoredSourcePath = Path.Combine(directory, "restored-source.db");
            var original = new SqliteNotificationDeliveryDbConnectionFactory(originalPath);
            var routing = new AdoNotificationRoutingStore(original); await routing.InitializeAsync(token);
            var deliveries = new AdoNotificationDeliveryStore(original); await deliveries.InitializeAsync(token);
            var e1 = Guid.NewGuid(); var e2 = Guid.NewGuid();
            await using (var c = await original.OpenAsync(token))
            await using (var cmd = c.CreateCommand())
            { cmd.CommandText = "CREATE TABLE notification_delivery_generation(id INTEGER PRIMARY KEY,generation TEXT NOT NULL); INSERT INTO notification_delivery_generation VALUES(1,'" + e1.ToString("D") + "')"; await cmd.ExecuteNonQueryAsync(token); }
            var profile = Profile(peer);
            var oldEvent = NotificationSafeEvent.Approved(Guid.NewGuid(), NotificationApprovedEventKind.ConsumerLagAlert, DateTimeOffset.UtcNow);
            await routing.CreateOrGetEventAsync(oldEvent, oldEvent.OccurredAtUtc, token);
            await deliveries.CreateOrGetAsync(new(oldEvent.EventId, profile.DestinationId, oldEvent.PayloadFingerprint,
                NotificationDeliveryState.Pending, 0, oldEvent.OccurredAtUtc, routedProfileRevisionFingerprint: profile.RevisionFingerprint), oldEvent.OccurredAtUtc, token);
            var source = new SqliteMutationDbConnectionFactory(originalSourcePath);
            var historicalSourceId = await V08G2PostgreSqlSafeResumeTests.CaptureProductAsync(source, token);
            await BackupAsync(originalPath, Path.Combine(directory, "backup.db"), token);
            await BackupAsync(originalSourcePath, Path.Combine(directory, "backup-source.db"), token);
            var anchor = Path.Combine(directory, "operator-anchor.json");
            var originalHash = await AnchorAsync(anchor, e1, "active", token);
            using (var initialHost = Host(originalPath, anchor, originalHash, peer))
            {
                await initialHost.StartAsync(token);
                await WaitAsync(deliveries, oldEvent.EventId, NotificationDeliveryState.Delivered, token);
                await initialHost.StopAsync(token);
            }
            Assert.Equal(1, peer.PhysicalPosts);
            var quarantine = await AnchorAsync(anchor, e2, "quarantined", token);
            File.Copy(Path.Combine(directory, "backup.db"), restoredPath);
            File.Copy(Path.Combine(directory, "backup-source.db"), restoredSourcePath);
            var restored = new SqliteNotificationDeliveryDbConnectionFactory(restoredPath);
            var restoredDeliveries = new AdoNotificationDeliveryStore(restored);
            Assert.Equal(NotificationDeliveryState.Pending, (await restoredDeliveries.GetAsync(oldEvent.EventId, profile.DestinationId, token))!.Snapshot.State);
            Assert.Null(await restoredDeliveries.GetAsync(historicalSourceId, profile.DestinationId, token));
            var recovery = new AdoNotificationRecoveryStore(restored, new NotificationRecoveryAuthority(new(e2, anchor, quarantine)),
                AdoNotificationRecoverySourceInventory.ForSqlite(restoredSourcePath));
            await recovery.InitializeAsync(token);
            NotificationRecoveryRequest Request(NotificationRecoveryCommand command) => new(command,
                command is NotificationRecoveryCommand.FenceBatch or NotificationRecoveryCommand.FenceSourceBatch ? NotificationRecoveryDisposition.UnresolvedRiskAccepted : null,
                A, B, e1, e2, quarantine);
            await V08G2PostgreSqlSafeResumeTests.ApplyAsync(recovery, Request(NotificationRecoveryCommand.FenceBatch), token);
            await V08G2PostgreSqlSafeResumeTests.ApplyAsync(recovery, Request(NotificationRecoveryCommand.FenceSourceBatch), token);
            var advance = await V08G2PostgreSqlSafeResumeTests.ApplyAsync(recovery, Request(NotificationRecoveryCommand.AdvanceGeneration), token);
            Assert.True(advance.ReadyForOperatorActivation);
            await Assert.ThrowsAsync<KafdeckConfigurationException>(() => new ConfiguredNotificationDeliveryInterlock(anchor, originalHash,
                new AdoNotificationDeliveryGenerationReader(original)).VerifyAsync(token).AsTask());
            await Assert.ThrowsAsync<KafdeckConfigurationException>(() => new ConfiguredNotificationDeliveryInterlock(anchor, quarantine,
                new AdoNotificationDeliveryGenerationReader(restored)).VerifyAsync(token).AsTask());
            var active = await AnchorAsync(anchor, e2, "active", token);
            using (var resumed = Host(restoredPath, anchor, active, peer))
            {
                await resumed.StartAsync(token);
                await PumpAsync(restored, restoredSourcePath, peer, token);
                await WaitAsync(restoredDeliveries, historicalSourceId, NotificationDeliveryState.UnknownExternalEffect, token);
                await resumed.StopAsync(token);
            }
            Assert.Equal(1, peer.PhysicalPosts);
            using (var restarted = Host(restoredPath, anchor, active, peer))
            {
                await restarted.StartAsync(token);
                var fresh = await V08G2PostgreSqlSafeResumeTests.CaptureProductAsync(new SqliteMutationDbConnectionFactory(restoredSourcePath), token);
                await PumpAsync(restored, restoredSourcePath, peer, token);
                var receipt = await WaitAsync(restoredDeliveries, fresh, NotificationDeliveryState.Delivered, token);
                Assert.Equal(1, receipt.Snapshot.AttemptCount);
                // Generic Webhook HTTP204 acknowledges delivery but supplies no typed provider receipt ID.
                Assert.Null(receipt.Snapshot.ProviderReceiptFingerprint);
                Assert.Single(peer.PostedIds, id => id == fresh);
                await restarted.StopAsync(token);
            }
            Assert.Equal(2, peer.PhysicalPosts);
            Assert.Equal(2, peer.AcceptedConnections);
            Assert.DoesNotContain(historicalSourceId, peer.PostedIds);
            Assert.Single(peer.PostedIds, id => id == oldEvent.EventId);
            Assert.False(peer.InvalidWire);
            var retained = (await restoredDeliveries.GetAsync(oldEvent.EventId, profile.DestinationId, token))!;
            Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, retained.Snapshot.State);
            Assert.Equal(0, retained.Snapshot.AttemptCount);
            Assert.Equal(advance.HistoricalCutoffUtc, (await new AdoNotificationRecoveryStore(restored).StatusAsync(token)).HistoricalCutoffUtc);
            var evidenceRoot = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY");
            if (!string.IsNullOrWhiteSpace(evidenceRoot))
            {
                Directory.CreateDirectory(evidenceRoot);
                await File.WriteAllBytesAsync(Path.Combine(evidenceRoot, "sqlite-governed-restore-safe-resume-evidence.json"), JsonSerializer.SerializeToUtf8Bytes(new
                { actualSeparateSqliteBackupAndRestore = true, originalAcceptedPost = 1, restoredIntentSuppressed = true,
                    historicalAbsentLedgerSourcePost = 0, freshProductPost = 1, totalTlsPosts = peer.PhysicalPosts,
                    persistedCutoffAfterNewHost = true, oldConfigurationRefused = true, automaticResends = 0,
                    freshLedgerOutcome = "Delivered", acknowledgement = "HTTP204", providerReceiptIdProvided = false,
                    realOidcApprovalCoveredSeparately = false }), token);
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
            Directory.Delete(directory);
        }
    }
    private static IHost Host(string database, string anchor, string fingerprint, NotificationRecoveryTlsPeer peer)
    {
        var b = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(); b.Logging.ClearProviders();
        var connections = new SqliteNotificationDeliveryDbConnectionFactory(database);
        var profile = Profile(peer); var network = new Network(peer.Port);
        var gate = new ConfiguredNotificationDeliveryInterlock(anchor, fingerprint, new AdoNotificationDeliveryGenerationReader(connections));
        b.Services.AddSingleton<X509Certificate2>(_ => X509CertificateLoader.LoadCertificate(peer.PublicCa));
        b.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(connections);
        b.Services.AddSingleton<INotificationDeliveryActivationGate>(gate);
        b.Services.AddSingleton<INotificationRoutingStore>(new AdoNotificationRoutingStore(connections));
        b.Services.AddSingleton<INotificationDeliveryStore>(new AdoNotificationDeliveryStore(connections));
        b.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([profile]));
        b.Services.AddSingleton<INotificationCredentialResolver>(new ConfiguredNotificationCredentialResolver([], new SecretResolver()));
        b.Services.AddSingleton<INotificationHostAddressLookup>(network);
        b.Services.AddSingleton<INotificationPinnedTcpDialer>(network);
        b.Services.AddSingleton(sp => new NotificationPinnedHttpHandlerFactory(network, NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots: new X509Certificate2Collection(sp.GetRequiredService<X509Certificate2>()), activationGate: gate));
        b.Services.AddKafdeckWebhookDelivery(new(true, NotificationPersistenceProvider.Sqlite, NotificationExecutionMode.Standalone, database, null,
            DestinationProfiles: [profile], WebhookDeliveryEnabled: true, DeliveryInterlockPath: anchor, DeliveryInterlockSha256: fingerprint));
        return b.Build();
    }
    private static NotificationDestinationProfile Profile(NotificationRecoveryTlsPeer peer) => new("recovery-webhook", NotificationProviderKind.Webhook,
        "Recovery fixture", [NotificationEventClass.Operational, NotificationEventClass.Mutation], new Uri(peer.Endpoint));
    private static async Task BackupAsync(string from, string to, CancellationToken token)
    {
        await using var c = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = from, Mode = SqliteOpenMode.ReadOnly }.ConnectionString);
        await using var target = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = to }.ConnectionString);
        await c.OpenAsync(token); await target.OpenAsync(token); c.BackupDatabase(target);
    }
    private static async Task PumpAsync(INotificationDeliveryDbConnectionFactory connections, string sourcePath, NotificationRecoveryTlsPeer peer, CancellationToken token)
    {
        var routing = new AdoNotificationRoutingStore(connections); await routing.InitializeAsync(token);
        if (await routing.GetSubscriptionAsync("restored-products", token) is null)
            await routing.CreateSubscriptionAsync(new("restored-products", "recovery-webhook", [NotificationEventClass.Mutation]), NotificationSubscriptionState.Active, DateTimeOffset.UtcNow, token);
        var policy = new NotificationProductEventPolicy();
        var coordinator = new NotificationRoutingCoordinator(routing, new AdoNotificationDeliveryStore(connections), new ConfiguredNotificationDestinationProfileCatalog([Profile(peer)]));
        var pump = new NotificationProductEventPump(new MutationNotificationOutbox(new SqliteMutationDbConnectionFactory(sourcePath)), coordinator,
            policy, new NotificationProductEventBackpressure(connections, policy), recoverySourceEvidence: new AdoNotificationRecoveryStore(connections));
        Assert.Equal(1, (await pump.RunCycleAsync(token)).Acknowledged);
    }
    private static async Task<NotificationDeliveryRecord> WaitAsync(INotificationDeliveryStore store, Guid id, NotificationDeliveryState state, CancellationToken token)
    { while (true) { var row = await store.GetAsync(id, "recovery-webhook", token); if (row?.Snapshot.State == state) return row; await Task.Delay(20, token); } }
    private static async Task<string> AnchorAsync(string path, Guid generation, string state, CancellationToken token)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new { version = 1, generation, state });
        await File.WriteAllBytesAsync(path, bytes, token);
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }
    private sealed class Network(int port) : INotificationHostAddressLookup, INotificationPinnedTcpDialer
    {
        public Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(string host, CancellationToken token) => Task.FromResult<IReadOnlyList<IPAddress>>([IPAddress.Parse("1.1.1.1")]);
        public ValueTask<Stream> DialAsync(IPAddress address, int requestedPort, CancellationToken token) => address.Equals(IPAddress.Parse("1.1.1.1")) && requestedPort == port
            ? new SocketNotificationPinnedTcpDialer().DialAsync(IPAddress.Loopback, port, token) : throw new InvalidOperationException("Unexpected test pin.");
    }
    private const string A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private const string B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
}
