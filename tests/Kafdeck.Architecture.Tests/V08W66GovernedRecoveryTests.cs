using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66GovernedRecoveryTests
{
    [Theory]
    [InlineData(NotificationRecoveryDisposition.ExternalAcceptanceObserved)]
    [InlineData(NotificationRecoveryDisposition.ExternalNonAcceptanceObserved)]
    [InlineData(NotificationRecoveryDisposition.UnresolvedRiskAccepted)]
    public async Task Reviewed_effect_dispositions_terminally_suppress_without_fabricating_provider_receipt(NotificationRecoveryDisposition disposition)
    {
        await using var f = await Fixture.CreateAsync();
        var id = await f.SeedAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.PreviewAsync(f.Request(NotificationRecoveryCommand.AdvanceGeneration), "requester", A, B, default));
        var preview = await f.Store.PreviewAsync(f.Request(NotificationRecoveryCommand.FenceBatch) with { Disposition = disposition }, "requester", A, B, default);
        Assert.Null(await f.Store.ApproveAsync(preview.OperationId, preview.Revision, preview.PreviewHash, "different-session-same-human", A, B, default));
        var approved = Assert.IsType<NotificationRecoveryPlan>(await f.Store.ApproveAsync(preview.OperationId, preview.Revision, preview.PreviewHash, "approver", C, B, default));
        Assert.Null(await f.Store.ApplyAsync(approved.OperationId, approved.Revision, approved.PreviewHash, C, B, () => true, default));
        Assert.Null(await f.Store.ApplyAsync(approved.OperationId, approved.Revision, approved.PreviewHash, A, B, () => false, default));
        var applied = Assert.IsType<NotificationRecoveryPlan>(await f.Store.ApplyAsync(approved.OperationId, approved.Revision, approved.PreviewHash, A, B, () => true, default));
        Assert.Equal(NotificationRecoveryState.Applied, applied.State);
        Assert.Equal(0, applied.RemainingUnreconciled);
        var row = Assert.IsType<NotificationDeliveryRecord>(await f.Deliveries.GetAsync(id, "recovery", default));
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, row.Snapshot.State);
        Assert.Equal(0, row.Snapshot.AttemptCount);
        Assert.Null(row.Snapshot.ProviderReceiptFingerprint);
        Assert.Equal(disposition.ToString(), await f.ScalarAsync("SELECT disposition FROM notification_recovery_receipts"));
        Assert.Equal("SuppressRestoredIntent", await f.ScalarAsync("SELECT action FROM notification_recovery_receipts"));
        Assert.Equal(3L, Convert.ToInt64(await f.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_audit")));
        Assert.Null(await f.Store.ApplyAsync(approved.OperationId, approved.Revision, approved.PreviewHash, A, B, () => true, default));
        Assert.Equal(f.Initial, await new AdoNotificationDeliveryGenerationReader(f.Connections).ReadAsync(default));
    }

    [Theory]
    [InlineData("row-drift")]
    [InlineData("policy-drift")]
    [InlineData("hash-drift")]
    [InlineData("anchor-active")]
    public async Task Changed_preview_or_current_authority_cannot_apply(string scenario)
    {
        await using var f = await Fixture.CreateAsync();
        await f.SeedAsync();
        var plan = await f.ApprovedAsync(f.Request(NotificationRecoveryCommand.FenceBatch));
        if (scenario == "row-drift") await f.SqlAsync("UPDATE kafdeck_notification_deliveries SET revision=revision+1");
        if (scenario == "anchor-active")
        {
            await File.WriteAllBytesAsync(f.Anchor, JsonSerializer.SerializeToUtf8Bytes(new { version = 1, generation = f.Target, state = "active" }));
            await Assert.ThrowsAsync<KafdeckConfigurationException>(() => f.Store.ApplyAsync(plan.OperationId, plan.Revision, plan.PreviewHash, A, B, () => true, default));
        }
        else Assert.Null(await f.Store.ApplyAsync(plan.OperationId, plan.Revision,
            scenario == "hash-drift" ? C : plan.PreviewHash, A, scenario == "policy-drift" ? C : B, () => true, default));
        Assert.Equal("Pending", await f.ScalarAsync("SELECT state FROM kafdeck_notification_deliveries"));
        Assert.Equal(0L, Convert.ToInt64(await f.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_receipts")));
    }

    [Fact]
    public async Task Epoch_and_cutoff_advance_together_only_after_all_batches_are_reconciled_and_survive_reopen()
    {
        await using var f = await Fixture.CreateAsync();
        await f.SeedAsync(); await f.SeedAsync();
        var first = await f.ApprovedAsync(f.Request(NotificationRecoveryCommand.FenceBatch) with { MaxItems = 1 });
        var fenced = Assert.IsType<NotificationRecoveryPlan>(await f.Store.ApplyAsync(first.OperationId, first.Revision, first.PreviewHash, A, B, () => true, default));
        Assert.Equal(1, fenced.RemainingUnreconciled);
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.PreviewAsync(f.Request(NotificationRecoveryCommand.AdvanceGeneration), "requester", A, B, default));
        await f.ApplyAsync(f.Request(NotificationRecoveryCommand.FenceBatch));
        var advance = await f.ApplyAsync(f.Request(NotificationRecoveryCommand.AdvanceGeneration));
        Assert.True(advance.ReadyForOperatorActivation);
        var reopened = new AdoNotificationRecoveryStore(f.Connections);
        var status = await reopened.StatusAsync(default);
        Assert.Equal(f.Target, status.DatabaseGeneration);
        Assert.Equal(advance.HistoricalCutoffUtc, status.HistoricalCutoffUtc);
        Assert.Equal(advance.OperationId, status.AdvanceOperationId);
        Assert.False(await reopened.CheckAsync(Guid.NewGuid(), "recovery", advance.HistoricalCutoffUtc, default));
        Assert.True(await reopened.CheckAsync(Guid.NewGuid(), "recovery", advance.HistoricalCutoffUtc.AddMilliseconds(1), default));
        await Assert.ThrowsAsync<KafdeckConfigurationException>(() => new ConfiguredNotificationDeliveryInterlock(f.Anchor, f.Hash,
            new AdoNotificationDeliveryGenerationReader(f.Connections)).VerifyAsync(default).AsTask());
        Assert.Equal("quarantined", JsonDocument.Parse(await File.ReadAllBytesAsync(f.Anchor)).RootElement.GetProperty("state").GetString());
    }

    [Fact]
    public async Task Future_clock_ledger_keeps_readable_monotonic_row_time_without_moving_trusted_epoch_cutoff_into_the_future()
    {
        await using var f = await Fixture.CreateAsync();
        var future = DateTimeOffset.UtcNow.AddHours(1);
        var id = await f.SeedAsync(future);
        await f.ApplyAsync(f.Request(NotificationRecoveryCommand.FenceBatch));
        var row = Assert.IsType<NotificationDeliveryRecord>(await f.Deliveries.GetAsync(id, "recovery"));
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, row.Snapshot.State);
        Assert.Equal(future, row.UpdatedAtUtc);
        Assert.Equal(0, row.Snapshot.AttemptCount);
        var advance = await f.ApplyAsync(f.Request(NotificationRecoveryCommand.AdvanceGeneration));
        Assert.True(advance.HistoricalCutoffUtc < future);
        Assert.Equal(advance.HistoricalCutoffUtc, (await f.Store.StatusAsync(default)).HistoricalCutoffUtc);
    }

    [Fact]
    public async Task Empty_enrollment_is_explicit_two_human_governed_and_never_performed_by_initialize_or_status()
    {
        await using var f = await Fixture.CreateAsync(enroll: false);
        Assert.Null((await f.Store.StatusAsync(default)).DatabaseGeneration);
        Assert.Null(await new AdoNotificationDeliveryGenerationReader(f.Connections).ReadAsync(default));
        var request = f.Request(NotificationRecoveryCommand.EnrollEmpty) with { ExpectedDatabaseGeneration = null };
        var result = await f.ApplyAsync(request);
        Assert.True(result.ReadyForOperatorActivation);
        Assert.Equal(f.Target, await new AdoNotificationDeliveryGenerationReader(f.Connections).ReadAsync(default));
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.PreviewAsync(request, "requester", A, B, default));
    }

    [Fact]
    public async Task Enrollment_refuses_even_one_pending_row_and_event_gate_never_auto_enrolls()
    {
        await using var f = await Fixture.CreateAsync(enroll: false);
        await f.SeedAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.PreviewAsync(
            f.Request(NotificationRecoveryCommand.EnrollEmpty) with { ExpectedDatabaseGeneration = null }, "requester", A, B, default));
        Assert.True(await new AdoNotificationRecoveryStore(f.Connections).CheckAsync(Guid.NewGuid(), "recovery", DateTimeOffset.UtcNow, default));
        Assert.Null(await new AdoNotificationDeliveryGenerationReader(f.Connections).ReadAsync(default));
    }

    [Fact]
    public async Task Explicit_historical_source_inventory_fences_future_clock_events_and_refuses_drift_without_changing_payload_identity()
    {
        await using var f = await Fixture.CreateAsync();
        var old = Guid.NewGuid(); f.Source.Ids.Add(old);
        var request = f.Request(NotificationRecoveryCommand.FenceSourceBatch);
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.PreviewAsync(f.Request(NotificationRecoveryCommand.AdvanceGeneration), "requester", A, B, default));
        var preview = await f.ApprovedAsync(request);
        var added = Guid.NewGuid(); f.Source.Ids.Add(added);
        Assert.Null(await f.Store.ApplyAsync(preview.OperationId, preview.Revision, preview.PreviewHash, A, B, () => true, default));
        f.Source.Ids.Remove(added);
        Assert.NotNull(await f.Store.ApplyAsync(preview.OperationId, preview.Revision, preview.PreviewHash, A, B, () => true, default));
        var advance = await f.ApplyAsync(f.Request(NotificationRecoveryCommand.AdvanceGeneration));
        var future = NotificationSafeEvent.Approved(old, NotificationApprovedEventKind.MutationStateChanged, advance.HistoricalCutoffUtc.AddYears(1));
        var fingerprint = future.PayloadFingerprint;
        Assert.False(await f.Store.CheckAsync(old, "recovery", future.OccurredAtUtc, default));
        Assert.Equal(fingerprint, future.PayloadFingerprint);
        Assert.True(await f.Store.CheckAsync(Guid.NewGuid(), "recovery", future.OccurredAtUtc, default));
        Assert.Equal("UnresolvedRiskAccepted", await f.ScalarAsync("SELECT disposition FROM notification_recovery_historical_sources"));
    }

    [Fact]
    public async Task Two_independently_approved_overlapping_source_batches_have_one_winner_and_a_durable_CAS_conflict()
    {
        await using var f = await Fixture.CreateAsync();
        f.Source.Ids.Add(Guid.NewGuid());
        var one = await f.ApprovedAsync(f.Request(NotificationRecoveryCommand.FenceSourceBatch));
        var two = await f.ApprovedAsync(f.Request(NotificationRecoveryCommand.FenceSourceBatch));
        var outcomes = await Task.WhenAll(f.Store.ApplyAsync(one.OperationId, one.Revision, one.PreviewHash, A, B, () => true, default),
            f.Store.ApplyAsync(two.OperationId, two.Revision, two.PreviewHash, A, B, () => true, default));
        Assert.Single(outcomes, outcome => outcome is not null);
        Assert.Single(outcomes, outcome => outcome is null);
        Assert.Equal(1L, Convert.ToInt64(await f.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_historical_sources")));
        Assert.Equal(5L, Convert.ToInt64(await f.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_audit")));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Readonly_source_inventory_never_creates_missing_database_or_installs_missing_schema(bool existingDatabase)
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-readonly-source-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            if (existingDatabase)
            { await using var c = new SqliteConnection("Data Source=" + path); await c.OpenAsync(); }
            var reader = AdoNotificationRecoverySourceInventory.ForSqlite(path);
            if (existingDatabase) await Assert.ThrowsAsync<InvalidOperationException>(() => reader.ReadAsync(default));
            else await Assert.ThrowsAsync<SqliteException>(() => reader.ReadAsync(default));
            Assert.Equal(existingDatabase, File.Exists(path));
            if (existingDatabase)
            {
                await using var c = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly }.ConnectionString);
                await c.OpenAsync(); await using var command = c.CreateCommand(); command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table'";
                Assert.Equal(0L, Convert.ToInt64(await command.ExecuteScalarAsync()));
            }
        }
        finally { SqliteConnection.ClearAllPools(); if (File.Exists(path)) File.Delete(path); }
    }

    [Theory]
    [InlineData(NotificationProviderKind.Webhook)]
    [InlineData(NotificationProviderKind.Email)]
    [InlineData(NotificationProviderKind.Slack)]
    [InlineData(NotificationProviderKind.MicrosoftTeams)]
    [InlineData(NotificationProviderKind.Telegram)]
    [InlineData(NotificationProviderKind.PagerDuty)]
    public async Task Historical_source_without_original_delivery_row_is_durably_suppressed_before_any_provider_DNS_secret_or_transport(NotificationProviderKind provider)
    {
        await using var f = await Fixture.CreateAsync();
        var historicalId = Guid.NewGuid();
        f.Source.Ids.Add(historicalId);
        await f.ApplyAsync(f.Request(NotificationRecoveryCommand.FenceSourceBatch));
        var advance = await f.ApplyAsync(f.Request(NotificationRecoveryCommand.AdvanceGeneration));
        var profile = provider switch
        {
            NotificationProviderKind.Webhook => new NotificationDestinationProfile("cutoff", provider, "Cutoff", [NotificationEventClass.Operational], new Uri("https://hooks.example.org/notify")),
            NotificationProviderKind.Email => new NotificationDestinationProfile("cutoff", provider, "Cutoff", [NotificationEventClass.Operational],
                credentialBindingId: new("cutoff-key"), emailRecipientAddress: "ops@example.org", smtp: new("smtp.example.org", "kafdeck@example.org", "kafdeck")),
            _ => V08W66ProviderPhysicalTlsTests.Profile(provider)
        };
        var routing = new AdoNotificationRoutingStore(f.Connections); await routing.InitializeAsync();
        var notification = NotificationSafeEvent.Approved(historicalId, NotificationApprovedEventKind.ConsumerLagAlert, advance.HistoricalCutoffUtc.AddYears(1));
        await routing.CreateOrGetEventAsync(notification, notification.OccurredAtUtc, default);
        Assert.Null(await f.Deliveries.GetAsync(notification.EventId, profile.DestinationId));
        var record = new NotificationDeliveryRecord(new NotificationDeliverySnapshot(notification.EventId, profile.DestinationId,
            notification.PayloadFingerprint, NotificationDeliveryState.InFlight, 1, notification.OccurredAtUtc,
            routedProfileRevisionFingerprint: profile.RevisionFingerprint), 1, notification.OccurredAtUtc);
        var ports = new NeverProvider();
        var catalog = new ConfiguredNotificationDestinationProfileCatalog([profile]);
        var webhook = new WebhookNotificationDeliveryDispatcher(routing, catalog, new WebhookNotificationAdapter(ports, ports, ports), f.Store);
        var composite = new NotificationProviderDeliveryDispatcher(routing, catalog, new(ports, ports), new(ports, ports),
            new(ports, ports), new(ports, ports), new(ports, ports), webhook, f.Store);
        var result = await composite.DispatchAsync(record, default);
        Assert.Equal(NotificationDeliveryDispatchOutcome.UnknownExternalEffect, result.Outcome);
        if (provider == NotificationProviderKind.Webhook)
            Assert.Equal(NotificationDeliveryDispatchOutcome.UnknownExternalEffect, (await webhook.DispatchAsync(record, default)).Outcome);
        Assert.Equal(0, ports.Calls);
        Assert.Equal(1L, Convert.ToInt64(await f.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_source_suppressions")));
        Assert.Equal("SuppressedHistoricalSource", await f.ScalarAsync("SELECT disposition FROM notification_recovery_source_suppressions"));
        Assert.False(await new AdoNotificationRecoveryStore(f.Connections).CheckAsync(notification.EventId, profile.DestinationId, notification.OccurredAtUtc, default));
        Assert.Equal(1L, Convert.ToInt64(await f.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_source_suppressions")));
    }

    private const string A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private const string B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    private const string C = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    internal sealed class Fixture(string directory, INotificationDeliveryDbConnectionFactory connections, Guid initial, Guid target,
        string anchor, string hash, AdoNotificationRecoveryStore store, AdoNotificationDeliveryStore deliveries, MutableSource source) : IAsyncDisposable
    {
        internal INotificationDeliveryDbConnectionFactory Connections => connections;
        internal AdoNotificationRecoveryStore Store => store;
        internal AdoNotificationDeliveryStore Deliveries => deliveries;
        internal Guid Initial => initial;
        internal Guid Target => target;
        internal string Anchor => anchor;
        internal string Hash => hash;
        internal MutableSource Source => source;
        internal static async Task<Fixture> CreateAsync(bool enroll = true)
        {
            var directory = Path.Combine(Path.GetTempPath(), "kafdeck-reviewed-recovery-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            var connections = new SqliteNotificationDeliveryDbConnectionFactory(Path.Combine(directory, "delivery.db"));
            var deliveries = new AdoNotificationDeliveryStore(connections); await deliveries.InitializeAsync();
            var initial = Guid.NewGuid(); var target = Guid.NewGuid(); var anchor = Path.Combine(directory, "anchor.json");
            var bytes = JsonSerializer.SerializeToUtf8Bytes(new { version = 1, generation = target, state = "quarantined" });
            await File.WriteAllBytesAsync(anchor, bytes);
            var hash = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
            var source = new MutableSource();
            var store = new AdoNotificationRecoveryStore(connections, new NotificationRecoveryAuthority(new(target, anchor, hash)), source);
            await store.InitializeAsync(default);
            var f = new Fixture(directory, connections, initial, target, anchor, hash, store, deliveries, source);
            if (enroll) await f.SqlAsync("CREATE TABLE notification_delivery_generation(id INTEGER PRIMARY KEY,generation TEXT); INSERT INTO notification_delivery_generation VALUES(1,'" + initial.ToString("D") + "')");
            return f;
        }
        internal NotificationRecoveryRequest Request(NotificationRecoveryCommand command) => new(command,
            command is NotificationRecoveryCommand.FenceBatch or NotificationRecoveryCommand.FenceSourceBatch ? NotificationRecoveryDisposition.UnresolvedRiskAccepted : null,
            A, C, initial, target, hash);
        internal async Task<NotificationRecoveryPlan> ApprovedAsync(NotificationRecoveryRequest request)
        {
            var plan = await store.PreviewAsync(request, "requester", A, B, default);
            return Assert.IsType<NotificationRecoveryPlan>(await store.ApproveAsync(plan.OperationId, plan.Revision, plan.PreviewHash, "approver", C, B, default));
        }
        internal async Task<NotificationRecoveryPlan> ApplyAsync(NotificationRecoveryRequest request)
        {
            var plan = await ApprovedAsync(request);
            return Assert.IsType<NotificationRecoveryPlan>(await store.ApplyAsync(plan.OperationId, plan.Revision, plan.PreviewHash, A, B, () => true, default));
        }
        internal async Task<Guid> SeedAsync(DateTimeOffset? createdAtUtc = null)
        {
            var id = Guid.NewGuid(); var now = createdAtUtc ?? DateTimeOffset.UtcNow;
            await deliveries.CreateOrGetAsync(new NotificationDeliverySnapshot(id, "recovery", A, NotificationDeliveryState.Pending,
                0, now, routedProfileRevisionFingerprint: B), now);
            return id;
        }
        internal async Task SqlAsync(string sql)
        { await using var c = await connections.OpenAsync(); await using var cmd = c.CreateCommand(); cmd.CommandText = sql; await cmd.ExecuteNonQueryAsync(); }
        internal async Task<object?> ScalarAsync(string sql)
        { await using var c = await connections.OpenAsync(); await using var cmd = c.CreateCommand(); cmd.CommandText = sql; return await cmd.ExecuteScalarAsync(); }
        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
            Directory.Delete(directory);
            return ValueTask.CompletedTask;
        }
    }
    internal sealed class MutableSource : INotificationRecoverySourceInventory
    {
        internal List<Guid> Ids { get; } = [];
        public Task<NotificationRecoverySourceInventory> ReadAsync(CancellationToken token) => Task.FromResult(new NotificationRecoverySourceInventory(C,
            Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(Ids.OrderBy(id => id).ToArray()))).ToLowerInvariant(), Ids.ToArray()));
    }
    private sealed class NeverProvider : INotificationCredentialResolver, INotificationEndpointResolutionPort,
        INotificationPinnedWebhookTransport, INotificationEmailTransport, INotificationSlackTransport,
        INotificationTeamsTransport, INotificationTelegramTransport, INotificationPagerDutyTransport
    {
        internal int Calls { get; private set; }
        private Exception Called() { Calls++; return new InvalidOperationException("Historical source must be suppressed before provider activity."); }
        public ValueTask<NotificationCredentialValue> ResolveAsync(NotificationCredentialResolutionRequest request, CancellationToken token) => throw Called();
        public Task<NotificationPinnedEndpoint> ResolveForConnectionAsync(Uri endpoint, CancellationToken token) => throw Called();
        public Task<NotificationWebhookTransportResult> SendAsync(NotificationPinnedWebhookRequest request, CancellationToken token) => throw Called();
        public Task<NotificationProviderTransportResult> SendAsync(NotificationEmailTransportRequest request, CancellationToken token) => throw Called();
        public Task<NotificationProviderTransportResult> SendAsync(NotificationBoundTransportRequest request, CancellationToken token) => throw Called();
    }
}
