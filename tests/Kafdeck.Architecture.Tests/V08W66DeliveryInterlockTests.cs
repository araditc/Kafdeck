using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66DeliveryInterlockTests
{
    [Theory]
    [InlineData(NotificationProviderKind.Webhook)]
    [InlineData(NotificationProviderKind.Email)]
    [InlineData(NotificationProviderKind.Slack)]
    [InlineData(NotificationProviderKind.MicrosoftTeams)]
    [InlineData(NotificationProviderKind.Telegram)]
    [InlineData(NotificationProviderKind.PagerDuty)]
    public async Task Every_physical_provider_checks_operator_gate_before_any_socket(NotificationProviderKind provider)
    {
        var gate = new RefusingGate();
        var network = new NeverDial();
        var factory = new NotificationPinnedHttpHandlerFactory(network, NotificationAddressPolicy.NoConfiguredNat64, activationGate: gate);
        var notification = NotificationSafeEvent.Approved(Guid.NewGuid(), NotificationApprovedEventKind.ConsumerLagAlert, DateTimeOffset.UtcNow);
        if (provider == NotificationProviderKind.Webhook)
        {
            var profile = new NotificationDestinationProfile("gate-webhook", provider, "Gate", [NotificationEventClass.Operational], new Uri("https://hooks.example.org/notify"));
            var result = await new NotificationPinnedWebhookHttpTransport(profile, factory).SendAsync(
                new NotificationPinnedWebhookRequest(profile.DestinationId, profile.RevisionFingerprint,
                    Pin(profile.ConfiguredEndpoint!), notification.ProjectJsonPayload(), null), CancellationToken.None);
            Assert.Equal(NotificationWebhookTransportOutcome.UnknownExternalEffect, result.Outcome);
        }
        else if (provider == NotificationProviderKind.Email)
        {
            var profile = new NotificationDestinationProfile("gate-email", provider, "Gate", [NotificationEventClass.Operational],
                credentialBindingId: new NotificationCredentialBindingId("gate-v1"), emailRecipientAddress: "ops@example.org",
                smtp: new NotificationSmtpProfile("smtp.example.org", "kafdeck@example.org", "kafdeck"));
            var result = await new NotificationPinnedSmtpTransport(profile, new Resolver(), factory).SendAsync(
                new NotificationEmailTransportRequest(profile.DestinationId, profile.RevisionFingerprint, profile.EmailRecipientAddress!,
                    $"[{notification.EventClass}] {notification.Subject}", string.Join("\n", notification.EventType, notification.Summary,
                        notification.OccurredAtUtc.ToString("O", CultureInfo.InvariantCulture), notification.EventId.ToString("D")),
                    new NotificationCredentialValue(new string('a', 32)), notification), CancellationToken.None);
            Assert.Equal(NotificationProviderTransportOutcome.UnknownExternalEffect, result.Outcome);
        }
        else
        {
            var profile = V08W66ProviderPhysicalTlsTests.Profile(provider);
            var credential = provider == NotificationProviderKind.Telegram ? "123456:" + new string('a', 32) : new string('a', 32);
            var result = await new NotificationPinnedProviderHttpTransport(profile, new Resolver(), factory).SendAsync(
                new NotificationBoundTransportRequest(profile.DestinationId, profile.RevisionFingerprint, provider, notification,
                    new NotificationCredentialValue(credential)), CancellationToken.None);
            Assert.Equal(NotificationProviderTransportOutcome.UnknownExternalEffect, result.Outcome);
        }
        Assert.True(gate.Checks >= 1, "Each concrete physical path must consult the operator gate.");
        Assert.Equal(0, network.Dials);
    }

    [Theory]
    [InlineData("quarantined")]
    [InlineData("missing")]
    [InlineData("hash-mismatch")]
    [InlineData("generation-mismatch")]
    [InlineData("unenrolled")]
    [InlineData("duplicate")]
    [InlineData("malformed")]
    [InlineData("oversized")]
    public async Task Interlock_refuses_untrusted_or_unreconciled_activation(string scenario)
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-interlock-" + Guid.NewGuid().ToString("N"));
        var generation = Guid.NewGuid();
        var contents = scenario switch
        {
            "duplicate" => "{\"version\":1,\"generation\":\"" + generation.ToString("D") + "\",\"state\":\"active\",\"state\":\"quarantined\"}",
            "malformed" => "not-json",
            "oversized" => new string('x', 1025),
            _ => JsonSerializer.Serialize(new { version = 1, generation, state = scenario == "quarantined" ? "quarantined" : "active" }),
        };
        try
        {
            if (scenario != "missing") await File.WriteAllTextAsync(path, contents);
            var digest = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(contents))).ToLowerInvariant();
            var reader = new GenerationReader(scenario == "unenrolled" ? null : scenario == "generation-mismatch" ? Guid.NewGuid() : generation);
            var gate = new ConfiguredNotificationDeliveryInterlock(path, scenario == "hash-mismatch" ? new string('a', 64) : digest, reader);
            await Assert.ThrowsAsync<KafdeckConfigurationException>(() => gate.VerifyAsync(CancellationToken.None).AsTask());
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task Active_matching_interlock_is_read_only_and_refused_worker_never_claims_pending()
    {
        var path = Path.Combine(Path.GetTempPath(), "kafdeck-gate-" + Guid.NewGuid().ToString("N"));
        var database = path + ".db";
        var generation = Guid.NewGuid();
        var contents = JsonSerializer.Serialize(new { version = 1, generation, state = "active" });
        try
        {
            await File.WriteAllTextAsync(path, contents);
            var gate = new ConfiguredNotificationDeliveryInterlock(path,
                Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(contents))).ToLowerInvariant(), new GenerationReader(generation));
            await gate.VerifyAsync(CancellationToken.None);
            Assert.Equal(contents, await File.ReadAllTextAsync(path));
            var connections = new SqliteNotificationDeliveryDbConnectionFactory(database);
            var store = new AdoNotificationDeliveryStore(connections);
            await store.InitializeAsync();
            Assert.Null(await new AdoNotificationDeliveryGenerationReader(connections).ReadAsync(CancellationToken.None));
            var id = Guid.NewGuid();
            var now = DateTimeOffset.UtcNow;
            await store.CreateOrGetAsync(new NotificationDeliverySnapshot(id, "gate", new string('a', 64),
                NotificationDeliveryState.Pending, 0, now, routedProfileRevisionFingerprint: new string('b', 64)), now);
            var worker = new NotificationDeliveryWorker(store, new NeverDispatch(), activationGate: new RefusingGate());
            await Assert.ThrowsAsync<KafdeckConfigurationException>(() => worker.RunDueCycleAsync());
            var row = await store.GetAsync(id, "gate");
            Assert.NotNull(row);
            Assert.Equal(NotificationDeliveryState.Pending, row.Snapshot.State);
            Assert.Equal(0, row.Snapshot.AttemptCount);
            Assert.Equal(1, row.Revision);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
            foreach (var suffix in new[] { "", "-wal", "-shm" }) File.Delete(database + suffix);
        }
    }

    private static NotificationPinnedEndpoint Pin(Uri endpoint) => NotificationAddressPolicy.NoConfiguredNat64.ValidatePinnedEndpoint(endpoint, [IPAddress.Parse("1.1.1.1")], DateTimeOffset.UtcNow);
    private sealed class Resolver : INotificationEndpointResolutionPort
    {
        public Task<NotificationPinnedEndpoint> ResolveForConnectionAsync(Uri endpoint, CancellationToken token) => Task.FromResult(Pin(endpoint));
    }
    private sealed class NeverDial : INotificationPinnedTcpDialer
    {
        public int Dials { get; private set; }
        public ValueTask<Stream> DialAsync(IPAddress address, int port, CancellationToken token)
        { Dials++; throw new InvalidOperationException("An interlocked sender must not dial."); }
    }
    private sealed class RefusingGate : INotificationDeliveryActivationGate
    {
        public int Checks { get; private set; }
        public ValueTask VerifyAsync(CancellationToken token)
        { Checks++; return ValueTask.FromException(new KafdeckConfigurationException("Operator interlock refused.")); }
    }
    private sealed class GenerationReader(Guid? generation) : INotificationDeliveryGenerationReader
    {
        public ValueTask<Guid?> ReadAsync(CancellationToken token) => ValueTask.FromResult(generation);
    }
    private sealed class NeverDispatch : INotificationDeliveryDispatcher
    {
        public Task<NotificationDeliveryDispatchResult> DispatchAsync(NotificationDeliveryRecord record, CancellationToken token) =>
            throw new InvalidOperationException("An interlocked worker must never dispatch.");
    }
}
