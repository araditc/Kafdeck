using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;
using TlsProvider = Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66ProviderPhysicalTlsTests(ITestOutputHelper output)
{
    public static IEnumerable<object[]> Cases() =>
        from provider in new[] { NotificationProviderKind.Slack, NotificationProviderKind.MicrosoftTeams,
            NotificationProviderKind.Telegram, NotificationProviderKind.PagerDuty }
        from scenario in new[] { "accepted", "denied", "provider-denied", "ambiguous", "malformed", "wrong-receipt", "oversized", "duplicate" }
        select new object[] { provider, scenario };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Hosted_typed_provider_posts_real_tls_and_preserves_receipt_or_unknown_truth(NotificationProviderKind provider, string scenario)
    {
        var id = Guid.NewGuid();
        var name = Hostname(provider);
        var status = scenario switch
        {
            "denied" => 403,
            "ambiguous" => 429,
            "provider-denied" when provider == NotificationProviderKind.MicrosoftTeams => 403,
            "provider-denied" when provider == NotificationProviderKind.PagerDuty => 400,
            _ when provider == NotificationProviderKind.MicrosoftTeams => 201,
            _ when provider == NotificationProviderKind.PagerDuty => 202,
            _ => 200,
        };
        await using var tls = new TlsProvider(certificateName: name, statusCode: status,
            responseBody: _ => Response(provider, scenario, id));
        var directory = Path.Combine(Path.GetTempPath(), $"kafdeck-w66-provider-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "ledger.db");
        var credentialPath = Path.Combine(directory, "credential");
        var secret = provider switch
        {
            NotificationProviderKind.Telegram => "123456:" + Convert.ToHexString(RandomNumberGenerator.GetBytes(24)).ToLowerInvariant(),
            NotificationProviderKind.PagerDuty => Convert.ToHexString(RandomNumberGenerator.GetBytes(16)).ToLowerInvariant(),
            _ => Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)),
        };
        var profile = Profile(provider);
        using var telemetry = new NotificationHttpTelemetryCapture(secret);
        try
        {
            await File.WriteAllTextAsync(credentialPath, secret);
            var reference = SecretReference.Parse("file:" + credentialPath);
            var network = new ProviderNetwork(name, tls.Port);
            var builder = Host.CreateApplicationBuilder();
            var options = new NotificationReadOptions(true, NotificationPersistenceProvider.Sqlite, NotificationExecutionMode.Standalone,
                path, null, DestinationProfiles: [profile], CredentialBindings:
                [new NotificationCredentialReferenceOptions(profile.DestinationId, profile.CredentialBindingId!, reference)],
                HttpProviderDeliveryEnabled: true);
            builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([profile]));
            builder.Services.AddSingleton<INotificationCredentialResolver>(new ConfiguredNotificationCredentialResolver(
                [new ConfiguredNotificationCredentialBinding(profile.DestinationId, provider, profile.RevisionFingerprint,
                    profile.CredentialBindingId!, reference)], new SecretResolver()));
            builder.Services.AddSingleton<INotificationHostAddressLookup>(network);
            builder.Services.AddSingleton<INotificationPinnedTcpDialer>(network);
            builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(network, NotificationAddressPolicy.NoConfiguredNat64,
                trustRoots: new X509Certificate2Collection(tls.Root)));
            var connections = new SqliteNotificationDeliveryDbConnectionFactory(path);
            builder.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(connections);
            var routing = new AdoNotificationRoutingStore(connections);
            var deliveries = new AdoNotificationDeliveryStore(connections);
            builder.Services.AddSingleton<INotificationRoutingStore>(routing);
            builder.Services.AddSingleton<INotificationDeliveryStore>(deliveries);
            builder.Services.AddKafdeckWebhookDelivery(options);
            using var host = builder.Build();
            await routing.InitializeAsync();
            await deliveries.InitializeAsync();
            var now = DateTimeOffset.UtcNow;
            var notification = NotificationSafeEvent.Approved(id, NotificationApprovedEventKind.ConsumerLagAlert, now);
            await routing.CreateOrGetEventAsync(notification, now);
            await deliveries.CreateOrGetAsync(new NotificationDeliverySnapshot(id, profile.DestinationId, notification.PayloadFingerprint,
                NotificationDeliveryState.Pending, 0, now, routedProfileRevisionFingerprint: profile.RevisionFingerprint), now);
            await host.StartAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var expected = scenario == "accepted" ? NotificationDeliveryState.Delivered :
                scenario is "denied" or "provider-denied" ? NotificationDeliveryState.Exhausted : NotificationDeliveryState.UnknownExternalEffect;
            NotificationDeliveryRecord? row;
            while ((row = await deliveries.GetAsync(id, profile.DestinationId, timeout.Token))?.Snapshot.State != expected)
                await Task.Delay(20, timeout.Token);
            await host.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync();
            await host.StopAsync();
            Assert.Equal(1, tls.RequestCount);
            Assert.Equal(name, tls.ServerName);
            Assert.Equal(1, network.Dials);
            Assert.Equal(1, row!.Snapshot.AttemptCount);
            if (scenario == "accepted") Assert.Equal(64, row.Snapshot.ProviderReceiptFingerprint?.Length);
            else Assert.Null(row.Snapshot.ProviderReceiptFingerprint);
            Assert.Null(row.Snapshot.ProviderRequestId);
            var request = tls.Requests.Single();
            VerifyWire(provider, request, profile, id, secret);
            var telemetryLeak = telemetry.Contains(secret);
            Assert.False(telemetryLeak, "Provider credential must not reach HttpClient EventSource or Activity telemetry.");
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(directory, "ledger.db*"))
            {
                var content = Encoding.UTF8.GetString(await File.ReadAllBytesAsync(file));
                Assert.False(content.Contains(secret, StringComparison.Ordinal), "Provider credential must not appear in durable files.");
            }
            output.WriteLine($"REAL_PROVIDER_TLS provider={provider}; scenario={scenario}; HTTP={status}; SNI={name}; physicalPOSTs=1; ledger={expected}; attempts=1; receiptSHA256={(row.Snapshot.ProviderReceiptFingerprint is null ? "none" : "present")}; automaticResends=0; credentialLedgerLeaks=0");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
            Directory.Delete(directory);
        }
    }

    [Fact]
    public void Provider_targets_are_closed_revision_bound_and_default_off()
    {
        var options = new NotificationReadOptions(true, NotificationPersistenceProvider.Sqlite, NotificationExecutionMode.Standalone, "unused", null);
        var services = new ServiceCollection();
        services.AddKafdeckWebhookDelivery(options);
        Assert.DoesNotContain(services, descriptor => descriptor.ServiceType == typeof(IHostedService));
        Assert.Throws<ArgumentException>(() => new NotificationDestinationProfile("bad", NotificationProviderKind.Slack, "Bad",
            [NotificationEventClass.Operational], credentialBindingId: new NotificationCredentialBindingId("key"), providerResourceId: "https://attacker.example"));
        Assert.Throws<ArgumentException>(() => new NotificationDestinationProfile("bad", NotificationProviderKind.Telegram, "Bad",
            [NotificationEventClass.Operational], credentialBindingId: new NotificationCredentialBindingId("key"), providerResourceId: "@unreviewed-name"));
        var old = Profile(NotificationProviderKind.Slack);
        var changed = new NotificationDestinationProfile(old.DestinationId, old.Provider, old.DisplayName, old.EnabledEvents,
            credentialBindingId: old.CredentialBindingId, providerResourceId: "C987ABC654");
        Assert.NotEqual(old.RevisionFingerprint, changed.RevisionFingerprint);
        var inactiveEmail = new NotificationDestinationProfile("inactive-email", NotificationProviderKind.Email, "Inactive",
            [NotificationEventClass.Operational], credentialBindingId: new NotificationCredentialBindingId("email-key"),
            emailRecipientAddress: "ops@example.org", enabled: false);
        NotificationPhysicalDeliveryAdmission.Validate(options with { WebhookDeliveryEnabled = true, DestinationProfiles = [inactiveEmail] });
    }

    internal static NotificationDestinationProfile Profile(NotificationProviderKind provider) =>
        new("typed-provider", provider, "Typed provider", [NotificationEventClass.Operational],
            credentialBindingId: new NotificationCredentialBindingId("typed-provider-v1"),
            providerResourceId: provider switch
            {
                NotificationProviderKind.Slack => "C123ABC456",
                NotificationProviderKind.MicrosoftTeams => "fbe2bf47-16c8-47cf-b4a5-4b9b187c508b",
                NotificationProviderKind.Telegram => "-1001234567",
                _ => null,
            },
            providerSecondaryResourceId: provider == NotificationProviderKind.MicrosoftTeams ? "19:abcdef1234567890@thread.tacv2" : null);

    internal static string Hostname(NotificationProviderKind provider) => provider switch
    {
        NotificationProviderKind.Slack => "slack.com",
        NotificationProviderKind.MicrosoftTeams => "graph.microsoft.com",
        NotificationProviderKind.Telegram => "api.telegram.org",
        _ => "events.pagerduty.com",
    };

    internal static string Response(NotificationProviderKind provider, string scenario, Guid id)
    {
        if (scenario == "malformed") return "not-json";
        if (scenario == "oversized") return new string('x', 16 * 1024 + 1);
        if (scenario == "duplicate") return provider switch
        {
            NotificationProviderKind.Slack => "{\"ok\":true,\"ok\":true}",
            NotificationProviderKind.MicrosoftTeams => "{\"id\":\"first\",\"id\":\"second\"}",
            NotificationProviderKind.Telegram => "{\"ok\":true,\"ok\":true}",
            _ => "{\"status\":\"success\",\"status\":\"success\"}",
        };
        if (scenario == "provider-denied") return provider == NotificationProviderKind.Slack
            ? "{\"ok\":false,\"error\":\"invalid_auth\"}" : "{\"ok\":false,\"error_code\":403}";
        var wrong = scenario == "wrong-receipt";
        return provider switch
        {
            NotificationProviderKind.Slack => JsonSerializer.Serialize(new { ok = true, channel = wrong ? "C987ABC654" : "C123ABC456", ts = "1503435956.000247" }),
            NotificationProviderKind.MicrosoftTeams => wrong ? "{}" : "{\"id\":\"message-id-42\"}",
            NotificationProviderKind.Telegram => JsonSerializer.Serialize(new { ok = true, result = new { message_id = 42, chat = new { id = wrong ? -1007654321L : -1001234567L } } }),
            _ => JsonSerializer.Serialize(new { status = "success", dedup_key = wrong ? Guid.Empty.ToString("D") : id.ToString("D") }),
        };
    }

    internal static void VerifyWire(NotificationProviderKind provider, string request, NotificationDestinationProfile profile, Guid id, string secret)
    {
        var boundary = request.IndexOf("\r\n\r\n", StringComparison.Ordinal);
        var headers = request[..boundary].Split("\r\n");
        using var document = JsonDocument.Parse(request[(boundary + 4)..]);
        var root = document.RootElement;
        if (provider is NotificationProviderKind.Slack or NotificationProviderKind.MicrosoftTeams)
        {
            var header = headers.SingleOrDefault(line => line.StartsWith("Authorization:", StringComparison.OrdinalIgnoreCase));
            Assert.True(header is not null && FixedEquals(header[(header.IndexOf(':') + 1)..].Trim(), "Bearer " + secret), "Typed provider requires its exact Bearer credential.");
        }
        else Assert.DoesNotContain(headers, line => line.StartsWith("Authorization:", StringComparison.OrdinalIgnoreCase));
        switch (provider)
        {
            case NotificationProviderKind.Slack:
                Assert.True(headers[0].StartsWith("POST /api/chat.postMessage ", StringComparison.Ordinal), "Expected Slack method must match without exposing captured wire material.");
                Assert.Equal(profile.ProviderResourceId, root.GetProperty("channel").GetString());
                Assert.False(root.GetProperty("mrkdwn").GetBoolean());
                break;
            case NotificationProviderKind.MicrosoftTeams:
                Assert.True(headers[0].StartsWith("POST /v1.0/teams/", StringComparison.Ordinal) && headers[0].Contains("/messages ", StringComparison.Ordinal));
                Assert.Equal("text", root.GetProperty("body").GetProperty("contentType").GetString());
                break;
            case NotificationProviderKind.Telegram:
                Assert.True(headers[0].StartsWith("POST /bot" + secret + "/sendMessage ", StringComparison.Ordinal), "Telegram credential belongs only in its fixed private Bot API path.");
                Assert.Equal(profile.ProviderResourceId, root.GetProperty("chat_id").GetInt64().ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;
            case NotificationProviderKind.PagerDuty:
                Assert.True(headers[0].StartsWith("POST /v2/enqueue ", StringComparison.Ordinal), "Expected PagerDuty method must match without exposing captured wire material.");
                Assert.True(FixedEquals(root.GetProperty("routing_key").GetString()!, secret), "PagerDuty routing credential must match its exact binding.");
                Assert.Equal(id.ToString("D"), root.GetProperty("dedup_key").GetString());
                Assert.Equal("trigger", root.GetProperty("event_action").GetString());
                break;
        }
    }

    private static bool FixedEquals(string first, string second) => CryptographicOperations.FixedTimeEquals(
        SHA256.HashData(Encoding.UTF8.GetBytes(first)), SHA256.HashData(Encoding.UTF8.GetBytes(second)));

    private sealed class ProviderNetwork(string host, int localPort) : INotificationHostAddressLookup, INotificationPinnedTcpDialer
    {
        public int Dials { get; private set; }
        public Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(string hostname, CancellationToken token)
        {
            Assert.Equal(host, hostname);
            return Task.FromResult<IReadOnlyList<IPAddress>>([IPAddress.Parse("1.1.1.1")]);
        }
        public ValueTask<Stream> DialAsync(IPAddress address, int port, CancellationToken token)
        {
            Assert.Equal("1.1.1.1", address.ToString());
            Assert.Equal(443, port);
            Dials++;
            return new SocketNotificationPinnedTcpDialer().DialAsync(IPAddress.Loopback, localPort, token);
        }
    }
}
