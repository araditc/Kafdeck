using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;
using TlsProvider = Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66SmtpPhysicalTlsTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("accepted")]
    [InlineData("auth-denied")]
    [InlineData("recipient-denied")]
    [InlineData("ack-lost")]
    [InlineData("no-auth")]
    [InlineData("oversized-reply")]
    [InlineData("wrong-hostname")]
    public async Task Hosted_smtp_requires_verified_implicit_tls_and_sasl_and_preserves_unknown_effects(string scenario)
    {
        var directory = Path.Combine(Path.GetTempPath(), $"kafdeck-smtp-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "ledger.db");
        var credentialPath = Path.Combine(directory, "credential");
        var secret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        var script = new SmtpScript(scenario, secret);
        await using var tls = new TlsProvider(certificateName: scenario == "wrong-hostname" ? "wrong.example.org" : "smtp.example.org", session: script.RunAsync);
        using var telemetry = new NotificationHttpTelemetryCapture();
        var profile = new NotificationDestinationProfile("smtp-profile", NotificationProviderKind.Email, "SMTP fixture",
            [NotificationEventClass.Operational], credentialBindingId: new NotificationCredentialBindingId("smtp-v1"),
            emailRecipientAddress: "ops@example.org", smtp: new NotificationSmtpProfile("smtp.example.org", "kafdeck@example.org", "kafdeck"));
        try
        {
            await File.WriteAllTextAsync(credentialPath, secret);
            var reference = SecretReference.Parse("file:" + credentialPath);
            var builder = Host.CreateApplicationBuilder();
            var options = new NotificationReadOptions(true, NotificationPersistenceProvider.Sqlite, NotificationExecutionMode.Standalone,
                path, null, DestinationProfiles: [profile], CredentialBindings:
                [new NotificationCredentialReferenceOptions(profile.DestinationId, profile.CredentialBindingId!, reference)], EmailDeliveryEnabled: true);
            builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([profile]));
            builder.Services.AddSingleton<INotificationCredentialResolver>(new ConfiguredNotificationCredentialResolver(
                [new ConfiguredNotificationCredentialBinding(profile.DestinationId, profile.Provider, profile.RevisionFingerprint,
                    profile.CredentialBindingId!, reference)], new SecretResolver()));
            var network = new SmtpNetwork(tls.Port);
            builder.Services.AddSingleton<INotificationHostAddressLookup>(network);
            builder.Services.AddSingleton<INotificationPinnedTcpDialer>(network);
            builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(network, NotificationAddressPolicy.NoConfiguredNat64,
                trustRoots: new X509Certificate2Collection(tls.Root)));
            var connections = new SqliteNotificationDeliveryDbConnectionFactory(path);
            var routing = new AdoNotificationRoutingStore(connections);
            var store = new AdoNotificationDeliveryStore(connections);
            builder.Services.AddSingleton<INotificationRoutingStore>(routing);
            builder.Services.AddSingleton<INotificationDeliveryStore>(store);
            builder.Services.AddKafdeckWebhookDelivery(options);
            using var host = builder.Build();
            await routing.InitializeAsync();
            await store.InitializeAsync();
            var now = DateTimeOffset.UtcNow;
            var notification = NotificationSafeEvent.Approved(Guid.NewGuid(), NotificationApprovedEventKind.ConsumerLagAlert, now);
            await routing.CreateOrGetEventAsync(notification, now);
            await store.CreateOrGetAsync(new NotificationDeliverySnapshot(notification.EventId, profile.DestinationId,
                notification.PayloadFingerprint, NotificationDeliveryState.Pending, 0, now,
                routedProfileRevisionFingerprint: profile.RevisionFingerprint), now);
            await host.StartAsync();
            var expected = scenario == "accepted" ? NotificationDeliveryState.Delivered :
                scenario is "auth-denied" or "recipient-denied" or "no-auth" ? NotificationDeliveryState.Exhausted : NotificationDeliveryState.UnknownExternalEffect;
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            NotificationDeliveryRecord? row;
            while ((row = await store.GetAsync(notification.EventId, profile.DestinationId, timeout.Token))?.Snapshot.State != expected)
                await Task.Delay(20, timeout.Token);
            await host.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync();
            await host.StopAsync();
            Assert.Equal(1, network.Dials);
            Assert.Equal("smtp.example.org", tls.ServerName);
            Assert.Equal(1, row!.Snapshot.AttemptCount);
            Assert.Equal(scenario is "no-auth" or "wrong-hostname" ? 0 : 1, script.AuthAttempts);
            Assert.Equal(scenario is "accepted" or "ack-lost" or "oversized-reply" ? 1 : 0, script.DataCount);
            if (script.AuthAttempts > 0) Assert.True(script.AuthMatched, "SMTP SASL credential must match its server-owned binding.");
            Assert.False(script.CredentialBodyLeak);
            if (scenario == "accepted") Assert.Equal(64, row.Snapshot.ProviderReceiptFingerprint?.Length);
            else Assert.Null(row.Snapshot.ProviderReceiptFingerprint);
            var telemetryLeak = telemetry.Contains(secret) || telemetry.Contains(script.ExpectedAuth);
            Assert.False(telemetryLeak, "SMTP credentials must not enter HTTP telemetry.");
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(directory, "ledger.db*"))
            {
                var text = Encoding.UTF8.GetString(await File.ReadAllBytesAsync(file));
                var leaked = text.Contains(secret, StringComparison.Ordinal) || text.Contains(script.ExpectedAuth, StringComparison.Ordinal);
                Assert.False(leaked, "SMTP credentials must not enter ledger storage.");
            }
            output.WriteLine($"REAL_PROVIDER_TLS provider=Email; scenario={scenario}; implicitTLS=true; SMTPauth={script.AuthAttempts}; physicalDATA={script.DataCount}; ledger={expected}; attempts=1; receiptSHA256={(row.Snapshot.ProviderReceiptFingerprint is null ? "none" : "present")}; automaticResends=0; credentialLeaks=0");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
            Directory.Delete(directory);
        }
    }

    [Fact]
    public void Smtp_configuration_is_typed_and_rejects_inline_passwords_or_command_material()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Notifications:Enabled"] = "true",
            ["Kafdeck:Notifications:EmailDeliveryEnabled"] = "true",
            ["Kafdeck:Notifications:Destinations:0:DestinationId"] = "email",
            ["Kafdeck:Notifications:Destinations:0:Provider"] = "Email",
            ["Kafdeck:Notifications:Destinations:0:DisplayName"] = "Email",
            ["Kafdeck:Notifications:Destinations:0:CredentialBindingId"] = "email-v1",
            ["Kafdeck:Notifications:Destinations:0:EnabledEvents:0"] = "Operational",
            ["Kafdeck:Notifications:Destinations:0:EmailRecipientAddress"] = "ops@example.org",
            ["Kafdeck:Notifications:Destinations:0:Smtp:Host"] = "smtp.example.org",
            ["Kafdeck:Notifications:Destinations:0:Smtp:SenderAddress"] = "kafdeck@example.org",
            ["Kafdeck:Notifications:Destinations:0:Smtp:Username"] = "kafdeck",
        };
        var parsed = KafdeckConfigurationLoader.Load(new ConfigurationBuilder().AddInMemoryCollection(values).Build());
        Assert.True(parsed.Notifications!.EmailDeliveryEnabled);
        Assert.Equal(465, parsed.Notifications.DestinationProfiles![0].Smtp!.Endpoint.Port);
        values["Kafdeck:Notifications:Destinations:0:Smtp:Password"] = "inline-secret-is-forbidden";
        Assert.Throws<KafdeckConfigurationException>(() => KafdeckConfigurationLoader.Load(new ConfigurationBuilder().AddInMemoryCollection(values).Build()));
        Assert.Throws<ArgumentException>(() => new NotificationSmtpProfile("smtp.example.org\r\nattacker", "kafdeck@example.org", "kafdeck"));
    }

    private sealed class SmtpNetwork(int port) : INotificationHostAddressLookup, INotificationPinnedTcpDialer
    {
        public int Dials { get; private set; }
        public Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(string host, CancellationToken token)
        {
            Assert.Equal("smtp.example.org", host);
            return Task.FromResult<IReadOnlyList<IPAddress>>([IPAddress.Parse("1.1.1.1")]);
        }
        public ValueTask<Stream> DialAsync(IPAddress address, int requestedPort, CancellationToken token)
        {
            Assert.Equal("1.1.1.1", address.ToString());
            Assert.Equal(465, requestedPort);
            Dials++;
            return new SocketNotificationPinnedTcpDialer().DialAsync(IPAddress.Loopback, port, token);
        }
    }

    internal sealed class SmtpScript(string scenario, string secret)
    {
        public string ExpectedAuth => Convert.ToBase64String(Encoding.UTF8.GetBytes("\0kafdeck\0" + secret));
        public int AuthAttempts { get; private set; }
        public int DataCount { get; private set; }
        public bool AuthMatched { get; private set; }
        public bool CredentialBodyLeak { get; private set; }
        public async Task RunAsync(SslStream tls, CancellationToken token)
        {
            using var reader = new StreamReader(tls, Encoding.ASCII, false, 1024, leaveOpen: true);
            await WriteAsync(tls, "220 fixture\r\n", token);
            var ehlo = await reader.ReadLineAsync(token);
            if (scenario == "wrong-hostname" && ehlo is null) return;
            CheckCommand("EHLO kafdeck.invalid", ehlo);
            if (scenario == "no-auth") { await WriteAsync(tls, "250 fixture\r\n", token); return; }
            await WriteAsync(tls, "250-fixture\r\n250 AUTH PLAIN\r\n", token);
            var auth = await reader.ReadLineAsync(token);
            AuthAttempts++;
            AuthMatched = auth is not null && CryptographicOperations.FixedTimeEquals(
                SHA256.HashData(Encoding.ASCII.GetBytes(auth)), SHA256.HashData(Encoding.ASCII.GetBytes("AUTH PLAIN " + ExpectedAuth)));
            if (scenario == "auth-denied") { await WriteAsync(tls, "535 5.7.8 denied\r\n", token); return; }
            await WriteAsync(tls, "235 2.7.0 authenticated\r\n", token);
            CheckCommand("MAIL FROM:<kafdeck@example.org>", await reader.ReadLineAsync(token));
            await WriteAsync(tls, "250 sender\r\n", token);
            CheckCommand("RCPT TO:<ops@example.org>", await reader.ReadLineAsync(token));
            if (scenario == "recipient-denied") { await WriteAsync(tls, "550 recipient\r\n", token); return; }
            await WriteAsync(tls, "250 recipient\r\n", token);
            CheckCommand("DATA", await reader.ReadLineAsync(token));
            await WriteAsync(tls, "354 send message\r\n", token);
            var content = new StringBuilder();
            while (content.Length < 65_536)
            {
                var line = await reader.ReadLineAsync(token);
                if (line is null) throw new IOException("Fixture closed before complete message.");
                if (line == ".") break;
                content.AppendLine(line);
            }
            DataCount++;
            CredentialBodyLeak = content.ToString().Contains(secret, StringComparison.Ordinal) || content.ToString().Contains(ExpectedAuth, StringComparison.Ordinal);
            if (scenario == "ack-lost") return;
            if (scenario == "oversized-reply") { await WriteAsync(tls, "250 " + new string('x', 512) + "\r\n", token); return; }
            await WriteAsync(tls, "250 2.0.0 queued-fixture-receipt\r\n", token);
        }
        private static Task WriteAsync(Stream stream, string value, CancellationToken token) => stream.WriteAsync(Encoding.ASCII.GetBytes(value), token).AsTask();
        private static void CheckCommand(string expected, string? actual)
        {
            var matches = actual is not null && CryptographicOperations.FixedTimeEquals(
                SHA256.HashData(Encoding.ASCII.GetBytes(expected)), SHA256.HashData(Encoding.ASCII.GetBytes(actual)));
            Assert.True(matches, "Expected SMTP protocol step must match without exposing captured wire material.");
        }
    }
}
