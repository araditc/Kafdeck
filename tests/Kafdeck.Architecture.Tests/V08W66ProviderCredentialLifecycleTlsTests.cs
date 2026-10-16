using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
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
using SmtpScript = Kafdeck.Architecture.Tests.V08W66SmtpPhysicalTlsTests.SmtpScript;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66ProviderCredentialLifecycleTlsTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(NotificationProviderKind.Email)]
    [InlineData(NotificationProviderKind.Slack)]
    [InlineData(NotificationProviderKind.MicrosoftTeams)]
    [InlineData(NotificationProviderKind.Telegram)]
    [InlineData(NotificationProviderKind.PagerDuty)]
    public async Task Physical_provider_rotation_fences_old_revision_and_revoked_missing_disabled_never_send_or_downgrade(NotificationProviderKind provider)
    {
        var directory = Path.Combine(Path.GetTempPath(), $"kafdeck-provider-lifecycle-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "ledger.db");
        var oldSecretPath = Path.Combine(directory, "credential-v1");
        var currentSecretPath = Path.Combine(directory, "credential-v2");
        var oldSecret = Credential(provider);
        var currentSecret = Credential(provider);
        var firstId = Guid.NewGuid();
        var currentId = firstId;
        var firstSmtp = new SmtpScript("accepted", oldSecret);
        var secondSmtp = new SmtpScript("accepted", currentSecret);
        var smtp = firstSmtp;
        var hostname = provider == NotificationProviderKind.Email ? "smtp.example.org" : V08W66ProviderPhysicalTlsTests.Hostname(provider);
        var status = provider == NotificationProviderKind.MicrosoftTeams ? 201 : provider == NotificationProviderKind.PagerDuty ? 202 : 200;
        await using var tls = new TlsProvider(certificateName: hostname, statusCode: status,
            responseBody: _ => V08W66ProviderPhysicalTlsTests.Response(provider, "accepted", currentId),
            session: provider == NotificationProviderKind.Email ? (stream, token) => smtp.RunAsync(stream, token) : null);
        var network = new LifecycleNetwork(hostname, tls.Port, provider == NotificationProviderKind.Email ? 465 : 443);
        var first = Profile(provider, "provider-v1");
        var rotated = Profile(provider, "provider-v2");
        using var telemetry = new NotificationHttpTelemetryCapture();
        try
        {
            await File.WriteAllTextAsync(oldSecretPath, oldSecret);
            await File.WriteAllTextAsync(currentSecretPath, currentSecret);
            using (var host = CreateHost(path, first, SecretReference.Parse("file:" + oldSecretPath), tls, network))
            {
                await SeedAsync(host.Services, first, firstId);
                await host.StartAsync();
                await WaitAsync(host.Services, first, firstId, NotificationDeliveryState.Delivered);
                await host.StopAsync();
            }
            if (provider == NotificationProviderKind.Email)
            {
                Assert.Equal(1, firstSmtp.DataCount);
                Assert.True(firstSmtp.AuthMatched, "The first physical SMTP session must use its admitted binding.");
                smtp = secondSmtp;
            }
            else V08W66ProviderPhysicalTlsTests.VerifyWire(provider, Assert.Single(tls.Requests), first, firstId, oldSecret);

            // Old mounted material is removed. The new host admits only v2; an
            // old pending revision must never resolve or downgrade through v2.
            File.Delete(oldSecretPath);
            var oldIntent = Guid.NewGuid();
            currentId = Guid.NewGuid();
            using (var host = CreateHost(path, rotated, SecretReference.Parse("file:" + currentSecretPath), tls, network))
            {
                await SeedAsync(host.Services, first, oldIntent);
                await SeedAsync(host.Services, rotated, currentId);
                await host.StartAsync();
                await WaitAsync(host.Services, rotated, oldIntent, NotificationDeliveryState.UnknownExternalEffect);
                var delivered = await WaitAsync(host.Services, rotated, currentId, NotificationDeliveryState.Delivered);
                Assert.Equal(64, delivered.Snapshot.ProviderReceiptFingerprint?.Length);
                await host.StopAsync();
            }
            Assert.NotEqual(first.RevisionFingerprint, rotated.RevisionFingerprint);
            Assert.Equal(2, network.Dials);
            if (provider == NotificationProviderKind.Email)
            {
                Assert.Equal(1, secondSmtp.DataCount);
                Assert.True(secondSmtp.AuthMatched, "The rotated physical SMTP session must use the new binding.");
            }
            else
            {
                Assert.Equal(2, tls.RequestCount);
                V08W66ProviderPhysicalTlsTests.VerifyWire(provider, tls.Requests.Last(), rotated, currentId, currentSecret);
            }

            foreach (var state in new[] { "revoked", "missing", "disabled" })
            {
                if (state == "missing") File.Delete(currentSecretPath);
                var admitted = Profile(provider, "provider-v2", enabled: state != "disabled");
                using var host = CreateHost(path, admitted, SecretReference.Parse("file:" + currentSecretPath), tls, network,
                    bindingEnabled: state != "revoked");
                var id = Guid.NewGuid();
                await SeedAsync(host.Services, rotated, id);
                await host.StartAsync();
                await WaitAsync(host.Services, rotated, id, state == "disabled"
                    ? NotificationDeliveryState.Exhausted : NotificationDeliveryState.UnknownExternalEffect);
                await host.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync();
                await host.StopAsync();
                Assert.Equal(2, network.Dials);
            }

            var leaked = telemetry.Contains(oldSecret) || telemetry.Contains(currentSecret);
            if (provider == NotificationProviderKind.Email)
            {
                leaked |= telemetry.Contains(firstSmtp.ExpectedAuth) || telemetry.Contains(secondSmtp.ExpectedAuth);
                Assert.Equal(2, firstSmtp.AuthAttempts + secondSmtp.AuthAttempts);
                Assert.Equal(2, firstSmtp.DataCount + secondSmtp.DataCount);
                Assert.False(firstSmtp.CredentialBodyLeak || secondSmtp.CredentialBodyLeak);
            }
            else Assert.Equal(2, tls.RequestCount);
            Assert.False(leaked, "Versioned provider credentials must not enter HTTP telemetry.");
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(directory, "ledger.db*"))
            {
                var content = Encoding.UTF8.GetString(await File.ReadAllBytesAsync(file));
                var ledgerLeak = content.Contains(oldSecret, StringComparison.Ordinal) || content.Contains(currentSecret, StringComparison.Ordinal);
                if (provider == NotificationProviderKind.Email)
                    ledgerLeak |= content.Contains(firstSmtp.ExpectedAuth, StringComparison.Ordinal) || content.Contains(secondSmtp.ExpectedAuth, StringComparison.Ordinal);
                Assert.False(ledgerLeak, "Versioned credentials must not enter the durable ledger.");
            }
            output.WriteLine($"REAL_PROVIDER_CREDENTIAL_LIFECYCLE provider={provider}; physicalAuthenticatedSends=2; versionedRotation=true; oldBindingRemoved=true; oldRevision=UnknownExternalEffect; revoked=UnknownExternalEffect; missing=UnknownExternalEffect; disabled=Exhausted; anonymousFallbacks=0; extraDials=0; credentialLeaks=0; restartProvisioned=true; hotCrossNodeRevocation=false");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(directory)) File.Delete(file);
            Directory.Delete(directory);
        }
    }

    private static NotificationDestinationProfile Profile(NotificationProviderKind provider, string binding, bool enabled = true)
    {
        if (provider == NotificationProviderKind.Email)
            return new("lifecycle-provider", provider, "Lifecycle fixture", [NotificationEventClass.Operational],
                credentialBindingId: new NotificationCredentialBindingId(binding), emailRecipientAddress: "ops@example.org",
                enabled: enabled, smtp: new NotificationSmtpProfile("smtp.example.org", "kafdeck@example.org", "kafdeck"));
        var template = V08W66ProviderPhysicalTlsTests.Profile(provider);
        return new("lifecycle-provider", provider, "Lifecycle fixture", template.EnabledEvents,
            credentialBindingId: new NotificationCredentialBindingId(binding), enabled: enabled,
            providerResourceId: template.ProviderResourceId, providerSecondaryResourceId: template.ProviderSecondaryResourceId);
    }

    private static string Credential(NotificationProviderKind provider) => provider switch
    {
        NotificationProviderKind.Telegram => "123456:" + Convert.ToHexString(RandomNumberGenerator.GetBytes(24)).ToLowerInvariant(),
        NotificationProviderKind.PagerDuty => Convert.ToHexString(RandomNumberGenerator.GetBytes(16)).ToLowerInvariant(),
        _ => Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)),
    };

    private static IHost CreateHost(string path, NotificationDestinationProfile profile, SecretReference reference,
        TlsProvider tls, LifecycleNetwork network, bool bindingEnabled = true)
    {
        var builder = Host.CreateApplicationBuilder();
        var options = new NotificationReadOptions(true, NotificationPersistenceProvider.Sqlite, NotificationExecutionMode.Standalone,
            path, null, DestinationProfiles: [profile], CredentialBindings:
            [new NotificationCredentialReferenceOptions(profile.DestinationId, profile.CredentialBindingId!, reference, bindingEnabled)],
            HttpProviderDeliveryEnabled: profile.Provider != NotificationProviderKind.Email,
            EmailDeliveryEnabled: profile.Provider == NotificationProviderKind.Email);
        builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([profile]));
        builder.Services.AddSingleton<INotificationCredentialResolver>(new ConfiguredNotificationCredentialResolver(
            [new ConfiguredNotificationCredentialBinding(profile.DestinationId, profile.Provider, profile.RevisionFingerprint,
                profile.CredentialBindingId!, reference, bindingEnabled)], new SecretResolver()));
        builder.Services.AddSingleton<INotificationHostAddressLookup>(network);
        builder.Services.AddSingleton<INotificationPinnedTcpDialer>(network);
        builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(network, NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots: new X509Certificate2Collection(tls.Root)));
        var connections = new SqliteNotificationDeliveryDbConnectionFactory(path);
        builder.Services.AddSingleton<INotificationRoutingStore>(new AdoNotificationRoutingStore(connections));
        builder.Services.AddSingleton<INotificationDeliveryStore>(new AdoNotificationDeliveryStore(connections));
        builder.Services.AddKafdeckWebhookDelivery(options);
        return builder.Build();
    }

    private static async Task SeedAsync(IServiceProvider services, NotificationDestinationProfile profile, Guid id)
    {
        var routing = services.GetRequiredService<INotificationRoutingStore>();
        var store = services.GetRequiredService<INotificationDeliveryStore>();
        await routing.InitializeAsync();
        await store.InitializeAsync();
        var now = DateTimeOffset.UtcNow;
        var notification = NotificationSafeEvent.Approved(id, NotificationApprovedEventKind.ConsumerLagAlert, now);
        await routing.CreateOrGetEventAsync(notification, now);
        await store.CreateOrGetAsync(new NotificationDeliverySnapshot(id, profile.DestinationId, notification.PayloadFingerprint,
            NotificationDeliveryState.Pending, 0, now, routedProfileRevisionFingerprint: profile.RevisionFingerprint), now);
    }

    private static async Task<NotificationDeliveryRecord> WaitAsync(IServiceProvider services, NotificationDestinationProfile profile,
        Guid id, NotificationDeliveryState expected)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var store = services.GetRequiredService<INotificationDeliveryStore>();
        while (true)
        {
            var row = await store.GetAsync(id, profile.DestinationId, deadline.Token);
            if (row?.Snapshot.State == expected) return row;
            await Task.Delay(20, deadline.Token);
        }
    }

    private sealed class LifecycleNetwork(string host, int localPort, int approvedPort) : INotificationHostAddressLookup, INotificationPinnedTcpDialer
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
            Assert.Equal(approvedPort, port);
            Dials++;
            return new SocketNotificationPinnedTcpDialer().DialAsync(IPAddress.Loopback, localPort, token);
        }
    }
}
