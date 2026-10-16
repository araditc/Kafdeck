using System.Net;
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

public sealed class V08W66AuthenticatedWebhookTlsTests(ITestOutputHelper output)
{
    [Fact]
    public async Task Physical_private_webhook_path_and_bearer_do_not_enter_http_telemetry()
    {
        await using var tls = new TlsProvider();
        using var telemetry = new NotificationHttpTelemetryCapture();
        var directory = Path.Combine(Path.GetTempPath(), $"kafdeck-w66-private-path-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var secretPath = Path.Combine(directory, "credential");
        var pathSecret = Convert.ToHexString(RandomNumberGenerator.GetBytes(24)).ToLowerInvariant();
        var bearer = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        var endpoint = new Uri($"https://hooks.example.org:{tls.Port}/private/{pathSecret}");
        var profile = new NotificationDestinationProfile("lab-webhook", NotificationProviderKind.Webhook,
            "Laboratory", [NotificationEventClass.Operational], endpoint,
            new NotificationCredentialBindingId("webhook-private-v1"),
            webhookAuthentication: NotificationWebhookAuthentication.Bearer);
        try
        {
            await File.WriteAllTextAsync(secretPath, bearer);
            using (var host = Host(Path.Combine(directory, "deliveries.db"), profile,
                SecretReference.Parse("file:" + secretPath), tls))
            {
                var id = await SeedAsync(host.Services, profile);
                await host.StartAsync();
                await WaitAsync(host.Services, id, NotificationDeliveryState.Delivered);
                await host.StopAsync();
            }
            Assert.Equal(1, tls.RequestCount);
            var request = Assert.Single(tls.Requests);
            Assert.True(request.StartsWith("POST " + endpoint.PathAndQuery + " HTTP/1.1\r\n", StringComparison.Ordinal),
                "The physical wire must retain the exact private endpoint path.");
            Assert.True(HasExactBearer(request, bearer), "The physical wire must carry the admitted credential.");
            var leaked = telemetry.Contains(pathSecret) || telemetry.Contains(bearer);
            Assert.False(leaked, "Private URL paths and Bearer credentials must not enter HTTP EventSource or Activity data.");
            SqliteConnection.ClearAllPools();
            foreach (var candidate in Directory.GetFiles(directory, "deliveries.db*"))
            {
                var text = Encoding.UTF8.GetString(await File.ReadAllBytesAsync(candidate));
                Assert.False(text.Contains(pathSecret, StringComparison.Ordinal) || text.Contains(bearer, StringComparison.Ordinal),
                    "Private URL paths and credentials must not enter the durable delivery ledger.");
            }
            output.WriteLine("REAL_TLS_PRIVATE_PATH HTTP204=1; exactPrivatePath=true; bearer=true; HTTPEventSourceAndActivityCredentialLeaks=0; ledgerCredentialLeaks=0");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var candidate in Directory.GetFiles(directory)) File.Delete(candidate);
            Directory.Delete(directory);
        }
    }

    [Fact]
    public async Task Http_telemetry_capture_observes_real_http_request_paths_and_activity_tags()
    {
        using var telemetry = new NotificationHttpTelemetryCapture();
        const string pathCanary = "nonsecret-w66-eventsource-canary";
        const string activityCanary = "nonsecret-w66-activity-canary";
        using (var source = new System.Diagnostics.ActivitySource("System.Net.Http.W66CaptureCanary"))
        using (var activity = source.StartActivity("capture-canary"))
            activity?.SetTag("url.full", activityCanary);
        using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false })
        {
            Timeout = TimeSpan.FromSeconds(2),
        };
        try { using var response = await client.GetAsync("http://127.0.0.1:1/" + pathCanary); }
        catch (HttpRequestException) { }
        catch (OperationCanceledException) { }
        Assert.True(telemetry.Contains(pathCanary), "The EventSource listener must observe a real HttpClient request path.");
        Assert.True(telemetry.Contains(activityCanary), "The Activity listener must observe recorded HTTP activity tags.");
    }

    [Fact]
    public async Task Physical_bearer_rotation_fences_old_intents_and_revoke_disable_missing_secret_never_downgrade()
    {
        await using var tls = new TlsProvider();
        var directory = Path.Combine(Path.GetTempPath(), $"kafdeck-w66-auth-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "deliveries.db");
        var secretPath = Path.Combine(directory, "credential");
        var firstSecret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        var rotatedSecret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
        var reference = SecretReference.Parse("file:" + secretPath);
        var first = Profile(tls, "webhook-key-v1");
        var rotated = Profile(tls, "webhook-key-v2");
        try
        {
            await File.WriteAllTextAsync(secretPath, firstSecret);
            using (var host = Host(path, first, reference, tls))
            {
                var id = await SeedAsync(host.Services, first);
                await host.StartAsync();
                await WaitAsync(host.Services, id, NotificationDeliveryState.Delivered);
                await host.StopAsync();
            }
            Assert.Equal(1, tls.RequestCount);
            Assert.True(HasExactBearer(tls.Requests.Single(), firstSecret), "Physical provider must receive only the admitted Bearer credential.");
            await File.WriteAllTextAsync(secretPath, rotatedSecret);
            Guid oldIntent;
            Guid currentIntent;
            using (var host = Host(path, rotated, reference, tls))
            {
                oldIntent = await SeedAsync(host.Services, first);
                currentIntent = await SeedAsync(host.Services, rotated);
                await host.StartAsync();
                await WaitAsync(host.Services, oldIntent, NotificationDeliveryState.UnknownExternalEffect);
                await WaitAsync(host.Services, currentIntent, NotificationDeliveryState.Delivered);
                await host.StopAsync();
            }
            Assert.NotEqual(first.RevisionFingerprint, rotated.RevisionFingerprint);
            Assert.Equal(2, tls.RequestCount);
            Assert.True(HasExactBearer(tls.Requests.Last(), rotatedSecret), "Rotated binding must use its current server-owned secret.");
            foreach (var state in new[] { "revoked", "missing", "disabled" })
            {
                if (state == "missing") File.Delete(secretPath);
                var profile = state == "disabled" ? Profile(tls, "webhook-key-v2", enabled: false) : rotated;
                using var host = Host(path, profile, reference, tls, bindingEnabled: state != "revoked");
                var id = await SeedAsync(host.Services, rotated);
                await host.StartAsync();
                await WaitAsync(host.Services, id, state == "disabled"
                    ? NotificationDeliveryState.Exhausted : NotificationDeliveryState.UnknownExternalEffect);
                await host.StopAsync();
                Assert.Equal(2, tls.RequestCount);
            }
            foreach (var request in tls.Requests)
            {
                var body = request[(request.IndexOf("\r\n\r\n", StringComparison.Ordinal) + 4)..];
                Assert.False(body.Contains(firstSecret, StringComparison.Ordinal) || body.Contains(rotatedSecret, StringComparison.Ordinal),
                    "Credentials must never appear in the safe event body.");
            }
            SqliteConnection.ClearAllPools();
            foreach (var candidate in Directory.GetFiles(directory, "deliveries.db*"))
            {
                var bytes = await File.ReadAllBytesAsync(candidate);
                var text = Encoding.UTF8.GetString(bytes);
                Assert.False(text.Contains(firstSecret, StringComparison.Ordinal) || text.Contains(rotatedSecret, StringComparison.Ordinal),
                    "Credentials must never appear in the persistent ledger.");
            }
            output.WriteLine("REAL_TLS_BEARER HTTP204=2; versionedRotation=true; oldRevision=UnknownExternalEffect; revoked=UnknownExternalEffect; missing=UnknownExternalEffect; disabled=Exhausted; anonymousFallbacks=0; credentialBodyLeaks=0; credentialLedgerLeaks=0");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var candidate in Directory.GetFiles(directory)) File.Delete(candidate);
            Directory.Delete(directory);
        }
    }

    [Theory]
    [InlineData("contains space")]
    [InlineData("bad\r\nX-Header: injected")]
    [InlineData("not-a-token:wrong")]
    [InlineData("=padding-first")]
    [InlineData("token=mid-token")]
    public async Task Invalid_bearer_material_is_rejected_before_any_socket(string credential)
    {
        await using var tls = new TlsProvider();
        var profile = Profile(tls, "webhook-key-v1");
        var transport = new NotificationPinnedWebhookHttpTransport(profile,
            new NotificationPinnedHttpHandlerFactory(tls, NotificationAddressPolicy.NoConfiguredNat64,
                trustRoots: new X509Certificate2Collection(tls.Root)));
        var request = new NotificationPinnedWebhookRequest(profile.DestinationId, profile.RevisionFingerprint,
            NotificationAddressPolicy.NoConfiguredNat64.ValidatePinnedEndpoint(profile.ConfiguredEndpoint!,
                [IPAddress.Parse("1.1.1.1")], DateTimeOffset.UtcNow), Encoding.UTF8.GetBytes("{}"),
            new NotificationCredentialValue(credential));
        var error = await Assert.ThrowsAsync<KafdeckConfigurationException>(() => transport.SendAsync(request, CancellationToken.None));
        Assert.False(error.ToString().Contains(credential, StringComparison.Ordinal), "Credential must not appear in an error.");
        Assert.Empty(tls.Dialed);
    }

    [Fact]
    public void Anonymous_fingerprint_is_compatible_and_authentication_is_closed_and_revision_bound()
    {
        const string canonical = "lab-webhook\n1\nhttps://hooks.example.org/notify\n\n1";
        var anonymous = new NotificationDestinationProfile("lab-webhook", NotificationProviderKind.Webhook,
            "Laboratory", [NotificationEventClass.Operational], new Uri("https://hooks.example.org/notify"));
        Assert.Equal(Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical))).ToLowerInvariant(), anonymous.RevisionFingerprint);
        Assert.Throws<ArgumentException>(() => new NotificationDestinationProfile("lab-webhook", NotificationProviderKind.Webhook,
            "Laboratory", [NotificationEventClass.Operational], anonymous.ConfiguredEndpoint,
            webhookAuthentication: NotificationWebhookAuthentication.Bearer));
        Assert.Throws<ArgumentException>(() => new NotificationDestinationProfile("lab-webhook", NotificationProviderKind.Webhook,
            "Laboratory", [NotificationEventClass.Operational], anonymous.ConfiguredEndpoint,
            webhookAuthentication: (NotificationWebhookAuthentication)99));
    }

    [Fact]
    public void Configuration_parses_explicit_authentication_and_disable_without_raw_secrets()
    {
        var settings = new Dictionary<string, string?>
        {
            ["Kafdeck:Notifications:Enabled"] = "true",
            ["Kafdeck:Notifications:Destinations:0:DestinationId"] = "lab-webhook",
            ["Kafdeck:Notifications:Destinations:0:Provider"] = "Webhook",
            ["Kafdeck:Notifications:Destinations:0:DisplayName"] = "Laboratory",
            ["Kafdeck:Notifications:Destinations:0:EnabledEvents:0"] = "Operational",
            ["Kafdeck:Notifications:Destinations:0:ConfiguredEndpoint"] = "https://hooks.example.org/notify",
            ["Kafdeck:Notifications:Destinations:0:CredentialBindingId"] = "webhook-v1",
            ["Kafdeck:Notifications:Destinations:0:WebhookAuthentication"] = "Bearer",
            ["Kafdeck:Notifications:Destinations:0:Enabled"] = "false",
            ["Kafdeck:Notifications:CredentialBindings:0:DestinationId"] = "lab-webhook",
            ["Kafdeck:Notifications:CredentialBindings:0:BindingId"] = "webhook-v1",
            ["Kafdeck:Notifications:CredentialBindings:0:SecretReference"] = "env:KAFDECK_W66_LAB_KEY",
            ["Kafdeck:Notifications:CredentialBindings:0:Enabled"] = "false",
        };
        var options = KafdeckConfigurationLoader.Load(new ConfigurationBuilder().AddInMemoryCollection(settings).Build());
        var profile = Assert.Single(options.Notifications!.DestinationProfiles!);
        Assert.Equal(NotificationWebhookAuthentication.Bearer, profile.WebhookAuthentication);
        Assert.False(profile.Enabled);
        Assert.False(Assert.Single(options.Notifications.CredentialBindings!).Enabled);
        settings["Kafdeck:Notifications:Destinations:0:WebhookAuthentication"] = "Basic";
        Assert.Throws<KafdeckConfigurationException>(() => KafdeckConfigurationLoader.Load(new ConfigurationBuilder().AddInMemoryCollection(settings).Build()));
    }

    private static bool HasExactBearer(string request, string expected)
    {
        var headers = request[..request.IndexOf("\r\n\r\n", StringComparison.Ordinal)].Split("\r\n");
        var header = headers.SingleOrDefault(line => line.StartsWith("Authorization:", StringComparison.OrdinalIgnoreCase));
        var actual = header is null ? string.Empty : header[(header.IndexOf(':') + 1)..].Trim();
        return CryptographicOperations.FixedTimeEquals(SHA256.HashData(Encoding.UTF8.GetBytes("Bearer " + expected)),
            SHA256.HashData(Encoding.UTF8.GetBytes(actual)));
    }

    private static NotificationDestinationProfile Profile(TlsProvider tls, string binding, bool enabled = true) =>
        new("lab-webhook", NotificationProviderKind.Webhook, "Laboratory", [NotificationEventClass.Operational],
            new Uri($"https://hooks.example.org:{tls.Port}/notify"), new NotificationCredentialBindingId(binding),
            webhookAuthentication: NotificationWebhookAuthentication.Bearer, enabled: enabled);

    private static IHost Host(string path, NotificationDestinationProfile profile, SecretReference reference,
        TlsProvider tls, bool bindingEnabled = true)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        var bindings = new[] { new NotificationCredentialReferenceOptions(profile.DestinationId,
            profile.CredentialBindingId!, reference, bindingEnabled) };
        var options = new NotificationReadOptions(true, NotificationPersistenceProvider.Sqlite, NotificationExecutionMode.Standalone,
            path, null, DestinationProfiles: [profile], CredentialBindings: bindings, WebhookDeliveryEnabled: true);
        builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([profile]));
        builder.Services.AddSingleton<INotificationCredentialResolver>(new ConfiguredNotificationCredentialResolver(
            [new ConfiguredNotificationCredentialBinding(profile.DestinationId, profile.Provider, profile.RevisionFingerprint,
                profile.CredentialBindingId!, reference, bindingEnabled)], new SecretResolver()));
        builder.Services.AddSingleton<INotificationHostAddressLookup>(tls);
        builder.Services.AddSingleton<INotificationPinnedTcpDialer>(tls);
        builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(tls, NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots: new X509Certificate2Collection(tls.Root)));
        var connections = new SqliteNotificationDeliveryDbConnectionFactory(path);
        builder.Services.AddSingleton<INotificationRoutingStore>(new AdoNotificationRoutingStore(connections));
        builder.Services.AddSingleton<INotificationDeliveryStore>(new AdoNotificationDeliveryStore(connections));
        builder.Services.AddKafdeckWebhookDelivery(options);
        return builder.Build();
    }

    private static async Task<Guid> SeedAsync(IServiceProvider services, NotificationDestinationProfile routed)
    {
        var routing = services.GetRequiredService<INotificationRoutingStore>();
        var deliveries = services.GetRequiredService<INotificationDeliveryStore>();
        await routing.InitializeAsync();
        await deliveries.InitializeAsync();
        var now = DateTimeOffset.UtcNow;
        var notification = NotificationSafeEvent.Approved(Guid.NewGuid(), NotificationApprovedEventKind.ConsumerLagAlert, now);
        await routing.CreateOrGetEventAsync(notification, now);
        await deliveries.CreateOrGetAsync(new NotificationDeliverySnapshot(notification.EventId, routed.DestinationId,
            notification.PayloadFingerprint, NotificationDeliveryState.Pending, 0, now,
            routedProfileRevisionFingerprint: routed.RevisionFingerprint), now);
        return notification.EventId;
    }

    private static async Task WaitAsync(IServiceProvider services, Guid id, NotificationDeliveryState state)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var store = services.GetRequiredService<INotificationDeliveryStore>();
        while ((await store.GetAsync(id, "lab-webhook", timeout.Token))?.Snapshot.State != state)
            await Task.Delay(20, timeout.Token);
    }
}
