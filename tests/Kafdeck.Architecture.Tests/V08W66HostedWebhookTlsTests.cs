using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66HostedWebhookTlsTests(ITestOutputHelper output)
{
    [Fact]
    public void Host_delivery_is_default_off_and_rejects_unadmitted_activation()
    {
        var options = new NotificationReadOptions(true, NotificationPersistenceProvider.Sqlite,
            NotificationExecutionMode.Standalone, Path.GetFullPath("unused.db"), null);
        var services = new ServiceCollection();
        services.AddKafdeckWebhookDelivery(options);
        Assert.DoesNotContain(services, item => item.ServiceType == typeof(IHostedService));
        Assert.DoesNotContain(services, item => item.ServiceType == typeof(INotificationPinnedWebhookTransport));
        Assert.Throws<KafdeckConfigurationException>(() =>
            services.AddKafdeckWebhookDelivery(options with { WebhookDeliveryEnabled = true }));
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData("false", false)]
    [InlineData("true", true)]
    public void Physical_delivery_configuration_requires_explicit_boolean(string? setting, bool expected)
    {
        var settings = new Dictionary<string, string?> { ["Kafdeck:Notifications:Enabled"] = "true" };
        if (setting is not null) settings["Kafdeck:Notifications:WebhookDeliveryEnabled"] = setting;
        var options = KafdeckConfigurationLoader.Load(new ConfigurationBuilder().AddInMemoryCollection(settings).Build());
        Assert.Equal(expected, options.Notifications!.WebhookDeliveryEnabled);
    }

    [Fact]
    public void Invalid_physical_delivery_boolean_is_rejected()
    {
        var settings = new Dictionary<string, string?> { ["Kafdeck:Notifications:WebhookDeliveryEnabled"] = "sometimes" };
        Assert.Throws<KafdeckConfigurationException>(() => KafdeckConfigurationLoader.Load(
            new ConfigurationBuilder().AddInMemoryCollection(settings).Build()));
    }

    [Fact]
    public async Task Hosted_worker_physically_posts_with_verified_tls_and_persists_receipt()
    {
        await using var fixture = new TlsProvider();
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-webhook-tls-{Guid.NewGuid():N}.db");
        try
        {
            using var host = CreateHost(path, fixture);
            var id = await SeedAsync(host.Services);
            await host.StartAsync();
            var request = await ReceiveAsync(fixture, host.Services, id);
            var ledger = host.Services.GetRequiredService<INotificationDeliveryStore>();
            var row = await WaitForStateAsync(ledger, id, NotificationDeliveryState.Delivered);
            await host.StopAsync();

            Assert.Equal("hooks.example.org", fixture.ServerName);
            Assert.StartsWith("POST /notify HTTP/1.1", request, StringComparison.Ordinal);
            Assert.Contains($"Host: hooks.example.org:{fixture.Port}", request, StringComparison.Ordinal);
            var body = request[(request.IndexOf("\r\n\r\n", StringComparison.Ordinal) + 4)..];
            using var json = JsonDocument.Parse(body);
            Assert.Equal(id, json.RootElement.GetProperty("eventId").GetGuid());
            Assert.Equal("consumer-lag", json.RootElement.GetProperty("eventType").GetString());
            Assert.Equal(1, row.Snapshot.AttemptCount);
            Assert.Equal(NotificationDeliveryOutcomeCodes.Delivered, row.Snapshot.OutcomeCode);

            var reopened = new AdoNotificationDeliveryStore(new SqliteNotificationDeliveryDbConnectionFactory(path));
            var durable = await reopened.GetAsync(id, "lab-webhook");
            Assert.Equal(NotificationDeliveryState.Delivered, durable!.Snapshot.State);
            Assert.Equal(row.Snapshot.PayloadFingerprint, durable.Snapshot.PayloadFingerprint);
            Assert.All(fixture.Dialed, address => Assert.Equal("1.1.1.1", address));
            output.WriteLine($"REAL_TLS_POST event={id:D}; SNI={fixture.ServerName}; HTTP=204; ledger=Delivered; attempts=1; persistedAcrossConnectionReopen=true; payloadSHA256={durable.Snapshot.PayloadFingerprint}");
        }
        finally { Cleanup(path); }
    }

    [Fact]
    public async Task Physical_send_interrupted_before_receipt_recovers_unknown_without_resend()
    {
        await using var fixture = new TlsProvider(holdResponse: true);
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-webhook-recovery-{Guid.NewGuid():N}.db");
        try
        {
            Guid id;
            using (var first = CreateHost(path, fixture))
            {
                id = await SeedAsync(first.Services);
                await first.StartAsync();
                await ReceiveAsync(fixture, first.Services, id);
                await first.StopAsync();
                var row = await first.Services.GetRequiredService<INotificationDeliveryStore>()
                    .GetAsync(id, "lab-webhook");
                Assert.Equal(NotificationDeliveryState.InFlight, row!.Snapshot.State);
            }

            using var restarted = CreateHost(path, fixture, DateTimeOffset.UtcNow.AddMinutes(3));
            await restarted.StartAsync();
            var store = restarted.Services.GetRequiredService<INotificationDeliveryStore>();
            var recovered = await WaitForStateAsync(store, id, NotificationDeliveryState.UnknownExternalEffect);
            await restarted.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync();
            await restarted.StopAsync();
            Assert.Equal(1, recovered.Snapshot.AttemptCount);
            Assert.Equal(1, fixture.RequestCount);
            Assert.Equal(NotificationDeliveryOutcomeCodes.UnknownExternalEffect, recovered.Snapshot.OutcomeCode);
            output.WriteLine($"REAL_TLS_AMBIGUOUS event={id:D}; physicalPOSTs=1; stopAfterProviderRead=true; preRestart=InFlight; postRestart=UnknownExternalEffect; attempts=1; blindResends=0");
        }
        finally { Cleanup(path); }
    }

    [Theory]
    [InlineData("wrong.example.org", true)]
    [InlineData("hooks.example.org", false)]
    public async Task Invalid_hostname_or_untrusted_certificate_never_reaches_provider_http(
        string certificateName, bool trustCertificate)
    {
        await using var fixture = new TlsProvider(certificateName: certificateName);
        var profile = Profile(fixture);
        var handlers = new NotificationPinnedHttpHandlerFactory(fixture,
            NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots: trustCertificate ? new X509Certificate2Collection(fixture.Root) : null);
        var request = new NotificationPinnedWebhookRequest(profile.DestinationId, profile.RevisionFingerprint,
            NotificationAddressPolicy.NoConfiguredNat64.ValidatePinnedEndpoint(profile.ConfiguredEndpoint!,
                [IPAddress.Parse("1.1.1.1")], DateTimeOffset.UtcNow), Encoding.UTF8.GetBytes("{}"), null);
        var result = await new NotificationPinnedWebhookHttpTransport(profile, handlers)
            .SendAsync(request, CancellationToken.None);
        Assert.Equal(NotificationWebhookTransportOutcome.UnknownExternalEffect, result.Outcome);
        Assert.Equal(0, fixture.RequestCount);
        Assert.Equal("hooks.example.org", fixture.ServerName);
        Assert.Single(fixture.Dialed);
    }

    [Fact]
    public async Task Physical_redirect_is_not_followed_and_ledger_retains_unknown_effect()
    {
        await using var fixture = new TlsProvider(statusCode: 302);
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-webhook-redirect-{Guid.NewGuid():N}.db");
        try
        {
            using var host = CreateHost(path, fixture);
            var id = await SeedAsync(host.Services);
            await host.StartAsync();
            await ReceiveAsync(fixture, host.Services, id);
            var row = await WaitForStateAsync(host.Services.GetRequiredService<INotificationDeliveryStore>(),
                id, NotificationDeliveryState.UnknownExternalEffect);
            await host.StopAsync();
            Assert.Equal(1, fixture.RequestCount);
            Assert.Single(fixture.Dialed);
            Assert.Equal(1, row.Snapshot.AttemptCount);
            output.WriteLine($"REAL_TLS_REDIRECT event={id:D}; HTTP=302; Location=blocked-loopback; physicalPOSTs=1; redirectsFollowed=0; ledger=UnknownExternalEffect; attempts=1");
        }
        finally { Cleanup(path); }
    }

    private static NotificationDestinationProfile Profile(TlsProvider fixture) =>
        new("lab-webhook", NotificationProviderKind.Webhook, "Laboratory Webhook",
            [NotificationEventClass.Operational], new Uri($"https://hooks.example.org:{fixture.Port}/notify"));

    private static IHost CreateHost(string path, TlsProvider fixture, DateTimeOffset? now = null)
    {
        var builder = Host.CreateApplicationBuilder();
        var options = new NotificationReadOptions(true, NotificationPersistenceProvider.Sqlite,
            NotificationExecutionMode.Standalone, path, null,
            DestinationProfiles: [Profile(fixture)], WebhookDeliveryEnabled: true);
        builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(
            new ConfiguredNotificationDestinationProfileCatalog(options.DestinationProfiles!));
        builder.Services.AddSingleton<INotificationCredentialResolver>(new NoCredentials());
        builder.Services.AddSingleton<INotificationHostAddressLookup>(fixture);
        builder.Services.AddSingleton<INotificationPinnedTcpDialer>(fixture);
        builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(fixture,
            NotificationAddressPolicy.NoConfiguredNat64, trustRoots: new X509Certificate2Collection(fixture.Root)));
        var connections = new SqliteNotificationDeliveryDbConnectionFactory(path);
        builder.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(connections);
        builder.Services.AddSingleton<INotificationRoutingStore>(new AdoNotificationRoutingStore(connections));
        builder.Services.AddSingleton<INotificationDeliveryStore>(new AdoNotificationDeliveryStore(connections));
        if (now is not null)
            builder.Services.AddSingleton<TimeProvider>(new FixedClock(now.Value));
        builder.Services.AddKafdeckWebhookDelivery(options);
        return builder.Build();
    }

    private static async Task<Guid> SeedAsync(IServiceProvider services)
    {
        var routing = services.GetRequiredService<INotificationRoutingStore>();
        await routing.InitializeAsync();
        await services.GetRequiredService<INotificationDeliveryStore>().InitializeAsync();
        await routing.CreateSubscriptionAsync(new NotificationSubscriptionDefinition("lab-subscription",
            "lab-webhook", [NotificationEventClass.Operational]), NotificationSubscriptionState.Active,
            DateTimeOffset.UtcNow);
        var notification = NotificationSafeEvent.Approved(Guid.NewGuid(),
            NotificationApprovedEventKind.ConsumerLagAlert, DateTimeOffset.UtcNow);
        await services.GetRequiredService<NotificationRoutingCoordinator>().RouteAsync(notification, DateTimeOffset.UtcNow);
        return notification.EventId;
    }

    private static async Task<NotificationDeliveryRecord> WaitForStateAsync(INotificationDeliveryStore store,
        Guid id, NotificationDeliveryState state)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (true)
        {
            var row = await store.GetAsync(id, "lab-webhook", timeout.Token);
            if (row?.Snapshot.State == state) return row;
            await Task.Delay(50, timeout.Token);
        }
    }

    private static async Task<string> ReceiveAsync(TlsProvider fixture, IServiceProvider services, Guid id)
    {
        try { return await fixture.Received.Task.WaitAsync(TimeSpan.FromSeconds(20)); }
        catch (TimeoutException)
        {
            var row = await services.GetRequiredService<INotificationDeliveryStore>().GetAsync(id, "lab-webhook");
            throw new InvalidOperationException($"No physical request. Dialed={string.Join(',', fixture.Dialed)}; SNI={fixture.ServerName}; TLS={fixture.LastError}; ledger={row?.Snapshot.State}; attempts={row?.Snapshot.AttemptCount}; code={row?.Snapshot.OutcomeCode}");
        }
    }

    private static void Cleanup(string path)
    {
        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" }) File.Delete(path + suffix);
    }

    private sealed class FixedClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private sealed class NoCredentials : INotificationCredentialResolver
    {
        public ValueTask<NotificationCredentialValue> ResolveAsync(NotificationCredentialResolutionRequest request,
            CancellationToken cancellationToken) => throw new InvalidOperationException("Anonymous fixture has no credential admission.");
    }

    // Test-only network virtualization: the approved public pin is dialed to
    // an isolated loopback TCP listener. TLS and HTTP are real, not mocks.
    // Production DI uses System DNS and SocketNotificationPinnedTcpDialer.
    internal sealed class TlsProvider : IAsyncDisposable, INotificationPinnedTcpDialer, INotificationHostAddressLookup
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly X509Certificate2 _server;
        private readonly Task _serve;
        private readonly bool _holdResponse;
        private readonly int _statusCode;
        private readonly Func<string, string>? _responseBody;
        private readonly Func<SslStream, CancellationToken, Task>? _session;
        public X509Certificate2 Root { get; }
        internal X509Certificate2 ServerCertificate => _server;
        public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
        public string? ServerName { get; private set; }
        public string? LastError { get; private set; }
        public int RequestCount { get; private set; }
        public List<string> Dialed { get; } = [];
        public System.Collections.Concurrent.ConcurrentQueue<string> Requests { get; } = new();
        public TaskCompletionSource<string> Received { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TlsProvider(bool holdResponse = false, string certificateName = "hooks.example.org", int statusCode = 204,
            Func<string, string>? responseBody = null,
            Func<SslStream, CancellationToken, Task>? session = null)
        {
            _holdResponse = holdResponse;
            _statusCode = statusCode;
            _responseBody = responseBody;
            _session = session;
            using var rootKey = RSA.Create(2048);
            var rootRequest = new CertificateRequest("CN=Kafdeck ephemeral Webhook laboratory CA", rootKey,
                HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            rootRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign, true));
            Root = rootRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
            using var serverKey = RSA.Create(2048);
            var request = new CertificateRequest($"CN={certificateName}", serverKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            var san = new SubjectAlternativeNameBuilder();
            if (IPAddress.TryParse(certificateName, out var certificateAddress)) san.AddIpAddress(certificateAddress);
            else san.AddDnsName(certificateName);
            request.CertificateExtensions.Add(san.Build());
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                new OidCollection { new("1.3.6.1.5.5.7.3.1") }, true));
            using var issued = request.Create(Root, DateTimeOffset.UtcNow.AddHours(-1),
                DateTimeOffset.UtcNow.AddHours(1), RandomNumberGenerator.GetBytes(16));
            // Schannel cannot use the ephemeral key handle returned by
            // CopyWithPrivateKey. Reimport without PersistKeySet; Windows owns
            // the temporary key container and removes it on certificate dispose.
            using var withKey = issued.CopyWithPrivateKey(serverKey);
            _server = X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pfx), null,
                X509KeyStorageFlags.DefaultKeySet);
            _listener.Start();
            _serve = ServeAsync();
        }

        public Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(string hostname, CancellationToken cancellationToken)
        {
            Assert.Equal("hooks.example.org", hostname);
            return Task.FromResult<IReadOnlyList<IPAddress>>([IPAddress.Parse("1.1.1.1")]);
        }

        public async ValueTask<Stream> DialAsync(IPAddress address, int port, CancellationToken cancellationToken)
        {
            Assert.Equal("1.1.1.1", address.ToString());
            Assert.Equal(Port, port);
            Dialed.Add(address.ToString());
            return await new SocketNotificationPinnedTcpDialer().DialAsync(IPAddress.Loopback, Port, cancellationToken);
        }

        private async Task ServeAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var tls = new SslStream(client.GetStream());
                    try
                    {
                        await tls.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                        {
                            ServerCertificateSelectionCallback = (_, host) => { ServerName = host; return _server; },
                            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                        }, _stop.Token);
                        if (_session is not null)
                        {
                            await _session(tls, _stop.Token);
                            continue;
                        }
                        using var content = new MemoryStream();
                        var buffer = new byte[4096];
                        while (content.Length < 65_536)
                        {
                            var count = await tls.ReadAsync(buffer, _stop.Token);
                            if (count == 0) break;
                            content.Write(buffer, 0, count);
                            var text = Encoding.UTF8.GetString(content.ToArray());
                            var headerEnd = text.IndexOf("\r\n\r\n", StringComparison.Ordinal);
                            if (headerEnd < 0) continue;
                            var lengthHeader = text[..headerEnd].Split("\r\n")
                                .Single(line => line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase));
                            var length = int.Parse(lengthHeader[(lengthHeader.IndexOf(':') + 1)..], System.Globalization.CultureInfo.InvariantCulture);
                            if (content.Length < headerEnd + 4 + length) continue;
                            RequestCount++;
                            Requests.Enqueue(text);
                            Received.TrySetResult(text);
                            if (_holdResponse) await Task.Delay(Timeout.InfiniteTimeSpan, _stop.Token);
                            else if (_responseBody is not null)
                            {
                                var response = Encoding.UTF8.GetBytes(_responseBody(text));
                                await tls.WriteAsync(Encoding.ASCII.GetBytes($"HTTP/1.1 {_statusCode} Fixture\r\nContent-Type: application/json\r\nContent-Length: {response.Length}\r\nConnection: close\r\n\r\n"), _stop.Token);
                                await tls.WriteAsync(response, _stop.Token);
                            }
                            else await tls.WriteAsync(Encoding.ASCII.GetBytes(_statusCode == 204
                                ? "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                : $"HTTP/1.1 {_statusCode} Redirect\r\nLocation: http://127.0.0.1:1/blocked\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), _stop.Token);
                            break;
                        }
                    }
                    catch (AuthenticationException error) { LastError = error.ToString(); }
                    catch (IOException error) { LastError = error.ToString(); }
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            _listener.Stop();
            await _serve;
            _stop.Dispose();
            _server.Dispose();
            Root.Dispose();
        }
    }
}
