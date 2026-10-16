using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafdeck.NotificationSender.Lab;

// Test executable only. It is not referenced by production projects, accepts no
// network control API and never emits private configuration or exception text.
internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length != 1) return 2;
        try
        {
            var input = await File.ReadAllBytesAsync(args[0]);
            if (input.Length > 16 * 1024) return 2;
            var config = JsonSerializer.Deserialize<SenderConfig>(input, new JsonSerializerOptions
                { UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow }) ?? throw new InvalidOperationException();
            var connectionString = (await File.ReadAllTextAsync(config.ConnectionStringFile)).Trim();
            var endpoint = NotificationHttpsEndpointPolicy.ValidateAndNormalize(new Uri(config.Endpoint));
            if (endpoint.DnsSafeHost != "hooks.example.org" || endpoint.Port != config.PhysicalFixturePort)
                return 2;
            using var certificate = X509CertificateLoader.LoadCertificateFromFile(config.CaCertificateFile);
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            var profile = new NotificationDestinationProfile("recovery-webhook", NotificationProviderKind.Webhook,
                "Recovery fixture", [NotificationEventClass.Operational], endpoint);
            var options = new NotificationReadOptions(true, NotificationPersistenceProvider.PostgreSql,
                NotificationExecutionMode.Standalone, null, SecretReference.Parse("env:KAFDECK_TEST_POSTGRES"),
                DestinationProfiles: [profile], WebhookDeliveryEnabled: true,
                DeliveryInterlockPath: config.InterlockPath, DeliveryInterlockSha256: config.InterlockSha256);
            var connections = new PostgreSqlNotificationDeliveryDbConnectionFactory(connectionString);
            INotificationDeliveryActivationGate? gate = config.InterlockPath is null ? null :
                new ConfiguredNotificationDeliveryInterlock(config.InterlockPath, config.InterlockSha256!,
                    new AdoNotificationDeliveryGenerationReader(connections));
            builder.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(connections);
            if (gate is not null) builder.Services.AddSingleton(gate);
            var network = new FixtureNetwork(config.PhysicalFixturePort);
            builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([profile]));
            builder.Services.AddSingleton<INotificationCredentialResolver>(new ConfiguredNotificationCredentialResolver([], new SecretResolver()));
            builder.Services.AddSingleton<INotificationHostAddressLookup>(network);
            builder.Services.AddSingleton<INotificationPinnedTcpDialer>(network);
            builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(network,
                NotificationAddressPolicy.NoConfiguredNat64, trustRoots: new X509Certificate2Collection(certificate), activationGate: gate));
            builder.Services.AddSingleton(new NotificationDeliveryWorkerPolicy(staleInFlightAfter: TimeSpan.FromSeconds(10),
                dispatchTimeout: TimeSpan.FromSeconds(5)));
            builder.Services.AddSingleton<INotificationRoutingStore>(new AdoNotificationRoutingStore(connections));
            builder.Services.AddSingleton<INotificationDeliveryStore>(new AdoNotificationDeliveryStore(connections));
            builder.Services.AddKafdeckWebhookDelivery(options);
            using var host = builder.Build();
            await host.StartAsync();
            await File.WriteAllTextAsync(config.ReadyFile, "sender-started");
            await host.WaitForShutdownAsync();
            return 0;
        }
        catch
        {
            Console.Error.WriteLine("sender-laboratory-startup-refused");
            return 3;
        }
    }

    private sealed record SenderConfig(string ConnectionStringFile, string Endpoint,
        string CaCertificateFile, int PhysicalFixturePort, string ReadyFile,
        string? InterlockPath = null, string? InterlockSha256 = null);

    private sealed class FixtureNetwork(int port) : INotificationHostAddressLookup, INotificationPinnedTcpDialer
    {
        public Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(string hostname, CancellationToken token)
        {
            if (hostname != "hooks.example.org") throw new InvalidOperationException("Unexpected laboratory hostname.");
            token.ThrowIfCancellationRequested();
            return Task.FromResult<IReadOnlyList<IPAddress>>([IPAddress.Parse("1.1.1.1")]);
        }
        public ValueTask<Stream> DialAsync(IPAddress address, int requestedPort, CancellationToken token)
        {
            if (!address.Equals(IPAddress.Parse("1.1.1.1")) || requestedPort != port)
                throw new InvalidOperationException("Unexpected laboratory pin.");
            return new SocketNotificationPinnedTcpDialer().DialAsync(IPAddress.Loopback, port, token);
        }
    }
}
