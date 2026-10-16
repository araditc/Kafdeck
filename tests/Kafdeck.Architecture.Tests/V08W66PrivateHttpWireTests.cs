using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;
using TlsProvider = Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66PrivateHttpWireTests
{
    [Theory]
    [InlineData("HTTP/1.1 204 No Content", true)]
    [InlineData("HTTP/1.1 204 ", true)]
    [InlineData("HTTP/1.0 204 No Content", true)]
    [InlineData("HTTP/1.1 204Xmalformed", false)]
    [InlineData("HTTP/1.1 204OK", false)]
    [InlineData("HTTP/1.1 204", false)]
    [InlineData("HTTP/1.1 204\tNo Content", false)]
    [InlineData("HTTP/1.1 204 reason\rinjected", false)]
    [InlineData("HTTP/1.1 0204 Wrong width", false)]
    [InlineData("HTTP/1.1 600 Invalid range", false)]
    [InlineData("HTTP/2.0 204 Unsupported version", false)]
    public async Task Physical_private_wire_accepts_only_bounded_strict_http_status_lines(string status, bool delivered)
    {
        await using var tls = new TlsProvider(session: async (stream, token) =>
        {
            await ReadRequestAsync(stream, token);
            await stream.WriteAsync(Encoding.ASCII.GetBytes(status + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), token);
        });
        var profile = Profile(new Uri($"https://hooks.example.org:{tls.Port}/notify"));
        var result = await Transport(profile, tls, tls).SendAsync(Request(profile, IPAddress.Parse("1.1.1.1")), CancellationToken.None);
        Assert.Equal(delivered ? NotificationWebhookTransportOutcome.Delivered : NotificationWebhookTransportOutcome.UnknownExternalEffect, result.Outcome);
        Assert.Single(tls.Dialed);
    }

    [Theory]
    [InlineData("hooks.example.org", "hooks.example.org")]
    [InlineData("bücher.example.org", "xn--bcher-kva.example.org")]
    [InlineData("2606:4700:4700::1111", "[2606:4700:4700::1111]")]
    public async Task Physical_private_wire_preserves_ascii_idna_or_ipv6_host_authority_and_port(string host, string wireHost)
    {
        var certificateIdentity = wireHost.Trim('[', ']');
        await using var tls = new TlsProvider(certificateName: certificateIdentity);
        var profile = Profile(new UriBuilder("https", host, tls.Port, "notify").Uri);
        var address = IPAddress.TryParse(host, out var literal) ? literal : IPAddress.Parse("1.1.1.1");
        var dialer = new LocalDialer(address, tls.Port);
        var result = await Transport(profile, tls, dialer).SendAsync(Request(profile, address), CancellationToken.None);
        Assert.Equal(NotificationWebhookTransportOutcome.Delivered, result.Outcome);
        var request = Assert.Single(tls.Requests);
        var expected = "Host: " + wireHost + ":" + tls.Port;
        var actual = request[..request.IndexOf("\r\n\r\n", StringComparison.Ordinal)].Split("\r\n")
            .Single(line => line.StartsWith("Host: ", StringComparison.Ordinal));
        Assert.Equal(expected, actual);
        Assert.Equal(1, dialer.Dials);
        if (!IPAddress.TryParse(host, out _))
        {
            Assert.NotNull(tls.ServerName);
            Assert.Equal(certificateIdentity, new System.Globalization.IdnMapping().GetAscii(tls.ServerName).ToLowerInvariant());
        }
    }

    private static NotificationDestinationProfile Profile(Uri endpoint) =>
        new("wire-profile", NotificationProviderKind.Webhook, "Wire fixture", [NotificationEventClass.Operational], endpoint);

    private static NotificationPinnedWebhookRequest Request(NotificationDestinationProfile profile, IPAddress address) =>
        new(profile.DestinationId, profile.RevisionFingerprint,
            NotificationAddressPolicy.NoConfiguredNat64.ValidatePinnedEndpoint(profile.ConfiguredEndpoint!, [address], DateTimeOffset.UtcNow),
            Encoding.UTF8.GetBytes("{}"), null);

    private static NotificationPinnedWebhookHttpTransport Transport(NotificationDestinationProfile profile, TlsProvider tls,
        INotificationPinnedTcpDialer dialer) => new(profile,
        new NotificationPinnedHttpHandlerFactory(dialer, NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots: new X509Certificate2Collection(tls.Root)));

    private static async Task ReadRequestAsync(SslStream stream, CancellationToken token)
    {
        using var reader = new StreamReader(stream, Encoding.ASCII, false, 1024, leaveOpen: true);
        var length = 0;
        var count = 0;
        while (true)
        {
            var line = await reader.ReadLineAsync(token) ?? throw new IOException("Fixture request ended prematurely.");
            if (++count > 32 || line.Length > 8192) throw new IOException("Fixture request budget exceeded.");
            if (line.Length == 0) break;
            if (line.StartsWith("Content-Length: ", StringComparison.Ordinal))
                length = int.Parse(line[16..], System.Globalization.CultureInfo.InvariantCulture);
        }
        if (length is < 0 or > 32768) throw new IOException("Fixture body budget exceeded.");
        var body = new char[length];
        if (await reader.ReadBlockAsync(body, token) != length) throw new IOException("Fixture body ended prematurely.");
    }

    private sealed class LocalDialer(IPAddress expectedAddress, int port) : INotificationPinnedTcpDialer
    {
        internal int Dials { get; private set; }
        public ValueTask<Stream> DialAsync(IPAddress address, int requestedPort, CancellationToken token)
        {
            Assert.Equal(expectedAddress, address);
            Assert.Equal(port, requestedPort);
            Dials++;
            return new SocketNotificationPinnedTcpDialer().DialAsync(IPAddress.Loopback, port, token);
        }
    }
}
