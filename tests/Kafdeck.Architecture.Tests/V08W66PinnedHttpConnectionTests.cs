using System.Net;
using System.Net.Sockets;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66PinnedHttpConnectionTests
{
    private sealed class FakeDialer : INotificationPinnedTcpDialer
    {
        public List<string> Attempts { get; } = [];
        public string? FailAddress { get; set; }
        public int LastPort { get; private set; }
        public Stream ExpectedStream { get; } = new MemoryStream();

        public ValueTask<Stream> DialAsync(IPAddress address, int port,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Attempts.Add(address.ToString());
            LastPort = port;
            if (address.ToString() == FailAddress)
                throw new SocketException((int)SocketError.HostUnreachable);
            return ValueTask.FromResult(ExpectedStream);
        }
    }

    private static NotificationPinnedEndpoint Pin(
        DateTimeOffset? at = null, string url = "https://hooks.example.org/notify") =>
        NotificationAddressPolicy.NoConfiguredNat64.ValidatePinnedEndpoint(
            new Uri(url),
            [IPAddress.Parse("1.1.1.1"), IPAddress.Parse("8.8.8.8")],
            at ?? DateTimeOffset.UtcNow);

    private static NotificationPinnedHttpHandlerFactory Factory(FakeDialer dialer) =>
        new(dialer, NotificationAddressPolicy.NoConfiguredNat64);

    [Fact]
    public void Handler_for_one_pinned_target_has_no_proxy_redirect_or_cookie_bypass()
    {
        var dialer = new FakeDialer();
        using var handler = Factory(dialer).CreateHandler(Pin());

        Assert.False(handler.UseProxy);
        Assert.False(handler.AllowAutoRedirect);
        Assert.False(handler.UseCookies);
        Assert.Equal(DecompressionMethods.None, handler.AutomaticDecompression);
        Assert.Equal(TimeSpan.Zero, handler.PooledConnectionLifetime);
        Assert.NotNull(handler.ConnectCallback);
        Assert.Null(handler.SslOptions.RemoteCertificateValidationCallback);
        Assert.Empty(dialer.Attempts);
    }

    [Fact]
    public async Task Exact_host_and_port_dial_only_verified_pinned_addresses()
    {
        var dialer = new FakeDialer();
        var pinned = Pin();
        var stream = await Factory(dialer).ConnectPinnedAsync(
            pinned, "hooks.example.org", 443, CancellationToken.None);

        Assert.Same(dialer.ExpectedStream, stream);
        Assert.Equal(["1.1.1.1"], dialer.Attempts);
        Assert.Equal(443, dialer.LastPort);
    }

    [Fact]
    public async Task Connection_refusal_tries_only_next_approved_pinned_address()
    {
        var dialer = new FakeDialer { FailAddress = "1.1.1.1" };
        var stream = await Factory(dialer).ConnectPinnedAsync(
            Pin(), "hooks.example.org", 443, CancellationToken.None);

        Assert.Same(dialer.ExpectedStream, stream);
        Assert.Equal(["1.1.1.1", "8.8.8.8"], dialer.Attempts);
    }

    [Theory]
    [InlineData("other.example.org", 443)]
    [InlineData("hooks.example.org", 80)]
    [InlineData("127.0.0.1", 443)]
    public async Task Host_or_port_change_is_denied_before_socket_use(
        string host, int port)
    {
        var dialer = new FakeDialer();
        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            async () => { _ = await Factory(dialer).ConnectPinnedAsync(
                Pin(), host, port, CancellationToken.None); });
        Assert.Empty(dialer.Attempts);
    }

    [Fact]
    public async Task Expired_dns_pin_fails_without_dialing()
    {
        var dialer = new FakeDialer();
        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            async () => { _ = await Factory(dialer).ConnectPinnedAsync(
                Pin(DateTimeOffset.UtcNow - TimeSpan.FromMinutes(1)),
                "hooks.example.org", 443, CancellationToken.None); });
        Assert.Empty(dialer.Attempts);
    }

    [Fact]
    public async Task Cancelled_connection_does_not_reach_any_network_port()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var dialer = new FakeDialer();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => { _ = await Factory(dialer).ConnectPinnedAsync(
                Pin(), "hooks.example.org", 443, cancellation.Token); });
        Assert.Empty(dialer.Attempts);
    }

    [Fact]
    public async Task All_refused_pinned_addresses_fail_closed_without_dns_fallback()
    {
        var dialer = new RejectingDialer();
        var factory = new NotificationPinnedHttpHandlerFactory(
            dialer, NotificationAddressPolicy.NoConfiguredNat64);
        var error = await Assert.ThrowsAsync<KafdeckConfigurationException>(
            async () => { _ = await factory.ConnectPinnedAsync(
                Pin(), "hooks.example.org", 443, CancellationToken.None); });
        Assert.Equal(2, dialer.Attempts);
        Assert.DoesNotContain("hooks.example.org", error.Message, StringComparison.Ordinal);
    }

    private sealed class RejectingDialer : INotificationPinnedTcpDialer
    {
        public int Attempts { get; private set; }

        public ValueTask<Stream> DialAsync(IPAddress address, int port,
            CancellationToken cancellationToken)
        {
            Attempts++;
            throw new SocketException((int)SocketError.HostUnreachable);
        }
    }
}
