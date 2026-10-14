using System.Net;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66PinnedRequestGuardTests
{
    private static NotificationPinnedEndpoint Pin(DateTimeOffset? at = null) =>
        NotificationAddressPolicy.NoConfiguredNat64.ValidatePinnedEndpoint(
            new Uri("https://hooks.example.org/notify"),
            [IPAddress.Parse("1.1.1.1")],
            at ?? DateTimeOffset.UtcNow);

    private sealed class RecordingTransport : HttpMessageHandler
    {
        public int Calls { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Accepted));
        }
    }

    [Theory]
    [InlineData("https://hooks.example.org/other", null)]
    [InlineData("https://hooks.example.org/notify?injected=1", null)]
    [InlineData("http://hooks.example.org/notify", null)]
    [InlineData("https://other.example.org/notify", null)]
    [InlineData("https://hooks.example.org/notify", "other.example.org")]
    [InlineData("https://hooks.example.org/notify", "hooks.example.org:8443")]
    public async Task URI_or_explicit_Host_pivot_is_rejected_before_inner_request(
        string uri, string? host)
    {
        var inner = new RecordingTransport();
        using var handler = new NotificationPinnedRequestGuardHandler(
            Pin(), NotificationAddressPolicy.NoConfiguredNat64, inner);
        using var invoker = new HttpMessageInvoker(handler);
        using var request = new HttpRequestMessage(HttpMethod.Post, uri)
        {
            Version = HttpVersion.Version20,
        };
        if (host is not null) request.Headers.Host = host;

        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            () => invoker.SendAsync(request, CancellationToken.None));

        Assert.Equal(0, inner.Calls);
    }

    [Fact]
    public async Task Canonical_explicit_Host_and_URI_are_allowed_without_network()
    {
        var inner = new RecordingTransport();
        using var handler = new NotificationPinnedRequestGuardHandler(
            Pin(), NotificationAddressPolicy.NoConfiguredNat64, inner);
        using var invoker = new HttpMessageInvoker(handler);
        using var request = new HttpRequestMessage(
            HttpMethod.Post, "https://hooks.example.org/notify")
        {
            Version = HttpVersion.Version20,
        };
        request.Headers.Host = "hooks.example.org";

        using var result = await invoker.SendAsync(request, CancellationToken.None);
        Assert.Equal(HttpStatusCode.Accepted, result.StatusCode);
        Assert.Equal(1, inner.Calls);
    }

    [Fact]
    public async Task Every_reused_request_is_checked_independently()
    {
        var inner = new RecordingTransport();
        using var guard = new NotificationPinnedRequestGuardHandler(
            Pin(), NotificationAddressPolicy.NoConfiguredNat64, inner);
        using var invoker = new HttpMessageInvoker(guard);
        using var first = new HttpRequestMessage(
            HttpMethod.Post, "https://hooks.example.org/notify");
        using var success = await invoker.SendAsync(first, CancellationToken.None);
        Assert.Equal(HttpStatusCode.Accepted, success.StatusCode);

        using var second = new HttpRequestMessage(
            HttpMethod.Post, "https://hooks.example.org/notify");
        second.Headers.Host = "other.example.org";
        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            () => invoker.SendAsync(second, CancellationToken.None));

        Assert.Equal(1, inner.Calls);
    }

    [Theory]
    [InlineData("other.example.org")]
    [InlineData("hooks.example.org:8443")]
    public void Synchronous_Send_cannot_bypass_explicit_Host_authority(
        string wrongHost)
    {
        var inner = new RecordingTransport();
        using var guard = new NotificationPinnedRequestGuardHandler(
            Pin(), NotificationAddressPolicy.NoConfiguredNat64, inner);
        using var invoker = new HttpMessageInvoker(guard);
        using var request = new HttpRequestMessage(
            HttpMethod.Post, "https://hooks.example.org/notify");
        request.Headers.Host = wrongHost;

        Assert.Throws<KafdeckConfigurationException>(
            () => invoker.Send(request, CancellationToken.None));
        Assert.Equal(0, inner.Calls);
    }

    [Fact]
    public async Task Stale_pin_is_rejected_before_inner_request()
    {
        var inner = new RecordingTransport();
        using var guard = new NotificationPinnedRequestGuardHandler(
            Pin(DateTimeOffset.UtcNow - TimeSpan.FromMinutes(1)),
            NotificationAddressPolicy.NoConfiguredNat64, inner);
        using var invoker = new HttpMessageInvoker(guard);
        using var request = new HttpRequestMessage(
            HttpMethod.Post, "https://hooks.example.org/notify");

        await Assert.ThrowsAsync<KafdeckConfigurationException>(
            () => invoker.SendAsync(request, CancellationToken.None));
        Assert.Equal(0, inner.Calls);
    }

    [Fact]
    public void Factory_composes_request_guard_over_approved_socket_handler()
    {
        var factory = new NotificationPinnedHttpHandlerFactory(
            new NeverUsedDialer(), NotificationAddressPolicy.NoConfiguredNat64);
        using var guard = factory.CreateGuardedHandler(Pin());
        var typed = Assert.IsType<NotificationPinnedRequestGuardHandler>(guard);
        var inner = Assert.IsType<SocketsHttpHandler>(typed.InnerHandler);
        Assert.False(inner.UseProxy);
        Assert.False(inner.AllowAutoRedirect);
        Assert.NotNull(inner.ConnectCallback);
    }

    private sealed class NeverUsedDialer : INotificationPinnedTcpDialer
    {
        public ValueTask<Stream> DialAsync(IPAddress address, int port,
            CancellationToken cancellationToken) =>
            throw new InvalidOperationException("No physical notifier is activated.");
    }
}
