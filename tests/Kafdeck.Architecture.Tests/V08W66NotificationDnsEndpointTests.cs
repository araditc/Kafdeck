using System.Net;
using System.Net.Sockets;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationDnsEndpointTests
{
    private sealed class FakeLookup : INotificationHostAddressLookup
    {
        public IReadOnlyList<IPAddress> Addresses { get; set; } =
            [IPAddress.Parse("1.1.1.1")];

        public int Calls { get; private set; }
        public string? LastHost { get; private set; }
        public bool FailDns { get; set; }

        public Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(
            string hostname, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Calls++;
            LastHost = hostname;
            if (FailDns) throw new SocketException((int)SocketError.HostNotFound);
            return Task.FromResult(Addresses);
        }
    }

    private static NotificationDnsEndpointResolver Resolver(FakeLookup lookup) =>
        new(lookup, NotificationAddressPolicy.NoConfiguredNat64);

    [Fact]
    public async Task Exact_https_origin_and_approved_public_dns_answer_are_pinned()
    {
        var lookup = new FakeLookup
        {
            Addresses = [IPAddress.Parse("1.1.1.1"), IPAddress.Parse("8.8.8.8")]
        };

        var pinned = await Resolver(lookup).ResolveForConnectionAsync(
            new Uri("https://alerts.example.org/webhook"), CancellationToken.None);

        Assert.Equal("alerts.example.org", lookup.LastHost);
        Assert.Equal("https://alerts.example.org/webhook", pinned.Endpoint.AbsoluteUri);
        Assert.Equal(2, pinned.Addresses.Count);
        Assert.Equal("1.1.1.1", pinned.Addresses[0].ToString());
        Assert.Equal("8.8.8.8", pinned.Addresses[1].ToString());
        Assert.Equal(NotificationAddressPolicy.NoConfiguredNat64.Fingerprint,
            pinned.AddressPolicyFingerprint);
    }

    [Theory]
    [InlineData("https://alerts.example.org/hook", "1.1.1.1", "127.0.0.1")]
    [InlineData("https://alerts.example.org/hook", "1.1.1.1", "169.254.169.254")]
    [InlineData("https://alerts.example.org/hook", "1.1.1.1", "64:ff9b::a9fe:a9fe")]
    public async Task Any_private_metadata_or_nat64_address_rejects_entire_answer(
        string url, string publicIp, string forbiddenIp)
    {
        var lookup = new FakeLookup
        {
            Addresses = [IPAddress.Parse(publicIp), IPAddress.Parse(forbiddenIp)]
        };
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Resolver(lookup).ResolveForConnectionAsync(new Uri(url), CancellationToken.None));
        Assert.Equal(1, lookup.Calls);
    }

    [Theory]
    [InlineData("http://alerts.example.org/hook")]
    [InlineData("https://user:pass@alerts.example.org/hook")]
    [InlineData("https://alerts.example.org/hook?token=bad")]
    [InlineData("https://alerts.example.org/hook#fragment")]
    [InlineData("https://localhost/hook")]
    public async Task Invalid_destination_uri_is_rejected_before_dns(string url)
    {
        var lookup = new FakeLookup();
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Resolver(lookup).ResolveForConnectionAsync(new Uri(url), CancellationToken.None));
        Assert.Equal(0, lookup.Calls);
    }

    [Fact]
    public async Task Public_literal_ip_does_not_trigger_dns_lookup()
    {
        var lookup = new FakeLookup();
        var pinned = await Resolver(lookup).ResolveForConnectionAsync(
            new Uri("https://1.1.1.1/hook"), CancellationToken.None);
        Assert.Single(pinned.Addresses);
        Assert.Equal(0, lookup.Calls);
    }

    [Fact]
    public async Task Duplicate_addresses_fail_without_selecting_a_subset()
    {
        var lookup = new FakeLookup
        {
            Addresses = [IPAddress.Parse("1.1.1.1"), IPAddress.Parse("1.1.1.1")]
        };
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Resolver(lookup).ResolveForConnectionAsync(
                new Uri("https://alerts.example.org/hook"), CancellationToken.None));
    }

    [Fact]
    public async Task Empty_or_excessive_answer_fails_closed()
    {
        var lookup = new FakeLookup { Addresses = [] };
        var resolver = Resolver(lookup);
        await Assert.ThrowsAsync<KafdeckConfigurationException>(() =>
            resolver.ResolveForConnectionAsync(
                new Uri("https://alerts.example.org/hook"), CancellationToken.None));
        lookup.Addresses = Enumerable.Range(1, NotificationPinnedEndpoint.HardMaxAddresses + 1)
            .Select(i => IPAddress.Parse($"8.8.8.{i}")).ToArray();
        await Assert.ThrowsAsync<KafdeckConfigurationException>(() =>
            resolver.ResolveForConnectionAsync(
                new Uri("https://alerts.example.org/hook"), CancellationToken.None));
    }

    [Fact]
    public async Task Dns_failure_produces_no_destination_hostname_in_exception()
    {
        var lookup = new FakeLookup { FailDns = true };
        var error = await Assert.ThrowsAsync<KafdeckConfigurationException>(() =>
            Resolver(lookup).ResolveForConnectionAsync(
                new Uri("https://very-sensitive.example.org/hook"),
                CancellationToken.None));
        Assert.DoesNotContain("very-sensitive.example.org", error.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Already_cancelled_request_makes_no_dns_call()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var lookup = new FakeLookup();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            Resolver(lookup).ResolveForConnectionAsync(
                new Uri("https://alerts.example.org/hook"), cts.Token));
        Assert.Equal(0, lookup.Calls);
    }
}
