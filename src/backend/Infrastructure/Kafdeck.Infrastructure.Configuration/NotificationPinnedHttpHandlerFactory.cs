using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Network-dial seam for a prevalidated pinned IP. It never performs DNS.
/// </summary>
public interface INotificationPinnedTcpDialer
{
    ValueTask<Stream> DialAsync(
        IPAddress address,
        int port,
        CancellationToken cancellationToken);
}

public sealed class SocketNotificationPinnedTcpDialer : INotificationPinnedTcpDialer
{
    public async ValueTask<Stream> DialAsync(
        IPAddress address,
        int port,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(address);
        cancellationToken.ThrowIfCancellationRequested();

        var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(new IPEndPoint(address, port), cancellationToken)
                .ConfigureAwait(false);
            return new NetworkStream(socket, ownsSocket: true);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}

/// <summary>
/// Creates one locked-down SocketsHttpHandler for one already verified
/// NotificationPinnedEndpoint. This factory is not wired into the host;
/// it does not issue requests, attach credentials or activate outbound sends.
/// TLS hostname/certificate checks are left to the .NET HTTPS stack and
/// MUST NOT be disabled by a future physical transport.
/// </summary>
public sealed class NotificationPinnedHttpHandlerFactory
{
    public static readonly TimeSpan MaxPinAge = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MaxFutureSkew = TimeSpan.FromSeconds(5);

    private readonly INotificationPinnedTcpDialer _dialer;
    private readonly NotificationAddressPolicy _policy;
    private readonly TimeProvider _clock;

    public NotificationPinnedHttpHandlerFactory(
        INotificationPinnedTcpDialer dialer,
        NotificationAddressPolicy policy,
        TimeProvider? clock = null)
    {
        _dialer = dialer ?? throw new ArgumentNullException(nameof(dialer));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _clock = clock ?? TimeProvider.System;
    }

    public SocketsHttpHandler CreateHandler(NotificationPinnedEndpoint pinned)
    {
        VerifyPinned(pinned);
        // The request URI retains its approved DNS name, allowing normal
        // .NET TLS SNI and certificate hostname validation over pinned TCP.
        // No pooled cross-destination handler, proxy, redirects or cookies.
        return new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            UseProxy = false,
            UseCookies = false,
            AutomaticDecompression = DecompressionMethods.None,
            ConnectTimeout = TimeSpan.FromSeconds(5),
            PooledConnectionLifetime = TimeSpan.Zero,
            ConnectCallback = (context, cancellationToken) =>
            {
                // This handler is scoped to ONE exact HTTPS destination,
                // not an arbitrary HTTP client. Restrict the full request
                // URI in addition to the pinned DNS host/port: callers may
                // not change scheme, path, query or fragment while reusing
                // the transport's approved address evidence.
                var requestUri = context.InitialRequestMessage.RequestUri;
                if (requestUri is null ||
                    !requestUri.IsAbsoluteUri ||
                    !string.Equals(
                        requestUri.AbsoluteUri,
                        pinned.Endpoint.AbsoluteUri,
                        StringComparison.Ordinal))
                {
                    throw new KafdeckConfigurationException(
                        "Notification pinned request does not match its approved HTTPS destination.");
                }

                return ConnectPinnedAsync(
                    pinned,
                    context.DnsEndPoint.Host,
                    context.DnsEndPoint.Port,
                    cancellationToken);
            },
        };
    }

    /// <summary>
    /// Exposed as a testable, exact-host dial boundary. Callers never supply
    /// an additional IP, URL, path, DNS response or proxy for fallback.
    /// </summary>
    public async ValueTask<Stream> ConnectPinnedAsync(
        NotificationPinnedEndpoint pinned,
        string requestedHost,
        int requestedPort,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        VerifyPinned(pinned);

        if (!string.Equals(
                requestedHost,
                pinned.Endpoint.DnsSafeHost,
                StringComparison.OrdinalIgnoreCase) ||
            requestedPort != pinned.Endpoint.Port)
        {
            throw new KafdeckConfigurationException(
                "Notification pinned connection target does not match the configured endpoint.");
        }

        foreach (var address in pinned.Addresses)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var connected = await _dialer.DialAsync(
                    address,
                    requestedPort,
                    cancellationToken).ConfigureAwait(false);
                if (connected is null)
                {
                    throw new KafdeckConfigurationException(
                        "Notification pinned TCP dialer returned no stream.");
                }

                return connected;
            }
            catch (SocketException)
            {
                // Only the finite, policy-checked original DNS snapshot may
                // be tried. Never resolve the hostname or substitute a new IP.
            }
        }

        throw new KafdeckConfigurationException(
            "Notification pinned destination could not be reached.");
    }

    private void VerifyPinned(NotificationPinnedEndpoint pinned)
    {
        ArgumentNullException.ThrowIfNull(pinned);
        var canonical = NotificationHttpsEndpointPolicy.ValidateAndNormalize(pinned.Endpoint);
        if (!string.Equals(
                canonical.AbsoluteUri,
                pinned.Endpoint.AbsoluteUri,
                StringComparison.Ordinal) ||
            !string.Equals(
                pinned.AddressPolicyFingerprint,
                _policy.Fingerprint,
                StringComparison.Ordinal))
        {
            throw new KafdeckConfigurationException(
                "Notification pinned connection policy or destination identity changed.");
        }

        var age = _clock.GetUtcNow() - pinned.ResolvedAtUtc;
        if (age > MaxPinAge || age < -MaxFutureSkew)
        {
            throw new KafdeckConfigurationException(
                "Notification pinned DNS evidence is stale or invalid.");
        }
    }
}
