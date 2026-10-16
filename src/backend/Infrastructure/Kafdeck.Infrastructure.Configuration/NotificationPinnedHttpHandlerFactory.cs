using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
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
/// NotificationPinnedEndpoint. Host registration requires explicit admission;
/// this factory itself does not issue requests or attach credentials.
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
    private readonly X509Certificate2Collection? _trustRoots;
    private readonly INotificationDeliveryActivationGate? _activationGate;

    public NotificationPinnedHttpHandlerFactory(
        INotificationPinnedTcpDialer dialer,
        NotificationAddressPolicy policy,
        TimeProvider? clock = null,
        X509Certificate2Collection? trustRoots = null,
        INotificationDeliveryActivationGate? activationGate = null)
    {
        _dialer = dialer ?? throw new ArgumentNullException(nameof(dialer));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _clock = clock ?? TimeProvider.System;
        _trustRoots = trustRoots is null ? null : new X509Certificate2Collection(trustRoots);
        _activationGate = activationGate;
    }

    /// <summary>
    /// The guarded composition used by the admitted physical notifier.
    /// Check EVERY HTTP request before an inner HTTP/2 connection can be
    /// reused; the inner SocketsHttpHandler still pins TCP to the approved IP.
    /// Caller owns/disposes the returned handler. Does not itself send.
    /// </summary>
    public HttpMessageHandler CreateGuardedHandler(NotificationPinnedEndpoint pinned)
    {
        VerifyPinned(pinned);
        HttpMessageHandler inner = CreateHandler(pinned);
        if (_activationGate is not null) inner = new ActivationGuard(inner, _activationGate);
        return new NotificationPinnedRequestGuardHandler(pinned, _policy, inner, _clock);
    }

    internal ValueTask VerifySendAdmissionAsync(CancellationToken token) =>
        _activationGate?.VerifyAsync(token) ?? ValueTask.CompletedTask;

    private sealed class ActivationGuard(HttpMessageHandler inner, INotificationDeliveryActivationGate gate)
        : DelegatingHandler(inner)
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            await gate.VerifyAsync(token).ConfigureAwait(false);
            return await base.SendAsync(request, token).ConfigureAwait(false);
        }
    }

    /// <summary>Opens only the approved pinned TCP endpoint, then performs normal
    /// TLS/SNI/hostname validation without creating HTTP diagnostics or a proxy.</summary>
    public async Task<SslStream> OpenPinnedTlsStreamAsync(NotificationPinnedEndpoint pinned, CancellationToken token)
    {
        await VerifySendAdmissionAsync(token).ConfigureAwait(false);
        using var connectDeadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        connectDeadline.CancelAfter(TimeSpan.FromSeconds(5));
        var stream = await ConnectPinnedAsync(pinned, pinned.Endpoint.DnsSafeHost, pinned.Endpoint.Port, connectDeadline.Token).ConfigureAwait(false);
        var tls = new SslStream(stream, leaveInnerStreamOpen: false);
        try
        {
            await tls.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = pinned.Endpoint.DnsSafeHost,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateChainPolicy = _trustRoots is null ? null : CreateChainPolicy(_trustRoots),
            }, token).ConfigureAwait(false);
            return tls;
        }
        catch { await tls.DisposeAsync().ConfigureAwait(false); throw; }
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
            // A supplied laboratory/private CA changes chain roots only.
            // SNI, hostname, expiry and EKU checks remain in SslStream.
            SslOptions = new SslClientAuthenticationOptions
            {
                CertificateChainPolicy = _trustRoots is null ? null : CreateChainPolicy(_trustRoots),
            },
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

                // An explicit Host header (HTTP/1.1 Host or HTTP/2
                // :authority) can override the URI's virtual host even when
                // ConnectCallback sees the approved URI. Never deliver a
                // notification body/credential under a different authority.
                // Require the original canonical host and approved port.
                var explicitHost = context.InitialRequestMessage.Headers.Host;
                if (explicitHost is not null &&
                    !string.Equals(
                        explicitHost,
                        pinned.Endpoint.Authority,
                        StringComparison.OrdinalIgnoreCase))
                {
                    throw new KafdeckConfigurationException(
                        "Notification request Host header is not the approved destination authority.");
                }

                return ConnectPinnedAsync(
                    pinned,
                    context.DnsEndPoint.Host,
                    context.DnsEndPoint.Port,
                    cancellationToken);
            },
        };
    }

    private static X509ChainPolicy CreateChainPolicy(X509Certificate2Collection roots)
    {
        var chain = new X509ChainPolicy
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,
            RevocationMode = X509RevocationMode.NoCheck,
        };
        chain.CustomTrustStore.AddRange(roots);
        chain.ApplicationPolicy.Add(new System.Security.Cryptography.Oid("1.3.6.1.5.5.7.3.1"));
        return chain;
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

    internal void VerifyPinned(NotificationPinnedEndpoint pinned)
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
