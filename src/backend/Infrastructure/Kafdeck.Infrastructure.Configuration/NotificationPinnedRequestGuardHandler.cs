using System.Net;
using System.Net.Http;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Enforces one server-approved notification HTTPS URI and authority for
/// EVERY request, including requests reusing an established HTTP/2 connection.
/// The inner handler is still responsible for dialing only the pinned IP and
/// validating TLS against the original configured hostname. This guard is
/// not host-registered and does not send requests by itself.
/// </summary>
public sealed class NotificationPinnedRequestGuardHandler : DelegatingHandler
{
    private readonly NotificationPinnedEndpoint _pinned;
    private readonly NotificationAddressPolicy _policy;
    private readonly TimeProvider _clock;

    public NotificationPinnedRequestGuardHandler(
        NotificationPinnedEndpoint pinned,
        NotificationAddressPolicy policy,
        HttpMessageHandler innerHandler,
        TimeProvider? clock = null)
        : base(innerHandler)
    {
        _pinned = pinned ?? throw new ArgumentNullException(nameof(pinned));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _clock = clock ?? TimeProvider.System;
    }

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        ValidateRequest(request, cancellationToken);
        return base.SendAsync(request, cancellationToken);
    }

    protected override HttpResponseMessage Send(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        // HttpClient.Send is a separate synchronous entry point. Do not
        // allow it to bypass the async-only request authority guard.
        ValidateRequest(request, cancellationToken);
        return base.Send(request, cancellationToken);
    }

    private void ValidateRequest(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        // Connection callbacks do not execute for every pooled request.
        // Check identity/expiry here BEFORE an inner handler can reuse a
        // connection, attach a payload, or initiate another socket.
        var canonical = NotificationHttpsEndpointPolicy.ValidateAndNormalize(
            _pinned.Endpoint);
        if (!string.Equals(canonical.AbsoluteUri,
                _pinned.Endpoint.AbsoluteUri, StringComparison.Ordinal) ||
            !string.Equals(_pinned.AddressPolicyFingerprint,
                _policy.Fingerprint, StringComparison.Ordinal))
        {
            throw new KafdeckConfigurationException(
                "Notification request destination or pinning policy changed.");
        }

        var age = _clock.GetUtcNow() - _pinned.ResolvedAtUtc;
        if (age > NotificationPinnedHttpHandlerFactory.MaxPinAge ||
            age < TimeSpan.FromSeconds(-5))
        {
            throw new KafdeckConfigurationException(
                "Notification pinned DNS evidence is stale or invalid.");
        }

        var uri = request.RequestUri;
        if (uri is null || !uri.IsAbsoluteUri ||
            !string.Equals(uri.AbsoluteUri,
                _pinned.Endpoint.AbsoluteUri, StringComparison.Ordinal))
        {
            throw new KafdeckConfigurationException(
                "Notification request URI is not its approved HTTPS destination.");
        }

        // Explicit Host in HTTP/1.1 maps to :authority in HTTP/2. A shared
        // public address or multi-SAN certificate is NOT destination authority.
        var explicitHost = request.Headers.Host;
        if (explicitHost is not null &&
            !string.Equals(explicitHost,
                _pinned.Endpoint.Authority, StringComparison.OrdinalIgnoreCase))
        {
            throw new KafdeckConfigurationException(
                "Notification request Host is not its approved HTTPS authority.");
        }

    }
}
