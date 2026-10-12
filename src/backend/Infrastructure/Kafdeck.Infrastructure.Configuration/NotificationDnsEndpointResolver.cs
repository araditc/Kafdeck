using System.Net;
using System.Net.Sockets;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Bounded DNS lookup for the not-yet-activated W66 webhook path.
/// DNS is observation, not authority to send: physical transports MUST still
/// connect only to the returned pinned IP while keeping TLS verification
/// and the original configured SNI/Host identity.
/// </summary>
public interface INotificationHostAddressLookup
{
    Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(
        string hostname,
        CancellationToken cancellationToken);
}

public sealed class SystemNotificationHostAddressLookup : INotificationHostAddressLookup
{
    public async Task<IReadOnlyList<IPAddress>> GetHostAddressesAsync(
        string hostname,
        CancellationToken cancellationToken) =>
        await Dns.GetHostAddressesAsync(hostname, cancellationToken)
            .ConfigureAwait(false);
}

public sealed class NotificationDnsEndpointResolver : INotificationEndpointResolutionPort
{
    private readonly INotificationHostAddressLookup _lookup;
    private readonly NotificationAddressPolicy _addressPolicy;
    private readonly TimeProvider _clock;

    public NotificationDnsEndpointResolver(
        INotificationHostAddressLookup lookup,
        NotificationAddressPolicy addressPolicy,
        TimeProvider? clock = null)
    {
        _lookup = lookup ?? throw new ArgumentNullException(nameof(lookup));
        _addressPolicy = addressPolicy ??
            throw new ArgumentNullException(nameof(addressPolicy));
        _clock = clock ?? TimeProvider.System;
    }

    public async Task<NotificationPinnedEndpoint> ResolveForConnectionAsync(
        Uri configuredEndpoint,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Validate before any DNS I/O, including scheme, userinfo, query,
        // fragment and prohibited literal hostnames.
        var endpoint = NotificationHttpsEndpointPolicy.ValidateAndNormalize(
            configuredEndpoint);

        IReadOnlyList<IPAddress> addresses;
        if (IPAddress.TryParse(endpoint.Host, out var literalAddress))
        {
            addresses = [literalAddress];
        }
        else
        {
            try
            {
                addresses = await _lookup.GetHostAddressesAsync(
                        endpoint.DnsSafeHost,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (SocketException)
            {
                throw new KafdeckConfigurationException(
                    "Notification destination DNS resolution failed.");
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (addresses is null || addresses.Count is < 1 or >
            NotificationPinnedEndpoint.HardMaxAddresses)
        {
            throw new KafdeckConfigurationException(
                "Notification destination DNS answer is empty or exceeds its finite address budget.");
        }

        // Reject the ENTIRE mixed answer when any address is disallowed.
        // Never filter private/metadata answers and continue with a subset.
        return _addressPolicy.ValidatePinnedEndpoint(
            endpoint,
            addresses,
            _clock.GetUtcNow());
    }
}
