using System.Net;

namespace Kafdeck.Infrastructure.Configuration;

public static class DeploymentHostPolicy
{
    private static readonly string[] LoopbackHosts =
    [
        "localhost",
        "127.0.0.1",
        "[::1]",
    ];

    public static IReadOnlyList<string> BuildAllowedHosts(
        DeploymentOptions deployment)
    {
        ArgumentNullException.ThrowIfNull(deployment);

        var parsed = deployment.ListenUrls
            .Select(url => new Uri(url, UriKind.Absolute))
            .ToArray();

        if (parsed.Any(IsWildcardHost))
        {
            if (deployment.Mode == AccessMode.Local)
            {
                throw new KafdeckConfigurationException(
                    "Wildcard host filtering is unavailable in Local access mode.");
            }

            // A wildcard Kestrel bind intentionally accepts traffic addressed
            // to any interface. The deployment access boundary must therefore
            // be Token/OIDC (validated separately) rather than Host filtering.
            return Array.AsReadOnly(new[] { "*" });
        }

        var hosts = parsed
            .Select(uri => uri.Host)
            .Concat(parsed.All(uri => IsLoopbackHost(uri.Host))
                ? LoopbackHosts
                : Array.Empty<string>())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return Array.AsReadOnly(hosts);
    }

    private static bool IsWildcardHost(Uri uri)
    {
        var host = uri.Host.Trim('[', ']');
        return IPAddress.TryParse(host, out var address) &&
               (address.Equals(IPAddress.Any) ||
                address.Equals(IPAddress.IPv6Any));
    }

    private static bool IsLoopbackHost(string host)
    {
        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var normalized = host.Trim('[', ']');
        return IPAddress.TryParse(normalized, out var address) &&
               IPAddress.IsLoopback(address);
    }
}
