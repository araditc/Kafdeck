using System.Net;
using System.Net.Sockets;

namespace Kafdeck.Cli;

/// <summary>
/// Pure, non-network loopback policy for a FUTURE browser-based CLI PKCE flow.
/// This class creates no listener, authenticates no caller, acquires no token
/// and is not used by the existing GET-only CLI command path.
/// </summary>
public static class CliOidcLoopbackPolicy
{
    public const string CallbackPath = "/oauth/callback";

    public static IPAddress ValidateRedirectUri(Uri redirect)
    {
        ArgumentNullException.ThrowIfNull(redirect);

        // An ephemeral loopback redirect URI must be exact, finite, absolute
        // and free of caller-provided credentials or callback parameters.
        if (!redirect.IsAbsoluteUri ||
            !string.Equals(redirect.Scheme, Uri.UriSchemeHttp, StringComparison.Ordinal) ||
            !string.IsNullOrEmpty(redirect.UserInfo) ||
            !string.IsNullOrEmpty(redirect.Query) ||
            !string.IsNullOrEmpty(redirect.Fragment) ||
            !string.Equals(redirect.AbsolutePath, CallbackPath, StringComparison.Ordinal) ||
            redirect.Port is < 1024 or > 65535 ||
            redirect.IsDefaultPort ||
            !string.Equals(redirect.OriginalString, redirect.AbsoluteUri, StringComparison.Ordinal))
        {
            throw new CliUsageException(
                "OIDC CLI callback must be an exact IP-literal HTTP loopback URI with a non-privileged explicit port.");
        }

        if (redirect.HostNameType is not (UriHostNameType.IPv4 or UriHostNameType.IPv6) ||
            !IPAddress.TryParse(redirect.Host.Trim('[', ']'), out var address) ||
            !(address.Equals(IPAddress.Loopback) || address.Equals(IPAddress.IPv6Loopback)))
        {
            throw new CliUsageException(
                "OIDC CLI callback must use 127.0.0.1 or [::1], never a hostname or other interface.");
        }

        return address;
    }

    public static void RequireExactListenerAddress(Uri redirect, IPAddress listenerAddress)
    {
        var expected = ValidateRedirectUri(redirect);
        ArgumentNullException.ThrowIfNull(listenerAddress);

        // Do not permit IPAddress.Any, IPv6Any, a different loopback family
        // or any hostname/DNS fallback. Binding is an independently checked
        // effect performed by a separately approved future CLI listener.
        if (!listenerAddress.Equals(expected))
        {
            throw new CliUsageException(
                "OIDC CLI callback listener must bind exclusively to the exact configured loopback IP.");
        }
    }
}
