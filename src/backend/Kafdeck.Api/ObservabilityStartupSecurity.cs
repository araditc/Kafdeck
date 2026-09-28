using System.Security.Cryptography;
using System.Text;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Security;

namespace Kafdeck.Api;

public static class ObservabilityStartupSecurity
{
    public static void ValidateResolvedCredentialIsolation(
        string? deploymentAccessToken,
        string? prometheusScrapeToken,
        string? otlpHeaders = null)
    {
        if (deploymentAccessToken is not null &&
            prometheusScrapeToken is not null &&
            DeploymentAccessTokenValidator.Matches(
                deploymentAccessToken,
                prometheusScrapeToken))
        {
            throw new KafdeckConfigurationException(
                "Prometheus scrape token must resolve to credential material distinct from the deployment access token.");
        }

        if (otlpHeaders is null)
        {
            return;
        }

        ValidateResolvedHeaderMaterial(otlpHeaders);

        if (deploymentAccessToken is not null &&
            ContainsCredential(
                otlpHeaders,
                deploymentAccessToken))
        {
            throw new KafdeckConfigurationException(
                "OTLP headers must resolve to credential material distinct from the deployment access token.");
        }

        if (prometheusScrapeToken is not null &&
            ContainsCredential(
                otlpHeaders,
                prometheusScrapeToken))
        {
            throw new KafdeckConfigurationException(
                "OTLP headers must resolve to credential material distinct from the Prometheus scrape token.");
        }
    }

    internal static void ValidateResolvedHeaderMaterial(
        string resolvedHeaders)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            resolvedHeaders);

        if (resolvedHeaders.Length > 16 * 1024)
        {
            throw new KafdeckConfigurationException(
                "Resolved OTLP headers exceed the 16 KiB safety limit.");
        }
    }

    internal static bool ContainsCredential(
        string resolvedHeaders,
        string credential)
    {
        ValidateResolvedHeaderMaterial(resolvedHeaders);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            credential);

        if (MatchesCredentialCandidate(
                resolvedHeaders,
                credential))
        {
            return true;
        }

        foreach (var item in resolvedHeaders.Split(
                     ',',
                     StringSplitOptions.RemoveEmptyEntries |
                     StringSplitOptions.TrimEntries))
        {
            var separator = item.IndexOf('=');
            if (separator <= 0 ||
                separator == item.Length - 1)
            {
                continue;
            }

            var value = item[(separator + 1)..].Trim();
            if (MatchesCredentialCandidate(
                    value,
                    credential))
            {
                return true;
            }

            string decoded;
            try
            {
                decoded = Uri.UnescapeDataString(value);
            }
            catch (UriFormatException)
            {
                continue;
            }

            if (!string.Equals(
                    decoded,
                    value,
                    StringComparison.Ordinal) &&
                MatchesCredentialCandidate(
                    decoded,
                    credential))
            {
                return true;
            }
        }

        return false;
    }

    private static bool MatchesCredentialCandidate(
        string candidate,
        string credential)
    {
        var normalized = candidate.Trim();
        if (DeploymentAccessTokenValidator.Matches(
                credential,
                normalized))
        {
            return true;
        }

        var separator = normalized.IndexOf(' ');
        if (separator <= 0 ||
            separator == normalized.Length - 1)
        {
            return false;
        }

        // Authorization schemes are extensible. Treat any
        // non-empty scheme + credential wrapper as potentially carrying
        // local credential material; restricting this check to Bearer
        // would allow custom schemes (for example "Token <secret>") to
        // bypass startup credential isolation.
        var scheme = normalized[..separator].Trim();
        var wrappedValue =
            normalized[(separator + 1)..].Trim();

        if (scheme.Length == 0 ||
            wrappedValue.Length == 0)
        {
            return false;
        }

        if (DeploymentAccessTokenValidator.Matches(
                credential,
                wrappedValue))
        {
            return true;
        }

        if (!string.Equals(
                scheme,
                "Basic",
                StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        byte[] decodedBytes;
        try
        {
            decodedBytes =
                Convert.FromBase64String(
                    wrappedValue);
        }
        catch (FormatException)
        {
            return false;
        }

        try
        {
            var decoded =
                Encoding.UTF8.GetString(
                    decodedBytes);
            var delimiter =
                decoded.IndexOf(':');

            if (delimiter < 0)
            {
                return DeploymentAccessTokenValidator
                    .Matches(
                        credential,
                        decoded);
            }

            var username =
                decoded[..delimiter];
            var password =
                decoded[(delimiter + 1)..];

            return DeploymentAccessTokenValidator
                       .Matches(
                           credential,
                           username) ||
                   DeploymentAccessTokenValidator
                       .Matches(
                           credential,
                           password);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                decodedBytes);
        }
    }
}
