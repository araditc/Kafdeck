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

    internal static bool ContainsCredential(
        string resolvedHeaders,
        string credential)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            resolvedHeaders);
        ArgumentException.ThrowIfNullOrWhiteSpace(
            credential);

        if (resolvedHeaders.Length > 16 * 1024)
        {
            throw new KafdeckConfigurationException(
                "Resolved OTLP headers exceed the 16 KiB safety limit.");
        }

        if (DeploymentAccessTokenValidator.Matches(
                credential,
                resolvedHeaders))
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
            if (DeploymentAccessTokenValidator.Matches(
                    credential,
                    value))
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
                DeploymentAccessTokenValidator.Matches(
                    credential,
                    decoded))
            {
                return true;
            }
        }

        return false;
    }
}
