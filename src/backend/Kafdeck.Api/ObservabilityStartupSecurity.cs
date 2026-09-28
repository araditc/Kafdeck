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
            DeploymentAccessTokenValidator.Matches(
                deploymentAccessToken,
                otlpHeaders))
        {
            throw new KafdeckConfigurationException(
                "OTLP headers must resolve to credential material distinct from the deployment access token.");
        }

        if (prometheusScrapeToken is not null &&
            DeploymentAccessTokenValidator.Matches(
                prometheusScrapeToken,
                otlpHeaders))
        {
            throw new KafdeckConfigurationException(
                "OTLP headers must resolve to credential material distinct from the Prometheus scrape token.");
        }
    }
}
