using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Security;

namespace Kafdeck.Api;

public static class ObservabilityStartupSecurity
{
    public static void ValidateResolvedCredentialIsolation(
        string? deploymentAccessToken,
        string? prometheusScrapeToken)
    {
        if (deploymentAccessToken is null ||
            prometheusScrapeToken is null)
        {
            return;
        }

        if (DeploymentAccessTokenValidator.Matches(
                deploymentAccessToken,
                prometheusScrapeToken))
        {
            throw new KafdeckConfigurationException(
                "Prometheus scrape token must resolve to credential material distinct from the deployment access token.");
        }
    }
}
