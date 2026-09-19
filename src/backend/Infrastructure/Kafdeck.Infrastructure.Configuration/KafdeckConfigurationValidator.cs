using System.Net;
using Kafdeck.Core.Security;

namespace Kafdeck.Infrastructure.Configuration;

public sealed class KafdeckConfigurationException : Exception
{
    public KafdeckConfigurationException(string message)
        : base(message)
    {
    }

    public KafdeckConfigurationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

public static class KafdeckConfigurationValidator
{
    public static void ValidateAndThrow(KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var errors = new List<string>();
        ValidateDeployment(options.Deployment, errors);
        ValidateClusters(options.Clusters, errors);

        if (errors.Count > 0)
        {
            throw new KafdeckConfigurationException(string.Join(" ", errors));
        }
    }

    public static bool IsLoopbackBinding(string listenUrl)
    {
        if (!Uri.TryCreate(listenUrl, UriKind.Absolute, out var uri))
        {
            return false;
        }

        if (string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var host = uri.Host.Trim('[', ']');
        return IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address);
    }

    private static void ValidateDeployment(DeploymentOptions deployment, ICollection<string> errors)
    {
        if (!Uri.TryCreate(deployment.ListenUrl, UriKind.Absolute, out var uri) ||
            !(string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) ||
              string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
        {
            errors.Add("Deployment listen URL must be an absolute HTTP or HTTPS URL.");
            return;
        }

        switch (deployment.Mode)
        {
            case AccessMode.Local:
                if (!IsLoopbackBinding(deployment.ListenUrl))
                {
                    errors.Add("Non-loopback deployment binding requires an access-token secret reference in Token mode or an enabled OIDC mode.");
                }

                if (deployment.AccessToken is not null)
                {
                    errors.Add("Local access mode must not configure a deployment access token.");
                }

                if (deployment.Oidc is not null)
                {
                    errors.Add("Local access mode must not configure OIDC settings.");
                }

                break;

            case AccessMode.Token:
                if (deployment.AccessToken is null)
                {
                    errors.Add("Token access mode requires an access-token secret reference.");
                }

                if (deployment.Oidc is not null)
                {
                    errors.Add("Token access mode must not configure OIDC settings.");
                }

                break;

            case AccessMode.Oidc:
                if (deployment.Oidc is null)
                {
                    errors.Add("OIDC access mode requires OIDC configuration.");
                }
                else
                {
                    ValidateOidc(deployment.Oidc, errors);
                }

                errors.Add("OIDC access mode is defined by v0.2 contracts but cannot be activated until the W13 OIDC session adapter is implemented.");
                break;

            default:
                errors.Add("Deployment access mode is unsupported.");
                break;
        }
    }

    private static void ValidateOidc(OidcProfile oidc, ICollection<string> errors)
    {
        if (!Uri.TryCreate(oidc.Issuer, UriKind.Absolute, out var issuer) ||
            !(string.Equals(issuer.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase) ||
              string.Equals(issuer.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)))
        {
            errors.Add("OIDC issuer must be an absolute HTTP or HTTPS URL.");
        }
        else if (!string.Equals(issuer.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase) &&
                 !IPAddress.TryParse(issuer.Host.Trim('[', ']'), out var address))
        {
            if (!string.Equals(issuer.Host, "localhost", StringComparison.OrdinalIgnoreCase))
            {
                errors.Add("OIDC issuer must use HTTPS unless it is a loopback development issuer.");
            }
        }
        else if (!string.Equals(issuer.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase) &&
                 address is not null &&
                 !IPAddress.IsLoopback(address))
        {
            errors.Add("OIDC issuer must use HTTPS unless it is a loopback development issuer.");
        }

        if (string.IsNullOrWhiteSpace(oidc.ClientId) || oidc.ClientId.Trim().Length > 512)
        {
            errors.Add("OIDC client ID is required and must not exceed 512 characters.");
        }

        if (oidc.GroupClaim is { Length: > 256 })
        {
            errors.Add("OIDC group-claim name must not exceed 256 characters.");
        }

        if (oidc.Scopes.Count == 0 ||
            oidc.Scopes.Count > 16 ||
            oidc.Scopes.Any(string.IsNullOrWhiteSpace) ||
            oidc.Scopes.Any(scope => scope.Trim().Length > 128) ||
            oidc.Scopes.Distinct(StringComparer.Ordinal).Count() != oidc.Scopes.Count)
        {
            errors.Add("OIDC scopes must contain 1 to 16 unique non-empty values of at most 128 characters.");
        }
        else if (!oidc.Scopes.Contains("openid", StringComparer.Ordinal))
        {
            errors.Add("OIDC scopes must include 'openid'.");
        }
    }

    private static void ValidateClusters(IReadOnlyList<ClusterProfile> clusters, ICollection<string> errors)
    {
        var clusterIds = new HashSet<string>(StringComparer.Ordinal);

        foreach (var cluster in clusters)
        {
            if (string.IsNullOrWhiteSpace(cluster.Id))
            {
                errors.Add("Every cluster profile requires a stable non-empty ID.");
            }
            else if (!clusterIds.Add(cluster.Id))
            {
                errors.Add("Cluster profile IDs must be unique.");
            }

            if (cluster.BootstrapServers.Count == 0 || cluster.BootstrapServers.Any(string.IsNullOrWhiteSpace))
            {
                errors.Add($"Cluster '{cluster.Id}' requires at least one non-empty bootstrap server.");
            }

            var usesTls = cluster.SecurityProtocol is KafkaSecurityProtocol.Ssl or KafkaSecurityProtocol.SaslSsl;
            var usesSasl = cluster.SecurityProtocol is KafkaSecurityProtocol.SaslPlaintext or KafkaSecurityProtocol.SaslSsl;

            if (usesTls)
            {
                if (cluster.Tls is null)
                {
                    errors.Add($"Cluster '{cluster.Id}' requires TLS configuration for its selected security protocol.");
                }
                else
                {
                    if (!cluster.Tls.VerifyServerCertificate)
                    {
                        errors.Add($"Cluster '{cluster.Id}' cannot disable TLS server-certificate verification.");
                    }

                    var hasClientCertificate = cluster.Tls.ClientCertificate is not null;
                    var hasClientKey = cluster.Tls.ClientKey is not null;
                    if (hasClientCertificate != hasClientKey)
                    {
                        errors.Add($"Cluster '{cluster.Id}' mTLS configuration requires both client certificate and client key references.");
                    }
                }
            }
            else if (cluster.Tls is not null)
            {
                errors.Add($"Cluster '{cluster.Id}' provides TLS settings while using a non-TLS security protocol.");
            }

            if (usesSasl && cluster.Sasl is null)
            {
                errors.Add($"Cluster '{cluster.Id}' requires SASL credentials for its selected security protocol.");
            }
            else if (!usesSasl && cluster.Sasl is not null)
            {
                errors.Add($"Cluster '{cluster.Id}' provides SASL settings while using a non-SASL security protocol.");
            }
        }
    }
}
