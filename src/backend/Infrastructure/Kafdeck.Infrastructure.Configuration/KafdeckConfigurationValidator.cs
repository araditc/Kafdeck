using System.Net;

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

        if (!IsLoopbackBinding(deployment.ListenUrl) && deployment.AccessToken is null)
        {
            errors.Add("Non-loopback deployment binding requires an access-token secret reference.");
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
