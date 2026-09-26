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
        ValidateCatalog(options.Catalog, options.Clusters, errors);
        ValidateAdministration(options.Administration, options.Deployment, errors);

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
        if (deployment.ListenUrls.Count == 0)
        {
            errors.Add("At least one deployment listen URL is required.");
            return;
        }

        var parsed = new List<(string Value, Uri Uri)>(deployment.ListenUrls.Count);
        foreach (var listenUrl in deployment.ListenUrls)
        {
            if (!Uri.TryCreate(listenUrl, UriKind.Absolute, out var uri) ||
                !(string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) ||
                  string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
            {
                errors.Add($"Deployment listen URL '{listenUrl}' must be an absolute HTTP or HTTPS URL.");
                continue;
            }

            parsed.Add((listenUrl, uri));
        }

        if (parsed.Count != deployment.ListenUrls.Count)
        {
            return;
        }

        switch (deployment.Mode)
        {
            case AccessMode.Local:
                if (deployment.ListenUrls.Any(url => !IsLoopbackBinding(url)))
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
                if (deployment.AccessToken is not null)
                {
                    errors.Add("OIDC access mode must not configure a deployment access token.");
                }

                if (parsed.Any(item =>
                        !IsLoopbackBinding(item.Value) &&
                        !string.Equals(
                            item.Uri.Scheme,
                            Uri.UriSchemeHttps,
                            StringComparison.OrdinalIgnoreCase)))
                {
                    errors.Add("Every non-loopback OIDC deployment binding requires HTTPS.");
                }

                if (deployment.Oidc is null)
                {
                    errors.Add("OIDC access mode requires OIDC configuration.");
                }
                else
                {
                    ValidateOidc(deployment.Oidc, errors);
                }

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
        else if (!string.Equals(issuer.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            var host = issuer.Host.Trim('[', ']');
            var isLoopbackIssuer =
                string.Equals(issuer.Host, "localhost", StringComparison.OrdinalIgnoreCase) ||
                (IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address));

            if (!isLoopbackIssuer)
            {
                errors.Add("OIDC issuer must use HTTPS unless it is a loopback development issuer.");
            }
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

    private static void ValidateAdministration(
        AdministrationOptions? administration,
        DeploymentOptions deployment,
        ICollection<string> errors)
    {
        if (administration is null || !administration.Mutations.Enabled)
        {
            return;
        }

        var mutations = administration.Mutations;
        if (deployment.Mode != AccessMode.Oidc)
        {
            errors.Add("Mutation mode requires OIDC access mode so every mutation has a canonical RBAC principal.");
        }

        if (mutations.MaterialDigestKey is null)
        {
            errors.Add("Mutation mode requires a material-digest key secret reference.");
        }

        if (mutations.PreviewTtl < TimeSpan.FromSeconds(30) ||
            mutations.PreviewTtl > TimeSpan.FromMinutes(30))
        {
            errors.Add("Mutation preview TTL must be between 30 seconds and 30 minutes.");
        }

        if (mutations.MaxConcurrentPerCluster is < 1 or > 16)
        {
            errors.Add("Mutation max concurrency per cluster must be between 1 and 16.");
        }

        if (mutations.Persistence is null)
        {
            errors.Add("Mutation mode requires durable persistence configuration.");
            return;
        }

        var persistence = mutations.Persistence;
        switch (persistence.Provider)
        {
            case MutationPersistenceProvider.Sqlite:
                if (persistence.ExecutionMode != MutationExecutionMode.Standalone)
                {
                    errors.Add("SQLite mutation persistence supports standalone execution only.");
                }

                if (string.IsNullOrWhiteSpace(persistence.SqliteDatabasePath) ||
                    !Path.IsPathFullyQualified(persistence.SqliteDatabasePath))
                {
                    errors.Add("SQLite mutation persistence requires an absolute database path.");
                }

                if (persistence.ConnectionString is not null)
                {
                    errors.Add("SQLite mutation persistence must not configure a PostgreSQL connection-string secret.");
                }

                break;

            case MutationPersistenceProvider.PostgreSql:
                if (!string.IsNullOrWhiteSpace(persistence.SqliteDatabasePath))
                {
                    errors.Add("PostgreSQL mutation persistence must not configure a SQLite database path.");
                }

                if (persistence.ConnectionString is null)
                {
                    errors.Add("PostgreSQL mutation persistence requires a connection-string secret reference.");
                }

                break;

            default:
                errors.Add("Mutation persistence provider is unsupported.");
                break;
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

            if (cluster.SchemaRegistry is not null)
            {
                ValidateSchemaRegistry(cluster, errors);
            }

            if (cluster.Connect is not null)
            {
                ValidateReadOnlyHttpProfile(
                    cluster.Id,
                    "Kafka Connect",
                    cluster.Connect.Url,
                    cluster.Connect.Username,
                    cluster.Connect.Password,
                    errors);

                if (!Enum.IsDefined(cluster.Connect.MutationProviderProfile))
                {
                    errors.Add(
                        $"Cluster '{cluster.Id}' Kafka Connect mutation provider profile is unsupported.");
                }
            }

            if (cluster.KsqlDb is not null)
            {
                ValidateReadOnlyHttpProfile(
                    cluster.Id,
                    "ksqlDB",
                    cluster.KsqlDb.Url,
                    cluster.KsqlDb.Username,
                    cluster.KsqlDb.Password,
                    errors);
            }
        }
    }

    private static void ValidateCatalog(
        TopicCatalogOptions? catalog,
        IReadOnlyList<ClusterProfile> clusters,
        ICollection<string> errors)
    {
        if (catalog is null)
        {
            return;
        }

        if (catalog.Topics.Count > 4096)
        {
            errors.Add("Topic catalog must not contain more than 4096 entries.");
            return;
        }

        var clusterIds = clusters.Select(cluster => cluster.Id).ToHashSet(StringComparer.Ordinal);
        var keys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var entry in catalog.Topics)
        {
            if (!clusterIds.Contains(entry.ClusterId))
            {
                errors.Add($"Topic catalog entry references unknown cluster '{entry.ClusterId}'.");
            }

            if (string.IsNullOrWhiteSpace(entry.TopicName) || entry.TopicName.Trim().Length > 249)
            {
                errors.Add($"Topic catalog entry for cluster '{entry.ClusterId}' requires a topic name of at most 249 characters.");
            }

            if (!keys.Add($"{entry.ClusterId}\u001f{entry.TopicName}"))
            {
                errors.Add($"Topic catalog contains duplicate entry '{entry.ClusterId}/{entry.TopicName}'.");
            }

            if (entry.Tags.Count > 32 ||
                entry.Tags.Any(string.IsNullOrWhiteSpace) ||
                entry.Tags.Any(tag => tag.Trim().Length > 64) ||
                entry.Tags.Distinct(StringComparer.Ordinal).Count() != entry.Tags.Count)
            {
                errors.Add($"Topic catalog entry '{entry.ClusterId}/{entry.TopicName}' must contain at most 32 unique non-empty tags of at most 64 characters.");
            }

            ValidateOptionalText(entry.Description, 2048, "description", entry, errors);
            ValidateOptionalText(entry.Owner, 256, "owner", entry, errors);
            ValidateOptionalText(entry.Domain, 256, "domain", entry, errors);
            ValidateOptionalText(entry.DocumentationReference, 2048, "documentation reference", entry, errors);
            ValidateOptionalText(entry.Classification, 128, "classification", entry, errors);
        }
    }

    private static void ValidateOptionalText(
        string? value,
        int maxLength,
        string field,
        TopicCatalogEntryProfile entry,
        ICollection<string> errors)
    {
        if (value is { Length: > 0 } && value.Length > maxLength)
        {
            errors.Add($"Topic catalog entry '{entry.ClusterId}/{entry.TopicName}' {field} must not exceed {maxLength} characters.");
        }
    }

    private static void ValidateReadOnlyHttpProfile(
        string clusterId,
        string profileName,
        string url,
        SecretReference? username,
        SecretReference? password,
        ICollection<string> errors)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) ||
            !(string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) ||
              string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
        {
            errors.Add($"Cluster '{clusterId}' {profileName} URL must be an absolute HTTP or HTTPS URL.");
            return;
        }

        if (!string.IsNullOrEmpty(uri.UserInfo) || !string.IsNullOrEmpty(uri.Fragment) || !string.IsNullOrEmpty(uri.Query))
        {
            errors.Add($"Cluster '{clusterId}' {profileName} URL must not contain user-info, query, or fragment components.");
        }

        var hasUsername = username is not null;
        var hasPassword = password is not null;
        if (hasUsername != hasPassword)
        {
            errors.Add($"Cluster '{clusterId}' {profileName} basic authentication requires both username and password secret references.");
        }

        if (hasUsername &&
            !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            var host = uri.Host.Trim('[', ']');
            var isLoopback =
                string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase) ||
                (IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address));

            if (!isLoopback)
            {
                errors.Add($"Cluster '{clusterId}' {profileName} basic authentication requires HTTPS unless the endpoint is loopback-only.");
            }
        }
    }

    private static void ValidateSchemaRegistry(ClusterProfile cluster, ICollection<string> errors)
    {
        var registry = cluster.SchemaRegistry!;

        if (!Uri.TryCreate(registry.Url, UriKind.Absolute, out var uri) ||
            !(string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) ||
              string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
        {
            errors.Add($"Cluster '{cluster.Id}' Schema Registry URL must be an absolute HTTP or HTTPS URL.");
            return;
        }

        var hasUsername = registry.Username is not null;
        var hasPassword = registry.Password is not null;
        if (hasUsername != hasPassword)
        {
            errors.Add($"Cluster '{cluster.Id}' Schema Registry basic authentication requires both username and password secret references.");
        }

        if (hasUsername &&
            !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            var host = uri.Host.Trim('[', ']');
            var isLoopback =
                string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase) ||
                (IPAddress.TryParse(host, out var address) && IPAddress.IsLoopback(address));

            if (!isLoopback)
            {
                errors.Add($"Cluster '{cluster.Id}' Schema Registry basic authentication requires HTTPS unless the registry is loopback-only.");
            }
        }
    }
}
