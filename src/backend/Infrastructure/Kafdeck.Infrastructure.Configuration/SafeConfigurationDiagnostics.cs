using Kafdeck.Core.Security;

namespace Kafdeck.Infrastructure.Configuration;

public sealed record SafeConfigurationDiagnostic(
    string ListenHost,
    bool IsRemoteBinding,
    AccessMode AccessMode,
    bool DeploymentTokenConfigured,
    bool OidcConfigured,
    IReadOnlyList<SafeClusterDiagnostic> Clusters);

public sealed record SafeClusterDiagnostic(
    string ClusterId,
    int BootstrapServerCount,
    KafkaSecurityProtocol SecurityProtocol,
    bool TlsConfigured,
    bool SaslConfigured);

public static class SafeConfigurationDiagnostics
{
    public static SafeConfigurationDiagnostic Create(KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var listenHost = Uri.TryCreate(options.Deployment.ListenUrl, UriKind.Absolute, out var listenUri)
            ? listenUri.Host
            : "<invalid>";

        var clusterDiagnostics = options.Clusters
            .Select(cluster => new SafeClusterDiagnostic(
                cluster.Id,
                cluster.BootstrapServers.Count,
                cluster.SecurityProtocol,
                cluster.Tls is not null,
                cluster.Sasl is not null))
            .ToArray();

        return new SafeConfigurationDiagnostic(
            listenHost,
            !KafdeckConfigurationValidator.IsLoopbackBinding(options.Deployment.ListenUrl),
            options.Deployment.Mode,
            options.Deployment.AccessToken is not null,
            options.Deployment.Oidc is not null,
            Array.AsReadOnly(clusterDiagnostics));
    }
}
