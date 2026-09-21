using Kafdeck.Core.Security;

namespace Kafdeck.Infrastructure.Configuration;

public sealed record SafeConfigurationDiagnostic(
    string ListenHost,
    bool IsRemoteBinding,
    AccessMode AccessMode,
    bool DeploymentTokenConfigured,
    bool OidcConfigured,
    int TopicCatalogEntryCount,
    bool MutationModeEnabled,
    MutationPersistenceProvider? MutationPersistenceProvider,
    MutationExecutionMode? MutationExecutionMode,
    IReadOnlyList<SafeClusterDiagnostic> Clusters);

public sealed record SafeClusterDiagnostic(
    string ClusterId,
    int BootstrapServerCount,
    KafkaSecurityProtocol SecurityProtocol,
    bool TlsConfigured,
    bool SaslConfigured,
    bool SchemaRegistryConfigured,
    bool ConnectConfigured,
    bool KsqlDbConfigured);

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
                cluster.Sasl is not null,
                cluster.SchemaRegistry is not null,
                cluster.Connect is not null,
                cluster.KsqlDb is not null))
            .ToArray();

        return new SafeConfigurationDiagnostic(
            listenHost,
            !KafdeckConfigurationValidator.IsLoopbackBinding(options.Deployment.ListenUrl),
            options.Deployment.Mode,
            options.Deployment.AccessToken is not null,
            options.Deployment.Oidc is not null,
            options.Catalog?.Topics.Count ?? 0,
            options.Administration?.Mutations.Enabled ?? false,
            options.Administration?.Mutations.Persistence?.Provider,
            options.Administration?.Mutations.Persistence?.ExecutionMode,
            Array.AsReadOnly(clusterDiagnostics));
    }
}
