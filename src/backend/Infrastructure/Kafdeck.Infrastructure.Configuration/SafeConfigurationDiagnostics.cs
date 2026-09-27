using Kafdeck.Core.Security;

namespace Kafdeck.Infrastructure.Configuration;

public sealed record SafeConfigurationDiagnostic(
    string ListenHost,
    int ListenEndpointCount,
    bool IsRemoteBinding,
    AccessMode AccessMode,
    bool DeploymentTokenConfigured,
    bool OidcConfigured,
    int TopicCatalogEntryCount,
    bool MutationModeEnabled,
    MutationPersistenceProvider? MutationPersistenceProvider,
    MutationExecutionMode? MutationExecutionMode,
    IReadOnlyList<SafeClusterDiagnostic> Clusters,
    bool ObservabilityConfigured = false,
    int ObservabilityMaxActiveSeries = ObservabilityOptions.DefaultMaxActiveSeries,
    bool PrometheusEnabled = false,
    bool PrometheusAccessTokenConfigured = false);

public sealed record SafeClusterDiagnostic(
    string ClusterId,
    int BootstrapServerCount,
    KafkaSecurityProtocol SecurityProtocol,
    bool TlsConfigured,
    bool SaslConfigured,
    bool SchemaRegistryConfigured,
    bool ConnectConfigured,
    bool KsqlDbConfigured,
    bool StreamsTelemetryConfigured = false);

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
                KafkaConnectProfileSet.Effective(cluster).Count > 0,
                cluster.KsqlDb is not null,
                cluster.StreamsTelemetry is not null))
            .ToArray();

        return new SafeConfigurationDiagnostic(
            listenHost,
            options.Deployment.ListenUrls.Count,
            options.Deployment.ListenUrls.Any(
                url => !KafdeckConfigurationValidator.IsLoopbackBinding(url)),
            options.Deployment.Mode,
            options.Deployment.AccessToken is not null,
            options.Deployment.Oidc is not null,
            options.Catalog?.Topics.Count ?? 0,
            options.Administration?.Mutations.Enabled ?? false,
            options.Administration?.Mutations.Persistence?.Provider,
            options.Administration?.Mutations.Persistence?.ExecutionMode,
            Array.AsReadOnly(clusterDiagnostics),
            options.Observability is not null,
            ObservabilityOptions.Effective(options).MaxActiveSeries,
            ObservabilityOptions.Effective(options).Prometheus.Enabled,
            ObservabilityOptions.Effective(options).Prometheus.AccessToken is not null);
    }
}
