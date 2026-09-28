using Kafdeck.Core.Records;
using Kafdeck.Core.Security;

namespace Kafdeck.Infrastructure.Configuration;

public enum KafkaSecurityProtocol
{
    Plaintext = 1,
    Ssl = 2,
    SaslPlaintext = 3,
    SaslSsl = 4,
}

public enum SaslMechanism
{
    Plain = 1,
    ScramSha256 = 2,
    ScramSha512 = 3,
}

public sealed record KafdeckOptions(
    DeploymentOptions Deployment,
    IReadOnlyList<ClusterProfile> Clusters,
    RecordDataOptions? Records = null,
    TopicCatalogOptions? Catalog = null,
    AdministrationOptions? Administration = null,
    DataGeneratorOptions? Generator = null,
    ObservabilityOptions? Observability = null);

public enum MutationPersistenceProvider
{
    Sqlite = 1,
    PostgreSql = 2,
}

public enum MutationExecutionMode
{
    Standalone = 1,
    HighAvailability = 2,
}

public sealed record AdministrationOptions(
    MutationOptions Mutations,
    ConnectAutoRestartOptions? ConnectAutoRestart = null);

public sealed record ConnectAutoRestartOptions(
    bool Enabled,
    string PolicyVersion,
    int MaxAttempts,
    int InitialBackoffSeconds,
    int MaxBackoffSeconds,
    int ActivationLifetimeSeconds,
    int MaxActivePoliciesPerProfile,
    int JitterBasisPoints)
{
    public static ConnectAutoRestartOptions DisabledDefault { get; } =
        new(
            Enabled: false,
            PolicyVersion: "v0.7-w55-default",
            MaxAttempts: 3,
            InitialBackoffSeconds: 10,
            MaxBackoffSeconds: 300,
            ActivationLifetimeSeconds: 1800,
            MaxActivePoliciesPerProfile: 10,
            JitterBasisPoints: 2000);
}

public sealed record MutationOptions(
    bool Enabled,
    MutationPersistenceOptions? Persistence,
    SecretReference? MaterialDigestKey,
    TimeSpan PreviewTtl,
    int MaxConcurrentPerCluster);

public sealed record MutationPersistenceOptions(
    MutationPersistenceProvider Provider,
    MutationExecutionMode ExecutionMode,
    string? SqliteDatabasePath,
    SecretReference? ConnectionString);

public sealed record RecordDataOptions(
    RecordMaskingPolicyDefinition MaskingPolicy);

public sealed record DataGeneratorOptions(
    IReadOnlyList<string> EnabledClusterIds);

public sealed record ObservabilityOptions(
    PrometheusMetricsOptions Prometheus,
    int MaxActiveMetricSeries,
    int MaxMetricLabelsPerSeries,
    int MaxMetricLabelValueBytes,
    int MaxTraceAttributes,
    int MaxLogAttributes,
    int MaxDiagnosticStringBytes)
{
    public static ObservabilityOptions Default { get; } =
        new(
            new PrometheusMetricsOptions(
                Enabled: false,
                Path: "/metrics",
                ScrapeToken: null),
            MaxActiveMetricSeries: 10_000,
            MaxMetricLabelsPerSeries: 8,
            MaxMetricLabelValueBytes: 64,
            MaxTraceAttributes: 24,
            MaxLogAttributes: 24,
            MaxDiagnosticStringBytes: 512);
}

public sealed record PrometheusMetricsOptions(
    bool Enabled,
    string Path,
    SecretReference? ScrapeToken);

public sealed record TopicCatalogOptions(
    IReadOnlyList<TopicCatalogEntryProfile> Topics);

public sealed record TopicCatalogEntryProfile(
    string ClusterId,
    string TopicName,
    string? Description,
    string? Owner,
    string? Domain,
    IReadOnlyList<string> Tags,
    string? DocumentationReference,
    string? Classification);

public sealed record DeploymentOptions(
    string ListenUrl,
    SecretReference? AccessToken,
    AccessMode Mode,
    OidcProfile? Oidc,
    IReadOnlyList<string>? AdditionalListenUrls = null)
{
    public IReadOnlyList<string> ListenUrls { get; } =
        BuildListenUrls(ListenUrl, AdditionalListenUrls);

    public DeploymentOptions(string listenUrl, SecretReference? accessToken)
        : this(
            listenUrl,
            accessToken,
            accessToken is null ? AccessMode.Local : AccessMode.Token,
            null)
    {
    }

    private static IReadOnlyList<string> BuildListenUrls(
        string listenUrl,
        IReadOnlyList<string>? additionalListenUrls)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(listenUrl);

        var items = new List<string> { listenUrl.Trim() };
        if (additionalListenUrls is not null)
        {
            items.AddRange(
                additionalListenUrls
                    .Where(value => !string.IsNullOrWhiteSpace(value))
                    .Select(value => value.Trim()));
        }

        return Array.AsReadOnly(
            items
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray());
    }
}

public sealed record OidcProfile(
    string Issuer,
    string ClientId,
    SecretReference? ClientSecret,
    string? GroupClaim,
    IReadOnlyList<string> Scopes);

public sealed record ClusterProfile(
    string Id,
    IReadOnlyList<string> BootstrapServers,
    KafkaSecurityProtocol SecurityProtocol,
    TlsProfile? Tls,
    SaslProfile? Sasl,
    SchemaRegistryProfile? SchemaRegistry = null,
    KafkaConnectProfile? Connect = null,
    KsqlDbProfile? KsqlDb = null,
    IReadOnlyList<KafkaConnectProfile>? ConnectProfiles = null,
    StreamsTelemetryProfile? StreamsTelemetry = null);

public enum SchemaRegistryProviderProfile
{
    ConfluentCompatibleV1 = 1,
    KarapaceCompatibleV1 = 2,
    ApicurioV3 = 3,
}

public sealed record SchemaRegistryProfile(
    string Url,
    SecretReference? Username,
    SecretReference? Password,
    SchemaRegistryProviderProfile ProviderProfile =
        SchemaRegistryProviderProfile.ConfluentCompatibleV1);

public enum KafkaConnectMutationProviderProfile
{
    None = 0,
    ConfluentCompatibleV1 = 1,
}

public sealed record KafkaConnectProfile(
    string Url,
    SecretReference? Username,
    SecretReference? Password,
    KafkaConnectMutationProviderProfile MutationProviderProfile =
        KafkaConnectMutationProviderProfile.None,
    string Id = KafkaConnectProfileSet.DefaultProfileId);

public static class KafkaConnectProfileSet
{
    public const string DefaultProfileId = "default";
    public const int MaxProfilesPerCluster = 16;
    public const int MaxProfileIdLength = 128;

    public static IReadOnlyList<KafkaConnectProfile> Effective(
        ClusterProfile cluster)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        if (cluster.ConnectProfiles is { Count: > 0 })
        {
            return cluster.ConnectProfiles;
        }

        return cluster.Connect is null
            ? Array.Empty<KafkaConnectProfile>()
            : new[] { cluster.Connect };
    }

    public static KafkaConnectProfile? Default(
        ClusterProfile cluster) =>
        Effective(cluster).FirstOrDefault(
            profile => string.Equals(
                profile.Id,
                DefaultProfileId,
                StringComparison.Ordinal));
}

public sealed record KsqlDbProfile(
    string Url,
    SecretReference? Username,
    SecretReference? Password);

public enum StreamsTelemetryProviderProfile
{
    KafdeckTelemetryV1 = 1,
}

public sealed record StreamsTelemetryProfile(
    string Url,
    SecretReference? Username,
    SecretReference? Password,
    StreamsTelemetryProviderProfile ProviderProfile =
        StreamsTelemetryProviderProfile.KafdeckTelemetryV1);

public sealed record TlsProfile(
    bool VerifyServerCertificate,
    SecretReference? CaCertificate,
    SecretReference? ClientCertificate,
    SecretReference? ClientKey);

public sealed record SaslProfile(
    SaslMechanism Mechanism,
    SecretReference Username,
    SecretReference Password);
