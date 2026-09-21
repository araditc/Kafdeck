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
    TopicCatalogOptions? Catalog = null);

public sealed record RecordDataOptions(
    RecordMaskingPolicyDefinition MaskingPolicy);

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
    OidcProfile? Oidc)
{
    public DeploymentOptions(string listenUrl, SecretReference? accessToken)
        : this(
            listenUrl,
            accessToken,
            accessToken is null ? AccessMode.Local : AccessMode.Token,
            null)
    {
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
    KsqlDbProfile? KsqlDb = null);

public sealed record SchemaRegistryProfile(
    string Url,
    SecretReference? Username,
    SecretReference? Password);

public sealed record KafkaConnectProfile(
    string Url,
    SecretReference? Username,
    SecretReference? Password);

public sealed record KsqlDbProfile(
    string Url,
    SecretReference? Username,
    SecretReference? Password);

public sealed record TlsProfile(
    bool VerifyServerCertificate,
    SecretReference? CaCertificate,
    SecretReference? ClientCertificate,
    SecretReference? ClientKey);

public sealed record SaslProfile(
    SaslMechanism Mechanism,
    SecretReference Username,
    SecretReference Password);
