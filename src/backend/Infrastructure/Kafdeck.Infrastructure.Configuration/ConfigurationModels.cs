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
    IReadOnlyList<ClusterProfile> Clusters);

public sealed record DeploymentOptions(
    string ListenUrl,
    SecretReference? AccessToken);

public sealed record ClusterProfile(
    string Id,
    IReadOnlyList<string> BootstrapServers,
    KafkaSecurityProtocol SecurityProtocol,
    TlsProfile? Tls,
    SaslProfile? Sasl);

public sealed record TlsProfile(
    bool VerifyServerCertificate,
    SecretReference? CaCertificate,
    SecretReference? ClientCertificate,
    SecretReference? ClientKey);

public sealed record SaslProfile(
    SaslMechanism Mechanism,
    SecretReference Username,
    SecretReference Password);
