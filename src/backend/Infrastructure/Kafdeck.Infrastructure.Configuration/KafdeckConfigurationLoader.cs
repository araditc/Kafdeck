using Kafdeck.Core.Security;
using Microsoft.Extensions.Configuration;

namespace Kafdeck.Infrastructure.Configuration;

public static class KafdeckConfigurationLoader
{
    public static KafdeckOptions Load(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var deploymentSection = configuration.GetSection("Kafdeck:Deployment");
        var listenUrl = deploymentSection["ListenUrl"] ?? "http://127.0.0.1:8080";
        var accessToken = ParseOptionalSecret(deploymentSection["AccessToken"]);
        var accessMode = ParseAccessMode(deploymentSection["AccessMode"], accessToken);
        var oidc = LoadOidcProfile(deploymentSection.GetSection("Oidc"));

        var clusters = configuration
            .GetSection("Kafdeck:Clusters")
            .GetChildren()
            .Select(LoadCluster)
            .ToArray();

        return new KafdeckOptions(
            new DeploymentOptions(listenUrl, accessToken, accessMode, oidc),
            Array.AsReadOnly(clusters));
    }

    private static AccessMode ParseAccessMode(string? value, SecretReference? accessToken)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return accessToken is null ? AccessMode.Local : AccessMode.Token;
        }

        return ParseEnum(value, AccessMode.Local, "Deployment access mode");
    }

    private static OidcProfile? LoadOidcProfile(IConfigurationSection section)
    {
        if (!section.GetChildren().Any())
        {
            return null;
        }

        var scopes = section
            .GetSection("Scopes")
            .GetChildren()
            .Select(child => child.Value ?? string.Empty)
            .ToArray();

        if (scopes.Length == 0)
        {
            scopes = ["openid", "profile"];
        }

        return new OidcProfile(
            section["Issuer"] ?? string.Empty,
            section["ClientId"] ?? string.Empty,
            ParseOptionalSecret(section["ClientSecret"]),
            string.IsNullOrWhiteSpace(section["GroupClaim"]) ? null : section["GroupClaim"]!.Trim(),
            Array.AsReadOnly(scopes));
    }

    private static ClusterProfile LoadCluster(IConfigurationSection section)
    {
        var id = section["Id"] ?? string.Empty;
        var bootstrapServers = section
            .GetSection("BootstrapServers")
            .GetChildren()
            .Select(child => child.Value ?? string.Empty)
            .ToArray();

        var securityProtocol = ParseEnum(
            section["SecurityProtocol"],
            KafkaSecurityProtocol.Plaintext,
            "Kafka security protocol");

        var tlsSection = section.GetSection("Tls");
        var tlsConfigured = tlsSection.GetChildren().Any();
        TlsProfile? tls = null;
        if (tlsConfigured)
        {
            var verifyServerCertificate = true;
            if (bool.TryParse(tlsSection["VerifyServerCertificate"], out var configuredVerification))
            {
                verifyServerCertificate = configuredVerification;
            }

            tls = new TlsProfile(
                verifyServerCertificate,
                ParseOptionalSecret(tlsSection["CaCertificate"]),
                ParseOptionalSecret(tlsSection["ClientCertificate"]),
                ParseOptionalSecret(tlsSection["ClientKey"]));
        }

        var saslSection = section.GetSection("Sasl");
        var saslConfigured = saslSection.GetChildren().Any();
        SaslProfile? sasl = null;
        if (saslConfigured)
        {
            var mechanism = ParseEnum(
                saslSection["Mechanism"],
                SaslMechanism.Plain,
                "SASL mechanism");

            sasl = new SaslProfile(
                mechanism,
                ParseRequiredSecret(saslSection["Username"], "SASL username"),
                ParseRequiredSecret(saslSection["Password"], "SASL password"));
        }

        var registrySection = section.GetSection("SchemaRegistry");
        SchemaRegistryProfile? schemaRegistry = null;
        if (registrySection.GetChildren().Any())
        {
            schemaRegistry = new SchemaRegistryProfile(
                registrySection["Url"] ?? string.Empty,
                ParseOptionalSecret(registrySection["Username"]),
                ParseOptionalSecret(registrySection["Password"]));
        }

        return new ClusterProfile(
            id,
            Array.AsReadOnly(bootstrapServers),
            securityProtocol,
            tls,
            sasl,
            schemaRegistry);
    }

    private static SecretReference? ParseOptionalSecret(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : SecretReference.Parse(value);

    private static SecretReference ParseRequiredSecret(string? value, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new KafdeckConfigurationException($"{fieldName} secret reference is required.");
        }

        return SecretReference.Parse(value);
    }

    private static TEnum ParseEnum<TEnum>(string? value, TEnum defaultValue, string fieldName)
        where TEnum : struct, Enum
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        var normalized = NormalizeEnumValue(value);
        foreach (var candidate in Enum.GetValues<TEnum>())
        {
            if (string.Equals(NormalizeEnumValue(candidate.ToString()), normalized, StringComparison.OrdinalIgnoreCase))
            {
                return candidate;
            }
        }

        throw new KafdeckConfigurationException($"{fieldName} value is unsupported.");
    }

    private static string NormalizeEnumValue(string value) =>
        value.Replace("-", string.Empty, StringComparison.Ordinal)
            .Replace("_", string.Empty, StringComparison.Ordinal)
            .Trim();
}
