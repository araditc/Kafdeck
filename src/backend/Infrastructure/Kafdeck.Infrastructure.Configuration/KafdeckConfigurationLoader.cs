using Kafdeck.Core.Records;
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
        var records = LoadRecordData(configuration.GetSection("Kafdeck:Records"));
        var catalog = LoadTopicCatalog(configuration.GetSection("Kafdeck:Catalog"));

        return new KafdeckOptions(
            new DeploymentOptions(listenUrl, accessToken, accessMode, oidc),
            Array.AsReadOnly(clusters),
            records,
            catalog);
    }

    private static RecordDataOptions LoadRecordData(IConfigurationSection section)
    {
        var masking = section.GetSection("Masking");
        var policyId = masking["PolicyId"] ?? "default";
        var version = int.TryParse(masking["Version"], out var parsedVersion) ? parsedVersion : 1;
        var maskKey = ParseOptionalBoolean(masking["MaskKey"], false, "Records masking MaskKey");
        var keyReplacement = masking["KeyReplacement"] ?? "[REDACTED]";

        var structuredRules = masking
            .GetSection("StructuredRules")
            .GetChildren()
            .Select(rule => new RecordStructuredMaskRule(
                rule["Path"] ?? string.Empty,
                rule["Replacement"] ?? "[REDACTED]"))
            .ToArray();

        var headerRules = masking
            .GetSection("HeaderRules")
            .GetChildren()
            .Select(rule => new RecordHeaderMaskRule(
                rule["Name"] ?? string.Empty,
                rule["Replacement"] ?? "[REDACTED]"))
            .ToArray();

        return new RecordDataOptions(
            new RecordMaskingPolicyDefinition(
                policyId,
                version,
                Array.AsReadOnly(structuredRules),
                Array.AsReadOnly(headerRules),
                maskKey,
                keyReplacement));
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

        var connectSection = section.GetSection("Connect");
        KafkaConnectProfile? connect = null;
        if (connectSection.GetChildren().Any())
        {
            connect = new KafkaConnectProfile(
                connectSection["Url"] ?? string.Empty,
                ParseOptionalSecret(connectSection["Username"]),
                ParseOptionalSecret(connectSection["Password"]));
        }

        var ksqlSection = section.GetSection("KsqlDb");
        KsqlDbProfile? ksqlDb = null;
        if (ksqlSection.GetChildren().Any())
        {
            ksqlDb = new KsqlDbProfile(
                ksqlSection["Url"] ?? string.Empty,
                ParseOptionalSecret(ksqlSection["Username"]),
                ParseOptionalSecret(ksqlSection["Password"]));
        }

        return new ClusterProfile(
            id,
            Array.AsReadOnly(bootstrapServers),
            securityProtocol,
            tls,
            sasl,
            schemaRegistry,
            connect,
            ksqlDb);
    }

    private static TopicCatalogOptions? LoadTopicCatalog(IConfigurationSection section)
    {
        var topics = section
            .GetSection("Topics")
            .GetChildren()
            .Select(item =>
            {
                var tags = item.GetSection("Tags")
                    .GetChildren()
                    .Select(tag => tag.Value ?? string.Empty)
                    .ToArray();

                return new TopicCatalogEntryProfile(
                    item["ClusterId"] ?? string.Empty,
                    item["TopicName"] ?? string.Empty,
                    NullIfBlank(item["Description"]),
                    NullIfBlank(item["Owner"]),
                    NullIfBlank(item["Domain"]),
                    Array.AsReadOnly(tags),
                    NullIfBlank(item["DocumentationReference"]),
                    NullIfBlank(item["Classification"]));
            })
            .ToArray();

        return topics.Length == 0 ? null : new TopicCatalogOptions(Array.AsReadOnly(topics));
    }

    private static string? NullIfBlank(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

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

    private static bool ParseOptionalBoolean(string? value, bool defaultValue, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        if (bool.TryParse(value, out var parsed))
        {
            return parsed;
        }

        throw new KafdeckConfigurationException($"{fieldName} value must be true or false.");
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
