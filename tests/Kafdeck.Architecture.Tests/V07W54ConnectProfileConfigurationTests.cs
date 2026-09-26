using System.Text.Json;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W54ConnectProfileConfigurationTests
{
    [Fact]
    public void Legacy_connect_configuration_normalizes_to_default_profile()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Clusters:0:Id"] = "prod",
            ["Kafdeck:Clusters:0:BootstrapServers:0"] = "broker.example:9092",
            ["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext",
            ["Kafdeck:Clusters:0:Connect:Url"] = "https://connect.example:8083",
            ["Kafdeck:Clusters:0:Connect:MutationProviderProfile"] = "ConfluentCompatibleV1",
        });

        var cluster = Assert.Single(options.Clusters);
        Assert.NotNull(cluster.Connect);
        Assert.Null(cluster.ConnectProfiles);

        var effective = KafkaConnectProfileSet.Effective(cluster);
        var profile = Assert.Single(effective);
        Assert.Equal(KafkaConnectProfileSet.DefaultProfileId, profile.Id);
        Assert.Equal("https://connect.example:8083", profile.Url);

        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Multi_profile_configuration_loads_stable_profile_ids()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Clusters:0:Id"] = "prod",
            ["Kafdeck:Clusters:0:BootstrapServers:0"] = "broker.example:9092",
            ["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext",
            ["Kafdeck:Clusters:0:ConnectProfiles:0:Id"] = "default",
            ["Kafdeck:Clusters:0:ConnectProfiles:0:Url"] = "https://connect-a.example:8083",
            ["Kafdeck:Clusters:0:ConnectProfiles:0:MutationProviderProfile"] = "ConfluentCompatibleV1",
            ["Kafdeck:Clusters:0:ConnectProfiles:1:Id"] = "analytics",
            ["Kafdeck:Clusters:0:ConnectProfiles:1:Url"] = "https://connect-b.example:8083",
            ["Kafdeck:Clusters:0:ConnectProfiles:1:MutationProviderProfile"] = "ConfluentCompatibleV1",
        });

        var cluster = Assert.Single(options.Clusters);
        Assert.Null(cluster.Connect);
        Assert.NotNull(cluster.ConnectProfiles);

        Assert.Equal(
            new[] { "default", "analytics" },
            KafkaConnectProfileSet.Effective(cluster)
                .Select(profile => profile.Id)
                .ToArray());

        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Mixed_legacy_and_multi_profile_configuration_fails_closed()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Clusters:0:Id"] = "prod",
            ["Kafdeck:Clusters:0:BootstrapServers:0"] = "broker.example:9092",
            ["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext",
            ["Kafdeck:Clusters:0:Connect:Url"] = "https://legacy.example:8083",
            ["Kafdeck:Clusters:0:ConnectProfiles:0:Id"] = "default",
            ["Kafdeck:Clusters:0:ConnectProfiles:0:Url"] = "https://new.example:8083",
        });

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains(
            "must not configure both legacy Connect and ConnectProfiles",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Duplicate_profile_ids_fail_closed()
    {
        var options = OptionsWithProfiles(
            new KafkaConnectProfile(
                "https://connect-a.example:8083",
                null,
                null,
                Id: "default"),
            new KafkaConnectProfile(
                "https://connect-b.example:8083",
                null,
                null,
                Id: "default"));

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains(
            "profile IDs must be unique",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Duplicate_http_origins_fail_closed_even_with_different_paths()
    {
        var options = OptionsWithProfiles(
            new KafkaConnectProfile(
                "https://connect.example:8083/a",
                null,
                null,
                Id: "default"),
            new KafkaConnectProfile(
                "https://connect.example:8083/b",
                null,
                null,
                Id: "analytics"));

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains(
            "must not target the same HTTP origin more than once",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Profile_count_and_id_shape_are_bounded()
    {
        var profiles = Enumerable.Range(
                0,
                KafkaConnectProfileSet.MaxProfilesPerCluster + 1)
            .Select(index =>
                new KafkaConnectProfile(
                    $"https://connect-{index}.example:8083",
                    null,
                    null,
                    Id: $"profile-{index}"))
            .ToArray();

        var tooMany = OptionsWithProfiles(profiles);
        var countException = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(tooMany));
        Assert.Contains(
            "must not configure more than",
            countException.Message,
            StringComparison.Ordinal);

        var invalidId = OptionsWithProfiles(
            new KafkaConnectProfile(
                "https://connect.example:8083",
                null,
                null,
                Id: "invalid/profile"));

        var idException = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(invalidId));
        Assert.Contains(
            "may contain only letters, digits",
            idException.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Multi_profile_safe_diagnostics_do_not_expose_secret_references()
    {
        const string userVariable = "KAFDECK_CONNECT_A_USER";
        const string passwordVariable = "KAFDECK_CONNECT_A_PASSWORD";

        var options = OptionsWithProfiles(
            new KafkaConnectProfile(
                "https://connect.example:8083",
                SecretReference.Parse($"env:{userVariable}"),
                SecretReference.Parse($"env:{passwordVariable}"),
                KafkaConnectMutationProviderProfile.ConfluentCompatibleV1,
                "default"));

        KafdeckConfigurationValidator.ValidateAndThrow(options);

        var json = JsonSerializer.Serialize(
            SafeConfigurationDiagnostics.Create(options));

        Assert.Contains(
            "\"connectConfigured\":true",
            json,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            userVariable,
            json,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            passwordVariable,
            json,
            StringComparison.Ordinal);
    }

    private static KafdeckOptions Load(
        IReadOnlyDictionary<string, string?> values)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        return KafdeckConfigurationLoader.Load(configuration);
    }

    private static KafdeckOptions OptionsWithProfiles(
        params KafkaConnectProfile[] profiles)
    {
        var cluster = new ClusterProfile(
            "prod",
            new[] { "broker.example:9092" },
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            null,
            null,
            profiles);

        return new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null),
            new[] { cluster });
    }
}
