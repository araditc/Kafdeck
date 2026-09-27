using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W55ConnectAutoRestartConfigurationTests
{
    [Fact]
    public void Auto_restart_configuration_is_disabled_by_default()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] =
                "http://127.0.0.1:8080",
            ["Kafdeck:Administration:Mutations:Enabled"] =
                "false",
        });

        Assert.NotNull(options.Administration);
        Assert.Null(
            options.Administration!.ConnectAutoRestart);

        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Standalone_enabled_auto_restart_without_mutation_persistence_fails_closed()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] =
                "https://127.0.0.1:8443",
            ["Kafdeck:Deployment:AccessMode"] = "Oidc",
            ["Kafdeck:Deployment:Oidc:Issuer"] =
                "https://idp.example",
            ["Kafdeck:Deployment:Oidc:ClientId"] =
                "kafdeck",
            ["Kafdeck:Deployment:Oidc:Scopes:0"] =
                "openid",
            ["Kafdeck:Administration:ConnectAutoRestart:Enabled"] =
                "true",
        });

        Assert.NotNull(options.Administration);
        Assert.NotNull(
            options.Administration!.ConnectAutoRestart);
        Assert.True(
            options.Administration.ConnectAutoRestart!.Enabled);

        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () =>
                    KafdeckConfigurationValidator
                        .ValidateAndThrow(options));

        Assert.Contains(
            "requires enabled durable mutation persistence",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Valid_bounded_auto_restart_policy_loads_and_validates()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            "kafdeck-w55.db");

        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] =
                "https://127.0.0.1:8443",
            ["Kafdeck:Deployment:AccessMode"] = "Oidc",
            ["Kafdeck:Deployment:Oidc:Issuer"] =
                "https://idp.example",
            ["Kafdeck:Deployment:Oidc:ClientId"] =
                "kafdeck",
            ["Kafdeck:Deployment:Oidc:Scopes:0"] =
                "openid",
            ["Kafdeck:Administration:Mutations:Enabled"] =
                "true",
            ["Kafdeck:Administration:Mutations:MaterialDigestKey"] =
                "env:KAFDECK_DIGEST_KEY",
            ["Kafdeck:Administration:Mutations:Persistence:Provider"] =
                "Sqlite",
            ["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] =
                "Standalone",
            ["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"] =
                path,
            ["Kafdeck:Administration:ConnectAutoRestart:Enabled"] =
                "true",
            ["Kafdeck:Administration:ConnectAutoRestart:PolicyVersion"] =
                "w55-policy-1",
            ["Kafdeck:Administration:ConnectAutoRestart:MaxAttempts"] =
                "10",
            ["Kafdeck:Administration:ConnectAutoRestart:InitialBackoffSeconds"] =
                "5",
            ["Kafdeck:Administration:ConnectAutoRestart:MaxBackoffSeconds"] =
                "1800",
            ["Kafdeck:Administration:ConnectAutoRestart:ActivationLifetimeSeconds"] =
                "86400",
            ["Kafdeck:Administration:ConnectAutoRestart:MaxActivePoliciesPerProfile"] =
                "100",
            ["Kafdeck:Administration:ConnectAutoRestart:JitterBasisPoints"] =
                "5000",
        });

        KafdeckConfigurationValidator.ValidateAndThrow(options);

        var policy =
            options.Administration!.ConnectAutoRestart!;
        Assert.True(policy.Enabled);
        Assert.Equal(10, policy.MaxAttempts);
        Assert.Equal(5, policy.InitialBackoffSeconds);
        Assert.Equal(1800, policy.MaxBackoffSeconds);
        Assert.Equal(86400, policy.ActivationLifetimeSeconds);
        Assert.Equal(100, policy.MaxActivePoliciesPerProfile);
        Assert.Equal(5000, policy.JitterBasisPoints);
    }

    [Theory]
    [InlineData(
        "Kafdeck:Administration:ConnectAutoRestart:MaxAttempts",
        "11")]
    [InlineData(
        "Kafdeck:Administration:ConnectAutoRestart:InitialBackoffSeconds",
        "4")]
    [InlineData(
        "Kafdeck:Administration:ConnectAutoRestart:MaxBackoffSeconds",
        "1801")]
    [InlineData(
        "Kafdeck:Administration:ConnectAutoRestart:ActivationLifetimeSeconds",
        "86401")]
    [InlineData(
        "Kafdeck:Administration:ConnectAutoRestart:MaxActivePoliciesPerProfile",
        "101")]
    [InlineData(
        "Kafdeck:Administration:ConnectAutoRestart:JitterBasisPoints",
        "5001")]
    public void Auto_restart_cap_plus_one_values_fail_validation(
        string key,
        string value)
    {
        var values = BaseDisabledConfiguration();
        values[
            "Kafdeck:Administration:ConnectAutoRestart:Enabled"] =
            "false";
        values[key] = value;

        var options = Load(values);

        Assert.Throws<KafdeckConfigurationException>(
            () =>
                KafdeckConfigurationValidator
                    .ValidateAndThrow(options));
    }

    [Fact]
    public void Provider_identity_fingerprint_detects_profile_and_secret_reference_drift_without_exposing_locator()
    {
        var original = Cluster(
            "https://connect.example:8083",
            "env:CONNECT_USER_A",
            "env:CONNECT_PASS_A");
        var same = Cluster(
            "https://connect.example:8083/",
            "env:CONNECT_USER_A",
            "env:CONNECT_PASS_A");
        var changedEndpoint = Cluster(
            "https://connect2.example:8083",
            "env:CONNECT_USER_A",
            "env:CONNECT_PASS_A");
        var changedSecret = Cluster(
            "https://connect.example:8083",
            "env:CONNECT_USER_B",
            "env:CONNECT_PASS_A");

        var first =
            KafkaConnectProfileIdentityFingerprint.Compute(
                original,
                "default");
        var equivalent =
            KafkaConnectProfileIdentityFingerprint.Compute(
                same,
                "default");
        var endpointDrift =
            KafkaConnectProfileIdentityFingerprint.Compute(
                changedEndpoint,
                "default");
        var secretDrift =
            KafkaConnectProfileIdentityFingerprint.Compute(
                changedSecret,
                "default");

        Assert.Equal(64, first.Length);
        Assert.Equal(first, equivalent);
        Assert.NotEqual(first, endpointDrift);
        Assert.NotEqual(first, secretDrift);
        Assert.DoesNotContain(
            "CONNECT_USER_A",
            first,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "CONNECT_PASS_A",
            first,
            StringComparison.Ordinal);
    }

    private static Dictionary<string, string?>
        BaseDisabledConfiguration() =>
        new()
        {
            ["Kafdeck:Deployment:ListenUrl"] =
                "http://127.0.0.1:8080",
            ["Kafdeck:Administration:Mutations:Enabled"] =
                "false",
            ["Kafdeck:Administration:ConnectAutoRestart:PolicyVersion"] =
                "w55-test",
        };

    private static KafdeckOptions Load(
        IReadOnlyDictionary<string, string?> values) =>
        KafdeckConfigurationLoader.Load(
            new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build());

    private static ClusterProfile Cluster(
        string url,
        string username,
        string password) =>
        new(
            "prod",
            new[] { "broker.example:9092" },
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            new KafkaConnectProfile(
                url,
                SecretReference.Parse(username),
                SecretReference.Parse(password),
                KafkaConnectMutationProviderProfile
                    .ConfluentCompatibleV1,
                "default"));
}
