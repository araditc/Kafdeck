using System.Text.Json;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W59StreamsTelemetryConfigurationTests
{
    [Fact]
    public void Loader_parses_explicit_streams_telemetry_profile()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Clusters:0:Id"] = "prod",
            ["Kafdeck:Clusters:0:BootstrapServers:0"] = "broker.example:9092",
            ["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Url"] = "https://streams.example",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Username"] = "env:KAFDECK_STREAMS_USER",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Password"] = "env:KAFDECK_STREAMS_PASSWORD",
            ["Kafdeck:Clusters:0:StreamsTelemetry:ProviderProfile"] = "KafdeckTelemetryV1",
        });

        var cluster = Assert.Single(options.Clusters);
        Assert.NotNull(cluster.StreamsTelemetry);
        Assert.Equal(
            "https://streams.example",
            cluster.StreamsTelemetry!.Url);
        Assert.Equal(
            StreamsTelemetryProviderProfile.KafdeckTelemetryV1,
            cluster.StreamsTelemetry.ProviderProfile);

        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Remote_streams_basic_auth_requires_https()
    {
        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Clusters:0:Id"] = "prod",
            ["Kafdeck:Clusters:0:BootstrapServers:0"] = "broker.example:9092",
            ["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Url"] = "http://streams.example",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Username"] = "env:KAFDECK_STREAMS_USER",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Password"] = "env:KAFDECK_STREAMS_PASSWORD",
        });

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains(
            "Streams telemetry basic authentication requires HTTPS",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Streams_safe_diagnostics_never_expose_secret_references()
    {
        const string userVariable = "KAFDECK_STREAMS_USER";
        const string passwordVariable = "KAFDECK_STREAMS_PASSWORD";

        var options = Load(new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Clusters:0:Id"] = "prod",
            ["Kafdeck:Clusters:0:BootstrapServers:0"] = "broker.example:9092",
            ["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Url"] = "https://streams.example",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Username"] = $"env:{userVariable}",
            ["Kafdeck:Clusters:0:StreamsTelemetry:Password"] = $"env:{passwordVariable}",
        });

        KafdeckConfigurationValidator.ValidateAndThrow(options);

        var json = JsonSerializer.Serialize(
            SafeConfigurationDiagnostics.Create(options));

        Assert.Contains(
            "\"streamsTelemetryConfigured\":true",
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
}
