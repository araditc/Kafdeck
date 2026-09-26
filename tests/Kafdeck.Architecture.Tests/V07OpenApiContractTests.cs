using System.Text.Json;
using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07OpenApiContractTests
{
    [Fact]
    public void Schema_registry_capability_openapi_is_typed_and_read_only()
    {
        using var document = JsonDocument.Parse(KafdeckV07OpenApi.Document);
        var root = document.RootElement;

        Assert.Equal(
            "3.1.0",
            root.GetProperty("openapi").GetString());
        Assert.Equal(
            "0.7.0",
            root.GetProperty("info")
                .GetProperty("version")
                .GetString());

        var path = root
            .GetProperty("paths")
            .GetProperty(
                "/api/v1/clusters/{clusterId}/schemas/capabilities");

        Assert.True(path.TryGetProperty("get", out _));
        Assert.False(path.TryGetProperty("post", out _));
        Assert.False(path.TryGetProperty("put", out _));
        Assert.False(path.TryGetProperty("patch", out _));
        Assert.False(path.TryGetProperty("delete", out _));

        var providerProfiles = root
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("SchemaRegistryCapabilitiesData")
            .GetProperty("properties")
            .GetProperty("providerProfile")
            .GetProperty("enum")
            .EnumerateArray()
            .Select(item => item.GetString())
            .ToArray();

        Assert.Equal(
            new[]
            {
                "Unconfigured",
                "ConfluentCompatibleV1",
                "KarapaceCompatibleV1",
                "ApicurioV3",
            },
            providerProfiles);
    }

    [Fact]
    public void Checked_in_v07_openapi_is_reproducible()
    {
        var root = FindRepositoryRoot();
        var checkedIn = File.ReadAllText(
            Path.Combine(
                root,
                "docs",
                "api",
                "openapi-v0.7.json"));

        Assert.Equal(
            Normalize(KafdeckV07OpenApi.Document),
            Normalize(checkedIn));
    }

    [Fact]
    public void V07_openapi_exposes_no_generic_registry_execution_surface()
    {
        var lower =
            KafdeckV07OpenApi.Document.ToLowerInvariant();

        Assert.DoesNotContain(
            "/proxy",
            lower,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "raw-protocol",
            lower,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "shell",
            lower,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "providerurl",
            lower,
            StringComparison.Ordinal);
    }

    private static string Normalize(string value) =>
        value
            .Replace(
                "\r\n",
                "\n",
                StringComparison.Ordinal)
            .Trim();

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current =
            new(AppContext.BaseDirectory);

        while (current is not null)
        {
            if (File.Exists(
                    Path.Combine(
                        current.FullName,
                        "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException(
            "Unable to locate Kafdeck repository root.");
    }
}
