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
    public void Schema_developer_tooling_openapi_declares_bounded_methods_and_mock_count()
    {
        using var document = JsonDocument.Parse(KafdeckV07OpenApi.Document);
        var paths = document.RootElement.GetProperty("paths");

        var references = paths.GetProperty(
            "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/references");
        Assert.True(references.TryGetProperty("get", out var referenceGet));
        Assert.False(references.TryGetProperty("post", out _));
        Assert.Contains(
            referenceGet.GetProperty("parameters").EnumerateArray(),
            parameter =>
                parameter.GetProperty("name").GetString() == "version" &&
                parameter.GetProperty("in").GetString() == "query" &&
                parameter.GetProperty("required").GetBoolean());

        var explanation = paths.GetProperty(
            "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/compatibility/explanation");
        Assert.True(explanation.TryGetProperty("get", out _));
        Assert.False(explanation.TryGetProperty("post", out _));

        var mock = paths.GetProperty(
            "/api/v1/clusters/{clusterId}/schemas/mock");
        Assert.True(mock.TryGetProperty("post", out _));
        Assert.False(mock.TryGetProperty("get", out _));

        var count = document.RootElement
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("SchemaMockRequest")
            .GetProperty("properties")
            .GetProperty("count");

        Assert.Equal(1, count.GetProperty("minimum").GetInt32());
        Assert.Equal(10, count.GetProperty("maximum").GetInt32());

        var graph = document.RootElement
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("SchemaReferenceGraph");

        Assert.Equal(
            64,
            graph.GetProperty("properties")
                .GetProperty("nodes")
                .GetProperty("maxItems")
                .GetInt32());
        Assert.Equal(
            256,
            graph.GetProperty("properties")
                .GetProperty("edges")
                .GetProperty("maxItems")
                .GetInt32());

        var edge = document.RootElement
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("SchemaReferenceEdge");

        Assert.Equal(
            SchemaDeveloperService.MaxReferenceNameLength,
            edge.GetProperty("properties")
                .GetProperty("name")
                .GetProperty("maxLength")
                .GetInt32());
        Assert.Equal(
            2097152,
            graph.GetProperty("properties")
                .GetProperty("totalSchemaBytes")
                .GetProperty("maximum")
                .GetInt32());
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
