using System.Text.Json;
using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V04ReadViewApiContractTests
{
    [Fact]
    public void V04_product_surface_is_get_only()
    {
        Assert.NotEmpty(V04ReadViewApiContract.ProductRoutes);
        Assert.All(
            V04ReadViewApiContract.ProductRoutes,
            route => Assert.Equal("GET", route.Method));

        Assert.DoesNotContain(
            V04ReadViewApiContract.ProductRoutes,
            route => route.Method is "POST" or "PUT" or "PATCH" or "DELETE");
    }

    [Fact]
    public void V04_contract_contains_all_authorized_read_surfaces()
    {
        var names = V04ReadViewApiContract.ProductRoutes
            .Select(route => route.Name)
            .ToHashSet(StringComparer.Ordinal);

        string[] required =
        [
            "v04-consumer-groups-list",
            "v04-consumer-groups-detail",
            "v04-consumer-groups-lag",
            "v04-consumer-groups-diagnostics",
            "v04-schema-subjects-list",
            "v04-schema-versions-list",
            "v04-schema-version-detail",
            "v04-schema-compatibility",
            "v04-schema-diff",
            "v04-connect-info",
            "v04-connect-connectors-list",
            "v04-connect-connectors-detail",
            "v04-ksql-info",
            "v04-ksql-metadata",
            "v04-topic-catalog-detail",
        ];

        Assert.All(required, name => Assert.Contains(name, names));
    }

    [Fact]
    public void V04_openapi_is_get_only_and_matches_registered_contract_paths()
    {
        using var document = JsonDocument.Parse(V04ReadViewApiContract.OpenApiJson);
        var paths = document.RootElement.GetProperty("paths")
            .EnumerateObject()
            .ToDictionary(item => item.Name, item => item.Value, StringComparer.Ordinal);

        Assert.Equal(V04ReadViewApiContract.ProductRoutes.Count, paths.Count);

        foreach (var route in V04ReadViewApiContract.ProductRoutes)
        {
            var normalized = route.Pattern.Replace("{version:int}", "{version}", StringComparison.Ordinal);
            Assert.True(paths.TryGetValue(normalized, out var pathItem), $"OpenAPI is missing {normalized}.");

            var operations = pathItem.EnumerateObject()
                .Select(property => property.Name)
                .ToArray();

            Assert.Contains("get", operations);
            Assert.DoesNotContain(operations, method =>
                method is "post" or "put" or "patch" or "delete");
        }
    }

    [Fact]
    public void Checked_in_v04_openapi_is_reproducible()
    {
        var root = FindRepositoryRoot();
        var checkedIn = File.ReadAllText(
            Path.Combine(root, "docs", "api", "openapi-v0.4.json"));

        Assert.Equal(
            Normalize(V04ReadViewApiContract.OpenApiJson),
            Normalize(checkedIn));
    }

    private static string Normalize(string value) =>
        value.Replace("\r\n", "\n", StringComparison.Ordinal).Trim();

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException(
            "Unable to locate Kafdeck repository root from test output directory.");
    }
}
