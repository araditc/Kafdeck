using System.Text.Json;
using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06OpenApiContractTests
{
    [Fact]
    public void Fleet_openapi_is_typed_read_only_and_reports_capability_states()
    {
        using var document = JsonDocument.Parse(KafdeckV06OpenApi.Document);
        var root = document.RootElement;

        Assert.Equal("3.1.0", root.GetProperty("openapi").GetString());
        Assert.Equal("0.6.0", root.GetProperty("info").GetProperty("version").GetString());

        var path = root
            .GetProperty("paths")
            .GetProperty("/api/v1/fleet/capabilities");

        Assert.True(path.TryGetProperty("get", out _));
        Assert.False(path.TryGetProperty("post", out _));
        Assert.False(path.TryGetProperty("put", out _));
        Assert.False(path.TryGetProperty("patch", out _));
        Assert.False(path.TryGetProperty("delete", out _));

        var state = root
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("FleetCapabilityStatus")
            .GetProperty("properties")
            .GetProperty("state")
            .GetProperty("enum")
            .EnumerateArray()
            .Select(item => item.GetString())
            .ToArray();

        Assert.Equal(
            new[] { "supported", "unsupported", "blocked", "unconfigured", "unknown" },
            state);
    }

    [Fact]
    public void Checked_in_v06_openapi_is_reproducible()
    {
        var root = FindRepositoryRoot();
        var checkedIn = File.ReadAllText(
            Path.Combine(root, "docs", "api", "openapi-v0.6.json"));

        Assert.Equal(
            Normalize(KafdeckV06OpenApi.Document),
            Normalize(checkedIn));
    }

    [Fact]
    public void Fleet_openapi_exposes_no_generic_provider_execution_surface()
    {
        var lower = KafdeckV06OpenApi.Document.ToLowerInvariant();

        Assert.DoesNotContain("adminclient", lower, StringComparison.Ordinal);
        Assert.DoesNotContain("shell", lower, StringComparison.Ordinal);
        Assert.DoesNotContain("raw-protocol", lower, StringComparison.Ordinal);
        Assert.DoesNotContain("proxy", lower, StringComparison.Ordinal);
    }

    private static string Normalize(string value) =>
        value.Replace("\r\n", "\n", StringComparison.Ordinal).Trim();

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "Kafdeck.slnx")))
                return current.FullName;
            current = current.Parent;
        }

        throw new DirectoryNotFoundException(
            "Unable to locate Kafdeck repository root.");
    }
}
