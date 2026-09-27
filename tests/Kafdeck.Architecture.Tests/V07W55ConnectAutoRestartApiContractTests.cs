using System.Text.Json;
using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W55ConnectAutoRestartApiContractTests
{
    [Fact]
    public void Auto_restart_openapi_exposes_only_typed_status_preview_and_apply_routes()
    {
        using var document = JsonDocument.Parse(KafdeckV07OpenApi.Document);
        var paths = document.RootElement.GetProperty("paths");

        var policy = paths.GetProperty(
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/restart-policy");
        Assert.True(policy.TryGetProperty("get", out _));
        Assert.True(policy.TryGetProperty("put", out _));
        Assert.False(policy.TryGetProperty("post", out _));
        Assert.False(policy.TryGetProperty("delete", out _));

        var preview = paths.GetProperty(
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/restart-policy/preview");
        Assert.True(preview.TryGetProperty("put", out _));
        Assert.False(preview.TryGetProperty("post", out _));
        Assert.False(preview.TryGetProperty("delete", out _));
    }

    [Fact]
    public void Auto_restart_openapi_publishes_server_owned_hard_caps()
    {
        using var document = JsonDocument.Parse(KafdeckV07OpenApi.Document);
        var schema = document.RootElement
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("ConnectAutoRestartPolicyPreviewRequest")
            .GetProperty("properties");

        Assert.Equal(10, schema.GetProperty("maxAttempts").GetProperty("maximum").GetInt32());
        Assert.Equal(5, schema.GetProperty("initialBackoffSeconds").GetProperty("minimum").GetInt32());
        Assert.Equal(1800, schema.GetProperty("maxBackoffSeconds").GetProperty("maximum").GetInt32());
        Assert.Equal(86400, schema.GetProperty("activationLifetimeSeconds").GetProperty("maximum").GetInt32());
        Assert.Equal(100, schema.GetProperty("maxActivePoliciesPerProfile").GetProperty("maximum").GetInt32());
        Assert.Equal(5000, schema.GetProperty("jitterBasisPoints").GetProperty("maximum").GetInt32());
    }

    [Fact]
    public void Auto_restart_status_contract_exposes_ambiguity_and_attempt_state()
    {
        using var document = JsonDocument.Parse(KafdeckV07OpenApi.Document);
        var required = document.RootElement
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("ConnectAutoRestartPolicyStatus")
            .GetProperty("required")
            .EnumerateArray()
            .Select(item => item.GetString())
            .ToHashSet(StringComparer.Ordinal);

        Assert.Contains("attemptsUsed", required);
        Assert.Contains("maxAttempts", required);
        Assert.Contains("hasUnresolvedDispatch", required);

        var circuit = document.RootElement
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("ConnectAutoRestartPolicyStatus")
            .GetProperty("properties")
            .GetProperty("circuitState")
            .GetProperty("enum")
            .EnumerateArray()
            .Where(item => item.ValueKind == JsonValueKind.String)
            .Select(item => item.GetString())
            .ToArray();

        Assert.Contains("Ambiguous", circuit);
        Assert.Contains("Exhausted", circuit);
        Assert.Contains("Blocked", circuit);
    }
}
