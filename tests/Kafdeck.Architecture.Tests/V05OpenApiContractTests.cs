using System.Text.Json;
using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05OpenApiContractTests
{
    [Fact]
    public void W39_openapi_document_is_valid_json_and_covers_all_admitted_mutation_families()
    {
        using var document = JsonDocument.Parse(KafdeckV05OpenApi.Document);
        var root = document.RootElement;

        Assert.Equal("3.1.0", root.GetProperty("openapi").GetString());
        Assert.Equal("0.5.0", root.GetProperty("info").GetProperty("version").GetString());

        var paths = root.GetProperty("paths");
        foreach (var required in new[]
                 {
                     "/api/v1/mutations/{operationId}",
                     "/api/v1/mutations/{operationId}/confirm",
                     "/api/v1/mutations/{operationId}/approve",
                     "/api/v1/mutations/{operationId}/reject",
                     "/api/v1/mutations/{operationId}/cancel",
                     "/api/v1/mutations/approvals",
                     "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/create/preview",
                     "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/produce/preview",
                     "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/mutations/offsets/preview",
                     "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/register/preview",
                     "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/create/preview",
                     "/api/v1/clusters/{clusterId}/records/purge/preview",
                 })
        {
            Assert.True(paths.TryGetProperty(required, out _), $"Missing OpenAPI path: {required}");
        }
    }

    [Fact]
    public void W39_openapi_contract_documents_governance_headers_without_generic_execution_paths()
    {
        using var document = JsonDocument.Parse(KafdeckV05OpenApi.Document);
        var parameters = document.RootElement
            .GetProperty("components")
            .GetProperty("parameters");

        Assert.Equal(
            "Idempotency-Key",
            parameters.GetProperty("IdempotencyKey").GetProperty("name").GetString());
        Assert.Equal(
            "X-Kafdeck-CSRF",
            parameters.GetProperty("CsrfHeader").GetProperty("name").GetString());

        var pathNames = document.RootElement
            .GetProperty("paths")
            .EnumerateObject()
            .Select(item => item.Name)
            .ToArray();

        Assert.DoesNotContain(pathNames, path =>
            path.Contains("command", StringComparison.OrdinalIgnoreCase) ||
            path.Contains("proxy", StringComparison.OrdinalIgnoreCase) ||
            path.Contains("adminclient", StringComparison.OrdinalIgnoreCase) ||
            path.Contains("shell", StringComparison.OrdinalIgnoreCase) ||
            path.Contains("sql", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void W39_openapi_status_schema_does_not_publish_durable_raw_material_or_internal_authorization_state()
    {
        using var document = JsonDocument.Parse(KafdeckV05OpenApi.Document);
        var properties = document.RootElement
            .GetProperty("components")
            .GetProperty("schemas")
            .GetProperty("MutationStatus")
            .GetProperty("properties");

        foreach (var forbidden in new[]
                 {
                     "canonicalIntent",
                     "materialDigests",
                     "idempotencyKeyHash",
                     "authorizationTargets",
                     "requesterPrincipalId",
                 })
        {
            Assert.False(properties.TryGetProperty(forbidden, out _));
        }
    }
}
