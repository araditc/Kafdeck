using System.Text.Json;
using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67BearerOpenApiTests
{
    [Fact]
    public void First_family_contract_is_openapi31_typed_bearer_and_preserves_durable_states()
    {
        using var json = JsonDocument.Parse(KafdeckV08BearerOpenApi.Document);
        var root = json.RootElement;
        Assert.Equal("3.1.0", root.GetProperty("openapi").GetString());
        var paths = root.GetProperty("paths");
        Assert.Equal(7, paths.EnumerateObject().Count());
        Assert.All(paths.EnumerateObject(), path => Assert.StartsWith("/api/v1/automation/", path.Name, StringComparison.Ordinal));
        Assert.Equal("bearer", root.GetProperty("components").GetProperty("securitySchemes").GetProperty("bearerAuth").GetProperty("scheme").GetString());
        var schemas = root.GetProperty("components").GetProperty("schemas");
        Assert.False(schemas.GetProperty("TopicCreateRequest").GetProperty("additionalProperties").GetBoolean());
        Assert.False(schemas.GetProperty("ConfirmRequest").GetProperty("additionalProperties").GetBoolean());
        var states = schemas.GetProperty("MutationStatus").GetProperty("properties").GetProperty("state").GetProperty("enum").EnumerateArray().Select(v => v.GetString()).ToArray();
        Assert.Contains("awaitingApproval", states); Assert.Contains("executionUnknown", states); Assert.Contains("stalePreview", states);
        Assert.True(paths.TryGetProperty("/api/v1/automation/mutations/{operationId}/approve", out _));
        Assert.False(schemas.GetProperty("ApprovalRequest").GetProperty("additionalProperties").GetBoolean());
        Assert.DoesNotContain("requesterPrincipalId", schemas.GetProperty("TopicCreateRequest").GetRawText(), StringComparison.Ordinal);
    }

    [Fact]
    public void Actual_new_request_dtos_reject_unmapped_actor_or_provider_fields()
    {
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var legitimate = "{\"partitionCount\":1,\"replicationFactor\":1,\"configurations\":{}}";
        Assert.NotNull(JsonSerializer.Deserialize<BearerTopicCreateRequest>(legitimate, options));
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<BearerTopicCreateRequest>(legitimate[..^1] + ",\"requesterPrincipalId\":\"spoof\"}", options));
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<BearerMutationConfirmRequest>("{\"previewHash\":\"hash\",\"providerCommand\":\"unadmitted\"}", options));
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<BearerMutationApprovalRequest>("{\"previewHash\":\"hash\",\"approver\":\"spoof\"}", options));
    }
}
