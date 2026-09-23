using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05MutationApiContractTests
{
    [Fact]
    public void Mutation_status_projection_is_safe_and_excludes_canonical_or_execution_material_metadata()
    {
        var now = DateTimeOffset.UtcNow;
        var intent = new MutationIntentDescriptor(
            MutationOperationKind.TopicDelete,
            "prod",
            "{\"operation\":\"topic-delete\",\"topic\":\"payments.events\"}",
            new[] { "topic/payments.events" },
            Preconditions:
                new[] { new MutationPrecondition("topic.metadata", "sha256:abc") },
            MaterialDigests:
                new[] { new MutationMaterialDigest("payload", new string('a', 64)) },
            AuthorizationTargets:
                new[]
                {
                    new MutationAuthorizationTarget(
                        Kafdeck.Core.Security.AuthorizationAction.TopicDelete,
                        "prod",
                        "payments.events"),
                });

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            intent,
            MutationRiskClassifier.Classify(
                new MutationRiskInput(MutationOperationKind.TopicDelete)),
            "v0.5-w39",
            now.AddMinutes(5),
            now,
            "status-contract");

        var projection = MutationStatusData.From(operation.Snapshot);
        var json = JsonSerializer.Serialize(
            projection,
            new JsonSerializerOptions(JsonSerializerDefaults.Web));

        Assert.Contains("operationId", json, StringComparison.Ordinal);
        Assert.Contains("previewHash", json, StringComparison.Ordinal);
        Assert.Contains("requiresIndependentApproval", json, StringComparison.Ordinal);
        Assert.Contains("requiresExecutionMaterial", json, StringComparison.Ordinal);
        Assert.True(projection.RequiresExecutionMaterial);

        Assert.DoesNotContain("canonicalIntent", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("materialDigests", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("idempotency", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("authorizationTargets", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("payments.events\\\"}", json, StringComparison.Ordinal);
    }
}
