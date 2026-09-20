using System.Text;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class RecordProjectionAndLegacyExportTests
{
    [Fact]
    public void Safe_projection_stops_at_aggregate_projected_byte_budget_and_preserves_resume_anchor()
    {
        var request = new RecordReadRequest(
            "cluster-a",
            "orders",
            0,
            RecordAnchor.Earliest(),
            RecordReadDirection.Forward,
            new RecordOperationBudget(10, 4096, 1, TimeSpan.FromSeconds(1), 10));
        var raw = new KafkaRawRecord(42, DateTimeOffset.UtcNow, Encoding.UTF8.GetBytes("key"), Encoding.UTF8.GetBytes("value"), []);
        var filtered = new RecordFilterPage(
            [new RecordFilteredItem(raw, null)],
            0,
            100,
            RecordAnchor.AtOffset(43),
            null,
            RecordBudgetOutcome.Complete,
            RecordFilterBudgetOutcome.Complete,
            1,
            8,
            []);
        var policy = RecordMaskingPolicyCompiler.Compile(new RecordMaskingPolicyDefinition("none", 1));

        var safe = new RecordMaskingService().Apply(request, filtered, policy);

        Assert.Empty(safe.Records);
        Assert.Equal(RecordBudgetOutcome.ProjectedByteLimit, safe.ReadBudgetOutcome);
        Assert.True(safe.NextAnchor.HasValue);
        Assert.Equal(42, safe.NextAnchor.Value.Offset);
    }

    [Fact]
    public async Task Legacy_deployment_export_requires_explicit_already_authorized_boundary_flag()
    {
        var service = new RecordExportService();
        var evaluator = new AuthorizationPolicyEvaluator(
            AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition([], [], [])));
        var page = SafePage();

        await using var denied = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            service.ExportAsync(
                page,
                new RecordExportRequest(RecordExportFormat.Ndjson),
                evaluator,
                identity: null,
                destination: denied,
                cancellationToken: CancellationToken.None,
                legacyDeploymentAuthorized: false));

        await using var allowed = new MemoryStream();
        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(RecordExportFormat.Ndjson),
            evaluator,
            identity: null,
            destination: allowed,
            cancellationToken: CancellationToken.None,
            legacyDeploymentAuthorized: true);

        Assert.Equal(RecordExportBudgetOutcome.Complete, summary.Outcome);
        Assert.Equal(1, summary.RowCount);
        Assert.True(allowed.Length > 0);
    }

    private static RecordSafePage SafePage()
    {
        var record = new RecordSafeProjection(
            0, 1, DateTimeOffset.UtcNow, Encoding.UTF8.GetBytes("safe-key"), false,
            RecordPayloadProjectionKind.Raw, Encoding.UTF8.GetBytes("safe-value"), null, [], "none", 1, []);
        return new RecordSafePage(
            "cluster-a", "orders", 0, [record], 0, 100, null, null,
            RecordBudgetOutcome.Complete, RecordFilterBudgetOutcome.Complete, [], "none", 1);
    }
}
