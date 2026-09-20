using System.Text;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class RecordMaskingAndExportTests
{
    [Fact]
    public void Structured_masking_redacts_nested_wildcards_key_and_headers()
    {
        const string secret = "4111111111111111";
        using var document = JsonDocument.Parse(
            $$"""{"customers":[{"card":"{{secret}}"},{"card":"{{secret}}"}],"visible":"ok"}""");
        var item = new RecordFilteredItem(
            new KafkaRawRecord(
                42,
                DateTimeOffset.UtcNow,
                Encoding.UTF8.GetBytes("private-key"),
                Encoding.UTF8.GetBytes(document.RootElement.GetRawText()),
                [
                    new KafkaRecordHeader("authorization", Encoding.UTF8.GetBytes("Bearer secret-token")),
                    new KafkaRecordHeader("trace", Encoding.UTF8.GetBytes("trace-1")),
                ]),
            new RecordDecodedValue(7, RecordSchemaFormat.JsonSchema, document.RootElement.Clone()));
        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "payments",
                3,
                [new RecordStructuredMaskRule("/customers/*/card")],
                [new RecordHeaderMaskRule("authorization")],
                MaskKey: true));

        var safe = new RecordMaskingService().Apply(0, item, policy);
        var serialized = JsonSerializer.Serialize(safe);

        Assert.Equal(RecordPayloadProjectionKind.Structured, safe.ValueKind);
        Assert.Null(safe.RawValue);
        Assert.True(safe.KeyRedacted);
        Assert.Equal("[REDACTED]", Encoding.UTF8.GetString(safe.Key!.Value.Span));
        Assert.DoesNotContain(secret, serialized, StringComparison.Ordinal);
        Assert.DoesNotContain("secret-token", serialized, StringComparison.Ordinal);
        Assert.Contains("[REDACTED]", safe.StructuredValue!.Value.GetRawText(), StringComparison.Ordinal);
        Assert.True(safe.Headers.Single(header => header.Name == "authorization").IsRedacted);
        Assert.False(safe.Headers.Single(header => header.Name == "trace").IsRedacted);
    }

    [Fact]
    public void Mandatory_structured_masking_fails_closed_without_decoded_value_or_required_path()
    {
        const string secret = "must-never-leak";
        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "pii",
                1,
                [new RecordStructuredMaskRule("/ssn")]));
        var service = new RecordMaskingService();

        var undecoded = new RecordFilteredItem(
            new KafkaRawRecord(
                1,
                DateTimeOffset.UtcNow,
                null,
                Encoding.UTF8.GetBytes($"{{\"ssn\":\"{secret}\"}}"),
                []),
            null);
        var undecodedSafe = service.Apply(0, undecoded, policy);

        using var wrongShapeDocument = JsonDocument.Parse($"{{\"other\":\"{secret}\"}}");
        var wrongShape = new RecordFilteredItem(
            new KafkaRawRecord(
                2,
                DateTimeOffset.UtcNow,
                null,
                Encoding.UTF8.GetBytes(wrongShapeDocument.RootElement.GetRawText()),
                []),
            new RecordDecodedValue(8, RecordSchemaFormat.JsonSchema, wrongShapeDocument.RootElement.Clone()));
        var wrongShapeSafe = service.Apply(0, wrongShape, policy);

        foreach (var safe in new[] { undecodedSafe, wrongShapeSafe })
        {
            Assert.Equal(RecordPayloadProjectionKind.FullyRedacted, safe.ValueKind);
            Assert.Null(safe.RawValue);
            Assert.Null(safe.StructuredValue);
            Assert.Contains("$payload", safe.RedactedPaths);
            Assert.DoesNotContain(secret, JsonSerializer.Serialize(safe), StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Export_requires_exact_record_export_authorization_and_contains_only_safe_values()
    {
        var page = SafePage(
            new RecordSafeProjection(
                0,
                11,
                DateTimeOffset.UtcNow,
                Encoding.UTF8.GetBytes("[REDACTED]"),
                true,
                RecordPayloadProjectionKind.FullyRedacted,
                null,
                null,
                [new RecordSafeHeader("authorization", Encoding.UTF8.GetBytes("[REDACTED]"), true)],
                "pii",
                1,
                ["$payload"]));
        var service = new RecordExportService();

        await using var denied = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            service.ExportAsync(
                page,
                new RecordExportRequest(RecordExportFormat.Json),
                ExportRequest(),
                AuthorizationDecision.Denied(AuthorizationDecisionReason.ActionDenied),
                denied));

        await using var wrongAction = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            service.ExportAsync(
                page,
                new RecordExportRequest(RecordExportFormat.Json),
                new AuthorizationRequest(AuthorizationAction.RecordRead, "cluster-a", "orders"),
                AuthorizationDecision.Allowed(["reader"]),
                wrongAction));

        await using var wrongTarget = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            service.ExportAsync(
                page,
                new RecordExportRequest(RecordExportFormat.Json),
                new AuthorizationRequest(AuthorizationAction.RecordExport, "cluster-b", "orders"),
                AuthorizationDecision.Allowed(["record-exporter"]),
                wrongTarget));

        await using var allowed = new MemoryStream();
        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(RecordExportFormat.Json),
            ExportRequest(),
            AuthorizationDecision.Allowed(["record-exporter"]),
            allowed);
        var output = Encoding.UTF8.GetString(allowed.ToArray());

        Assert.Equal(1, summary.RowCount);
        Assert.Equal(RecordExportBudgetOutcome.Complete, summary.Outcome);
        Assert.Contains("FullyRedacted", output, StringComparison.Ordinal);
        Assert.Contains("W1JFREFDVEVEXQ==", output, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(RecordExportFormat.Json)]
    [InlineData(RecordExportFormat.Ndjson)]
    [InlineData(RecordExportFormat.Csv)]
    public async Task Export_formats_enforce_row_budget(RecordExportFormat format)
    {
        var page = SafePage(
            SafeProjection(1, "one"),
            SafeProjection(2, "two"),
            SafeProjection(3, "three"));
        var service = new RecordExportService();
        await using var destination = new MemoryStream();

        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(
                format,
                new RecordExportBudget(maxRows: 2, maxBytes: 4096, maxDuration: TimeSpan.FromSeconds(1))),
            ExportRequest(),
            AuthorizationDecision.Allowed(["export"]),
            destination);

        Assert.Equal(2, summary.RowCount);
        Assert.Equal(RecordExportBudgetOutcome.RowLimit, summary.Outcome);
        Assert.InRange(summary.ByteCount, 1, 4096);
        Assert.Equal(summary.ByteCount, destination.Length);
    }

    [Fact]
    public async Task Json_export_stops_before_byte_budget_and_remains_valid()
    {
        var page = SafePage(SafeProjection(1, new string('x', 500)));
        var service = new RecordExportService();
        await using var destination = new MemoryStream();

        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(
                RecordExportFormat.Json,
                new RecordExportBudget(maxRows: 10, maxBytes: 256, maxDuration: TimeSpan.FromSeconds(1))),
            ExportRequest(),
            AuthorizationDecision.Allowed(["export"]),
            destination);

        Assert.Equal(0, summary.RowCount);
        Assert.Equal(RecordExportBudgetOutcome.ByteLimit, summary.Outcome);
        Assert.InRange(summary.ByteCount, 2, 256);
        using var parsed = JsonDocument.Parse(destination.ToArray());
        Assert.Equal(0, parsed.RootElement.GetArrayLength());
    }

    [Fact]
    public void Policy_validation_and_audit_metadata_are_bounded_and_payload_free()
    {
        Assert.Throws<ArgumentException>(() =>
            RecordMaskingPolicyCompiler.Compile(
                new RecordMaskingPolicyDefinition(
                    "duplicate",
                    1,
                    [
                        new RecordStructuredMaskRule("/customer/ssn"),
                        new RecordStructuredMaskRule("/customer/ssn"),
                    ])));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RecordStructuredMaskRule("/" + string.Join('/', Enumerable.Repeat("x", 17))));

        var page = SafePage(SafeProjection(7, "safe"));
        var audit = new RecordExportService().BuildExportAudit(
            page,
            new RecordExportSummary(RecordExportFormat.Ndjson, 1, 120, RecordExportBudgetOutcome.Complete));

        Assert.Equal("record.export", audit.Operation);
        Assert.Equal("orders", audit.TopicName);
        Assert.DoesNotContain("RawValue", JsonSerializer.Serialize(audit), StringComparison.Ordinal);
    }

    private static AuthorizationRequest ExportRequest() =>
        new(AuthorizationAction.RecordExport, "cluster-a", "orders");

    private static RecordSafeProjection SafeProjection(long offset, string value) =>
        new(
            0,
            offset,
            DateTimeOffset.UtcNow,
            Encoding.UTF8.GetBytes("safe-key"),
            false,
            RecordPayloadProjectionKind.Raw,
            Encoding.UTF8.GetBytes(value),
            null,
            [],
            "none",
            1,
            []);

    private static RecordSafePage SafePage(params RecordSafeProjection[] records) =>
        new(
            "cluster-a",
            "orders",
            0,
            Array.AsReadOnly(records),
            0,
            100,
            null,
            null,
            RecordBudgetOutcome.Complete,
            RecordFilterBudgetOutcome.Complete,
            [],
            records.Length == 0 ? "none" : records[0].PolicyId,
            records.Length == 0 ? 1 : records[0].PolicyVersion);
}
