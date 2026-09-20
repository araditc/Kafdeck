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
            $$"""{"customers":[{"name":"alice","card":"{{secret}}"},{"name":"bob","card":"{{secret}}"}],"visible":"ok"}""");

        var raw = new KafkaRawRecord(
            42,
            DateTimeOffset.UtcNow,
            Encoding.UTF8.GetBytes("private-key"),
            Encoding.UTF8.GetBytes(document.RootElement.GetRawText()),
            [
                new KafkaRecordHeader("authorization", Encoding.UTF8.GetBytes("Bearer secret-token")),
                new KafkaRecordHeader("trace", Encoding.UTF8.GetBytes("trace-1")),
            ]);
        var decoded = new RecordDecodedValue(7, RecordSchemaFormat.JsonSchema, document.RootElement.Clone());
        var item = new RecordFilteredItem(raw, decoded);
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
        Assert.Contains("/customers/*/card", safe.RedactedPaths);
        Assert.True(safe.Headers.Single(header => header.Name == "authorization").IsRedacted);
        Assert.False(safe.Headers.Single(header => header.Name == "trace").IsRedacted);
    }

    [Fact]
    public void Mandatory_structured_masking_fails_closed_without_decoded_value()
    {
        const string secret = "must-never-leak";
        var item = new RecordFilteredItem(
            new KafkaRawRecord(
                1,
                DateTimeOffset.UtcNow,
                null,
                Encoding.UTF8.GetBytes($"{{\"ssn\":\"{secret}\"}}"),
                []),
            null);
        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "pii",
                1,
                [new RecordStructuredMaskRule("/ssn")])) ;

        var safe = new RecordMaskingService().Apply(0, item, policy);
        var serialized = JsonSerializer.Serialize(safe);

        Assert.Equal(RecordPayloadProjectionKind.FullyRedacted, safe.ValueKind);
        Assert.Null(safe.RawValue);
        Assert.Null(safe.StructuredValue);
        Assert.Contains("$payload", safe.RedactedPaths);
        Assert.DoesNotContain(secret, serialized, StringComparison.Ordinal);
    }

    [Fact]
    public void Masking_policy_rejects_duplicate_or_oversized_rules()
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
    }

    [Fact]
    public async Task Export_requires_explicit_authorization_and_uses_safe_projection_only()
    {
        const string clearSecret = "clear-secret";
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

        await using var deniedStream = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            service.ExportAsync(
                page,
                new RecordExportRequest(RecordExportFormat.Json),
                AuthorizationDecision.Denied(AuthorizationDecisionReason.ActionDenied),
                deniedStream));

        await using var allowedStream = new MemoryStream();
        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(RecordExportFormat.Json),
            AuthorizationDecision.Allowed(["record-exporter"]),
            allowedStream);
        var output = Encoding.UTF8.GetString(allowedStream.ToArray());

        Assert.Equal(1, summary.RowCount);
        Assert.Equal(RecordExportBudgetOutcome.Complete, summary.Outcome);
        Assert.Contains("FullyRedacted", output, StringComparison.Ordinal);
        Assert.DoesNotContain(clearSecret, output, StringComparison.Ordinal);
        Assert.DoesNotContain("authorization", output, StringComparison.OrdinalIgnoreCase | StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(RecordExportFormat.Json)]
    [InlineData(RecordExportFormat.Ndjson)]
    [InlineData(RecordExportFormat.Csv)]
    public async Task Export_formats_are_bounded_and_never_receive_raw_record_objects(RecordExportFormat format)
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
            AuthorizationDecision.Allowed(["export"]),
            destination);

        Assert.Equal(2, summary.RowCount);
        Assert.Equal(RecordExportBudgetOutcome.RowLimit, summary.Outcome);
        Assert.InRange(summary.ByteCount, 1, 4096);
        Assert.Equal(summary.ByteCount, destination.Length);
    }

    [Fact]
    public async Task Export_stops_before_exceeding_byte_budget_and_json_remains_valid()
    {
        var page = SafePage(SafeProjection(1, new string('x', 500)));
        var service = new RecordExportService();
        await using var destination = new MemoryStream();

        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(
                RecordExportFormat.Json,
                new RecordExportBudget(maxRows: 10, maxBytes: 256, maxDuration: TimeSpan.FromSeconds(1))),
            AuthorizationDecision.Allowed(["export"]),
            destination);

        Assert.Equal(0, summary.RowCount);
        Assert.Equal(RecordExportBudgetOutcome.ByteLimit, summary.Outcome);
        Assert.InRange(summary.ByteCount, 2, 256);
        using var parsed = JsonDocument.Parse(destination.ToArray());
        Assert.Equal(JsonValueKind.Array, parsed.RootElement.ValueKind);
        Assert.Equal(0, parsed.RootElement.GetArrayLength());
    }

    [Fact]
    public void Audit_metadata_contains_scope_and_counts_but_no_payload_fields()
    {
        var page = SafePage(SafeProjection(7, "safe"));
        var masking = new RecordMaskingService();
        var export = new RecordExportService();

        var readAudit = masking.BuildReadAudit(page);
        var exportAudit = export.BuildExportAudit(
            page,
            new RecordExportSummary(RecordExportFormat.Ndjson, 1, 120, RecordExportBudgetOutcome.Complete));

        Assert.Equal("record.read", readAudit.Operation);
        Assert.Equal("record.export", exportAudit.Operation);
        Assert.Equal("orders", exportAudit.TopicName);
        Assert.Equal(1, exportAudit.ExportRowCount);
        Assert.DoesNotContain("RawValue", JsonSerializer.Serialize(exportAudit), StringComparison.Ordinal);
    }

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
