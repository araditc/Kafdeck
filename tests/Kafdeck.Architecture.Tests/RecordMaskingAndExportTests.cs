using System.Text;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class RecordMaskingAndExportTests
{
    private static readonly TimeSpan WriteStartBudget = TimeSpan.FromMilliseconds(250);

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
    public void Structured_masking_preserves_significant_whitespace_in_json_pointer_segments()
    {
        const string secret = "space-sensitive-secret";
        using var document = JsonDocument.Parse(
            $$"""{"secret":"visible-value","secret ":"{{secret}}"}""");
        var item = new RecordFilteredItem(
            new KafkaRawRecord(
                10,
                DateTimeOffset.UtcNow,
                null,
                Encoding.UTF8.GetBytes(document.RootElement.GetRawText()),
                []),
            new RecordDecodedValue(1, RecordSchemaFormat.JsonSchema, document.RootElement.Clone()));
        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "space-sensitive",
                1,
                [new RecordStructuredMaskRule("/secret ")]));

        var safe = new RecordMaskingService().Apply(0, item, policy);
        var json = safe.StructuredValue!.Value;

        Assert.Equal("visible-value", json.GetProperty("secret").GetString());
        Assert.Equal("[REDACTED]", json.GetProperty("secret ").GetString());
        Assert.DoesNotContain(secret, json.GetRawText(), StringComparison.Ordinal);
        Assert.Contains("/secret ", safe.RedactedPaths);
    }

    [Fact]
    public void Header_masking_preserves_exact_whitespace_significant_name()
    {
        const string secret = "space-suffixed-header-secret";
        var item = new RecordFilteredItem(
            new KafkaRawRecord(
                11,
                DateTimeOffset.UtcNow,
                null,
                Encoding.UTF8.GetBytes("safe"),
                [
                    new KafkaRecordHeader("authorization", Encoding.UTF8.GetBytes("visible")),
                    new KafkaRecordHeader("authorization ", Encoding.UTF8.GetBytes(secret)),
                ]),
            null);
        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "exact-header",
                1,
                HeaderRules: [new RecordHeaderMaskRule("authorization ")]));

        var safe = new RecordMaskingService().Apply(0, item, policy);

        var ordinary = safe.Headers.Single(header => header.Name == "authorization");
        var suffixed = safe.Headers.Single(header => header.Name == "authorization ");
        Assert.False(ordinary.IsRedacted);
        Assert.Equal("visible", Encoding.UTF8.GetString(ordinary.Value.Span));
        Assert.True(suffixed.IsRedacted);
        Assert.Equal("[REDACTED]", Encoding.UTF8.GetString(suffixed.Value.Span));
        Assert.DoesNotContain(secret, JsonSerializer.Serialize(safe), StringComparison.Ordinal);
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
    public void Structured_masking_fails_closed_when_aggregate_traversal_budget_is_exhausted()
    {
        const string secret = "never-leak-on-budget-exhaustion";
        using var document = JsonDocument.Parse(
            $$"""{"items":[{"secret":"{{secret}}"},{"secret":"{{secret}}"},{"secret":"{{secret}}"}]}""");
        var item = new RecordFilteredItem(
            new KafkaRawRecord(
                3,
                DateTimeOffset.UtcNow,
                null,
                Encoding.UTF8.GetBytes(document.RootElement.GetRawText()),
                []),
            new RecordDecodedValue(9, RecordSchemaFormat.JsonSchema, document.RootElement.Clone()));
        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "bounded",
                1,
                [new RecordStructuredMaskRule("/items/*/secret")]));

        var safe = new RecordMaskingService(maxTraversalSteps: 3).Apply(0, item, policy);

        Assert.Equal(RecordPayloadProjectionKind.FullyRedacted, safe.ValueKind);
        Assert.Null(safe.RawValue);
        Assert.Null(safe.StructuredValue);
        Assert.DoesNotContain(secret, JsonSerializer.Serialize(safe), StringComparison.Ordinal);
    }

    [Fact]
    public void Object_wildcard_key_enumeration_is_charged_to_shared_traversal_budget()
    {
        var wide = new StringBuilder("{");
        for (var index = 0; index < 1024; index++)
        {
            if (index > 0)
            {
                wide.Append(',');
            }

            wide.Append('"').Append("field").Append(index).Append("\":\"secret\"");
        }

        wide.Append('}');
        using var document = JsonDocument.Parse(wide.ToString());
        var item = new RecordFilteredItem(
            new KafkaRawRecord(
                4,
                DateTimeOffset.UtcNow,
                null,
                Encoding.UTF8.GetBytes(wide.ToString()),
                []),
            new RecordDecodedValue(10, RecordSchemaFormat.JsonSchema, document.RootElement.Clone()));
        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "wide-object",
                1,
                [new RecordStructuredMaskRule("/*")]));

        var safe = new RecordMaskingService(maxTraversalSteps: 2).Apply(0, item, policy);

        Assert.Equal(RecordPayloadProjectionKind.FullyRedacted, safe.ValueKind);
        Assert.Null(safe.RawValue);
        Assert.Null(safe.StructuredValue);
    }

    [Fact]
    public async Task Export_evaluates_record_export_authorization_for_the_exact_page_target()
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
        var (evaluator, identity) = ExportAuthorization();

        await using var denied = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            service.ExportAsync(
                page,
                new RecordExportRequest(RecordExportFormat.Json),
                evaluator,
                null,
                denied));

        var otherPage = SafePageFor("cluster-b", "orders", SafeProjection(12, "safe"));
        await using var wrongTarget = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            service.ExportAsync(
                otherPage,
                new RecordExportRequest(RecordExportFormat.Json),
                evaluator,
                identity,
                wrongTarget));

        await using var allowed = new MemoryStream();
        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(RecordExportFormat.Json),
            evaluator,
            identity,
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
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new MemoryStream();

        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(
                format,
                new RecordExportBudget(maxRows: 2, maxBytes: 4096, maxDuration: TimeSpan.FromSeconds(1))),
            evaluator,
            identity,
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
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new MemoryStream();

        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(
                RecordExportFormat.Json,
                new RecordExportBudget(maxRows: 10, maxBytes: 256, maxDuration: TimeSpan.FromSeconds(1))),
            evaluator,
            identity,
            destination);

        Assert.Equal(0, summary.RowCount);
        Assert.Equal(RecordExportBudgetOutcome.ByteLimit, summary.Outcome);
        Assert.InRange(summary.ByteCount, 2, 256);
        using var parsed = JsonDocument.Parse(destination.ToArray());
        Assert.Equal(0, parsed.RootElement.GetArrayLength());
    }

    [Fact]
    public async Task Export_enforces_duration_budget_during_stream_writes()
    {
        var page = SafePage(SafeProjection(1, "one"));
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new DelayedWriteStream(TimeSpan.FromSeconds(1));
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(
                RecordExportFormat.Ndjson,
                new RecordExportBudget(maxRows: 10, maxBytes: 4096, maxDuration: WriteStartBudget)),
            evaluator,
            identity,
            destination);

        Assert.Equal(RecordExportBudgetOutcome.Indeterminate, summary.Outcome);
        Assert.Equal(0, summary.RowCount);
        Assert.Equal(0, summary.ByteCount);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Export_deadline_does_not_depend_on_destination_honoring_cancellation()
    {
        var page = SafePage(SafeProjection(1, "one"));
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new NonCooperativeWriteStream();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var summary = await service.ExportAsync(
            page,
            new RecordExportRequest(
                RecordExportFormat.Ndjson,
                new RecordExportBudget(maxRows: 10, maxBytes: 4096, maxDuration: WriteStartBudget)),
            evaluator,
            identity,
            destination);

        Assert.Equal(RecordExportBudgetOutcome.Indeterminate, summary.Outcome);
        Assert.Equal(0, summary.RowCount);
        Assert.Equal(0, summary.ByteCount);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Export_preserves_caller_cancellation_during_stream_write()
    {
        var page = SafePage(SafeProjection(1, "one"));
        var service = new RecordExportService();
        var (evaluator, identity) = ExportAuthorization();
        await using var destination = new DelayedWriteStream(TimeSpan.FromSeconds(1));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(20));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            service.ExportAsync(
                page,
                new RecordExportRequest(
                    RecordExportFormat.Ndjson,
                    new RecordExportBudget(maxRows: 10, maxBytes: 4096, maxDuration: TimeSpan.FromSeconds(2))),
                evaluator,
                identity,
                destination,
                cancellation.Token));
    }

    [Fact]
    public void Export_audit_reports_only_offsets_actually_exported()
    {
        var page = SafePage(
            SafeProjection(1, "one"),
            SafeProjection(2, "two"),
            SafeProjection(3, "three"));
        var service = new RecordExportService();

        var partial = service.BuildExportAudit(
            page,
            new RecordExportSummary(RecordExportFormat.Ndjson, 2, 120, RecordExportBudgetOutcome.RowLimit));
        var none = service.BuildExportAudit(
            page,
            new RecordExportSummary(RecordExportFormat.Ndjson, 0, 0, RecordExportBudgetOutcome.ByteLimit));

        Assert.Equal(1, partial.FirstOffset);
        Assert.Equal(2, partial.LastOffset);
        Assert.Null(none.FirstOffset);
        Assert.Null(none.LastOffset);
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

    private static (AuthorizationPolicyEvaluator Evaluator, OperatorIdentity Identity) ExportAuthorization()
    {
        var policy = AuthorizationPolicyCompiler.Compile(
            new AuthorizationPolicyDefinition(
                [
                    new AuthorizationRoleDefinition(
                        "exporter",
                        [
                            new AuthorizationPermissionDefinition(
                                AuthorizationAction.RecordExport,
                                ["cluster-a"],
                                ["orders"]),
                        ]),
                ],
                [
                    new AuthorizationSubjectBindingDefinition(
                        "https://issuer.example",
                        "operator-1",
                        ["exporter"]),
                ],
                []));

        return (
            new AuthorizationPolicyEvaluator(policy),
            new OperatorIdentity(new OperatorIdentityKey("https://issuer.example", "operator-1")));
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
        SafePageFor("cluster-a", "orders", records);

    private static RecordSafePage SafePageFor(
        string clusterId,
        string topicName,
        params RecordSafeProjection[] records) =>
        new(
            clusterId,
            topicName,
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

    private sealed class DelayedWriteStream : Stream
    {
        private readonly MemoryStream _inner = new();
        private readonly TimeSpan _delay;

        public DelayedWriteStream(TimeSpan delay)
        {
            _delay = delay;
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _inner.Length;
        public override long Position
        {
            get => _inner.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

        public override async ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            await Task.Delay(_delay, cancellationToken);
            await _inner.WriteAsync(buffer, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync();
            GC.SuppressFinalize(this);
        }
    }

    private sealed class NonCooperativeWriteStream : Stream
    {
        private readonly MemoryStream _inner = new();
        private bool _disposed;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => !_disposed;
        public override long Length => _inner.Length;
        public override long Position
        {
            get => _inner.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

        public override async ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None).ConfigureAwait(false);
            if (!_disposed)
            {
                await _inner.WriteAsync(buffer, CancellationToken.None).ConfigureAwait(false);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                _disposed = true;
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }

        public override ValueTask DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }
}
