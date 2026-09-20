using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Records;

public sealed class RecordMaskingService
{
    private const string FullyRedactedPath = "$payload";
    public const int DefaultMaxTraversalSteps = 65_536;
    public const int HardMaxTraversalSteps = 1_000_000;
    private readonly int _maxTraversalSteps;

    public RecordMaskingService(int maxTraversalSteps = DefaultMaxTraversalSteps)
    {
        if (maxTraversalSteps is < 1 or > HardMaxTraversalSteps) throw new ArgumentOutOfRangeException(nameof(maxTraversalSteps));
        _maxTraversalSteps = maxTraversalSteps;
    }

    public RecordSafePage Apply(RecordReadRequest request, RecordFilterPage page, CompiledRecordMaskingPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(policy);

        var records = new List<RecordSafeProjection>(page.Records.Count);
        long projectedBytes = 0;
        var readBudgetOutcome = page.ReadBudgetOutcome;
        var nextAnchor = page.NextAnchor;

        foreach (var item in page.Records)
        {
            var projected = Apply(request.Partition, item, policy);
            var projectedSize = JsonSerializer.SerializeToUtf8Bytes(projected).LongLength;
            if (projectedSize > request.Budget.MaxProjectedBytes - projectedBytes)
            {
                readBudgetOutcome = RecordBudgetOutcome.ProjectedByteLimit;
                nextAnchor = RecordAnchor.AtOffset(item.RawRecord.Offset);
                break;
            }

            records.Add(projected);
            projectedBytes += projectedSize;
        }

        return new RecordSafePage(
            request.ClusterId,
            request.TopicName,
            request.Partition,
            records.AsReadOnly(),
            page.LowWatermark,
            page.HighWatermark,
            nextAnchor,
            page.PreviousAnchor,
            readBudgetOutcome,
            page.FilterBudgetOutcome,
            page.Limitations,
            policy.PolicyId,
            policy.Version);
    }

    public RecordSafeProjection Apply(int partition, RecordFilteredItem item, CompiledRecordMaskingPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(policy);

        var key = item.RawRecord.Key;
        var keyRedacted = false;
        if (policy.MaskKey && key.HasValue) { key = Encoding.UTF8.GetBytes(policy.KeyReplacement); keyRedacted = true; }

        var headerRules = policy.HeaderRules.ToDictionary(rule => rule.HeaderName, rule => rule, StringComparer.Ordinal);
        var headers = item.RawRecord.Headers.Select(header =>
        {
            if (!headerRules.TryGetValue(header.Name, out var rule)) return new RecordSafeHeader(header.Name, header.Value, false);
            return new RecordSafeHeader(header.Name, Encoding.UTF8.GetBytes(rule.Replacement), true);
        }).ToArray();

        if (!policy.RequiresStructuredValue)
        {
            return new RecordSafeProjection(
                partition, item.RawRecord.Offset, item.RawRecord.TimestampUtc, key, keyRedacted,
                item.DecodedValue is null ? RecordPayloadProjectionKind.Raw : RecordPayloadProjectionKind.Structured,
                item.RawRecord.Value, item.DecodedValue?.StructuredValue.Clone(), Array.AsReadOnly(headers),
                policy.PolicyId, policy.Version, Array.Empty<string>());
        }

        if (item.DecodedValue is null) return FullyRedacted(partition, item, key, keyRedacted, headers, policy);

        try
        {
            var root = JsonNode.Parse(item.DecodedValue.StructuredValue.GetRawText());
            if (root is null) return FullyRedacted(partition, item, key, keyRedacted, headers, policy);

            var traversalStepsRemaining = _maxTraversalSteps;
            var redactedPaths = new List<string>(policy.StructuredRules.Count);
            foreach (var rule in policy.StructuredRules)
            {
                var segments = ParsePointer(rule.JsonPointer);
                var matchCount = ApplyPath(root, segments, 0, rule.Replacement, ref traversalStepsRemaining);
                if (matchCount <= 0) return FullyRedacted(partition, item, key, keyRedacted, headers, policy);
                redactedPaths.Add(rule.JsonPointer);
            }

            var projectedBytes = JsonSerializer.SerializeToUtf8Bytes(root);
            if (projectedBytes.LongLength > RecordOperationBudget.HardMaxProjectedBytes)
                return FullyRedacted(partition, item, key, keyRedacted, headers, policy);

            using var projectedDocument = JsonDocument.Parse(projectedBytes);
            return new RecordSafeProjection(
                partition, item.RawRecord.Offset, item.RawRecord.TimestampUtc, key, keyRedacted,
                RecordPayloadProjectionKind.Structured, null, projectedDocument.RootElement.Clone(), Array.AsReadOnly(headers),
                policy.PolicyId, policy.Version, redactedPaths.AsReadOnly());
        }
        catch (JsonException)
        {
            return FullyRedacted(partition, item, key, keyRedacted, headers, policy);
        }
    }

    public RecordAccessAuditMetadata BuildReadAudit(RecordSafePage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        return new RecordAccessAuditMetadata(
            page.ClusterId, page.TopicName, page.Partition, "record.read", page.PolicyId, page.PolicyVersion,
            page.Records.Count == 0 ? null : page.Records.Min(record => record.Offset),
            page.Records.Count == 0 ? null : page.Records.Max(record => record.Offset),
            page.ReadBudgetOutcome, page.FilterBudgetOutcome);
    }

    private static RecordSafeProjection FullyRedacted(int partition, RecordFilteredItem item, ReadOnlyMemory<byte>? key,
        bool keyRedacted, RecordSafeHeader[] headers, CompiledRecordMaskingPolicy policy) =>
        new(partition, item.RawRecord.Offset, item.RawRecord.TimestampUtc, key, keyRedacted,
            RecordPayloadProjectionKind.FullyRedacted, null, null, Array.AsReadOnly(headers), policy.PolicyId,
            policy.Version, Array.AsReadOnly([FullyRedactedPath]));

    private static string[] ParsePointer(string pointer) => pointer.Split('/', StringSplitOptions.None).Skip(1)
        .Select(segment => segment.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal)).ToArray();

    private static int ApplyPath(JsonNode node, IReadOnlyList<string> segments, int segmentIndex, string replacement,
        ref int traversalStepsRemaining)
    {
        if (!ConsumeTraversalStep(ref traversalStepsRemaining)) return -1;
        if (segmentIndex >= segments.Count) return 0;
        var segment = segments[segmentIndex];
        var isLast = segmentIndex == segments.Count - 1;

        if (node is JsonObject jsonObject)
        {
            if (segment == "*")
            {
                var names = new List<string>(Math.Min(jsonObject.Count, Math.Max(0, traversalStepsRemaining)));
                foreach (var property in jsonObject)
                {
                    if (!ConsumeTraversalStep(ref traversalStepsRemaining)) return -1;
                    names.Add(property.Key);
                }
                var count = 0;
                foreach (var name in names)
                {
                    if (isLast) { jsonObject[name] = replacement; count++; }
                    else if (jsonObject[name] is JsonNode child)
                    {
                        var childCount = ApplyPath(child, segments, segmentIndex + 1, replacement, ref traversalStepsRemaining);
                        if (childCount < 0) return -1;
                        count += childCount;
                    }
                }
                return count;
            }
            if (!jsonObject.TryGetPropertyValue(segment, out var propertyNode)) return 0;
            if (isLast) { jsonObject[segment] = replacement; return 1; }
            return propertyNode is null ? 0 : ApplyPath(propertyNode, segments, segmentIndex + 1, replacement, ref traversalStepsRemaining);
        }

        if (node is JsonArray jsonArray)
        {
            if (segment == "*")
            {
                var count = 0;
                for (var index = 0; index < jsonArray.Count; index++)
                {
                    if (!ConsumeTraversalStep(ref traversalStepsRemaining)) return -1;
                    if (isLast) { jsonArray[index] = replacement; count++; }
                    else if (jsonArray[index] is JsonNode child)
                    {
                        var childCount = ApplyPath(child, segments, segmentIndex + 1, replacement, ref traversalStepsRemaining);
                        if (childCount < 0) return -1;
                        count += childCount;
                    }
                }
                return count;
            }
            if (!int.TryParse(segment, NumberStyles.None, CultureInfo.InvariantCulture, out var arrayIndex) || arrayIndex < 0 || arrayIndex >= jsonArray.Count) return 0;
            if (isLast) { jsonArray[arrayIndex] = replacement; return 1; }
            return jsonArray[arrayIndex] is JsonNode arrayChild ? ApplyPath(arrayChild, segments, segmentIndex + 1, replacement, ref traversalStepsRemaining) : 0;
        }
        return 0;
    }

    private static bool ConsumeTraversalStep(ref int traversalStepsRemaining)
    {
        if (traversalStepsRemaining <= 0) return false;
        traversalStepsRemaining--;
        return true;
    }
}

public sealed class RecordExportService
{
    private const int MaxOwnedPendingWrites = 16;
    private static readonly TimeSpan AbortSettlementGrace = TimeSpan.FromMilliseconds(100);
    private static readonly byte[] JsonStart = "["u8.ToArray();
    private static readonly byte[] JsonEnd = "]"u8.ToArray();
    private static readonly byte[] Comma = ","u8.ToArray();
    private static readonly byte[] NewLine = "\n"u8.ToArray();
    private static readonly byte[] CsvHeader = Encoding.UTF8.GetBytes("partition,offset,timestampUtc,keyBase64,valueKind,value,headersJson,policyId,policyVersion\n");
    private static readonly SemaphoreSlim OwnedWriteSlots = new(MaxOwnedPendingWrites, MaxOwnedPendingWrites);
    private static readonly ConcurrentDictionary<long, OwnedPendingWrite> OwnedPendingWrites = new();
    private static long _ownedPendingWriteSequence;

    public async Task<RecordExportSummary> ExportAsync(
        RecordSafePage page,
        RecordExportRequest request,
        AuthorizationPolicyEvaluator authorizationEvaluator,
        OperatorIdentity? identity,
        Stream destination,
        CancellationToken cancellationToken = default,
        bool legacyDeploymentAuthorized = false)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(authorizationEvaluator);
        ArgumentNullException.ThrowIfNull(destination);
        EnsureExportAuthorized(page, authorizationEvaluator, identity, legacyDeploymentAuthorized);
        if (!destination.CanWrite) throw new ArgumentException("Export destination must be writable.", nameof(destination));

        var stopwatch = Stopwatch.StartNew();
        var bytesWritten = 0L;
        var rowsWritten = 0;
        var outcome = RecordExportBudgetOutcome.Complete;

        if (request.Format == RecordExportFormat.Json)
        {
            var result = await WriteWithinBudgetAsync(destination, JsonStart, stopwatch, request.Budget.MaxDuration, cancellationToken);
            if (result == BudgetedWriteResult.DeadlineExceeded) return new(request.Format, 0, 0, RecordExportBudgetOutcome.DurationLimit);
            if (result == BudgetedWriteResult.Indeterminate) return new(request.Format, 0, 0, RecordExportBudgetOutcome.Indeterminate);
            bytesWritten += JsonStart.Length;
            if (result == BudgetedWriteResult.CompletedAfterDeadline) return new(request.Format, 0, bytesWritten, RecordExportBudgetOutcome.DurationLimit);
        }
        else if (request.Format == RecordExportFormat.Csv)
        {
            if (CsvHeader.LongLength > request.Budget.MaxBytes) return new(request.Format, 0, 0, RecordExportBudgetOutcome.ByteLimit);
            var result = await WriteWithinBudgetAsync(destination, CsvHeader, stopwatch, request.Budget.MaxDuration, cancellationToken);
            if (result == BudgetedWriteResult.DeadlineExceeded) return new(request.Format, 0, 0, RecordExportBudgetOutcome.DurationLimit);
            if (result == BudgetedWriteResult.Indeterminate) return new(request.Format, 0, 0, RecordExportBudgetOutcome.Indeterminate);
            bytesWritten += CsvHeader.Length;
            if (result == BudgetedWriteResult.CompletedAfterDeadline) return new(request.Format, 0, bytesWritten, RecordExportBudgetOutcome.DurationLimit);
        }

        for (var index = 0; index < page.Records.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (stopwatch.Elapsed >= request.Budget.MaxDuration) { outcome = RecordExportBudgetOutcome.DurationLimit; break; }
            if (rowsWritten >= request.Budget.MaxRows) { outcome = RecordExportBudgetOutcome.RowLimit; break; }
            var row = request.Format switch
            {
                RecordExportFormat.Json => SerializeJsonRow(page.Records[index]),
                RecordExportFormat.Ndjson => AppendNewLine(SerializeJsonRow(page.Records[index])),
                RecordExportFormat.Csv => Encoding.UTF8.GetBytes(SerializeCsvRow(page.Records[index])),
                _ => throw new ArgumentOutOfRangeException(nameof(request.Format)),
            };
            var payload = request.Format == RecordExportFormat.Json && rowsWritten > 0 ? PrependComma(row) : row;
            var reservedClosingBytes = request.Format == RecordExportFormat.Json ? JsonEnd.LongLength : 0L;
            if (bytesWritten + payload.LongLength + reservedClosingBytes > request.Budget.MaxBytes) { outcome = RecordExportBudgetOutcome.ByteLimit; break; }
            var write = await WriteWithinBudgetAsync(destination, payload, stopwatch, request.Budget.MaxDuration, cancellationToken);
            if (write == BudgetedWriteResult.DeadlineExceeded) return new(request.Format, rowsWritten, bytesWritten, RecordExportBudgetOutcome.DurationLimit);
            if (write == BudgetedWriteResult.Indeterminate) return new(request.Format, rowsWritten, bytesWritten, RecordExportBudgetOutcome.Indeterminate);
            bytesWritten += payload.Length;
            rowsWritten++;
            if (write == BudgetedWriteResult.CompletedAfterDeadline) return new(request.Format, rowsWritten, bytesWritten, RecordExportBudgetOutcome.DurationLimit);
        }

        if (request.Format == RecordExportFormat.Json)
        {
            var end = await WriteWithinBudgetAsync(destination, JsonEnd, stopwatch, request.Budget.MaxDuration, cancellationToken);
            if (end == BudgetedWriteResult.Indeterminate) return new(request.Format, rowsWritten, bytesWritten, RecordExportBudgetOutcome.Indeterminate);
            if (end != BudgetedWriteResult.DeadlineExceeded) bytesWritten += JsonEnd.Length;
            if (end != BudgetedWriteResult.Completed) outcome = RecordExportBudgetOutcome.DurationLimit;
        }
        return new RecordExportSummary(request.Format, rowsWritten, bytesWritten, outcome);
    }

    public RecordAccessAuditMetadata BuildExportAudit(RecordSafePage page, RecordExportSummary summary)
    {
        ArgumentNullException.ThrowIfNull(page); ArgumentNullException.ThrowIfNull(summary);
        var exportedRecords = page.Records.Take(Math.Clamp(summary.RowCount, 0, page.Records.Count)).ToArray();
        return new RecordAccessAuditMetadata(page.ClusterId, page.TopicName, page.Partition, "record.export", page.PolicyId, page.PolicyVersion,
            exportedRecords.Length == 0 ? null : exportedRecords.Min(record => record.Offset),
            exportedRecords.Length == 0 ? null : exportedRecords.Max(record => record.Offset),
            page.ReadBudgetOutcome, page.FilterBudgetOutcome, summary.Format, summary.RowCount, summary.ByteCount, summary.Outcome);
    }

    private static void EnsureExportAuthorized(RecordSafePage page, AuthorizationPolicyEvaluator evaluator, OperatorIdentity? identity, bool legacyDeploymentAuthorized)
    {
        if (identity is null && legacyDeploymentAuthorized) return;
        var decision = evaluator.Evaluate(identity, new AuthorizationRequest(AuthorizationAction.RecordExport, page.ClusterId, page.TopicName));
        if (!decision.IsAllowed) throw new UnauthorizedAccessException("record.export authorization for the exact cluster/topic target is required.");
    }

    private static async Task<BudgetedWriteResult> WriteWithinBudgetAsync(Stream destination, ReadOnlyMemory<byte> payload, Stopwatch stopwatch,
        TimeSpan maxDuration, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var remaining = maxDuration - stopwatch.Elapsed;
        if (remaining <= TimeSpan.Zero) return BudgetedWriteResult.DeadlineExceeded;
        if (!await OwnedWriteSlots.WaitAsync(remaining, cancellationToken).ConfigureAwait(false)) return BudgetedWriteResult.DeadlineExceeded;
        var ownershipTransferred = false;
        try
        {
            var lengthBefore = TryGetStableLength(destination);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var writeTask = Task.Run(async () => await destination.WriteAsync(payload, linked.Token).ConfigureAwait(false), CancellationToken.None);
            var completed = await Task.WhenAny(writeTask, Task.Delay(remaining, CancellationToken.None), Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)).ConfigureAwait(false);
            if (completed == writeTask)
            {
                await writeTask.ConfigureAwait(false);
                return stopwatch.Elapsed >= maxDuration ? BudgetedWriteResult.CompletedAfterDeadline : BudgetedWriteResult.Completed;
            }
            linked.Cancel();
            var abortTask = Task.Run(() => { try { destination.Dispose(); } catch (Exception) { } }, CancellationToken.None);
            var settlementTask = SettleAbortedWriteAsync(destination, writeTask, abortTask, lengthBefore);
            var settled = await Task.WhenAny(settlementTask, Task.Delay(AbortSettlementGrace, CancellationToken.None)).ConfigureAwait(false);
            if (settled == settlementTask)
            {
                var result = await settlementTask.ConfigureAwait(false);
                if (cancellationToken.IsCancellationRequested) cancellationToken.ThrowIfCancellationRequested();
                return result;
            }
            ownershipTransferred = true;
            OwnPendingWrite(destination, settlementTask);
            if (cancellationToken.IsCancellationRequested) cancellationToken.ThrowIfCancellationRequested();
            return BudgetedWriteResult.Indeterminate;
        }
        finally { if (!ownershipTransferred) OwnedWriteSlots.Release(); }
    }

    private static async Task<BudgetedWriteResult> SettleAbortedWriteAsync(Stream destination, Task writeTask, Task abortTask, long? lengthBefore)
    {
        try { await abortTask.ConfigureAwait(false); } catch (Exception) { }
        try
        {
            await writeTask.ConfigureAwait(false);
            var lengthAfter = TryGetStableLength(destination);
            return lengthBefore.HasValue && lengthAfter.HasValue && lengthBefore.Value == lengthAfter.Value
                ? BudgetedWriteResult.DeadlineExceeded : BudgetedWriteResult.Indeterminate;
        }
        catch (OperationCanceledException) { return BudgetedWriteResult.DeadlineExceeded; }
        catch (ObjectDisposedException) { return BudgetedWriteResult.DeadlineExceeded; }
        catch (Exception) { return BudgetedWriteResult.Indeterminate; }
    }

    private static long? TryGetStableLength(Stream destination) { try { return destination.CanSeek ? destination.Length : null; } catch (Exception) { return null; } }
    private static void OwnPendingWrite(Stream destination, Task<BudgetedWriteResult> settlementTask)
    {
        var id = Interlocked.Increment(ref _ownedPendingWriteSequence);
        var owned = new OwnedPendingWrite(destination, settlementTask);
        OwnedPendingWrites[id] = owned;
        _ = settlementTask.ContinueWith(completed => { _ = completed.Exception; OwnedPendingWrites.TryRemove(id, out _); OwnedWriteSlots.Release(); GC.KeepAlive(owned); },
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private static byte[] SerializeJsonRow(RecordSafeProjection projection)
    {
        var row = new ExportRow(projection.Partition, projection.Offset, projection.TimestampUtc, ToBase64(projection.Key), projection.ValueKind.ToString(),
            projection.RawValue.HasValue ? ToBase64(projection.RawValue) : null, projection.StructuredValue,
            projection.Headers.Select(header => new ExportHeader(header.Name, Convert.ToBase64String(header.Value.Span), header.IsRedacted)).ToArray(),
            projection.PolicyId, projection.PolicyVersion, projection.RedactedPaths);
        return JsonSerializer.SerializeToUtf8Bytes(row);
    }
    private static string SerializeCsvRow(RecordSafeProjection projection)
    {
        var value = projection.StructuredValue.HasValue ? projection.StructuredValue.Value.GetRawText() : projection.RawValue.HasValue ? ToBase64(projection.RawValue) : "[REDACTED]";
        var headers = JsonSerializer.Serialize(projection.Headers.Select(header => new ExportHeader(header.Name, Convert.ToBase64String(header.Value.Span), header.IsRedacted)));
        return string.Join(",", projection.Partition.ToString(CultureInfo.InvariantCulture), projection.Offset.ToString(CultureInfo.InvariantCulture),
            CsvEscape(projection.TimestampUtc?.ToString("O", CultureInfo.InvariantCulture) ?? string.Empty), CsvEscape(ToBase64(projection.Key) ?? string.Empty),
            CsvEscape(projection.ValueKind.ToString()), CsvEscape(value ?? string.Empty), CsvEscape(headers), CsvEscape(projection.PolicyId), projection.PolicyVersion.ToString(CultureInfo.InvariantCulture)) + "\n";
    }
    private static string CsvEscape(string value) => $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    private static string? ToBase64(ReadOnlyMemory<byte>? value) => value.HasValue ? Convert.ToBase64String(value.Value.Span) : null;
    private static byte[] AppendNewLine(byte[] value) { var result = new byte[value.Length + NewLine.Length]; value.CopyTo(result, 0); NewLine.CopyTo(result, value.Length); return result; }
    private static byte[] PrependComma(byte[] value) { var result = new byte[value.Length + Comma.Length]; Comma.CopyTo(result, 0); value.CopyTo(result, Comma.Length); return result; }
    private enum BudgetedWriteResult { Completed = 1, CompletedAfterDeadline = 2, DeadlineExceeded = 3, Indeterminate = 4 }
    private sealed record OwnedPendingWrite(Stream Destination, Task<BudgetedWriteResult> SettlementTask);
    private sealed record ExportHeader(string Name, string ValueBase64, bool Redacted);
    private sealed record ExportRow(int Partition, long Offset, DateTimeOffset? TimestampUtc, string? KeyBase64, string ValueKind, string? RawValueBase64,
        JsonElement? StructuredValue, IReadOnlyList<ExportHeader> Headers, string PolicyId, int PolicyVersion, IReadOnlyList<string> RedactedPaths);
}
