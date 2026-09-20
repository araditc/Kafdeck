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

    public RecordSafePage Apply(
        RecordReadRequest request,
        RecordFilterPage page,
        CompiledRecordMaskingPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(policy);

        var records = page.Records
            .Select(item => Apply(request.Partition, item, policy))
            .ToArray();

        return new RecordSafePage(
            request.ClusterId,
            request.TopicName,
            request.Partition,
            Array.AsReadOnly(records),
            page.LowWatermark,
            page.HighWatermark,
            page.NextAnchor,
            page.PreviousAnchor,
            page.ReadBudgetOutcome,
            page.FilterBudgetOutcome,
            page.Limitations,
            policy.PolicyId,
            policy.Version);
    }

    public RecordSafeProjection Apply(
        int partition,
        RecordFilteredItem item,
        CompiledRecordMaskingPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(policy);

        var key = item.RawRecord.Key;
        var keyRedacted = false;
        if (policy.MaskKey && key.HasValue)
        {
            key = Encoding.UTF8.GetBytes(policy.KeyReplacement);
            keyRedacted = true;
        }

        var headerRules = policy.HeaderRules.ToDictionary(
            rule => rule.HeaderName,
            rule => rule,
            StringComparer.Ordinal);
        var headers = item.RawRecord.Headers
            .Select(header =>
            {
                if (!headerRules.TryGetValue(header.Name, out var rule))
                {
                    return new RecordSafeHeader(header.Name, header.Value, false);
                }

                return new RecordSafeHeader(
                    header.Name,
                    Encoding.UTF8.GetBytes(rule.Replacement),
                    true);
            })
            .ToArray();

        if (!policy.RequiresStructuredValue)
        {
            return new RecordSafeProjection(
                partition,
                item.RawRecord.Offset,
                item.RawRecord.TimestampUtc,
                key,
                keyRedacted,
                item.DecodedValue is null
                    ? RecordPayloadProjectionKind.Raw
                    : RecordPayloadProjectionKind.Structured,
                item.RawRecord.Value,
                item.DecodedValue?.StructuredValue.Clone(),
                Array.AsReadOnly(headers),
                policy.PolicyId,
                policy.Version,
                Array.Empty<string>());
        }

        if (item.DecodedValue is null)
        {
            return FullyRedacted(partition, item, key, keyRedacted, headers, policy);
        }

        try
        {
            var root = JsonNode.Parse(item.DecodedValue.StructuredValue.GetRawText());
            if (root is null)
            {
                return FullyRedacted(partition, item, key, keyRedacted, headers, policy);
            }

            var redactedPaths = new List<string>(policy.StructuredRules.Count);
            foreach (var rule in policy.StructuredRules)
            {
                var segments = ParsePointer(rule.JsonPointer);
                var matchCount = ApplyPath(root, segments, 0, rule.Replacement);
                if (matchCount == 0)
                {
                    return FullyRedacted(partition, item, key, keyRedacted, headers, policy);
                }

                redactedPaths.Add(rule.JsonPointer);
            }

            var projectedBytes = JsonSerializer.SerializeToUtf8Bytes(root);
            if (projectedBytes.LongLength > RecordOperationBudget.HardMaxProjectedBytes)
            {
                return FullyRedacted(partition, item, key, keyRedacted, headers, policy);
            }

            using var projectedDocument = JsonDocument.Parse(projectedBytes);
            return new RecordSafeProjection(
                partition,
                item.RawRecord.Offset,
                item.RawRecord.TimestampUtc,
                key,
                keyRedacted,
                RecordPayloadProjectionKind.Structured,
                null,
                projectedDocument.RootElement.Clone(),
                Array.AsReadOnly(headers),
                policy.PolicyId,
                policy.Version,
                redactedPaths.AsReadOnly());
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
            page.ClusterId,
            page.TopicName,
            page.Partition,
            "record.read",
            page.PolicyId,
            page.PolicyVersion,
            page.Records.Count == 0 ? null : page.Records.Min(record => record.Offset),
            page.Records.Count == 0 ? null : page.Records.Max(record => record.Offset),
            page.ReadBudgetOutcome,
            page.FilterBudgetOutcome);
    }

    private static RecordSafeProjection FullyRedacted(
        int partition,
        RecordFilteredItem item,
        ReadOnlyMemory<byte>? key,
        bool keyRedacted,
        RecordSafeHeader[] headers,
        CompiledRecordMaskingPolicy policy) =>
        new(
            partition,
            item.RawRecord.Offset,
            item.RawRecord.TimestampUtc,
            key,
            keyRedacted,
            RecordPayloadProjectionKind.FullyRedacted,
            null,
            null,
            Array.AsReadOnly(headers),
            policy.PolicyId,
            policy.Version,
            Array.AsReadOnly([FullyRedactedPath]));

    private static string[] ParsePointer(string pointer) =>
        pointer
            .Split('/', StringSplitOptions.None)
            .Skip(1)
            .Select(segment => segment.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal))
            .ToArray();

    private static int ApplyPath(
        JsonNode node,
        IReadOnlyList<string> segments,
        int segmentIndex,
        string replacement)
    {
        if (segmentIndex >= segments.Count)
        {
            return 0;
        }

        var segment = segments[segmentIndex];
        var isLast = segmentIndex == segments.Count - 1;

        if (node is JsonObject jsonObject)
        {
            if (segment == "*")
            {
                var names = jsonObject.Select(pair => pair.Key).ToArray();
                var count = 0;
                foreach (var name in names)
                {
                    if (isLast)
                    {
                        jsonObject[name] = replacement;
                        count++;
                    }
                    else if (jsonObject[name] is JsonNode child)
                    {
                        count += ApplyPath(child, segments, segmentIndex + 1, replacement);
                    }
                }

                return count;
            }

            if (!jsonObject.TryGetPropertyValue(segment, out var propertyNode))
            {
                return 0;
            }

            if (isLast)
            {
                jsonObject[segment] = replacement;
                return 1;
            }

            return propertyNode is null
                ? 0
                : ApplyPath(propertyNode, segments, segmentIndex + 1, replacement);
        }

        if (node is JsonArray jsonArray)
        {
            if (segment == "*")
            {
                var count = 0;
                for (var index = 0; index < jsonArray.Count; index++)
                {
                    if (isLast)
                    {
                        jsonArray[index] = replacement;
                        count++;
                    }
                    else if (jsonArray[index] is JsonNode child)
                    {
                        count += ApplyPath(child, segments, segmentIndex + 1, replacement);
                    }
                }

                return count;
            }

            if (!int.TryParse(segment, NumberStyles.None, CultureInfo.InvariantCulture, out var arrayIndex) ||
                arrayIndex < 0 || arrayIndex >= jsonArray.Count)
            {
                return 0;
            }

            if (isLast)
            {
                jsonArray[arrayIndex] = replacement;
                return 1;
            }

            return jsonArray[arrayIndex] is JsonNode arrayChild
                ? ApplyPath(arrayChild, segments, segmentIndex + 1, replacement)
                : 0;
        }

        return 0;
    }
}

public sealed class RecordExportService
{
    private static readonly byte[] JsonStart = "["u8.ToArray();
    private static readonly byte[] JsonEnd = "]"u8.ToArray();
    private static readonly byte[] Comma = ","u8.ToArray();
    private static readonly byte[] NewLine = "\n"u8.ToArray();
    private static readonly byte[] CsvHeader = Encoding.UTF8.GetBytes(
        "partition,offset,timestampUtc,keyBase64,valueKind,value,headersJson,policyId,policyVersion\n");

    public async Task<RecordExportSummary> ExportAsync(
        RecordSafePage page,
        RecordExportRequest request,
        AuthorizationRequest authorizationRequest,
        AuthorizationDecision exportAuthorization,
        Stream destination,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(authorizationRequest);
        ArgumentNullException.ThrowIfNull(exportAuthorization);
        ArgumentNullException.ThrowIfNull(destination);

        EnsureExportAuthorized(page, authorizationRequest, exportAuthorization);

        if (!destination.CanWrite)
        {
            throw new ArgumentException("Export destination must be writable.", nameof(destination));
        }

        var stopwatch = Stopwatch.StartNew();
        var bytesWritten = 0L;
        var rowsWritten = 0;
        var outcome = RecordExportBudgetOutcome.Complete;

        if (request.Format == RecordExportFormat.Json)
        {
            await destination.WriteAsync(JsonStart, cancellationToken);
            bytesWritten += JsonStart.Length;
        }
        else if (request.Format == RecordExportFormat.Csv)
        {
            if (CsvHeader.LongLength > request.Budget.MaxBytes)
            {
                return new RecordExportSummary(request.Format, 0, 0, RecordExportBudgetOutcome.ByteLimit);
            }

            await destination.WriteAsync(CsvHeader, cancellationToken);
            bytesWritten += CsvHeader.Length;
        }

        for (var index = 0; index < page.Records.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (stopwatch.Elapsed >= request.Budget.MaxDuration)
            {
                outcome = RecordExportBudgetOutcome.DurationLimit;
                break;
            }

            if (rowsWritten >= request.Budget.MaxRows)
            {
                outcome = RecordExportBudgetOutcome.RowLimit;
                break;
            }

            var row = request.Format switch
            {
                RecordExportFormat.Json => SerializeJsonRow(page.Records[index]),
                RecordExportFormat.Ndjson => AppendNewLine(SerializeJsonRow(page.Records[index])),
                RecordExportFormat.Csv => Encoding.UTF8.GetBytes(SerializeCsvRow(page.Records[index])),
                _ => throw new ArgumentOutOfRangeException(nameof(request.Format)),
            };

            if (stopwatch.Elapsed >= request.Budget.MaxDuration)
            {
                outcome = RecordExportBudgetOutcome.DurationLimit;
                break;
            }

            var delimiterBytes = request.Format == RecordExportFormat.Json && rowsWritten > 0
                ? Comma.LongLength
                : 0L;
            var reservedClosingBytes = request.Format == RecordExportFormat.Json
                ? JsonEnd.LongLength
                : 0L;

            if (bytesWritten + delimiterBytes + row.LongLength + reservedClosingBytes > request.Budget.MaxBytes)
            {
                outcome = RecordExportBudgetOutcome.ByteLimit;
                break;
            }

            if (delimiterBytes > 0)
            {
                await destination.WriteAsync(Comma, cancellationToken);
                bytesWritten += Comma.Length;
            }

            await destination.WriteAsync(row, cancellationToken);
            bytesWritten += row.Length;
            rowsWritten++;
        }

        if (request.Format == RecordExportFormat.Json)
        {
            await destination.WriteAsync(JsonEnd, cancellationToken);
            bytesWritten += JsonEnd.Length;
        }

        return new RecordExportSummary(request.Format, rowsWritten, bytesWritten, outcome);
    }

    public RecordAccessAuditMetadata BuildExportAudit(
        RecordSafePage page,
        RecordExportSummary summary)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(summary);

        return new RecordAccessAuditMetadata(
            page.ClusterId,
            page.TopicName,
            page.Partition,
            "record.export",
            page.PolicyId,
            page.PolicyVersion,
            page.Records.Count == 0 ? null : page.Records.Min(record => record.Offset),
            page.Records.Count == 0 ? null : page.Records.Max(record => record.Offset),
            page.ReadBudgetOutcome,
            page.FilterBudgetOutcome,
            summary.Format,
            summary.RowCount,
            summary.ByteCount,
            summary.Outcome);
    }

    private static void EnsureExportAuthorized(
        RecordSafePage page,
        AuthorizationRequest authorizationRequest,
        AuthorizationDecision exportAuthorization)
    {
        if (!exportAuthorization.IsAllowed ||
            authorizationRequest.Action != AuthorizationAction.RecordExport ||
            !string.Equals(authorizationRequest.ClusterId, page.ClusterId, StringComparison.Ordinal) ||
            !string.Equals(authorizationRequest.ResourceName, page.TopicName, StringComparison.Ordinal))
        {
            throw new UnauthorizedAccessException("record.export authorization for the exact cluster/topic target is required.");
        }
    }

    private static byte[] SerializeJsonRow(RecordSafeProjection projection)
    {
        var row = new ExportRow(
            projection.Partition,
            projection.Offset,
            projection.TimestampUtc,
            ToBase64(projection.Key),
            projection.ValueKind.ToString(),
            projection.RawValue.HasValue ? ToBase64(projection.RawValue) : null,
            projection.StructuredValue,
            projection.Headers.Select(header => new ExportHeader(
                header.Name,
                Convert.ToBase64String(header.Value.Span),
                header.IsRedacted)).ToArray(),
            projection.PolicyId,
            projection.PolicyVersion,
            projection.RedactedPaths);

        return JsonSerializer.SerializeToUtf8Bytes(row);
    }

    private static string SerializeCsvRow(RecordSafeProjection projection)
    {
        var value = projection.StructuredValue.HasValue
            ? projection.StructuredValue.Value.GetRawText()
            : projection.RawValue.HasValue
                ? ToBase64(projection.RawValue)
                : "[REDACTED]";
        var headers = JsonSerializer.Serialize(
            projection.Headers.Select(header => new ExportHeader(
                header.Name,
                Convert.ToBase64String(header.Value.Span),
                header.IsRedacted)));

        return string.Join(",",
            projection.Partition.ToString(CultureInfo.InvariantCulture),
            projection.Offset.ToString(CultureInfo.InvariantCulture),
            CsvEscape(projection.TimestampUtc?.ToString("O", CultureInfo.InvariantCulture) ?? string.Empty),
            CsvEscape(ToBase64(projection.Key) ?? string.Empty),
            CsvEscape(projection.ValueKind.ToString()),
            CsvEscape(value ?? string.Empty),
            CsvEscape(headers),
            CsvEscape(projection.PolicyId),
            projection.PolicyVersion.ToString(CultureInfo.InvariantCulture)) + "\n";
    }

    private static string CsvEscape(string value) =>
        $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

    private static string? ToBase64(ReadOnlyMemory<byte>? value) =>
        value.HasValue ? Convert.ToBase64String(value.Value.Span) : null;

    private static byte[] AppendNewLine(byte[] value)
    {
        var result = new byte[value.Length + NewLine.Length];
        value.CopyTo(result, 0);
        NewLine.CopyTo(result, value.Length);
        return result;
    }

    private sealed record ExportHeader(string Name, string ValueBase64, bool Redacted);

    private sealed record ExportRow(
        int Partition,
        long Offset,
        DateTimeOffset? TimestampUtc,
        string? KeyBase64,
        string ValueKind,
        string? RawValueBase64,
        JsonElement? StructuredValue,
        IReadOnlyList<ExportHeader> Headers,
        string PolicyId,
        int PolicyVersion,
        IReadOnlyList<string> RedactedPaths);
}
