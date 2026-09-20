using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

public static class KafdeckRecordEndpoints
{
    public static IEndpointRouteBuilder MapKafdeckRecordEndpoints(
        this IEndpointRouteBuilder app,
        KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(options);

        app.MapGet(
                "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions/{partition:int}/records",
                BrowseAsync)
            .WithName("v03-records-browse")
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId")
            .RequireKafdeckAuthorization(AuthorizationAction.TopicRead, "clusterId", "topicName")
            .RequireKafdeckAuthorization(AuthorizationAction.RecordRead, "clusterId", "topicName");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions/{partition:int}/records/tail",
                TailAsync)
            .WithName("v03-records-tail")
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId")
            .RequireKafdeckAuthorization(AuthorizationAction.TopicRead, "clusterId", "topicName")
            .RequireKafdeckAuthorization(AuthorizationAction.RecordRead, "clusterId", "topicName");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions/{partition:int}/records/export",
                ExportAsync)
            .WithName("v03-records-export")
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId")
            .RequireKafdeckAuthorization(AuthorizationAction.TopicRead, "clusterId", "topicName")
            .RequireKafdeckAuthorization(AuthorizationAction.RecordRead, "clusterId", "topicName")
            .RequireKafdeckAuthorization(AuthorizationAction.RecordExport, "clusterId", "topicName");

        return app;
    }

    private static async Task<IResult> BrowseAsync(
        string clusterId,
        string topicName,
        int partition,
        HttpContext http,
        KafdeckOptions options,
        RecordFilterService filterService,
        RecordMaskingService maskingService,
        CompiledRecordMaskingPolicy maskingPolicy,
        CancellationToken cancellationToken)
    {
        if (!IsConfiguredCluster(options, clusterId))
        {
            return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
        }

        try
        {
            var query = RecordHttpQuery.Parse(http.Request.Query, clusterId, topicName, partition);
            var plan = RecordFilterCompiler.Compile(query.Filter);
            var operation = new KafkaOperationContext(DateTimeOffset.UtcNow + query.Read.Budget.MaxDuration);
            var filtered = await filterService.FilterPageAsync(
                    query.Read,
                    plan,
                    operation,
                    maskingPolicy.RequiresStructuredValue,
                    cancellationToken)
                .ConfigureAwait(false);

            if (!filtered.IsSuccess || filtered.Value is null)
            {
                return ApiResults.Problem(ApiProblemMapper.FromKafka(filtered.Failure!));
            }

            var safe = maskingService.Apply(query.Read, filtered.Value, maskingPolicy);
            return Results.Ok(safe);
        }
        catch (Exception exception) when (IsSafeQueryError(exception))
        {
            return InvalidRecordQuery(exception.Message);
        }
    }

    private static async Task TailAsync(
        string clusterId,
        string topicName,
        int partition,
        HttpContext http,
        KafdeckOptions options,
        RecordLiveTailService tailService,
        RecordMaskingService maskingService,
        CompiledRecordMaskingPolicy maskingPolicy,
        CancellationToken cancellationToken)
    {
        if (!IsConfiguredCluster(options, clusterId))
        {
            await WriteProblemAsync(http, ApiProblemMapper.InvalidClusterId(clusterId), cancellationToken).ConfigureAwait(false);
            return;
        }

        RecordHttpQuery query;
        try
        {
            query = RecordHttpQuery.Parse(http.Request.Query, clusterId, topicName, partition, forceForward: true);
        }
        catch (Exception exception) when (IsSafeQueryError(exception))
        {
            await WriteProblemAsync(
                    http,
                    InvalidRecordQueryDefinition(exception.Message),
                    cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        var admissionIdentity = "legacy-deployment";
        if (OperatorSessionContextFactory.TryCreate(http.User, out var session) && session is not null)
        {
            admissionIdentity = $"{session.Identity.Key.Issuer}|{session.Identity.Key.Subject}";
        }

        var tailBudget = new RecordTailBudget(
            maxRecords: Math.Min(query.Read.Budget.MaxRecords, RecordTailBudget.HardMaxRecords),
            maxRawBytes: Math.Min(query.Read.Budget.MaxRawBytes, RecordTailBudget.HardMaxRawBytes),
            maxDuration: query.Read.Budget.MaxDuration,
            maxRecordsPerSecond: query.Read.Budget.MaxRecordsPerSecond);
        var tailRequest = new RecordTailRequest(admissionIdentity, query.Read, query.Filter, tailBudget);

        http.Response.StatusCode = StatusCodes.Status200OK;
        http.Response.ContentType = "text/event-stream";
        http.Response.Headers.CacheControl = "no-store";
        http.Response.Headers.Append("X-Accel-Buffering", "no");

        await foreach (var frame in tailService
                           .TailAsync(tailRequest, maskingPolicy.RequiresStructuredValue, cancellationToken)
                           .ConfigureAwait(false))
        {
            object payload = frame.Kind switch
            {
                RecordTailFrameKind.Records when frame.Page is not null =>
                    new RecordTailSafeFrame(
                        "records",
                        maskingService.Apply(query.Read, frame.Page, maskingPolicy),
                        null),
                RecordTailFrameKind.KafkaFailure when frame.Failure is not null =>
                    new RecordTailSafeFrame("error", null, SafeFailure(frame.Failure)),
                RecordTailFrameKind.AdmissionDenied =>
                    new RecordTailSafeFrame("admissionDenied", null, null),
                _ => new RecordTailSafeFrame("completed", null, null),
            };

            await http.Response.WriteAsync("data: ", cancellationToken).ConfigureAwait(false);
            await JsonSerializer.SerializeAsync(
                    http.Response.Body,
                    payload,
                    payload.GetType(),
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            await http.Response.WriteAsync("\n\n", cancellationToken).ConfigureAwait(false);
            await http.Response.Body.FlushAsync(cancellationToken).ConfigureAwait(false);

            if (frame.Kind is RecordTailFrameKind.KafkaFailure or RecordTailFrameKind.AdmissionDenied or RecordTailFrameKind.Completed)
            {
                break;
            }
        }
    }

    private static async Task ExportAsync(
        string clusterId,
        string topicName,
        int partition,
        HttpContext http,
        KafdeckOptions options,
        RecordFilterService filterService,
        RecordMaskingService maskingService,
        RecordExportService exportService,
        CompiledRecordMaskingPolicy maskingPolicy,
        AuthorizationPolicyEvaluator authorizationEvaluator,
        CancellationToken cancellationToken)
    {
        if (!IsConfiguredCluster(options, clusterId))
        {
            await WriteProblemAsync(http, ApiProblemMapper.InvalidClusterId(clusterId), cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            var query = RecordHttpQuery.Parse(http.Request.Query, clusterId, topicName, partition);
            var plan = RecordFilterCompiler.Compile(query.Filter);
            var operation = new KafkaOperationContext(DateTimeOffset.UtcNow + query.Read.Budget.MaxDuration);
            var filtered = await filterService.FilterPageAsync(
                    query.Read,
                    plan,
                    operation,
                    maskingPolicy.RequiresStructuredValue,
                    cancellationToken)
                .ConfigureAwait(false);

            if (!filtered.IsSuccess || filtered.Value is null)
            {
                await WriteProblemAsync(http, ApiProblemMapper.FromKafka(filtered.Failure!), cancellationToken).ConfigureAwait(false);
                return;
            }

            var safe = maskingService.Apply(query.Read, filtered.Value, maskingPolicy);
            var format = RecordHttpQuery.ParseExportFormat(http.Request.Query["format"]);
            var exportRequest = new RecordExportRequest(format);
            var identity = OperatorSessionContextFactory.TryCreate(http.User, out var session) && session is not null
                ? session.Identity
                : null;

            http.Response.StatusCode = StatusCodes.Status200OK;
            http.Response.Headers.CacheControl = "no-store";
            http.Response.ContentType = format switch
            {
                RecordExportFormat.Csv => "text/csv; charset=utf-8",
                RecordExportFormat.Ndjson => "application/x-ndjson",
                _ => "application/json",
            };

            await exportService.ExportAsync(
                    safe,
                    exportRequest,
                    authorizationEvaluator,
                    identity,
                    http.Response.Body,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (UnauthorizedAccessException)
        {
            if (!http.Response.HasStarted)
            {
                await WriteProblemAsync(
                        http,
                        new ApiProblemDefinition(
                            StatusCodes.Status403Forbidden,
                            "urn:kafdeck:problem:record-export-denied",
                            "Record export denied",
                            "The current operator identity is not authorized to export this record target.",
                            "record_export_denied"),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception exception) when (IsSafeQueryError(exception))
        {
            if (!http.Response.HasStarted)
            {
                await WriteProblemAsync(http, InvalidRecordQueryDefinition(exception.Message), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static bool IsConfiguredCluster(KafdeckOptions options, string clusterId) =>
        options.Clusters.Any(cluster => string.Equals(cluster.Id, clusterId, StringComparison.Ordinal));

    private static bool IsSafeQueryError(Exception exception) =>
        exception is ArgumentException or FormatException or OverflowException;

    private static IResult InvalidRecordQuery(string detail) =>
        ApiResults.Problem(InvalidRecordQueryDefinition(detail));

    private static ApiProblemDefinition InvalidRecordQueryDefinition(string detail) => new(
        StatusCodes.Status400BadRequest,
        "urn:kafdeck:problem:invalid-record-query",
        "Invalid record query",
        detail,
        "invalid_record_query");

    private static async Task WriteProblemAsync(
        HttpContext http,
        ApiProblemDefinition problem,
        CancellationToken cancellationToken)
    {
        http.Response.StatusCode = problem.Status;
        http.Response.ContentType = "application/problem+json";
        await http.Response.WriteAsJsonAsync(
                new
                {
                    type = problem.Type,
                    title = problem.Title,
                    status = problem.Status,
                    detail = problem.Detail,
                    code = problem.Code,
                },
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static object SafeFailure(KafkaFailure failure) => new
    {
        category = failure.Category.ToString(),
        code = failure.Code,
        message = failure.SafeMessage,
        retryable = failure.IsRetryable,
    };

    private sealed record RecordTailSafeFrame(
        string Kind,
        RecordSafePage? Page,
        object? Failure);

    private sealed record RecordHttpQuery(
        RecordReadRequest Read,
        RecordFilterRequest Filter)
    {
        public static RecordHttpQuery Parse(
            IQueryCollection query,
            string clusterId,
            string topicName,
            int partition,
            bool forceForward = false)
        {
            var anchor = ParseAnchor(query);
            var direction = forceForward
                ? RecordReadDirection.Forward
                : ParseDirection(query["direction"]);
            var budget = new RecordOperationBudget(
                ParseInt(query["maxRecords"], RecordOperationBudget.DefaultMaxRecords),
                ParseLong(query["maxBytes"], RecordOperationBudget.DefaultMaxRawBytes),
                RecordOperationBudget.DefaultMaxProjectedBytes,
                RecordOperationBudget.DefaultMaxDuration,
                RecordOperationBudget.DefaultMaxRecordsPerSecond);
            var read = new RecordReadRequest(clusterId, topicName, partition, anchor, direction, budget);

            var headers = CreateHeaderPredicates(query);
            var preFilter = new RecordPreFilter(
                ParseNullableLong(query["minimumOffset"]),
                ParseNullableLong(query["maximumOffset"]),
                ParseNullableTimestamp(query["minimumTimestampUtc"]),
                ParseNullableTimestamp(query["maximumTimestampUtc"]),
                EmptyToNull(query["keyEquals"]),
                EmptyToNull(query["keyPrefix"]),
                headers);

            RecordStructuredFilter? structured = null;
            var expression = EmptyToNull(query["filter"]);
            if (expression is not null)
            {
                structured = new RecordStructuredFilter(
                    ParseFilterLanguage(query["filterLanguage"]),
                    expression);
            }

            return new RecordHttpQuery(read, new RecordFilterRequest(preFilter, structured));
        }

        public static RecordExportFormat ParseExportFormat(string? value)
        {
            if (string.IsNullOrWhiteSpace(value) || value.Equals("json", StringComparison.OrdinalIgnoreCase))
            {
                return RecordExportFormat.Json;
            }

            if (value.Equals("ndjson", StringComparison.OrdinalIgnoreCase))
            {
                return RecordExportFormat.Ndjson;
            }

            if (value.Equals("csv", StringComparison.OrdinalIgnoreCase))
            {
                return RecordExportFormat.Csv;
            }

            throw new ArgumentException("format must be json, ndjson or csv.");
        }

        private static RecordAnchor ParseAnchor(IQueryCollection query)
        {
            if (ParseNullableLong(query["offset"]) is { } offset)
            {
                return RecordAnchor.AtOffset(offset);
            }

            if (ParseNullableTimestamp(query["timestampUtc"]) is { } timestamp)
            {
                return RecordAnchor.AtTimestamp(timestamp);
            }

            var anchor = EmptyToNull(query["anchor"]);
            return anchor?.ToLowerInvariant() switch
            {
                null or "earliest" => RecordAnchor.Earliest(),
                "latest" => RecordAnchor.Latest(),
                _ => throw new ArgumentException("anchor must be earliest or latest when offset/timestampUtc is not supplied."),
            };
        }

        private static RecordReadDirection ParseDirection(string? value) =>
            value?.Trim().ToLowerInvariant() switch
            {
                null or "" or "forward" => RecordReadDirection.Forward,
                "previous" => RecordReadDirection.Previous,
                _ => throw new ArgumentException("direction must be forward or previous."),
            };

        private static RecordFilterLanguage ParseFilterLanguage(string? value) =>
            value?.Trim().ToLowerInvariant() switch
            {
                null or "" or "cel" => RecordFilterLanguage.Cel,
                "jq" or "jqstyle" => RecordFilterLanguage.JqStyle,
                _ => throw new ArgumentException("filterLanguage must be cel or jq."),
            };

        private static IReadOnlyList<RecordHeaderPredicate> CreateHeaderPredicates(IQueryCollection query)
        {
            var name = EmptyToNull(query["headerName"]);
            var equals = EmptyToNull(query["headerEquals"]);
            var prefix = EmptyToNull(query["headerPrefix"]);
            if (name is null)
            {
                if (equals is not null || prefix is not null)
                {
                    throw new ArgumentException("headerName is required with a header predicate.");
                }

                return Array.Empty<RecordHeaderPredicate>();
            }

            return [new RecordHeaderPredicate(name, equals, prefix)];
        }

        private static int ParseInt(string? value, int fallback) =>
            string.IsNullOrWhiteSpace(value)
                ? fallback
                : int.Parse(value, NumberStyles.None, CultureInfo.InvariantCulture);

        private static long ParseLong(string? value, long fallback) =>
            string.IsNullOrWhiteSpace(value)
                ? fallback
                : long.Parse(value, NumberStyles.None, CultureInfo.InvariantCulture);

        private static long? ParseNullableLong(string? value) =>
            string.IsNullOrWhiteSpace(value)
                ? null
                : long.Parse(value, NumberStyles.None, CultureInfo.InvariantCulture);

        private static DateTimeOffset? ParseNullableTimestamp(string? value) =>
            string.IsNullOrWhiteSpace(value)
                ? null
                : DateTimeOffset.Parse(
                    value,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

        private static string? EmptyToNull(string? value) =>
            string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
