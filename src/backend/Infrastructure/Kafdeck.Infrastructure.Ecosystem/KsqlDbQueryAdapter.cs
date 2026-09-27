using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Ecosystem;

public sealed class KsqlDbQueryAdapter : IKsqlQueryPort, IDisposable
{
    public const int HardMaxConcurrentQueriesPerCluster = 8;
    public const int HardMaxColumns = 1_000;
    private readonly IReadOnlyDictionary<string, HttpReadRuntime> _runtimes;
    private readonly IReadOnlyDictionary<string, SemaphoreSlim> _gates;
    private readonly TimeProvider _timeProvider;

    public KsqlDbQueryAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
        : this(
            clusterProfiles,
            secretResolver,
            static _ => new HttpClientHandler { AllowAutoRedirect = false },
            timeProvider,
            HardMaxConcurrentQueriesPerCluster)
    {
    }

    internal KsqlDbQueryAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        Func<ClusterProfile, HttpMessageHandler> handlerFactory,
        TimeProvider? timeProvider = null,
        int maxConcurrentQueriesPerCluster =
            HardMaxConcurrentQueriesPerCluster)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);
        ArgumentNullException.ThrowIfNull(secretResolver);
        ArgumentNullException.ThrowIfNull(handlerFactory);

        if (maxConcurrentQueriesPerCluster is < 1 or
            > HardMaxConcurrentQueriesPerCluster)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConcurrentQueriesPerCluster));
        }

        _timeProvider = timeProvider ?? TimeProvider.System;
        _runtimes = clusterProfiles
            .Where(profile => profile.KsqlDb is not null)
            .ToDictionary(
                profile => profile.Id,
                profile =>
                {
                    var ksql = profile.KsqlDb!;
                    return ReadOnlyHttpSupport.CreateRuntime(
                        ksql.Url,
                        ksql.Username,
                        ksql.Password,
                        secretResolver,
                        handlerFactory(profile));
                },
                StringComparer.Ordinal);

        _gates = _runtimes.Keys.ToDictionary(
            clusterId => clusterId,
            _ => new SemaphoreSlim(
                maxConcurrentQueriesPerCluster,
                maxConcurrentQueriesPerCluster),
            StringComparer.Ordinal);
    }

    public async Task<ReadViewResult<KsqlQueryResult>> ExecuteQueryAsync(
        string clusterId,
        string statement,
        KsqlQueryLimits limits,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentNullException.ThrowIfNull(limits);

        var admission = KsqlStatementClassifier.Classify(statement);
        if (!admission.IsAllowed)
        {
            return Failed<KsqlQueryResult>(
                admission.Outcome == KsqlStatementAdmissionOutcome.Invalid
                    ? ReadViewFailureCategory.InvalidRequest
                    : ReadViewFailureCategory.Unsupported,
                admission.Code,
                admission.SafeMessage,
                false);
        }

        if (!_runtimes.TryGetValue(clusterId, out var runtime) ||
            !_gates.TryGetValue(clusterId, out var gate))
        {
            return Failed<KsqlQueryResult>(
                ReadViewFailureCategory.NotConfigured,
                "ksql_not_configured",
                "ksqlDB is not configured for the requested cluster.",
                false);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed<KsqlQueryResult>(
                ReadViewFailureCategory.Cancelled,
                "operation_cancelled",
                "ksqlDB query was cancelled.",
                false);
        }

        if (!limits.IsWithinHardCaps)
        {
            return Failed<KsqlQueryResult>(
                ReadViewFailureCategory.InvalidRequest,
                "ksql_query_limits_invalid",
                "ksqlDB query limits are outside the admitted hard bounds.",
                false);
        }

        var bounded = limits;
        using var duration = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        duration.CancelAfter(bounded.MaxDuration);
        var queryToken = duration.Token;

        var gateAcquired = false;
        try
        {
            await gate.WaitAsync(queryToken).ConfigureAwait(false);
            gateAcquired = true;

            using var request = BuildRequest(
                runtime,
                admission.CanonicalStatement!);
            using var response = await runtime.Client.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    queryToken)
                .ConfigureAwait(false);

            ValidateResponse(response);

            return await ReadResultAsync(
                    response,
                    bounded,
                    cancellationToken,
                    queryToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return Failed<KsqlQueryResult>(
                ReadViewFailureCategory.Cancelled,
                "operation_cancelled",
                "ksqlDB query was cancelled.",
                false);
        }
        catch (OperationCanceledException)
        {
            return Failed<KsqlQueryResult>(
                ReadViewFailureCategory.Timeout,
                "ksql_query_duration_exceeded",
                "ksqlDB query exceeded the configured duration bound.",
                false);
        }
        catch (ReadViewHttpException exception)
        {
            return Failed<KsqlQueryResult>(
                exception.Category,
                exception.Code,
                exception.SafeMessage,
                exception.Retryable);
        }
        catch (HttpRequestException)
        {
            return Failed<KsqlQueryResult>(
                ReadViewFailureCategory.Unavailable,
                "ksql_unavailable",
                "ksqlDB is temporarily unavailable.",
                true);
        }
        catch (JsonException)
        {
            return Failed<KsqlQueryResult>(
                ReadViewFailureCategory.InvalidResponse,
                "invalid_ksql_query_response",
                "ksqlDB returned an invalid bounded query response.",
                false);
        }
        finally
        {
            if (gateAcquired)
            {
                gate.Release();
            }
        }
    }

    public void Dispose()
    {
        foreach (var runtime in _runtimes.Values)
        {
            runtime.Client.Dispose();
        }

        foreach (var gate in _gates.Values)
        {
            gate.Dispose();
        }
    }

    private static HttpRequestMessage BuildRequest(
        HttpReadRuntime runtime,
        string statement)
    {
        var request = new HttpRequestMessage(
            HttpMethod.Post,
            "query-stream")
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
            Content = JsonContent.Create(
                new KsqlQueryRequest(
                    statement,
                    new Dictionary<string, string>(
                        StringComparer.Ordinal)),
                options: null,
                mediaType: new MediaTypeHeaderValue(
                    "application/vnd.ksql.v1+json")),
        };

        request.Headers.Accept.Add(
            new MediaTypeWithQualityHeaderValue(
                "application/vnd.ksqlapi.delimited.v1"));

        if (runtime.Authorization is not null)
        {
            request.Headers.Authorization = runtime.Authorization;
        }

        return request;
    }

    private static void ValidateResponse(
        HttpResponseMessage response)
    {
        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.InvalidResponse,
                "ksql_query_redirect_rejected",
                "ksqlDB query redirect was rejected.",
                false);
        }

        if (response.StatusCode is
            HttpStatusCode.Unauthorized or
            HttpStatusCode.Forbidden)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.Unauthorized,
                "ksql_query_authorization_denied",
                "ksqlDB denied the query.",
                false);
        }

        if ((int)response.StatusCode >= 500)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.Unavailable,
                "ksql_query_unavailable",
                "ksqlDB query endpoint is temporarily unavailable.",
                true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.InvalidResponse,
                "ksql_query_rejected",
                "ksqlDB rejected the admitted read query.",
                false);
        }
    }

    private static async Task<ReadViewResult<KsqlQueryResult>>
        ReadResultAsync(
            HttpResponseMessage response,
            KsqlQueryLimits limits,
            CancellationToken callerToken,
            CancellationToken queryToken)
    {
        await using var stream = await response.Content
            .ReadAsStreamAsync(queryToken)
            .ConfigureAwait(false);

        var rows = new List<KsqlQueryRow>(
            Math.Min(limits.MaxRows, 256));
        KsqlQueryHeader? header = null;
        using var line = new MemoryStream();
        var buffer = new byte[8 * 1024];
        long totalBytes = 0;
        var truncated = false;
        string? limitReason = null;
        var sawExtraRow = false;

        bool ProcessLine()
        {
            if (line.Length == 0)
            {
                return false;
            }

            var bytes = line.ToArray();
            line.SetLength(0);

            using var document = JsonDocument.Parse(bytes);
            var root = document.RootElement;

            if (header is null)
            {
                if (root.ValueKind != JsonValueKind.Object ||
                    !root.TryGetProperty(
                        "columnNames",
                        out var columnNames) ||
                    columnNames.ValueKind != JsonValueKind.Array ||
                    !root.TryGetProperty(
                        "columnTypes",
                        out var columnTypes) ||
                    columnTypes.ValueKind != JsonValueKind.Array)
                {
                    throw new JsonException(
                        "ksqlDB query header is invalid.");
                }

                if (columnNames.GetArrayLength() is < 1 or > HardMaxColumns ||
                    columnTypes.GetArrayLength() != columnNames.GetArrayLength())
                {
                    throw new JsonException(
                        "ksqlDB query header column bounds are invalid.");
                }

                header = new KsqlQueryHeader(
                    root.TryGetProperty(
                        "queryId",
                        out var queryId) &&
                    queryId.ValueKind == JsonValueKind.String
                        ? queryId.GetString()
                        : null,
                    columnNames
                        .EnumerateArray()
                        .Select(item =>
                            item.ValueKind == JsonValueKind.String
                                ? item.GetString() ?? string.Empty
                                : throw new JsonException(
                                    "ksqlDB column name is invalid."))
                        .ToArray(),
                    columnTypes
                        .EnumerateArray()
                        .Select(item =>
                            item.ValueKind == JsonValueKind.String
                                ? item.GetString() ?? string.Empty
                                : throw new JsonException(
                                    "ksqlDB column type is invalid."))
                        .ToArray());

                return false;
            }

            if (root.ValueKind != JsonValueKind.Array)
            {
                throw new JsonException(
                    "ksqlDB query row is invalid.");
            }

            if (rows.Count >= limits.MaxRows)
            {
                sawExtraRow = true;
                return true;
            }

            var columns = root
                .EnumerateArray()
                .Select(item => item.Clone())
                .ToArray();

            if (columns.Length != header.ColumnNames.Count)
            {
                throw new JsonException(
                    "ksqlDB query row column count does not match the header.");
            }

            rows.Add(new KsqlQueryRow(columns));
            return false;
        }

        while (true)
        {
            int read;
            try
            {
                read = await stream.ReadAsync(
                        buffer.AsMemory(),
                        queryToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (!callerToken.IsCancellationRequested &&
                      header is not null)
            {
                truncated = true;
                limitReason = "duration_limit";
                break;
            }

            if (read == 0)
            {
                if (line.Length > 0)
                {
                    _ = ProcessLine();
                }
                break;
            }

            for (var index = 0; index < read; index++)
            {
                totalBytes++;
                if (totalBytes > limits.MaxBytes)
                {
                    truncated = true;
                    limitReason = "byte_limit";
                    break;
                }

                var value = buffer[index];
                if (value == (byte)'\n')
                {
                    if (ProcessLine())
                    {
                        truncated = true;
                        limitReason = "row_limit";
                        break;
                    }
                    continue;
                }

                if (value != (byte)'\r')
                {
                    line.WriteByte(value);
                }
            }

            if (truncated)
            {
                break;
            }
        }

        if (sawExtraRow)
        {
            truncated = true;
            limitReason = "row_limit";
        }

        if (header is null)
        {
            return Failed<KsqlQueryResult>(
                ReadViewFailureCategory.InvalidResponse,
                "ksql_query_header_missing",
                "ksqlDB query response did not contain a valid header.",
                false);
        }

        return ReadViewResult<KsqlQueryResult>.Success(
            new KsqlQueryResult(
                header,
                rows,
                truncated,
                limitReason,
                Math.Min(totalBytes, limits.MaxBytes)));
    }

    private static ReadViewResult<T> Failed<T>(
        ReadViewFailureCategory category,
        string code,
        string message,
        bool retryable) =>
        ReadViewResult<T>.Failed(
            new ReadViewFailure(
                category,
                code,
                message,
                retryable));

    private sealed record KsqlQueryRequest(
        string Sql,
        IReadOnlyDictionary<string, string> Properties);
}
