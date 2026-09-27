using System.Text.Json;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Ecosystem;

public sealed class StreamsTelemetryReadAdapter :
    IStreamsTelemetryReadPort,
    IDisposable
{
    private readonly IReadOnlyDictionary<string, HttpReadRuntime> _runtimes;
    private readonly TimeProvider _timeProvider;

    public StreamsTelemetryReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
        : this(
            clusterProfiles,
            secretResolver,
            static _ => new HttpClientHandler
            {
                AllowAutoRedirect = false,
            },
            timeProvider)
    {
    }

    internal StreamsTelemetryReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        Func<ClusterProfile, HttpMessageHandler> handlerFactory,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(clusterProfiles);
        ArgumentNullException.ThrowIfNull(secretResolver);
        ArgumentNullException.ThrowIfNull(handlerFactory);

        _timeProvider = timeProvider ?? TimeProvider.System;
        _runtimes = clusterProfiles
            .Where(cluster =>
                cluster.StreamsTelemetry?.ProviderProfile ==
                StreamsTelemetryProviderProfile.KafdeckTelemetryV1)
            .ToDictionary(
                cluster => cluster.Id,
                cluster =>
                {
                    var telemetry = cluster.StreamsTelemetry!;
                    return ReadOnlyHttpSupport.CreateRuntime(
                        telemetry.Url,
                        telemetry.Username,
                        telemetry.Password,
                        secretResolver,
                        handlerFactory(cluster));
                },
                StringComparer.Ordinal);
    }

    public Task<ReadViewResult<IReadOnlyList<StreamsApplicationSummary>>>
        ListApplicationsAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        ExecuteAsync<IReadOnlyList<StreamsApplicationSummary>>(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        "applications",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                if (root.ValueKind != JsonValueKind.Array)
                {
                    throw new JsonException(
                        "Streams telemetry applications response is invalid.");
                }

                var result = new List<StreamsApplicationSummary>();
                var applicationIds = new HashSet<string>(
                    StringComparer.Ordinal);
                foreach (var item in root.EnumerateArray())
                {
                    if (result.Count >= operation.MaxItems)
                    {
                        throw new ResponseBoundExceededException();
                    }

                    var application = ParseApplication(item);
                    if (!applicationIds.Add(application.ApplicationId))
                    {
                        throw new JsonException(
                            "Streams telemetry contains duplicate application IDs.");
                    }

                    result.Add(application);
                }

                return result
                    .OrderBy(
                        application => application.ApplicationId,
                        StringComparer.Ordinal)
                    .ToArray();
            });

    public Task<ReadViewResult<StreamsTopologyObservation>>
        GetTopologyAsync(
            string clusterId,
            string applicationId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        var normalized = NormalizeApplicationId(applicationId);
        if (normalized is null)
        {
            return Task.FromResult(
                Failed<StreamsTopologyObservation>(
                    ReadViewFailureCategory.InvalidRequest,
                    "invalid_streams_application_id",
                    "Streams application ID is invalid.",
                    false));
        }

        return ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        $"applications/{Uri.EscapeDataString(normalized)}/topology",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                return ParseTopology(
                    root,
                    normalized,
                    operation.MaxItems);
            });
    }

    public Task<ReadViewResult<StreamsStateStoreObservation>>
        GetStateStoresAsync(
            string clusterId,
            string applicationId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
    {
        var normalized = NormalizeApplicationId(applicationId);
        if (normalized is null)
        {
            return Task.FromResult(
                Failed<StreamsStateStoreObservation>(
                    ReadViewFailureCategory.InvalidRequest,
                    "invalid_streams_application_id",
                    "Streams application ID is invalid.",
                    false));
        }

        return ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            async (runtime, token) =>
            {
                var root = await ReadOnlyHttpSupport.GetJsonAsync(
                        runtime,
                        $"applications/{Uri.EscapeDataString(normalized)}/state-stores",
                        operation.MaxResponseBytes,
                        token)
                    .ConfigureAwait(false);

                return ParseStateStores(
                    root,
                    normalized,
                    operation.MaxItems);
            });
    }

    public void Dispose()
    {
        foreach (var runtime in _runtimes.Values)
        {
            runtime.Client.Dispose();
        }
    }

    private async Task<ReadViewResult<T>> ExecuteAsync<T>(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken,
        Func<HttpReadRuntime, CancellationToken, Task<T>> action)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(
                ReadViewFailureCategory.Cancelled,
                "operation_cancelled",
                "Streams telemetry read was cancelled.",
                false);
        }

        if (operation.DeadlineUtc <= _timeProvider.GetUtcNow())
        {
            return Failed<T>(
                ReadViewFailureCategory.Timeout,
                "deadline_exceeded",
                "Streams telemetry read exceeded its deadline.",
                true);
        }

        if (!_runtimes.TryGetValue(clusterId, out var runtime))
        {
            return Failed<T>(
                ReadViewFailureCategory.NotConfigured,
                "streams_telemetry_not_configured",
                "Streams telemetry is not configured for the requested cluster.",
                false);
        }

        try
        {
            using var deadline =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            var remaining =
                operation.DeadlineUtc - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return Failed<T>(
                    ReadViewFailureCategory.Timeout,
                    "deadline_exceeded",
                    "Streams telemetry read exceeded its deadline.",
                    true);
            }

            deadline.CancelAfter(remaining);
            var value = await action(
                    runtime,
                    deadline.Token)
                .ConfigureAwait(false);

            return ReadViewResult<T>.Success(value);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(
                ReadViewFailureCategory.Cancelled,
                "operation_cancelled",
                "Streams telemetry read was cancelled.",
                false);
        }
        catch (OperationCanceledException)
        {
            return Failed<T>(
                ReadViewFailureCategory.Timeout,
                "deadline_exceeded",
                "Streams telemetry read exceeded its deadline.",
                true);
        }
        catch (ReadViewHttpException exception)
        {
            return Failed<T>(
                exception.Category,
                exception.Code,
                exception.SafeMessage,
                exception.Retryable);
        }
        catch (ResponseBoundExceededException)
        {
            return Failed<T>(
                ReadViewFailureCategory.ResponseTooLarge,
                "streams_telemetry_response_too_large",
                "Streams telemetry response exceeded the configured bound.",
                false);
        }
        catch (HttpRequestException)
        {
            return Failed<T>(
                ReadViewFailureCategory.Unavailable,
                "streams_telemetry_unavailable",
                "Streams telemetry is temporarily unavailable.",
                true);
        }
        catch (JsonException)
        {
            return Failed<T>(
                ReadViewFailureCategory.InvalidResponse,
                "invalid_streams_telemetry_response",
                "Streams telemetry returned an invalid response.",
                false);
        }
    }

    private static StreamsApplicationSummary ParseApplication(
        JsonElement item)
    {
        if (item.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(
                "Streams application summary is invalid.");
        }

        return new StreamsApplicationSummary(
            RequireString(item, "applicationId", 256),
            RequireString(item, "evidenceSource", 256),
            RequireTimestamp(item, "observedAtUtc"),
            RequireBoolean(item, "stale"));
    }

    private static StreamsTopologyObservation ParseTopology(
        JsonElement root,
        string expectedApplicationId,
        int maxItems)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(
                "Streams topology response is invalid.");
        }

        var applicationId =
            RequireString(root, "applicationId", 256);
        if (!string.Equals(
                applicationId,
                expectedApplicationId,
                StringComparison.Ordinal))
        {
            throw new JsonException(
                "Streams topology application identity changed.");
        }

        if (!root.TryGetProperty("nodes", out var nodesElement) ||
            nodesElement.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(
                "Streams topology nodes are invalid.");
        }

        var nodes = new List<StreamsTopologyNode>();
        var nodeIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var node in nodesElement.EnumerateArray())
        {
            if (nodes.Count >= maxItems)
            {
                throw new ResponseBoundExceededException();
            }

            if (node.ValueKind != JsonValueKind.Object)
            {
                throw new JsonException(
                    "Streams topology node is invalid.");
            }

            var nodeId = RequireString(node, "id", 256);
            if (!nodeIds.Add(nodeId))
            {
                throw new JsonException(
                    "Streams topology contains duplicate node IDs.");
            }

            nodes.Add(
                new StreamsTopologyNode(
                    nodeId,
                    RequireString(node, "name", 512),
                    RequireString(node, "type", 128),
                    ReadStringArray(
                        node,
                        "inputTopics",
                        maxItems),
                    ReadStringArray(
                        node,
                        "outputTopics",
                        maxItems),
                    ReadStringArray(
                        node,
                        "stateStores",
                        maxItems)));
        }

        return new StreamsTopologyObservation(
            applicationId,
            RequireString(root, "evidenceSource", 256),
            RequireTimestamp(root, "observedAtUtc"),
            RequireBoolean(root, "stale"),
            nodes);
    }

    private static StreamsStateStoreObservation ParseStateStores(
        JsonElement root,
        string expectedApplicationId,
        int maxItems)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(
                "Streams state-store response is invalid.");
        }

        var applicationId =
            RequireString(root, "applicationId", 256);
        if (!string.Equals(
                applicationId,
                expectedApplicationId,
                StringComparison.Ordinal))
        {
            throw new JsonException(
                "Streams state-store application identity changed.");
        }

        if (!root.TryGetProperty("stores", out var storesElement) ||
            storesElement.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(
                "Streams state-store metrics are invalid.");
        }

        var stores = new List<StreamsStateStoreMetric>();
        foreach (var store in storesElement.EnumerateArray())
        {
            if (stores.Count >= maxItems)
            {
                throw new ResponseBoundExceededException();
            }

            if (store.ValueKind != JsonValueKind.Object)
            {
                throw new JsonException(
                    "Streams state-store metric is invalid.");
            }

            stores.Add(
                new StreamsStateStoreMetric(
                    RequireString(store, "name", 512),
                    RequireString(store, "type", 128),
                    OptionalInt64(
                        store,
                        "approximateEntries"),
                    OptionalInt64(
                        store,
                        "sizeBytes"),
                    OptionalString(
                        store,
                        "health",
                        128)));
        }

        return new StreamsStateStoreObservation(
            applicationId,
            RequireString(root, "evidenceSource", 256),
            RequireTimestamp(root, "observedAtUtc"),
            RequireBoolean(root, "stale"),
            stores);
    }

    private static IReadOnlyList<string> ReadStringArray(
        JsonElement root,
        string propertyName,
        int maxItems)
    {
        if (!root.TryGetProperty(
                propertyName,
                out var element))
        {
            return Array.Empty<string>();
        }

        if (element.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(
                $"Streams telemetry '{propertyName}' is invalid.");
        }

        var values = new List<string>();
        foreach (var item in element.EnumerateArray())
        {
            if (values.Count >= maxItems)
            {
                throw new ResponseBoundExceededException();
            }

            if (item.ValueKind != JsonValueKind.String)
            {
                throw new JsonException(
                    $"Streams telemetry '{propertyName}' value is invalid.");
            }

            var value = item.GetString();
            if (string.IsNullOrWhiteSpace(value) ||
                value.Length > 512 ||
                value.Any(char.IsControl))
            {
                throw new JsonException(
                    $"Streams telemetry '{propertyName}' value is invalid.");
            }

            values.Add(value.Trim());
        }

        return values
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
    }

    private static string RequireString(
        JsonElement root,
        string propertyName,
        int maxLength)
    {
        var value = OptionalString(
            root,
            propertyName,
            maxLength);
        return value ??
            throw new JsonException(
                $"Streams telemetry '{propertyName}' is required.");
    }

    private static string? OptionalString(
        JsonElement root,
        string propertyName,
        int maxLength)
    {
        if (!root.TryGetProperty(
                propertyName,
                out var element) ||
            element.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (element.ValueKind != JsonValueKind.String)
        {
            throw new JsonException(
                $"Streams telemetry '{propertyName}' is invalid.");
        }

        var value = element.GetString();
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new JsonException(
                $"Streams telemetry '{propertyName}' is invalid.");
        }

        return value.Trim();
    }

    private static DateTimeOffset RequireTimestamp(
        JsonElement root,
        string propertyName)
    {
        var value = RequireString(
            root,
            propertyName,
            64);
        if (!DateTimeOffset.TryParse(
                value,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal |
                System.Globalization.DateTimeStyles.AdjustToUniversal,
                out var timestamp))
        {
            throw new JsonException(
                $"Streams telemetry '{propertyName}' timestamp is invalid.");
        }

        return timestamp;
    }

    private static bool RequireBoolean(
        JsonElement root,
        string propertyName)
    {
        if (!root.TryGetProperty(
                propertyName,
                out var element) ||
            element.ValueKind is not
                (JsonValueKind.True or JsonValueKind.False))
        {
            throw new JsonException(
                $"Streams telemetry '{propertyName}' is invalid.");
        }

        return element.GetBoolean();
    }

    private static long? OptionalInt64(
        JsonElement root,
        string propertyName)
    {
        if (!root.TryGetProperty(
                propertyName,
                out var element) ||
            element.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (!element.TryGetInt64(out var value) ||
            value < 0)
        {
            throw new JsonException(
                $"Streams telemetry '{propertyName}' is invalid.");
        }

        return value;
    }

    private static string? NormalizeApplicationId(
        string? applicationId)
    {
        if (string.IsNullOrWhiteSpace(applicationId))
        {
            return null;
        }

        var normalized = applicationId.Trim();
        return normalized.Length <= 256 &&
               !normalized.Any(char.IsControl)
            ? normalized
            : null;
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
}

public sealed class StreamsLineageReadService :
    ILineageReadPort
{
    private readonly IStreamsTelemetryReadPort _telemetry;

    public StreamsLineageReadService(
        IStreamsTelemetryReadPort telemetry)
    {
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public async Task<ReadViewResult<LineageGraph>> GetLineageAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        var applications = await _telemetry
            .ListApplicationsAsync(
                clusterId,
                operation,
                cancellationToken)
            .ConfigureAwait(false);

        if (!applications.IsSuccess ||
            applications.Value is null)
        {
            return ReadViewResult<LineageGraph>.Failed(
                applications.Failure!);
        }

        var edges = new List<LineageEdge>();
        var limitations = new List<ReadViewLimitation>();
        var appTopicEvidence = new List<AppTopicEvidence>();
        var partial = false;
        var edgeBoundHit = false;

        foreach (var application in applications.Value)
        {
            var topology = await _telemetry
                .GetTopologyAsync(
                    clusterId,
                    application.ApplicationId,
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);

            if (!topology.IsSuccess ||
                topology.Value is null)
            {
                partial = true;
                limitations.Add(
                    new ReadViewLimitation(
                        "streams_topology_partial",
                        $"Topology evidence for application '{application.ApplicationId}' was unavailable."));
                continue;
            }

            var observed = topology.Value;
            var inputs = new HashSet<string>(
                StringComparer.Ordinal);
            var outputs = new HashSet<string>(
                StringComparer.Ordinal);

            foreach (var node in observed.Nodes)
            {
                foreach (var topic in node.InputTopics)
                {
                    inputs.Add(topic);
                    if (!TryAddEdge(
                            edges,
                            operation.MaxItems,
                            new LineageEdge(
                                new LineageEntity(
                                    "topic",
                                    topic),
                                new LineageEntity(
                                    "streams-node",
                                    $"{observed.ApplicationId}/{node.Id}"),
                                LineageEvidenceKind.Observed,
                                observed.EvidenceSource,
                                observed.ObservedAtUtc,
                                1.0,
                                observed.Stale)))
                    {
                        partial = true;
                        edgeBoundHit = true;
                        break;
                    }
                }

                foreach (var topic in node.OutputTopics)
                {
                    outputs.Add(topic);
                    if (!TryAddEdge(
                            edges,
                            operation.MaxItems,
                            new LineageEdge(
                                new LineageEntity(
                                    "streams-node",
                                    $"{observed.ApplicationId}/{node.Id}"),
                                new LineageEntity(
                                    "topic",
                                    topic),
                                LineageEvidenceKind.Observed,
                                observed.EvidenceSource,
                                observed.ObservedAtUtc,
                                1.0,
                                observed.Stale)))
                    {
                        partial = true;
                        edgeBoundHit = true;
                        break;
                    }
                }

                if (partial &&
                    edges.Count >= operation.MaxItems)
                {
                    break;
                }
            }

            appTopicEvidence.Add(
                new AppTopicEvidence(
                    observed.ApplicationId,
                    inputs,
                    outputs,
                    observed.EvidenceSource,
                    observed.ObservedAtUtc,
                    observed.Stale));

            if (edges.Count >= operation.MaxItems)
            {
                break;
            }
        }

        if (edges.Count < operation.MaxItems)
        {
            for (var left = 0;
                 left < appTopicEvidence.Count;
                 left++)
            {
                for (var right = 0;
                     right < appTopicEvidence.Count;
                     right++)
                {
                    if (left == right)
                    {
                        continue;
                    }

                    var producer = appTopicEvidence[left];
                    var consumer = appTopicEvidence[right];
                    foreach (var topic in producer.OutputTopics
                                 .Intersect(
                                     consumer.InputTopics,
                                     StringComparer.Ordinal))
                    {
                        if (!TryAddEdge(
                                edges,
                                operation.MaxItems,
                                new LineageEdge(
                                    new LineageEntity(
                                        "streams-application",
                                        producer.ApplicationId),
                                    new LineageEntity(
                                        "streams-application",
                                        consumer.ApplicationId),
                                    LineageEvidenceKind.Inferred,
                                    $"topic-match:{topic}",
                                    producer.ObservedAtUtc <
                                    consumer.ObservedAtUtc
                                        ? producer.ObservedAtUtc
                                        : consumer.ObservedAtUtc,
                                    0.9,
                                    producer.Stale ||
                                    consumer.Stale)))
                        {
                            partial = true;
                            edgeBoundHit = true;
                            break;
                        }
                    }

                    if (edges.Count >= operation.MaxItems)
                    {
                        break;
                    }
                }

                if (edges.Count >= operation.MaxItems)
                {
                    break;
                }
            }
        }

        if (edgeBoundHit &&
            !limitations.Any(item =>
                string.Equals(
                    item.Code,
                    "lineage_edge_bound",
                    StringComparison.Ordinal)))
        {
            limitations.Add(
                new ReadViewLimitation(
                    "lineage_edge_bound",
                    "Lineage edges were truncated by the configured item bound."));
        }

        var graph = new LineageGraph(
            edges
                .OrderBy(
                    edge => edge.Source.Kind,
                    StringComparer.Ordinal)
                .ThenBy(
                    edge => edge.Source.Id,
                    StringComparer.Ordinal)
                .ThenBy(
                    edge => edge.Destination.Kind,
                    StringComparer.Ordinal)
                .ThenBy(
                    edge => edge.Destination.Id,
                    StringComparer.Ordinal)
                .ToArray(),
            partial,
            limitations);

        return ReadViewResult<LineageGraph>.Success(
            graph,
            limitations);
    }

    private static bool TryAddEdge(
        ICollection<LineageEdge> edges,
        int maxItems,
        LineageEdge edge)
    {
        if (edges.Count >= maxItems)
        {
            return false;
        }

        edges.Add(edge);
        return true;
    }

    private sealed record AppTopicEvidence(
        string ApplicationId,
        IReadOnlySet<string> InputTopics,
        IReadOnlySet<string> OutputTopics,
        string EvidenceSource,
        DateTimeOffset ObservedAtUtc,
        bool Stale);
}
