using System.Net;
using System.Text;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W59StreamsTelemetryTests
{
    [Fact]
    public async Task Typed_telemetry_adapter_uses_only_fixed_get_routes()
    {
        var handler = new StubHandler(request =>
            request.RequestUri!.AbsolutePath switch
            {
                "/applications" => Json(
                    HttpStatusCode.OK,
                    """
                    [
                      {
                        "applicationId":"orders-app",
                        "evidenceSource":"streams-agent-v1",
                        "observedAtUtc":"2026-09-27T12:00:00Z",
                        "stale":false
                      }
                    ]
                    """),
                "/applications/orders-app/topology" => Json(
                    HttpStatusCode.OK,
                    """
                    {
                      "applicationId":"orders-app",
                      "evidenceSource":"streams-agent-v1",
                      "observedAtUtc":"2026-09-27T12:00:00Z",
                      "stale":false,
                      "nodes":[
                        {
                          "id":"source",
                          "name":"orders-source",
                          "type":"source",
                          "inputTopics":["orders"],
                          "outputTopics":["orders-normalized"],
                          "stateStores":["orders-store"]
                        }
                      ]
                    }
                    """),
                "/applications/orders-app/state-stores" => Json(
                    HttpStatusCode.OK,
                    """
                    {
                      "applicationId":"orders-app",
                      "evidenceSource":"streams-agent-v1",
                      "observedAtUtc":"2026-09-27T12:00:00Z",
                      "stale":false,
                      "stores":[
                        {
                          "name":"orders-store",
                          "type":"RocksDB",
                          "approximateEntries":42,
                          "sizeBytes":4096,
                          "health":"Healthy"
                        }
                      ]
                    }
                    """),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            });

        using var adapter = CreateAdapter(handler);

        var applications = await adapter.ListApplicationsAsync(
            "prod",
            Operation(),
            CancellationToken.None);
        var topology = await adapter.GetTopologyAsync(
            "prod",
            "orders-app",
            Operation(),
            CancellationToken.None);
        var stores = await adapter.GetStateStoresAsync(
            "prod",
            "orders-app",
            Operation(),
            CancellationToken.None);

        Assert.True(applications.IsSuccess, applications.Failure?.SafeMessage);
        Assert.True(topology.IsSuccess, topology.Failure?.SafeMessage);
        Assert.True(stores.IsSuccess, stores.Failure?.SafeMessage);
        Assert.Equal("orders", Assert.Single(topology.Value!.Nodes).InputTopics[0]);
        Assert.Equal(42, Assert.Single(stores.Value!.Stores).ApproximateEntries);
        Assert.Equal(
            new[]
            {
                "/applications",
                "/applications/orders-app/topology",
                "/applications/orders-app/state-stores",
            },
            handler.Paths);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Telemetry_identity_mismatch_fails_closed()
    {
        var handler = new StubHandler(_ =>
            Json(
                HttpStatusCode.OK,
                """
                {
                  "applicationId":"different-app",
                  "evidenceSource":"streams-agent-v1",
                  "observedAtUtc":"2026-09-27T12:00:00Z",
                  "stale":false,
                  "nodes":[]
                }
                """));

        using var adapter = CreateAdapter(handler);

        var result = await adapter.GetTopologyAsync(
            "prod",
            "orders-app",
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ReadViewFailureCategory.InvalidResponse,
            result.Failure!.Category);
        Assert.Equal(
            "invalid_streams_telemetry_response",
            result.Failure.Code);
    }

    [Fact]
    public async Task Duplicate_application_ids_are_rejected()
    {
        var handler = new StubHandler(_ =>
            Json(
                HttpStatusCode.OK,
                """
                [
                  {
                    "applicationId":"app",
                    "evidenceSource":"agent",
                    "observedAtUtc":"2026-09-27T12:00:00Z",
                    "stale":false
                  },
                  {
                    "applicationId":"app",
                    "evidenceSource":"agent",
                    "observedAtUtc":"2026-09-27T12:00:01Z",
                    "stale":false
                  }
                ]
                """));

        using var adapter = CreateAdapter(handler);
        var result = await adapter.ListApplicationsAsync(
            "prod",
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ReadViewFailureCategory.InvalidResponse,
            result.Failure!.Category);
    }

    [Fact]
    public async Task Non_string_topic_evidence_is_invalid_not_size_overflow()
    {
        var handler = new StubHandler(_ =>
            Json(
                HttpStatusCode.OK,
                """
                {
                  "applicationId":"app",
                  "evidenceSource":"agent",
                  "observedAtUtc":"2026-09-27T12:00:00Z",
                  "stale":false,
                  "nodes":[
                    {
                      "id":"node",
                      "name":"node",
                      "type":"source",
                      "inputTopics":[42],
                      "outputTopics":[],
                      "stateStores":[]
                    }
                  ]
                }
                """));

        using var adapter = CreateAdapter(handler);
        var result = await adapter.GetTopologyAsync(
            "prod",
            "app",
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ReadViewFailureCategory.InvalidResponse,
            result.Failure!.Category);
        Assert.Equal(
            "invalid_streams_telemetry_response",
            result.Failure.Code);
    }

    [Fact]
    public async Task Missing_telemetry_profile_is_explicit_not_configured()
    {
        using var adapter = new StreamsTelemetryReadAdapter(
            new[]
            {
                new ClusterProfile(
                    "prod",
                    new[] { "broker.example:9092" },
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null),
            },
            new SecretResolver());

        var result = await adapter.ListApplicationsAsync(
            "prod",
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ReadViewFailureCategory.NotConfigured,
            result.Failure!.Category);
        Assert.Equal(
            "streams_telemetry_not_configured",
            result.Failure.Code);
    }

    [Fact]
    public async Task Lineage_preserves_observed_and_inferred_truth()
    {
        var observedAt =
            new DateTimeOffset(
                2026,
                9,
                27,
                12,
                0,
                0,
                TimeSpan.Zero);

        var telemetry = new FakeTelemetryPort(
            new[]
            {
                new StreamsTopologyObservation(
                    "producer-app",
                    "agent-a",
                    observedAt,
                    false,
                    new[]
                    {
                        new StreamsTopologyNode(
                            "sink",
                            "producer-sink",
                            "sink",
                            Array.Empty<string>(),
                            new[] { "normalized-orders" },
                            Array.Empty<string>()),
                    }),
                new StreamsTopologyObservation(
                    "consumer-app",
                    "agent-b",
                    observedAt.AddMinutes(-1),
                    true,
                    new[]
                    {
                        new StreamsTopologyNode(
                            "source",
                            "consumer-source",
                            "source",
                            new[] { "normalized-orders" },
                            Array.Empty<string>(),
                            Array.Empty<string>()),
                    }),
            });

        var service = new StreamsLineageReadService(telemetry);
        var result = await service.GetLineageAsync(
            "prod",
            Operation(maxItems: 100),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);

        var observed = result.Value!.Edges
            .Where(edge =>
                edge.EvidenceKind ==
                LineageEvidenceKind.Observed)
            .ToArray();
        var inferred = Assert.Single(
            result.Value.Edges.Where(edge =>
                edge.EvidenceKind ==
                LineageEvidenceKind.Inferred));

        Assert.Equal(2, observed.Length);
        Assert.Equal(
            "producer-app",
            inferred.Source.Id);
        Assert.Equal(
            "consumer-app",
            inferred.Destination.Id);
        Assert.Equal(0.9, inferred.Confidence);
        Assert.True(inferred.Stale);
        Assert.Equal(
            "topic-match:normalized-orders",
            inferred.Provenance);
    }

    [Fact]
    public async Task Missing_topology_is_partial_without_false_edge_bound()
    {
        var telemetry = new PartialTelemetryPort();
        var result = await new StreamsLineageReadService(telemetry)
            .GetLineageAsync(
                "prod",
                Operation(maxItems: 10),
                CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);
        Assert.True(result.Value!.Partial);
        Assert.Contains(
            result.Value.Limitations,
            item => item.Code == "streams_topology_partial");
        Assert.DoesNotContain(
            result.Value.Limitations,
            item => item.Code == "lineage_edge_bound");
    }

    [Fact]
    public async Task Lineage_edge_bound_is_explicit()
    {
        var nodes = Enumerable.Range(0, 5)
            .Select(index =>
                new StreamsTopologyNode(
                    $"node-{index}",
                    $"node-{index}",
                    "processor",
                    new[] { $"input-{index}" },
                    new[] { $"output-{index}" },
                    Array.Empty<string>()))
            .ToArray();

        var telemetry = new FakeTelemetryPort(
            new[]
            {
                new StreamsTopologyObservation(
                    "app",
                    "agent",
                    DateTimeOffset.UtcNow,
                    false,
                    nodes),
            });

        var result = await new StreamsLineageReadService(telemetry)
            .GetLineageAsync(
                "prod",
                Operation(maxItems: 3),
                CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);
        Assert.Equal(3, result.Value!.Edges.Count);
        Assert.True(result.Value.Partial);
        Assert.Contains(
            result.Value.Limitations,
            item => item.Code == "lineage_edge_bound");
    }

    private static StreamsTelemetryReadAdapter CreateAdapter(
        HttpMessageHandler handler) =>
        new(
            new[] { Cluster() },
            new SecretResolver(),
            _ => handler);

    private static ClusterProfile Cluster() =>
        new(
            "prod",
            new[] { "broker.example:9092" },
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            null,
            null,
            null,
            new StreamsTelemetryProfile(
                "https://streams.example",
                null,
                null));

    private static ReadViewOperationContext Operation(
        int maxItems = 100) =>
        new(
            DateTimeOffset.UtcNow.AddSeconds(10),
            maxItems,
            1024 * 1024);

    private static HttpResponseMessage Json(
        HttpStatusCode status,
        string body) =>
        new(status)
        {
            Content = new StringContent(
                body,
                Encoding.UTF8,
                "application/json"),
        };

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _response;

        public StubHandler(
            Func<HttpRequestMessage, HttpResponseMessage> response)
        {
            _response = response;
        }

        public List<string> Paths { get; } = [];
        public List<HttpMethod> Methods { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Paths.Add(request.RequestUri!.AbsolutePath);
            Methods.Add(request.Method);
            return Task.FromResult(_response(request));
        }
    }

    private sealed class PartialTelemetryPort :
        IStreamsTelemetryReadPort
    {
        public Task<ReadViewResult<IReadOnlyList<StreamsApplicationSummary>>>
            ListApplicationsAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<IReadOnlyList<StreamsApplicationSummary>>.Success(
                    new[]
                    {
                        new StreamsApplicationSummary(
                            "missing-app",
                            "agent",
                            DateTimeOffset.UtcNow,
                            false),
                    }));

        public Task<ReadViewResult<StreamsTopologyObservation>>
            GetTopologyAsync(
                string clusterId,
                string applicationId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<StreamsTopologyObservation>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Unavailable,
                        "topology_unavailable",
                        "Topology unavailable.",
                        true)));

        public Task<ReadViewResult<StreamsStateStoreObservation>>
            GetStateStoresAsync(
                string clusterId,
                string applicationId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<StreamsStateStoreObservation>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Unavailable,
                        "stores_unavailable",
                        "Stores unavailable.",
                        true)));
    }

    private sealed class FakeTelemetryPort :
        IStreamsTelemetryReadPort
    {
        private readonly IReadOnlyDictionary<
            string,
            StreamsTopologyObservation> _topologies;

        public FakeTelemetryPort(
            IEnumerable<StreamsTopologyObservation> topologies)
        {
            _topologies = topologies.ToDictionary(
                topology => topology.ApplicationId,
                StringComparer.Ordinal);
        }

        public Task<ReadViewResult<IReadOnlyList<StreamsApplicationSummary>>>
            ListApplicationsAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<IReadOnlyList<StreamsApplicationSummary>>
                    .Success(
                        _topologies.Values
                            .Select(topology =>
                                new StreamsApplicationSummary(
                                    topology.ApplicationId,
                                    topology.EvidenceSource,
                                    topology.ObservedAtUtc,
                                    topology.Stale))
                            .OrderBy(
                                item => item.ApplicationId,
                                StringComparer.Ordinal)
                            .ToArray()));

        public Task<ReadViewResult<StreamsTopologyObservation>>
            GetTopologyAsync(
                string clusterId,
                string applicationId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                _topologies.TryGetValue(
                    applicationId,
                    out var topology)
                    ? ReadViewResult<StreamsTopologyObservation>
                        .Success(topology)
                    : ReadViewResult<StreamsTopologyObservation>
                        .Failed(
                            new ReadViewFailure(
                                ReadViewFailureCategory.Unavailable,
                                "topology_missing",
                                "Topology missing.",
                                false)));

        public Task<ReadViewResult<StreamsStateStoreObservation>>
            GetStateStoresAsync(
                string clusterId,
                string applicationId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<StreamsStateStoreObservation>.Success(
                    new StreamsStateStoreObservation(
                        applicationId,
                        "fake",
                        DateTimeOffset.UtcNow,
                        false,
                        Array.Empty<StreamsStateStoreMetric>())));
    }
}
