using System.Net;
using System.Text;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class EcosystemReadAdapterTests
{
    [Fact]
    public async Task Connect_reads_are_get_only_and_configuration_is_redacted()
    {
        var handler = new StubHandler(request =>
        {
            return request.RequestUri!.PathAndQuery switch
            {
                "/" => Json(HttpStatusCode.OK, """{"version":"4.1.0","commit":"abc","kafka_cluster_id":"cluster-k"}"""),
                "/connectors" => Json(HttpStatusCode.OK, """["payments"]"""),
                "/connectors/payments/status" => Json(HttpStatusCode.OK, """
                {
                  "name":"payments",
                  "connector":{"state":"RUNNING","worker_id":"worker:8083"},
                  "tasks":[
                    {"id":0,"state":"FAILED","worker_id":"worker:8083","trace":"password=supersecret\njava.lang.RuntimeException: boom"}
                  ]
                }
                """),
                "/connectors/payments/config" => Json(HttpStatusCode.OK, """
                {
                  "connector.class":"example.PaymentsConnector",
                  "api.key":"supersecret-key",
                  "db.password":"supersecret-password",
                  "topics":"payments",
                  "connection.url":"jdbc:postgresql://user:secret@db.example/payments"
                }
                """),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            };
        });

        using var adapter = CreateConnect(handler);

        var info = await adapter.GetClusterInfoAsync("cluster-a", Operation(), CancellationToken.None);
        var list = await adapter.ListConnectorsAsync("cluster-a", Operation(), CancellationToken.None);
        var detail = await adapter.GetConnectorAsync("cluster-a", "payments", Operation(), CancellationToken.None);

        Assert.True(info.IsSuccess, info.Failure?.SafeMessage);
        Assert.Equal("cluster-k", info.Value!.KafkaClusterId);
        Assert.True(list.IsSuccess, list.Failure?.SafeMessage);
        Assert.Single(list.Value!);
        Assert.True(detail.IsSuccess, detail.Failure?.SafeMessage);
        Assert.Equal("[REDACTED]", detail.Value!.SafeConfiguration["api.key"]);
        Assert.Equal("[REDACTED]", detail.Value.SafeConfiguration["db.password"]);
        Assert.Equal("payments", detail.Value.SafeConfiguration["topics"]);
        Assert.Equal("[REDACTED]", detail.Value.SafeConfiguration["connection.url"]);
        Assert.DoesNotContain("supersecret", detail.Value.Tasks[0].SafeTrace, StringComparison.OrdinalIgnoreCase);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Connect_redirect_is_rejected_without_following_target()
    {
        var handler = new StubHandler(_ =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
            response.Headers.Location = new Uri("https://attacker.example/");
            return response;
        });

        using var adapter = CreateConnect(handler);
        var result = await adapter.ListConnectorsAsync("cluster-a", Operation(), CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(ReadViewFailureCategory.InvalidResponse, result.Failure!.Category);
        Assert.Equal("upstream_redirect_rejected", result.Failure.Code);
        Assert.Equal(1, handler.CallCount);
    }

    [Fact]
    public async Task Ksql_uses_only_info_and_health_get_endpoints()
    {
        var handler = new StubHandler(request =>
            request.RequestUri!.AbsolutePath switch
            {
                "/info" => Json(HttpStatusCode.OK, """
                {"KsqlServerInfo":{"version":"0.29.0","kafkaClusterId":"kafka-1","ksqlServiceId":"default_"}}
                """),
                "/healthcheck" => Json(HttpStatusCode.OK, """{"isHealthy":true}"""),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            });

        using var adapter = CreateKsql(handler);
        var info = await adapter.GetServerInfoAsync("cluster-a", Operation(), CancellationToken.None);

        Assert.True(info.IsSuccess, info.Failure?.SafeMessage);
        Assert.Equal("0.29.0", info.Value!.Version);
        Assert.Equal("kafka-1", info.Value.KafkaClusterId);
        Assert.Equal("Healthy", info.Value.State);
        Assert.Equal(new[] { "/info", "/healthcheck" }, handler.Paths);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Ksql_metadata_does_not_execute_show_or_any_statement()
    {
        var handler = new StubHandler(_ =>
            throw new InvalidOperationException("No HTTP request should be made for unsupported metadata discovery."));

        using var adapter = CreateKsql(handler);
        var result = await adapter.ListMetadataAsync("cluster-a", Operation(), CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(ReadViewFailureCategory.Unsupported, result.Failure!.Category);
        Assert.Equal("ksql_metadata_requires_statement_execution", result.Failure.Code);
        Assert.Equal(0, handler.CallCount);
    }

    [Fact]
    public void Connect_secret_key_classifier_covers_common_credentials()
    {
        string[] secretKeys =
        [
            "password",
            "db.password",
            "api.key",
            "apiKey",
            "api_key",
            "access.key",
            "accessKey",
            "client.secret",
            "oauth.token",
            "sasl.jaas.config",
            "private.key",
            "credential",
        ];

        Assert.All(secretKeys, key => Assert.True(KafkaConnectReadAdapter.IsSecretKey(key)));
        Assert.False(KafkaConnectReadAdapter.IsSecretKey("connector.class"));
        Assert.False(KafkaConnectReadAdapter.IsSecretKey("tasks.max"));
    }

    [Fact]
    public void Connect_trace_sanitization_removes_credential_assignments()
    {
        var safe = KafkaConnectReadAdapter.SanitizeTrace(
            "password=hunter2\ntoken:abc123\nclientSecret=\"correct horse battery staple\"\nAuthorization: Bearer xyz\njava.lang.Exception: failed");

        Assert.NotNull(safe);
        Assert.DoesNotContain("hunter2", safe, StringComparison.Ordinal);
        Assert.DoesNotContain("abc123", safe, StringComparison.Ordinal);
        Assert.DoesNotContain("Bearer xyz", safe, StringComparison.Ordinal);
        Assert.DoesNotContain("correct horse battery staple", safe, StringComparison.Ordinal);
        Assert.Contains("java.lang.Exception: failed", safe, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Connect_bulkhead_limits_concurrency_per_cluster()
    {
        var handler = new BlockingHandler("""{"version":"4.1.0","commit":"abc","kafka_cluster_id":"cluster-k"}""");
        using var adapter = new KafkaConnectReadAdapter(
            [
                new ClusterProfile(
                    "cluster-a",
                    ["localhost:9092"],
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null,
                    null,
                    new KafkaConnectProfile("https://connect.example/", null, null))
            ],
            new SecretResolver(),
            _ => handler,
            maxConcurrencyPerCluster: 1);

        var first = adapter.GetClusterInfoAsync("cluster-a", Operation(), CancellationToken.None);
        await handler.FirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var second = adapter.GetClusterInfoAsync("cluster-a", Operation(), CancellationToken.None);
        await Task.Delay(75);

        Assert.Equal(1, handler.CallCount);
        Assert.Equal(1, handler.MaxActive);

        handler.Release();
        var results = await Task.WhenAll(first, second);

        Assert.All(results, result => Assert.True(result.IsSuccess, result.Failure?.SafeMessage));
        Assert.Equal(2, handler.CallCount);
        Assert.Equal(1, handler.MaxActive);
    }

    [Fact]
    public async Task Ksql_bulkhead_is_independent_and_limits_concurrency()
    {
        var handler = new BlockingHandler("""{"KsqlServerInfo":{"version":"0.29.0","kafkaClusterId":"kafka-1"}}""");
        using var adapter = new KsqlDbMetadataReadAdapter(
            [
                new ClusterProfile(
                    "cluster-a",
                    ["localhost:9092"],
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null,
                    null,
                    null,
                    new KsqlDbProfile("https://ksql.example/", null, null))
            ],
            new SecretResolver(),
            _ => handler,
            maxConcurrencyPerCluster: 1);

        var first = adapter.GetServerInfoAsync("cluster-a", Operation(), CancellationToken.None);
        await handler.FirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var second = adapter.GetServerInfoAsync("cluster-a", Operation(), CancellationToken.None);
        await Task.Delay(75);

        Assert.Equal(1, handler.CallCount);
        Assert.Equal(1, handler.MaxActive);

        handler.Release();
        var results = await Task.WhenAll(first, second);

        Assert.All(results, result => Assert.True(result.IsSuccess, result.Failure?.SafeMessage));
        Assert.Equal(4, handler.CallCount);
        Assert.Equal(1, handler.MaxActive);
    }

    private static KafkaConnectReadAdapter CreateConnect(HttpMessageHandler handler) =>
        new(
            [
                new ClusterProfile(
                    "cluster-a",
                    ["localhost:9092"],
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null,
                    null,
                    new KafkaConnectProfile("https://connect.example/", null, null))
            ],
            new SecretResolver(),
            _ => handler);

    private static KsqlDbMetadataReadAdapter CreateKsql(HttpMessageHandler handler) =>
        new(
            [
                new ClusterProfile(
                    "cluster-a",
                    ["localhost:9092"],
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null,
                    null,
                    null,
                    new KsqlDbProfile("https://ksql.example/", null, null))
            ],
            new SecretResolver(),
            _ => handler);

    private static ReadViewOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10), maxItems: 100, maxResponseBytes: 1024 * 1024);

    private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
        new(status)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

    private sealed class BlockingHandler : HttpMessageHandler
    {
        private readonly string _infoBody;
        private readonly TaskCompletionSource _firstEntered =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _active;
        private int _maxActive;
        private int _callCount;

        public BlockingHandler(string infoBody)
        {
            _infoBody = infoBody;
        }

        public TaskCompletionSource FirstEntered => _firstEntered;
        public int CallCount => Volatile.Read(ref _callCount);
        public int MaxActive => Volatile.Read(ref _maxActive);

        public void Release() => _release.TrySetResult();

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _callCount);
            var active = Interlocked.Increment(ref _active);
            UpdateMax(active);
            _firstEntered.TrySetResult();

            try
            {
                await _release.Task.WaitAsync(cancellationToken);

                var body = request.RequestUri!.AbsolutePath == "/healthcheck"
                    ? """{"isHealthy":true}"""
                    : _infoBody;

                return Json(HttpStatusCode.OK, body);
            }
            finally
            {
                Interlocked.Decrement(ref _active);
            }
        }

        private void UpdateMax(int active)
        {
            while (true)
            {
                var current = Volatile.Read(ref _maxActive);
                if (active <= current ||
                    Interlocked.CompareExchange(ref _maxActive, active, current) == current)
                {
                    return;
                }
            }
        }
    }

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _response;

        public StubHandler(Func<HttpRequestMessage, HttpResponseMessage> response)
        {
            _response = response;
        }

        public int CallCount { get; private set; }
        public List<HttpMethod> Methods { get; } = [];
        public List<string> Paths { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            CallCount++;
            Methods.Add(request.Method);
            Paths.Add(request.RequestUri!.AbsolutePath);
            return Task.FromResult(_response(request));
        }
    }
}
