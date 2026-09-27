using System.Net;
using System.Text;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W59KsqlQueryTests
{
    [Theory]
    [InlineData("SELECT * FROM ORDERS LIMIT 10;")]
    [InlineData("/* CREATE STREAM nope */ SELECT 'INSERT' AS value FROM ORDERS LIMIT 1;")]
    [InlineData("-- harmless CREATE\nSELECT * FROM ORDERS LIMIT 1;")]
    public void Classifier_admits_only_single_read_selects(string statement)
    {
        var result = KsqlStatementClassifier.Classify(statement);

        Assert.True(result.IsAllowed, result.SafeMessage);
        Assert.Equal(
            KsqlStatementAdmissionOutcome.Allowed,
            result.Outcome);
    }

    [Theory]
    [InlineData("CREATE STREAM X AS SELECT * FROM ORDERS;")]
    [InlineData("INSERT INTO X SELECT * FROM ORDERS;")]
    [InlineData("TERMINATE QUERY q1;")]
    [InlineData("SELECT * FROM ORDERS; SELECT * FROM USERS;")]
    [InlineData("SELECT * FROM ORDERS; /* comment */ SELECT * FROM USERS")]
    [InlineData("SELECT * FROM ORDERS WHERE ID = '${id}';")]
    [InlineData("SELECT 'unterminated FROM ORDERS;")]
    public void Classifier_fails_closed_for_mutating_ambiguous_or_multi_statement_input(
        string statement)
    {
        var result = KsqlStatementClassifier.Classify(statement);

        Assert.False(result.IsAllowed);
        Assert.NotEqual(
            KsqlStatementAdmissionOutcome.Allowed,
            result.Outcome);
    }

    [Fact]
    public async Task Rejected_statement_performs_zero_provider_io()
    {
        var handler = new StubHandler(_ =>
            throw new InvalidOperationException(
                "Rejected statements must not reach the provider."));

        using var adapter = CreateAdapter(handler);

        var result = await adapter.ExecuteQueryAsync(
            "prod",
            "DROP STREAM ORDERS;",
            KsqlQueryLimits.Default,
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ReadViewFailureCategory.Unsupported,
            result.Failure!.Category);
        Assert.Equal(0, handler.CallCount);
    }

    [Fact]
    public async Task Query_stream_is_fixed_path_and_rows_are_bounded()
    {
        var response =
            """
            {"queryId":"q1","columnNames":["ID","NAME"],"columnTypes":["BIGINT","STRING"]}
            [1,"one"]
            [2,"two"]
            [3,"three"]
            """;

        var handler = new StubHandler(_ =>
            Json(HttpStatusCode.OK, response));

        using var adapter = CreateAdapter(handler);

        var result = await adapter.ExecuteQueryAsync(
            "prod",
            "SELECT ID, NAME FROM ORDERS EMIT CHANGES;",
            new KsqlQueryLimits(
                MaxRows: 2,
                MaxBytes: 1024 * 1024,
                MaxDuration: TimeSpan.FromSeconds(5)),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);
        Assert.Equal(2, result.Value!.Rows.Count);
        Assert.True(result.Value.Truncated);
        Assert.Equal("row_limit", result.Value.LimitReason);
        Assert.Equal(new[] { "ID", "NAME" }, result.Value.Header.ColumnNames);
        Assert.Equal("/query-stream", Assert.Single(handler.Paths));
        Assert.Equal(HttpMethod.Post, Assert.Single(handler.Methods));
        Assert.Equal(HttpVersion.Version20, Assert.Single(handler.Versions));
        Assert.DoesNotContain(
            "streamsProperties",
            Assert.Single(handler.RequestBodies),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Query_stream_byte_ceiling_returns_explicit_truncation()
    {
        const string header =
            """{"queryId":"q1","columnNames":["VALUE"],"columnTypes":["STRING"]}""";
        var response =
            header + "\n" +
            """["abcdefghijklmnopqrstuvwxyz"]""" + "\n";

        var handler = new StubHandler(_ =>
            Json(HttpStatusCode.OK, response));

        using var adapter = CreateAdapter(handler);
        var maxBytes = Encoding.UTF8.GetByteCount(header) + 4;

        var result = await adapter.ExecuteQueryAsync(
            "prod",
            "SELECT VALUE FROM ORDERS EMIT CHANGES;",
            new KsqlQueryLimits(
                MaxRows: 10,
                MaxBytes: maxBytes,
                MaxDuration: TimeSpan.FromSeconds(5)),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);
        Assert.Empty(result.Value!.Rows);
        Assert.True(result.Value.Truncated);
        Assert.Equal("byte_limit", result.Value.LimitReason);
        Assert.Equal(maxBytes, result.Value.ResponseBytes);
    }

    [Fact]
    public async Task Query_concurrency_is_bounded_per_cluster()
    {
        var handler = new BlockingHandler();

        using var adapter = new KsqlDbQueryAdapter(
            new[] { Cluster() },
            new SecretResolver(),
            _ => handler,
            maxConcurrentQueriesPerCluster: 1);

        var first = adapter.ExecuteQueryAsync(
            "prod",
            "SELECT * FROM ORDERS EMIT CHANGES;",
            new KsqlQueryLimits(
                10,
                1024 * 1024,
                TimeSpan.FromSeconds(5)),
            CancellationToken.None);

        await handler.FirstEntered.Task.WaitAsync(
            TimeSpan.FromSeconds(2));

        var second = adapter.ExecuteQueryAsync(
            "prod",
            "SELECT * FROM USERS EMIT CHANGES;",
            new KsqlQueryLimits(
                10,
                1024 * 1024,
                TimeSpan.FromSeconds(5)),
            CancellationToken.None);

        await Task.Delay(75);
        Assert.Equal(1, handler.CallCount);
        Assert.Equal(1, handler.MaxActive);

        handler.Release();
        var results = await Task.WhenAll(first, second);

        Assert.All(
            results,
            result => Assert.True(
                result.IsSuccess,
                result.Failure?.SafeMessage));
        Assert.Equal(2, handler.CallCount);
        Assert.Equal(1, handler.MaxActive);
    }

    private static KsqlDbQueryAdapter CreateAdapter(
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
            new KsqlDbProfile(
                "https://ksql.example:8088",
                null,
                null));

    private static HttpResponseMessage Json(
        HttpStatusCode status,
        string body) =>
        new(status)
        {
            Content = new StringContent(
                body,
                Encoding.UTF8,
                "application/vnd.ksqlapi.delimited.v1"),
            Version = HttpVersion.Version20,
        };

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _response;

        public StubHandler(
            Func<HttpRequestMessage, HttpResponseMessage> response)
        {
            _response = response;
        }

        public int CallCount { get; private set; }
        public List<string> Paths { get; } = [];
        public List<HttpMethod> Methods { get; } = [];
        public List<Version> Versions { get; } = [];
        public List<string> RequestBodies { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            CallCount++;
            Paths.Add(request.RequestUri!.AbsolutePath);
            Methods.Add(request.Method);
            Versions.Add(request.Version);
            RequestBodies.Add(
                request.Content is null
                    ? string.Empty
                    : await request.Content
                        .ReadAsStringAsync(cancellationToken));

            return _response(request);
        }
    }

    private sealed class BlockingHandler : HttpMessageHandler
    {
        private readonly TaskCompletionSource _firstEntered =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _active;
        private int _maxActive;
        private int _callCount;

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
                return Json(
                    HttpStatusCode.OK,
                    """
                    {"queryId":"q","columnNames":["ID"],"columnTypes":["BIGINT"]}
                    [1]
                    """);
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
                    Interlocked.CompareExchange(
                        ref _maxActive,
                        active,
                        current) == current)
                {
                    return;
                }
            }
        }
    }
}
