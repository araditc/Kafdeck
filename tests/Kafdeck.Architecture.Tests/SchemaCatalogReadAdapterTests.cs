using System.Net;
using System.Text;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Modules.Schemas;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class SchemaCatalogReadAdapterTests
{
    [Fact]
    public async Task Catalog_operations_are_get_only_and_project_subject_versions()
    {
        var handler = new StubHandler(request =>
        {
            var path = request.RequestUri!.PathAndQuery;
            if (path == "/subjects")
            {
                return Json(HttpStatusCode.OK, """["orders-value"]""");
            }

            if (path == "/subjects/orders-value/versions")
            {
                return Json(HttpStatusCode.OK, """[1,2]""");
            }

            if (path.EndsWith("/versions/1", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """{"id":11,"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"OrderV1\",\"fields\":[]}","references":[]}""");
            }

            return Json(HttpStatusCode.OK, """{"id":12,"schemaType":"AVRO","schema":"{\"type\":\"record\",\"name\":\"OrderV2\",\"fields\":[]}","references":[{"name":"common.avsc","subject":"common","version":1}]}""");
        });

        using var adapter = CreateAdapter(handler);
        var operation = Operation();

        var subjects = await adapter.ListSubjectsAsync("cluster-a", operation, CancellationToken.None);
        var versions = await adapter.ListVersionsAsync("cluster-a", "orders-value", operation, CancellationToken.None);

        Assert.True(subjects.IsSuccess, subjects.Failure?.SafeMessage);
        Assert.Single(subjects.Value!);
        Assert.Equal("orders-value", subjects.Value![0].Subject);
        Assert.True(versions.IsSuccess, versions.Failure?.SafeMessage);
        Assert.Equal(2, versions.Value!.Count);
        Assert.Equal(12, versions.Value[1].SchemaId);
        Assert.Single(versions.Value[1].References);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Compatibility_inheritance_is_determined_by_subject_then_global_gets()
    {
        var handler = new StubHandler(request =>
            request.RequestUri!.PathAndQuery switch
            {
                "/config/orders-value" => Json(HttpStatusCode.NotFound, "{}"),
                "/config" => Json(HttpStatusCode.OK, """{"compatibilityLevel":"BACKWARD_TRANSITIVE"}"""),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            });

        using var adapter = CreateAdapter(handler);
        var result = await adapter.GetCompatibilityAsync(
            "cluster-a",
            "orders-value",
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(Kafdeck.Core.Schemas.SchemaCompatibilityMode.BackwardTransitive, result.Value!.Mode);
        Assert.True(result.Value.IsInherited);
        Assert.Equal(new[] { "/config/orders-value", "/config" }, handler.Paths);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Subject_specific_compatibility_is_not_reported_as_inherited()
    {
        var handler = new StubHandler(request =>
            request.RequestUri!.PathAndQuery == "/config/orders-value"
                ? Json(HttpStatusCode.OK, """{"compatibilityLevel":"FULL"}""")
                : Json(HttpStatusCode.InternalServerError, "{}"));

        using var adapter = CreateAdapter(handler);
        var result = await adapter.GetCompatibilityAsync(
            "cluster-a",
            "orders-value",
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(Kafdeck.Core.Schemas.SchemaCompatibilityMode.Full, result.Value!.Mode);
        Assert.False(result.Value.IsInherited);
        Assert.Equal(new[] { "/config/orders-value" }, handler.Paths);
    }

    [Fact]
    public async Task Redirects_are_rejected_without_following_new_origin()
    {
        var handler = new StubHandler(_ =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.Found);
            response.Headers.Location = new Uri("https://attacker.example/steal");
            return response;
        });

        using var adapter = CreateAdapter(handler);
        var result = await adapter.ListSubjectsAsync("cluster-a", Operation(), CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(ReadViewFailureCategory.InvalidResponse, result.Failure!.Category);
        Assert.Equal("schema_registry_redirect_rejected", result.Failure.Code);
        Assert.Equal(1, handler.CallCount);
    }

    [Fact]
    public async Task Subject_lists_are_bounded_by_operation_budget()
    {
        var handler = new StubHandler(_ =>
            Json(HttpStatusCode.OK, """["a","b","c"]"""));

        using var adapter = CreateAdapter(handler);
        var result = await adapter.ListSubjectsAsync(
            "cluster-a",
            new ReadViewOperationContext(DateTimeOffset.UtcNow.AddSeconds(10), maxItems: 2),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(ReadViewFailureCategory.ResponseTooLarge, result.Failure!.Category);
    }

    [Fact]
    public void Schema_diff_is_local_deterministic_and_bounded()
    {
        var service = new SchemaDiffService();
        var left = new RecordSchemaDocument(
            1,
            RecordSchemaFormat.JsonSchema,
            "{\n  \"type\": \"object\",\n  \"title\": \"A\"\n}",
            []);
        var right = left with
        {
            Id = 2,
            SchemaText = "{\n  \"type\": \"object\",\n  \"title\": \"B\"\n}",
        };

        var first = service.Compare(left, right);
        var second = service.Compare(left, right);

        Assert.False(first.IsEqual);
        Assert.Equal(first.IsEqual, second.IsEqual);
        Assert.Equal(first.Hunks.Count, second.Hunks.Count);
        Assert.Equal(first.Hunks[0].LeftStartLine, second.Hunks[0].LeftStartLine);
        Assert.Equal(first.Hunks[0].RightStartLine, second.Hunks[0].RightStartLine);
        Assert.Equal(first.Hunks[0].RemovedLines, second.Hunks[0].RemovedLines);
        Assert.Equal(first.Hunks[0].AddedLines, second.Hunks[0].AddedLines);
        Assert.Single(first.Hunks);
        Assert.Contains(first.Hunks[0].RemovedLines, line => line.Contains("\"A\"", StringComparison.Ordinal));
        Assert.Contains(first.Hunks[0].AddedLines, line => line.Contains("\"B\"", StringComparison.Ordinal));

        var huge = left with { SchemaText = new string('x', SchemaDiffService.MaxSchemaCharacters + 1) };
        Assert.Throws<ArgumentOutOfRangeException>(() => service.Compare(huge, right));
    }

    private static ConfluentSchemaCatalogReadAdapter CreateAdapter(HttpMessageHandler handler) =>
        new(
            [
                new ClusterProfile(
                    "cluster-a",
                    ["localhost:9092"],
                    KafkaSecurityProtocol.Plaintext,
                    null,
                    null,
                    new SchemaRegistryProfile("https://registry.example/", null, null))
            ],
            new SecretResolver(),
            _ => handler);

    private static ReadViewOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10), 100, 1024 * 1024);

    private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
        new(status)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

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
            Paths.Add(request.RequestUri!.PathAndQuery);
            return Task.FromResult(_response(request));
        }
    }
}
