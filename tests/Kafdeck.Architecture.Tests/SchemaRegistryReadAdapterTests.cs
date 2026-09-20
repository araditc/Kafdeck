using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.SchemaRegistry;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class SchemaRegistryReadAdapterTests
{
    [Fact]
    public async Task Schema_type_defaults_to_Avro_when_registry_omits_schemaType()
    {
        var handler = new StubHandler(_ =>
            Json(HttpStatusCode.OK, """
            {
              "schema": "{\"type\":\"record\",\"name\":\"A\",\"fields\":[]}"
            }
            """));

        using var adapter = CreateAdapter("cluster-a", handler);

        var result = await adapter.GetSchemaByIdAsync(
            "cluster-a",
            12,
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);
        Assert.Equal(12, result.Value.Id);
        Assert.Equal(RecordSchemaFormat.Avro, result.Value.Format);
        Assert.Empty(result.Value.References);
        Assert.Equal(1, handler.CallCount);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Json_and_Protobuf_schema_types_are_normalized()
    {
        var handler = new StubHandler(request =>
        {
            var path = request.RequestUri!.AbsolutePath;

            return path.EndsWith("/31", StringComparison.Ordinal)
                ? Json(HttpStatusCode.OK, """{"schemaType":"JSON","schema":"{\"type\":\"object\"}"}""")
                : Json(HttpStatusCode.OK, """{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message A { string value = 1; }"}""");
        });

        using var adapter = CreateAdapter("cluster-a", handler);

        var json = await adapter.GetSchemaByIdAsync("cluster-a", 31, Operation(), CancellationToken.None);
        var proto = await adapter.GetSchemaByIdAsync("cluster-a", 32, Operation(), CancellationToken.None);

        Assert.True(json.IsSuccess);
        Assert.Equal(RecordSchemaFormat.JsonSchema, json.Value!.Format);

        Assert.True(proto.IsSuccess);
        Assert.Equal(RecordSchemaFormat.Protobuf, proto.Value!.Format);
    }


    [Fact]
    public async Task Protobuf_serialized_lookup_preserves_initial_schema_type()
    {
        var descriptor = new FileDescriptorProto
        {
            Name = "record.proto",
            Package = "test",
            Syntax = "proto3",
        };
        descriptor.MessageType.Add(new DescriptorProto { Name = "Record" });
        var serialized = Convert.ToBase64String(descriptor.ToByteArray());

        var handler = new StubHandler(request =>
        {
            if (request.RequestUri!.Query.Contains("format=serialized", StringComparison.Ordinal))
            {
                // Intentionally omit schemaType to prove the initial type is retained.
                return Json(HttpStatusCode.OK, JsonSerializer.Serialize(new { schema = serialized }));
            }

            return Json(HttpStatusCode.OK, """
            {
              "schemaType":"PROTOBUF",
              "schema":"syntax = \"proto3\"; message Record {}"
            }
            """);
        });

        using var adapter = CreateAdapter("cluster-a", handler);

        var result = await adapter.GetSchemaByIdAsync(
            "cluster-a",
            33,
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(RecordSchemaFormat.Protobuf, result.Value!.Format);
        Assert.Equal(serialized, result.Value.SchemaText);
        Assert.Equal(2, handler.CallCount);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Avro_references_trigger_resolved_read_only_lookup()
    {
        var handler = new StubHandler(request =>
        {
            if (request.RequestUri!.Query.Contains("format=resolved", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """
                {
                  "schemaType":"AVRO",
                  "schema":"{\"type\":\"record\",\"name\":\"Resolved\",\"fields\":[]}",
                  "references":[{"name":"shared.avsc","subject":"shared-value","version":1}]
                }
                """);
            }

            return Json(HttpStatusCode.OK, """
            {
              "schemaType":"AVRO",
              "schema":"{\"type\":\"record\",\"name\":\"Root\",\"fields\":[]}",
              "references":[{"name":"shared.avsc","subject":"shared-value","version":1}]
            }
            """);
        });

        using var adapter = CreateAdapter("cluster-a", handler);

        var result = await adapter.GetSchemaByIdAsync("cluster-a", 44, Operation(), CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Contains("Resolved", result.Value!.SchemaText, StringComparison.Ordinal);
        Assert.Equal(2, handler.CallCount);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Cache_is_bounded_by_cluster_and_schema_identity()
    {
        var firstHandler = new StubHandler(_ =>
            Json(HttpStatusCode.OK, """{"schemaType":"JSON","schema":"{\"type\":\"object\",\"title\":\"A\"}"}"""));

        var secondHandler = new StubHandler(_ =>
            Json(HttpStatusCode.OK, """{"schemaType":"JSON","schema":"{\"type\":\"object\",\"title\":\"B\"}"}"""));

        var profiles = new[]
        {
            Profile("cluster-a", "http://registry-a/"),
            Profile("cluster-b", "http://registry-b/"),
        };

        var handlers = new Dictionary<string, HttpMessageHandler>(StringComparer.Ordinal)
        {
            ["cluster-a"] = firstHandler,
            ["cluster-b"] = secondHandler,
        };

        using var adapter = new ConfluentSchemaRegistryReadAdapter(
            profiles,
            new SecretResolver(),
            profile => handlers[profile.Id]);

        var first = await adapter.GetSchemaByIdAsync("cluster-a", 7, Operation(), CancellationToken.None);
        var firstAgain = await adapter.GetSchemaByIdAsync("cluster-a", 7, Operation(), CancellationToken.None);
        var second = await adapter.GetSchemaByIdAsync("cluster-b", 7, Operation(), CancellationToken.None);

        Assert.True(first.IsSuccess);
        Assert.True(firstAgain.IsSuccess);
        Assert.True(second.IsSuccess);
        Assert.Equal(1, firstHandler.CallCount);
        Assert.Equal(1, secondHandler.CallCount);
        Assert.Contains("A", first.Value!.SchemaText, StringComparison.Ordinal);
        Assert.Contains("B", second.Value!.SchemaText, StringComparison.Ordinal);
    }


    [Fact]
    public async Task Subject_version_lookup_is_get_only_and_uses_response_schema_id()
    {
        Uri? observed = null;
        var handler = new StubHandler(request =>
        {
            observed = request.RequestUri;
            return Json(HttpStatusCode.OK, """
            {
              "id":55,
              "schemaType":"JSON",
              "schema":"{\"type\":\"object\"}"
            }
            """);
        });

        using var adapter = CreateAdapter("cluster-a", handler);

        var result = await adapter.GetSchemaBySubjectVersionAsync(
            "cluster-a",
            "orders-value",
            3,
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(55, result.Value!.Id);
        Assert.Equal(RecordSchemaFormat.JsonSchema, result.Value.Format);
        Assert.NotNull(observed);
        Assert.EndsWith(
            "/subjects/orders-value/versions/3",
            observed!.AbsolutePath,
            StringComparison.Ordinal);
        Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
    }

    [Fact]
    public async Task Basic_auth_uses_secret_references_and_never_mutates_registry()
    {
        var usernameName = $"KAFDECK_SR_USER_{Guid.NewGuid():N}";
        var passwordName = $"KAFDECK_SR_PASS_{Guid.NewGuid():N}";
        Environment.SetEnvironmentVariable(usernameName, "registry-user");
        Environment.SetEnvironmentVariable(passwordName, "registry-password");

        try
        {
            AuthenticationHeaderValue? observed = null;
            var handler = new StubHandler(request =>
            {
                observed = request.Headers.Authorization;
                return Json(HttpStatusCode.OK, """{"schemaType":"JSON","schema":"{\"type\":\"object\"}"}""");
            });

            var profile = new ClusterProfile(
                "cluster-a",
                ["localhost:9092"],
                KafkaSecurityProtocol.Plaintext,
                null,
                null,
                new SchemaRegistryProfile(
                    "https://registry.example/",
                    SecretReference.Parse($"env:{usernameName}"),
                    SecretReference.Parse($"env:{passwordName}")));

            using var adapter = new ConfluentSchemaRegistryReadAdapter(
                [profile],
                new SecretResolver(),
                _ => handler);

            var result = await adapter.GetSchemaByIdAsync(
                "cluster-a",
                5,
                Operation(),
                CancellationToken.None);

            Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
            Assert.NotNull(observed);
            Assert.Equal("Basic", observed!.Scheme);
            Assert.Equal(
                Convert.ToBase64String(Encoding.UTF8.GetBytes("registry-user:registry-password")),
                observed.Parameter);
            Assert.All(handler.Methods, method => Assert.Equal(HttpMethod.Get, method));
        }
        finally
        {
            Environment.SetEnvironmentVariable(usernameName, null);
            Environment.SetEnvironmentVariable(passwordName, null);
        }
    }

    [Fact]
    public async Task Registry_errors_map_to_safe_categories()
    {
        var deniedHandler = new StubHandler(_ => Json(HttpStatusCode.Forbidden, """{"message":"secret-detail"}"""));
        using var denied = CreateAdapter("cluster-a", deniedHandler);

        var deniedResult = await denied.GetSchemaByIdAsync("cluster-a", 9, Operation(), CancellationToken.None);

        Assert.False(deniedResult.IsSuccess);
        Assert.Equal(RecordSchemaFailureCategory.Unauthorized, deniedResult.Failure!.Category);
        Assert.DoesNotContain("secret-detail", deniedResult.Failure.SafeMessage, StringComparison.Ordinal);

        var missingHandler = new StubHandler(_ => Json(HttpStatusCode.NotFound, """{"error_code":40403}"""));
        using var missing = CreateAdapter("cluster-a", missingHandler);

        var missingResult = await missing.GetSchemaByIdAsync("cluster-a", 9, Operation(), CancellationToken.None);

        Assert.False(missingResult.IsSuccess);
        Assert.Equal(RecordSchemaFailureCategory.SchemaNotFound, missingResult.Failure!.Category);
    }

    private static ConfluentSchemaRegistryReadAdapter CreateAdapter(
        string clusterId,
        HttpMessageHandler handler) =>
        new(
            [Profile(clusterId, "http://registry.local/")],
            new SecretResolver(),
            _ => handler);

    private static ClusterProfile Profile(string id, string registryUrl) =>
        new(
            id,
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            new SchemaRegistryProfile(registryUrl, null, null));

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10));

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

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            CallCount++;
            Methods.Add(request.Method);
            return Task.FromResult(_response(request));
        }
    }
}
