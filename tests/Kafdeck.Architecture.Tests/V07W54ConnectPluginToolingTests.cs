using System.Net;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W54ConnectPluginToolingTests
{
    [Fact]
    public async Task Plugin_discovery_is_profile_scoped_and_get_only()
    {
        var defaultHandler = new RecordingHandler(
            request => request.RequestUri!.AbsolutePath switch
            {
                "/connector-plugins" => Json(
                    HttpStatusCode.OK,
                    """
                    [
                      {
                        "class":"org.example.DefaultSink",
                        "type":"sink",
                        "version":"1.0.0"
                      }
                    ]
                    """),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            });
        var analyticsHandler = new RecordingHandler(
            request => request.RequestUri!.AbsolutePath switch
            {
                "/connector-plugins" => Json(
                    HttpStatusCode.OK,
                    """
                    [
                      {
                        "class":"org.example.AnalyticsSink",
                        "type":"sink",
                        "version":"2.0.0"
                      }
                    ]
                    """),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            });

        using var adapter = CreateAdapter(
            defaultHandler,
            analyticsHandler);

        var result = await adapter.ListPluginsAsync(
            "prod",
            "analytics",
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        var plugin = Assert.Single(result.Value!);
        Assert.Equal("org.example.AnalyticsSink", plugin.Class);
        Assert.Equal("sink", plugin.Type);
        Assert.Equal("2.0.0", plugin.Version);

        Assert.Empty(defaultHandler.Requests);
        Assert.Equal(
            new[] { ("GET", "/connector-plugins") },
            analyticsHandler.Requests);
    }

    [Fact]
    public async Task Plugin_validation_uses_fixed_typed_route_and_never_projects_config_value()
    {
        const string secret = "provider-secret-value";
        var handler = new RecordingHandler(
            request =>
            {
                Assert.Equal(HttpMethod.Put, request.Method);
                Assert.Equal(
                    "/connector-plugins/org.example.AnalyticsSink/config/validate",
                    request.RequestUri!.AbsolutePath);

                var body = request.Content!
                    .ReadAsStringAsync()
                    .GetAwaiter()
                    .GetResult();
                Assert.Contains(secret, body, StringComparison.Ordinal);

                return Json(
                    HttpStatusCode.OK,
                    """
                    {
                      "name":"org.example.AnalyticsSink",
                      "error_count":1,
                      "configs":[
                        {
                          "definition":{
                            "name":"api.password",
                            "type":"PASSWORD",
                            "required":true
                          },
                          "value":{
                            "name":"api.password",
                            "value":"__SECRET__",
                            "recommended_values":[],
                            "errors":["credential rejected"],
                            "visible":false
                          }
                        },
                        {
                          "definition":{
                            "name":"topics",
                            "type":"LIST",
                            "required":true
                          },
                          "value":{
                            "name":"topics",
                            "value":"orders",
                            "recommended_values":["orders","payments"],
                            "errors":[],
                            "visible":true
                          }
                        }
                      ]
                    }
                    """.Replace(
                        "__SECRET__",
                        secret,
                        StringComparison.Ordinal));
            });

        using var adapter = CreateAdapter(
            new RecordingHandler(),
            handler);

        var result = await adapter.ValidateConfigurationAsync(
            "prod",
            "analytics",
            "org.example.AnalyticsSink",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["connector.class"] = "org.example.AnalyticsSink",
                ["api.password"] = secret,
                ["topics"] = "orders",
            },
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.NotNull(result.Value);
        Assert.Equal(1, result.Value!.ErrorCount);
        Assert.Equal("org.example.AnalyticsSink", result.Value.ConnectorClass);
        Assert.Equal(2, result.Value.Fields.Count);
        Assert.Contains(
            result.Value.Fields,
            field =>
                field.Name == "api.password" &&
                field.Required &&
                field.Errors.SequenceEqual(
                    new[] { "[REDACTED_PROVIDER_VALIDATION_ERROR]" }) &&
                field.RecommendedValues.Count == 0);
        Assert.Contains(
            result.Value.Fields,
            field =>
                field.Name == "topics" &&
                field.RecommendedValues.Count == 0);

        var serialized = JsonSerializer.Serialize(result.Value);
        Assert.DoesNotContain(
            secret,
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "credential rejected",
            serialized,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "payments",
            serialized,
            StringComparison.Ordinal);
        Assert.Equal(
            new[]
            {
                (
                    "PUT",
                    "/connector-plugins/org.example.AnalyticsSink/config/validate"),
            },
            handler.Requests);
    }

    [Fact]
    public async Task Plugin_validation_redirect_is_rejected_without_following_target()
    {
        var handler = new RecordingHandler(
            _ =>
            {
                var response =
                    new HttpResponseMessage(
                        HttpStatusCode.TemporaryRedirect);
                response.Headers.Location =
                    new Uri("https://attacker.example/");
                return response;
            });

        using var adapter = CreateAdapter(
            new RecordingHandler(),
            handler);

        var result = await adapter.ValidateConfigurationAsync(
            "prod",
            "analytics",
            "org.example.AnalyticsSink",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["connector.class"] = "org.example.AnalyticsSink",
            },
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.InvalidResponse,
            result.Failure!.Category);
        Assert.Equal(
            "connect_plugin_validation_redirect_rejected",
            result.Failure.Code);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public void Validation_projection_ignores_provider_echoed_value()
    {
        const string secret = "do-not-project";
        using var document = JsonDocument.Parse(
            """
            {
              "error_count":0,
              "configs":[
                {
                  "definition":{
                    "name":"password",
                    "type":"PASSWORD",
                    "required":false
                  },
                  "value":{
                    "value":"__SECRET__",
                    "errors":[],
                    "recommended_values":[]
                  }
                }
              ]
            }
            """.Replace(
                "__SECRET__",
                secret,
                StringComparison.Ordinal));

        var projected =
            KafkaConnectReadAdapter.ProjectPluginValidation(
                "org.example.Sink",
                document.RootElement,
                maxItems: 10);

        Assert.DoesNotContain(
            secret,
            JsonSerializer.Serialize(projected),
            StringComparison.Ordinal);
    }

    private static KafkaConnectReadAdapter CreateAdapter(
        HttpMessageHandler defaultHandler,
        HttpMessageHandler analyticsHandler)
    {
        var cluster = new ClusterProfile(
            "prod",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            null,
            null,
            new[]
            {
                new KafkaConnectProfile(
                    "https://connect-default.example:8083",
                    null,
                    null,
                    Id: "default"),
                new KafkaConnectProfile(
                    "https://connect-analytics.example:8083",
                    null,
                    null,
                    Id: "analytics"),
            });

        return new KafkaConnectReadAdapter(
            new[] { cluster },
            new SecretResolver(),
            (_, profile) =>
                profile.Id == "default"
                    ? defaultHandler
                    : analyticsHandler);
    }

    private static ReadViewOperationContext Operation() =>
        new(
            DateTimeOffset.UtcNow.AddSeconds(10),
            maxItems: 100,
            maxResponseBytes: 1024 * 1024);

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

    private sealed class RecordingHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _response;

        public RecordingHandler()
            : this(_ => Json(HttpStatusCode.NotFound, "{}"))
        {
        }

        public RecordingHandler(
            Func<HttpRequestMessage, HttpResponseMessage> response)
        {
            _response = response;
        }

        public List<(string Method, string Path)> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Requests.Add(
                (
                    request.Method.Method,
                    request.RequestUri!.AbsolutePath));

            return Task.FromResult(_response(request));
        }
    }
}
