using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Clusters;
using Kafdeck.Modules.Topics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ApiContractTests
{
    [Fact]
    public void Accepted_v01_product_contract_is_exactly_read_only()
    {
        var expected = new[]
        {
            "/api/v1/system/info",
            "/api/v1/system/health",
            "/api/v1/clusters",
            "/api/v1/clusters/{clusterId}",
            "/api/v1/clusters/{clusterId}/health",
            "/api/v1/clusters/{clusterId}/capabilities",
            "/api/v1/clusters/{clusterId}/brokers",
            "/api/v1/clusters/{clusterId}/brokers/{brokerId}",
            "/api/v1/clusters/{clusterId}/brokers/{brokerId}/configuration",
            "/api/v1/clusters/{clusterId}/topics",
            "/api/v1/clusters/{clusterId}/topics/{topicName}",
            "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions",
            "/api/v1/clusters/{clusterId}/topics/{topicName}/configuration",
        };

        Assert.Equal(expected, V01ApiContract.ProductRoutes.Select(route => route.Pattern));
        Assert.All(V01ApiContract.ProductRoutes, route => Assert.Equal("GET", route.Method));
        Assert.DoesNotContain(V01ApiContract.ProductRoutes, route =>
            route.Method is "POST" or "PUT" or "PATCH" or "DELETE");
    }

    [Fact]
    public void Runtime_endpoint_mapping_matches_the_accepted_product_contract()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddSingleton<ClusterExplorerService>(_ => throw new NotSupportedException());
        builder.Services.AddSingleton<TopicExplorerService>(_ => throw new NotSupportedException());
        builder.Services.AddSingleton<IKafkaAdministrationPort>(_ => throw new NotSupportedException());
        builder.Services.AddSingleton<KafkaSnapshotCoordinator>(_ => throw new NotSupportedException());
        builder.Services.AddSingleton(new KafdeckAuthorizationService(
            new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0", null), Array.Empty<ClusterProfile>()),
            new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition(
                Array.Empty<AuthorizationRoleDefinition>(),
                Array.Empty<AuthorizationSubjectBindingDefinition>(),
                Array.Empty<AuthorizationGroupBindingDefinition>())))));

        var app = builder.Build();
        app.MapKafdeckV01(new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:0", null),
            Array.Empty<ClusterProfile>()));

        var routeBuilder = (IEndpointRouteBuilder)app;
        var productEndpoints = routeBuilder.DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Where(endpoint => endpoint.RoutePattern.RawText?.StartsWith(V01ApiContract.BasePath, StringComparison.Ordinal) == true)
            .Select(endpoint => new ApiRouteDefinition(
                endpoint.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods.Single() ?? string.Empty,
                NormalizeRuntimePattern(endpoint.RoutePattern.RawText!),
                endpoint.Metadata.GetMetadata<RouteNameMetadata>()?.RouteName ?? string.Empty))
            .OrderBy(route => route.Pattern, StringComparer.Ordinal)
            .ToArray();

        var expected = V01ApiContract.ProductRoutes
            .OrderBy(route => route.Pattern, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(expected, productEndpoints);
    }

    [Fact]
    public void OpenApi_document_matches_the_registered_read_only_contract()
    {
        using var document = JsonDocument.Parse(V01ApiContract.OpenApiJson);
        Assert.Equal("3.1.0", document.RootElement.GetProperty("openapi").GetString());

        var paths = document.RootElement.GetProperty("paths")
            .EnumerateObject()
            .ToDictionary(property => property.Name, property => property.Value, StringComparer.Ordinal);

        Assert.Equal(V01ApiContract.ProductRoutes.Count, paths.Count);

        foreach (var route in V01ApiContract.ProductRoutes)
        {
            Assert.True(paths.TryGetValue(route.Pattern, out var pathItem), $"OpenAPI is missing {route.Pattern}.");
            var operations = pathItem.EnumerateObject().Select(property => property.Name).ToArray();
            Assert.Contains("get", operations);
            Assert.DoesNotContain(operations, operation =>
                operation is "post" or "put" or "patch" or "delete");
        }
    }

    [Fact]
    public void Checked_in_OpenApi_document_is_reproducible_from_the_runtime_contract()
    {
        var root = FindRepositoryRoot();
        var checkedIn = File.ReadAllText(Path.Combine(root, "docs", "api", "openapi-v0.1.json"));

        Assert.Equal(NormalizeText(V01ApiContract.OpenApiJson), NormalizeText(checkedIn));
    }

    [Fact]
    public void Sensitive_Kafka_configuration_values_are_always_redacted_from_API_models()
    {
        const string secret = "must-never-be-serialized";
        var mapped = ApiConfigurationMapper.Create(new KafkaConfigurationEntry(
            "sasl.password",
            secret,
            IsSensitive: true,
            IsReadOnly: false,
            Source: "DynamicBrokerConfig"));

        Assert.True(mapped.IsSensitive);
        Assert.Null(mapped.Value);
        Assert.DoesNotContain(secret, JsonSerializer.Serialize(mapped), StringComparison.Ordinal);
    }

    [Fact]
    public void Kafka_failures_map_to_safe_RFC9457_problem_types()
    {
        var mapped = ApiProblemMapper.FromKafka(new KafkaFailure(
            KafkaFailureCategory.Unauthorized,
            "kafka_topic_authorization_failed",
            "Kafka denied the requested operation.",
            IsRetryable: false));

        Assert.Equal(403, mapped.Status);
        Assert.Equal("urn:kafdeck:problem:authorization-denied", mapped.Type);
        Assert.Equal("Kafka denied the requested operation.", mapped.Detail);
        Assert.Equal("kafka_topic_authorization_failed", mapped.Code);
    }

    [Fact]
    public void Observation_envelope_preserves_stale_state_and_original_observed_time()
    {
        var observedAt = new DateTimeOffset(2026, 9, 17, 10, 0, 0, TimeSpan.Zero);
        var observation = new ObservationMetadata(
            observedAt,
            observedAt + TimeSpan.FromSeconds(5),
            observedAt + TimeSpan.FromMinutes(1),
            ObservationSource.StaleSnapshot);

        var mapped = ApiObservationMapper.Create(
            observation,
            partial: true,
            nowUtc: observedAt + TimeSpan.FromSeconds(12));

        Assert.Equal(observedAt, mapped.ObservedAt);
        Assert.Equal("stale", mapped.Freshness);
        Assert.Equal(12000, mapped.CacheAgeMs);
        Assert.True(mapped.Partial);
    }

    [Fact]
    public void Telemetry_dimensions_are_bounded_and_do_not_accept_resource_identifiers()
    {
        Assert.Equal("v01-topics-list", ApiTelemetry.NormalizeRouteName("v01-topics-list"));
        Assert.Equal("other", ApiTelemetry.NormalizeRouteName("payments-prod/customer-secret-topic"));
        Assert.Equal("GET", ApiTelemetry.NormalizeMethod("get"));
        Assert.Equal("OTHER", ApiTelemetry.NormalizeMethod("POST"));
        Assert.Equal("2xx", ApiTelemetry.NormalizeStatusClass(200));
        Assert.Equal("5xx", ApiTelemetry.NormalizeStatusClass(503));
    }

    private static string NormalizeRuntimePattern(string pattern) =>
        pattern.Replace("{brokerId:int}", "{brokerId}", StringComparison.Ordinal);

    private static string NormalizeText(string value) =>
        value.Replace("\r\n", "\n", StringComparison.Ordinal).Trim();

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Unable to locate Kafdeck repository root from test output directory.");
    }
}
