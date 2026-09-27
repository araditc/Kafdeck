using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W54ConnectApiContractTests
{
    [Fact]
    public void Profile_scoped_read_and_plugin_routes_are_registered_with_expected_methods()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddKafdeckAntiforgery("https://127.0.0.1:8443");

        using var app = builder.Build();
        var options = Options();
        app.MapKafdeckV04ReadViews(options);

        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/profiles",
            "GET");
        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}",
            "GET");
        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/plugins",
            "GET");
        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/plugins/{connectorClass}/validate",
            "POST");
        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors",
            "GET");
        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}",
            "GET");
    }

    [Fact]
    public void Oidc_plugin_validation_and_profile_mutation_previews_require_antiforgery()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddKafdeckAntiforgery("https://127.0.0.1:8443");

        using var app = builder.Build();
        var options = Options();

        app.MapKafdeckV04ReadViews(options);
        app.MapKafdeckConnectMutationEndpoints();

        string[] stateChangingRoutes =
        [
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/plugins/{connectorClass}/validate",
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/mutations/create/preview",
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/mutations/update/preview",
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/mutations/control/preview",
            "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/mutations/delete/preview",
        ];

        foreach (var route in stateChangingRoutes)
        {
            var endpoint = FindEndpoint(app, route);
            Assert.Contains(
                "POST",
                endpoint.Metadata.GetMetadata<HttpMethodMetadata>()!.HttpMethods);
            var antiforgery =
                endpoint.Metadata.GetMetadata<IAntiforgeryMetadata>();
            Assert.NotNull(antiforgery);
            Assert.True(antiforgery.RequiresValidation);
        }
    }

    [Fact]
    public void Legacy_default_routes_remain_registered_for_backward_compatibility()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddKafdeckAntiforgery("https://127.0.0.1:8443");

        using var app = builder.Build();
        var options = Options();

        app.MapKafdeckV04ReadViews(options);
        app.MapKafdeckConnectMutationEndpoints();

        AssertMethod(app, "/api/v1/clusters/{clusterId}/connect", "GET");
        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/connectors",
            "GET");
        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}",
            "GET");
        AssertMethod(
            app,
            "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/create/preview",
            "POST");
    }

    private static KafdeckOptions Options()
    {
        var deployment = new DeploymentOptions(
            "https://127.0.0.1:8443",
            null,
            AccessMode.Oidc,
            new OidcProfile(
                "https://idp.example",
                "kafdeck",
                null,
                "groups",
                new[] { "openid", "profile" }));

        return new KafdeckOptions(
            deployment,
            Array.Empty<ClusterProfile>());
    }

    private static void AssertMethod(
        WebApplication app,
        string pattern,
        string method)
    {
        var endpoint = FindEndpoint(app, pattern);
        var methods =
            endpoint.Metadata.GetMetadata<HttpMethodMetadata>()!.HttpMethods;

        Assert.Contains(method, methods);
    }

    private static RouteEndpoint FindEndpoint(
        WebApplication app,
        string pattern) =>
        ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Single(item => string.Equals(
                item.RoutePattern.RawText,
                pattern,
                StringComparison.Ordinal));
}
