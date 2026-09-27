using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W55ConnectAutoRestartEndpointContractTests
{
    private const string PolicyRoute =
        "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/restart-policy";

    [Fact]
    public void Auto_restart_status_preview_and_apply_routes_use_expected_methods()
    {
        using var app = BuildApp();

        AssertMethod(app, PolicyRoute, "GET");
        AssertMethod(app, PolicyRoute, "PUT");
        AssertMethod(app, $"{PolicyRoute}/preview", "PUT");
    }

    [Fact]
    public void Auto_restart_state_changing_routes_require_antiforgery()
    {
        using var app = BuildApp();

        foreach (var pattern in new[]
                 {
                     PolicyRoute,
                     $"{PolicyRoute}/preview",
                 })
        {
            var endpoint = FindEndpoint(app, pattern, "PUT");
            var antiforgery =
                endpoint.Metadata.GetMetadata<IAntiforgeryMetadata>();

            Assert.NotNull(antiforgery);
            Assert.True(antiforgery.RequiresValidation);
        }
    }

    private static WebApplication BuildApp()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddKafdeckAntiforgery(
            "https://127.0.0.1:8443");

        Type[] services =
        [
            typeof(KafdeckAuthorizationService),
            typeof(IConnectAutoRestartStateStore),
            typeof(IConnectAutoRestartRuntimePolicyProvider),
            typeof(MutationRequestAuthorizationService),
            typeof(ConnectAutoRestartPolicyPlanner),
            typeof(MutationAdmissionService),
            typeof(IMutationOperationRepository),
            typeof(MutationDispatchService),
        ];

        foreach (var serviceType in services)
        {
            builder.Services.AddSingleton(
                serviceType,
                _ => throw new InvalidOperationException(
                    $"Metadata-only service '{serviceType.Name}' must not be resolved."));
        }

        var app = builder.Build();
        app.MapKafdeckConnectAutoRestartEndpoints();
        return app;
    }

    private static void AssertMethod(
        WebApplication app,
        string pattern,
        string method)
    {
        _ = FindEndpoint(app, pattern, method);
    }

    private static RouteEndpoint FindEndpoint(
        WebApplication app,
        string pattern,
        string method) =>
        ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Single(item =>
                string.Equals(
                    item.RoutePattern.RawText,
                    pattern,
                    StringComparison.Ordinal) &&
                item.Metadata
                    .GetMetadata<HttpMethodMetadata>()!
                    .HttpMethods
                    .Contains(method, StringComparer.Ordinal));
}
