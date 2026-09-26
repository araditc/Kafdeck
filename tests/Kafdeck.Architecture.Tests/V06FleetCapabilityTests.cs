using Kafdeck.Api;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06FleetCapabilityTests
{
    [Fact]
    public void Catalog_keeps_pinned_client_gaps_explicit_and_never_promotes_them_to_supported()
    {
        Assert.Equal(
            FleetCapabilityState.Unsupported,
            FleetCapabilityCatalog.Require("client-quotas").State);
        Assert.Equal(
            FleetCapabilityState.Unsupported,
            FleetCapabilityCatalog.Require("partition-reassignment").State);
        Assert.Equal(
            FleetCapabilityState.Blocked,
            FleetCapabilityCatalog.Require("broker-maintenance").State);
        Assert.Equal(
            FleetCapabilityState.Blocked,
            FleetCapabilityCatalog.Require("managed-replication").State);

        Assert.Contains(
            FleetCapabilityCatalog.Require("client-quotas").Evidence,
            item => item.Contains("DescribeClientQuotasAsync absent", StringComparison.Ordinal));
        Assert.Contains(
            FleetCapabilityCatalog.Require("partition-reassignment").Evidence,
            item => item.Contains("AlterPartitionReassignmentsAsync absent", StringComparison.Ordinal));
    }

    [Fact]
    public void Admitted_provider_contracts_are_not_confused_with_unregistered_runtime_activation()
    {
        foreach (var id in new[]
                 {
                     "acl-administration",
                     "scram-administration",
                     "dynamic-configuration",
                     "finite-cluster-transfer",
                     "preferred-leader-election",
                 })
        {
            Assert.Equal(
                FleetCapabilityState.Blocked,
                FleetCapabilityCatalog.Require(id).State);
        }

        Assert.Contains(
            "no W42 handler registration",
            string.Join(
                ' ',
                FleetCapabilityCatalog.Require("acl-administration").Evidence),
            StringComparison.Ordinal);
        Assert.Contains(
            "no W43 handler registration",
            string.Join(
                ' ',
                FleetCapabilityCatalog.Require("scram-administration").Evidence),
            StringComparison.Ordinal);
        Assert.Contains(
            "no W44 handler registration",
            string.Join(
                ' ',
                FleetCapabilityCatalog.Require("dynamic-configuration").Evidence),
            StringComparison.Ordinal);
    }

    [Fact]
    public void Fleet_capability_runtime_surface_is_get_only()
    {
        var builder = WebApplication.CreateBuilder();
        var app = builder.Build();
        app.MapKafdeckFleetCapabilities();

        var endpoint = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Single(item => string.Equals(
                item.RoutePattern.RawText,
                "/api/v1/fleet/capabilities",
                StringComparison.Ordinal));

        Assert.Equal(
            new[] { "GET" },
            endpoint.Metadata.GetMetadata<HttpMethodMetadata>()!.HttpMethods);
        Assert.Equal(
            "v06-fleet-capabilities",
            endpoint.Metadata.GetMetadata<RouteNameMetadata>()!.RouteName);
    }

    [Fact]
    public void Capability_catalog_contains_no_execution_tunnel()
    {
        var text = string.Join(
            "\n",
            FleetCapabilityCatalog.All.SelectMany(item =>
                new[] { item.Id, item.DisplayName, item.Reason }.Concat(item.Evidence)));

        Assert.DoesNotContain("shell execute", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("adminclient proxy", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("raw protocol executor", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("sidecar bypass", text, StringComparison.OrdinalIgnoreCase);
    }
}
