using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationManagementApiTests
{
    [Fact]
    public void Management_does_not_activate_without_explicit_opt_in()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            Array.Empty<ClusterProfile>(),
            Notifications: new NotificationReadOptions(
                false, NotificationPersistenceProvider.Sqlite,
                NotificationExecutionMode.Standalone, null, null,
                ManagementEnabled: true));

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
        Assert.Contains("cannot be enabled while observation is disabled",
            exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Management_is_independently_disabled_by_default()
    {
        var options = new NotificationReadOptions(
            true, NotificationPersistenceProvider.Sqlite,
            NotificationExecutionMode.Standalone,
            Path.Combine(Path.GetTempPath(), "w66-notification-management.db"), null);

        Assert.False(options.ManagementEnabled);
    }

    [Fact]
    public void Management_maps_only_its_explicit_subscription_put_route()
    {
        var builder = WebApplication.CreateBuilder();
        var app = builder.Build();
        app.MapKafdeckV08NotificationManagement();

        var mapped = ((IEndpointRouteBuilder)app)
            .DataSources.SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Where(route => route.RoutePattern.RawText?.Contains(
                "/notifications/", StringComparison.Ordinal) == true).ToArray();

        Assert.Single(mapped);
        Assert.Equal("/api/v1/notifications/subscriptions/{subscriptionId}",
            mapped[0].RoutePattern.RawText);
        Assert.Equal("PUT", Assert.Single(mapped[0].Metadata
            .GetMetadata<HttpMethodMetadata>()!.HttpMethods));
    }

    [Fact]
    public void Write_contract_accepts_only_bounded_domain_identifiers_not_provider_secrets()
    {
        var request = new NotificationSubscriptionWriteRequest(
            "ops-destination", NotificationSubscriptionState.Active,
            [NotificationEventClass.Operational], ["consumer-lag"], null);
        Assert.Null(request.ExpectedRevision);
        Assert.Equal("ops-destination", request.DestinationId);
        Assert.Throws<ArgumentException>(() => new NotificationSubscriptionDefinition(
            "ops-sub", "https://unapproved.example/path",
            request.EventClasses, request.EventTypes));

        var fields = typeof(NotificationSubscriptionWriteRequest)
            .GetProperties().Select(x => x.Name).ToArray();
        foreach (var forbidden in new[] { "Url", "Endpoint", "Credential", "Secret", "Headers", "ProviderPayload" })
            Assert.DoesNotContain(fields, x => x.Contains(forbidden, StringComparison.OrdinalIgnoreCase));
    }
}
