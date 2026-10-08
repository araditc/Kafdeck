using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationReadApiTests
{
    [Fact]
    public void Notification_observation_is_disabled_without_explicit_configuration()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            Array.Empty<ClusterProfile>());

        Assert.Null(options.Notifications);
        KafdeckConfigurationValidator.ValidateAndThrow(options);
    }

    [Fact]
    public void Notification_observation_does_not_accept_legacy_unidentified_access()
    {
        var options = Options(
            AccessMode.Local,
            new NotificationReadOptions(
                true,
                NotificationPersistenceProvider.Sqlite,
                NotificationExecutionMode.Standalone,
                Path.Combine(Path.GetTempPath(), "kafdeck-notify-test.db"),
                null));

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
        Assert.Contains(
            "requires OIDC operator access mode",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Notification_observation_rejects_unsafe_provider_configuration()
    {
        var options = Options(
            AccessMode.Oidc,
            new NotificationReadOptions(
                true,
                NotificationPersistenceProvider.Sqlite,
                NotificationExecutionMode.HighAvailability,
                "relative-path.db",
                null));

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));
        Assert.Contains("standalone-only", exception.Message);
        Assert.Contains("absolute database path", exception.Message);
    }

    [Fact]
    public void No_notification_mutation_or_raw_data_route_is_mapped()
    {
        var builder = WebApplication.CreateBuilder();
        var app = builder.Build();
        app.MapKafdeckV08NotificationReads();

        var mapped = ((IEndpointRouteBuilder)app)
            .DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .Where(route =>
                route.RoutePattern.RawText?
                    .Contains("/notifications/", StringComparison.Ordinal) == true)
            .Select(route => new
            {
                Method = route.Metadata.GetMetadata<HttpMethodMetadata>()?
                    .HttpMethods.Single(),
                Path = route.RoutePattern.RawText,
            })
            .ToArray();

        Assert.Equal(3, mapped.Length);
        Assert.All(mapped, route => Assert.Equal("GET", route.Method));
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/subscriptions");
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/subscriptions/{subscriptionId}");
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/deliveries/{notificationId:guid}/{destinationId}");
    }

    [Fact]
    public void Subscription_data_only_contains_governed_metadata()
    {
        var value = new NotificationSubscriptionSnapshot(
            new NotificationSubscriptionDefinition(
                "ops-sub",
                "ops-destination",
                [NotificationEventClass.Operational],
                ["consumer-lag"]),
            NotificationSubscriptionState.Active,
            4,
            DateTimeOffset.UtcNow);
        var dto = NotificationSubscriptionData.From(value);

        Assert.Equal("ops-sub", dto.SubscriptionId);
        Assert.Equal(["consumer-lag"], dto.EventTypes);
        var json = JsonSerializer.Serialize(dto);
        foreach (var forbidden in new[]
                 {
                     "Credential", "Secret", "Token", "RawValue",
                     "Headers", "Endpoint", "ConfiguredUrl",
                 })
        {
            Assert.DoesNotContain(forbidden, json, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Delivery_evidence_omits_secrets_and_provider_request_ids()
    {
        var when = DateTimeOffset.UtcNow;
        var value = new NotificationDeliveryRecord(
            new NotificationDeliverySnapshot(
                Guid.NewGuid(),
                "ops-destination",
                new string('a', 64),
                NotificationDeliveryState.Pending,
                0,
                when,
                routedProfileRevisionFingerprint: new string('b', 64)),
            1,
            when);

        var evidence = NotificationDeliveryEvidenceData.From(value);
        Assert.True(evidence.ProfileRevisionBound);
        var json = JsonSerializer.Serialize(evidence);
        foreach (var forbidden in new[]
                 {
                     "Credential", "Secret", "Token",
                     "ProviderRequestId", "PayloadFingerprint",
                     "RoutedProfileRevisionFingerprint",
                     "RawValue", "Headers", "Url",
                 })
        {
            Assert.DoesNotContain(forbidden, json, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Notification_authorization_is_distinct_from_legacy_permissions()
    {
        Assert.NotEqual(AuthorizationAction.NotificationRead, AuthorizationAction.TopicRead);
        Assert.NotEqual(AuthorizationAction.NotificationRead, AuthorizationAction.DataQualityRead);
        Assert.NotEqual(AuthorizationAction.NotificationRead, AuthorizationAction.NotificationManage);
        Assert.True(Enum.IsDefined(AuthorizationAction.NotificationRead));
    }

    private static KafdeckOptions Options(
        AccessMode accessMode,
        NotificationReadOptions notification) =>
        new(
            accessMode == AccessMode.Oidc
                ? new DeploymentOptions(
                    "http://127.0.0.1:8080",
                    null,
                    AccessMode.Oidc,
                    new OidcProfile(
                        "http://127.0.0.1:5555",
                        "kafdeck-tests",
                        null,
                        null,
                        ["openid"]))
                : new DeploymentOptions("http://127.0.0.1:8080", null),
            Array.Empty<ClusterProfile>(),
            Notifications: notification);
}
