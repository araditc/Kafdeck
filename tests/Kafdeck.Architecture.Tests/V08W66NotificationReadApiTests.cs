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
    public void Notification_startup_preflights_delivery_before_routing_migration()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null &&
               !File.Exists(Path.Combine(root.FullName, "Kafdeck.slnx")))
        {
            root = root.Parent;
        }

        Assert.NotNull(root);
        var program = File.ReadAllText(Path.Combine(
            root!.FullName, "src", "backend", "Kafdeck.Api", "Program.cs"));
        var section = program.IndexOf(
            "if (notificationReadOptions?.Enabled == true)",
            program.IndexOf("var app = builder.Build();", StringComparison.Ordinal),
            StringComparison.Ordinal);
        Assert.True(section >= 0);
        var delivery = program.IndexOf(
            "GetRequiredService<INotificationDeliveryStore>()",
            section, StringComparison.Ordinal);
        var routing = program.IndexOf(
            "GetRequiredService<INotificationRoutingStore>()",
            section, StringComparison.Ordinal);
        Assert.True(delivery > section && routing > delivery,
            "Notification delivery preflight must reject unresolved v1 queues before any routing migration.");
    }

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
    public void Enabled_observation_registers_only_immutable_destination_catalog_not_outbound_worker()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null &&
               !File.Exists(Path.Combine(root.FullName, "Kafdeck.slnx")))
            root = root.Parent;
        Assert.NotNull(root);
        var source = File.ReadAllText(Path.Combine(
            root!.FullName, "src", "backend", "Kafdeck.Api", "Program.cs"));
        Assert.Contains("AddSingleton<INotificationDestinationProfileCatalog>",
            source, StringComparison.Ordinal);
        Assert.Contains("new ConfiguredNotificationDestinationProfileCatalog(",
            source, StringComparison.Ordinal);
        Assert.Contains("notificationReadOptions.DestinationProfiles",
            source, StringComparison.Ordinal);
        Assert.DoesNotContain("AddHostedService<NotificationDeliveryWorker>",
            source, StringComparison.Ordinal);
        Assert.Contains("AddSingleton<INotificationCredentialResolver>",
            source, StringComparison.Ordinal);
        Assert.Contains("ConfiguredNotificationCredentialResolver(",
            source, StringComparison.Ordinal);
        Assert.Contains("notificationReadOptions.CredentialBindings",
            source, StringComparison.Ordinal);
        Assert.Contains("profile.RevisionFingerprint",
            source, StringComparison.Ordinal);
        Assert.DoesNotContain("AddSingleton<NotificationProviderDeliveryDispatcher>",
            source, StringComparison.Ordinal);
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

        Assert.Equal(6, mapped.Length);
        Assert.All(mapped, route => Assert.Equal("GET", route.Method));
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/subscriptions");
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/subscriptions/{subscriptionId}");
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/deliveries/{notificationId:guid}/{destinationId}");
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/destinations/{destinationId}/deliveries");
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/deliveries/history");
        Assert.Contains(mapped, route =>
            route.Path == "/api/v1/notifications/deliveries/evidence/{notificationId:guid}");
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
    public void Authorization_filtered_pagination_never_emits_a_hidden_identity()
    {
        var when = DateTimeOffset.UtcNow;
        NotificationSubscriptionSnapshot Subscription(
            string id, string destination) =>
            new(
                new NotificationSubscriptionDefinition(
                    id,
                    destination,
                    [NotificationEventClass.Operational],
                    ["consumer-lag"]),
                NotificationSubscriptionState.Active,
                1,
                when);

        var permitted = Subscription("a-visible", "ops-visible");
        var hidden = Subscription("b-secret", "ops-secret");
        var page = new NotificationSubscriptionPage(
            [permitted, hidden],
            truncated: true,
            nextSubscriptionId: hidden.Definition.SubscriptionId);

        var projected = NotificationSubscriptionReadProjection.Project(
            page,
            snapshot =>
                snapshot.Definition.SubscriptionId == "a-visible");

        Assert.Single(projected.Items);
        Assert.Equal("a-visible", projected.Items[0].SubscriptionId);
        Assert.True(projected.AuthorizationFiltered);
        Assert.True(projected.Truncated);
        Assert.True(projected.ContinuationRestricted);
        Assert.Null(projected.NextSubscriptionId);
        Assert.DoesNotContain(
            "b-secret",
            JsonSerializer.Serialize(projected),
            StringComparison.Ordinal);

        var allPermitted = NotificationSubscriptionReadProjection.Project(
            page,
            _ => true);
        Assert.False(allPermitted.AuthorizationFiltered);
        Assert.False(allPermitted.ContinuationRestricted);
        Assert.Equal("b-secret", allPermitted.NextSubscriptionId);
    }

    [Fact]
    public void Empty_visible_page_does_not_expose_hidden_continuation()
    {
        var when = DateTimeOffset.UtcNow;
        var hidden = new NotificationSubscriptionSnapshot(
            new NotificationSubscriptionDefinition(
                "secret-only",
                "secret-destination",
                [NotificationEventClass.Security]),
            NotificationSubscriptionState.Active,
            1,
            when);
        var page = new NotificationSubscriptionPage(
            [hidden], true, hidden.Definition.SubscriptionId);

        var projected = NotificationSubscriptionReadProjection.Project(
            page,
            _ => false);

        Assert.Empty(projected.Items);
        Assert.True(projected.AuthorizationFiltered);
        Assert.True(projected.ContinuationRestricted);
        Assert.Null(projected.NextSubscriptionId);
        Assert.DoesNotContain(
            "secret-only",
            JsonSerializer.Serialize(projected),
            StringComparison.Ordinal);
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
