using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Data.Sqlite;
using System.Text.Json;
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
    [Theory]
    [InlineData(5, 5, true)]
    [InlineData(6, 6, true)]
    [InlineData(5, 517, true)]
    [InlineData(6, 262, true)]
    [InlineData(1, 1, false)]
    [InlineData(19, 2067, false)]
    public void SQLite_write_contention_classification_is_narrow(
        int primary, int extended, bool expected)
    {
        var error = new SqliteException("synthetic regression", primary, extended);
        Assert.Equal(expected, KafdeckNotificationManagementEndpoints.IsSqliteContention(error));
    }

    [Fact]
    public void Management_receipt_never_leaks_filters_or_destination_without_read_grant()
    {
        var snapshot = new NotificationSubscriptionSnapshot(
            new NotificationSubscriptionDefinition(
                "ops-sub", "secret-destination",
                [NotificationEventClass.Security], ["authorization.denied"]),
            NotificationSubscriptionState.Active, 7, DateTimeOffset.UtcNow);

        var receipt = NotificationSubscriptionWriteReceipt.From(snapshot);
        var serialized = JsonSerializer.Serialize(receipt);

        Assert.Equal("ops-sub", receipt.SubscriptionId);
        Assert.Equal(7, receipt.Revision);
        Assert.DoesNotContain("secret-destination", serialized, StringComparison.Ordinal);
        Assert.DoesNotContain("authorization.denied", serialized, StringComparison.Ordinal);
        Assert.DoesNotContain("EventClasses", serialized, StringComparison.Ordinal);
        Assert.DoesNotContain("DestinationId", serialized, StringComparison.Ordinal);
    }

    [Fact]
    public void Startup_diagnostic_reflects_real_notification_management_activation_without_secret_values()
    {
        var notification = new NotificationReadOptions(
            Enabled: true,
            Provider: NotificationPersistenceProvider.PostgreSql,
            ExecutionMode: NotificationExecutionMode.HighAvailability,
            SqliteDatabasePath: null,
            ConnectionString: null,
            ManagementEnabled: true);
        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            Array.Empty<ClusterProfile>(),
            Notifications: notification);
        var snapshot = SafeConfigurationDiagnostics.Create(options);

        Assert.True(snapshot.NotificationsEnabled);
        Assert.True(snapshot.NotificationManagementEnabled);
        Assert.Equal(NotificationPersistenceProvider.PostgreSql, snapshot.NotificationPersistenceProvider);
        Assert.Equal(NotificationExecutionMode.HighAvailability, snapshot.NotificationExecutionMode);
        var json = System.Text.Json.JsonSerializer.Serialize(snapshot);
        Assert.DoesNotContain("password", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("connectionString", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Disabled_management_is_reported_accurately()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            Array.Empty<ClusterProfile>());
        var status = SafeConfigurationDiagnostics.Create(options);
        Assert.False(status.NotificationsEnabled);
        Assert.False(status.NotificationManagementEnabled);
    }

}
