using System.Text;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68SubscriptionObservationBridgeTests
{
    private static CandidateSubscriptionManifest Desired(string deployment = "production-a") =>
        CandidateSubscriptionManifest.ParseJson(Encoding.UTF8.GetBytes(
            $$$"""
            {
              "apiVersion":"kafdeck.io/v0.8-proposal",
              "kind":"NotificationSubscription",
              "metadata":{"deploymentRef":"{{{deployment}}}","name":"ops-lag","ownerRef":"team-ops"},
              "spec":{"destinationRef":"ops-notifier","state":"Active","eventClasses":["Operational"],"eventTypes":["consumer.lag.alert"]}
            }
            """));

    [Fact]
    public async Task Missing_is_never_inferred_without_current_read_rights_and_ownership_proof()
    {
        var path = Path.Combine(Path.GetTempPath(), "w68-observation-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            var store = new AdoNotificationRoutingStore(new SqliteNotificationDeliveryDbConnectionFactory(path));
            await store.InitializeAsync();
            var manifest = Desired();
            var blocked = Bridge(store, read: false, own: true);
            Assert.Equal(CandidateDriftStatus.Inaccessible, await blocked.InspectAsync(manifest));
            var unowned = Bridge(store, read: true, own: false);
            Assert.Equal(CandidateDriftStatus.Conflict, await unowned.InspectAsync(manifest));
            var authorized = Bridge(store, read: true, own: true);
            Assert.Equal(CandidateDriftStatus.Missing, await authorized.InspectAsync(manifest));
            Assert.Equal(CandidateDriftStatus.Inaccessible, await authorized.InspectAsync(Desired("another-deployment")));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(Path.GetDirectoryName(path)!, Path.GetFileName(path) + "*"))
                File.Delete(file);
        }
    }

    [Fact]
    public async Task Existing_subscription_requires_old_destination_rights_and_never_treats_revocation_as_missing()
    {
        var path = Path.Combine(Path.GetTempPath(), "w68-observation-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            var store = new AdoNotificationRoutingStore(new SqliteNotificationDeliveryDbConnectionFactory(path));
            await store.InitializeAsync();
            var currentDefinition = new NotificationSubscriptionDefinition(
                "ops-lag", "existing-notifier", [NotificationEventClass.Operational], ["consumer.lag.alert"]);
            await store.CreateSubscriptionAsync(currentDefinition, NotificationSubscriptionState.Active, DateTimeOffset.UtcNow);

            var desired = Desired();
            var denied = new W68CandidateSubscriptionObservationBridge(store, _ => true,
                destination => destination != "existing-notifier", _ => true,
                (_, _, _) => ValueTask.FromResult(true), "production-a");
            Assert.Equal(CandidateDriftStatus.Inaccessible, await denied.InspectAsync(desired));

            var approved = Bridge(store, read: true, own: true);
            Assert.Equal(CandidateDriftStatus.OutOfSync, await approved.InspectAsync(desired));

            var conflicting = Bridge(store, read: true, own: false);
            Assert.Equal(CandidateDriftStatus.Conflict, await conflicting.InspectAsync(desired));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in Directory.GetFiles(Path.GetDirectoryName(path)!, Path.GetFileName(path) + "*"))
                File.Delete(file);
        }
    }

    private static W68CandidateSubscriptionObservationBridge Bridge(
        INotificationRoutingStore store, bool read, bool own) =>
        new(store, _ => read, _ => read, _ => read,
            (_, _, _) => ValueTask.FromResult(own), "production-a");
}
