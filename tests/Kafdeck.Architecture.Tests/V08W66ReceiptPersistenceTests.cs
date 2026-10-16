using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66ReceiptPersistenceTests
{
    [Fact]
    public async Task V2_pending_work_upgrades_without_retargeting_and_persists_only_safe_receipt_hash()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafdeck-receipt-v3-{Guid.NewGuid():N}.db");
        try
        {
            var connections = new SqliteNotificationDeliveryDbConnectionFactory(path);
            var store = new AdoNotificationDeliveryStore(connections);
            await store.InitializeAsync();
            var now = DateTimeOffset.UtcNow;
            var id = Guid.NewGuid();
            var initial = await store.CreateOrGetAsync(new NotificationDeliverySnapshot(id, "receipt-target",
                new string('a', 64), NotificationDeliveryState.Pending, 0, now,
                routedProfileRevisionFingerprint: new string('b', 64)), now);
            await using (var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path }.ConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "ALTER TABLE kafdeck_notification_deliveries DROP COLUMN provider_receipt_fingerprint; UPDATE kafdeck_schema_info SET schema_version = 2 WHERE component = 'notification-delivery'";
                await command.ExecuteNonQueryAsync();
            }
            await store.InitializeAsync();
            var pending = await store.GetAsync(id, "receipt-target");
            Assert.Equal(initial.Revision, pending!.Revision);
            Assert.Equal(initial.Snapshot.RoutedProfileRevisionFingerprint, pending.Snapshot.RoutedProfileRevisionFingerprint);
            Assert.Equal(NotificationDeliveryState.Pending, pending.Snapshot.State);
            Assert.Null(pending.Snapshot.ProviderReceiptFingerprint);
            var cycle = await new NotificationDeliveryWorker(store, new ReceiptDispatcher()).RunDueCycleAsync();
            Assert.Equal(1, cycle.Delivered);
            var reopened = new AdoNotificationDeliveryStore(connections);
            await reopened.InitializeAsync();
            var delivered = await reopened.GetAsync(id, "receipt-target");
            Assert.Equal(new string('c', 64), delivered!.Snapshot.ProviderReceiptFingerprint);
            Assert.Null(delivered.Snapshot.ProviderRequestId);
            Assert.Equal(NotificationDeliveryState.Delivered, delivered.Snapshot.State);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-shm", "-wal" }) File.Delete(path + suffix);
        }
    }

    [Fact]
    public void Receipt_metadata_rejects_raw_or_nonterminal_material()
    {
        Assert.Throws<ArgumentException>(() => new NotificationDeliverySnapshot(Guid.NewGuid(), "receipt-target",
            new string('a', 64), NotificationDeliveryState.Pending, 0, DateTimeOffset.UtcNow,
            providerReceiptFingerprint: new string('c', 64)));
        Assert.Throws<ArgumentException>(() => new NotificationDeliverySnapshot(Guid.NewGuid(), "receipt-target",
            new string('a', 64), NotificationDeliveryState.Delivered, 1, DateTimeOffset.UtcNow,
            providerReceiptFingerprint: "raw-provider-data"));
    }

    private sealed class ReceiptDispatcher : INotificationDeliveryDispatcher
    {
        public Task<NotificationDeliveryDispatchResult> DispatchAsync(NotificationDeliveryRecord claimedDelivery,
            CancellationToken cancellationToken) => Task.FromResult(new NotificationDeliveryDispatchResult(
                NotificationDeliveryDispatchOutcome.Delivered, new string('c', 64)));
    }
}
