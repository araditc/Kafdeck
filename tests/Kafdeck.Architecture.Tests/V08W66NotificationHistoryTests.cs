using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationHistoryTests
{
    [Fact]
    public async Task SQLite_history_is_destination_scoped_bounded_and_seekable()
    {
        var path = Path.Combine(Path.GetTempPath(),
            $"kafdeck-w66-history-{Guid.NewGuid():N}.db");
        try
        {
            var store = new AdoNotificationDeliveryStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            await store.InitializeAsync();

            var at = new DateTimeOffset(2026, 10, 8, 12, 0, 0, TimeSpan.Zero);
            var first = Guid.Parse("00000000-0000-0000-0000-000000000001");
            var second = Guid.Parse("00000000-0000-0000-0000-000000000002");
            var third = Guid.Parse("00000000-0000-0000-0000-000000000003");
            var hidden = Guid.Parse("00000000-0000-0000-0000-000000000004");

            async Task Insert(Guid id, string destination, DateTimeOffset created)
            {
                await store.CreateOrGetAsync(new NotificationDeliverySnapshot(
                    id, destination,
                    new string('a', 64),
                    NotificationDeliveryState.Pending,
                    attemptCount: 0,
                    createdAtUtc: created,
                    routedProfileRevisionFingerprint: new string('b', 64)),
                    created);
            }

            await Insert(first, "public-destination", at);
            await Insert(second, "public-destination", at);
            await Insert(third, "public-destination", at.AddMilliseconds(1));
            await Insert(hidden, "restricted-destination", at);

            var firstPage = await store.ListByDestinationAsync(
                new NotificationDestinationDeliveryQuery(
                    "public-destination", maxResults: 2));
            Assert.Equal(2, firstPage.Items.Count);
            Assert.Equal([first, second],
                firstPage.Items.Select(r => r.Snapshot.NotificationId).ToArray());
            Assert.True(firstPage.Truncated);
            Assert.NotNull(firstPage.Next);
            Assert.Equal(second, firstPage.Next.NotificationId);

            var next = await store.ListByDestinationAsync(
                new NotificationDestinationDeliveryQuery(
                    "public-destination", 2, firstPage.Next));
            Assert.Single(next.Items);
            Assert.Equal(third, next.Items[0].Snapshot.NotificationId);
            Assert.False(next.Truncated);
            Assert.Null(next.Next);

            var serialized = JsonSerializer.Serialize(
                new NotificationDeliveryHistoryListData(
                    next.Items.Select(NotificationDeliveryEvidenceData.From).ToArray(),
                    next.Truncated, next.Next?.CreatedAtUtc, next.Next?.NotificationId));
            Assert.DoesNotContain(hidden.ToString("D"), serialized, StringComparison.Ordinal);
            Assert.DoesNotContain("PayloadFingerprint", serialized, StringComparison.Ordinal);
            Assert.DoesNotContain("RoutedProfileRevisionFingerprint", serialized, StringComparison.Ordinal);
            Assert.DoesNotContain("ProviderRequestId", serialized, StringComparison.Ordinal);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in new[] { path, path + "-wal", path + "-shm" })
                if (File.Exists(file)) File.Delete(file);
        }
    }

    [Fact]
    public void Invalid_history_page_size_or_cursor_is_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new NotificationDestinationDeliveryQuery("ops", maxResults: 201));
        Assert.Throws<ArgumentException>(() =>
            new NotificationDestinationDeliveryQuery("https://untrusted.example"));
        Assert.Throws<ArgumentException>(() =>
            new NotificationDestinationDeliveryCursor(DateTimeOffset.UtcNow, Guid.Empty));
    }
}
