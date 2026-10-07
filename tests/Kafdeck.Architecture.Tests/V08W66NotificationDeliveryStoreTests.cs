using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationDeliveryStoreTests
{
    [Fact]
    public async Task Sqlite_store_round_trips_idempotent_delivery_and_cas_transition()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationDeliveryStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var createdAt =
                new DateTimeOffset(
                    2026,
                    10,
                    4,
                    12,
                    0,
                    0,
                    TimeSpan.Zero);
            var pending =
                Snapshot(
                    NotificationDeliveryState.Pending,
                    0,
                    createdAt);

            var created =
                await store.CreateOrGetAsync(
                    pending,
                    createdAt);

            Assert.Equal(
                1,
                created.Revision);

            var repeated =
                await store.CreateOrGetAsync(
                    pending,
                    createdAt.AddMilliseconds(1));

            Assert.Equal(
                1,
                repeated.Revision);

            var inFlight =
                Snapshot(
                    NotificationDeliveryState.InFlight,
                    1,
                    createdAt);

            var claimed =
                await store.ReplaceAsync(
                    inFlight,
                    expectedRevision: 1,
                    createdAt.AddSeconds(1));

            Assert.NotNull(
                claimed);
            Assert.Equal(
                2,
                claimed!.Revision);
            Assert.Equal(
                NotificationDeliveryState.InFlight,
                claimed.Snapshot.State);

            var stale =
                await store.ReplaceAsync(
                    Snapshot(
                        NotificationDeliveryState.Exhausted,
                        1,
                        createdAt),
                    expectedRevision: 1,
                    createdAt.AddSeconds(2));

            Assert.Null(
                stale);
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_store_rejects_revision_timestamp_regression()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationDeliveryStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var createdAt =
                new DateTimeOffset(
                    2026,
                    10,
                    5,
                    6,
                    0,
                    0,
                    TimeSpan.Zero);
            var pending =
                Snapshot(
                    NotificationDeliveryState.Pending,
                    0,
                    createdAt);
            await store.CreateOrGetAsync(
                pending,
                createdAt.AddSeconds(2));

            await Assert.ThrowsAsync<ArgumentException>(
                () =>
                    store.ReplaceAsync(
                        Snapshot(
                            NotificationDeliveryState.InFlight,
                            1,
                            createdAt),
                        expectedRevision: 1,
                        updatedAtUtc:
                            createdAt.AddSeconds(1)));
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_store_rejects_identity_reuse_with_different_payload_fingerprint()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationDeliveryStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var createdAt =
                DateTimeOffset.UtcNow;
            var first =
                Snapshot(
                    NotificationDeliveryState.Pending,
                    0,
                    createdAt);

            await store.CreateOrGetAsync(
                first,
                createdAt);

            var mismatch =
                new NotificationDeliverySnapshot(
                    first.NotificationId,
                    first.DestinationId,
                    new string(
                        'b',
                        64),
                    NotificationDeliveryState.Pending,
                    0,
                    createdAt);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () =>
                    store.CreateOrGetAsync(
                        mismatch,
                        createdAt));
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_store_rejects_identity_reuse_with_different_created_time()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationDeliveryStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var createdAt =
                new DateTimeOffset(
                    2026,
                    10,
                    5,
                    6,
                    0,
                    0,
                    TimeSpan.Zero);
            var first =
                Snapshot(
                    NotificationDeliveryState.Pending,
                    0,
                    createdAt);

            await store.CreateOrGetAsync(
                first,
                createdAt);

            var mismatch =
                new NotificationDeliverySnapshot(
                    first.NotificationId,
                    first.DestinationId,
                    first.PayloadFingerprint,
                    NotificationDeliveryState.Pending,
                    0,
                    createdAt.AddMilliseconds(1));

            await Assert.ThrowsAsync<InvalidOperationException>(
                () =>
                    store.CreateOrGetAsync(
                        mismatch,
                        createdAt.AddMilliseconds(1)));
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_store_lists_due_deliveries_with_stable_bounded_cursor()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationDeliveryStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var createdAt =
                new DateTimeOffset(
                    2026,
                    10,
                    4,
                    12,
                    0,
                    0,
                    TimeSpan.Zero);

            foreach (var id in new[]
                     {
                         "11111111-1111-1111-1111-111111111111",
                         "22222222-2222-2222-2222-222222222222",
                         "33333333-3333-3333-3333-333333333333",
                     })
            {
                await store.CreateOrGetAsync(
                    Snapshot(
                        NotificationDeliveryState.Pending,
                        0,
                        createdAt,
                        notificationId:
                            Guid.Parse(id)),
                    createdAt);
            }

            var first =
                await store.ListDueAsync(
                    new NotificationDeliveryDueQuery(
                        createdAt.AddSeconds(1),
                        maxResults: 2));

            Assert.Equal(
                2,
                first.Items.Count);
            Assert.True(
                first.Truncated);
            Assert.NotNull(
                first.NextCursor);

            var second =
                await store.ListDueAsync(
                    new NotificationDeliveryDueQuery(
                        createdAt.AddSeconds(1),
                        maxResults: 2,
                        after:
                            first.NextCursor));

            Assert.Single(
                second.Items);
            Assert.False(
                second.Truncated);
            Assert.Null(
                second.NextCursor);

            Assert.Equal(
                3,
                first.Items
                    .Concat(second.Items)
                    .Select(
                        record =>
                            record.Snapshot.NotificationId)
                    .Distinct()
                    .Count());
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Failed_delivery_becomes_due_only_at_bounded_retry_time()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationDeliveryStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var createdAt =
                new DateTimeOffset(
                    2026,
                    10,
                    4,
                    12,
                    0,
                    0,
                    TimeSpan.Zero);
            var pending =
                Snapshot(
                    NotificationDeliveryState.Pending,
                    0,
                    createdAt);

            await store.CreateOrGetAsync(
                pending,
                createdAt);

            var inFlight =
                Snapshot(
                    NotificationDeliveryState.InFlight,
                    1,
                    createdAt);
            var claimed =
                await store.ReplaceAsync(
                    inFlight,
                    1,
                    createdAt.AddSeconds(1));
            Assert.NotNull(
                claimed);

            var retryAt =
                createdAt.AddSeconds(10);
            var failed =
                Snapshot(
                    NotificationDeliveryState.Failed,
                    1,
                    createdAt,
                    nextAttemptAtUtc:
                        retryAt,
                    outcomeCode:
                        NotificationDeliveryOutcomeCodes.RetryableFailure);
            var failedRecord =
                await store.ReplaceAsync(
                    failed,
                    2,
                    createdAt.AddSeconds(2));
            Assert.NotNull(
                failedRecord);

            var before =
                await store.ListDueAsync(
                    new NotificationDeliveryDueQuery(
                        retryAt.AddTicks(-1)));
            Assert.Empty(
                before.Items);

            var due =
                await store.ListDueAsync(
                    new NotificationDeliveryDueQuery(
                        retryAt));
            Assert.Single(
                due.Items);
            Assert.Equal(
                NotificationDeliveryState.Failed,
                due.Items[0].Snapshot.State);
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_store_lists_stale_inflight_deliveries_with_stable_bounded_cursor()
    {
        var path =
            TempPath();

        try
        {
            var store =
                new AdoNotificationDeliveryStore(
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        path));
            await store.InitializeAsync();

            var createdAt =
                new DateTimeOffset(
                    2026,
                    10,
                    5,
                    6,
                    0,
                    0,
                    TimeSpan.Zero);
            var claimedAt =
                createdAt.AddSeconds(1);

            foreach (var id in new[]
                     {
                         "11111111-1111-1111-1111-111111111111",
                         "22222222-2222-2222-2222-222222222222",
                         "33333333-3333-3333-3333-333333333333",
                     })
            {
                var notificationId =
                    Guid.Parse(id);
                await store.CreateOrGetAsync(
                    Snapshot(
                        NotificationDeliveryState.Pending,
                        0,
                        createdAt,
                        notificationId:
                            notificationId),
                    createdAt);

                var claimed =
                    await store.ReplaceAsync(
                        Snapshot(
                            NotificationDeliveryState.InFlight,
                            1,
                            createdAt,
                            notificationId:
                                notificationId),
                        expectedRevision: 1,
                        updatedAtUtc:
                            claimedAt);
                Assert.NotNull(
                    claimed);
            }

            var first =
                await store.ListStaleInFlightAsync(
                    new NotificationStaleInFlightQuery(
                        claimedAt,
                        maxResults: 2));

            Assert.Equal(
                2,
                first.Items.Count);
            Assert.True(
                first.Truncated);
            Assert.NotNull(
                first.NextCursor);

            var second =
                await store.ListStaleInFlightAsync(
                    new NotificationStaleInFlightQuery(
                        claimedAt,
                        maxResults: 2,
                        after:
                            first.NextCursor));

            Assert.Single(
                second.Items);
            Assert.False(
                second.Truncated);

            Assert.Equal(
                3,
                first.Items
                    .Concat(second.Items)
                    .Select(
                        record =>
                            record.Snapshot.NotificationId)
                    .Distinct()
                    .Count());
            Assert.All(
                first.Items.Concat(second.Items),
                record =>
                    Assert.Equal(
                        NotificationDeliveryState.InFlight,
                        record.Snapshot.State));
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_due_index_matches_due_time_seek_and_order_key()
    {
        var path =
            TempPath();

        try
        {
            var factory =
                new SqliteNotificationDeliveryDbConnectionFactory(
                    path);
            var store =
                new AdoNotificationDeliveryStore(
                    factory);
            await store.InitializeAsync();

            await using var connection =
                await factory.OpenAsync();
            await using var command =
                connection.CreateCommand();
            command.CommandText =
                "PRAGMA index_info(ix_kafdeck_notification_delivery_due)";

            var columns =
                new List<string>();
            await using var reader =
                await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                columns.Add(
                    reader.GetString(2));
            }

            Assert.Equal(
                [
                    "due_at_utc",
                    "notification_id",
                    "destination_id",
                ],
                columns);

            await using var sqlCommand =
                connection.CreateCommand();
            sqlCommand.CommandText =
                """
                SELECT sql
                FROM sqlite_master
                WHERE type = 'index'
                  AND name = 'ix_kafdeck_notification_delivery_due'
                """;
            var indexSql =
                (string?)await sqlCommand
                    .ExecuteScalarAsync();

            Assert.NotNull(
                indexSql);
            Assert.Contains(
                "WHERE state IN ('Pending', 'Failed')",
                indexSql!,
                StringComparison.Ordinal);
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_schema_rejects_arbitrary_outcome_code_even_for_direct_write()
    {
        var path =
            TempPath();

        try
        {
            var factory =
                new SqliteNotificationDeliveryDbConnectionFactory(
                    path);
            var store =
                new AdoNotificationDeliveryStore(
                    factory);
            await store.InitializeAsync();

            await using var connection =
                await factory.OpenAsync();
            await using var command =
                connection.CreateCommand();
            command.CommandText =
                """
                INSERT INTO kafdeck_notification_deliveries (
                    notification_id,
                    destination_id,
                    payload_fingerprint,
                    state,
                    attempt_count,
                    created_at_utc,
                    next_attempt_at_utc,
                    due_at_utc,
                    outcome_code,
                    revision,
                    updated_at_utc)
                VALUES (
                    '99999999-9999-9999-9999-999999999999',
                    'ops-webhook',
                    @fingerprint,
                    'Failed',
                    1,
                    @created_at,
                    @retry_at,
                    @retry_at,
                    'http-503-token-like-detail',
                    1,
                    @updated_at)
                """;
            AddParameter(
                command,
                "@fingerprint",
                new string('a', 64));
            AddParameter(
                command,
                "@created_at",
                "2026-10-05T06:00:00.0000000+00:00");
            AddParameter(
                command,
                "@retry_at",
                "2026-10-05T06:00:05.0000000+00:00");
            AddParameter(
                command,
                "@updated_at",
                "2026-10-05T06:00:01.0000000+00:00");

            await Assert.ThrowsAsync<SqliteException>(
                () =>
                    command.ExecuteNonQueryAsync());
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_schema_rejects_null_outcome_for_terminal_state()
    {
        var path =
            TempPath();

        try
        {
            var factory =
                new SqliteNotificationDeliveryDbConnectionFactory(
                    path);
            var store =
                new AdoNotificationDeliveryStore(
                    factory);
            await store.InitializeAsync();

            await using var connection =
                await factory.OpenAsync();
            await using var command =
                connection.CreateCommand();
            command.CommandText =
                """
                INSERT INTO kafdeck_notification_deliveries (
                    notification_id,
                    destination_id,
                    payload_fingerprint,
                    state,
                    attempt_count,
                    created_at_utc,
                    next_attempt_at_utc,
                    due_at_utc,
                    outcome_code,
                    revision,
                    updated_at_utc)
                VALUES (
                    '88888888-8888-8888-8888-888888888888',
                    'ops-webhook',
                    @fingerprint,
                    'Delivered',
                    1,
                    @created_at,
                    NULL,
                    @created_at,
                    NULL,
                    1,
                    @updated_at)
                """;
            AddParameter(
                command,
                "@fingerprint",
                new string('a', 64));
            AddParameter(
                command,
                "@created_at",
                "2026-10-05T06:00:00.0000000+00:00");
            AddParameter(
                command,
                "@updated_at",
                "2026-10-05T06:00:01.0000000+00:00");

            await Assert.ThrowsAsync<SqliteException>(
                () =>
                    command.ExecuteNonQueryAsync());
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_schema_persists_metadata_only_without_payload_or_secret_columns()
    {
        var path =
            TempPath();

        try
        {
            var factory =
                new SqliteNotificationDeliveryDbConnectionFactory(
                    path);
            var store =
                new AdoNotificationDeliveryStore(
                    factory);
            await store.InitializeAsync();

            await using var connection =
                await factory.OpenAsync();
            await using var command =
                connection.CreateCommand();
            command.CommandText =
                "PRAGMA table_info(kafdeck_notification_deliveries)";

            var columns =
                new List<string>();
            await using var reader =
                await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                columns.Add(
                    reader.GetString(1));
            }

            Assert.DoesNotContain(
                columns,
                column =>
                    column.Contains(
                        "payload",
                        StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(
                        column,
                        "payload_fingerprint",
                        StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(
                columns,
                column =>
                    column.Contains(
                        "secret",
                        StringComparison.OrdinalIgnoreCase) ||
                    column.Contains(
                        "provider_request",
                        StringComparison.OrdinalIgnoreCase) ||
                    column.Contains(
                        "header",
                        StringComparison.OrdinalIgnoreCase) ||
                    column.Contains(
                        "body",
                        StringComparison.OrdinalIgnoreCase) ||
                    column.Contains(
                        "url",
                        StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public void Delivery_page_rejects_cursor_not_bound_to_last_returned_item()
    {
        var createdAt =
            new DateTimeOffset(
                2026,
                10,
                4,
                12,
                0,
                0,
                TimeSpan.Zero);
        var record =
            new NotificationDeliveryRecord(
                Snapshot(
                    NotificationDeliveryState.Pending,
                    0,
                    createdAt),
                revision: 1,
                updatedAtUtc:
                    createdAt);

        Assert.Throws<ArgumentException>(
            () =>
                new NotificationDeliveryPage(
                    [record],
                    truncated: true,
                    new NotificationDeliveryDueCursor(
                        createdAt,
                        Guid.Parse(
                            "22222222-2222-2222-2222-222222222222"),
                        record.Snapshot.DestinationId)));
    }

    [Fact]
    public async Task PostgreSql_store_exercises_idempotency_cas_pagination_recovery_and_constraints()
    {
        var connectionString =
            Environment.GetEnvironmentVariable(
                "KAFDECK_TEST_POSTGRES");

        if (string.IsNullOrWhiteSpace(
                connectionString))
        {
            return;
        }

        var factory =
            new PostgreSqlNotificationDeliveryDbConnectionFactory(
                connectionString);
        var store =
            new AdoNotificationDeliveryStore(
                factory);
        await store.InitializeAsync();
        await ClearDeliveryTableAsync(
            factory);

        try
        {
            var createdAt =
                new DateTimeOffset(
                    2026,
                    10,
                    5,
                    6,
                    0,
                    0,
                    TimeSpan.Zero);
            var claimedAt =
                createdAt.AddSeconds(1);

            var ids =
                new[]
                {
                    Guid.Parse(
                        "11111111-1111-1111-1111-111111111111"),
                    Guid.Parse(
                        "22222222-2222-2222-2222-222222222222"),
                    Guid.Parse(
                        "33333333-3333-3333-3333-333333333333"),
                    Guid.Parse(
                        "44444444-4444-4444-4444-444444444444"),
                };

            foreach (var id in ids)
            {
                var created =
                    await store.CreateOrGetAsync(
                        Snapshot(
                            NotificationDeliveryState.Pending,
                            0,
                            createdAt,
                            notificationId:
                                id),
                        createdAt);

                Assert.Equal(
                    1,
                    created.Revision);
            }

            var repeated =
                await store.CreateOrGetAsync(
                    Snapshot(
                        NotificationDeliveryState.Pending,
                        0,
                        createdAt,
                        notificationId:
                            ids[0]),
                    createdAt.AddMilliseconds(1));
            Assert.Equal(
                1,
                repeated.Revision);

            var firstClaim =
                await store.ReplaceAsync(
                    Snapshot(
                        NotificationDeliveryState.InFlight,
                        1,
                        createdAt,
                        notificationId:
                            ids[0]),
                    expectedRevision: 1,
                    updatedAtUtc:
                        claimedAt);
            Assert.NotNull(
                firstClaim);

            var stale =
                await store.ReplaceAsync(
                    Snapshot(
                        NotificationDeliveryState.Exhausted,
                        1,
                        createdAt,
                        notificationId:
                            ids[0],
                        outcomeCode:
                            NotificationDeliveryOutcomeCodes.Exhausted),
                    expectedRevision: 1,
                    updatedAtUtc:
                        claimedAt.AddSeconds(1));
            Assert.Null(
                stale);

            var secondClaim =
                await store.ReplaceAsync(
                    Snapshot(
                        NotificationDeliveryState.InFlight,
                        1,
                        createdAt,
                        notificationId:
                            ids[1]),
                    expectedRevision: 1,
                    updatedAtUtc:
                        claimedAt);
            Assert.NotNull(
                secondClaim);

            var retryAt =
                createdAt.AddSeconds(10);
            var failed =
                await store.ReplaceAsync(
                    Snapshot(
                        NotificationDeliveryState.Failed,
                        1,
                        createdAt,
                        notificationId:
                            ids[1],
                        nextAttemptAtUtc:
                            retryAt,
                        outcomeCode:
                            NotificationDeliveryOutcomeCodes.RetryableFailure),
                    expectedRevision: 2,
                    updatedAtUtc:
                        createdAt.AddSeconds(2));
            Assert.NotNull(
                failed);

            var firstDue =
                await store.ListDueAsync(
                    new NotificationDeliveryDueQuery(
                        retryAt,
                        maxResults: 2));
            Assert.Equal(
                2,
                firstDue.Items.Count);
            Assert.True(
                firstDue.Truncated);

            var secondDue =
                await store.ListDueAsync(
                    new NotificationDeliveryDueQuery(
                        retryAt,
                        maxResults: 2,
                        after:
                            firstDue.NextCursor));
            Assert.Single(
                secondDue.Items);

            var recovery =
                await store.ListStaleInFlightAsync(
                    new NotificationStaleInFlightQuery(
                        claimedAt));
            Assert.Single(
                recovery.Items);
            Assert.Equal(
                ids[0],
                recovery.Items[0]
                    .Snapshot.NotificationId);

            await using var connection =
                await factory.OpenAsync();
            await using var command =
                connection.CreateCommand();
            command.CommandText =
                """
                INSERT INTO kafdeck_notification_deliveries (
                    notification_id,
                    destination_id,
                    payload_fingerprint,
                    state,
                    attempt_count,
                    created_at_utc,
                    next_attempt_at_utc,
                    due_at_utc,
                    outcome_code,
                    revision,
                    updated_at_utc)
                VALUES (
                    '99999999-9999-9999-9999-999999999999',
                    'ops-webhook',
                    @fingerprint,
                    'Delivered',
                    1,
                    @created_at,
                    NULL,
                    @created_at,
                    NULL,
                    1,
                    @updated_at)
                """;
            AddParameter(
                command,
                "@fingerprint",
                new string('a', 64));
            AddParameter(
                command,
                "@created_at",
                "2026-10-05T06:00:00.0000000+00:00");
            AddParameter(
                command,
                "@updated_at",
                "2026-10-05T06:00:01.0000000+00:00");

            await Assert.ThrowsAnyAsync<
                System.Data.Common.DbException>(
                () =>
                    command.ExecuteNonQueryAsync());
        }
        finally
        {
            await ClearDeliveryTableAsync(
                factory);
        }
    }

    [Fact]
    public void PostgreSql_factory_uses_ha_capable_provider_semantics()
    {
        var factory =
            new PostgreSqlNotificationDeliveryDbConnectionFactory(
                "Host=localhost;Database=kafdeck;Username=test;Password=test");

        Assert.True(
            factory.SupportsSelectForUpdate);
    }

    private static NotificationDeliverySnapshot
        Snapshot(
            NotificationDeliveryState state,
            int attemptCount,
            DateTimeOffset createdAtUtc,
            Guid? notificationId = null,
            DateTimeOffset? nextAttemptAtUtc = null,
            string? outcomeCode = null)
    {
        return new NotificationDeliverySnapshot(
            notificationId ??
            Guid.Parse(
                "11111111-1111-1111-1111-111111111111"),
            "ops-webhook",
            new string(
                'a',
                64),
            state,
            attemptCount,
            createdAtUtc,
            nextAttemptAtUtc,
            outcomeCode);
    }

    private static async Task ClearDeliveryTableAsync(
        INotificationDeliveryDbConnectionFactory factory)
    {
        await using var connection =
            await factory.OpenAsync();
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            "DELETE FROM kafdeck_notification_deliveries";
        await command.ExecuteNonQueryAsync();
    }

    private static string TempPath() =>
        Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-notification-delivery-{Guid.NewGuid():N}.db");

    private static void AddParameter(
        System.Data.Common.DbCommand command,
        string name,
        object value)
    {
        var parameter =
            command.CreateParameter();
        parameter.ParameterName =
            name;
        parameter.Value =
            value;
        command.Parameters.Add(
            parameter);
    }

    private static void Cleanup(
        string path)
    {
        SqliteConnection.ClearAllPools();

        foreach (var candidate in
                 new[]
                 {
                     path,
                     path + "-wal",
                     path + "-shm",
                 })
        {
            if (File.Exists(
                    candidate))
            {
                File.Delete(
                    candidate);
            }
        }
    }
}
