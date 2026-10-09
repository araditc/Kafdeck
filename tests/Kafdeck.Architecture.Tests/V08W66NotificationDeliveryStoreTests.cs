using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W66NotificationDeliveryStoreTests
{
    [Theory]
    [InlineData("Pending")]
    [InlineData("Failed")]
    [InlineData("InFlight")]
    public async Task Sqlite_v1_upgrade_refuses_unresolved_deliveries_without_modifying_them(
        string state)
    {
        var path = TempPath();
        try
        {
            const string id = "99999999-9999-9999-9999-999999999999";
            var timestamp = "2026-10-07T12:00:00.0000000+00:00";
            await using (var raw = new SqliteConnection(
                new SqliteConnectionStringBuilder
                {
                    DataSource = path,
                }.ConnectionString))
            {
                await raw.OpenAsync();
                await using var create = raw.CreateCommand();
                create.CommandText =
                    """
                    CREATE TABLE kafdeck_schema_info (
                        component TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL);
                    INSERT INTO kafdeck_schema_info (component, schema_version)
                    VALUES ('notification-delivery', 1);
                    CREATE TABLE kafdeck_notification_deliveries (
                        notification_id TEXT NOT NULL,
                        destination_id TEXT NOT NULL,
                        payload_fingerprint TEXT NOT NULL,
                        state TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL,
                        created_at_utc TEXT NOT NULL,
                        next_attempt_at_utc TEXT NULL,
                        due_at_utc TEXT NOT NULL,
                        outcome_code TEXT NULL,
                        revision BIGINT NOT NULL,
                        updated_at_utc TEXT NOT NULL,
                        PRIMARY KEY(notification_id, destination_id));
                    """;
                await create.ExecuteNonQueryAsync();

                await using var insert = raw.CreateCommand();
                insert.CommandText =
                    """
                    INSERT INTO kafdeck_notification_deliveries (
                        notification_id, destination_id, payload_fingerprint,
                        state, attempt_count, created_at_utc, next_attempt_at_utc,
                        due_at_utc, outcome_code, revision, updated_at_utc)
                    VALUES (
                        @id, 'ops-webhook', @fingerprint,
                        @state, 0, @timestamp, NULL,
                        @timestamp, NULL, 1, @timestamp)
                    """;
                foreach (var (name, value) in new[]
                         {
                             ("@id", id),
                             ("@fingerprint", new string('a', 64)),
                             ("@state", state),
                             ("@timestamp", timestamp),
                         })
                {
                    var parameter = insert.CreateParameter();
                    parameter.ParameterName = name;
                    parameter.Value = value;
                    insert.Parameters.Add(parameter);
                }
                await insert.ExecuteNonQueryAsync();
            }

            var store = new AdoNotificationDeliveryStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            var failure = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.InitializeAsync());
            Assert.Contains("upgrade blocked", failure.Message,
                StringComparison.Ordinal);
            Assert.Contains("No queued deliveries were modified",
                failure.Message, StringComparison.Ordinal);

            await using var verify = new SqliteConnection(
                new SqliteConnectionStringBuilder
                {
                    DataSource = path,
                }.ConnectionString);
            await verify.OpenAsync();
            await using (var check = verify.CreateCommand())
            {
                check.CommandText =
                    "SELECT schema_version FROM kafdeck_schema_info WHERE component = 'notification-delivery'";
                Assert.Equal(1L, Convert.ToInt64(
                    await check.ExecuteScalarAsync()));
            }
            await using (var check = verify.CreateCommand())
            {
                check.CommandText =
                    "SELECT state FROM kafdeck_notification_deliveries WHERE notification_id = @id";
                var parameter = check.CreateParameter();
                parameter.ParameterName = "@id";
                parameter.Value = id;
                check.Parameters.Add(parameter);
                Assert.Equal(state, await check.ExecuteScalarAsync());
            }
            await using (var check = verify.CreateCommand())
            {
                check.CommandText =
                    "PRAGMA table_info(kafdeck_notification_deliveries)";
                await using var reader = await check.ExecuteReaderAsync();
                var names = new List<string>();
                while (await reader.ReadAsync())
                {
                    names.Add(reader.GetString(1));
                }
                Assert.DoesNotContain("routed_profile_revision_fingerprint",
                    names);
            }
        }
        finally
        {
            Cleanup(path);
        }
    }

    [Fact]
    public async Task Sqlite_v1_ledger_upgrades_to_v2_and_fences_profile_revision()
    {
        var path = TempPath();
        try
        {
            await using (var raw = new SqliteConnection(
                new SqliteConnectionStringBuilder
                {
                    DataSource = path,
                }.ConnectionString))
            {
                await raw.OpenAsync();
                await using var ddl = raw.CreateCommand();
                ddl.CommandText =
                    """
                    CREATE TABLE kafdeck_schema_info (
                        component TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL);
                    INSERT INTO kafdeck_schema_info
                        (component, schema_version)
                    VALUES ('notification-delivery', 1);
                    CREATE TABLE kafdeck_notification_deliveries (
                        notification_id TEXT NOT NULL,
                        destination_id TEXT NOT NULL,
                        payload_fingerprint TEXT NOT NULL,
                        state TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL,
                        created_at_utc TEXT NOT NULL,
                        next_attempt_at_utc TEXT NULL,
                        due_at_utc TEXT NOT NULL,
                        outcome_code TEXT NULL,
                        revision BIGINT NOT NULL,
                        updated_at_utc TEXT NOT NULL,
                        PRIMARY KEY(notification_id, destination_id));
                    """;
                await ddl.ExecuteNonQueryAsync();
            }

            var store = new AdoNotificationDeliveryStore(
                new SqliteNotificationDeliveryDbConnectionFactory(path));
            await store.InitializeAsync();

            await using (var verified = new SqliteConnection(
                new SqliteConnectionStringBuilder
                {
                    DataSource = path,
                }.ConnectionString))
            {
                await verified.OpenAsync();
                await using var version = verified.CreateCommand();
                version.CommandText =
                    "SELECT schema_version FROM kafdeck_schema_info WHERE component = 'notification-delivery'";
                Assert.Equal(2L, Convert.ToInt64(
                    await version.ExecuteScalarAsync()));

                // A still-running v1 binary omits the new revision column.
                // The v2 database must reject its nonterminal write even
                // after the migration transaction has committed.
                await using var legacyInsert = verified.CreateCommand();
                legacyInsert.CommandText =
                    """
                    INSERT INTO kafdeck_notification_deliveries (
                        notification_id, destination_id, payload_fingerprint,
                        state, attempt_count, created_at_utc, next_attempt_at_utc,
                        due_at_utc, outcome_code, revision, updated_at_utc)
                    VALUES (
                        '88888888-8888-8888-8888-888888888888',
                        'ops-webhook',
                        @payload_fingerprint,
                        'Pending', 0,
                        '2026-10-07T12:00:00.0000000+00:00',
                        NULL,
                        '2026-10-07T12:00:00.0000000+00:00',
                        NULL, 1,
                        '2026-10-07T12:00:00.0000000+00:00')
                    """;
                AddParameter(
                    legacyInsert,
                    "@payload_fingerprint",
                    new string('a', 64));
                await Assert.ThrowsAnyAsync<
                    System.Data.Common.DbException>(
                    () => legacyInsert.ExecuteNonQueryAsync());
            }

            var now = DateTimeOffset.UtcNow;
            var id = Guid.NewGuid();
            var approved = new string('b', 64);
            var record = new NotificationDeliverySnapshot(
                id,
                "email-ops",
                new string('a', 64),
                NotificationDeliveryState.Pending,
                0,
                now,
                routedProfileRevisionFingerprint: approved);
            var created = await store.CreateOrGetAsync(record, now);
            Assert.Equal(
                approved,
                created.Snapshot.RoutedProfileRevisionFingerprint);
            var retrieved = await store.GetAsync(id, "email-ops");
            Assert.Equal(
                approved,
                retrieved!.Snapshot.RoutedProfileRevisionFingerprint);

            var changed = new NotificationDeliverySnapshot(
                id,
                "email-ops",
                new string('a', 64),
                NotificationDeliveryState.Pending,
                0,
                now,
                routedProfileRevisionFingerprint: new string('c', 64));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.CreateOrGetAsync(changed, now));
        }
        finally
        {
            Cleanup(path);
        }
    }

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
                    createdAt,
                routedProfileRevisionFingerprint: new string('b', 64));

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
                    createdAt.AddMilliseconds(1),
                routedProfileRevisionFingerprint: new string('b', 64));

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
    public async Task Sqlite_cursor_queries_seek_from_continuation_indexes_without_temp_sort()
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

            await using var due =
                connection.CreateCommand();
            due.CommandText =
                """
                EXPLAIN QUERY PLAN
                SELECT notification_id
                FROM kafdeck_notification_deliveries
                WHERE state IN ('Pending', 'Failed')
                  AND due_at_utc <= @now_utc
                  AND (
                      due_at_utc,
                      notification_id,
                      destination_id) > (
                      @after_due_at_utc,
                      @after_notification_id,
                      @after_destination_id)
                ORDER BY
                    due_at_utc,
                    notification_id,
                    destination_id
                LIMIT @row_limit
                """;
            AddParameter(
                due,
                "@now_utc",
                "2026-10-05T07:00:00.0000000+00:00");
            AddParameter(
                due,
                "@after_due_at_utc",
                "2026-10-05T06:00:00.0000000+00:00");
            AddParameter(
                due,
                "@after_notification_id",
                "11111111-1111-1111-1111-111111111111");
            AddParameter(
                due,
                "@after_destination_id",
                "ops-webhook");
            AddParameter(
                due,
                "@row_limit",
                11);

            var duePlan =
                new List<string>();
            await using (var reader =
                         await due.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    duePlan.Add(
                        reader.GetString(3));
                }
            }

            Assert.Contains(
                duePlan,
                detail =>
                    detail.Contains(
                        "ix_kafdeck_notification_delivery_due",
                        StringComparison.Ordinal) &&
                    detail.Contains(
                        "due_at_utc,notification_id,destination_id",
                        StringComparison.Ordinal) &&
                    detail.Contains(
                        ">(?,?,?)",
                        StringComparison.Ordinal));
            Assert.DoesNotContain(
                duePlan,
                detail =>
                    detail.Contains(
                        "USE TEMP B-TREE",
                        StringComparison.Ordinal));

            await using var recovery =
                connection.CreateCommand();
            recovery.CommandText =
                """
                EXPLAIN QUERY PLAN
                SELECT notification_id
                FROM kafdeck_notification_deliveries
                WHERE state = 'InFlight'
                  AND updated_at_utc <= @stale_before_utc
                  AND (
                      updated_at_utc,
                      notification_id,
                      destination_id) > (
                      @after_updated_at_utc,
                      @after_notification_id,
                      @after_destination_id)
                ORDER BY
                    updated_at_utc,
                    notification_id,
                    destination_id
                LIMIT @row_limit
                """;
            AddParameter(
                recovery,
                "@stale_before_utc",
                "2026-10-05T07:00:00.0000000+00:00");
            AddParameter(
                recovery,
                "@after_updated_at_utc",
                "2026-10-05T06:00:00.0000000+00:00");
            AddParameter(
                recovery,
                "@after_notification_id",
                "11111111-1111-1111-1111-111111111111");
            AddParameter(
                recovery,
                "@after_destination_id",
                "ops-webhook");
            AddParameter(
                recovery,
                "@row_limit",
                11);

            var recoveryPlan =
                new List<string>();
            await using (var reader =
                         await recovery.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    recoveryPlan.Add(
                        reader.GetString(3));
                }
            }

            Assert.Contains(
                recoveryPlan,
                detail =>
                    detail.Contains(
                        "ix_kafdeck_notification_delivery_recovery",
                        StringComparison.Ordinal) &&
                    detail.Contains(
                        "updated_at_utc,notification_id,destination_id",
                        StringComparison.Ordinal) &&
                    detail.Contains(
                        ">(?,?,?)",
                        StringComparison.Ordinal));
            Assert.DoesNotContain(
                recoveryPlan,
                detail =>
                    detail.Contains(
                        "USE TEMP B-TREE",
                        StringComparison.Ordinal));
        }
        finally
        {
            Cleanup(
                path);
        }
    }

    [Fact]
    public async Task Sqlite_recovery_index_matches_recovery_seek_and_order_key()
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
                "PRAGMA index_info(ix_kafdeck_notification_delivery_recovery)";

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
                    "updated_at_utc",
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
                  AND name = 'ix_kafdeck_notification_delivery_recovery'
                """;
            var indexSql =
                (string?)await sqlCommand
                    .ExecuteScalarAsync();

            Assert.NotNull(
                indexSql);
            Assert.Contains(
                "WHERE state = 'InFlight'",
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

            // A legacy PostgreSQL writer cannot enqueue Pending without
            // the v2 routed-profile revision after the migration commits.
            await using var legacyWriter = connection.CreateCommand();
            legacyWriter.CommandText =
                """
                INSERT INTO kafdeck_notification_deliveries (
                    notification_id, destination_id, payload_fingerprint,
                    state, attempt_count, created_at_utc, next_attempt_at_utc,
                    due_at_utc, outcome_code, revision, updated_at_utc)
                VALUES (
                    '88888888-8888-8888-8888-888888888888',
                    'ops-webhook', @fingerprint, 'Pending', 0,
                    @created_at, NULL, @created_at, NULL, 1, @updated_at)
                """;
            AddParameter(legacyWriter, "@fingerprint", new string('a', 64));
            AddParameter(legacyWriter, "@created_at",
                "2026-10-05T06:00:00.0000000+00:00");
            AddParameter(legacyWriter, "@updated_at",
                "2026-10-05T06:00:01.0000000+00:00");
            await Assert.ThrowsAnyAsync<System.Data.Common.DbException>(
                () => legacyWriter.ExecuteNonQueryAsync());
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


    [Fact]
    public async Task PostgreSql_destination_history_keyset_and_index_are_real()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            "KAFDECK_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        var factory = new PostgreSqlNotificationDeliveryDbConnectionFactory(connectionString);
        var store = new AdoNotificationDeliveryStore(factory);
        await store.InitializeAsync();

        var target = "hist-" + Guid.NewGuid().ToString("N");
        var other = "hist-" + Guid.NewGuid().ToString("N");
        var first = Guid.Parse("10000000-0000-0000-0000-000000000001");
        var second = Guid.Parse("10000000-0000-0000-0000-000000000002");
        var third = Guid.Parse("10000000-0000-0000-0000-000000000003");
        var hidden = Guid.Parse("10000000-0000-0000-0000-000000000004");
        var at = new DateTimeOffset(2026, 10, 8, 16, 0, 0, TimeSpan.Zero);

        async Task Insert(Guid id, string destination, DateTimeOffset timestamp)
        {
            await store.CreateOrGetAsync(
                new NotificationDeliverySnapshot(
                    id, destination, new string('a', 64),
                    NotificationDeliveryState.Pending, 0, timestamp,
                    routedProfileRevisionFingerprint: new string('b', 64)),
                timestamp);
        }

        // One exact timestamp tie, one later row and one strictly different
        // destination prove cursor order, offset-free traversal and isolation.
        await Insert(first, target, at);
        await Insert(second, target, at);
        await Insert(third, target, at.AddMilliseconds(1));
        await Insert(hidden, other, at);

        var page1 = await store.ListByDestinationAsync(
            new NotificationDestinationDeliveryQuery(target, 2));
        Assert.Equal([first, second],
            page1.Items.Select(x => x.Snapshot.NotificationId).ToArray());
        Assert.True(page1.Truncated);
        Assert.NotNull(page1.Next);

        var page2 = await store.ListByDestinationAsync(
            new NotificationDestinationDeliveryQuery(target, 2, page1.Next));
        Assert.Single(page2.Items);
        Assert.Equal(third, page2.Items[0].Snapshot.NotificationId);
        Assert.False(page2.Truncated);
        Assert.Null(page2.Next);
        Assert.DoesNotContain(page1.Items, x => x.Snapshot.DestinationId == other);

        await using var connection = await factory.OpenAsync();
        await using var index = connection.CreateCommand();
        index.CommandText = """
            SELECT COUNT(*)
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = 'kafdeck_notification_deliveries'
              AND indexname = 'ix_kafdeck_notification_delivery_history'
            """;
        Assert.Equal(1L, Convert.ToInt64(await index.ExecuteScalarAsync()));
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
            outcomeCode,
                routedProfileRevisionFingerprint: new string('b', 64));
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
