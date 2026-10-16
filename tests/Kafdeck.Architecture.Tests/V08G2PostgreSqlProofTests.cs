using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class G2PostgreSqlFactAttribute : FactAttribute
{
    public G2PostgreSqlFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")))
            Skip = "Requires a real isolated PostgreSQL server; set KAFDECK_TEST_POSTGRES. No database proof without it.";
    }
}

// Real database coordination evidence, not physical sender-host HA admission.
public sealed class V08G2PostgreSqlProofTests
{
    private const string Destination = "g2-ops";

    [G2PostgreSqlFact]
    public async Task PostgreSql_rate_denial_is_observed_with_real_claim_timestamps_not_a_presumed_window()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        await using var fixture = await DatabaseFixture.CreateAsync(deadline.Token);
        var first = fixture.Store();
        var second = fixture.Store();
        await first.InitializeAsync(deadline.Token);
        var denied = false;
        var observations = new List<object>();
        for (var attempt = 0; attempt < 3 && !denied; attempt++)
        {
            var now = await first.GetCoordinationUtcNowAsync(DateTimeOffset.UtcNow, deadline.Token);
            var destination = "g2-rate-" + attempt;
            var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };
            foreach (var id in ids)
                await first.CreateOrGetAsync(new NotificationDeliverySnapshot(id, destination,
                    new string('a', 64), NotificationDeliveryState.Pending, 0, now,
                    routedProfileRevisionFingerprint: new string('b', 64)), now, deadline.Token);
            var left = await first.TryClaimForDispatchAsync(ids[0], destination, 1,
                now.AddMinutes(-5), now.AddMinutes(10), maxConcurrency: 2, ratePerSecond: 1, deadline.Token);
            Assert.Equal(NotificationDeliveryClaimOutcome.Claimed, left.Outcome);
            var right = await second.TryClaimForDispatchAsync(ids[1], destination, 1,
                now.AddMinutes(5), now.AddMinutes(10), maxConcurrency: 2, ratePerSecond: 1, deadline.Token);
            denied = right.Outcome == NotificationDeliveryClaimOutcome.RateLimited;
            if (!denied)
            {
                Assert.Equal(NotificationDeliveryClaimOutcome.Claimed, right.Outcome);
                Assert.NotNull(right.Record);
                Assert.NotNull(left.Record);
                // A second permitted claim is valid only after one real second,
                // regardless of the five-minute skew supplied by either replica.
                Assert.True(right.Record.UpdatedAtUtc - left.Record.UpdatedAtUtc >= TimeSpan.FromSeconds(1),
                    "Two physical claims within one database-second violated the configured rate cap.");
            }
            observations.Add(new { attempt = attempt + 1, secondOutcome = right.Outcome.ToString(),
                acceptedIntervalMilliseconds = right.Record is null ? (double?)null :
                    (right.Record.UpdatedAtUtc - left.Record!.UpdatedAtUtc).TotalMilliseconds });
        }
        Assert.True(denied, "The host could not observe a one-second rate window in three bounded physical attempts.");
        await WriteEvidenceAsync("postgres-rate-clock-evidence.json", new
        {
            serverVersion = fixture.ServerVersion, observedAtUtc = DateTimeOffset.UtcNow,
            configuredRatePerSecond = 1, concurrentSlotLimit = 2, suppliedReplicaClockSkewMinutes = new[] { -5, 5 },
            actualRateDenialObserved = denied, observations
        }, deadline.Token);
    }

    [G2PostgreSqlFact]
    public async Task Independent_PostgreSql_workers_contending_for_one_delivery_dispatch_once()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        await using var fixture = await DatabaseFixture.CreateAsync(deadline.Token);
        var first = fixture.Store();
        var second = fixture.Store();
        await first.InitializeAsync(deadline.Token);
        await second.InitializeAsync(deadline.Token);
        var now = await first.GetCoordinationUtcNowAsync(DateTimeOffset.UtcNow, deadline.Token);
        var id = Guid.NewGuid();
        await first.CreateOrGetAsync(Pending(id, now), now, deadline.Token);
        var dispatcher = new CountingDispatcher();
        var left = new NotificationDeliveryWorker(first, dispatcher);
        var right = new NotificationDeliveryWorker(second, dispatcher);
        var watch = Stopwatch.StartNew();
        var results = await Task.WhenAll(left.RunDueCycleAsync(deadline.Token), right.RunDueCycleAsync(deadline.Token));
        Assert.Equal(1, dispatcher.Calls);
        Assert.Equal(1, results.Sum(result => result.Delivered));

        // Open through a third repository instance after both contenders complete.
        var reopened = fixture.Store();
        await reopened.InitializeAsync(deadline.Token);
        var persisted = await reopened.GetAsync(id, Destination, deadline.Token);
        Assert.NotNull(persisted);
        Assert.Equal(NotificationDeliveryState.Delivered, persisted.Snapshot.State);
        Assert.Equal(1, persisted.Snapshot.AttemptCount);
        Assert.Equal(3, persisted.Revision);
        var after = await new NotificationDeliveryWorker(reopened, dispatcher).RunDueCycleAsync(deadline.Token);
        Assert.Equal(0, after.Visited);
        Assert.Equal(1, dispatcher.Calls);
        await WriteEvidenceAsync("postgres-worker-cas-evidence.json", new
        {
            proof = "independent-worker-instances-real-postgresql",
            serverVersion = fixture.ServerVersion,
            runtime = RuntimeInformation.FrameworkDescription,
            observedAtUtc = DateTimeOffset.UtcNow,
            durationMilliseconds = watch.ElapsedMilliseconds,
            independentWorkerInstances = 2,
            dispatchCalls = dispatcher.Calls,
            durableState = persisted.Snapshot.State.ToString(),
            durableAttemptCount = persisted.Snapshot.AttemptCount,
            durableRevision = persisted.Revision,
            automaticRedispatches = after.Visited,
            physicalSenderHostHaAccepted = false
        }, deadline.Token);
    }

    [G2PostgreSqlFact]
    public async Task PostgreSql_reopened_worker_recovers_unfinished_claim_and_fences_late_finalization()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var fixture = await DatabaseFixture.CreateAsync(deadline.Token);
        var original = fixture.Store();
        await original.InitializeAsync(deadline.Token);
        var now = await original.GetCoordinationUtcNowAsync(DateTimeOffset.UtcNow, deadline.Token);
        var id = Guid.NewGuid();
        var pending = await original.CreateOrGetAsync(Pending(id, now), now, deadline.Token);
        var claim = await original.TryClaimForDispatchAsync(id, Destination, pending.Revision, now,
            now.AddMinutes(10), 1, 10, deadline.Token);
        Assert.Equal(NotificationDeliveryClaimOutcome.Claimed, claim.Outcome);
        Assert.NotNull(claim.Record);
        Assert.Equal(NotificationDeliveryState.InFlight, claim.Record.Snapshot.State);

        // The old instance makes no finalization. A fresh instance uses PostgreSQL's
        // real shared clock; no database timestamps or recovery rows are rewritten.
        var fresh = fixture.Store();
        await fresh.InitializeAsync(deadline.Token);
        var dispatcher = new CountingDispatcher();
        var recovery = new NotificationDeliveryWorker(fresh, dispatcher, workerPolicy:
            new NotificationDeliveryWorkerPolicy(staleInFlightAfter: TimeSpan.FromSeconds(10),
                dispatchTimeout: TimeSpan.FromSeconds(1)));
        var beforeExpiry = await recovery.RecoverStaleInFlightAsync(deadline.Token);
        Assert.Equal(0, beforeExpiry.MarkedUnknownExternalEffect);
        await Task.Delay(TimeSpan.FromSeconds(11), deadline.Token);
        var recovered = await recovery.RecoverStaleInFlightAsync(deadline.Token);
        Assert.Equal(1, recovered.MarkedUnknownExternalEffect);
        var unknown = await fresh.GetAsync(id, Destination, deadline.Token);
        Assert.NotNull(unknown);
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, unknown.Snapshot.State);
        Assert.Equal(claim.Record.Revision + 1, unknown.Revision);

        // A late result from the old revision cannot overwrite the recovered truth.
        var lateDelivered = new NotificationDeliverySnapshot(id, Destination,
            pending.Snapshot.PayloadFingerprint, NotificationDeliveryState.Delivered, 1,
            pending.Snapshot.CreatedAtUtc, outcomeCode: NotificationDeliveryOutcomeCodes.Delivered,
            routedProfileRevisionFingerprint: pending.Snapshot.RoutedProfileRevisionFingerprint);
        var updatedNow = await fresh.GetCoordinationUtcNowAsync(DateTimeOffset.UtcNow, deadline.Token);
        Assert.Null(await original.ReplaceAsync(lateDelivered, claim.Record.Revision, updatedNow, deadline.Token));
        var replay = await fresh.TryClaimForDispatchAsync(id, Destination, unknown.Revision, updatedNow,
            now.AddMinutes(10), 1, 10, deadline.Token);
        Assert.Equal(NotificationDeliveryClaimOutcome.NotClaimable, replay.Outcome);
        Assert.Equal(0, (await recovery.RunDueCycleAsync(deadline.Token)).Visited);
        Assert.Equal(0, dispatcher.Calls);
        var persisted = await fixture.Store().GetAsync(id, Destination, deadline.Token);
        Assert.NotNull(persisted);
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, persisted.Snapshot.State);
        Assert.Equal(unknown.Revision, persisted.Revision);
        await WriteEvidenceAsync("postgres-recovery-fence-evidence.json", new
        {
            proof = "unfinished-claim-reopen-real-clock-recovery-and-stale-cas-fencing",
            serverVersion = fixture.ServerVersion,
            observedAtUtc = DateTimeOffset.UtcNow,
            staleThresholdSeconds = 10,
            realWaitSeconds = 11,
            beforeExpiryMarkedUnknown = beforeExpiry.MarkedUnknownExternalEffect,
            recoveredMarkedUnknown = recovered.MarkedUnknownExternalEffect,
            lateDeliveredFinalizationAccepted = false,
            replayClaimOutcome = replay.Outcome.ToString(),
            durableState = persisted.Snapshot.State.ToString(),
            durableRevision = persisted.Revision,
            dispatchCalls = dispatcher.Calls,
            physicalSenderCrashOrHostHaAccepted = false
        }, deadline.Token);
    }

    private static NotificationDeliverySnapshot Pending(Guid id, DateTimeOffset now) => new(id,
        Destination, new string('a', 64), NotificationDeliveryState.Pending, 0, now,
        routedProfileRevisionFingerprint: new string('b', 64));

    private static async Task WriteEvidenceAsync(string name, object value, CancellationToken cancellationToken)
    {
        var directory = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY");
        if (string.IsNullOrWhiteSpace(directory))
        {
            DirectoryInfo? root = new DirectoryInfo(AppContext.BaseDirectory);
            while (root is not null && !File.Exists(Path.Combine(root.FullName, "Kafdeck.slnx"))) root = root.Parent;
            if (root is null) throw new InvalidOperationException("Could not find repository evidence directory.");
            directory = Path.Combine(root.FullName, "artifacts", "g2");
        }
        Directory.CreateDirectory(directory);
        await File.WriteAllTextAsync(Path.Combine(directory, name),
            JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true }), cancellationToken);
    }

    private sealed class CountingDispatcher : INotificationDeliveryDispatcher
    {
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);
        public async Task<NotificationDeliveryDispatchResult> DispatchAsync(NotificationDeliveryRecord claimedDelivery,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(NotificationDeliveryState.InFlight, claimedDelivery.Snapshot.State);
            Interlocked.Increment(ref _calls);
            await Task.Yield();
            return new(NotificationDeliveryDispatchOutcome.Delivered);
        }
    }

    private sealed class DatabaseFixture(NpgsqlConnection admin, string schema, string scopedConnection)
        : IAsyncDisposable
    {
        public string ServerVersion => admin.ServerVersion;
        public AdoNotificationDeliveryStore Store() => new(new PostgreSqlNotificationDeliveryDbConnectionFactory(scopedConnection));

        public static async Task<DatabaseFixture> CreateAsync(CancellationToken cancellationToken)
        {
            var configured = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES");
            if (string.IsNullOrWhiteSpace(configured)) throw new InvalidOperationException("Real PostgreSQL fixture is required.");
            var builder = new NpgsqlConnectionStringBuilder(configured) { Pooling = false };
            var admin = new NpgsqlConnection(builder.ConnectionString);
            await admin.OpenAsync(cancellationToken);
            var schema = "g2_proof_" + Guid.NewGuid().ToString("N");
            try
            {
                await using var command = admin.CreateCommand();
                command.CommandText = $"CREATE SCHEMA \"{schema}\"";
                await command.ExecuteNonQueryAsync(cancellationToken);
                builder.SearchPath = schema;
                return new(admin, schema, builder.ConnectionString);
            }
            catch { await admin.DisposeAsync(); throw; }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await using var command = admin.CreateCommand();
                command.CommandText = $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE";
                await command.ExecuteNonQueryAsync();
            }
            finally { await admin.DisposeAsync(); }
        }
    }
}
