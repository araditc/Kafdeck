using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Xunit;
using Xunit.Abstractions;
using TlsProvider = Kafdeck.Architecture.Tests.V08W66HostedWebhookTlsTests.TlsProvider;

namespace Kafdeck.Architecture.Tests;

// Two actual hosted senders share PostgreSQL coordination, not one test dispatcher.
// This first proof intentionally uses the existing Standalone admission. It does
// not enable production HighAvailability or claim failover/restore acceptance.
public sealed class V08W66PostgreSqlHostedTlsTests(ITestOutputHelper output)
{
    [G2PostgreSqlFact]
    public async Task Two_PostgreSql_hosts_contending_for_one_event_physically_post_exactly_once()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var database = await Database.CreateAsync(deadline.Token);
        await using var tls = new TlsProvider();
        var profile = Profile(tls);
        using var first = CreateHost(database.ConnectionString, profile, tls, TimeSpan.FromHours(-1));
        using var second = CreateHost(database.ConnectionString, profile, tls, TimeSpan.FromHours(1));
        var id = await SeedAsync(first.Services, profile, deadline.Token);
        await Task.WhenAll(first.StartAsync(deadline.Token), second.StartAsync(deadline.Token));
        var record = await WaitAsync(second.Services, id, NotificationDeliveryState.Delivered, deadline.Token);
        await Task.WhenAll(
            first.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync(deadline.Token),
            second.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync(deadline.Token));
        await Task.WhenAll(first.StopAsync(deadline.Token), second.StopAsync(deadline.Token));

        Assert.Equal(1, tls.RequestCount);
        Assert.Equal("hooks.example.org", tls.ServerName);
        Assert.Equal(1, record.Snapshot.AttemptCount);
        var wire = Assert.Single(tls.Requests);
        using var json = JsonDocument.Parse(wire[(wire.IndexOf("\r\n\r\n", StringComparison.Ordinal) + 4)..]);
        Assert.Equal(id, json.RootElement.GetProperty("eventId").GetGuid());
        var durable = await database.Store().GetAsync(id, profile.DestinationId, deadline.Token);
        Assert.NotNull(durable);
        Assert.Equal(NotificationDeliveryState.Delivered, durable.Snapshot.State);
        Assert.Equal(record.Revision, durable.Revision);
        Assert.Equal(1, durable.Snapshot.AttemptCount);
        output.WriteLine("REAL_POSTGRES_TWO_HOST_TLS hosts=2; independentConnections=true; physicalPOSTs=1; HTTP204=1; ledger=Delivered; attempts=1; replicaClockSkewHours=-1,+1; durableReopen=true; blindResends=0; productionHAAdmission=false");
    }

    [G2PostgreSqlFact]
    public async Task Interrupted_PostgreSql_sender_recovers_on_second_host_after_real_database_time_without_resend()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var database = await Database.CreateAsync(deadline.Token);
        await using var tls = new TlsProvider(holdResponse: true);
        var profile = Profile(tls);
        using var first = CreateHost(database.ConnectionString, profile, tls, TimeSpan.FromHours(-1));
        using var survivor = CreateHost(database.ConnectionString, profile, tls, TimeSpan.FromHours(1));
        var id = await SeedAsync(first.Services, profile, deadline.Token);
        await first.StartAsync(deadline.Token);
        await tls.Received.Task.WaitAsync(deadline.Token);
        var sending = await first.Services.GetRequiredService<INotificationDeliveryStore>()
            .GetAsync(id, profile.DestinationId, deadline.Token);
        Assert.NotNull(sending);
        Assert.Equal(NotificationDeliveryState.InFlight, sending.Snapshot.State);

        // Stop after the remote TLS peer read the complete POST, before any HTTP
        // receipt. Shutdown leaves InFlight, preserving real external ambiguity.
        await first.StopAsync(deadline.Token);
        var interrupted = await database.Store().GetAsync(id, profile.DestinationId, deadline.Token);
        Assert.NotNull(interrupted);
        Assert.Equal(NotificationDeliveryState.InFlight, interrupted.Snapshot.State);
        await survivor.StartAsync(deadline.Token);
        var recovered = await WaitAsync(survivor.Services, id, NotificationDeliveryState.UnknownExternalEffect, deadline.Token);
        await survivor.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync(deadline.Token);
        await survivor.Services.GetRequiredService<NotificationDeliveryWorker>().RecoverStaleInFlightAsync(deadline.Token);
        await survivor.StopAsync(deadline.Token);

        var elapsedDatabaseTime = recovered.UpdatedAtUtc - sending.UpdatedAtUtc;
        Assert.True(elapsedDatabaseTime >= TimeSpan.FromSeconds(10),
            "Recovery must wait for actual database time, despite a replica clock one hour ahead.");
        Assert.Equal(1, tls.RequestCount);
        Assert.Equal(1, recovered.Snapshot.AttemptCount);
        Assert.Equal(NotificationDeliveryOutcomeCodes.UnknownExternalEffect, recovered.Snapshot.OutcomeCode);
        var durable = await database.Store().GetAsync(id, profile.DestinationId, deadline.Token);
        Assert.NotNull(durable);
        Assert.Equal(NotificationDeliveryState.UnknownExternalEffect, durable.Snapshot.State);
        Assert.Equal(recovered.Revision, durable.Revision);
        Assert.Equal(1, durable.Snapshot.AttemptCount);
        output.WriteLine($"REAL_POSTGRES_TWO_HOST_INTERRUPTED_TLS hosts=2; physicalPOSTs=1; providerReadComplete=true; HTTPReceipts=0; preRecovery=InFlight; postRecovery=UnknownExternalEffect; attempts=1; realDatabaseElapsedMilliseconds={(long)elapsedDatabaseTime.TotalMilliseconds}; syntheticClockAging=false; replicaClockSkewHours=-1,+1; durableReopen=true; blindResends=0; productionHAAdmission=false");
    }

    private static NotificationDestinationProfile Profile(TlsProvider tls) =>
        new("pg-webhook", NotificationProviderKind.Webhook, "PostgreSQL TLS fixture",
            [NotificationEventClass.Operational], new Uri($"https://hooks.example.org:{tls.Port}/notify"));

    private static IHost CreateHost(string connectionString, NotificationDestinationProfile profile,
        TlsProvider tls, TimeSpan replicaSkew)
    {
        var builder = Host.CreateApplicationBuilder();
        var options = new NotificationReadOptions(true, NotificationPersistenceProvider.PostgreSql,
            NotificationExecutionMode.Standalone, null, SecretReference.Parse("env:KAFDECK_TEST_POSTGRES"),
            DestinationProfiles: [profile], WebhookDeliveryEnabled: true);
        var replicaClock = new SkewedReplicaClock(replicaSkew);
        builder.Services.AddSingleton<TimeProvider>(replicaClock);
        builder.Services.AddSingleton(new NotificationDeliveryWorkerPolicy(
            staleInFlightAfter: TimeSpan.FromSeconds(10), dispatchTimeout: TimeSpan.FromSeconds(5)));
        builder.Services.AddSingleton(new NotificationDeliveryPolicy(maxConcurrency: 1));
        builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([profile]));
        builder.Services.AddSingleton<INotificationCredentialResolver>(new ConfiguredNotificationCredentialResolver([], new SecretResolver()));
        builder.Services.AddSingleton<INotificationHostAddressLookup>(tls);
        builder.Services.AddSingleton<INotificationPinnedTcpDialer>(tls);
        builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(tls, NotificationAddressPolicy.NoConfiguredNat64, replicaClock,
            trustRoots: new X509Certificate2Collection(tls.Root)));
        var connections = new PostgreSqlNotificationDeliveryDbConnectionFactory(connectionString);
        builder.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(connections);
        builder.Services.AddSingleton<INotificationRoutingStore>(new AdoNotificationRoutingStore(connections));
        builder.Services.AddSingleton<INotificationDeliveryStore>(new AdoNotificationDeliveryStore(connections));
        builder.Services.AddKafdeckWebhookDelivery(options);
        return builder.Build();
    }

    private static async Task<Guid> SeedAsync(IServiceProvider services, NotificationDestinationProfile profile, CancellationToken token)
    {
        var routing = services.GetRequiredService<INotificationRoutingStore>();
        var deliveries = services.GetRequiredService<INotificationDeliveryStore>();
        await routing.InitializeAsync(token);
        await deliveries.InitializeAsync(token);
        var now = await deliveries.GetCoordinationUtcNowAsync(DateTimeOffset.UtcNow, token);
        var notification = NotificationSafeEvent.Approved(Guid.NewGuid(), NotificationApprovedEventKind.ConsumerLagAlert, now);
        await routing.CreateOrGetEventAsync(notification, now, token);
        await deliveries.CreateOrGetAsync(new NotificationDeliverySnapshot(notification.EventId, profile.DestinationId,
            notification.PayloadFingerprint, NotificationDeliveryState.Pending, 0, now,
            routedProfileRevisionFingerprint: profile.RevisionFingerprint), now, token);
        return notification.EventId;
    }

    private static async Task<NotificationDeliveryRecord> WaitAsync(IServiceProvider services, Guid id,
        NotificationDeliveryState state, CancellationToken token)
    {
        var store = services.GetRequiredService<INotificationDeliveryStore>();
        while (true)
        {
            var row = await store.GetAsync(id, "pg-webhook", token);
            if (row?.Snapshot.State == state) return row;
            await Task.Delay(25, token);
        }
    }

    private sealed class SkewedReplicaClock(TimeSpan skew) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UtcNow + skew;
    }

    private sealed class Database(NpgsqlConnection admin, string schema, string connectionString) : IAsyncDisposable
    {
        internal string ConnectionString { get; } = connectionString;
        internal AdoNotificationDeliveryStore Store() => new(new PostgreSqlNotificationDeliveryDbConnectionFactory(ConnectionString));
        internal static async Task<Database> CreateAsync(CancellationToken token)
        {
            var configured = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")!;
            var admin = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(configured) { Pooling = false, IncludeErrorDetail = false }.ConnectionString);
            await admin.OpenAsync(token);
            var schema = "w66_tls_" + Guid.NewGuid().ToString("N");
            try
            {
                await using var command = admin.CreateCommand();
                command.CommandText = $"CREATE SCHEMA \"{schema}\"";
                await command.ExecuteNonQueryAsync(token);
                return new Database(admin, schema, new NpgsqlConnectionStringBuilder(configured)
                    { SearchPath = schema, Pooling = false, IncludeErrorDetail = false }.ConnectionString);
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
