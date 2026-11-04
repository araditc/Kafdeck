using System.Text.Json;
using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Npgsql;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class W68G3PostgresTheoryAttribute : TheoryAttribute
{
    public W68G3PostgresTheoryAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")))
            Skip = "Requires an actual PostgreSQL fixture; no in-memory substitute.";
    }
}

// These callbacks prove storage fencing, not authentication. Genuine current
// OIDC/RBAC and CLI authority are exercised in the separate hosted G3 suite.
internal sealed class W68G3PersistenceFixture : IAsyncDisposable
{
    internal const string Deployment = "g3-storage-deployment";
    internal const string Owner = "reviewed-owner";
    internal const string Source = "reviewed-source";
    internal const string Controller = "registered-controller";
    internal static readonly string Principal = "oidc-human:" + new string('a', 300) + ":https://issuer.invalid/subject/client";
    internal static readonly string R1 = W68RuntimeFingerprints.Desired("source-revision-one");
    internal static readonly string R2 = W68RuntimeFingerprints.Desired("source-revision-two");
    internal Guid Generation { get; set; } = Guid.NewGuid();
    internal Guid SourceGeneration { get; } = Guid.NewGuid();
    internal INotificationDeliveryDbConnectionFactory Connections { get; }
    internal W68RuntimeStore Store => new(Connections, Generation);
    internal AdoNotificationRoutingStore Routing => new(Connections);
    private readonly string? _adminConnection;
    private readonly string? _schema;

    private W68G3PersistenceFixture(INotificationDeliveryDbConnectionFactory connections, string? admin = null, string? schema = null)
    { Connections = connections; _adminConnection = admin; _schema = schema; }

    internal static async Task<W68G3PersistenceFixture> CreateAsync(bool postgres, CancellationToken token = default)
    {
        W68G3PersistenceFixture fixture;
        if (postgres)
        {
            var admin = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES") ?? throw new InvalidOperationException("Explicit PostgreSQL fixture required.");
            var schema = "w68g3_" + Guid.NewGuid().ToString("N");
            await using var db = new NpgsqlConnection(admin); await db.OpenAsync(token);
            await using var create = db.CreateCommand(); create.CommandText = "CREATE SCHEMA \"" + schema + "\""; await create.ExecuteNonQueryAsync(token);
            fixture = new(new PostgreSqlNotificationDeliveryDbConnectionFactory(new NpgsqlConnectionStringBuilder(admin)
                { SearchPath = schema, Pooling = false, IncludeErrorDetail = false }.ConnectionString), admin, schema);
        }
        else
        {
            var directory = Path.Combine(Path.GetTempPath(), "kafdeck-w68-g3-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
            fixture = new(new SqliteNotificationDeliveryDbConnectionFactory(Path.Combine(directory, "metadata.db")));
        }
        try
        {
            await W68RuntimeSchema.InstallOrUpgradeAsync(fixture.Connections, token);
            await fixture.Routing.InitializeAsync(token);
            await W68RuntimeSchema.AdmitGenerationAsync(fixture.Connections, Deployment, null, fixture.Generation, R1, token);
            await fixture.SeedRegistrationsAsync(token);
            return fixture;
        }
        catch { await fixture.DisposeAsync(); throw; }
    }

    internal async Task SeedRegistrationsAsync(CancellationToken token = default)
    {
        var owner = new W68RuntimeRegistration(W68RuntimeRegistrationKind.Owner, Deployment, Owner, Guid.NewGuid(), 0, Owner, "", "", "", "", true);
        var source = new W68RuntimeRegistration(W68RuntimeRegistrationKind.Source, Deployment, Source, SourceGeneration, 0, Owner, "", "approved-repository", "notifications.yaml", R1, true);
        var controller = new W68RuntimeRegistration(W68RuntimeRegistrationKind.Controller, Deployment, Controller, Guid.NewGuid(), 0, Owner, Principal, "", "", "", true);
        foreach (var registration in new[] { owner, source, controller })
            Assert.NotNull(await Store.UpsertRegistrationAsync(registration, 0, Yes, token));
    }
    internal static Task<bool> Yes(CancellationToken _) => Task.FromResult(true);
    internal static Task<bool> Yes(W68StoredRuntimePlan _, CancellationToken __) => Task.FromResult(true);

    internal async Task<NotificationSubscriptionSnapshot> SeedSubscriptionAsync(string id = "subscription-a", CancellationToken token = default) =>
        await Routing.CreateSubscriptionAsync(new(id, "destination-a", [NotificationEventClass.Operational]),
            NotificationSubscriptionState.Active, await Store.ReadDatabaseUtcNowAsync(token), token);

    internal async Task<(W68StoredRuntimePlan Plan, W68RuntimeLease Lease)> PlanAsync(W68RuntimePlanCommand command,
        string id = "subscription-a", string? sourceRevision = null, int ttlSeconds = 120, bool save = true,
        CancellationToken token = default)
    {
        var operation = Guid.NewGuid();
        var lease = await Store.AcquireLeaseAsync(Deployment, W68RuntimeFingerprints.LeaseScope(id), Controller, Owner,
            Principal, operation.ToString("N"), ttlSeconds, Yes, token);
        Assert.NotNull(lease);
        var key = new W68RuntimeResourceKey(Deployment, "Deployment", Deployment, "NotificationSubscription", id);
        var owned = await Store.GetOwnershipAsync(key, token);
        var current = await Routing.GetSubscriptionAsync(id, token);
        var source = await Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, Deployment, Source, token);
        Assert.NotNull(source);
        var definition = new NotificationSubscriptionDefinition(id, "destination-b", [NotificationEventClass.Operational], ["fixture.changed"]);
        var canonical = JsonSerializer.Serialize(new { id, definition.DestinationId, state = "Paused" });
        var plan = new W68StoredRuntimePlan(operation, command, key, Owner, Source, SourceGeneration, sourceRevision ?? R1,
            Principal, owned?.Revision ?? 0, owned?.GenerationId, current?.Revision,
            current is null ? "missing" : W68RuntimeFingerprints.Subscription(current), current?.Definition.DestinationId,
            definition, NotificationSubscriptionState.Paused, canonical, W68RuntimeFingerprints.Desired(canonical), R1,
            Controller, lease.LeaseScopeId, lease.HolderId, lease.Epoch, lease.ExpiresAtUtc.AddMilliseconds(-100), "",
            owned?.SourceRevisionHash, Generation, source.Revision);
        plan = plan with { PlanFingerprint = W68RuntimeFingerprints.Plan(plan) };
        if (save) Assert.NotNull(await Store.SavePlanAsync(plan, Yes, token));
        return (plan, lease);
    }

    public async ValueTask DisposeAsync()
    {
        if (_adminConnection is not null && _schema is not null)
        {
            await using var db = new NpgsqlConnection(_adminConnection); await db.OpenAsync();
            await using var command = db.CreateCommand(); command.CommandText = "DROP SCHEMA \"" + _schema + "\" CASCADE";
            await command.ExecuteNonQueryAsync();
        }
    }
}
