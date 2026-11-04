using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Core.Notifications;
using Microsoft.Data.Sqlite;

namespace Kafdeck.Infrastructure.Persistence;

// Storage is not an identity authority. Every write requires a current backend
// authorization callback; production callers derive its principal from OIDC.
public sealed partial class W68RuntimeStore(INotificationDeliveryDbConnectionFactory connections, Guid? requiredGeneration = null) : IW68RuntimeStore
{
    private readonly Guid? _requiredGeneration = requiredGeneration == Guid.Empty
        ? throw new ArgumentException("Empty declarative deployment generation.", nameof(requiredGeneration)) : requiredGeneration;
    private static readonly JsonSerializerOptions Json = new()
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow, MaxDepth = 24,
    };
    private const string LockSql = "SELECT pg_advisory_xact_lock(68080068)";

    public async Task InitializeAsync(CancellationToken token = default)
    {
        await W68RuntimeSchema.VerifyAsync(connections, token).ConfigureAwait(false);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        var total = Convert.ToInt64(await ScalarAsync(c, null, "SELECT COUNT(*) FROM kafdeck_declarative_deployments", token).ConfigureAwait(false), CultureInfo.InvariantCulture);
        var matching = Convert.ToInt64(await ScalarAsync(c, null,
            "SELECT COUNT(*) FROM kafdeck_declarative_deployments WHERE generation=@generation", token,
            ("@generation", _requiredGeneration?.ToString("D"))).ConfigureAwait(false), CultureInfo.InvariantCulture);
        if (_requiredGeneration is null ? total != 0 : total != 1 || matching != 1)
            throw new InvalidOperationException("Declarative deployment generation requires explicit operator admission.");
    }

    public async Task<DateTimeOffset> ReadDatabaseUtcNowAsync(CancellationToken token = default)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        return await NowAsync(c, null, token).ConfigureAwait(false);
    }

    public async Task<W68RuntimeRegistration?> GetRegistrationAsync(W68RuntimeRegistrationKind kind, string deploymentId,
        string registrationId, CancellationToken token = default)
    {
        Id(deploymentId); Id(registrationId);
        if (!Enum.IsDefined(kind)) throw new ArgumentException("Invalid registration kind.");
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        return await RegistrationAsync(c, null, kind, deploymentId, registrationId, token).ConfigureAwait(false);
    }

    public async Task<W68RuntimeRegistration?> UpsertRegistrationAsync(W68RuntimeRegistration registration,
        long expectedRevision, Func<CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default)
    {
        ValidateRegistration(registration);
        if (expectedRevision < 0) throw new ArgumentOutOfRangeException(nameof(expectedRevision));
        if (!await currentAuthority(token).ConfigureAwait(false)) return null;
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await BeginAsync(c, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        if (!await GenerationAllowsAsync(c, t, registration.DeploymentId, token).ConfigureAwait(false)) return null;
        var old = await RegistrationAsync(c, t, registration.Kind, registration.DeploymentId, registration.RegistrationId, token).ConfigureAwait(false);
        if ((old?.Revision ?? 0) != expectedRevision) return null;
        // Registration generations and repository/path bindings are immutable.
        // A new repository binding requires a new source identity, never reuse.
        if (old is not null && (old.GenerationId != registration.GenerationId || old.OwnerId != registration.OwnerId ||
            old.PrincipalId != registration.PrincipalId || old.RepositoryId != registration.RepositoryId || old.Path != registration.Path)) return null;
        if (registration.Kind != W68RuntimeRegistrationKind.Owner &&
            !await OwnerEnabledAsync(c, t, registration.DeploymentId, registration.OwnerId, token).ConfigureAwait(false)) return null;
        if (old is null && await CountAsync(c, t, "kafdeck_declarative_registrations", token).ConfigureAwait(false) >= 10000)
            throw new InvalidOperationException("Declarative registration retention budget exhausted.");
        var next = registration with { Revision = checked(expectedRevision + 1) };
        var changed = await ExecuteAsync(c, t, old is null ? """
            INSERT INTO kafdeck_declarative_registrations(deployment_id,kind,registration_id,revision,registration_json)
            VALUES(@d,@kind,@id,@next,@json) ON CONFLICT(deployment_id,kind,registration_id) DO NOTHING
            """ : """
            UPDATE kafdeck_declarative_registrations SET revision=@next,registration_json=@json
            WHERE deployment_id=@d AND kind=@kind AND registration_id=@id AND revision=@old
            """, token, ("@d", next.DeploymentId), ("@kind", next.Kind.ToString()), ("@id", next.RegistrationId),
            ("@next", next.Revision), ("@old", expectedRevision), ("@json", Serialize(next))).ConfigureAwait(false);
        if (changed != 1 || !await currentAuthority(token).ConfigureAwait(false)) return null;
        await AuditAsync(c, t, Guid.NewGuid(), "Registration", next.Revision, next.DeploymentId,
            next.PrincipalId.Length == 0 ? "server-registration" : next.PrincipalId,
            W68RuntimeFingerprints.Desired(Serialize(next)), token).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
        return next;
    }

    public async Task<W68RuntimeOwnership?> GetOwnershipAsync(W68RuntimeResourceKey key, CancellationToken token = default)
    {
        ValidateKey(key);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        return await OwnershipAsync(c, null, key, false, token).ConfigureAwait(false);
    }

    public async Task<W68StoredRuntimePlan?> GetPlanAsync(Guid planId, CancellationToken token = default)
    {
        if (planId == Guid.Empty) throw new ArgumentException("Invalid plan identity.");
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        return await PlanAsync(c, null, planId, token).ConfigureAwait(false);
    }
    public Task<W68StoredRuntimePlan?> LoadPlanAsync(Guid planId, CancellationToken token = default) => GetPlanAsync(planId, token);

    public async Task<W68StoredRuntimePlan?> SavePlanAsync(W68StoredRuntimePlan plan,
        Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default)
    {
        ValidatePlan(plan);
        if (!await currentAuthority(plan, token).ConfigureAwait(false)) return null;
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await BeginAsync(c, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        if (!await GenerationAllowsAsync(c, t, plan.Key.DeploymentId, token).ConfigureAwait(false) ||
            plan.DeploymentGeneration != (_requiredGeneration ?? Guid.Empty)) return null;
        var existing = await PlanAsync(c, t, plan.PlanId, token).ConfigureAwait(false);
        if (existing is not null)
            return existing.PlanFingerprint == plan.PlanFingerprint && existing.RequesterPrincipalId == plan.RequesterPrincipalId &&
                await currentAuthority(existing, token).ConfigureAwait(false) ? existing : null;
        var now = await NowAsync(c, t, token).ConfigureAwait(false);
        if (plan.ExpiresAtUtc <= now || plan.ExpiresAtUtc > now.AddMinutes(5) ||
            !await RegistrationsAllowAsync(c, t, plan, token).ConfigureAwait(false) ||
            !await LeaseAllowsAsync(c, t, plan, token).ConfigureAwait(false)) return null;
        if (await CountAsync(c, t, "kafdeck_declarative_plans", token).ConfigureAwait(false) >= 10000)
            throw new InvalidOperationException("Declarative immutable-plan retention budget exhausted.");
        var current = await AdoNotificationRoutingStore.ReadSubscriptionInTransactionAsync(c, t, plan.Key.ResourceId,
            connections.SupportsSelectForUpdate, token).ConfigureAwait(false);
        if (!SubscriptionMatches(plan, current)) return null;
        var owner = await OwnershipAsync(c, t, plan.Key, true, token).ConfigureAwait(false);
        if (!OwnershipMatches(plan, owner) || !await CompensationAllowsAsync(c, t, plan, owner, current, token).ConfigureAwait(false)) return null;
        if (await ExecuteAsync(c, t, """
            INSERT INTO kafdeck_declarative_plans(operation_id,plan_fingerprint,principal_id,plan_json)
            VALUES(@id,@hash,@principal,@json) ON CONFLICT(operation_id) DO NOTHING
            """, token, ("@id", plan.PlanId.ToString("D")), ("@hash", plan.PlanFingerprint),
            ("@principal", plan.RequesterPrincipalId), ("@json", Serialize(plan))).ConfigureAwait(false) != 1) return null;
        await AuditAsync(c, t, plan.PlanId, "Plan", 1, plan.Key.DeploymentId, plan.RequesterPrincipalId, plan.PlanFingerprint, token).ConfigureAwait(false);
        if (!await currentAuthority(plan, token).ConfigureAwait(false) || !await LeaseAllowsAsync(c, t, plan, token).ConfigureAwait(false)) return null;
        await t.CommitAsync(token).ConfigureAwait(false);
        return plan;
    }

    public async Task<W68RuntimeLease?> AcquireLeaseAsync(string deploymentId, string leaseScopeId, string controllerId,
        string ownerId, string authenticatedPrincipalId, string holderId, int ttlSeconds,
        Func<CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default)
    {
        ValidateLeaseInputs(deploymentId, leaseScopeId, controllerId, ownerId, authenticatedPrincipalId, holderId, ttlSeconds);
        if (!await currentAuthority(token).ConfigureAwait(false)) return null;
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await BeginAsync(c, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        if (!await GenerationAllowsAsync(c, t, deploymentId, token).ConfigureAwait(false)) return null;
        if (!await ControllerAllowsAsync(c, t, deploymentId, controllerId, ownerId, authenticatedPrincipalId, token).ConfigureAwait(false)) return null;
        var now = await NowAsync(c, t, token).ConfigureAwait(false);
        var old = await LeaseAsync(c, t, deploymentId, leaseScopeId, token).ConfigureAwait(false);
        if (old is not null && old.ExpiresAtUtc > now)
        {
            // Stable operation holder survives process restart, but cannot be
            // borrowed by a different registered controller/principal.
            return old.HolderId == holderId && await BindingMatchesAsync(c, t, old, controllerId, authenticatedPrincipalId, token).ConfigureAwait(false) &&
                await currentAuthority(token).ConfigureAwait(false) ? old : null;
        }
        await ExecuteAsync(c, t, """
            INSERT INTO kafdeck_declarative_lease_epoch_high_water(deployment_id,lease_scope_id,epoch_high_water)
            VALUES(@d,@s,0) ON CONFLICT(deployment_id,lease_scope_id) DO NOTHING
            """, token, ("@d", deploymentId), ("@s", leaseScopeId)).ConfigureAwait(false);
        var high = Convert.ToInt64(await ScalarAsync(c, t, """
            SELECT epoch_high_water FROM kafdeck_declarative_lease_epoch_high_water WHERE deployment_id=@d AND lease_scope_id=@s
            """, token, ("@d", deploymentId), ("@s", leaseScopeId)).ConfigureAwait(false), CultureInfo.InvariantCulture);
        if (old is not null && high < old.Epoch) throw new InvalidOperationException("Declarative lease high-water corruption.");
        var epoch = checked(high + 1);
        await ExecuteAsync(c, t, """
            UPDATE kafdeck_declarative_lease_epoch_high_water SET epoch_high_water=@next
            WHERE deployment_id=@d AND lease_scope_id=@s AND epoch_high_water=@old
            """, token, ("@next", epoch), ("@d", deploymentId), ("@s", leaseScopeId), ("@old", high)).ConfigureAwait(false);
        var lease = new W68RuntimeLease(deploymentId, leaseScopeId, holderId, epoch, now.AddSeconds(ttlSeconds));
        await ExecuteAsync(c, t, """
            INSERT INTO kafdeck_declarative_controller_leases(deployment_id,lease_scope_id,holder_id,epoch,expires_at_utc)
            VALUES(@d,@s,@holder,@epoch,@expiry) ON CONFLICT(deployment_id,lease_scope_id)
            DO UPDATE SET holder_id=excluded.holder_id,epoch=excluded.epoch,expires_at_utc=excluded.expires_at_utc
            """, token, ("@d", deploymentId), ("@s", leaseScopeId), ("@holder", holderId), ("@epoch", epoch),
            ("@expiry", Timestamp(c, lease.ExpiresAtUtc))).ConfigureAwait(false);
        await ExecuteAsync(c, t, """
            INSERT INTO kafdeck_declarative_lease_bindings(deployment_id,lease_scope_id,holder_id,epoch,controller_id,principal_id)
            VALUES(@d,@s,@holder,@epoch,@controller,@principal) ON CONFLICT(deployment_id,lease_scope_id)
            DO UPDATE SET holder_id=excluded.holder_id,epoch=excluded.epoch,controller_id=excluded.controller_id,principal_id=excluded.principal_id
            """, token, ("@d", deploymentId), ("@s", leaseScopeId), ("@holder", holderId), ("@epoch", epoch),
            ("@controller", controllerId), ("@principal", authenticatedPrincipalId)).ConfigureAwait(false);
        if (!await currentAuthority(token).ConfigureAwait(false) || lease.ExpiresAtUtc <= await NowAsync(c, t, token).ConfigureAwait(false)) return null;
        await AuditAsync(c, t, Guid.NewGuid(), "LeaseAcquire", epoch, deploymentId, authenticatedPrincipalId,
            W68RuntimeFingerprints.Desired(Serialize(lease)), token).ConfigureAwait(false);
        await t.CommitAsync(token).ConfigureAwait(false);
        return lease;
    }

    public async Task<W68RuntimeLease?> RenewLeaseAsync(W68RuntimeLease lease, string controllerId, string ownerId,
        string authenticatedPrincipalId, int ttlSeconds, Func<CancellationToken, Task<bool>> currentAuthority,
        CancellationToken token = default)
    {
        ValidateLeaseInputs(lease.DeploymentId, lease.LeaseScopeId, controllerId, ownerId, authenticatedPrincipalId, lease.HolderId, ttlSeconds);
        if (!await currentAuthority(token).ConfigureAwait(false)) return null;
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await BeginAsync(c, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        if (!await GenerationAllowsAsync(c, t, lease.DeploymentId, token).ConfigureAwait(false)) return null;
        var now = await NowAsync(c, t, token).ConfigureAwait(false);
        var old = await LeaseAsync(c, t, lease.DeploymentId, lease.LeaseScopeId, token).ConfigureAwait(false);
        if (old is null || old.HolderId != lease.HolderId || old.Epoch != lease.Epoch || old.ExpiresAtUtc <= now ||
            !await ControllerAllowsAsync(c, t, lease.DeploymentId, controllerId, ownerId, authenticatedPrincipalId, token).ConfigureAwait(false) ||
            !await BindingMatchesAsync(c, t, old, controllerId, authenticatedPrincipalId, token).ConfigureAwait(false)) return null;
        var next = old with { ExpiresAtUtc = now.AddSeconds(ttlSeconds) };
        await ExecuteAsync(c, t, """
            UPDATE kafdeck_declarative_controller_leases SET expires_at_utc=@expiry
            WHERE deployment_id=@d AND lease_scope_id=@s AND holder_id=@holder AND epoch=@epoch
            """, token, ("@expiry", Timestamp(c, next.ExpiresAtUtc)), ("@d", lease.DeploymentId), ("@s", lease.LeaseScopeId),
            ("@holder", lease.HolderId), ("@epoch", lease.Epoch)).ConfigureAwait(false);
        if (!await currentAuthority(token).ConfigureAwait(false) || next.ExpiresAtUtc <= await NowAsync(c, t, token).ConfigureAwait(false)) return null;
        await t.CommitAsync(token).ConfigureAwait(false);
        return next;
    }

    public async Task<bool> ReleaseLeaseAsync(W68RuntimeLease lease, string controllerId, string ownerId,
        string authenticatedPrincipalId, Func<CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default)
    {
        ValidateLeaseInputs(lease.DeploymentId, lease.LeaseScopeId, controllerId, ownerId, authenticatedPrincipalId, lease.HolderId, 1);
        if (!await currentAuthority(token).ConfigureAwait(false)) return false;
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await BeginAsync(c, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        if (!await GenerationAllowsAsync(c, t, lease.DeploymentId, token).ConfigureAwait(false)) return false;
        var old = await LeaseAsync(c, t, lease.DeploymentId, lease.LeaseScopeId, token).ConfigureAwait(false);
        if (old is null || old.HolderId != lease.HolderId || old.Epoch != lease.Epoch ||
            !await ControllerAllowsAsync(c, t, lease.DeploymentId, controllerId, ownerId, authenticatedPrincipalId, token).ConfigureAwait(false) ||
            !await BindingMatchesAsync(c, t, old, controllerId, authenticatedPrincipalId, token).ConfigureAwait(false)) return false;
        var now = await NowAsync(c, t, token).ConfigureAwait(false);
        await ExecuteAsync(c, t, """
            UPDATE kafdeck_declarative_controller_leases SET expires_at_utc=@now
            WHERE deployment_id=@d AND lease_scope_id=@s AND holder_id=@holder AND epoch=@epoch
            """, token, ("@now", Timestamp(c, now)), ("@d", lease.DeploymentId), ("@s", lease.LeaseScopeId),
            ("@holder", lease.HolderId), ("@epoch", lease.Epoch)).ConfigureAwait(false);
        if (!await currentAuthority(token).ConfigureAwait(false)) return false;
        await t.CommitAsync(token).ConfigureAwait(false);
        return true;
    }

    public async Task<W68RuntimeCommitReceipt?> GetCommitReceiptAsync(Guid operationId, CancellationToken token = default)
    {
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        return await ReceiptAsync(c, null, operationId, token).ConfigureAwait(false);
    }

    private async Task<DateTimeOffset> NowAsync(DbConnection c, DbTransaction? t, CancellationToken token) =>
        Utc(await ScalarAsync(c, t, connections.DatabaseUtcNowSql, token).ConfigureAwait(false));
    private static DateTimeOffset Utc(object? value) => value switch
    {
        DateTimeOffset time => time.ToUniversalTime(), DateTime time => new DateTimeOffset(DateTime.SpecifyKind(time, DateTimeKind.Utc)),
        string text => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUniversalTime(),
        _ => throw new InvalidOperationException("Invalid declarative database time."),
    };
    private static object Timestamp(DbConnection c, DateTimeOffset time) => c is SqliteConnection ? time.ToUniversalTime().ToString("O") : time.UtcDateTime;
    private static object GuidValue(DbConnection c, Guid value) => c is SqliteConnection ? value.ToString("D") : value;
    private static Guid ReadGuid(object value) => value is Guid id ? id : Guid.Parse((string)value);
    private static async Task<DbTransaction> BeginAsync(DbConnection c, CancellationToken token) => c is SqliteConnection sqlite
        ? sqlite.BeginTransaction(IsolationLevel.Serializable, deferred: false)
        : await c.BeginTransactionAsync(IsolationLevel.ReadCommitted, token).ConfigureAwait(false);
    private static async Task LockAsync(DbConnection c, DbTransaction t, CancellationToken token)
    {
        if (c is not SqliteConnection) await ScalarAsync(c, t, LockSql, token).ConfigureAwait(false);
    }
    private static string Serialize<T>(T value)
    {
        var json = JsonSerializer.Serialize(value, Json);
        if (json.Length > 131072) throw new ArgumentException("Declarative stored projection exceeds finite budget.");
        return json;
    }
    private static bool Sha(string? value) => value is { Length: 64 } && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
    private static void Id(string value, int limit = 128)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > limit || value != value.Trim() || value.Any(char.IsControl))
            throw new ArgumentException("Invalid canonical declarative identity.");
    }
    private static void Principal(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 2048 || value != value.Trim() || value.Any(char.IsControl))
            throw new ArgumentException("Invalid authenticated declarative principal.");
    }
    private static void ValidateKey(W68RuntimeResourceKey key)
    {
        Id(key.DeploymentId); Id(key.ScopeKind, 64); Id(key.ScopeId); Id(key.ResourceId); Id(key.ResourceKind, 64);
        if (key.ResourceKind != "NotificationSubscription" || key.ScopeKind != "Deployment" || key.ScopeId != key.DeploymentId)
            throw new NotSupportedException("Declarative resource family or exact deployment scope is not admitted.");
        NotificationSubscriptionDefinition.NormalizeSubscriptionId(key.ResourceId);
    }
    private static void ValidateRegistration(W68RuntimeRegistration r)
    {
        if (!Enum.IsDefined(r.Kind) || r.GenerationId == Guid.Empty) throw new ArgumentException("Invalid declarative registration.");
        Id(r.DeploymentId); Id(r.RegistrationId); Id(r.OwnerId);
        if (r.Kind == W68RuntimeRegistrationKind.Owner && r.OwnerId != r.RegistrationId) throw new ArgumentException("Owner identity mismatch.");
        if (r.Kind == W68RuntimeRegistrationKind.Controller) Principal(r.PrincipalId);
        else if (r.PrincipalId.Length != 0) Principal(r.PrincipalId);
        if (r.Kind == W68RuntimeRegistrationKind.Source)
        {
            Id(r.RepositoryId); Id(r.Path, 512);
            if (!Sha(r.PinnedRevision) || r.Path.StartsWith('/') || r.Path.Contains('\\') ||
                r.Path.Split('/').Any(x => x is "" or "." or "..")) throw new ArgumentException("Invalid pinned declarative source.");
        }
        else if (r.RepositoryId.Length != 0 || r.Path.Length != 0 || r.PinnedRevision.Length != 0)
            throw new ArgumentException("Non-source registration cannot contain repository fields.");
    }
    private static void ValidateLeaseInputs(string deployment, string scope, string controller, string owner, string principal, string holder, int ttl)
    {
        foreach (var id in new[] { deployment, scope, controller, owner, holder }) Id(id);
        Principal(principal);
        if (ttl is < 1 or > 300) throw new ArgumentOutOfRangeException(nameof(ttl));
    }
    private static void ValidatePlan(W68StoredRuntimePlan plan)
    {
        ValidateKey(plan.Key); Id(plan.OwnerId); Id(plan.SourceId); Principal(plan.RequesterPrincipalId); Id(plan.ControllerId); Id(plan.LeaseScopeId); Id(plan.LeaseHolderId);
        if (plan.PlanId == Guid.Empty || !Enum.IsDefined(plan.Command) || plan.SourceGeneration == Guid.Empty ||
            plan.LeaseEpoch < 1 || plan.ExpectedOwnershipRevision < 0 || plan.ExpectedSubscriptionRevision is < 1 || plan.SourceRegistrationRevision < 0 ||
            plan.LeaseHolderId != plan.PlanId.ToString("N") || plan.LeaseScopeId != W68RuntimeFingerprints.LeaseScope(plan.Key.ResourceId) ||
            !Sha(plan.SourceRevisionHash) || (!Sha(plan.ObservedSubscriptionFingerprint) &&
                !(plan.ObservedSubscriptionFingerprint == "missing" && plan.ExpectedSubscriptionRevision is null && plan.ObservedDestinationId is null)) ||
            !Sha(plan.DesiredFingerprint) || !Sha(plan.PolicyFingerprint) ||
            !Sha(plan.PlanFingerprint) || (plan.ExpectedOwnedSourceRevisionHash is not null && !Sha(plan.ExpectedOwnedSourceRevisionHash)) ||
            plan.DesiredSubscription.SubscriptionId != plan.Key.ResourceId ||
            plan.DesiredState is not (NotificationSubscriptionState.Active or NotificationSubscriptionState.Paused) ||
            plan.DesiredCanonicalJson.Length is < 1 or > 65536 ||
            W68RuntimeFingerprints.Desired(plan.DesiredCanonicalJson) != plan.DesiredFingerprint ||
            W68RuntimeFingerprints.Plan(plan) != plan.PlanFingerprint) throw new ArgumentException("Invalid immutable declarative plan.");
        using var document = JsonDocument.Parse(plan.DesiredCanonicalJson, new JsonDocumentOptions { MaxDepth = 16 });
    }

    private static DbCommand Command(DbConnection c, DbTransaction? t, string sql, params (string Name, object? Value)[] values)
    {
        var cmd = c.CreateCommand(); cmd.Transaction = t; cmd.CommandText = sql; cmd.CommandTimeout = 15;
        foreach (var (name, value) in values) { var p = cmd.CreateParameter(); p.ParameterName = name; p.Value = value ?? DBNull.Value; cmd.Parameters.Add(p); }
        return cmd;
    }
    private static async Task<object?> ScalarAsync(DbConnection c, DbTransaction? t, string sql, CancellationToken token, params (string Name, object? Value)[] values)
    { await using var cmd = Command(c, t, sql, values); return await cmd.ExecuteScalarAsync(token).ConfigureAwait(false); }
    private static async Task<int> ExecuteAsync(DbConnection c, DbTransaction t, string sql, CancellationToken token, params (string Name, object? Value)[] values)
    { await using var cmd = Command(c, t, sql, values); return await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false); }
    private static async Task<long> CountAsync(DbConnection c, DbTransaction t, string table, CancellationToken token) =>
        Convert.ToInt64(await ScalarAsync(c, t, "SELECT COUNT(*) FROM " + table, token).ConfigureAwait(false), CultureInfo.InvariantCulture);
    private static async Task AuditAsync(DbConnection c, DbTransaction t, Guid operation, string action, long revision,
        string deployment, string principal, string fingerprint, CancellationToken token)
    {
        await ExecuteAsync(c, t, """
            INSERT INTO kafdeck_declarative_runtime_audit(operation_id,event_kind,event_revision,deployment_id,principal_id,event_fingerprint)
            VALUES(@id,@kind,@revision,@d,@principal,@hash)
            """, token, ("@id", operation.ToString("D")), ("@kind", action), ("@revision", revision),
            ("@d", deployment), ("@principal", principal), ("@hash", fingerprint)).ConfigureAwait(false);
    }
}
