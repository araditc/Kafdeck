using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

public sealed partial class W68RuntimeStore
{
    public async Task<bool> IsPlanCurrentAsync(Guid planId, string expectedPlanHash,
        Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> currentAuthority, CancellationToken token = default)
    {
        if (planId == Guid.Empty || !Sha(expectedPlanHash)) return false;
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await BeginAsync(c, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        var plan = await PlanAsync(c, t, planId, token).ConfigureAwait(false);
        if (plan is null || !await currentAuthority(plan, token).ConfigureAwait(false)) return false;
        if (plan.PlanFingerprint != expectedPlanHash ||
            await CurrentPlanStateAsync(c, t, plan, token).ConfigureAwait(false) is null) return false;
        return await currentAuthority(plan, token).ConfigureAwait(false) &&
            await GenerationAllowsAsync(c, t, plan.Key.DeploymentId, token).ConfigureAwait(false) &&
            await RegistrationsAllowAsync(c, t, plan, token).ConfigureAwait(false) &&
            await LeaseAllowsAsync(c, t, plan, token).ConfigureAwait(false) &&
            plan.ExpiresAtUtc > await NowAsync(c, t, token).ConfigureAwait(false);
    }

    public async Task<W68RuntimeCommitReceipt?> GetVerifiedCommitReceiptAsync(Guid planId, string expectedPlanHash,
        string authenticatedPrincipalId, Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> currentAuthority,
        CancellationToken token = default)
    {
        Principal(authenticatedPrincipalId);
        if (planId == Guid.Empty || !Sha(expectedPlanHash)) throw new ArgumentException("Invalid declarative receipt reference.");
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await BeginAsync(c, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        var plan = await PlanAsync(c, t, planId, token).ConfigureAwait(false);
        if (plan is null || plan.RequesterPrincipalId != authenticatedPrincipalId ||
            !await currentAuthority(plan, token).ConfigureAwait(false)) return null;
        if (!await GenerationAllowsAsync(c, t, plan.Key.DeploymentId, token).ConfigureAwait(false) ||
            plan.DeploymentGeneration != (_requiredGeneration ?? Guid.Empty) ||
            !await RegistrationsAllowAsync(c, t, plan, token).ConfigureAwait(false)) return null;
        if (plan.PlanFingerprint != expectedPlanHash) return null;
        var receipt = await ReceiptAsync(c, t, planId, token).ConfigureAwait(false);
        if (receipt is null || receipt.PlanFingerprint != plan.PlanFingerprint || receipt.OwnershipRevision < 1 ||
            receipt.OwnershipGeneration == Guid.Empty || receipt.SubscriptionRevision < 0 ||
            plan.Command != W68RuntimePlanCommand.Relinquish && receipt.SubscriptionRevision < 1 ||
            receipt.CommittedAtUtc == default || receipt.CommittedAtUtc > plan.ExpiresAtUtc) return null;
        // This is a verified historical read, not lease reacquisition or an
        // effect retry. A plan may have expired after its original SQL commit.
        if (!await currentAuthority(plan, token).ConfigureAwait(false) ||
            !await GenerationAllowsAsync(c, t, plan.Key.DeploymentId, token).ConfigureAwait(false) ||
            !await RegistrationsAllowAsync(c, t, plan, token).ConfigureAwait(false)) return null;
        return receipt;
    }

    public async Task<W68RuntimeCommitReceipt?> ApplySubscriptionAsync(Guid planId, string expectedPlanHash,
        string authenticatedPrincipalId, Func<W68StoredRuntimePlan, CancellationToken, Task<bool>> currentAuthority,
        CancellationToken token = default)
    {
        Principal(authenticatedPrincipalId);
        if (planId == Guid.Empty || !Sha(expectedPlanHash)) throw new ArgumentException("Invalid declarative plan reference.");
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        await using var t = await BeginAsync(c, token).ConfigureAwait(false);
        await LockAsync(c, t, token).ConfigureAwait(false);
        var plan = await PlanAsync(c, t, planId, token).ConfigureAwait(false);
        if (plan is null || plan.RequesterPrincipalId != authenticatedPrincipalId ||
            !await currentAuthority(plan, token).ConfigureAwait(false)) return null;
        // A hash is concurrency/integrity evidence, never authorization. A
        // committed receipt resolves lost acknowledgements without a new write.
        if (plan.PlanFingerprint != expectedPlanHash) return null;
        if (!await GenerationAllowsAsync(c, t, plan.Key.DeploymentId, token).ConfigureAwait(false) ||
            plan.DeploymentGeneration != (_requiredGeneration ?? Guid.Empty) ||
            !await RegistrationsAllowAsync(c, t, plan, token).ConfigureAwait(false)) return null;
        var receipt = await ReceiptAsync(c, t, planId, token).ConfigureAwait(false);
        if (receipt is not null)
            return receipt.PlanFingerprint == plan.PlanFingerprint && await currentAuthority(plan, token).ConfigureAwait(false) ? receipt : null;
        var currentState = await CurrentPlanStateAsync(c, t, plan, token).ConfigureAwait(false);
        if (currentState is null) return null;
        var now = currentState.ObservedAtUtc;
        var owner = currentState.Ownership;
        var current = currentState.Subscription;
        NotificationSubscriptionSnapshot? next = current;
        if (plan.Command == W68RuntimePlanCommand.Apply)
        {
            // Typed product SQL only; no arbitrary payload, provider endpoint,
            // HTTP callback or caller SQL is forwarded by the registry.
            next = await AdoNotificationRoutingStore.WriteSubscriptionInTransactionAsync(c, t,
                plan.DesiredSubscription, plan.DesiredState, plan.ExpectedSubscriptionRevision,
                now, connections.SupportsSelectForUpdate, token).ConfigureAwait(false);
            if (next is null) return null;
        }
        await ExecuteAsync(c, t, """
            INSERT INTO kafdeck_declarative_key_versions(deployment_id,scope_kind,scope_id,resource_kind,resource_id,revision_high_water)
            VALUES(@d,@sk,@s,@rk,@r,0) ON CONFLICT(deployment_id,scope_kind,scope_id,resource_kind,resource_id) DO NOTHING
            """, token, KeyValues(plan.Key)).ConfigureAwait(false);
        var high = Convert.ToInt64(await ScalarAsync(c, t, """
            SELECT revision_high_water FROM kafdeck_declarative_key_versions
            WHERE deployment_id=@d AND scope_kind=@sk AND scope_id=@s AND resource_kind=@rk AND resource_id=@r
            """, token, KeyValues(plan.Key)).ConfigureAwait(false), CultureInfo.InvariantCulture);
        if (owner is not null && high < owner.Revision) throw new InvalidOperationException("Declarative ownership high-water corruption.");
        var revision = checked(high + 1);
        var generation = owner is null || owner.State == "Relinquished" ? Guid.NewGuid() : owner.GenerationId;
        if (owner is null || owner.State == "Relinquished")
            await ExecuteAsync(c, t, """
                INSERT INTO kafdeck_declarative_generation_history(generation_id,deployment_id,resource_key,allocated_revision)
                VALUES(@generation,@d,@key,@revision)
                """, token, ("@generation", GuidValue(c, generation)), ("@d", plan.Key.DeploymentId),
                ("@key", Serialize(plan.Key)), ("@revision", revision)).ConfigureAwait(false);
        if (await ExecuteAsync(c, t, """
            UPDATE kafdeck_declarative_key_versions SET revision_high_water=@next
            WHERE deployment_id=@d AND scope_kind=@sk AND scope_id=@s AND resource_kind=@rk AND resource_id=@r AND revision_high_water=@high
            """, token, KeyValues(plan.Key).Concat([("@next", (object?)revision), ("@high", high)]).ToArray()).ConfigureAwait(false) != 1) return null;
        var state = plan.Command == W68RuntimePlanCommand.Relinquish ? "Relinquished" : "Managed";
        var values = KeyValues(plan.Key).Concat(new (string, object?)[]
        {
            ("@owner",plan.OwnerId),("@source",plan.SourceId),("@sourceGen",GuidValue(c,plan.SourceGeneration)),
            ("@sourceRev",plan.SourceRevisionHash),("@generation",GuidValue(c,generation)),("@revision",revision),
            ("@epoch",plan.LeaseEpoch),("@state",state),("@evidence",plan.PlanFingerprint),
            ("@principal",W68RuntimeFingerprints.Desired("W68/principal/v1:" + plan.RequesterPrincipalId)),("@policy",plan.PolicyFingerprint),("@op",plan.PlanId.ToString("D")),
            ("@now",Timestamp(c,now)),("@old",owner?.Revision ?? 0),
        }).ToArray();
        var changed = await ExecuteAsync(c, t, owner is null ? """
            INSERT INTO kafdeck_declarative_ownership(deployment_id,scope_kind,scope_id,resource_kind,resource_id,
                registered_owner_id,registered_source_id,registered_source_generation_id,pinned_source_revision,
                ownership_generation_id,ownership_revision,stored_lease_epoch,ownership_state,evidence_fingerprint,
                claimant_principal_id,policy_version,audit_operation_id,updated_at_utc)
            VALUES(@d,@sk,@s,@rk,@r,@owner,@source,@sourceGen,@sourceRev,@generation,@revision,@epoch,@state,@evidence,@principal,@policy,@op,@now)
            ON CONFLICT(deployment_id,scope_kind,scope_id,resource_kind,resource_id) DO NOTHING
            """ : """
            UPDATE kafdeck_declarative_ownership SET registered_owner_id=@owner,registered_source_id=@source,
                registered_source_generation_id=@sourceGen,pinned_source_revision=@sourceRev,ownership_generation_id=@generation,
                ownership_revision=@revision,stored_lease_epoch=@epoch,ownership_state=@state,evidence_fingerprint=@evidence,
                claimant_principal_id=@principal,policy_version=@policy,audit_operation_id=@op,updated_at_utc=@now
            WHERE deployment_id=@d AND scope_kind=@sk AND scope_id=@s AND resource_kind=@rk AND resource_id=@r AND ownership_revision=@old
            """, token, values).ConfigureAwait(false);
        if (changed != 1) return null;
        await ExecuteAsync(c, t, """
            INSERT INTO kafdeck_declarative_ownership_audit(operation_id,deployment_id,scope_kind,scope_id,resource_kind,resource_id,
                principal_id,owner_id,source_id,lease_epoch,new_revision,event_fingerprint)
            VALUES(@op,@d,@sk,@s,@rk,@r,@principal,@owner,@source,@epoch,@revision,@evidence)
            """, token, values).ConfigureAwait(false);
        receipt = new W68RuntimeCommitReceipt(plan.PlanId, plan.PlanFingerprint, revision, generation,
            next?.Revision ?? 0, await NowAsync(c, t, token).ConfigureAwait(false));
        await ExecuteAsync(c, t, """
            INSERT INTO kafdeck_declarative_commit_receipts(operation_id,plan_fingerprint,receipt_json)
            VALUES(@id,@hash,@json)
            """, token, ("@id", plan.PlanId.ToString("D")), ("@hash", plan.PlanFingerprint), ("@json", Serialize(receipt))).ConfigureAwait(false);
        await AuditAsync(c, t, plan.PlanId, "Commit", revision, plan.Key.DeploymentId, plan.RequesterPrincipalId, plan.PlanFingerprint, token).ConfigureAwait(false);
        // Row locks remain held through the last live authorization and DB-time
        // lease check. Expiry during an async policy check rolls everything back.
        if (!await currentAuthority(plan, token).ConfigureAwait(false) ||
            !await RegistrationsAllowAsync(c, t, plan, token).ConfigureAwait(false) ||
            !await LeaseAllowsAsync(c, t, plan, token).ConfigureAwait(false) ||
            plan.ExpiresAtUtc <= await NowAsync(c, t, token).ConfigureAwait(false)) return null;
        if (await ExecuteAsync(c, t, """
            UPDATE kafdeck_declarative_controller_leases SET expires_at_utc=@now
            WHERE deployment_id=@d AND lease_scope_id=@scope AND holder_id=@holder AND epoch=@epoch
            """, token, ("@now", Timestamp(c, await NowAsync(c, t, token).ConfigureAwait(false))),
            ("@d", plan.Key.DeploymentId), ("@scope", plan.LeaseScopeId), ("@holder", plan.LeaseHolderId),
            ("@epoch", plan.LeaseEpoch)).ConfigureAwait(false) != 1) return null;
        await t.CommitAsync(token).ConfigureAwait(false);
        return receipt;
    }

    private static bool SubscriptionMatches(W68StoredRuntimePlan plan, NotificationSubscriptionSnapshot? current)
    {
        if (current is null)
            return plan.ExpectedSubscriptionRevision is null && plan.ObservedDestinationId is null &&
                (plan.ObservedSubscriptionFingerprint == W68RuntimeFingerprints.MissingSubscription || plan.ObservedSubscriptionFingerprint == "missing") &&
                (plan.Command == W68RuntimePlanCommand.Relinquish ||
                    plan.Command == W68RuntimePlanCommand.Apply && plan.ExpectedOwnershipRevision == 0 && plan.ExpectedOwnershipGeneration is null);
        return plan.ExpectedSubscriptionRevision == current.Revision && plan.ObservedDestinationId == current.Definition.DestinationId &&
            (plan.Command == W68RuntimePlanCommand.Relinquish || current.State != NotificationSubscriptionState.Retired) &&
            W68RuntimeFingerprints.Subscription(current) == plan.ObservedSubscriptionFingerprint;
    }

    private sealed record CurrentPlanState(W68RuntimeOwnership? Ownership, NotificationSubscriptionSnapshot? Subscription, DateTimeOffset ObservedAtUtc);
    private async Task<CurrentPlanState?> CurrentPlanStateAsync(DbConnection c, DbTransaction t, W68StoredRuntimePlan plan, CancellationToken token)
    {
        var now = await NowAsync(c, t, token).ConfigureAwait(false);
        if (!await GenerationAllowsAsync(c, t, plan.Key.DeploymentId, token).ConfigureAwait(false) ||
            plan.DeploymentGeneration != (_requiredGeneration ?? Guid.Empty) || plan.ExpiresAtUtc <= now ||
            !await RegistrationsAllowAsync(c, t, plan, token).ConfigureAwait(false) ||
            !await LeaseAllowsAsync(c, t, plan, token).ConfigureAwait(false)) return null;
        var owner = await OwnershipAsync(c, t, plan.Key, true, token).ConfigureAwait(false);
        if (!OwnershipMatches(plan, owner)) return null;
        var subscription = await AdoNotificationRoutingStore.ReadSubscriptionInTransactionAsync(c, t, plan.Key.ResourceId,
            connections.SupportsSelectForUpdate, token).ConfigureAwait(false);
        return SubscriptionMatches(plan, subscription) ? new(owner, subscription, now) : null;
    }

    private static bool OwnershipMatches(W68StoredRuntimePlan plan, W68RuntimeOwnership? owner)
    {
        if (owner is null)
            return plan.ExpectedOwnershipRevision == 0 && plan.ExpectedOwnershipGeneration is null &&
                (plan.Command == W68RuntimePlanCommand.Adopt || plan.Command == W68RuntimePlanCommand.Apply && plan.ExpectedSubscriptionRevision is null);
        if (owner.Revision != plan.ExpectedOwnershipRevision || owner.GenerationId != plan.ExpectedOwnershipGeneration) return false;
        if (owner.State == "Relinquished") return plan.Command == W68RuntimePlanCommand.Adopt;
        return owner.State == "Managed" && plan.Command != W68RuntimePlanCommand.Adopt && owner.OwnerId == plan.OwnerId &&
            owner.SourceId == plan.SourceId && owner.SourceGeneration == plan.SourceGeneration &&
            owner.SourceRevisionHash == (plan.ExpectedOwnedSourceRevisionHash ?? plan.SourceRevisionHash) &&
            owner.LeaseEpoch <= plan.LeaseEpoch;
    }

    private async Task<bool> RegistrationsAllowAsync(DbConnection c, DbTransaction t, W68StoredRuntimePlan p, CancellationToken token)
    {
        if (!await ControllerAllowsAsync(c, t, p.Key.DeploymentId, p.ControllerId, p.OwnerId, p.RequesterPrincipalId, token).ConfigureAwait(false)) return false;
        var source = await RegistrationAsync(c, t, W68RuntimeRegistrationKind.Source, p.Key.DeploymentId, p.SourceId, token).ConfigureAwait(false);
        return source is { Enabled: true } && source.OwnerId == p.OwnerId && source.GenerationId == p.SourceGeneration &&
            source.PinnedRevision == p.SourceRevisionHash && source.Revision == p.SourceRegistrationRevision;
    }
    private static async Task<bool> OwnerEnabledAsync(DbConnection c, DbTransaction t, string deployment, string owner, CancellationToken token) =>
        await RegistrationAsync(c, t, W68RuntimeRegistrationKind.Owner, deployment, owner, token).ConfigureAwait(false) is { Enabled: true };
    private static async Task<bool> ControllerAllowsAsync(DbConnection c, DbTransaction t, string deployment, string controller,
        string owner, string principal, CancellationToken token)
    {
        var record = await RegistrationAsync(c, t, W68RuntimeRegistrationKind.Controller, deployment, controller, token).ConfigureAwait(false);
        return record is { Enabled: true } && record.PrincipalId == principal && record.OwnerId == owner &&
            await OwnerEnabledAsync(c, t, deployment, owner, token).ConfigureAwait(false);
    }
    private async Task<bool> LeaseAllowsAsync(DbConnection c, DbTransaction t, W68StoredRuntimePlan p, CancellationToken token)
    {
        var lease = await LeaseAsync(c, t, p.Key.DeploymentId, p.LeaseScopeId, token).ConfigureAwait(false);
        return lease is not null && lease.HolderId == p.LeaseHolderId && lease.Epoch == p.LeaseEpoch &&
            lease.ExpiresAtUtc > await NowAsync(c, t, token).ConfigureAwait(false) && lease.ExpiresAtUtc >= p.ExpiresAtUtc &&
            await BindingMatchesAsync(c, t, lease, p.ControllerId, p.RequesterPrincipalId, token).ConfigureAwait(false);
    }
    private static async Task<bool> BindingMatchesAsync(DbConnection c, DbTransaction t, W68RuntimeLease lease,
        string controller, string principal, CancellationToken token) =>
        Convert.ToInt64(await ScalarAsync(c, t, """
            SELECT COUNT(*) FROM kafdeck_declarative_lease_bindings WHERE deployment_id=@d AND lease_scope_id=@s
                AND holder_id=@holder AND epoch=@epoch AND controller_id=@controller AND principal_id=@principal
            """, token, ("@d", lease.DeploymentId), ("@s", lease.LeaseScopeId), ("@holder", lease.HolderId),
            ("@epoch", lease.Epoch), ("@controller", controller), ("@principal", principal)).ConfigureAwait(false), CultureInfo.InvariantCulture) == 1;

    private static async Task<W68RuntimeRegistration?> RegistrationAsync(DbConnection c, DbTransaction? t,
        W68RuntimeRegistrationKind kind, string deployment, string id, CancellationToken token)
    {
        await using var cmd = Command(c, t, """
            SELECT revision,registration_json FROM kafdeck_declarative_registrations
            WHERE deployment_id=@d AND kind=@kind AND registration_id=@id
            """, ("@d", deployment), ("@kind", kind.ToString()), ("@id", id));
        await using var r = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await r.ReadAsync(token).ConfigureAwait(false)) return null;
        var record = JsonSerializer.Deserialize<W68RuntimeRegistration>(r.GetString(1), Json)!;
        ValidateRegistration(record);
        if (record.Revision != r.GetInt64(0) || record.DeploymentId != deployment || record.RegistrationId != id || record.Kind != kind)
            throw new InvalidOperationException("Corrupt declarative registration binding.");
        return record;
    }
    private static async Task<W68StoredRuntimePlan?> PlanAsync(DbConnection c, DbTransaction? t, Guid id, CancellationToken token)
    {
        await using var cmd = Command(c, t, "SELECT plan_fingerprint,principal_id,plan_json FROM kafdeck_declarative_plans WHERE operation_id=@id", ("@id", id.ToString("D")));
        await using var r = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await r.ReadAsync(token).ConfigureAwait(false)) return null;
        var plan = JsonSerializer.Deserialize<W68StoredRuntimePlan>(r.GetString(2), Json)!;
        ValidatePlan(plan);
        if (plan.PlanId != id || plan.PlanFingerprint != r.GetString(0) || plan.RequesterPrincipalId != r.GetString(1))
            throw new InvalidOperationException("Corrupt immutable declarative plan binding.");
        return plan;
    }
    private static async Task<W68RuntimeCommitReceipt?> ReceiptAsync(DbConnection c, DbTransaction? t, Guid id, CancellationToken token)
    {
        await using var cmd = Command(c, t, "SELECT plan_fingerprint,receipt_json FROM kafdeck_declarative_commit_receipts WHERE operation_id=@id", ("@id", id.ToString("D")));
        await using var r = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await r.ReadAsync(token).ConfigureAwait(false)) return null;
        var receipt = JsonSerializer.Deserialize<W68RuntimeCommitReceipt>(r.GetString(1), Json)!;
        if (receipt.OperationId != id || receipt.PlanFingerprint != r.GetString(0) || receipt.OwnershipRevision < 1 || receipt.OwnershipGeneration == Guid.Empty)
            throw new InvalidOperationException("Corrupt declarative commit receipt.");
        return receipt;
    }
    private async Task<W68RuntimeLease?> LeaseAsync(DbConnection c, DbTransaction t, string deployment, string scope, CancellationToken token)
    {
        await using var cmd = Command(c, t, """
            SELECT holder_id,epoch,expires_at_utc FROM kafdeck_declarative_controller_leases
            WHERE deployment_id=@d AND lease_scope_id=@s
            """ + (connections.SupportsSelectForUpdate ? " FOR UPDATE" : ""), ("@d", deployment), ("@s", scope));
        await using var r = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        return await r.ReadAsync(token).ConfigureAwait(false) ? new W68RuntimeLease(deployment, scope, r.GetString(0), r.GetInt64(1), Utc(r.GetValue(2))) : null;
    }
    private async Task<W68RuntimeOwnership?> OwnershipAsync(DbConnection c, DbTransaction? t, W68RuntimeResourceKey key, bool locked, CancellationToken token)
    {
        await using var cmd = Command(c, t, """
            SELECT registered_owner_id,registered_source_id,registered_source_generation_id,pinned_source_revision,
                ownership_generation_id,ownership_revision,stored_lease_epoch,ownership_state,claimant_principal_id,policy_version
            FROM kafdeck_declarative_ownership
            WHERE deployment_id=@d AND scope_kind=@sk AND scope_id=@s AND resource_kind=@rk AND resource_id=@r
            """ + (locked && connections.SupportsSelectForUpdate ? " FOR UPDATE" : ""), KeyValues(key));
        await using var r = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        return await r.ReadAsync(token).ConfigureAwait(false) ? new W68RuntimeOwnership(key, r.GetString(0), r.GetString(1),
            ReadGuid(r.GetValue(2)), r.GetString(3), ReadGuid(r.GetValue(4)), r.GetInt64(5), r.GetInt64(6),
            r.GetString(7), r.GetString(8), r.GetString(9)) : null;
    }
    private static (string Name, object? Value)[] KeyValues(W68RuntimeResourceKey key) =>
        [("@d", key.DeploymentId), ("@sk", key.ScopeKind), ("@s", key.ScopeId), ("@rk", key.ResourceKind), ("@r", key.ResourceId)];

    public async Task<Guid?> GetDeploymentGenerationAsync(string deploymentId, CancellationToken token = default)
    {
        Id(deploymentId);
        await using var c = await connections.OpenAsync(token).ConfigureAwait(false);
        return await GenerationAsync(c, null, deploymentId, token).ConfigureAwait(false);
    }
    private async Task<bool> GenerationAllowsAsync(DbConnection c, DbTransaction t, string deployment, CancellationToken token) =>
        await GenerationAsync(c, t, deployment, token).ConfigureAwait(false) == _requiredGeneration;
    private static async Task<Guid?> GenerationAsync(DbConnection c, DbTransaction? t, string deployment, CancellationToken token)
    {
        var value = await ScalarAsync(c, t, "SELECT generation FROM kafdeck_declarative_deployments WHERE deployment_id=@d", token,
            ("@d", deployment)).ConfigureAwait(false);
        return value is string text ? Guid.Parse(text) : null;
    }
}
