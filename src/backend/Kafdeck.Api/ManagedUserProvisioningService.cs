using System.Net;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

internal sealed record ManagedUserProvisioningResult(
    int Status, string? Code = null, ManagedUser? User = null, Guid? OperationId = null);

// The file lease fences dispatch and reconciliation across processes. No password
// is persisted, and no request with an unknown external outcome is dispatched again.
internal sealed class ManagedUserProvisioningService(
    ManagedIdentityStore store, ManagedKeycloakUsers provider, ISecurityAuditSink audit,
    string[] availableClusters)
{
    internal async Task<ManagedUserProvisioningResult> Create(
        OperatorSessionContext owner, ManagedIdentityEndpoints.NewUserInput input)
    {
        using var lease = store.TryLock();
        if (lease is null) return Failure(409, "identity-busy");
        if (!ManagedIdentityEndpoints.IsOwner(store, owner)) return Failure(403, "identity-forbidden");
        if (!ManagedIdentityEndpoints.TryCredentials(input.Email, input.Password, out var email) ||
            !ManagedIdentityEndpoints.TryAccess(input.Role, input.ClusterIds, availableClusters, out var clusters) ||
            input.RequestId == Guid.Empty) return Failure(400, "identity-input");
        var state = store.Read();
        var requesterId = store.Find(owner)!.Id;
        var intents = state.UserProvisionings ?? [];
        var intent = input.RequestId is { } requestId
            ? intents.SingleOrDefault(item => item.RequestId == requestId) : null;
        intent ??= intents.FirstOrDefault(item => item.Email == email && !item.Completed && item.RejectionCode is null);
        if (intent is not null)
        {
            if (intent.RequestedBy != requesterId || intent.Email != email || intent.Role != input.Role ||
                !intent.ClusterIds.SequenceEqual(clusters, StringComparer.Ordinal) ||
                (input.RequestId is { } key && key != intent.RequestId))
                return Failure(409, "identity-provisioning-conflict");
            return await Resume(state, intent);
        }
        if (state.Users.Any(user => string.Equals(user.Email, email, StringComparison.OrdinalIgnoreCase)))
            return Failure(409, "identity-duplicate");
        if (intents.Length >= 2048 || state.Users.Length + intents.Count(item =>
                !item.Completed && item.RejectionCode is null) >= 1000) return Failure(409, "identity-limit");
        try
        {
            if (!await provider.ProvisioningProfileReady()) return Failure(503, "identity-provisioning-profile");
        }
        catch (Exception error) when (ProviderFailed(error)) { return Failure(502, "identity-provider"); }

        intent = new(input.RequestId ?? Guid.NewGuid(), requesterId, email, input.Role!, clusters,
            Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(32)), DateTimeOffset.UtcNow);
        state = state with { UserProvisionings = [.. intents, intent] };
        try { store.Write(state); }
        catch (Exception error) when (StorageFailed(error)) { return Failure(503, "identity-storage"); }
        try
        {
            var (id, status) = await provider.CreateProvisioned(email, input.Password!, intent.Proof);
            if (id is null)
            {
                if (status is HttpStatusCode.BadRequest or HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden or HttpStatusCode.Conflict)
                {
                    var code = status == HttpStatusCode.Conflict ? "identity-duplicate" :
                        status == HttpStatusCode.BadRequest ? "identity-input" : "identity-provider";
                    intent = intent with { RejectionCode = code };
                    store.Write(Replace(state, intent));
                    return Failure(status == HttpStatusCode.Conflict ? 409 : status == HttpStatusCode.BadRequest ? 400 : 502, code);
                }
                return Pending(intent);
            }
            intent = intent with { UserId = id };
            state = Replace(state, intent);
            store.Write(state); // Retain the positive receipt before local membership.
            return await Resume(state, intent);
        }
        catch (Exception error) when (ProviderFailed(error) || StorageFailed(error)) { return Pending(intent); }
    }

    internal async Task<ManagedUserProvisioningResult> Recover(OperatorSessionContext owner, Guid requestId)
    {
        using var lease = store.TryLock();
        if (lease is null) return Failure(409, "identity-busy");
        if (!ManagedIdentityEndpoints.IsOwner(store, owner)) return Failure(403, "identity-forbidden");
        var state = store.Read();
        var intent = state.UserProvisionings?.SingleOrDefault(item => item.RequestId == requestId);
        if (intent is null || intent.RequestedBy != store.Find(owner)!.Id) return Failure(404, "identity-provisioning-missing");
        return await Resume(state, intent);
    }

    private async Task<ManagedUserProvisioningResult> Resume(ManagedIdentityState state, ManagedUserProvisioning intent)
    {
        if (intent.RejectionCode is { } rejection) return Failure(409, rejection);
        if (!ManagedIdentityEndpoints.TryAccess(intent.Role, intent.ClusterIds, availableClusters, out _))
            return Failure(409, "identity-provisioning-conflict", intent.RequestId);
        try
        {
            ManagedUser? user;
            if (intent.Completed)
            {
                user = state.Users.SingleOrDefault(item => item.Id == intent.UserId);
                if (user is null) return Failure(409, "identity-provisioning-conflict", intent.RequestId);
            }
            else
            {
                if (!await provider.ProvisioningProfileReady()) return Failure(503, "identity-provisioning-profile", intent.RequestId);
                var id = await provider.FindProvisioned(intent.Email, intent.Proof, intent.UserId);
                if (id is null) return Pending(intent);
                if (id == state.OwnerId || state.Users.Any(item => item.Id == id ||
                    string.Equals(item.Email, intent.Email, StringComparison.OrdinalIgnoreCase)))
                    return Failure(409, "identity-provisioning-conflict", intent.RequestId);
                user = new(id.Value, intent.Email, intent.Role, intent.ClusterIds, true, default);
                intent = intent with { UserId = id, Completed = true };
                state = Replace(state with { Users = [.. state.Users, user] }, intent);
                store.Write(state);
            }
            if (!intent.Audited)
            {
                // At-least-once delivery; RequestId is stable for deduplication if
                // a crash occurs after audit delivery but before its receipt write.
                await audit.WriteAsync(new SecurityAuditEvent(DateTimeOffset.UtcNow, SecurityAuditEventType.ManagedIdentityChanged,
                    $"oidc:{state.Issuer}|{intent.RequestedBy:D}", intent.RequestId.ToString("N"), null,
                    user.Id.ToString("D"), SecurityAuditOutcome.Succeeded, "managed_user_created"));
                intent = intent with { Audited = true };
                store.Write(Replace(state, intent));
            }
            return new(201, User: user, OperationId: intent.RequestId);
        }
        catch (Exception error) when (ProviderFailed(error) || StorageFailed(error)) { return Pending(intent); }
    }

    private static ManagedIdentityState Replace(ManagedIdentityState state, ManagedUserProvisioning intent) =>
        state with { UserProvisionings = state.UserProvisionings!.Select(item => item.RequestId == intent.RequestId ? intent : item).ToArray() };
    private static ManagedUserProvisioningResult Pending(ManagedUserProvisioning intent) =>
        Failure(409, "identity-provisioning-pending", intent.RequestId);
    private static ManagedUserProvisioningResult Failure(int status, string code, Guid? operation = null) => new(status, code, OperationId: operation);
    private static bool ProviderFailed(Exception error) => error is HttpRequestException or TaskCanceledException or JsonException or InvalidDataException;
    private static bool StorageFailed(Exception error) => error is IOException or UnauthorizedAccessException;
}
