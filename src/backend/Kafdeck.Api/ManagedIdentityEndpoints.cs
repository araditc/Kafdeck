using System.Net;
using System.Net.Mail;
using System.Text.Json;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

internal static class ManagedIdentityEndpoints
{
    internal static string SetupState(ManagedIdentityState state) =>
        state.OwnerId is not null ? "complete" : state.Provisioning ? "recovery" : "available";

    internal static bool IsOwner(ManagedIdentityStore store, OperatorSessionContext session)
    {
        var user = store.Find(session);
        return user?.Role == "owner" && ManagedIdentityStore.SessionAllowed(user, session);
    }

    internal static void MapManagedIdentity(this WebApplication app, ManagedIdentityStore store, KafdeckOptions options)
    {
        Dictionary<string, string> ClusterNames() => options.Clusters.Select(cluster => (cluster.Id, Name: cluster.Id))
            .Concat(KafkaConnectionWorkspace.ReadRegistered(store).Select(cluster => (cluster.Id, cluster.Name)))
            .ToDictionary(cluster => cluster.Id, cluster => cluster.Name, StringComparer.Ordinal);

        app.MapGet("/api/v1/setup", (HttpContext context, IAntiforgery antiforgery) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            var state = SetupState(store.Read());
            var tokens = state == "available" ? antiforgery.GetAndStoreTokens(context) : null;
            return Results.Ok(new { state, csrf = tokens?.RequestToken });
        });

        app.MapPost("/api/v1/setup", async (SetupInput input, HttpContext context, ManagedKeycloakUsers provider, ISecurityAuditSink audit) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            using var lease = store.TryLock();
            if (lease is null) return Problem(409, "identity-busy");
            var state = store.Read();
            if (SetupState(state) != "available") return Problem(409, "setup-closed");
            if (input.Code is null || !ManagedIdentityStore.MatchesCode(state, input.Code.Trim()))
                return Problem(403, "setup-code");
            if (!TryCredentials(input.Email, input.Password, out var email))
                return Problem(400, "identity-input");

            try
            {
                if (await provider.HasExistingUsers())
                    return Problem(409, "setup-realm-in-use");
            }
            catch (Exception error) when (ProviderFailed(error)) { return Problem(502, "identity-provider"); }

            // Persist the reservation before writing to the other system. An
            // indeterminate provider outcome cannot reopen first-admin setup.
            var reserved = state with { Provisioning = true, CodeHash = null, CodeExpires = default };
            store.Write(reserved);
            try
            {
                var (id, status) = await provider.Create(email, input.Password!, temporary: false);
                if (id is null)
                {
                    if (status is HttpStatusCode.BadRequest or HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden or HttpStatusCode.Conflict)
                        store.Write(reserved with { Provisioning = false });
                    return CreateProblem(status);
                }
                var owner = new ManagedUser(id.Value, email, "owner", [], true, default);
                store.Write(reserved with { OwnerId = id, Provisioning = false, Users = [owner] });
                await Audit(audit, SecurityAuditPrincipal.Anonymous, id.Value, "initial_owner_created");
                return Results.NoContent();
            }
            catch (Exception error) when (ProviderFailed(error)) { return Problem(502, "identity-provider"); }
        })
        .WithMetadata(new RequestSizeLimitAttribute(8192))
        .RequireKafdeckAntiforgery();

        app.MapGet("/api/v1/identity/users", (HttpContext context) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            if (Owner(context, store) is null) return Problem(403, "identity-forbidden");
            var clusterNames = ClusterNames();
            return Results.Ok(new { users = store.Read().Users.Select(PublicUser), clusterIds = clusterNames.Keys, clusterNames });
        });

        app.MapPost("/api/v1/identity/users", async (
            NewUserInput input, HttpContext context, ManagedKeycloakUsers provider, ISecurityAuditSink audit) =>
        {
            var owner = Owner(context, store);
            if (owner is null) return Problem(403, "identity-forbidden");
            using var lease = store.TryLock();
            if (lease is null) return Problem(409, "identity-busy");
            if (!TryCredentials(input.Email, input.Password, out var email) ||
                !TryAccess(input.Role, input.ClusterIds, ClusterNames().Keys, out var assigned))
                return Problem(400, "identity-input");
            var state = store.Read();
            if (state.Users.Length >= 1000) return Problem(409, "identity-limit");
            if (state.Users.Any(user => string.Equals(user.Email, email, StringComparison.OrdinalIgnoreCase)))
                return Problem(409, "identity-duplicate");
            try
            {
                var (id, status) = await provider.Create(email, input.Password!, temporary: true);
                if (id is null) return CreateProblem(status);
                var user = new ManagedUser(id.Value, email, input.Role!, assigned, true, default);
                store.Write(state with { Users = [.. state.Users, user] });
                await Audit(audit, SecurityAuditPrincipal.FromOperator(owner.Identity), id.Value, "managed_user_created");
                return Results.Json(PublicUser(user), statusCode: 201);
            }
            catch (Exception error) when (ProviderFailed(error)) { return Problem(502, "identity-provider"); }
        })
        .WithMetadata(new RequestSizeLimitAttribute(16384))
        .RequireKafdeckAntiforgery();

        app.MapPut("/api/v1/identity/users/{id:guid}", async (
            Guid id, UpdateUserInput input, HttpContext context, ManagedKeycloakUsers provider, ISecurityAuditSink audit) =>
        {
            var owner = Owner(context, store);
            if (owner is null) return Problem(403, "identity-forbidden");
            using var lease = store.TryLock();
            if (lease is null) return Problem(409, "identity-busy");
            var state = store.Read();
            if (id == state.OwnerId) return Problem(409, "identity-owner");
            var existing = state.Users.SingleOrDefault(user => user.Id == id);
            if (existing is null) return Problem(404, "identity-missing");
            if (input.Enabled is not { } enabled || !TryAccess(input.Role, input.ClusterIds, ClusterNames().Keys, out var assigned))
                return Problem(400, "identity-input");

            var changed = existing with
            {
                Role = input.Role!, ClusterIds = assigned, Enabled = enabled,
                SessionsValidFrom = existing.Enabled == enabled ? existing.SessionsValidFrom : DateTimeOffset.UtcNow,
            };
            var updated = state with { Users = state.Users.Select(user => user.Id == id ? changed : user).ToArray() };
            try
            {
                if (!enabled) store.Write(updated); // Revoke locally even if Keycloak is temporarily unavailable.
                if (!enabled || !existing.Enabled) await provider.SetEnabled(id, enabled);
                if (enabled) store.Write(updated);
                await Audit(audit, SecurityAuditPrincipal.FromOperator(owner.Identity), id, "managed_user_updated");
                return Results.Ok(PublicUser(changed));
            }
            catch (Exception error) when (ProviderFailed(error)) { return Problem(502, "identity-provider"); }
        })
        .WithMetadata(new RequestSizeLimitAttribute(16384))
        .RequireKafdeckAntiforgery();
    }

    internal static bool Allows(ManagedIdentityStore store, OperatorSessionContext session, AuthorizationAction action,
        string? clusterId, bool registeredWrites = false, bool schemaRegistryConfigured = false)
    {
        var user = store.Find(session);
        if (user is null || !ManagedIdentityStore.SessionAllowed(user, session)) return false;
        if (action == AuthorizationAction.SystemRead) return true;
        if (clusterId is null || (user.Role != "owner" && !user.ClusterIds.Contains(clusterId, StringComparer.Ordinal))) return false;
        if (action is AuthorizationAction.RecordRead or AuthorizationAction.RecordExport)
            return CanManageRegisteredTarget(user, clusterId) && IsRegisteredTarget(store, clusterId);
        if (action == AuthorizationAction.SchemaCreate)
            return registeredWrites && schemaRegistryConfigured && CanManageRegisteredTarget(user, clusterId);
        if (IsRegisteredMutationAction(action))
            return registeredWrites && CanManageRegisteredTarget(user, clusterId) && IsRegisteredTarget(store, clusterId);
        return action is AuthorizationAction.ClusterRead or AuthorizationAction.BrokerRead or AuthorizationAction.TopicList or AuthorizationAction.TopicRead or AuthorizationAction.SchemaRead ||
            (user.Role is "owner" or "operator" or "manager" &&
                action is AuthorizationAction.TopicConfigRead or AuthorizationAction.BrokerConfigRead or AuthorizationAction.ConsumerRead or AuthorizationAction.ConnectRead or AuthorizationAction.KsqlRead);
    }

    internal static bool IsRegisteredMutationAction(AuthorizationAction action) =>
        action is AuthorizationAction.TopicCreate or AuthorizationAction.TopicAlter or AuthorizationAction.TopicDelete or
            AuthorizationAction.RecordProduce or AuthorizationAction.RecordsPurge or
            AuthorizationAction.ConsumerOffsetAlter or AuthorizationAction.ConsumerDelete or
            AuthorizationAction.SchemaCreate or AuthorizationAction.ConnectCreate or AuthorizationAction.ConnectAlter or
            AuthorizationAction.ConnectDelete or AuthorizationAction.MutationApprove;

    internal static bool IsRegisteredTarget(ManagedIdentityStore store, string? id) =>
        id?.StartsWith("connection-", StringComparison.Ordinal) == true &&
        KafkaConnectionWorkspace.ReadRegistered(store).Any(target => target.Id == id);

    internal static bool CanManageRegisteredTarget(ManagedUser? user, string? id) =>
        user is { Enabled: true } && id is not null &&
        (user.Role == "owner" || user.Role == "manager" && user.ClusterIds.Contains(id, StringComparer.Ordinal));

    private static OperatorSessionContext? Owner(HttpContext context, ManagedIdentityStore store) =>
        context.User.Identity is not NonBrowserReadIdentity &&
        OperatorSessionContextFactory.TryCreate(context.User, out var session) &&
        session is not null && IsOwner(store, session) ? session : null;

    private static object PublicUser(ManagedUser user) =>
        new { user.Id, user.Email, user.Role, user.ClusterIds, user.Enabled };

    private static bool TryCredentials(string? address, string? password, out string email)
    {
        email = address?.Trim().ToLowerInvariant() ?? "";
        return email.Length is > 0 and <= 254 && !email.Any(char.IsControl) &&
            MailAddress.TryCreate(email, out var parsed) && parsed.Address == email &&
            password is { Length: >= 12 and <= 128 } && !string.IsNullOrWhiteSpace(password) && !password.Any(char.IsControl);
    }

    private static bool TryAccess(string? role, string[]? clusters, ICollection<string> available, out string[] assigned)
    {
        assigned = clusters?.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray() ?? [];
        return role is "viewer" or "operator" or "manager" && clusters is not null &&
            assigned.All(id => available.Contains(id, StringComparer.Ordinal));
    }

    private static bool ProviderFailed(Exception error) =>
        error is HttpRequestException or TaskCanceledException or JsonException or InvalidDataException;

    private static IResult CreateProblem(HttpStatusCode status) => status switch
    {
        HttpStatusCode.Conflict => Problem(409, "identity-duplicate"),
        HttpStatusCode.BadRequest => Problem(400, "identity-input"),
        _ => Problem(502, "identity-provider"),
    };

    private static IResult Problem(int status, string code) =>
        Results.Problem(statusCode: status, type: $"urn:kafdeck:problem:{code}", title: "Managed identity operation could not be completed.");

    private static ValueTask Audit(ISecurityAuditSink audit, string principal, Guid user, string category) =>
        audit.WriteAsync(new SecurityAuditEvent(DateTimeOffset.UtcNow, SecurityAuditEventType.ManagedIdentityChanged,
            principal, null, null, user.ToString("D"), SecurityAuditOutcome.Succeeded, category));

    internal sealed record SetupInput(string? Code, string? Email, string? Password);
    internal sealed record NewUserInput(string? Email, string? Password, string? Role, string[]? ClusterIds);
    internal sealed record UpdateUserInput(string? Role, string[]? ClusterIds, bool? Enabled);
}
