using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;

namespace Kafdeck.Api;

/// <summary>
/// Independently gated OIDC-only subscription write surface. The read-only
/// notification feature never implicitly enables these routes. No destination
/// profile, provider transport, delivery worker or secret mutation is exposed.
/// </summary>
public static class KafdeckNotificationManagementEndpoints
{
    public static WebApplication MapKafdeckV08NotificationManagement(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapPut("/api/v1/notifications/subscriptions/{subscriptionId}",
                async (
                    string subscriptionId,
                    NotificationSubscriptionWriteRequest request,
                    HttpContext context,
                    [FromServices] KafdeckAuthorizationService authorization,
                    [FromServices] INotificationRoutingStore store,
                    [FromServices] ISecurityAuditSink audit,
                    CancellationToken cancellationToken) =>
                {
                    try
                    {
                        var definition = new NotificationSubscriptionDefinition(
                            subscriptionId,
                            request.DestinationId,
                            request.EventClasses,
                            request.EventTypes);

                        // Route permission is independently enforced by middleware.
                        // Verify the new destination BEFORE any write or disclosure.
                        if (!Allowed(authorization, context, definition.DestinationId))
                        {
                            await AuditDeniedAsync(context, audit, "rbac_denied_notification_new_destination");
                            return Forbidden();
                        }

                        var existing = await store.GetSubscriptionAsync(
                            subscriptionId, cancellationToken).ConfigureAwait(false);

                        if (existing is not null)
                        {
                            // Do not disclose the existing destination through a
                            // stale CAS error or move its subscription across RBAC.
                            if (!Allowed(authorization, context, existing.Definition.DestinationId))
                            {
                                await AuditDeniedAsync(context, audit, "rbac_denied_notification_existing_destination");
                                // Same outward result as a missing subscription: do not
                                // reveal existence outside the existing destination scope.
                                return NotFound();
                            }
                            if (existing.State == NotificationSubscriptionState.Retired)
                                return Conflict();
                        }

                        if (request.ExpectedRevision is null)
                        {
                            // Retirement requires an existing CAS revision; create-only
                            // must not permanently tombstone a never-active identity.
                            if (request.State == NotificationSubscriptionState.Retired)
                                return InvalidRequest();
                            if (existing is not null) return Conflict();
                            try
                            {
                                var created = await store.CreateSubscriptionAsync(
                                    definition, request.State, DateTimeOffset.UtcNow, cancellationToken)
                                    .ConfigureAwait(false);
                                return Results.Created(
                                    $"/api/v1/notifications/subscriptions/{Uri.EscapeDataString(subscriptionId)}",
                                    NotificationSubscriptionWriteReceipt.From(created));
                            }
                            catch (InvalidOperationException)
                            {
                                // Includes concurrent INSERT with the same identity.
                                return Conflict();
                            }
                            catch (SqliteException exception) when (IsSqliteContention(exception))
                            {
                                // Shared-cache SQLite can return LOCKED_SHAREDCACHE
                                // during overlapping creates, not only BUSY_SNAPSHOT.
                                return Conflict();
                            }
                        }

                        if (request.ExpectedRevision.Value < 1)
                            return InvalidRequest();

                        if (existing is null)
                            return NotFound();

                        if (request.ExpectedRevision.Value != existing.Revision)
                            return Conflict();

                        try
                        {
                            var updated = await store.ReplaceSubscriptionAsync(
                                definition, request.State, request.ExpectedRevision.Value,
                                DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                            return updated is null ? Conflict() : Results.Ok(
                                NotificationSubscriptionWriteReceipt.From(updated));
                        }
                        catch (SqliteException exception) when (IsSqliteContention(exception))
                        {
                            // SQLite WAL/snapshot and shared-cache writer locks are
                            // recoverable concurrent-write conflicts, not HTTP 500.
                            return Conflict();
                        }
                    }
                    catch (ArgumentException)
                    {
                        return InvalidRequest();
                    }
                })
            .WithName("v08-notification-subscription-upsert")
            .RequireKafdeckAuthorization(
                AuthorizationAction.NotificationManage,
                resourceRouteKey: "subscriptionId")
            .RequireKafdeckAntiforgery();

        return app;
    }

    /// <summary>
    /// SQLite primary BUSY (5) or LOCKED (6), including BUSY_SNAPSHOT=517
    /// and LOCKED_SHAREDCACHE=262, are write contention outcomes. Scoped to
    /// subscription create/replace, never to connection/other SQL failures.
    /// </summary>
    public static bool IsSqliteContention(SqliteException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception.SqliteErrorCode is 5 or 6;
    }

    private static async Task AuditDeniedAsync(HttpContext context, ISecurityAuditSink audit, string code)
    {
        var hasOperator = OperatorSessionContextFactory.TryCreate(context.User, out var session);
        await audit.WriteAsync(new SecurityAuditEvent(
            DateTimeOffset.UtcNow,
            SecurityAuditEventType.AuthorizationDenied,
            hasOperator && session is not null
                ? SecurityAuditPrincipal.FromOperator(session.Identity)
                : SecurityAuditPrincipal.Anonymous,
            session?.SessionId.Value.ToString("N"),
            null, null, SecurityAuditOutcome.Denied, code),
            context.RequestAborted).ConfigureAwait(false);
    }

    private static bool Allowed(
        KafdeckAuthorizationService authorization, HttpContext context, string destinationId) =>
        authorization.Authorize(
            context.User,
            new AuthorizationRequest(AuthorizationAction.NotificationManage,
                ResourceName: destinationId)) == KafdeckAuthorizationOutcome.Allowed;

    private static IResult Conflict() =>
        Results.Problem(statusCode: StatusCodes.Status409Conflict,
            type: "urn:kafdeck:problem:notification-subscription-conflict",
            title: "Notification subscription state or revision conflict");

    private static IResult InvalidRequest() =>
        Results.Problem(statusCode: StatusCodes.Status400BadRequest,
            type: "urn:kafdeck:problem:invalid-notification-subscription",
            title: "Invalid notification subscription request");

    private static IResult NotFound() =>
        Results.Problem(statusCode: StatusCodes.Status404NotFound,
            type: "urn:kafdeck:problem:notification-subscription-not-found",
            title: "Notification subscription not found");

    private static IResult Forbidden() =>
        Results.Problem(statusCode: StatusCodes.Status403Forbidden,
            type: "urn:kafdeck:problem:operator-authorization-denied",
            title: "Forbidden");
}

public sealed record NotificationSubscriptionWriteRequest(
    string DestinationId,
    NotificationSubscriptionState State,
    IReadOnlyList<NotificationEventClass> EventClasses,
    IReadOnlyList<string>? EventTypes,
    long? ExpectedRevision);


// Return only the revision needed for future CAS writes. NotificationManage
// permission does not imply NotificationRead; do not expose the stored
// destination, class/type filters or any read-only evidence on a write route.
public sealed record NotificationSubscriptionWriteReceipt(
    string SubscriptionId,
    NotificationSubscriptionState State,
    long Revision)
{
    public static NotificationSubscriptionWriteReceipt From(NotificationSubscriptionSnapshot value) =>
        new(value.Definition.SubscriptionId, value.State, value.Revision);
}
