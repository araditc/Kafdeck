using System.Security.Claims;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

/// <summary>
/// Optional metadata-only notification control-plane observation.
/// No destination mutation, event dispatch, worker activation or remote
/// provider operation is reachable through these endpoints.
/// </summary>
public static class KafdeckNotificationReadEndpoints
{
    public static WebApplication MapKafdeckV08NotificationReads(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet(
                "/api/v1/notifications/subscriptions",
                async (
                    HttpContext context,
                    int? maxResults,
                    string? afterSubscriptionId,
                    NotificationSubscriptionState? state,
                    NotificationEventClass? eventClass,
                    [FromServices] KafdeckAuthorizationService authorization,
                    [FromServices] INotificationRoutingStore store,
                    CancellationToken cancellationToken) =>
                {
                    var admission = await RequireCollectionReadAsync(
                        context, authorization);
                    if (admission is not null)
                    {
                        return admission;
                    }

                    try
                    {
                        // A response is bounded even when every returned
                        // subscription is hidden by resource-scoped RBAC.
                        var page = await store.ListSubscriptionsAsync(
                                new NotificationSubscriptionQuery(
                                    maxResults ?? 50,
                                    afterSubscriptionId,
                                    state,
                                    eventClass),
                                cancellationToken)
                            .ConfigureAwait(false);

                        return Results.Ok(
                            NotificationSubscriptionReadProjection.Project(
                                page,
                                item =>
                                    IsAllowed(
                                        authorization,
                                        context.User,
                                        item.Definition.SubscriptionId) &&
                                    IsAllowed(
                                        authorization,
                                        context.User,
                                        item.Definition.DestinationId)));
                    }
                    catch (ArgumentException)
                    {
                        return InvalidQuery();
                    }
                })
            .WithName("v08-notification-subscriptions-read");

        app.MapGet(
                "/api/v1/notifications/subscriptions/{subscriptionId}",
                async (
                    string subscriptionId,
                    HttpContext context,
                    [FromServices] KafdeckAuthorizationService authorization,
                    [FromServices] INotificationRoutingStore store,
                    CancellationToken cancellationToken) =>
                {
                    try
                    {
                        var snapshot = await store.GetSubscriptionAsync(
                                subscriptionId,
                                cancellationToken)
                            .ConfigureAwait(false);
                        if (snapshot is null ||
                            !IsAllowed(
                                authorization,
                                context.User,
                                snapshot.Definition.DestinationId))
                        {
                            return NotFound();
                        }

                        return Results.Ok(
                            NotificationSubscriptionData.From(snapshot));
                    }
                    catch (ArgumentException)
                    {
                        return InvalidQuery();
                    }
                })
            .WithName("v08-notification-subscription-detail")
            .RequireKafdeckAuthorization(
                AuthorizationAction.NotificationRead,
                resourceRouteKey: "subscriptionId");

        // Exact destination identity is authorized before invoking persistence.
        // A continuation only traverses rows already in that same destination.
        app.MapGet(
                "/api/v1/notifications/destinations/{destinationId}/deliveries",
                async (
                    string destinationId,
                    int? maxResults,
                    DateTimeOffset? afterCreatedAtUtc,
                    Guid? afterNotificationId,
                    [FromServices] INotificationDeliveryHistoryReader store,
                    CancellationToken cancellationToken) =>
                {
                    if ((afterCreatedAtUtc is null) !=
                        (afterNotificationId is null))
                        return InvalidQuery();

                    try
                    {
                        var after = afterCreatedAtUtc is not null
                            ? new NotificationDestinationDeliveryCursor(
                                afterCreatedAtUtc.Value, afterNotificationId!.Value)
                            : null;
                        var page = await store.ListByDestinationAsync(
                            new NotificationDestinationDeliveryQuery(
                                destinationId, maxResults ?? 50, after),
                            cancellationToken).ConfigureAwait(false);
                        return Results.Ok(new NotificationDeliveryHistoryListData(
                            page.Items.Select(NotificationDeliveryEvidenceData.From).ToArray(),
                            page.Truncated,
                            page.Next?.CreatedAtUtc,
                            page.Next?.NotificationId));
                    }
                    catch (ArgumentException)
                    {
                        return InvalidQuery();
                    }
                })
            .WithName("v08-notification-destination-deliveries-read")
            .RequireKafdeckAuthorization(
                AuthorizationAction.NotificationRead,
                resourceRouteKey: "destinationId");

        app.MapGet(
                "/api/v1/notifications/deliveries/{notificationId:guid}/{destinationId}",
                async (
                    Guid notificationId,
                    string destinationId,
                    [FromServices] INotificationDeliveryStore store,
                    CancellationToken cancellationToken) =>
                {
                    try
                    {
                        var record = await store.GetAsync(
                                notificationId,
                                destinationId,
                                cancellationToken)
                            .ConfigureAwait(false);
                        return record is null
                            ? NotFound()
                            : Results.Ok(
                                NotificationDeliveryEvidenceData.From(
                                    record));
                    }
                    catch (ArgumentException)
                    {
                        return InvalidQuery();
                    }
                })
            .WithName("v08-notification-delivery-detail")
            .RequireKafdeckAuthorization(
                AuthorizationAction.NotificationRead,
                resourceRouteKey: "destinationId");

        // Query-addressed aliases preserve exact dot-only destination IDs
        // that URL path normalization would otherwise collapse. Never let
        // untrusted query text choose a transport or skip per-resource RBAC.
        app.MapGet(
                "/api/v1/notifications/deliveries/history",
                async (
                    string? destinationId,
                    HttpContext context,
                    int? maxResults,
                    DateTimeOffset? afterCreatedAtUtc,
                    Guid? afterNotificationId,
                    [FromServices] KafdeckAuthorizationService authorization,
                    [FromServices] INotificationDeliveryHistoryReader store,
                    CancellationToken cancellationToken) =>
                {
                    var admission = await RequireQueryDestinationReadAsync(
                        context, authorization, destinationId).ConfigureAwait(false);
                    if (admission is not null) return admission;
                    if ((afterCreatedAtUtc is null) !=
                        (afterNotificationId is null)) return InvalidQuery();

                    try
                    {
                        var after = afterCreatedAtUtc is not null
                            ? new NotificationDestinationDeliveryCursor(
                                afterCreatedAtUtc.Value, afterNotificationId!.Value)
                            : null;
                        var page = await store.ListByDestinationAsync(
                            new NotificationDestinationDeliveryQuery(
                                destinationId!, maxResults ?? 50, after),
                            cancellationToken).ConfigureAwait(false);
                        return Results.Ok(new NotificationDeliveryHistoryListData(
                            page.Items.Select(NotificationDeliveryEvidenceData.From).ToArray(),
                            page.Truncated, page.Next?.CreatedAtUtc,
                            page.Next?.NotificationId));
                    }
                    catch (ArgumentException)
                    {
                        return InvalidQuery();
                    }
                })
            .WithName("v08-notification-destination-deliveries-query-read");

        app.MapGet(
                "/api/v1/notifications/deliveries/evidence/{notificationId:guid}",
                async (
                    Guid notificationId,
                    string? destinationId,
                    HttpContext context,
                    [FromServices] KafdeckAuthorizationService authorization,
                    [FromServices] INotificationDeliveryStore store,
                    CancellationToken cancellationToken) =>
                {
                    var admission = await RequireQueryDestinationReadAsync(
                        context, authorization, destinationId).ConfigureAwait(false);
                    if (admission is not null) return admission;

                    try
                    {
                        var record = await store.GetAsync(
                            notificationId, destinationId!, cancellationToken)
                            .ConfigureAwait(false);
                        return record is null ? NotFound() :
                            Results.Ok(NotificationDeliveryEvidenceData.From(record));
                    }
                    catch (ArgumentException)
                    {
                        return InvalidQuery();
                    }
                })
            .WithName("v08-notification-delivery-evidence-query-read");

        return app;
    }

    private static bool IsAllowed(
        KafdeckAuthorizationService authorization,
        ClaimsPrincipal principal,
        string resource) =>
        authorization.Authorize(
            principal,
            new AuthorizationRequest(
                AuthorizationAction.NotificationRead,
                ResourceName: resource)) ==
        KafdeckAuthorizationOutcome.Allowed;

    // This query-specific resource guard runs before any persistence access.
    // It cannot reuse route-value authorization because destinationId is
    // deliberately NOT a path segment ('.' and '..' must remain literal).
    private static async Task<IResult?> RequireQueryDestinationReadAsync(
        HttpContext context,
        KafdeckAuthorizationService authorization,
        string? destinationId)
    {
        if (!OperatorSessionContextFactory.TryCreate(
                context.User, out var operatorSession) || operatorSession is null)
        {
            return Results.Problem(
                statusCode: StatusCodes.Status401Unauthorized,
                title: "Authentication required");
        }

        // Fail closed on absent/ambiguous repeated query keys as well as
        // invalid identity shape. No unknown destination is inferred as absent.
        if (!context.Request.Query.TryGetValue("destinationId", out var values) ||
            values.Count != 1 ||
            !string.Equals(values[0], destinationId, StringComparison.Ordinal))
            return InvalidQuery();

        try
        {
            NotificationDeliveryIdentity.NormalizeDestinationId(destinationId!);
        }
        catch (ArgumentException)
        {
            return InvalidQuery();
        }

        var outcome = authorization.Authorize(
            context.User,
            new AuthorizationRequest(
                AuthorizationAction.NotificationRead,
                ResourceName: destinationId));
        if (outcome == KafdeckAuthorizationOutcome.Allowed)
            return null;
        if (outcome == KafdeckAuthorizationOutcome.Unauthenticated)
            return Results.Problem(
                statusCode: StatusCodes.Status401Unauthorized,
                title: "Authentication required");

        var audit = context.RequestServices.GetRequiredService<ISecurityAuditSink>();
        await audit.WriteAsync(
            new SecurityAuditEvent(
                DateTimeOffset.UtcNow,
                SecurityAuditEventType.AuthorizationDenied,
                SecurityAuditPrincipal.FromOperator(operatorSession.Identity),
                operatorSession.SessionId.Value.ToString("N"),
                null, destinationId, SecurityAuditOutcome.Denied,
                "rbac_denied_notification_destination_query_read"),
            context.RequestAborted).ConfigureAwait(false);
        return Results.Problem(
            statusCode: StatusCodes.Status403Forbidden,
            type: "urn:kafdeck:problem:operator-authorization-denied",
            title: "Forbidden");
    }

    private static async Task<IResult?> RequireCollectionReadAsync(
        HttpContext context,
        KafdeckAuthorizationService authorization)
    {
        var result = authorization.AuthorizeCollection(
            context.User,
            AuthorizationAction.NotificationRead,
            clusterId: null);

        if (result == KafdeckAuthorizationOutcome.Allowed)
        {
            return null;
        }

        if (result == KafdeckAuthorizationOutcome.Unauthenticated)
        {
            return Results.Problem(
                statusCode: StatusCodes.Status401Unauthorized,
                title: "Authentication required");
        }

        var audit = context.RequestServices
            .GetRequiredService<ISecurityAuditSink>();
        var hasOperator = OperatorSessionContextFactory.TryCreate(
            context.User,
            out var operatorSession);
        await audit.WriteAsync(
                new SecurityAuditEvent(
                    DateTimeOffset.UtcNow,
                    SecurityAuditEventType.AuthorizationDenied,
                    hasOperator && operatorSession is not null
                        ? SecurityAuditPrincipal.FromOperator(
                            operatorSession.Identity)
                        : SecurityAuditPrincipal.Anonymous,
                    operatorSession?.SessionId.Value.ToString("N"),
                    null,
                    null,
                    SecurityAuditOutcome.Denied,
                    "rbac_denied_notification_collection"),
                context.RequestAborted)
            .ConfigureAwait(false);

        return Results.Problem(
            statusCode: StatusCodes.Status403Forbidden,
            type: "urn:kafdeck:problem:operator-authorization-denied",
            title: "Forbidden");
    }

    private static IResult InvalidQuery() =>
        Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            type: "urn:kafdeck:problem:invalid-notification-observation-query",
            title: "Invalid notification observation query");

    private static IResult NotFound() =>
        Results.Problem(
            statusCode: StatusCodes.Status404NotFound,
            type: "urn:kafdeck:problem:notification-observation-not-found",
            title: "Notification observation not found");
}

public static class NotificationSubscriptionReadProjection
{
    public static NotificationSubscriptionListData Project(
        NotificationSubscriptionPage page,
        Func<NotificationSubscriptionSnapshot, bool> isAuthorized)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(isAuthorized);

        var visible = page.Items.Where(isAuthorized).ToArray();
        var authorizationFiltered = visible.Length != page.Items.Count;

        // The raw cursor may be the identity of an unauthorized row.
        // Do not expose or manufacture cursors when filtering occurred;
        // declare the continuation restricted rather than claiming the
        // underlying complete page was returned to the operator.
        var next = authorizationFiltered
            ? null
            : page.NextSubscriptionId;

        return new NotificationSubscriptionListData(
            visible.Select(NotificationSubscriptionData.From).ToArray(),
            page.Truncated,
            next,
            authorizationFiltered,
            page.Truncated && authorizationFiltered);
    }
}

public sealed record NotificationSubscriptionListData(
    IReadOnlyList<NotificationSubscriptionData> Items,
    bool Truncated,
    string? NextSubscriptionId,
    bool AuthorizationFiltered,
    bool ContinuationRestricted);

public sealed record NotificationSubscriptionData(
    string SubscriptionId,
    string DestinationId,
    NotificationSubscriptionState State,
    long Revision,
    DateTimeOffset UpdatedAtUtc,
    IReadOnlyList<NotificationEventClass> EventClasses,
    IReadOnlyList<string> EventTypes)
{
    public static NotificationSubscriptionData From(
        NotificationSubscriptionSnapshot value) =>
        new(
            value.Definition.SubscriptionId,
            value.Definition.DestinationId,
            value.State,
            value.Revision,
            value.UpdatedAtUtc,
            value.Definition.EventClasses,
            value.Definition.EventTypes);
}

public sealed record NotificationDeliveryEvidenceData(
    Guid NotificationId,
    string DestinationId,
    NotificationDeliveryState State,
    int AttemptCount,
    long Revision,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc,
    DateTimeOffset? NextAttemptAtUtc,
    string? OutcomeCode,
    bool ProfileRevisionBound)
{
    public static NotificationDeliveryEvidenceData From(
        NotificationDeliveryRecord record) =>
        new(
            record.Snapshot.NotificationId,
            record.Snapshot.DestinationId,
            record.Snapshot.State,
            record.Snapshot.AttemptCount,
            record.Revision,
            record.Snapshot.CreatedAtUtc,
            record.UpdatedAtUtc,
            record.Snapshot.NextAttemptAtUtc,
            record.Snapshot.OutcomeCode,
            record.Snapshot.RoutedProfileRevisionFingerprint is not null);
}


public sealed record NotificationDeliveryHistoryListData(
    IReadOnlyList<NotificationDeliveryEvidenceData> Items,
    bool Truncated,
    DateTimeOffset? NextCreatedAtUtc,
    Guid? NextNotificationId);
