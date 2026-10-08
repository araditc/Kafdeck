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
                    KafdeckAuthorizationService authorization,
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

                        var visible = page.Items
                            .Where(item =>
                                IsAllowed(
                                    authorization,
                                    context.User,
                                    item.Definition.SubscriptionId) &&
                                IsAllowed(
                                    authorization,
                                    context.User,
                                    item.Definition.DestinationId))
                            .ToArray();

                        var authorizationFiltered =
                            visible.Length != page.Items.Count;

                        // A hidden last item may be the cursor. Never emit
                        // its raw subscription ID through pagination.
                        // Return a truthful incomplete result instead.
                        var next = !authorizationFiltered
                            ? page.NextSubscriptionId
                            : null;

                        return Results.Ok(
                            new NotificationSubscriptionListData(
                                visible.Select(
                                    NotificationSubscriptionData.From)
                                    .ToArray(),
                                page.Truncated,
                                next,
                                authorizationFiltered,
                                page.Truncated && authorizationFiltered));
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
                    KafdeckAuthorizationService authorization,
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
