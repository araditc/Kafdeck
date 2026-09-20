using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Http;

namespace Kafdeck.Api;

public sealed record KafdeckAuthorizationRequirement(
    AuthorizationAction Action,
    string? ClusterRouteKey = null,
    string? ResourceRouteKey = null);

public static class KafdeckAuthorizationEndpointExtensions
{
    public static RouteHandlerBuilder RequireKafdeckAuthorization(
        this RouteHandlerBuilder builder,
        AuthorizationAction action,
        string? clusterRouteKey = null,
        string? resourceRouteKey = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var requirement = new KafdeckAuthorizationRequirement(action, clusterRouteKey, resourceRouteKey);
        return builder.AddEndpointFilter(async (invocation, next) =>
        {
            var http = invocation.HttpContext;
            var options = http.RequestServices.GetRequiredService<KafdeckOptions>();

            // Local and Token modes retain their accepted deployment-boundary semantics.
            // Deployment tokens are never projected into operator identity or RBAC.
            if (options.Deployment.Mode != AccessMode.Oidc)
            {
                return await next(invocation).ConfigureAwait(false);
            }

            if (!OperatorSessionContextFactory.TryCreate(http.User, out var session) || session is null)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status401Unauthorized,
                    title: "Authentication required",
                    detail: "An authenticated operator session is required.");
            }

            var clusterId = requirement.ClusterRouteKey is null
                ? null
                : http.Request.RouteValues[requirement.ClusterRouteKey]?.ToString();
            var resourceName = requirement.ResourceRouteKey is null
                ? null
                : http.Request.RouteValues[requirement.ResourceRouteKey]?.ToString();

            var evaluator = http.RequestServices.GetRequiredService<AuthorizationPolicyEvaluator>();
            var decision = evaluator.Evaluate(
                session.Identity,
                new AuthorizationRequest(requirement.Action, clusterId, resourceName));

            if (!decision.IsAllowed)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status403Forbidden,
                    title: "Forbidden",
                    detail: "The authenticated operator is not authorized for this operation.");
            }

            return await next(invocation).ConfigureAwait(false);
        });
    }
}
