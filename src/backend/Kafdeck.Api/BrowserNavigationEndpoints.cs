using Kafdeck.Core.Security;

namespace Kafdeck.Api;

// Experimental browser presentation hint. Every destination retains its own
// current resource authorization, approval and effect controls.
internal static class BrowserNavigationEndpoints
{
    internal static WebApplication MapBrowserNavigation(this WebApplication app)
    {
        app.MapGet("/api/v1/auth/navigation", (HttpContext context, string clusterId, [Microsoft.AspNetCore.Mvc.FromServices] KafdeckAuthorizationService auth) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            if (context.User.Identity is NonBrowserReadIdentity ||
                !OperatorSessionContextFactory.TryCreate(context.User, out var session) || session is null)
                return Results.Problem(statusCode: 401, type: "urn:kafdeck:problem:operator-authentication-required", title: "Operator authentication required");
            if (auth.ManagedIdentity is { } managed &&
                (managed.Find(session) is not { } user || !ManagedIdentityStore.SessionAllowed(user, session)))
                return Results.Problem(statusCode: 403, type: "urn:kafdeck:problem:operator-authorization-denied", title: "Operator authorization denied");
            if (string.IsNullOrWhiteSpace(clusterId) || clusterId.Length > 256 || clusterId.Any(char.IsControl))
                return Results.Problem(statusCode: 400, type: "urn:kafdeck:problem:invalid-cluster", title: "Invalid cluster scope");
            bool Collection(AuthorizationAction action) => auth.AuthorizeCollection(context.User, action, clusterId) == KafdeckAuthorizationOutcome.Allowed;
            bool Active(AuthorizationAction action) => auth.Authorize(context.User, new(action, clusterId, "active")) == KafdeckAuthorizationOutcome.Allowed;
            return Results.Ok(new
            {
                clusterId,
                ownershipCatalog = Collection(AuthorizationAction.CatalogRead),
                governancePolicy = Collection(AuthorizationAction.GovernancePolicyRead) && Active(AuthorizationAction.GovernancePolicyRead),
                serviceRequests = Collection(AuthorizationAction.RequestRead),
                governanceAttribution = Collection(AuthorizationAction.AttributionRead) && Active(AuthorizationAction.AttributionRead),
            });
        }).WithName("browser-navigation");
        return app;
    }
}
