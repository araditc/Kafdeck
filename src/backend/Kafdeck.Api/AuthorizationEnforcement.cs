using System.Security.Claims;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Http;

namespace Kafdeck.Api;

public enum KafdeckAuthorizationOutcome
{
    Allowed = 1,
    Unauthenticated = 2,
    Forbidden = 3,
}

public sealed class KafdeckAuthorizationService
{
    private readonly KafdeckOptions _options;
    private readonly AuthorizationPolicyEvaluator _evaluator;

    public KafdeckAuthorizationService(KafdeckOptions options, AuthorizationPolicyEvaluator evaluator)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _evaluator = evaluator ?? throw new ArgumentNullException(nameof(evaluator));
    }

    public KafdeckAuthorizationOutcome Authorize(ClaimsPrincipal? principal, AuthorizationRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (_options.Deployment.Mode != AccessMode.Oidc)
        {
            return KafdeckAuthorizationOutcome.Allowed;
        }

        if (!OperatorSessionContextFactory.TryCreate(principal, out var session) || session is null)
        {
            return KafdeckAuthorizationOutcome.Unauthenticated;
        }

        return _evaluator.Evaluate(session.Identity, request).IsAllowed
            ? KafdeckAuthorizationOutcome.Allowed
            : KafdeckAuthorizationOutcome.Forbidden;
    }
}

public static class KafdeckAuthorizationEndpointExtensions
{
    public static RouteHandlerBuilder RequireKafdeckAuthorization(
        this RouteHandlerBuilder builder,
        AuthorizationAction action,
        string? clusterRouteKey = null,
        string? resourceRouteKey = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddEndpointFilter(async (invocation, next) =>
        {
            var http = invocation.HttpContext;
            var clusterId = clusterRouteKey is null
                ? null
                : http.Request.RouteValues[clusterRouteKey]?.ToString();
            var resourceName = resourceRouteKey is null
                ? null
                : http.Request.RouteValues[resourceRouteKey]?.ToString();

            var authorization = http.RequestServices.GetRequiredService<KafdeckAuthorizationService>();
            var request = new AuthorizationRequest(action, clusterId, resourceName);
            var outcome = authorization.Authorize(http.User, request);

            var shouldAudit = outcome == KafdeckAuthorizationOutcome.Forbidden ||
                (outcome == KafdeckAuthorizationOutcome.Allowed &&
                 action is AuthorizationAction.BrokerConfigRead or AuthorizationAction.TopicConfigRead);
            var audit = shouldAudit
                ? http.RequestServices.GetRequiredService<ISecurityAuditSink>()
                : null;
            if (outcome == KafdeckAuthorizationOutcome.Forbidden)
            {
                var principal = OperatorSessionContextFactory.TryCreate(http.User, out var session) && session is not null
                    ? SecurityAuditPrincipal.FromOperator(session.Identity)
                    : SecurityAuditPrincipal.Anonymous;
                await audit!.WriteAsync(
                    new SecurityAuditEvent(
                        DateTimeOffset.UtcNow,
                        SecurityAuditEventType.AuthorizationDenied,
                        principal,
                        session?.SessionId.Value.ToString("N"),
                        clusterId,
                        resourceName,
                        SecurityAuditOutcome.Denied,
                        "rbac_denied"),
                    http.RequestAborted).ConfigureAwait(false);
            }

            if (outcome == KafdeckAuthorizationOutcome.Allowed &&
                action is AuthorizationAction.BrokerConfigRead or AuthorizationAction.TopicConfigRead)
            {
                var principal = OperatorSessionContextFactory.TryCreate(http.User, out var sensitiveSession) && sensitiveSession is not null
                    ? SecurityAuditPrincipal.FromOperator(sensitiveSession.Identity)
                    : SecurityAuditPrincipal.LegacyDeployment;
                await audit!.WriteAsync(
                    new SecurityAuditEvent(
                        DateTimeOffset.UtcNow,
                        SecurityAuditEventType.SensitiveRead,
                        principal,
                        sensitiveSession?.SessionId.Value.ToString("N"),
                        clusterId,
                        resourceName,
                        SecurityAuditOutcome.Succeeded,
                        action == AuthorizationAction.BrokerConfigRead ? "broker_config_read" : "topic_config_read"),
                    http.RequestAborted).ConfigureAwait(false);
            }

            return outcome switch
            {
                KafdeckAuthorizationOutcome.Allowed => await next(invocation).ConfigureAwait(false),
                KafdeckAuthorizationOutcome.Unauthenticated => Results.Problem(
                    statusCode: StatusCodes.Status401Unauthorized,
                    title: "Authentication required",
                    detail: "An authenticated operator session is required."),
                _ => Results.Problem(
                    statusCode: StatusCodes.Status403Forbidden,
                    type: "urn:kafdeck:problem:operator-authorization-denied",
                    title: "Forbidden",
                    detail: "The authenticated operator is not authorized for this operation."),
            };
        });
    }
}
