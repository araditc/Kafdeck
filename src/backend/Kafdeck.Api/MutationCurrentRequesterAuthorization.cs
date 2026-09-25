using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

internal static class MutationCurrentRequesterAuthorization
{
    public static MutationPreDispatchGuardResult Evaluate(
        MutationExecutionRequestContextAccessor requestContext,
        MutationRequestAuthorizationService authorization,
        MutationOperationSnapshot operation)
    {
        ArgumentNullException.ThrowIfNull(requestContext);
        ArgumentNullException.ThrowIfNull(authorization);
        ArgumentNullException.ThrowIfNull(operation);

        var principal = requestContext.CurrentPrincipal;
        if (principal is null ||
            !OperatorSessionContextFactory.TryCreate(principal, out var session) ||
            session is null)
        {
            return Denied("current_requester_identity_missing");
        }

        var currentPrincipalId =
            SecurityAuditPrincipal.FromOperator(session.Identity);
        if (!string.Equals(
                currentPrincipalId,
                operation.RequesterPrincipalId,
                StringComparison.Ordinal))
        {
            return Denied("current_requester_identity_mismatch");
        }

        var decision = authorization.AuthorizeForDispatch(
            principal,
            operation);
        return decision == KafdeckAuthorizationOutcome.Allowed
            ? MutationPreDispatchGuardResult.Allowed
            : Denied(
                decision == KafdeckAuthorizationOutcome.Unauthenticated
                    ? "current_requester_identity_missing"
                    : "current_requester_authorization_denied");
    }

    private static MutationPreDispatchGuardResult Denied(string code) =>
        new(
            MutationPreDispatchGuardOutcome.AuthorizationDenied,
            code);
}

/// <summary>
/// Supplies the W42 execution coordinator with the actual live request
/// principal. It never rehydrates OIDC claims from durable mutation state and
/// never substitutes provider or worker credentials for requester authority.
/// </summary>
public sealed class W42AclEffectAuthorizationGuard :
    IAclEffectAuthorizationGuard
{
    private readonly MutationExecutionRequestContextAccessor _requestContext;
    private readonly MutationRequestAuthorizationService _authorization;

    public W42AclEffectAuthorizationGuard(
        MutationExecutionRequestContextAccessor requestContext,
        MutationRequestAuthorizationService authorization)
    {
        _requestContext = requestContext ??
            throw new ArgumentNullException(nameof(requestContext));
        _authorization = authorization ??
            throw new ArgumentNullException(nameof(authorization));
    }

    public Task<MutationPreDispatchGuardResult> ValidateCurrentRequesterAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(
            MutationCurrentRequesterAuthorization.Evaluate(
                _requestContext,
                _authorization,
                operation));
    }
}
