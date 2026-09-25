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
    private readonly MutationApprovalAuthorizer _approvalAuthorizer;

    public W42AclEffectAuthorizationGuard(
        MutationExecutionRequestContextAccessor requestContext,
        MutationRequestAuthorizationService authorization,
        MutationApprovalAuthorizer approvalAuthorizer)
    {
        _requestContext = requestContext ??
            throw new ArgumentNullException(nameof(requestContext));
        _authorization = authorization ??
            throw new ArgumentNullException(nameof(authorization));
        _approvalAuthorizer = approvalAuthorizer ??
            throw new ArgumentNullException(nameof(approvalAuthorizer));
    }

    public Task<MutationPreDispatchGuardResult> ValidateCurrentRequesterAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var requester = MutationCurrentRequesterAuthorization.Evaluate(
            _requestContext,
            _authorization,
            operation);
        if (requester.Outcome != MutationPreDispatchGuardOutcome.Allowed ||
            !operation.Risk.RequiresIndependentApproval)
        {
            return Task.FromResult(requester);
        }

        if (string.IsNullOrWhiteSpace(operation.ApprovedByPrincipalId) ||
            string.IsNullOrWhiteSpace(operation.ApprovalAuthorizationEvidenceHash) ||
            operation.ApprovedAtUtc is null)
        {
            return Task.FromResult(Denied(
                "current_required_approver_missing"));
        }

        if (string.Equals(
                operation.RequesterPrincipalId,
                operation.ApprovedByPrincipalId,
                StringComparison.Ordinal))
        {
            return Task.FromResult(Denied(
                "current_required_approver_not_distinct"));
        }

        if (!_approvalAuthorizer.IsCurrentlyEligibleDirectSubject(
                operation.ApprovedByPrincipalId,
                operation))
        {
            return Task.FromResult(Denied(
                "current_required_approver_eligibility_unavailable_or_denied"));
        }

        return Task.FromResult(
            MutationPreDispatchGuardResult.Allowed);
    }

    private static MutationPreDispatchGuardResult Denied(string code) =>
        new(
            MutationPreDispatchGuardOutcome.AuthorizationDenied,
            code);
}
