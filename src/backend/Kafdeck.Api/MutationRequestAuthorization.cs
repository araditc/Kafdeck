using System.Security.Claims;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

/// <summary>
/// Re-evaluates the live operator identity against every explicit mutation
/// authorization target. No identity, claim, or group data is reconstructed
/// from durable mutation state.
/// </summary>
public sealed class MutationRequestAuthorizationService
{
    private readonly KafdeckAuthorizationService _authorization;

    public MutationRequestAuthorizationService(
        KafdeckAuthorizationService authorization)
    {
        _authorization = authorization ??
            throw new ArgumentNullException(nameof(authorization));
    }

    public KafdeckAuthorizationOutcome AuthorizeTargets(
        ClaimsPrincipal? principal,
        IReadOnlyList<MutationAuthorizationTarget> targets)
    {
        ArgumentNullException.ThrowIfNull(targets);

        if (targets.Count == 0)
        {
            return KafdeckAuthorizationOutcome.Forbidden;
        }

        foreach (var target in targets)
        {
            var outcome = _authorization.Authorize(
                principal,
                new AuthorizationRequest(
                    target.Action,
                    target.ClusterId,
                    target.ResourceName));

            if (outcome != KafdeckAuthorizationOutcome.Allowed)
            {
                return outcome;
            }
        }

        return KafdeckAuthorizationOutcome.Allowed;
    }

    public KafdeckAuthorizationOutcome AuthorizeForDispatch(
        ClaimsPrincipal? principal,
        MutationOperationSnapshot operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        var lifecycle = AuthorizeForLifecycle(principal, operation);
        if (lifecycle != KafdeckAuthorizationOutcome.Allowed) return lifecycle;
        if (principal?.Identity is NonBrowserReadIdentity)
        {
            if (operation.Risk.RequiresIndependentApproval && operation.State is MutationOperationState.Ready or MutationOperationState.Executing &&
                (string.IsNullOrWhiteSpace(operation.ApprovedByPrincipalId) ||
                 operation.AuthorizationTargets.Any(target => !_authorization.IsCurrentApprovalAllowed(operation.ApprovedByPrincipalId,
                     new AuthorizationRequest(target.Action, target.ClusterId, target.ResourceName)))))
                return KafdeckAuthorizationOutcome.Forbidden;
        }
        return KafdeckAuthorizationOutcome.Allowed;
    }

    /// <summary>Current kind, policy and target authority; callers separately require exact plan ownership.</summary>
    public KafdeckAuthorizationOutcome AuthorizeForLifecycle(ClaimsPrincipal? principal, MutationOperationSnapshot operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (principal?.Identity is NonBrowserReadIdentity &&
            (!BearerTopicMutationScope.IsKind(operation.OperationKind) || operation.PolicyVersion != PolicyVersionFor(principal)))
            return KafdeckAuthorizationOutcome.Forbidden;
        return AuthorizeTargets(principal, operation.AuthorizationTargets);
    }

    public bool IsApprovalAllowed(ClaimsPrincipal? principal, MutationOperationSnapshot operation)
    {
        return operation.OperationKind == MutationOperationKind.TopicDelete && operation.Risk.RequiresIndependentApproval &&
            operation.PolicyVersion == "v0.8-bearer:" + _authorization.PolicyFingerprint &&
            MutationActor.TryCreate(principal, out var actor) && actor?.HumanKey is not null &&
            !MutationActor.IsSameHuman(operation.RequesterPrincipalId, actor.HumanKey) &&
            operation.AuthorizationTargets.Count > 0 && operation.AuthorizationTargets.All(target =>
                _authorization.IsLiveApprovalAllowed(principal, new(target.Action, target.ClusterId, target.ResourceName)));
    }

    public string PolicyVersionFor(ClaimsPrincipal? principal) => principal?.Identity is NonBrowserReadIdentity
        ? "v0.8-bearer:" + _authorization.PolicyFingerprint : MutationAdmissionService.PolicyVersion;
}
