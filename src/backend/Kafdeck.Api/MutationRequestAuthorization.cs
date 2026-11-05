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
                DeclarativeMutationAuthorization.Request(target));

            if (outcome != KafdeckAuthorizationOutcome.Allowed)
            {
                return outcome;
            }
        }

        return KafdeckAuthorizationOutcome.Allowed;
    }

    public KafdeckAuthorizationOutcome AuthorizeProofTargets(ClaimsPrincipal? principal, IReadOnlyList<MutationAuthorizationTarget> targets)
    {
        if (!MutationActor.TryCreate(principal, out var actor) || actor is null) return KafdeckAuthorizationOutcome.Unauthenticated;
        if (principal?.Identity is NonBrowserReadIdentity) return AuthorizeTargets(principal, targets);
        return targets.Count > 0 && targets.All(target => _authorization.IsDirectSubjectAllowed(actor.PrincipalId,
            DeclarativeMutationAuthorization.Request(target))) ? KafdeckAuthorizationOutcome.Allowed : KafdeckAuthorizationOutcome.Forbidden;
    }

    public KafdeckAuthorizationOutcome AuthorizeForDispatch(
        ClaimsPrincipal? principal,
        MutationOperationSnapshot operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        var lifecycle = AuthorizeForLifecycle(principal, operation);
        if (lifecycle != KafdeckAuthorizationOutcome.Allowed) return lifecycle;
        if (principal?.Identity is NonBrowserReadIdentity || IsProofKind(operation.OperationKind))
        {
            if (operation.Risk.RequiresIndependentApproval && operation.State is MutationOperationState.Ready or MutationOperationState.Executing &&
                (string.IsNullOrWhiteSpace(operation.ApprovedByPrincipalId) ||
                 operation.AuthorizationTargets.Any(target => !(operation.OperationKind==MutationOperationKind.GovernancePolicyActivation
                     ? _authorization.IsCurrentGovernanceApprovalAllowed(operation.ApprovedByPrincipalId,DeclarativeMutationAuthorization.Request(target))
                     : _authorization.IsCurrentApprovalAllowed(operation.ApprovedByPrincipalId,DeclarativeMutationAuthorization.Request(target))))))
                return KafdeckAuthorizationOutcome.Forbidden;
        }
        return KafdeckAuthorizationOutcome.Allowed;
    }

    /// <summary>Current kind, policy and target authority; callers separately require exact plan ownership.</summary>
    public KafdeckAuthorizationOutcome AuthorizeForLifecycle(ClaimsPrincipal? principal, MutationOperationSnapshot operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        try
        {
            // A kind admitted elsewhere cannot reinterpret another family's
            // authorization targets. Validate the exact closed conjunction.
            var normalized = MutationAuthorizationRequirements.Normalize(operation.OperationKind, operation.ClusterId,
                operation.AuthorizationTargets, operation.ResourceKeys);
            if (!operation.AuthorizationTargets.SequenceEqual(normalized)) return KafdeckAuthorizationOutcome.Forbidden;
        }
        catch (Exception exception) when (exception is ArgumentException or MutationStateException)
        { return KafdeckAuthorizationOutcome.Forbidden; }
        if ((principal?.Identity is NonBrowserReadIdentity || IsProofKind(operation.OperationKind)) &&
            (!BearerFamilyRoutes.IsKind(operation.OperationKind) || operation.PolicyVersion != PolicyVersionFor(principal, operation.OperationKind)))
            return KafdeckAuthorizationOutcome.Forbidden;
        return IsProofKind(operation.OperationKind)
            ? AuthorizeProofTargets(principal, operation.AuthorizationTargets) : AuthorizeTargets(principal, operation.AuthorizationTargets);
    }

    public bool IsApprovalAllowed(ClaimsPrincipal? principal, MutationOperationSnapshot operation, bool requireCurrentPolicy = true)
    {
        return BearerFamilyRoutes.IsKind(operation.OperationKind) && operation.Risk.RequiresIndependentApproval &&
            (!requireCurrentPolicy || operation.PolicyVersion == "v0.8-bearer:" + _authorization.PolicyFingerprint) &&
            MutationActor.TryCreate(principal, out var actor) && actor?.HumanKey is not null &&
            !MutationActor.IsSameHuman(operation.RequesterPrincipalId, actor.HumanKey) &&
            operation.AuthorizationTargets.Count > 0 && operation.AuthorizationTargets.All(target =>
                operation.OperationKind==MutationOperationKind.GovernancePolicyActivation
                    ? _authorization.IsLiveGovernanceApprovalAllowed(principal,DeclarativeMutationAuthorization.Request(target))
                    : _authorization.IsLiveApprovalAllowed(principal, DeclarativeMutationAuthorization.Request(target)));
    }

    private static bool IsProofKind(MutationOperationKind? kind) => kind is MutationOperationKind.FleetUncertaintyDisposition or MutationOperationKind.DeclarativeNotificationSubscription or
        MutationOperationKind.GovernancePolicyVersion or MutationOperationKind.GovernancePolicyActivation;

    public string PolicyVersionFor(ClaimsPrincipal? principal, MutationOperationKind? kind = null) => principal?.Identity is NonBrowserReadIdentity || IsProofKind(kind)
        ? "v0.8-bearer:" + _authorization.PolicyFingerprint : MutationAdmissionService.PolicyVersion;
}
