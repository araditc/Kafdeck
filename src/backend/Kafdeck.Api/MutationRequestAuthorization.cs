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
        return AuthorizeTargets(principal, operation.AuthorizationTargets);
    }
}
