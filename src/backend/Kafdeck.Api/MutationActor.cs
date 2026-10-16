using System.Security.Claims;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

/// <summary>Live authenticated mutation attribution, without inventing a browser session for bearer clients.</summary>
public sealed record MutationActor(string PrincipalId, OperatorIdentityKey? HumanKey)
{
    public static bool TryCreate(ClaimsPrincipal? principal, out MutationActor? actor)
    {
        actor = null;
        if (principal?.Identity is NonBrowserReadIdentity bearer && bearer.Evidence.ExpiresAtUtc > DateTimeOffset.UtcNow)
        {
            actor = new(bearer.AuditId, bearer.Kind == NonBrowserPrincipalKind.Human
                ? new(bearer.Evidence.Issuer, bearer.Evidence.Subject) : null);
            return true;
        }
        if (!OperatorSessionContextFactory.TryCreate(principal, out var session) || session is null) return false;
        actor = new(SecurityAuditPrincipal.FromOperator(session.Identity), session.Identity.Key);
        return true;
    }

    public static bool IsSameHuman(string requester, OperatorIdentityKey currentHuman)
    {
        if (requester == SecurityAuditPrincipal.FromOperatorKey(currentHuman)) return true;
        return NonBrowserHumanPrincipalId.TryParse(requester, out var parsed) && parsed!.HumanKey == currentHuman;
    }
}
