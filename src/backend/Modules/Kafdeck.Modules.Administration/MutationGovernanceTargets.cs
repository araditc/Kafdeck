using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

/// <summary>Policy-only authority projection. Raw mutation targets remain unchanged for RBAC,
/// approval and material checks. Global notification controls also bind the reserved deployment policy.</summary>
public static class MutationGovernanceTargets
{
    public const string GlobalNotificationScope="$deployment";
    public static IReadOnlyList<MutationAuthorizationTarget> Project(IReadOnlyList<MutationAuthorizationTarget> targets)
    {
        ArgumentNullException.ThrowIfNull(targets);
        return targets.Concat(targets.Where(t=>t.Action==AuthorizationAction.NotificationManage&&t.ClusterId!=GlobalNotificationScope)
            .Select(t=>new MutationAuthorizationTarget(t.Action,GlobalNotificationScope,t.ResourceName))).Distinct().ToArray();
    }
}
