using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

/// <summary>Closed deployment-scoped declarative conjunction; notification actions retain their existing global scope.</summary>
public static class DeclarativeMutationAuthorization
{
    public static AuthorizationRequest Request(MutationAuthorizationTarget target) =>
        new(target.Action, target.Action is AuthorizationAction.NotificationRead or AuthorizationAction.NotificationManage
            ? null : target.ClusterId, target.ResourceName);

    public static IReadOnlyList<MutationAuthorizationTarget> Normalize(string deploymentId,
        IReadOnlyList<MutationAuthorizationTarget>? requirements)
    {
        if (requirements is not { Count: >= 4 and <= 10 }) throw new ArgumentException("Declarative authorization requires an exact bounded conjunction.");
        foreach (var target in requirements)
        {
            if (target is null || target.ClusterId != deploymentId || string.IsNullOrWhiteSpace(target.ResourceName) ||
                target.ResourceName.Length > 128 || target.ResourceName != target.ResourceName.Trim() ||
                target.ResourceName.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-')) ||
                target.Action is not (AuthorizationAction.DeclarativeRead or AuthorizationAction.DeclarativePlan or
                    AuthorizationAction.DeclarativeApply or AuthorizationAction.DeclarativeAdopt or
                    AuthorizationAction.NotificationRead or AuthorizationAction.NotificationManage))
                throw new ArgumentException("Declarative authorization contains an unsupported target.");
        }
        var normalized = requirements.Distinct().OrderBy(t => t.Action).ThenBy(t => t.ClusterId, StringComparer.Ordinal)
            .ThenBy(t => t.ResourceName, StringComparer.Ordinal).ToArray();
        var declarative = normalized.Where(t => t.Action is not (AuthorizationAction.NotificationRead or AuthorizationAction.NotificationManage)).ToArray();
        if (declarative.Select(t => t.ResourceName).Distinct(StringComparer.Ordinal).Count() != 1 ||
            !new[] { AuthorizationAction.DeclarativeRead, AuthorizationAction.DeclarativePlan }
                .All(action => declarative.Any(t => t.Action == action)) ||
            !declarative.Any(t => t.Action is AuthorizationAction.DeclarativeApply or AuthorizationAction.DeclarativeAdopt))
            throw new ArgumentException("Declarative read, plan and command authority must bind one exact subscription.");
        var resource = declarative[0].ResourceName;
        var reads = normalized.Where(t => t.Action == AuthorizationAction.NotificationRead).Select(t => t.ResourceName).ToArray();
        var writes = normalized.Where(t => t.Action == AuthorizationAction.NotificationManage).Select(t => t.ResourceName).ToArray();
        if (reads.Length is < 1 or > 3 || !reads.Contains(resource, StringComparer.Ordinal) ||
            (writes.Length > 0 ? !reads.SequenceEqual(writes) || !declarative.Any(t => t.Action == AuthorizationAction.DeclarativeApply)
                : !declarative.Any(t => t.Action == AuthorizationAction.DeclarativeAdopt)))
            throw new ArgumentException("Notification read and manage must bind subscription and exact old/new destinations.");
        return Array.AsReadOnly(normalized);
    }
}
