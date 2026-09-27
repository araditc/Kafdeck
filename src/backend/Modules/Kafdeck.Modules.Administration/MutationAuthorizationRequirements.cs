namespace Kafdeck.Modules.Administration;

/// <summary>
/// Selects the closed authorization normalizer for an admitted mutation kind
/// without widening the legacy v0.5 single-action normalizer.
///
/// v0.5 kinds continue through <see cref="MutationAuthorization"/> unchanged.
/// v0.6 fleet kinds require explicit server-derived requirement lists and are
/// validated by <see cref="FleetMutationAuthorization"/>.
/// </summary>
public static class MutationAuthorizationRequirements
{
    public static IReadOnlyList<MutationAuthorizationTarget> Normalize(
        MutationOperationKind kind,
        string operationClusterId,
        IReadOnlyList<MutationAuthorizationTarget>? requirements,
        IReadOnlyList<string>? fallbackResourceKeys = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationClusterId);
        var normalizedOperationCluster = RequireBounded(
            operationClusterId,
            "Operation physical cluster",
            256);

        if (kind == MutationOperationKind.ConnectAutoRestartPolicy)
        {
            return NormalizeConnectAutoRestartRequirements(
                normalizedOperationCluster,
                requirements);
        }

        if (!FleetMutationAuthorization.IsFleetKind(kind))
        {
            return MutationAuthorization.NormalizeTargets(
                kind,
                normalizedOperationCluster,
                requirements,
                fallbackResourceKeys);
        }

        if (requirements is not { Count: > 0 })
        {
            throw new ArgumentException(
                "Fleet mutation kinds require an explicit closed authorization conjunction.",
                nameof(requirements));
        }

        var normalized = FleetMutationAuthorization.NormalizeRequirements(
            kind,
            requirements);

        if (RequiresSingleOperationClusterAnchor(kind) &&
            normalized.Any(requirement =>
                !string.Equals(
                    requirement.ClusterId,
                    normalizedOperationCluster,
                    StringComparison.Ordinal)))
        {
            throw new ArgumentException(
                $"Mutation kind '{kind}' requires every authorization target to bind the operation's exact physical cluster.",
                nameof(requirements));
        }

        return normalized;
    }

    private static IReadOnlyList<MutationAuthorizationTarget>
        NormalizeConnectAutoRestartRequirements(
            string operationClusterId,
            IReadOnlyList<MutationAuthorizationTarget>? requirements)
    {
        if (requirements is not { Count: >= 1 and <= 2 })
        {
            throw new ArgumentException(
                "Connect auto-restart policy mutations require one or two explicit authorization requirements.",
                nameof(requirements));
        }

        var normalized = requirements
            .Select(requirement =>
            {
                ArgumentNullException.ThrowIfNull(requirement);
                if (requirement.Action is not (
                        Kafdeck.Core.Security.AuthorizationAction.ConnectAutoRestartManage or
                        Kafdeck.Core.Security.AuthorizationAction.ConnectRead))
                {
                    throw new ArgumentException(
                        "Connect auto-restart policy authorization permits only manage and read actions.",
                        nameof(requirements));
                }

                var clusterId = RequireBounded(
                    requirement.ClusterId,
                    "Connect auto-restart authorization cluster",
                    256);
                if (!string.Equals(
                        clusterId,
                        operationClusterId,
                        StringComparison.Ordinal))
                {
                    throw new ArgumentException(
                        "Connect auto-restart authorization must bind the operation cluster.",
                        nameof(requirements));
                }

                return new MutationAuthorizationTarget(
                    requirement.Action,
                    clusterId,
                    RequireBounded(
                        requirement.ResourceName,
                        "Connect auto-restart authorization resource",
                        512));
            })
            .Distinct()
            .OrderBy(item => item.Action)
            .ToArray();

        if (!normalized.Any(item =>
                item.Action ==
                Kafdeck.Core.Security.AuthorizationAction.ConnectAutoRestartManage))
        {
            throw new ArgumentException(
                "Connect auto-restart policy mutation requires manage authorization.",
                nameof(requirements));
        }

        if (normalized
            .Select(item => item.ResourceName)
            .Distinct(StringComparer.Ordinal)
            .Count() != 1)
        {
            throw new ArgumentException(
                "Connect auto-restart authorization requirements must bind one exact resource.",
                nameof(requirements));
        }

        return Array.AsReadOnly(normalized);
    }

    private static bool RequiresSingleOperationClusterAnchor(
        MutationOperationKind kind) =>
        kind is not (
            MutationOperationKind.ClusterTransfer or
            MutationOperationKind.ReplicationIntegration or
            MutationOperationKind.FleetUncertaintyDisposition);

    private static string RequireBounded(
        string value,
        string fieldName,
        int maxLength)
    {
        var normalized = value.Trim();
        if (normalized.Length == 0 ||
            normalized.Length > maxLength ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                fieldName,
                $"{fieldName} must be non-empty, at most {maxLength} characters, and contain no control characters.");
        }

        return normalized;
    }
}
