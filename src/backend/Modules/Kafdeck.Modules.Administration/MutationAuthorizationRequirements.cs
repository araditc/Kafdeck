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
