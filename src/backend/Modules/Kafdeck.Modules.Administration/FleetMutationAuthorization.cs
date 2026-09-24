using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Closed v0.6 authorization-requirement normalizer.
///
/// This is intentionally separate from <see cref="MutationAuthorization"/> so the
/// admitted v0.5 single-action/single-cluster contract for kinds 1-14 is not
/// widened into an arbitrary action-list API.
/// </summary>
public static class FleetMutationAuthorization
{
    public const int RequirementSchemaVersion = 1;

    public static bool IsFleetKind(MutationOperationKind kind) => kind is
        MutationOperationKind.AclAlter or
        MutationOperationKind.ScramAlter or
        MutationOperationKind.QuotaAlter or
        MutationOperationKind.ClusterConfigAlter or
        MutationOperationKind.PreferredLeaderElection or
        MutationOperationKind.PartitionReassign or
        MutationOperationKind.ReplicationFactorAlter or
        MutationOperationKind.ReassignmentThrottle or
        MutationOperationKind.BrokerMaintenance or
        MutationOperationKind.LogDirectoryMaintenance or
        MutationOperationKind.ClusterTransfer or
        MutationOperationKind.ReplicationIntegration or
        MutationOperationKind.FleetUncertaintyDisposition;

    public static IReadOnlyList<MutationAuthorizationTarget> NormalizeRequirements(
        MutationOperationKind kind,
        IReadOnlyList<MutationAuthorizationTarget> requirements)
    {
        if (!IsFleetKind(kind))
        {
            throw new ArgumentOutOfRangeException(
                nameof(kind),
                kind,
                "Only admitted v0.6 fleet mutation kinds use compound authorization requirements.");
        }

        ArgumentNullException.ThrowIfNull(requirements);
        if (requirements.Count == 0 || requirements.Count > MutationLimits.MaxResourceKeys)
        {
            throw new ArgumentOutOfRangeException(
                nameof(requirements),
                $"Fleet authorization requirements must contain between 1 and {MutationLimits.MaxResourceKeys} items.");
        }

        var rule = RuleFor(kind);
        var normalized = requirements
            .Select(requirement =>
            {
                ArgumentNullException.ThrowIfNull(requirement);
                if (!rule.AllowedActions.Contains(requirement.Action))
                {
                    throw new ArgumentException(
                        $"Mutation kind '{kind}' does not admit authorization action '{requirement.Action}'.",
                        nameof(requirements));
                }

                return new MutationAuthorizationTarget(
                    requirement.Action,
                    RequireBounded(requirement.ClusterId, "Authorization target physical cluster", 256),
                    RequireBounded(requirement.ResourceName, "Authorization target resource", 512));
            })
            .Distinct()
            .OrderBy(requirement => requirement.Action)
            .ThenBy(requirement => requirement.ClusterId, StringComparer.Ordinal)
            .ThenBy(requirement => requirement.ResourceName, StringComparer.Ordinal)
            .ToArray();

        var actions = normalized
            .Select(requirement => requirement.Action)
            .ToHashSet();

        foreach (var requiredAction in rule.RequiredActions)
        {
            if (!actions.Contains(requiredAction))
            {
                throw new ArgumentException(
                    $"Mutation kind '{kind}' requires authorization action '{requiredAction}'.",
                    nameof(requirements));
            }
        }

        var clusters = normalized
            .Select(requirement => requirement.ClusterId)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(cluster => cluster, StringComparer.Ordinal)
            .ToArray();

        switch (kind)
        {
            case MutationOperationKind.ClusterTransfer:
                ValidateTransferTopology(normalized, clusters);
                break;

            case MutationOperationKind.ReplicationIntegration:
                if (clusters.Length < 2)
                {
                    throw new ArgumentException(
                        "Replication integration must bind at least two physical clusters.",
                        nameof(requirements));
                }

                var lifecycleMutationCount =
                    (actions.Contains(AuthorizationAction.ConnectCreate) ? 1 : 0) +
                    (actions.Contains(AuthorizationAction.ConnectAlter) ? 1 : 0);
                if (lifecycleMutationCount != 1)
                {
                    throw new ArgumentException(
                        "Replication integration requires exactly one lifecycle-specific Connect mutation action.",
                        nameof(requirements));
                }
                break;

            case MutationOperationKind.FleetUncertaintyDisposition:
                if (!normalized.Any(requirement =>
                        requirement.Action != AuthorizationAction.MutationReconcile))
                {
                    throw new ArgumentException(
                        "Uncertainty disposition must bind the unresolved original effect's current authorization conjunction.",
                        nameof(requirements));
                }
                break;

            default:
                if (clusters.Length != 1)
                {
                    throw new ArgumentException(
                        $"Mutation kind '{kind}' is single-physical-cluster and cannot bind cross-cluster requirements.",
                        nameof(requirements));
                }
                break;
        }

        return Array.AsReadOnly(normalized);
    }

    private static void ValidateTransferTopology(
        IReadOnlyList<MutationAuthorizationTarget> requirements,
        IReadOnlyList<string> clusters)
    {
        if (clusters.Count != 2)
        {
            throw new ArgumentException(
                "Finite cluster transfer must bind exactly two physical clusters.",
                nameof(requirements));
        }

        var readClusters = ClustersFor(requirements, AuthorizationAction.RecordRead);
        var exportClusters = ClustersFor(requirements, AuthorizationAction.RecordExport);
        var produceClusters = ClustersFor(requirements, AuthorizationAction.RecordProduce);

        if (readClusters.Length != 1 ||
            exportClusters.Length != 1 ||
            !string.Equals(readClusters[0], exportClusters[0], StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Finite transfer requires record.read and record.export on the same exact source physical cluster.",
                nameof(requirements));
        }

        if (produceClusters.Length != 1 ||
            string.Equals(readClusters[0], produceClusters[0], StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Finite transfer requires record.produce on one distinct destination physical cluster.",
                nameof(requirements));
        }

        var source = readClusters[0];
        var destination = produceClusters[0];
        foreach (var cluster in new[] { source, destination })
        {
            if (!requirements.Any(requirement =>
                    requirement.Action == AuthorizationAction.ClusterRead &&
                    string.Equals(requirement.ClusterId, cluster, StringComparison.Ordinal)))
            {
                throw new ArgumentException(
                    $"Finite transfer requires cluster.read on physical cluster '{cluster}'.",
                    nameof(requirements));
            }

            if (!requirements.Any(requirement =>
                    requirement.Action == AuthorizationAction.TopicRead &&
                    string.Equals(requirement.ClusterId, cluster, StringComparison.Ordinal)))
            {
                throw new ArgumentException(
                    $"Finite transfer requires topic.read on physical cluster '{cluster}'.",
                    nameof(requirements));
            }
        }
    }

    private static string[] ClustersFor(
        IReadOnlyList<MutationAuthorizationTarget> requirements,
        AuthorizationAction action) =>
        requirements
            .Where(requirement => requirement.Action == action)
            .Select(requirement => requirement.ClusterId)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(cluster => cluster, StringComparer.Ordinal)
            .ToArray();

    private static AuthorizationRule RuleFor(MutationOperationKind kind) => kind switch
    {
        MutationOperationKind.AclAlter => Rule(
            new[] { AuthorizationAction.AclAlter, AuthorizationAction.AclRead }),

        MutationOperationKind.ScramAlter => Rule(
            new[] { AuthorizationAction.ScramAlter, AuthorizationAction.ScramRead }),

        MutationOperationKind.QuotaAlter => Rule(
            new[] { AuthorizationAction.QuotaAlter, AuthorizationAction.QuotaRead }),

        MutationOperationKind.ClusterConfigAlter => Rule(
            new[] { AuthorizationAction.ClusterConfigAlter, AuthorizationAction.ClusterConfigRead }),

        MutationOperationKind.PreferredLeaderElection => Rule(
            new[]
            {
                AuthorizationAction.LeaderElect,
                AuthorizationAction.ClusterRead,
                AuthorizationAction.TopicRead,
                AuthorizationAction.BrokerRead,
            },
            AuthorizationAction.TopicConfigRead,
            AuthorizationAction.BrokerConfigRead),

        MutationOperationKind.PartitionReassign => Rule(
            new[]
            {
                AuthorizationAction.PartitionReassign,
                AuthorizationAction.ClusterRead,
                AuthorizationAction.TopicRead,
                AuthorizationAction.BrokerRead,
                AuthorizationAction.TopicConfigRead,
                AuthorizationAction.BrokerConfigRead,
            }),

        MutationOperationKind.ReplicationFactorAlter => Rule(
            new[]
            {
                AuthorizationAction.PartitionReassign,
                AuthorizationAction.ReplicationFactorAlter,
                AuthorizationAction.ClusterRead,
                AuthorizationAction.TopicRead,
                AuthorizationAction.BrokerRead,
                AuthorizationAction.TopicConfigRead,
                AuthorizationAction.BrokerConfigRead,
            }),

        MutationOperationKind.ReassignmentThrottle => Rule(
            new[]
            {
                AuthorizationAction.ReassignmentThrottle,
                AuthorizationAction.TopicAlter,
                AuthorizationAction.ClusterConfigAlter,
                AuthorizationAction.TopicConfigRead,
                AuthorizationAction.BrokerConfigRead,
                AuthorizationAction.ClusterRead,
                AuthorizationAction.TopicRead,
                AuthorizationAction.BrokerRead,
            }),

        MutationOperationKind.BrokerMaintenance => Rule(
            new[]
            {
                AuthorizationAction.BrokerMaintenance,
                AuthorizationAction.ClusterRead,
                AuthorizationAction.BrokerRead,
            },
            AuthorizationAction.TopicRead,
            AuthorizationAction.TopicConfigRead,
            AuthorizationAction.BrokerConfigRead,
            AuthorizationAction.PartitionReassign,
            AuthorizationAction.ReplicationFactorAlter,
            AuthorizationAction.ReassignmentThrottle,
            AuthorizationAction.TopicAlter,
            AuthorizationAction.ClusterConfigAlter),

        MutationOperationKind.LogDirectoryMaintenance => Rule(
            new[]
            {
                AuthorizationAction.LogdirMaintenance,
                AuthorizationAction.ClusterRead,
                AuthorizationAction.BrokerRead,
            },
            AuthorizationAction.TopicRead,
            AuthorizationAction.TopicConfigRead,
            AuthorizationAction.BrokerConfigRead,
            AuthorizationAction.PartitionReassign,
            AuthorizationAction.ReplicationFactorAlter,
            AuthorizationAction.ReassignmentThrottle,
            AuthorizationAction.TopicAlter,
            AuthorizationAction.ClusterConfigAlter),

        MutationOperationKind.ClusterTransfer => Rule(
            new[]
            {
                AuthorizationAction.ClusterTransferPlan,
                AuthorizationAction.ClusterTransferExecute,
                AuthorizationAction.ClusterRead,
                AuthorizationAction.TopicRead,
                AuthorizationAction.RecordRead,
                AuthorizationAction.RecordExport,
                AuthorizationAction.RecordProduce,
            },
            AuthorizationAction.TopicConfigRead,
            AuthorizationAction.BrokerConfigRead),

        MutationOperationKind.ReplicationIntegration => Rule(
            new[]
            {
                AuthorizationAction.ReplicationIntegrationAlter,
                AuthorizationAction.ClusterRead,
                AuthorizationAction.TopicRead,
                AuthorizationAction.ConnectRead,
                AuthorizationAction.RecordRead,
                AuthorizationAction.RecordExport,
                AuthorizationAction.RecordProduce,
            },
            AuthorizationAction.ConnectCreate,
            AuthorizationAction.ConnectAlter,
            AuthorizationAction.TopicCreate,
            AuthorizationAction.TopicConfigRead,
            AuthorizationAction.BrokerConfigRead),

        MutationOperationKind.FleetUncertaintyDisposition => UncertaintyRule(),

        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported fleet mutation kind."),
    };

    private static AuthorizationRule Rule(
        AuthorizationAction[] requiredActions,
        params AuthorizationAction[] optionalActions) =>
        new(
            requiredActions,
            requiredActions
                .Concat(optionalActions)
                .Distinct()
                .ToArray());

    private static AuthorizationRule UncertaintyRule()
    {
        var disallowed = new HashSet<AuthorizationAction>
        {
            AuthorizationAction.SystemRead,
            AuthorizationAction.TopicList,
            AuthorizationAction.ConsumerRead,
            AuthorizationAction.SchemaRead,
            AuthorizationAction.KsqlRead,
            AuthorizationAction.CatalogRead,
        };

        return Rule(
            new[] { AuthorizationAction.MutationReconcile },
            Enum.GetValues<AuthorizationAction>()
                .Where(action => !disallowed.Contains(action))
                .ToArray());
    }

    private static string RequireBounded(string value, string fieldName, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (normalized.Length > maxLength || normalized.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                fieldName,
                $"{fieldName} must be at most {maxLength} characters and contain no control characters.");
        }

        return normalized;
    }

    private sealed record AuthorizationRule(
        IReadOnlyList<AuthorizationAction> RequiredActions,
        IReadOnlyList<AuthorizationAction> AllowedActions);
}
