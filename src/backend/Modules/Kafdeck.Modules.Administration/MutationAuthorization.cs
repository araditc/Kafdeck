using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

public static class MutationAuthorization
{
    public static AuthorizationAction ExpectedAction(MutationOperationKind kind) => kind switch
    {
        MutationOperationKind.TopicCreate => AuthorizationAction.TopicCreate,
        MutationOperationKind.TopicAlter => AuthorizationAction.TopicAlter,
        MutationOperationKind.TopicIncreasePartitions => AuthorizationAction.TopicAlter,
        MutationOperationKind.TopicDelete => AuthorizationAction.TopicDelete,
        MutationOperationKind.RecordProduce => AuthorizationAction.RecordProduce,
        MutationOperationKind.ConsumerOffsetAlter => AuthorizationAction.ConsumerOffsetAlter,
        MutationOperationKind.ConsumerDelete => AuthorizationAction.ConsumerDelete,
        MutationOperationKind.SchemaCreate => AuthorizationAction.SchemaCreate,
        MutationOperationKind.SchemaAlter => AuthorizationAction.SchemaAlter,
        MutationOperationKind.SchemaDelete => AuthorizationAction.SchemaDelete,
        MutationOperationKind.ConnectCreate => AuthorizationAction.ConnectCreate,
        MutationOperationKind.ConnectAlter => AuthorizationAction.ConnectAlter,
        MutationOperationKind.ConnectDelete => AuthorizationAction.ConnectDelete,
        MutationOperationKind.RecordsPurge => AuthorizationAction.RecordsPurge,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported mutation operation kind."),
    };

    public static IReadOnlyList<MutationAuthorizationTarget> NormalizeTargets(
        MutationOperationKind kind,
        string clusterId,
        IReadOnlyList<MutationAuthorizationTarget>? targets,
        IReadOnlyList<string>? fallbackResourceKeys = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        var normalizedCluster = RequireBounded(clusterId, "Authorization cluster ID", 256);
        var expectedAction = ExpectedAction(kind);

        IReadOnlyList<MutationAuthorizationTarget> items =
            targets is { Count: > 0 }
                ? targets
                : (fallbackResourceKeys ?? Array.Empty<string>())
                    .Select(resourceKey => new MutationAuthorizationTarget(
                        expectedAction,
                        normalizedCluster,
                        resourceKey))
                    .ToArray();

        if (items.Count == 0 || items.Count > MutationLimits.MaxResourceKeys)
        {
            throw new ArgumentOutOfRangeException(
                nameof(targets),
                $"Mutation authorization targets must contain between 1 and {MutationLimits.MaxResourceKeys} items.");
        }
        var normalized = items
            .Select(target =>
            {
                ArgumentNullException.ThrowIfNull(target);
                if (target.Action != expectedAction)
                {
                    throw new ArgumentException(
                        $"Mutation kind '{kind}' requires authorization action '{expectedAction}', not '{target.Action}'.",
                        nameof(targets));
                }

                var targetCluster = RequireBounded(target.ClusterId, "Authorization target cluster", 256);
                if (!string.Equals(targetCluster, normalizedCluster, StringComparison.Ordinal))
                {
                    throw new ArgumentException(
                        "Mutation authorization target cluster must match the operation cluster.",
                        nameof(targets));
                }

                return new MutationAuthorizationTarget(
                    target.Action,
                    targetCluster,
                    RequireBounded(target.ResourceName, "Authorization resource name", 512));
            })
            .Distinct()
            .OrderBy(target => target.Action)
            .ThenBy(target => target.ClusterId, StringComparer.Ordinal)
            .ThenBy(target => target.ResourceName, StringComparer.Ordinal)
            .ToArray();

        return Array.AsReadOnly(normalized);
    }

    public static string? BuildConfirmationChallenge(
        MutationConfirmationMode mode,
        IReadOnlyList<MutationAuthorizationTarget> targets)
    {
        ArgumentNullException.ThrowIfNull(targets);
        if (mode == MutationConfirmationMode.Explicit)
        {
            return null;
        }

        if (mode != MutationConfirmationMode.TypedTarget)
        {
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unsupported mutation confirmation mode.");
        }

        if (targets.Count == 1)
        {
            return targets[0].ResourceName;
        }

        var digest = ComputeTargetDigest(targets);
        return $"MULTI:{targets.Count}:{digest[..16]}";
    }

    internal static string ComputeTargetDigest(
        IReadOnlyList<MutationAuthorizationTarget> targets)
    {
        ArgumentNullException.ThrowIfNull(targets);
        var builder = new StringBuilder();
        foreach (var target in targets
                     .OrderBy(target => target.Action)
                     .ThenBy(target => target.ClusterId, StringComparer.Ordinal)
                     .ThenBy(target => target.ResourceName, StringComparer.Ordinal))
        {
            builder
                .Append((int)target.Action).Append('|')
                .Append(target.ClusterId).Append('|')
                .Append(target.ResourceName).Append('\n');
        }

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
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
}

public sealed class MutationApprovalAuthorizationEvidence
{
    internal MutationApprovalAuthorizationEvidence(
        Guid operationId,
        string principalId,
        string previewHash,
        string evidenceHash)
    {
        OperationId = operationId;
        PrincipalId = principalId;
        PreviewHash = previewHash;
        EvidenceHash = evidenceHash;
    }

    public Guid OperationId { get; }
    public string PrincipalId { get; }
    public string PreviewHash { get; }
    public string EvidenceHash { get; }
}

public sealed class MutationApprovalAuthorizer
{
    private readonly AuthorizationPolicyEvaluator _authorization;

    public MutationApprovalAuthorizer(AuthorizationPolicyEvaluator authorization)
    {
        _authorization = authorization ?? throw new ArgumentNullException(nameof(authorization));
    }

    public MutationApprovalAuthorizationEvidence Authorize(
        OperatorIdentity approver,
        MutationOperationSnapshot operation)
    {
        ArgumentNullException.ThrowIfNull(approver);
        ArgumentNullException.ThrowIfNull(operation);

        if (operation.AuthorizationTargets.Count == 0)
        {
            throw new MutationStateException(
                "Mutation approval cannot be authorized without explicit authorization targets.");
        }

        foreach (var target in operation.AuthorizationTargets)
        {
            var decision = _authorization.Evaluate(
                approver,
                new AuthorizationRequest(
                    target.Action,
                    target.ClusterId,
                    target.ResourceName));

            if (!decision.IsAllowed)
            {
                throw new MutationStateException(
                    $"Approver is not authorized for mutation action '{target.Action}' on the required resource.");
            }
        }

        var principalId = SecurityAuditPrincipal.FromOperator(approver);
        var evidenceHash = ComputeEvidenceHash(
            operation.OperationId,
            principalId,
            operation.PreviewHash,
            operation.AuthorizationTargets);

        return new MutationApprovalAuthorizationEvidence(
            operation.OperationId,
            principalId,
            operation.PreviewHash,
            evidenceHash);
    }

    private static string ComputeEvidenceHash(
        Guid operationId,
        string principalId,
        string previewHash,
        IReadOnlyList<MutationAuthorizationTarget> targets)
    {
        var canonical = string.Join(
            "\n",
            operationId.ToString("D"),
            principalId,
            previewHash,
            MutationAuthorization.ComputeTargetDigest(targets));

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(canonical)))
            .ToLowerInvariant();
    }
}
