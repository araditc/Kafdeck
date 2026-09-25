using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

public static class MutationLimits
{
    public const int MaxResourceKeys = 1_024;
    public const int MaxPreconditions = 2_048;
    public const int MaxMaterialDigests = 64;
    public const int MaxCanonicalIntentCharacters = 128 * 1024;
    public const int MaxExecutionMaterialItemBytes = 16 * 1024 * 1024;
    public const int MaxExecutionMaterialTotalBytes = 32 * 1024 * 1024;
    public const int MaxProviderEvidenceEntries = 32;
    public const int MaxProviderEvidenceKeyCharacters = 128;
    public const int MaxProviderEvidenceValueCharacters = 1_024;
    public const int MaxProviderEvidenceTotalCharacters = 8 * 1_024;
}

public enum MutationOperationKind
{
    TopicCreate = 1,
    TopicAlter = 2,
    TopicIncreasePartitions = 3,
    TopicDelete = 4,
    RecordProduce = 5,
    ConsumerOffsetAlter = 6,
    ConsumerDelete = 7,
    SchemaCreate = 8,
    SchemaAlter = 9,
    SchemaDelete = 10,
    ConnectCreate = 11,
    ConnectAlter = 12,
    ConnectDelete = 13,
    RecordsPurge = 14,
    AclAlter = 15,
    ScramAlter = 16,
    QuotaAlter = 17,
    ClusterConfigAlter = 18,
    PreferredLeaderElection = 19,
    PartitionReassign = 20,
    ReplicationFactorAlter = 21,
    ReassignmentThrottle = 22,
    BrokerMaintenance = 23,
    LogDirectoryMaintenance = 24,
    ClusterTransfer = 25,
    ReplicationIntegration = 26,
    FleetUncertaintyDisposition = 27,
}

public enum MutationRiskClass
{
    Low = 1,
    Moderate = 2,
    High = 3,
    Critical = 4,
}

public enum MutationOperationState
{
    Previewed = 1,
    AwaitingConfirmation = 2,
    AwaitingApproval = 3,
    Ready = 4,
    Executing = 5,
    AppliedVerified = 6,
    AppliedUnverified = 7,
    PartiallyApplied = 8,
    ExecutionUnknown = 9,
    Rejected = 10,
    Expired = 11,
    Cancelled = 12,
    StalePreview = 13,
    FailedBeforeDispatch = 14,
    FailedDefinitive = 15,
}

public enum MutationConfirmationMode
{
    Explicit = 1,
    TypedTarget = 2,
}

public enum MutationExecutionResultKind
{
    AppliedVerified = 1,
    AppliedUnverified = 2,
    PartiallyApplied = 3,
    ExecutionUnknown = 4,
    FailedBeforeDispatch = 5,
    FailedDefinitive = 6,
}

public sealed record MutationPrecondition(string Key, string Fingerprint);

public sealed record MutationMaterialDigest(string Name, string Digest);

public sealed record MutationAuthorizationTarget(
    AuthorizationAction Action,
    string ClusterId,
    string ResourceName);

public sealed record MutationRiskContext(
    bool PermanentDelete = false,
    bool DurabilitySensitiveChange = false);

public sealed record MutationIntentDescriptor(
    MutationOperationKind Kind,
    string ClusterId,
    string CanonicalIntent,
    IReadOnlyList<string> ResourceKeys,
    IReadOnlyList<MutationPrecondition>? Preconditions = null,
    IReadOnlyList<MutationMaterialDigest>? MaterialDigests = null,
    IReadOnlyList<MutationAuthorizationTarget>? AuthorizationTargets = null,
    MutationRiskContext? RiskContext = null);

public sealed record MutationRiskDecision(
    MutationRiskClass RiskClass,
    IReadOnlyList<string> Reasons,
    MutationConfirmationMode ConfirmationMode,
    bool RequiresIndependentApproval);

public sealed record MutationOperationSnapshot
{
    public required Guid OperationId { get; init; }
    public required string RequesterPrincipalId { get; init; }
    public required string ClusterId { get; init; }
    public required MutationOperationKind OperationKind { get; init; }
    public required MutationRiskDecision Risk { get; init; }
    public required MutationOperationState State { get; init; }
    public required string CanonicalIntent { get; init; }
    public required string CanonicalIntentHash { get; init; }
    public required IReadOnlyList<string> ResourceKeys { get; init; }
    public required IReadOnlyList<MutationPrecondition> Preconditions { get; init; }
    public required IReadOnlyList<MutationMaterialDigest> MaterialDigests { get; init; }
    public required IReadOnlyList<MutationAuthorizationTarget> AuthorizationTargets { get; init; }
    public string? ConfirmationChallenge { get; init; }
    public required string PreviewHash { get; init; }
    public required DateTimeOffset PreviewExpiresAtUtc { get; init; }
    public required string PolicyVersion { get; init; }
    public required string IdempotencyScope { get; init; }
    public required string IdempotencyKeyHash { get; init; }
    public string? ConfirmedByPrincipalId { get; init; }
    public string? ConfirmedChallenge { get; init; }
    public DateTimeOffset? ConfirmedAtUtc { get; init; }
    public string? ApprovedByPrincipalId { get; init; }
    public string? ApprovalAuthorizationEvidenceHash { get; init; }
    public DateTimeOffset? ApprovedAtUtc { get; init; }
    public string? RejectedByPrincipalId { get; init; }
    public string? RejectionAuthorizationEvidenceHash { get; init; }
    public DateTimeOffset? RejectedAtUtc { get; init; }
    public long Version { get; init; }
    public long ExecutionClaimGeneration { get; init; }
    public DateTimeOffset? ExecutionClaimExpiresAtUtc { get; init; }
    public DateTimeOffset? DispatchStartedAtUtc { get; init; }
    public string? ResultCode { get; init; }
    public IReadOnlyDictionary<string, string> SafeProviderEvidence { get; init; } =
        new Dictionary<string, string>(StringComparer.Ordinal);
    public DateTimeOffset CreatedAtUtc { get; init; }
    public DateTimeOffset UpdatedAtUtc { get; init; }
}

public sealed class MutationStateException : InvalidOperationException
{
    public MutationStateException(string message)
        : base(message)
    {
    }
}

public static class MutationIdempotency
{
    public const int MaxKeyLength = 256;

    public static string BuildScope(
        string principalId,
        string clusterId,
        MutationOperationKind operationKind)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(principalId);
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        var builder = new StringBuilder(512);
        Append(builder, "principal", principalId.Trim());
        Append(builder, "cluster", clusterId.Trim());
        Append(
            builder,
            "operation-kind",
            ((int)operationKind).ToString(
                System.Globalization.CultureInfo.InvariantCulture));

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    public static string HashKey(string idempotencyKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idempotencyKey);
        if (idempotencyKey.Length > MaxKeyLength || idempotencyKey.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                nameof(idempotencyKey),
                $"Idempotency key must be at most {MaxKeyLength} characters and contain no control characters.");
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(idempotencyKey)))
            .ToLowerInvariant();
    }

    internal static string HashCanonicalIntent(string canonicalIntent)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(canonicalIntent);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonicalIntent)))
            .ToLowerInvariant();
    }

    internal static string HashAdmittedIntent(
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        string policyVersion)
    {
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentNullException.ThrowIfNull(risk);
        ArgumentException.ThrowIfNullOrWhiteSpace(policyVersion);

        var builder = new StringBuilder(2048);
        Append(builder, "kind", ((int)intent.Kind).ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(builder, "cluster", intent.ClusterId.Trim());
        Append(builder, "intent", HashCanonicalIntent(intent.CanonicalIntent));

        foreach (var resource in MutationPreviewHasher.NormalizeResources(intent.ResourceKeys))
        {
            Append(builder, "resource", resource);
        }

        foreach (var precondition in MutationPreviewHasher.NormalizePreconditions(intent.Preconditions))
        {
            Append(builder, "precondition-key", precondition.Key);
            Append(builder, "precondition-fingerprint", precondition.Fingerprint);
        }

        foreach (var digest in MutationPreviewHasher.NormalizeDigests(intent.MaterialDigests))
        {
            Append(builder, "material-name", digest.Name);
            Append(builder, "material-digest", digest.Digest);
        }

        foreach (var target in MutationAuthorizationRequirements.Normalize(
                     intent.Kind,
                     intent.ClusterId,
                     intent.AuthorizationTargets,
                     intent.ResourceKeys))
        {
            Append(builder, "authorization-action", ((int)target.Action).ToString(System.Globalization.CultureInfo.InvariantCulture));
            Append(builder, "authorization-cluster", target.ClusterId);
            Append(builder, "authorization-resource", target.ResourceName);
        }

        Append(builder, "risk", ((int)risk.RiskClass).ToString(System.Globalization.CultureInfo.InvariantCulture));
        foreach (var reason in risk.Reasons
                     .Where(reason => !string.IsNullOrWhiteSpace(reason))
                     .Select(reason => reason.Trim())
                     .Distinct(StringComparer.Ordinal)
                     .OrderBy(reason => reason, StringComparer.Ordinal))
        {
            Append(builder, "risk-reason", reason);
        }

        Append(builder, "confirmation", ((int)risk.ConfirmationMode).ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(builder, "independent-approval", risk.RequiresIndependentApproval ? "1" : "0");
        Append(builder, "policy-version", policyVersion.Trim());

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    private static void Append(StringBuilder builder, string key, string value)
    {
        builder
            .Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
    }

}
