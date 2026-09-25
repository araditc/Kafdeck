using System.Collections.ObjectModel;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

public enum KafkaAclResourceType
{
    Topic = 1,
    Group = 2,
    Cluster = 3,
    TransactionalId = 4,
}

public enum KafkaAclPatternType
{
    Literal = 1,
    Prefixed = 2,
}

public enum KafkaAclOperation
{
    All = 1,
    Read = 2,
    Write = 3,
    Create = 4,
    Delete = 5,
    Alter = 6,
    Describe = 7,
    ClusterAction = 8,
    DescribeConfigs = 9,
    AlterConfigs = 10,
    IdempotentWrite = 11,
}

public enum KafkaAclPermissionType
{
    Deny = 1,
    Allow = 2,
}

public enum KafkaAclFilterPatternMode
{
    Any = 1,
    Literal = 2,
    Prefixed = 3,
    Match = 4,
}

public sealed record KafkaAclBinding(
    KafkaAclResourceType ResourceType,
    string ResourceName,
    KafkaAclPatternType PatternType,
    string Principal,
    string Host,
    KafkaAclOperation Operation,
    KafkaAclPermissionType PermissionType);

public sealed record KafkaAclBindingFilter(
    KafkaAclResourceType? ResourceType = null,
    string? ResourceName = null,
    KafkaAclFilterPatternMode PatternMode = KafkaAclFilterPatternMode.Any,
    string? Principal = null,
    string? Host = null,
    KafkaAclOperation? Operation = null,
    KafkaAclPermissionType? PermissionType = null);

public enum AclMutationMode
{
    Create = 1,
    Remove = 2,
    Replace = 3,
}

public sealed record AclMutationPlan(
    AclMutationMode Mode,
    string ClusterId,
    IReadOnlyList<KafkaAclBinding> CreateBindings,
    IReadOnlyList<KafkaAclBinding> RemoveBindings,
    KafkaAclBindingFilter? SourceFilter,
    string ObservedBindingSetFingerprint);

public enum AclAccessEvidenceState
{
    NoMatchingBinding = 1,
    ObservedAllow = 2,
    ObservedDeny = 3,
    ConflictingEvidence = 4,
}

public sealed record AclAccessQuery(
    KafkaAclResourceType ResourceType,
    string ResourceName,
    string Principal,
    string Host,
    KafkaAclOperation Operation);

public sealed record AclAccessAnalysis(
    AclAccessEvidenceState EvidenceState,
    IReadOnlyList<KafkaAclBinding> MatchingBindings,
    bool EffectiveAccessKnown = false);

public enum AclPolicyFailureCode
{
    InvalidBinding = 1,
    InvalidFilter = 2,
    TooManyBindings = 3,
    ProtectedPrincipal = 4,
    GrantCeilingExceeded = 5,
}

public sealed class AclPolicyException : ArgumentException
{
    public AclPolicyException(AclPolicyFailureCode code, string message)
        : base(message)
    {
        Code = code;
    }

    public AclPolicyFailureCode Code { get; }
}

public sealed class AclServerPolicy
{
    private readonly HashSet<string> _protectedPrincipals;
    private readonly HashSet<string> _allowedGrantPrincipals;
    private readonly HashSet<KafkaAclResourceType> _grantResourceTypes;
    private readonly HashSet<KafkaAclOperation> _grantOperations;

    public AclServerPolicy(
        IEnumerable<string> protectedPrincipals,
        IEnumerable<string> allowedGrantPrincipals,
        IEnumerable<KafkaAclResourceType> grantResourceTypes,
        IEnumerable<KafkaAclOperation> grantOperations,
        bool allowPrefixedGrants,
        bool allowWildcardResourceGrants,
        bool allowAllOperationGrants,
        int maxBindingsPerMutation = AclMutationPolicy.DefaultMaxBindings)
    {
        ArgumentNullException.ThrowIfNull(protectedPrincipals);
        ArgumentNullException.ThrowIfNull(allowedGrantPrincipals);
        ArgumentNullException.ThrowIfNull(grantResourceTypes);
        ArgumentNullException.ThrowIfNull(grantOperations);

        if (maxBindingsPerMutation is < 1 or > AclMutationPolicy.HardMaxBindings)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBindingsPerMutation));
        }

        _protectedPrincipals = protectedPrincipals
            .Select(value => AclMutationPolicy.RequireExactIdentifier(
                value,
                "Protected principal",
                AclMutationPolicy.MaxPrincipalLength))
            .ToHashSet(StringComparer.Ordinal);

        _allowedGrantPrincipals = allowedGrantPrincipals
            .Select(value => AclMutationPolicy.RequireExactIdentifier(
                value,
                "Allowed ACL grant principal",
                AclMutationPolicy.MaxPrincipalLength))
            .ToHashSet(StringComparer.Ordinal);

        _grantResourceTypes = grantResourceTypes.ToHashSet();
        _grantOperations = grantOperations.ToHashSet();

        if (_allowedGrantPrincipals.Count == 0 ||
            _grantResourceTypes.Count == 0 ||
            _grantOperations.Count == 0)
        {
            throw new ArgumentException(
                "ACL grant ceiling must admit at least one principal, resource type and operation.");
        }

        if (_grantResourceTypes.Any(value => !Enum.IsDefined(value)) ||
            _grantOperations.Any(value => !Enum.IsDefined(value)))
        {
            throw new ArgumentOutOfRangeException(
                nameof(grantResourceTypes),
                "ACL grant ceiling contains an unsupported enum value.");
        }

        AllowPrefixedGrants = allowPrefixedGrants;
        AllowWildcardResourceGrants = allowWildcardResourceGrants;
        AllowAllOperationGrants = allowAllOperationGrants;
        MaxBindingsPerMutation = maxBindingsPerMutation;
    }

    public bool AllowPrefixedGrants { get; }
    public bool AllowWildcardResourceGrants { get; }
    public bool AllowAllOperationGrants { get; }
    public int MaxBindingsPerMutation { get; }

    public bool TargetsProtectedPrincipal(string principal)
    {
        if (_protectedPrincipals.Contains(principal))
        {
            return true;
        }

        return AclMutationPolicy.IsWildcardPrincipal(principal) &&
               _protectedPrincipals.Any(value =>
                   value.StartsWith("User:", StringComparison.Ordinal));
    }

    public bool AllowsGrantPrincipal(string principal) =>
        _allowedGrantPrincipals.Contains(principal);

    public bool AllowsGrantResourceType(KafkaAclResourceType resourceType) =>
        _grantResourceTypes.Contains(resourceType);

    public bool AllowsGrantOperation(KafkaAclOperation operation) =>
        _grantOperations.Contains(operation);
}

public static class AclMutationPolicy
{
    public const int DefaultMaxBindings = 25;
    public const int HardMaxBindings = 100;
    public const int MaxResourceNameLength = 249;
    public const int MaxPrincipalLength = 256;
    public const int MaxHostLength = 255;

    private static readonly JsonSerializerOptions CanonicalJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    public static KafkaAclBinding NormalizeBinding(KafkaAclBinding binding)
    {
        ArgumentNullException.ThrowIfNull(binding);

        if (!Enum.IsDefined(binding.ResourceType) ||
            !Enum.IsDefined(binding.PatternType) ||
            !Enum.IsDefined(binding.Operation) ||
            !Enum.IsDefined(binding.PermissionType))
        {
            throw InvalidBinding("ACL binding contains an unsupported enum value.");
        }

        return binding with
        {
            ResourceName = RequireExactIdentifier(
                binding.ResourceName,
                "ACL resource name",
                MaxResourceNameLength),
            Principal = RequireExactIdentifier(
                binding.Principal,
                "ACL principal",
                MaxPrincipalLength),
            Host = RequireExactIdentifier(
                binding.Host,
                "ACL host",
                MaxHostLength),
        };
    }

    public static KafkaAclBindingFilter NormalizeFilter(
        KafkaAclBindingFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);

        if (filter.ResourceType is { } resourceType &&
            !Enum.IsDefined(resourceType))
        {
            throw InvalidFilter("ACL filter contains an unsupported resource type.");
        }

        if (filter.Operation is { } operation && !Enum.IsDefined(operation))
        {
            throw InvalidFilter("ACL filter contains an unsupported operation.");
        }

        if (filter.PermissionType is { } permission &&
            !Enum.IsDefined(permission))
        {
            throw InvalidFilter("ACL filter contains an unsupported permission type.");
        }

        if (!Enum.IsDefined(filter.PatternMode))
        {
            throw InvalidFilter("ACL filter contains an unsupported pattern mode.");
        }

        var resourceName = NormalizeOptional(
            filter.ResourceName,
            "ACL filter resource name",
            MaxResourceNameLength);
        var principal = NormalizeOptional(
            filter.Principal,
            "ACL filter principal",
            MaxPrincipalLength);
        var host = NormalizeOptional(
            filter.Host,
            "ACL filter host",
            MaxHostLength);

        if (filter.PatternMode is
                KafkaAclFilterPatternMode.Literal or
                KafkaAclFilterPatternMode.Prefixed or
                KafkaAclFilterPatternMode.Match &&
            resourceName is null)
        {
            throw InvalidFilter(
                "Literal, prefixed and match ACL filters require an exact resource name.");
        }

        if (filter.ResourceType is null &&
            resourceName is null &&
            principal is null &&
            host is null &&
            filter.Operation is null &&
            filter.PermissionType is null)
        {
            throw InvalidFilter(
                "An entirely unbounded ACL filter is not admitted.");
        }

        return filter with
        {
            ResourceName = resourceName,
            Principal = principal,
            Host = host,
        };
    }

    public static KafkaAclBindingFilter NormalizeMutationFilter(
        KafkaAclBindingFilter filter)
    {
        var normalized = NormalizeFilter(filter);
        if (normalized.ResourceType is null ||
            normalized.ResourceName is null ||
            normalized.Principal is null)
        {
            throw new AclPolicyException(
                AclPolicyFailureCode.InvalidFilter,
                "ACL mutation filters must bind resource type, resource name and principal before provider expansion.");
        }

        return normalized;
    }

    public static KafkaAclBindingFilter ExactFilter(KafkaAclBinding binding)
    {
        var normalized = NormalizeBinding(binding);
        return new KafkaAclBindingFilter(
            normalized.ResourceType,
            normalized.ResourceName,
            normalized.PatternType == KafkaAclPatternType.Literal
                ? KafkaAclFilterPatternMode.Literal
                : KafkaAclFilterPatternMode.Prefixed,
            normalized.Principal,
            normalized.Host,
            normalized.Operation,
            normalized.PermissionType);
    }

    public static bool MatchesFilter(
        KafkaAclBinding binding,
        KafkaAclBindingFilter filter)
    {
        var normalizedBinding = NormalizeBinding(binding);
        var normalizedFilter = NormalizeFilter(filter);

        if (normalizedFilter.ResourceType is { } resourceType &&
            normalizedBinding.ResourceType != resourceType)
        {
            return false;
        }

        if (normalizedFilter.Principal is { } principal &&
            !string.Equals(normalizedBinding.Principal, principal, StringComparison.Ordinal))
        {
            return false;
        }

        if (normalizedFilter.Host is { } host &&
            !string.Equals(normalizedBinding.Host, host, StringComparison.Ordinal))
        {
            return false;
        }

        if (normalizedFilter.Operation is { } operation &&
            normalizedBinding.Operation != operation)
        {
            return false;
        }

        if (normalizedFilter.PermissionType is { } permission &&
            normalizedBinding.PermissionType != permission)
        {
            return false;
        }

        if (normalizedFilter.ResourceName is null)
        {
            return normalizedFilter.PatternMode == KafkaAclFilterPatternMode.Any;
        }

        return normalizedFilter.PatternMode switch
        {
            KafkaAclFilterPatternMode.Any =>
                string.Equals(
                    normalizedBinding.ResourceName,
                    normalizedFilter.ResourceName,
                    StringComparison.Ordinal),
            KafkaAclFilterPatternMode.Literal =>
                normalizedBinding.PatternType == KafkaAclPatternType.Literal &&
                string.Equals(
                    normalizedBinding.ResourceName,
                    normalizedFilter.ResourceName,
                    StringComparison.Ordinal),
            KafkaAclFilterPatternMode.Prefixed =>
                normalizedBinding.PatternType == KafkaAclPatternType.Prefixed &&
                string.Equals(
                    normalizedBinding.ResourceName,
                    normalizedFilter.ResourceName,
                    StringComparison.Ordinal),
            KafkaAclFilterPatternMode.Match =>
                normalizedBinding.PatternType switch
                {
                    KafkaAclPatternType.Literal =>
                        normalizedBinding.ResourceName == "*" ||
                        string.Equals(
                            normalizedBinding.ResourceName,
                            normalizedFilter.ResourceName,
                            StringComparison.Ordinal),
                    KafkaAclPatternType.Prefixed =>
                        normalizedFilter.ResourceName.StartsWith(
                            normalizedBinding.ResourceName,
                            StringComparison.Ordinal),
                    _ => false,
                },
            _ => false,
        };
    }

    public static AclMutationPlan DeserializePlan(string canonicalIntent)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(canonicalIntent);
        return JsonSerializer.Deserialize<AclMutationPlan>(
                   canonicalIntent,
                   CanonicalJsonOptions) ??
               throw new MutationStateException(
                   "Canonical ACL mutation intent could not be deserialized.");
    }

    public static IReadOnlyList<KafkaAclBinding> NormalizeExactBindings(
        IEnumerable<KafkaAclBinding> bindings,
        int maxBindings)
    {
        ArgumentNullException.ThrowIfNull(bindings);
        if (maxBindings is < 1 or > HardMaxBindings)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBindings));
        }

        var normalized = bindings
            .Select(NormalizeBinding)
            .Distinct()
            .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
            .Take(maxBindings + 1)
            .ToArray();

        if (normalized.Length == 0)
        {
            throw InvalidBinding("At least one exact ACL binding is required.");
        }

        if (normalized.Length > maxBindings)
        {
            throw new AclPolicyException(
                AclPolicyFailureCode.TooManyBindings,
                $"ACL mutation exceeds the server-owned {maxBindings}-binding limit.");
        }

        return Array.AsReadOnly(normalized);
    }

    public static IReadOnlyList<KafkaAclBinding> ValidateCreates(
        IEnumerable<KafkaAclBinding> bindings,
        AclServerPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        var normalized = NormalizeExactBindings(
            bindings,
            policy.MaxBindingsPerMutation);

        foreach (var binding in normalized)
        {
            EnsurePrincipalMutable(binding.Principal, policy);

            if (binding.PermissionType != KafkaAclPermissionType.Allow)
            {
                continue;
            }

            if (!policy.AllowsGrantPrincipal(binding.Principal) ||
                !policy.AllowsGrantResourceType(binding.ResourceType) ||
                !policy.AllowsGrantOperation(binding.Operation) ||
                (binding.PatternType == KafkaAclPatternType.Prefixed &&
                 !policy.AllowPrefixedGrants) ||
                (binding.ResourceName == "*" &&
                 !policy.AllowWildcardResourceGrants) ||
                (binding.Operation == KafkaAclOperation.All &&
                 !policy.AllowAllOperationGrants))
            {
                throw new AclPolicyException(
                    AclPolicyFailureCode.GrantCeilingExceeded,
                    "ACL allow binding exceeds the server-owned grant ceiling.");
            }
        }

        return normalized;
    }

    public static IReadOnlyList<KafkaAclBinding> ValidateRemovals(
        IEnumerable<KafkaAclBinding> bindings,
        AclServerPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        var normalized = NormalizeExactBindings(
            bindings,
            policy.MaxBindingsPerMutation);

        foreach (var binding in normalized)
        {
            EnsurePrincipalMutable(binding.Principal, policy);
        }

        return normalized;
    }

    public static MutationRiskDecision ClassifyRisk(
        IReadOnlyList<KafkaAclBinding> creates,
        IReadOnlyList<KafkaAclBinding> removals)
    {
        ArgumentNullException.ThrowIfNull(creates);
        ArgumentNullException.ThrowIfNull(removals);

        var total = creates
            .Concat(removals)
            .Distinct()
            .Count();
        if (total == 0)
        {
            throw InvalidBinding("ACL mutation must contain at least one exact effect.");
        }

        if (total > HardMaxBindings)
        {
            throw new AclPolicyException(
                AclPolicyFailureCode.TooManyBindings,
                $"ACL mutation exceeds the approved hard limit of {HardMaxBindings} exact entries.");
        }

        // ACL count has its own admitted budget: narrow edits remain HIGH up to
        // the default 25-entry threshold. The generic mutation classifier's
        // any-multiple-target escalation is intentionally not reused here.
        var floorInput = new MutationRiskInput(
            MutationOperationKind.AclAlter,
            TargetCount: 1);
        var floor = MutationRiskClassifier.Classify(floorInput);
        var reasons = floor.Reasons.ToList();
        var risk = floor.RiskClass;

        if (total > DefaultMaxBindings)
        {
            risk = MutationRiskClass.Critical;
            reasons.Add("acl_entry_count_above_default");
        }

        if (creates.Any(IsBroadGrant))
        {
            risk = MutationRiskClass.Critical;
            reasons.Add("acl_broad_grant");
        }

        if (removals.Any(IsSecuritySensitiveRemoval))
        {
            risk = MutationRiskClass.Critical;
            reasons.Add("acl_security_sensitive_removal");
        }

        return MutationRiskClassifier.EnforceBuiltInFloor(
            floorInput,
            new MutationRiskDecision(
                risk,
                Array.AsReadOnly(
                    reasons
                        .Distinct(StringComparer.Ordinal)
                        .OrderBy(value => value, StringComparer.Ordinal)
                        .ToArray()),
                MutationConfirmationMode.TypedTarget,
                risk == MutationRiskClass.Critical));
    }

    public static MutationIntentDescriptor BuildIntent(
        AclMutationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var clusterId = RequireExactIdentifier(
            plan.ClusterId,
            "ACL cluster ID",
            256);

        var creates = plan.CreateBindings
            .Select(NormalizeBinding)
            .Distinct()
            .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
            .ToArray();
        var removals = plan.RemoveBindings
            .Select(NormalizeBinding)
            .Distinct()
            .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
            .ToArray();

        if (creates.Length + removals.Length == 0)
        {
            throw InvalidBinding("ACL plan must contain at least one exact effect.");
        }

        var affected = creates
            .Concat(removals)
            .Distinct()
            .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
            .ToArray();

        var requirements = affected
            .SelectMany(binding =>
            {
                var resource = AclBindingIdentity.AuthorizationResource(binding);
                return new[]
                {
                    new MutationAuthorizationTarget(
                        AuthorizationAction.AclRead,
                        clusterId,
                        resource),
                    new MutationAuthorizationTarget(
                        AuthorizationAction.AclAlter,
                        clusterId,
                        resource),
                };
            })
            .ToArray();

        var resources = affected
            .Select(binding => AclBindingIdentity.ResourceKey(clusterId, binding))
            .ToArray();

        var normalizedPlan = plan with
        {
            ClusterId = clusterId,
            CreateBindings = Array.AsReadOnly(creates),
            RemoveBindings = Array.AsReadOnly(removals),
            SourceFilter = plan.SourceFilter is null
                ? null
                : NormalizeFilter(plan.SourceFilter),
            ObservedBindingSetFingerprint = RequireExactIdentifier(
                plan.ObservedBindingSetFingerprint,
                "ACL observed binding-set fingerprint",
                128),
        };

        return new MutationIntentDescriptor(
            MutationOperationKind.AclAlter,
            clusterId,
            JsonSerializer.Serialize(normalizedPlan, CanonicalJsonOptions),
            resources,
            Preconditions: new[]
            {
                new MutationPrecondition(
                    "acl.binding-set",
                    normalizedPlan.ObservedBindingSetFingerprint),
            },
            AuthorizationTargets: requirements);
    }

    public static string FingerprintBindings(
        IEnumerable<KafkaAclBinding> bindings)
    {
        ArgumentNullException.ThrowIfNull(bindings);

        var builder = new StringBuilder();
        foreach (var canonical in bindings
                     .Select(NormalizeBinding)
                     .Distinct()
                     .Select(AclBindingIdentity.Canonical)
                     .OrderBy(value => value, StringComparer.Ordinal))
        {
            Append(builder, "binding", canonical);
        }

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    public static AclAccessAnalysis AnalyzeObservedAccess(
        AclAccessQuery query,
        IEnumerable<KafkaAclBinding> observedBindings)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(observedBindings);

        if (!Enum.IsDefined(query.ResourceType) ||
            !Enum.IsDefined(query.Operation))
        {
            throw new ArgumentOutOfRangeException(
                nameof(query),
                "ACL access query contains an unsupported enum value.");
        }

        var resourceName = RequireExactIdentifier(
            query.ResourceName,
            "ACL access resource name",
            MaxResourceNameLength);
        var principal = RequireExactIdentifier(
            query.Principal,
            "ACL access principal",
            MaxPrincipalLength);
        var host = RequireExactIdentifier(
            query.Host,
            "ACL access host",
            MaxHostLength);

        var matches = observedBindings
            .Select(NormalizeBinding)
            .Distinct()
            .Where(binding =>
                BindingMatches(
                    binding,
                    query.ResourceType,
                    resourceName,
                    principal,
                    host,
                    query.Operation))
            .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
            .ToArray();

        var hasAllow = matches.Any(
            binding => binding.PermissionType == KafkaAclPermissionType.Allow);
        var hasDeny = matches.Any(
            binding => binding.PermissionType == KafkaAclPermissionType.Deny);

        var state = (hasAllow, hasDeny) switch
        {
            (false, false) => AclAccessEvidenceState.NoMatchingBinding,
            (true, false) => AclAccessEvidenceState.ObservedAllow,
            (false, true) => AclAccessEvidenceState.ObservedDeny,
            _ => AclAccessEvidenceState.ConflictingEvidence,
        };

        return new AclAccessAnalysis(
            state,
            Array.AsReadOnly(matches),
            EffectiveAccessKnown: false);
    }

    public static bool IsBroadGrant(KafkaAclBinding binding)
    {
        var normalized = NormalizeBinding(binding);
        return normalized.PermissionType == KafkaAclPermissionType.Allow &&
               (normalized.ResourceName == "*" ||
                normalized.PatternType == KafkaAclPatternType.Prefixed ||
                normalized.Operation == KafkaAclOperation.All ||
                IsWildcardPrincipal(normalized.Principal));
    }

    public static bool IsSecuritySensitiveRemoval(KafkaAclBinding binding)
    {
        var normalized = NormalizeBinding(binding);
        return normalized.PermissionType == KafkaAclPermissionType.Deny ||
               normalized.ResourceName == "*" ||
               normalized.PatternType == KafkaAclPatternType.Prefixed ||
               normalized.Operation == KafkaAclOperation.All ||
               IsWildcardPrincipal(normalized.Principal);
    }

    internal static string RequireExactIdentifier(
        string value,
        string field,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new AclPolicyException(
                AclPolicyFailureCode.InvalidBinding,
                $"{field} is not an admitted exact ACL identifier.");
        }

        return value;
    }

    private static string? NormalizeOptional(
        string? value,
        string field,
        int maxLength)
    {
        if (value is null)
        {
            return null;
        }

        try
        {
            return RequireExactIdentifier(value, field, maxLength);
        }
        catch (AclPolicyException exception)
        {
            throw new AclPolicyException(
                AclPolicyFailureCode.InvalidFilter,
                exception.Message);
        }
    }

    public static bool IsWildcardPrincipal(string principal) =>
        string.Equals(principal, "User:*", StringComparison.Ordinal);

    private static void EnsurePrincipalMutable(
        string principal,
        AclServerPolicy policy)
    {
        if (policy.TargetsProtectedPrincipal(principal))
        {
            throw new AclPolicyException(
                AclPolicyFailureCode.ProtectedPrincipal,
                "ACL mutation targets or overlaps a server-protected principal.");
        }
    }

    private static bool OperationProvidesEvidence(
        KafkaAclBinding binding,
        KafkaAclOperation requestedOperation)
    {
        if (binding.Operation == KafkaAclOperation.All ||
            binding.Operation == requestedOperation)
        {
            return true;
        }

        if (binding.PermissionType != KafkaAclPermissionType.Allow)
        {
            return false;
        }

        return requestedOperation switch
        {
            KafkaAclOperation.Describe =>
                binding.Operation is
                    KafkaAclOperation.Read or
                    KafkaAclOperation.Write or
                    KafkaAclOperation.Delete or
                    KafkaAclOperation.Alter,
            KafkaAclOperation.DescribeConfigs =>
                binding.Operation == KafkaAclOperation.AlterConfigs,
            _ => false,
        };
    }

    private static bool BindingMatches(
        KafkaAclBinding binding,
        KafkaAclResourceType resourceType,
        string resourceName,
        string principal,
        string host,
        KafkaAclOperation operation)
    {
        if (binding.ResourceType != resourceType ||
            !(string.Equals(binding.Principal, principal, StringComparison.Ordinal) ||
              IsWildcardPrincipal(binding.Principal)) ||
            !(string.Equals(binding.Host, host, StringComparison.Ordinal) ||
              binding.Host == "*") ||
            !OperationProvidesEvidence(binding, operation))
        {
            return false;
        }

        return binding.PatternType switch
        {
            KafkaAclPatternType.Literal =>
                binding.ResourceName == "*" ||
                string.Equals(
                    binding.ResourceName,
                    resourceName,
                    StringComparison.Ordinal),
            KafkaAclPatternType.Prefixed =>
                resourceName.StartsWith(
                    binding.ResourceName,
                    StringComparison.Ordinal),
            _ => false,
        };
    }

    private static AclPolicyException InvalidBinding(string message) =>
        new(AclPolicyFailureCode.InvalidBinding, message);

    private static AclPolicyException InvalidFilter(string message) =>
        new(AclPolicyFailureCode.InvalidFilter, message);

    private static void Append(
        StringBuilder builder,
        string key,
        string value) =>
        builder
            .Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}

public static class AclBindingIdentity
{
    public static string Canonical(KafkaAclBinding binding)
    {
        var normalized = AclMutationPolicy.NormalizeBinding(binding);
        var builder = new StringBuilder(512);
        Append(builder, "resource-type", ((int)normalized.ResourceType).ToString());
        Append(builder, "resource-name", normalized.ResourceName);
        Append(builder, "pattern-type", ((int)normalized.PatternType).ToString());
        Append(builder, "principal", normalized.Principal);
        Append(builder, "host", normalized.Host);
        Append(builder, "operation", ((int)normalized.Operation).ToString());
        Append(builder, "permission", ((int)normalized.PermissionType).ToString());
        return builder.ToString();
    }

    public static string Hash(KafkaAclBinding binding) =>
        Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(Canonical(binding))))
            .ToLowerInvariant();

    public static string AuthorizationResource(KafkaAclBinding binding)
    {
        var normalized = AclMutationPolicy.NormalizeBinding(binding);
        return $"acl/{normalized.ResourceType.ToString().ToLowerInvariant()}/" +
               $"{Uri.EscapeDataString(normalized.ResourceName)}/{Hash(normalized)[..20]}";
    }

    public static string ResourceKey(
        string clusterId,
        KafkaAclBinding binding)
    {
        var cluster = AclMutationPolicy.RequireExactIdentifier(
            clusterId,
            "ACL cluster ID",
            256);
        return $"cluster/{cluster}/acl/{Hash(binding)}";
    }

    private static void Append(
        StringBuilder builder,
        string key,
        string value) =>
        builder
            .Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}
