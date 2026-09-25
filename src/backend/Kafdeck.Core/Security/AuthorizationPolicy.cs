using System.Collections.Frozen;

namespace Kafdeck.Core.Security;

public enum AuthorizationAction
{
    SystemRead = 1,
    ClusterRead = 2,
    BrokerRead = 3,
    TopicList = 4,
    TopicRead = 5,
    TopicConfigRead = 6,
    BrokerConfigRead = 7,
    RecordRead = 8,
    RecordExport = 9,
    ConsumerRead = 10,
    SchemaRead = 11,
    ConnectRead = 12,
    KsqlRead = 13,
    CatalogRead = 14,
    TopicCreate = 15,
    TopicAlter = 16,
    TopicDelete = 17,
    RecordProduce = 18,
    ConsumerOffsetAlter = 19,
    ConsumerDelete = 20,
    SchemaCreate = 21,
    SchemaAlter = 22,
    SchemaDelete = 23,
    ConnectCreate = 24,
    ConnectAlter = 25,
    ConnectDelete = 26,
    RecordsPurge = 27,
    AclRead = 28,
    AclAlter = 29,
    ScramRead = 30,
    ScramAlter = 31,
    QuotaRead = 32,
    QuotaAlter = 33,
    ClusterConfigRead = 34,
    ClusterConfigAlter = 35,
    LeaderElect = 36,
    PartitionReassign = 37,
    ReplicationFactorAlter = 38,
    ReassignmentThrottle = 39,
    BrokerMaintenance = 40,
    LogdirMaintenance = 41,
    ClusterTransferPlan = 42,
    ClusterTransferExecute = 43,
    ReplicationIntegrationAlter = 44,
    MutationReconcile = 45,
}

public enum AuthorizationDecisionReason
{
    Allowed = 1,
    Unauthenticated = 2,
    NoMatchingBinding = 3,
    ActionDenied = 4,
    ResourceDenied = 5,
}

public sealed record AuthorizationPermissionDefinition(
    AuthorizationAction Action,
    IReadOnlyList<string>? ClusterIds = null,
    IReadOnlyList<string>? ResourcePatterns = null);

public sealed record AuthorizationRoleDefinition(
    string Id,
    IReadOnlyList<AuthorizationPermissionDefinition> Permissions);

public sealed record AuthorizationSubjectBindingDefinition(
    string Issuer,
    string Subject,
    IReadOnlyList<string> RoleIds);

public sealed record AuthorizationGroupBindingDefinition(
    string ExternalGroup,
    IReadOnlyList<string> RoleIds);

public sealed record AuthorizationPolicyDefinition(
    IReadOnlyList<AuthorizationRoleDefinition> Roles,
    IReadOnlyList<AuthorizationSubjectBindingDefinition> SubjectBindings,
    IReadOnlyList<AuthorizationGroupBindingDefinition> GroupBindings);

public sealed record AuthorizationRequest(
    AuthorizationAction Action,
    string? ClusterId = null,
    string? ResourceName = null);

public sealed record AuthorizationDecision(
    bool IsAllowed,
    AuthorizationDecisionReason Reason,
    IReadOnlyList<string> MatchedRoleIds)
{
    public static AuthorizationDecision Denied(AuthorizationDecisionReason reason, IReadOnlyList<string>? roles = null) =>
        new(false, reason, roles ?? Array.Empty<string>());

    public static AuthorizationDecision Allowed(IReadOnlyList<string> roles) =>
        new(true, AuthorizationDecisionReason.Allowed, roles);
}

public sealed class AuthorizationPolicyException : Exception
{
    public AuthorizationPolicyException(string message)
        : base(message)
    {
    }
}

public sealed class AuthorizationPolicySnapshot
{
    internal AuthorizationPolicySnapshot(
        FrozenDictionary<string, CompiledRole> roles,
        FrozenDictionary<OperatorIdentityKey, FrozenSet<string>> subjectBindings,
        FrozenDictionary<string, FrozenSet<string>> groupBindings)
    {
        Roles = roles;
        SubjectBindings = subjectBindings;
        GroupBindings = groupBindings;
    }

    internal FrozenDictionary<string, CompiledRole> Roles { get; }

    internal FrozenDictionary<OperatorIdentityKey, FrozenSet<string>> SubjectBindings { get; }

    internal FrozenDictionary<string, FrozenSet<string>> GroupBindings { get; }
}

internal sealed record CompiledRole(
    string Id,
    IReadOnlyList<CompiledPermission> Permissions);

internal sealed record CompiledPermission(
    AuthorizationAction Action,
    FrozenSet<string> ClusterIds,
    IReadOnlyList<BoundedGlob> ResourcePatterns);

internal sealed class BoundedGlob
{
    public const int MaxPatternLength = 256;
    public const int MaxWildcards = 8;

    private readonly string _pattern;

    public BoundedGlob(string pattern)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        var normalized = pattern.Trim();
        if (normalized.Length > MaxPatternLength)
        {
            throw new AuthorizationPolicyException($"Resource pattern must not exceed {MaxPatternLength} characters.");
        }

        var wildcardCount = normalized.Count(character => character == '*');
        if (wildcardCount > MaxWildcards)
        {
            throw new AuthorizationPolicyException($"Resource pattern must not contain more than {MaxWildcards} wildcards.");
        }

        if (normalized.Any(char.IsControl))
        {
            throw new AuthorizationPolicyException("Resource pattern must not contain control characters.");
        }

        _pattern = normalized;
    }

    public bool IsMatch(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var patternIndex = 0;
        var valueIndex = 0;
        var starIndex = -1;
        var retryValueIndex = -1;

        while (valueIndex < value.Length)
        {
            if (patternIndex < _pattern.Length && _pattern[patternIndex] != '*' &&
                _pattern[patternIndex] == value[valueIndex])
            {
                patternIndex++;
                valueIndex++;
                continue;
            }

            if (patternIndex < _pattern.Length && _pattern[patternIndex] == '*')
            {
                starIndex = patternIndex++;
                retryValueIndex = valueIndex;
                continue;
            }

            if (starIndex >= 0)
            {
                patternIndex = starIndex + 1;
                valueIndex = ++retryValueIndex;
                continue;
            }

            return false;
        }

        while (patternIndex < _pattern.Length && _pattern[patternIndex] == '*')
        {
            patternIndex++;
        }

        return patternIndex == _pattern.Length;
    }
}

public static class AuthorizationPolicyCompiler
{
    private const int MaxRoles = 256;
    private const int MaxPermissionsPerRole = 128;
    private const int MaxBindings = 4096;
    private const int MaxRoleBindingsPerPrincipal = 64;
    private const int MaxScopeEntries = 256;

    public static AuthorizationPolicySnapshot Compile(AuthorizationPolicyDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        if (definition.Roles.Count > MaxRoles)
        {
            throw new AuthorizationPolicyException($"Authorization policy must not define more than {MaxRoles} roles.");
        }

        if (definition.SubjectBindings.Count + definition.GroupBindings.Count > MaxBindings)
        {
            throw new AuthorizationPolicyException($"Authorization policy must not define more than {MaxBindings} bindings.");
        }

        var roles = new Dictionary<string, CompiledRole>(StringComparer.Ordinal);
        foreach (var role in definition.Roles)
        {
            var roleId = RequireIdentifier(role.Id, "Role ID", 128);
            if (roles.ContainsKey(roleId))
            {
                throw new AuthorizationPolicyException($"Role '{roleId}' is defined more than once.");
            }

            if (role.Permissions.Count == 0)
            {
                throw new AuthorizationPolicyException($"Role '{roleId}' must define at least one permission.");
            }

            if (role.Permissions.Count > MaxPermissionsPerRole)
            {
                throw new AuthorizationPolicyException(
                    $"Role '{roleId}' must not define more than {MaxPermissionsPerRole} permissions.");
            }

            var permissions = role.Permissions.Select(CompilePermission).ToArray();
            roles.Add(roleId, new CompiledRole(roleId, Array.AsReadOnly(permissions)));
        }

        var subjectBindings = new Dictionary<OperatorIdentityKey, HashSet<string>>();
        foreach (var binding in definition.SubjectBindings)
        {
            var key = new OperatorIdentityKey(binding.Issuer, binding.Subject);
            var roleIds = ValidateRoleIds(binding.RoleIds, roles, $"Subject binding '{key.Issuer}|{key.Subject}'");

            if (!subjectBindings.TryGetValue(key, out var existing))
            {
                existing = new HashSet<string>(StringComparer.Ordinal);
                subjectBindings.Add(key, existing);
            }

            existing.UnionWith(roleIds);
        }

        var groupBindings = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (var binding in definition.GroupBindings)
        {
            var group = RequireIdentifier(binding.ExternalGroup, "External group", 512);
            var roleIds = ValidateRoleIds(binding.RoleIds, roles, $"Group binding '{group}'");

            if (!groupBindings.TryGetValue(group, out var existing))
            {
                existing = new HashSet<string>(StringComparer.Ordinal);
                groupBindings.Add(group, existing);
            }

            existing.UnionWith(roleIds);
        }

        return new AuthorizationPolicySnapshot(
            roles.ToFrozenDictionary(StringComparer.Ordinal),
            subjectBindings.ToFrozenDictionary(
                pair => pair.Key,
                pair => pair.Value.ToFrozenSet(StringComparer.Ordinal)),
            groupBindings.ToFrozenDictionary(
                pair => pair.Key,
                pair => pair.Value.ToFrozenSet(StringComparer.Ordinal),
                StringComparer.Ordinal));
    }

    private static CompiledPermission CompilePermission(AuthorizationPermissionDefinition permission)
    {
        var clusters = NormalizeScope(permission.ClusterIds, "Cluster scope");
        var patterns = NormalizePatterns(permission.ResourcePatterns);

        return new CompiledPermission(
            permission.Action,
            clusters.ToFrozenSet(StringComparer.Ordinal),
            Array.AsReadOnly(patterns));
    }

    private static string[] NormalizeScope(IReadOnlyList<string>? values, string fieldName)
    {
        var items = values ?? Array.Empty<string>();
        if (items.Count > MaxScopeEntries)
        {
            throw new AuthorizationPolicyException($"{fieldName} must not contain more than {MaxScopeEntries} entries.");
        }

        return items
            .Select(value => RequireIdentifier(value, fieldName, 256))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
    }

    private static BoundedGlob[] NormalizePatterns(IReadOnlyList<string>? values)
    {
        var items = values ?? Array.Empty<string>();
        if (items.Count > MaxScopeEntries)
        {
            throw new AuthorizationPolicyException(
                $"Resource pattern scope must not contain more than {MaxScopeEntries} entries.");
        }

        return items
            .Distinct(StringComparer.Ordinal)
            .Select(pattern => new BoundedGlob(pattern))
            .ToArray();
    }

    private static string[] ValidateRoleIds(
        IReadOnlyList<string> values,
        IReadOnlyDictionary<string, CompiledRole> roles,
        string bindingName)
    {
        if (values.Count == 0)
        {
            throw new AuthorizationPolicyException($"{bindingName} must reference at least one role.");
        }

        if (values.Count > MaxRoleBindingsPerPrincipal)
        {
            throw new AuthorizationPolicyException(
                $"{bindingName} must not reference more than {MaxRoleBindingsPerPrincipal} roles.");
        }

        var roleIds = values
            .Select(role => RequireIdentifier(role, "Role reference", 128))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        foreach (var roleId in roleIds)
        {
            if (!roles.ContainsKey(roleId))
            {
                throw new AuthorizationPolicyException($"{bindingName} references unknown role '{roleId}'.");
            }
        }

        return roleIds;
    }

    private static string RequireIdentifier(string value, string fieldName, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();

        if (normalized.Length > maxLength)
        {
            throw new AuthorizationPolicyException($"{fieldName} must not exceed {maxLength} characters.");
        }

        if (normalized.Any(char.IsControl))
        {
            throw new AuthorizationPolicyException($"{fieldName} must not contain control characters.");
        }

        return normalized;
    }
}

public sealed class AuthorizationPolicyEvaluator
{
    private readonly AuthorizationPolicySnapshot _snapshot;

    public AuthorizationPolicyEvaluator(AuthorizationPolicySnapshot snapshot)
    {
        _snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
    }

    public bool HasApplicablePermission(
        OperatorIdentity? identity,
        AuthorizationAction action,
        string? clusterId = null)
    {
        if (identity is null)
        {
            return false;
        }

        var roleIds = ResolveRoleIds(identity);
        foreach (var roleId in roleIds)
        {
            var role = _snapshot.Roles[roleId];
            foreach (var permission in role.Permissions)
            {
                if (permission.Action != action)
                {
                    continue;
                }

                if (permission.ClusterIds.Count > 0 &&
                    (string.IsNullOrWhiteSpace(clusterId) || !permission.ClusterIds.Contains(clusterId)))
                {
                    continue;
                }

                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Re-evaluates only current direct subject bindings for a durable canonical
    /// operator identity. External-group roles are intentionally excluded:
    /// callers must not reconstruct or trust persisted OIDC group claims.
    /// Ambiguous canonical identities fail closed.
    /// </summary>
    public AuthorizationDecision EvaluateCanonicalDirectSubject(
        string canonicalPrincipalId,
        AuthorizationRequest request)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(canonicalPrincipalId);
        ArgumentNullException.ThrowIfNull(request);

        var keys = _snapshot.SubjectBindings.Keys
            .Where(key => string.Equals(
                SecurityAuditPrincipal.FromOperatorKey(key),
                canonicalPrincipalId,
                StringComparison.Ordinal))
            .Take(2)
            .ToArray();

        if (keys.Length != 1)
        {
            return AuthorizationDecision.Denied(
                AuthorizationDecisionReason.NoMatchingBinding);
        }

        return Evaluate(
            new OperatorIdentity(keys[0]),
            request);
    }

    public AuthorizationDecision Evaluate(OperatorIdentity? identity, AuthorizationRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (identity is null)
        {
            return AuthorizationDecision.Denied(AuthorizationDecisionReason.Unauthenticated);
        }

        var roleIds = ResolveRoleIds(identity);
        if (roleIds.Count == 0)
        {
            return AuthorizationDecision.Denied(AuthorizationDecisionReason.NoMatchingBinding);
        }

        var actionMatched = false;
        foreach (var roleId in roleIds)
        {
            var role = _snapshot.Roles[roleId];
            foreach (var permission in role.Permissions)
            {
                if (permission.Action != request.Action)
                {
                    continue;
                }

                actionMatched = true;
                if (ScopeAllows(permission, request))
                {
                    return AuthorizationDecision.Allowed(roleIds);
                }
            }
        }

        return AuthorizationDecision.Denied(
            actionMatched ? AuthorizationDecisionReason.ResourceDenied : AuthorizationDecisionReason.ActionDenied,
            roleIds);
    }

    private IReadOnlyList<string> ResolveRoleIds(OperatorIdentity identity)
    {
        var roleIds = new HashSet<string>(StringComparer.Ordinal);

        if (_snapshot.SubjectBindings.TryGetValue(identity.Key, out var subjectRoles))
        {
            roleIds.UnionWith(subjectRoles);
        }

        foreach (var group in identity.ExternalGroups)
        {
            if (_snapshot.GroupBindings.TryGetValue(group, out var groupRoles))
            {
                roleIds.UnionWith(groupRoles);
            }
        }

        return roleIds.OrderBy(role => role, StringComparer.Ordinal).ToArray();
    }

    private static bool ScopeAllows(CompiledPermission permission, AuthorizationRequest request)
    {
        if (permission.ClusterIds.Count > 0)
        {
            if (string.IsNullOrWhiteSpace(request.ClusterId) || !permission.ClusterIds.Contains(request.ClusterId))
            {
                return false;
            }
        }

        if (permission.ResourcePatterns.Count > 0)
        {
            if (string.IsNullOrWhiteSpace(request.ResourceName))
            {
                return false;
            }

            if (!permission.ResourcePatterns.Any(pattern => pattern.IsMatch(request.ResourceName)))
            {
                return false;
            }
        }

        return true;
    }
}
