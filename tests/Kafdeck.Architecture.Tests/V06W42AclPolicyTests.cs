using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W42AclPolicyTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 25, 6, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Kernel_selects_fleet_authorization_without_widening_legacy_normalizer()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            MutationAuthorization.ExpectedAction(MutationOperationKind.AclAlter));

        var requirements = new[]
        {
            Target(AuthorizationAction.AclRead, "prod", "acl/topic/payments/a1"),
            Target(AuthorizationAction.AclAlter, "prod", "acl/topic/payments/a1"),
        };

        var normalized = MutationAuthorizationRequirements.Normalize(
            MutationOperationKind.AclAlter,
            "prod",
            requirements);

        Assert.Equal(2, normalized.Count);

        Assert.Throws<ArgumentException>(() =>
            MutationAuthorizationRequirements.Normalize(
                MutationOperationKind.AclAlter,
                "prod",
                new[]
                {
                    Target(AuthorizationAction.AclRead, "dr", "acl/topic/payments/a1"),
                    Target(AuthorizationAction.AclAlter, "dr", "acl/topic/payments/a1"),
                }));
    }

    [Fact]
    public void Acl_operation_can_enter_common_preview_kernel_with_closed_conjunction()
    {
        var binding = Binding(
            resourceName: "payments",
            principal: "User:alice",
            operation: KafkaAclOperation.Read,
            permission: KafkaAclPermissionType.Allow);
        var fingerprint = AclMutationPolicy.FingerprintBindings(Array.Empty<KafkaAclBinding>());
        var plan = new AclMutationPlan(
            AclMutationMode.Create,
            "prod",
            new[] { binding },
            Array.Empty<KafkaAclBinding>(),
            null,
            fingerprint);
        var intent = AclMutationPolicy.BuildIntent(plan);
        var risk = AclMutationPolicy.ClassifyRisk(
            plan.CreateBindings,
            plan.RemoveBindings);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            intent,
            risk,
            "v0.6-w42",
            Now.AddMinutes(5),
            Now,
            "w42-acl-preview");

        Assert.Equal(MutationOperationKind.AclAlter, operation.Snapshot.OperationKind);
        Assert.Equal(MutationRiskClass.High, operation.Snapshot.Risk.RiskClass);
        Assert.True(operation.Snapshot.AuthorizationTargets.Any(
            target => target.Action == AuthorizationAction.AclRead));
        Assert.True(operation.Snapshot.AuthorizationTargets.Any(
            target => target.Action == AuthorizationAction.AclAlter));
        Assert.Equal(2, operation.Snapshot.AuthorizationTargets.Count);
    }

    [Fact]
    public void Protected_principals_and_grant_ceiling_are_server_owned()
    {
        var policy = Policy(
            protectedPrincipals: new[] { "User:kafdeck-service" },
            allowPrefixed: false,
            allowWildcard: false,
            allowAll: false);

        Assert.Throws<AclPolicyException>(() =>
            AclMutationPolicy.ValidateCreates(
                new[]
                {
                    Binding(
                        "payments",
                        "User:kafdeck-service",
                        KafkaAclOperation.Read,
                        KafkaAclPermissionType.Allow),
                },
                policy));

        var wildcard = Binding(
            "*",
            "User:alice",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);
        Assert.Throws<AclPolicyException>(() =>
            AclMutationPolicy.ValidateCreates(new[] { wildcard }, policy));

        var prefix = Binding(
            "payments.",
            "User:alice",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow,
            KafkaAclPatternType.Prefixed);
        Assert.Throws<AclPolicyException>(() =>
            AclMutationPolicy.ValidateCreates(new[] { prefix }, policy));

        var all = Binding(
            "payments",
            "User:alice",
            KafkaAclOperation.All,
            KafkaAclPermissionType.Allow);
        Assert.Throws<AclPolicyException>(() =>
            AclMutationPolicy.ValidateCreates(new[] { all }, policy));
    }

    [Fact]
    public void Broad_grants_and_sensitive_removals_are_critical()
    {
        var broadGrant = Binding(
            "*",
            "User:alice",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);
        var grantRisk = AclMutationPolicy.ClassifyRisk(
            new[] { broadGrant },
            Array.Empty<KafkaAclBinding>());
        Assert.Equal(MutationRiskClass.Critical, grantRisk.RiskClass);
        Assert.True(grantRisk.RequiresIndependentApproval);
        Assert.Contains("acl_broad_grant", grantRisk.Reasons);

        var denyRemoval = Binding(
            "payments",
            "User:alice",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Deny);
        var removeRisk = AclMutationPolicy.ClassifyRisk(
            Array.Empty<KafkaAclBinding>(),
            new[] { denyRemoval });
        Assert.Equal(MutationRiskClass.Critical, removeRisk.RiskClass);
        Assert.True(removeRisk.RequiresIndependentApproval);
        Assert.Contains("acl_security_sensitive_removal", removeRisk.Reasons);
    }

    [Fact]
    public void Exact_binding_identity_binds_host_pattern_operation_and_permission()
    {
        var baseline = Binding(
            "payments",
            "User:alice",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);

        var changedHost = baseline with { Host = "10.0.0.8" };
        var changedPattern = baseline with { PatternType = KafkaAclPatternType.Prefixed };
        var changedOperation = baseline with { Operation = KafkaAclOperation.Write };
        var changedPermission = baseline with { PermissionType = KafkaAclPermissionType.Deny };

        var hashes = new[]
        {
            AclBindingIdentity.Hash(baseline),
            AclBindingIdentity.Hash(changedHost),
            AclBindingIdentity.Hash(changedPattern),
            AclBindingIdentity.Hash(changedOperation),
            AclBindingIdentity.Hash(changedPermission),
        };

        Assert.Equal(hashes.Length, hashes.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void Access_analysis_reports_observed_evidence_without_claiming_effective_access()
    {
        var bindings = new[]
        {
            Binding(
                "payments.",
                "User:alice",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Allow,
                KafkaAclPatternType.Prefixed),
            Binding(
                "payments.orders",
                "User:alice",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Deny),
        };

        var analysis = AclMutationPolicy.AnalyzeObservedAccess(
            new AclAccessQuery(
                KafkaAclResourceType.Topic,
                "payments.orders",
                "User:alice",
                "10.0.0.10",
                KafkaAclOperation.Read),
            bindings);

        Assert.Equal(AclAccessEvidenceState.ConflictingEvidence, analysis.EvidenceState);
        Assert.Equal(2, analysis.MatchingBindings.Count);
        Assert.False(analysis.EffectiveAccessKnown);

        var none = AclMutationPolicy.AnalyzeObservedAccess(
            new AclAccessQuery(
                KafkaAclResourceType.Topic,
                "audit.events",
                "User:alice",
                "10.0.0.10",
                KafkaAclOperation.Read),
            bindings);
        Assert.Equal(AclAccessEvidenceState.NoMatchingBinding, none.EvidenceState);
        Assert.False(none.EffectiveAccessKnown);
    }

    [Fact]
    public void Completely_unbounded_acl_filter_is_rejected()
    {
        var exception = Assert.Throws<AclPolicyException>(() =>
            AclMutationPolicy.NormalizeFilter(new KafkaAclBindingFilter()));

        Assert.Equal(AclPolicyFailureCode.InvalidFilter, exception.Code);
    }

    private static KafkaAclBinding Binding(
        string resourceName,
        string principal,
        KafkaAclOperation operation,
        KafkaAclPermissionType permission,
        KafkaAclPatternType pattern = KafkaAclPatternType.Literal) =>
        new(
            KafkaAclResourceType.Topic,
            resourceName,
            pattern,
            principal,
            "*",
            operation,
            permission);

    private static AclServerPolicy Policy(
        IReadOnlyList<string> protectedPrincipals,
        bool allowPrefixed,
        bool allowWildcard,
        bool allowAll) =>
        new(
            protectedPrincipals,
            Enum.GetValues<KafkaAclResourceType>(),
            Enum.GetValues<KafkaAclOperation>(),
            allowPrefixed,
            allowWildcard,
            allowAll,
            maxBindingsPerMutation: 64);

    private static MutationAuthorizationTarget Target(
        AuthorizationAction action,
        string clusterId,
        string resource) =>
        new(action, clusterId, resource);
}
