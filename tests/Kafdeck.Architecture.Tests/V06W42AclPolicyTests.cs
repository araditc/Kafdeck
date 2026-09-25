using System.Globalization;
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
        Assert.Contains(
            operation.Snapshot.AuthorizationTargets,
            target => target.Action == AuthorizationAction.AclRead);
        Assert.Contains(
            operation.Snapshot.AuthorizationTargets,
            target => target.Action == AuthorizationAction.AclAlter);
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
    public void Authorization_resource_uses_the_full_exact_acl_binding_hash()
    {
        var baseline = Binding(
            "payments",
            "User:alice",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);
        var changedOperation = baseline with
        {
            Operation = KafkaAclOperation.Write,
        };
        var changedPermission = baseline with
        {
            PermissionType = KafkaAclPermissionType.Deny,
        };

        var baselineHash = AclBindingIdentity.Hash(baseline);
        var baselineResource = AclBindingIdentity.AuthorizationResource(baseline);

        Assert.EndsWith(
            "/" + baselineHash,
            baselineResource,
            StringComparison.Ordinal);
        Assert.Equal(64, baselineHash.Length);
        Assert.NotEqual(
            baselineResource,
            AclBindingIdentity.AuthorizationResource(changedOperation));
        Assert.NotEqual(
            baselineResource,
            AclBindingIdentity.AuthorizationResource(changedPermission));
    }

    [Fact]
    public void Exact_acl_binding_hash_is_stable_across_current_cultures()
    {
        var binding = Binding(
            "payments",
            "User:alice",
            KafkaAclOperation.AlterConfigs,
            KafkaAclPermissionType.Deny,
            KafkaAclPatternType.Prefixed);
        var original = CultureInfo.CurrentCulture;

        try
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            var invariant = AclBindingIdentity.Hash(binding);

            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fa-IR");
            var persian = AclBindingIdentity.Hash(binding);

            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ar-SA");
            var arabic = AclBindingIdentity.Hash(binding);

            Assert.Equal(invariant, persian);
            Assert.Equal(invariant, arabic);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void Fleet_conflict_identity_uses_the_full_exact_acl_binding_hash()
    {
        var allow = Binding(
            "payments",
            "User:alice",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);
        var deny = allow with { PermissionType = KafkaAclPermissionType.Deny };

        var allowKey = AclBindingIdentity.FleetConflictKey("prod", allow);
        var denyKey = AclBindingIdentity.FleetConflictKey("prod", deny);

        Assert.NotEqual(allowKey, denyKey);

        var target = FleetConflictKeyCodec.Decode(allowKey);
        Assert.Equal(FleetConflictTargetKind.AclBinding, target.Kind);
        Assert.Equal("prod", target.PhysicalClusterId);
        Assert.Equal(AclBindingIdentity.Hash(allow), target.ResourceId);
        Assert.Null(target.SubresourceId);
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
    public void Grant_principal_ceiling_and_wildcard_overlap_protect_service_principals()
    {
        var policy = new AclServerPolicy(
            new[] { "User:kafdeck-service" },
            new[] { "User:alice" },
            new[] { KafkaAclResourceType.Topic },
            new[] { KafkaAclOperation.Read },
            allowPrefixedGrants: false,
            allowWildcardResourceGrants: false,
            allowAllOperationGrants: false);

        var unauthorizedPrincipal = Binding(
            "payments",
            "User:attacker",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);
        var grant = Assert.Throws<AclPolicyException>(() =>
            AclMutationPolicy.ValidateCreates(
                new[] { unauthorizedPrincipal },
                policy));
        Assert.Equal(AclPolicyFailureCode.GrantCeilingExceeded, grant.Code);

        var wildcard = Binding(
            "payments",
            "User:*",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Deny);
        var protectedOverlap = Assert.Throws<AclPolicyException>(() =>
            AclMutationPolicy.ValidateRemovals(
                new[] { wildcard },
                policy));
        Assert.Equal(AclPolicyFailureCode.ProtectedPrincipal, protectedOverlap.Code);
    }

    [Fact]
    public void Narrow_multi_binding_edits_remain_high_until_acl_count_threshold()
    {
        var narrow = Enumerable.Range(0, AclMutationPolicy.DefaultMaxBindings)
            .Select(index => Binding(
                $"payments-{index}",
                "User:alice",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Allow))
            .ToArray();

        var high = AclMutationPolicy.ClassifyRisk(
            narrow,
            Array.Empty<KafkaAclBinding>());
        Assert.Equal(MutationRiskClass.High, high.RiskClass);
        Assert.False(high.RequiresIndependentApproval);

        var aboveDefault = narrow
            .Append(Binding(
                "payments-over-threshold",
                "User:alice",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Allow))
            .ToArray();
        var critical = AclMutationPolicy.ClassifyRisk(
            aboveDefault,
            Array.Empty<KafkaAclBinding>());
        Assert.Equal(MutationRiskClass.Critical, critical.RiskClass);
        Assert.True(critical.RequiresIndependentApproval);
        Assert.Contains("acl_entry_count_above_default", critical.Reasons);
    }

    [Fact]
    public void Access_evidence_includes_wildcard_principal_and_allow_implications()
    {
        var observed = new[]
        {
            Binding(
                "payments",
                "User:*",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Allow),
            Binding(
                "payments",
                "User:alice",
                KafkaAclOperation.AlterConfigs,
                KafkaAclPermissionType.Allow),
        };

        var describe = AclMutationPolicy.AnalyzeObservedAccess(
            new AclAccessQuery(
                KafkaAclResourceType.Topic,
                "payments",
                "User:alice",
                "10.0.0.10",
                KafkaAclOperation.Describe),
            observed);
        Assert.Equal(AclAccessEvidenceState.ObservedAllow, describe.EvidenceState);
        Assert.Single(describe.MatchingBindings);

        var describeConfigs = AclMutationPolicy.AnalyzeObservedAccess(
            new AclAccessQuery(
                KafkaAclResourceType.Topic,
                "payments",
                "User:alice",
                "10.0.0.10",
                KafkaAclOperation.DescribeConfigs),
            observed);
        Assert.Equal(
            AclAccessEvidenceState.ObservedAllow,
            describeConfigs.EvidenceState);
        Assert.Single(describeConfigs.MatchingBindings);
        Assert.False(describeConfigs.EffectiveAccessKnown);
    }

    [Fact]
    public void Common_preview_preserves_acl_specific_multi_binding_high_floor()
    {
        var bindings = new[]
        {
            Binding(
                "payments-orders",
                "User:alice",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Allow),
            Binding(
                "payments-audit",
                "User:alice",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Allow),
        };
        var plan = new AclMutationPlan(
            AclMutationMode.Create,
            "prod",
            bindings,
            Array.Empty<KafkaAclBinding>(),
            null,
            AclMutationPolicy.FingerprintBindings(
                Array.Empty<KafkaAclBinding>()));
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
            "w42-acl-multi-preview");

        Assert.Equal(MutationRiskClass.High, operation.Snapshot.Risk.RiskClass);
        Assert.False(operation.Snapshot.Risk.RequiresIndependentApproval);
        Assert.DoesNotContain(
            "multiple_targets",
            operation.Snapshot.Risk.Reasons);
    }

    [Fact]
    public void Access_evidence_models_inverse_deny_operation_implications()
    {
        var read = AclMutationPolicy.AnalyzeObservedAccess(
            new AclAccessQuery(
                KafkaAclResourceType.Topic,
                "payments",
                "User:alice",
                "10.0.0.10",
                KafkaAclOperation.Read),
            new[]
            {
                Binding(
                    "payments",
                    "User:alice",
                    KafkaAclOperation.Read,
                    KafkaAclPermissionType.Allow),
                Binding(
                    "payments",
                    "User:alice",
                    KafkaAclOperation.Describe,
                    KafkaAclPermissionType.Deny),
            });

        Assert.Equal(
            AclAccessEvidenceState.ConflictingEvidence,
            read.EvidenceState);

        var alterConfigs = AclMutationPolicy.AnalyzeObservedAccess(
            new AclAccessQuery(
                KafkaAclResourceType.Topic,
                "payments",
                "User:alice",
                "10.0.0.10",
                KafkaAclOperation.AlterConfigs),
            new[]
            {
                Binding(
                    "payments",
                    "User:alice",
                    KafkaAclOperation.AlterConfigs,
                    KafkaAclPermissionType.Allow),
                Binding(
                    "payments",
                    "User:alice",
                    KafkaAclOperation.DescribeConfigs,
                    KafkaAclPermissionType.Deny),
            });

        Assert.Equal(
            AclAccessEvidenceState.ConflictingEvidence,
            alterConfigs.EvidenceState);
        Assert.False(alterConfigs.EffectiveAccessKnown);
    }

    [Fact]
    public void Acl_request_budget_matches_admitted_25_default_and_100_hard_cap()
    {
        Assert.Equal(25, AclMutationPolicy.DefaultMaxBindings);
        Assert.Equal(100, AclMutationPolicy.HardMaxBindings);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new AclServerPolicy(
                Array.Empty<string>(),
                new[] { "User:alice" },
                new[] { KafkaAclResourceType.Topic },
                new[] { KafkaAclOperation.Read },
                false,
                false,
                false,
                maxBindingsPerMutation: 101));
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
            new[] { "User:alice" },
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
