using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W44QuotaDynamicConfigContractTests
{
    private const string Fingerprint =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    private static readonly DateTimeOffset Now =
        new(2026, 9, 25, 15, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Quota_entity_identity_distinguishes_default_named_and_missing_dimensions()
    {
        var defaultUser = new KafkaQuotaEntity(
            new[]
            {
                new KafkaQuotaEntityComponent(
                    KafkaQuotaEntityDimension.User,
                    null),
            });
        var namedUser = new KafkaQuotaEntity(
            new[]
            {
                new KafkaQuotaEntityComponent(
                    KafkaQuotaEntityDimension.User,
                    "alice"),
            });
        var userAndDefaultClient = new KafkaQuotaEntity(
            new[]
            {
                new KafkaQuotaEntityComponent(
                    KafkaQuotaEntityDimension.User,
                    "alice"),
                new KafkaQuotaEntityComponent(
                    KafkaQuotaEntityDimension.ClientId,
                    null),
            });

        Assert.NotEqual(
            QuotaMutationPolicy.EntityIdentityHash(defaultUser),
            QuotaMutationPolicy.EntityIdentityHash(namedUser));
        Assert.NotEqual(
            QuotaMutationPolicy.EntityIdentityHash(namedUser),
            QuotaMutationPolicy.EntityIdentityHash(userAndDefaultClient));
    }

    [Fact]
    public void Quota_plan_preserves_reset_and_requires_exact_read_write_conjunction()
    {
        var plan = QuotaMutationPolicy.CreatePlan(
            "prod",
            new KafkaQuotaEntity(
                new[]
                {
                    new KafkaQuotaEntityComponent(
                        KafkaQuotaEntityDimension.User,
                        "alice"),
                }),
            new[]
            {
                new KafkaQuotaChange(
                    KafkaQuotaMetric.ProducerByteRate,
                    null),
            },
            Fingerprint);

        Assert.Null(plan.Changes.Single().Value);

        var intent = QuotaMutationPolicy.BuildIntent(plan);
        var risk = QuotaMutationPolicy.ClassifyRisk(plan);
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            intent,
            risk,
            "v0.6-w44",
            Now.AddMinutes(5),
            Now,
            "quota-reset");

        Assert.Equal(
            MutationOperationKind.QuotaAlter,
            operation.Snapshot.OperationKind);
        Assert.Equal(
            MutationRiskClass.High,
            operation.Snapshot.Risk.RiskClass);
        Assert.False(operation.Snapshot.Risk.RequiresIndependentApproval);
        Assert.Equal(
            new[]
            {
                AuthorizationAction.QuotaRead,
                AuthorizationAction.QuotaAlter,
            },
            operation.Snapshot.AuthorizationTargets
                .Select(target => target.Action)
                .OrderBy(action => action)
                .ToArray());
    }

    [Theory]
    [InlineData("ssl.keystore.password")]
    [InlineData("sasl.enabled.mechanisms")]
    [InlineData("leader.replication.throttled.rate")]
    [InlineData("follower.replication.throttled.rate")]
    [InlineData("log.dirs")]
    [InlineData("process.roles")]
    public void Ordinary_dynamic_config_registry_rejects_escape_keys(string key)
    {
        Assert.Throws<ArgumentException>(() =>
            new DynamicConfigRegistry(
                new[]
                {
                    new DynamicConfigDefinition(
                        key,
                        DynamicConfigUpdateMode.Both,
                        DynamicConfigValueKind.String,
                        DynamicConfigSafetyClass.Ordinary,
                        "fixture-evidence",
                        new[] { "fixture-version" }),
                }));
    }

    [Fact]
    public void Dynamic_config_plan_binds_registry_evidence_target_and_current_state()
    {
        var registry = Registry(DynamicConfigSafetyClass.Ordinary);
        var plan = DynamicConfigMutationPolicy.CreatePlan(
            new DynamicConfigTarget(
                "prod",
                DynamicConfigTargetKind.Broker,
                "1"),
            new[]
            {
                new DynamicConfigChange(
                    "test.dynamic.integer",
                    DynamicConfigMutationMode.Set,
                    "64"),
            },
            Fingerprint,
            registry);

        Assert.Equal(
            "fixture-evidence-v1",
            plan.Changes.Single().ProviderEvidenceId);
        Assert.Equal(
            new[] { "fixture-1", "fixture-2" },
            plan.Changes.Single().SupportedBrokerVersions);

        var intent = DynamicConfigMutationPolicy.BuildIntent(plan, registry);
        var risk = DynamicConfigMutationPolicy.ClassifyRisk(plan, registry);
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            intent,
            risk,
            "v0.6-w44",
            Now.AddMinutes(5),
            Now,
            "config-set");

        Assert.Equal(
            MutationOperationKind.ClusterConfigAlter,
            operation.Snapshot.OperationKind);
        Assert.Equal(
            MutationRiskClass.High,
            operation.Snapshot.Risk.RiskClass);
        Assert.Contains(
            operation.Snapshot.AuthorizationTargets,
            target => target.Action == AuthorizationAction.ClusterConfigRead);
        Assert.Contains(
            operation.Snapshot.AuthorizationTargets,
            target => target.Action == AuthorizationAction.ClusterConfigAlter);
        Assert.Single(operation.Snapshot.Preconditions);
        Assert.Equal(
            Fingerprint,
            operation.Snapshot.Preconditions[0].Fingerprint);
    }

    [Fact]
    public void Critical_dynamic_config_definition_requires_independent_approval()
    {
        var registry = Registry(DynamicConfigSafetyClass.Critical);
        var plan = DynamicConfigMutationPolicy.CreatePlan(
            new DynamicConfigTarget(
                "prod",
                DynamicConfigTargetKind.ClusterDefault),
            new[]
            {
                new DynamicConfigChange(
                    "test.dynamic.integer",
                    DynamicConfigMutationMode.Set,
                    "32"),
            },
            Fingerprint,
            registry);

        var risk = DynamicConfigMutationPolicy.ClassifyRisk(plan, registry);

        Assert.Equal(MutationRiskClass.Critical, risk.RiskClass);
        Assert.True(risk.RequiresIndependentApproval);
        Assert.Contains("dynamic_config_sensitive_key", risk.Reasons);
    }

    [Fact]
    public void Dynamic_config_registry_rejects_value_shape_and_scope_mismatch()
    {
        var registry = Registry(
            DynamicConfigSafetyClass.Ordinary,
            DynamicConfigUpdateMode.PerBroker);

        Assert.Throws<MutationStateException>(() =>
            DynamicConfigMutationPolicy.CreatePlan(
                new DynamicConfigTarget(
                    "prod",
                    DynamicConfigTargetKind.ClusterDefault),
                new[]
                {
                    new DynamicConfigChange(
                        "test.dynamic.integer",
                        DynamicConfigMutationMode.Set,
                        "64"),
                },
                Fingerprint,
                registry));

        Assert.Throws<ArgumentException>(() =>
            DynamicConfigMutationPolicy.CreatePlan(
                new DynamicConfigTarget(
                    "prod",
                    DynamicConfigTargetKind.Broker,
                    "1"),
                new[]
                {
                    new DynamicConfigChange(
                        "test.dynamic.integer",
                        DynamicConfigMutationMode.Set,
                        "not-an-integer"),
                },
                Fingerprint,
                registry));

        Assert.Throws<ArgumentException>(() =>
            DynamicConfigMutationPolicy.CreatePlan(
                new DynamicConfigTarget(
                    "prod",
                    DynamicConfigTargetKind.Broker,
                    "1"),
                new[]
                {
                    new DynamicConfigChange(
                        "test.dynamic.integer",
                        DynamicConfigMutationMode.Reset,
                        "64"),
                },
                Fingerprint,
                registry));
    }

    [Fact]
    public void W44_common_kernel_floors_are_high_and_multi_target_escalates()
    {
        var quota = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.QuotaAlter));
        var config = MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.ClusterConfigAlter));
        var multiConfig = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                MutationOperationKind.ClusterConfigAlter,
                TargetCount: 2));

        Assert.Equal(MutationRiskClass.High, quota.RiskClass);
        Assert.Equal(MutationRiskClass.High, config.RiskClass);
        Assert.Equal(MutationRiskClass.Critical, multiConfig.RiskClass);
        Assert.True(multiConfig.RequiresIndependentApproval);
    }

    private static DynamicConfigRegistry Registry(
        DynamicConfigSafetyClass safetyClass,
        DynamicConfigUpdateMode updateMode = DynamicConfigUpdateMode.Both) =>
        new(
            new[]
            {
                new DynamicConfigDefinition(
                    "test.dynamic.integer",
                    updateMode,
                    DynamicConfigValueKind.Int32,
                    safetyClass,
                    "fixture-evidence-v1",
                    new[] { "fixture-2", "fixture-1" }),
            });
}
