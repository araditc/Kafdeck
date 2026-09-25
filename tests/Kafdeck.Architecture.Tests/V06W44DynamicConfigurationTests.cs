using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W44DynamicConfigurationTests
{
    [Fact]
    public void Registry_is_closed_versioned_and_rejects_escape_keys()
    {
        var registry = new DynamicConfigurationRegistry();

        foreach (var version in
                 Enum.GetValues<KafkaCompatibilityLine>())
        {
            Assert.True(
                registry.IsAdmitted(
                    version,
                    "log.cleaner.threads"));
            Assert.True(
                registry.IsAdmitted(
                    version,
                    "message.max.bytes"));

            Assert.False(
                registry.IsAdmitted(
                    version,
                    "leader.replication.throttled.rate"));
            Assert.False(
                registry.IsAdmitted(
                    version,
                    "follower.replication.throttled.rate"));
            Assert.False(
                registry.IsAdmitted(
                    version,
                    "listener.name.internal.scram-sha-512.sasl.jaas.config"));
            Assert.False(
                registry.IsAdmitted(
                    version,
                    "process.roles"));
            Assert.False(
                registry.IsAdmitted(
                    version,
                    "unknown.future.key"));
        }
    }

    [Fact]
    public async Task Set_plan_binds_exact_read_and_alter_target_and_high_floor()
    {
        var target = new DynamicConfigurationTarget(
            "prod",
            DynamicConfigurationScope.BrokerOverride,
            4,
            "log.cleaner.threads");
        var provider = new StubProvider(
            Observation(
                target,
                "1",
                "DefaultConfig",
                synonyms:
                [
                    new("DefaultConfig", "1"),
                ]));
        var planner = new DynamicConfigurationPlanner(
            provider,
            new DynamicConfigurationRegistry());

        var result = await planner.PlanSetAsync(
            KafkaCompatibilityLine.Kafka431,
            target,
            "2");

        Assert.True(result.IsSuccess);
        Assert.Equal(
            MutationOperationKind.ClusterConfigAlter,
            result.Intent!.Kind);
        Assert.Equal(
            MutationRiskClass.High,
            result.Risk!.RiskClass);
        Assert.False(result.Risk.RequiresIndependentApproval);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            result.Risk.ConfirmationMode);

        var requirements =
            MutationAuthorizationRequirements.Normalize(
                result.Intent.Kind,
                result.Intent.ClusterId,
                result.Intent.AuthorizationTargets,
                result.Intent.ResourceKeys);
        Assert.Equal(2, requirements.Count);
        Assert.Contains(
            requirements,
            item =>
                item.Action ==
                AuthorizationAction.ClusterConfigRead);
        Assert.Contains(
            requirements,
            item =>
                item.Action ==
                AuthorizationAction.ClusterConfigAlter);
        Assert.All(
            requirements,
            item => Assert.Equal(
                result.Intent.ResourceKeys.Single(),
                item.ResourceName));

        var plan =
            DynamicConfigurationPolicy.DeserializePlan(
                result.Intent.CanonicalIntent);
        Assert.Equal("2", plan.Mutation.Value);
        Assert.Equal(
            DynamicConfigurationScope.BrokerOverride,
            plan.Mutation.Target.Scope);
        Assert.Equal(4, plan.Mutation.Target.BrokerId);
        Assert.Single(result.Intent.Preconditions!);
        Assert.Equal(
            "cluster.config",
            result.Intent.Preconditions![0].Key);
    }

    [Fact]
    public async Task Sensitive_operational_key_escalates_to_critical()
    {
        var target = new DynamicConfigurationTarget(
            "prod",
            DynamicConfigurationScope.ClusterDefault,
            null,
            "max.connections");
        var provider = new StubProvider(
            Observation(
                target,
                "2147483647",
                "DefaultConfig",
                synonyms:
                [
                    new("DefaultConfig", "2147483647"),
                ]));
        var planner = new DynamicConfigurationPlanner(
            provider,
            new DynamicConfigurationRegistry());

        var result = await planner.PlanSetAsync(
            KafkaCompatibilityLine.Kafka431,
            target,
            "20000");

        Assert.True(result.IsSuccess);
        Assert.Equal(
            MutationRiskClass.Critical,
            result.Risk!.RiskClass);
        Assert.True(result.Risk.RequiresIndependentApproval);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            result.Risk.ConfirmationMode);
    }

    [Fact]
    public async Task Reset_binds_observed_inherited_value_and_source()
    {
        var target = new DynamicConfigurationTarget(
            "prod",
            DynamicConfigurationScope.BrokerOverride,
            2,
            "log.cleaner.backoff.ms");
        var provider = new StubProvider(
            Observation(
                target,
                "25000",
                "DynamicBrokerConfig",
                synonyms:
                [
                    new("DynamicBrokerConfig", "25000"),
                    new("DynamicDefaultBrokerConfig", "20000"),
                    new("DefaultConfig", "15000"),
                ]));
        var planner = new DynamicConfigurationPlanner(
            provider,
            new DynamicConfigurationRegistry());

        var result = await planner.PlanResetAsync(
            KafkaCompatibilityLine.Kafka431,
            target);

        Assert.True(result.IsSuccess);
        Assert.Null(result.Plan!.Mutation.Value);
        Assert.Equal("20000", result.Plan.InheritedValue);
        Assert.Equal(
            "DynamicDefaultBrokerConfig",
            result.Plan.InheritedSource);
    }

    [Fact]
    public async Task Reset_fails_closed_without_exact_dynamic_override()
    {
        var target = new DynamicConfigurationTarget(
            "prod",
            DynamicConfigurationScope.BrokerOverride,
            2,
            "log.cleaner.backoff.ms");
        var provider = new StubProvider(
            Observation(
                target,
                "15000",
                "DefaultConfig",
                synonyms:
                [
                    new("DefaultConfig", "15000"),
                ]));
        var planner = new DynamicConfigurationPlanner(
            provider,
            new DynamicConfigurationRegistry());

        var result = await planner.PlanResetAsync(
            KafkaCompatibilityLine.Kafka431,
            target);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            DynamicConfigurationPlanningFailureCode.ResetSourceUnavailable,
            result.Failure!.Code);
    }

    [Fact]
    public async Task Sensitive_or_read_only_provider_evidence_is_not_mutable()
    {
        var sensitiveTarget = new DynamicConfigurationTarget(
            "prod",
            DynamicConfigurationScope.ClusterDefault,
            null,
            "log.cleaner.backoff.ms");
        var sensitive = new StubProvider(
            Observation(
                sensitiveTarget,
                null,
                "DefaultConfig",
                isSensitive: true));
        var sensitivePlanner =
            new DynamicConfigurationPlanner(
                sensitive,
                new DynamicConfigurationRegistry());

        var sensitiveResult =
            await sensitivePlanner.PlanSetAsync(
                KafkaCompatibilityLine.Kafka431,
                sensitiveTarget,
                "1000");
        Assert.False(sensitiveResult.IsSuccess);
        Assert.Equal(
            DynamicConfigurationPlanningFailureCode.SensitiveValue,
            sensitiveResult.Failure!.Code);

        var readOnly = new StubProvider(
            Observation(
                sensitiveTarget,
                "15000",
                "DefaultConfig",
                isReadOnly: true));
        var readOnlyPlanner =
            new DynamicConfigurationPlanner(
                readOnly,
                new DynamicConfigurationRegistry());

        var readOnlyResult =
            await readOnlyPlanner.PlanSetAsync(
                KafkaCompatibilityLine.Kafka431,
                sensitiveTarget,
                "1000");
        Assert.False(readOnlyResult.IsSuccess);
        Assert.Equal(
            DynamicConfigurationPlanningFailureCode.ReadOnlyValue,
            readOnlyResult.Failure!.Code);
    }

    [Fact]
    public async Task Precondition_revalidation_detects_out_of_band_drift()
    {
        var target = new DynamicConfigurationTarget(
            "prod",
            DynamicConfigurationScope.ClusterDefault,
            null,
            "log.cleaner.backoff.ms");
        var initial = Observation(
            target,
            "15000",
            "DefaultConfig",
            synonyms:
            [
                new("DefaultConfig", "15000"),
            ]);
        var provider = new StubProvider(initial);
        var registry = new DynamicConfigurationRegistry();
        var planner =
            new DynamicConfigurationPlanner(provider, registry);

        var planned = await planner.PlanSetAsync(
            KafkaCompatibilityLine.Kafka431,
            target,
            "20000");
        Assert.True(planned.IsSuccess);

        var now = DateTimeOffset.UtcNow;
        var operation = MutationOperation.CreatePreview(
                Guid.NewGuid(),
                "principal:owner",
                planned.Intent!,
                planned.Risk!,
                "policy-v1",
                now.AddMinutes(5),
                now,
                $"w44-{Guid.NewGuid():N}")
            .Snapshot;

        var validator =
            new DynamicConfigurationPreconditionValidator(
                provider,
                registry);
        var allowed =
            await validator.ValidateAsync(operation);
        Assert.Equal(
            MutationPreDispatchGuardOutcome.Allowed,
            allowed.Outcome);

        provider.Current = Observation(
            target,
            "16000",
            "DynamicDefaultBrokerConfig",
            synonyms:
            [
                new("DynamicDefaultBrokerConfig", "16000"),
                new("DefaultConfig", "15000"),
            ]);

        var stale =
            await validator.ValidateAsync(operation);
        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            stale.Outcome);
    }

    [Fact]
    public void Quota_provider_gap_remains_explicit_and_fail_closed()
    {
        IQuotaAdministrationPort quota =
            new PinnedClientUnavailableQuotaAdministrationPort();

        Assert.False(
            quota.IsTypedProviderCapabilityAvailable);
        Assert.Equal(
            MutationRiskClass.High,
            MutationRiskClassifier.Classify(
                    new MutationRiskInput(
                        MutationOperationKind.QuotaAlter))
                .RiskClass);
    }

    private static DynamicConfigurationObservation Observation(
        DynamicConfigurationTarget target,
        string? value,
        string source,
        bool isSensitive = false,
        bool isReadOnly = false,
        IReadOnlyList<DynamicConfigurationSynonym>? synonyms = null) =>
        new(
            target,
            value,
            source,
            isSensitive,
            isReadOnly,
            synonyms ??
            Array.Empty<DynamicConfigurationSynonym>());

    private sealed class StubProvider :
        IDynamicConfigurationPort
    {
        public StubProvider(
            DynamicConfigurationObservation current)
        {
            Current = current;
        }

        public DynamicConfigurationObservation Current
        {
            get;
            set;
        }

        public Task<KafkaResult<DynamicConfigurationObservation>>
            DescribeAsync(
                DynamicConfigurationTarget target,
                KafkaOperationContext operation,
                CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<DynamicConfigurationObservation>.Success(
                    Current,
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }

        public Task<MutationProviderResult> AlterAsync(
            DynamicConfigurationMutation mutation,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.FailedDefinitive,
                    "not_dispatched_by_planning_test"));
    }
}
