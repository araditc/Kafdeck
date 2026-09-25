using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W42AclPreconditionTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 25, 7, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Create_preview_becomes_stale_when_exact_target_appears()
    {
        var binding = Binding("payments", "User:alice");
        var plannerObservation = new MutableObservation();
        var policy = Policy();
        var planner = new AclMutationPlanner(
            plannerObservation,
            policy,
            timeProvider: new FixedTimeProvider());

        var planned = await planner.PlanCreateAsync(
            "prod",
            new[] { binding });
        Assert.True(planned.IsSuccess);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Intent!,
            planned.Risk!,
            "v0.6-w42",
            Now.AddMinutes(5),
            Now,
            "w42-create-precondition");

        plannerObservation.Bindings = new[] { binding };
        var validator = new AclMutationPreconditionValidator(
            plannerObservation,
            policy,
            timeProvider: new FixedTimeProvider());
        var result = await validator.ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.StalePreview, result.Outcome);
        Assert.Equal("acl_precondition_binding_set_changed", result.ResultCode);
    }

    [Fact]
    public async Task Remove_preview_becomes_stale_when_filter_inventory_changes()
    {
        var binding = Binding("payments", "User:alice");
        var observation = new MutableObservation
        {
            Bindings = new[] { binding },
        };
        var policy = Policy();
        var planner = new AclMutationPlanner(
            observation,
            policy,
            timeProvider: new FixedTimeProvider());

        var planned = await planner.PlanRemoveAsync(
            "prod",
            new KafkaAclBindingFilter(
                KafkaAclResourceType.Topic,
                "payments",
                KafkaAclFilterPatternMode.Literal,
                "User:alice"));
        Assert.True(planned.IsSuccess);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Intent!,
            planned.Risk!,
            "v0.6-w42",
            Now.AddMinutes(5),
            Now,
            "w42-remove-precondition");

        observation.Bindings = new[]
        {
            binding,
            binding with { Operation = KafkaAclOperation.Write },
        };

        var validator = new AclMutationPreconditionValidator(
            observation,
            policy,
            timeProvider: new FixedTimeProvider());
        var result = await validator.ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.StalePreview, result.Outcome);
        Assert.Equal("acl_precondition_binding_set_changed", result.ResultCode);
    }

    [Fact]
    public async Task Replace_preview_with_multiple_exact_bindings_rebuilds_the_same_immutable_resources()
    {
        var current = Binding("payments", "User:alice");
        var desired = current with { Operation = KafkaAclOperation.Write };
        var observation = new MutableObservation
        {
            Bindings = new[] { current },
        };
        var policy = Policy();
        var planner = new AclMutationPlanner(
            observation,
            policy,
            timeProvider: new FixedTimeProvider());

        var planned = await planner.PlanReplaceAsync(
            "prod",
            new KafkaAclBindingFilter(
                KafkaAclResourceType.Topic,
                "payments",
                KafkaAclFilterPatternMode.Literal,
                "User:alice",
                "*"),
            new[] { desired });
        Assert.True(planned.IsSuccess);
        Assert.Single(planned.Plan!.CreateBindings);
        Assert.Single(planned.Plan.RemoveBindings);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Intent!,
            planned.Risk!,
            "v0.6-w42",
            Now.AddMinutes(5),
            Now,
            "w42-replace-precondition");

        var validator = new AclMutationPreconditionValidator(
            observation,
            policy,
            timeProvider: new FixedTimeProvider());
        var result = await validator.ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.Allowed, result.Outcome);
    }

    [Fact]
    public async Task Replace_preview_becomes_stale_when_current_combined_delta_limit_is_lower()
    {
        var current = Binding("payments", "User:alice");
        var desired = current with { Operation = KafkaAclOperation.Write };
        var observation = new MutableObservation
        {
            Bindings = new[] { current },
        };
        var planningPolicy = PolicyWithLimit(2);
        var planner = new AclMutationPlanner(
            observation,
            planningPolicy,
            timeProvider: new FixedTimeProvider());

        var planned = await planner.PlanReplaceAsync(
            "prod",
            new KafkaAclBindingFilter(
                KafkaAclResourceType.Topic,
                "payments",
                KafkaAclFilterPatternMode.Literal,
                "User:alice",
                "*"),
            new[] { desired });
        Assert.True(planned.IsSuccess);
        Assert.Single(planned.Plan!.CreateBindings);
        Assert.Single(planned.Plan.RemoveBindings);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Intent!,
            planned.Risk!,
            "v0.6-w42",
            Now.AddMinutes(5),
            Now,
            "w42-replace-policy-limit");

        var validator = new AclMutationPreconditionValidator(
            observation,
            PolicyWithLimit(1),
            timeProvider: new FixedTimeProvider());
        var result = await validator.ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.StalePreview, result.Outcome);
        Assert.Equal("acl_precondition_policy_changed", result.ResultCode);
    }

    [Fact]
    public async Task Dispatch_revalidates_server_grant_policy()
    {
        var binding = Binding("payments", "User:alice");
        var planningPolicy = Policy("User:alice");
        var observation = new MutableObservation();
        var planner = new AclMutationPlanner(
            observation,
            planningPolicy,
            timeProvider: new FixedTimeProvider());
        var planned = await planner.PlanCreateAsync("prod", new[] { binding });
        Assert.True(planned.IsSuccess);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Intent!,
            planned.Risk!,
            "v0.6-w42",
            Now.AddMinutes(5),
            Now,
            "w42-policy-precondition");

        var changedPolicy = Policy("User:bob");
        var validator = new AclMutationPreconditionValidator(
            observation,
            changedPolicy,
            timeProvider: new FixedTimeProvider());
        var result = await validator.ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.StalePreview, result.Outcome);
        Assert.Equal("acl_precondition_policy_changed", result.ResultCode);
    }

    [Fact]
    public async Task Pre_dispatch_create_reuses_one_bounded_deadline_for_all_exact_targets()
    {
        var bindings = new[]
        {
            Binding("payments.orders", "User:alice"),
            Binding("payments.audit", "User:alice"),
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
            "w42-create-deadline");

        var observation = new DeadlineCapturingObservation();
        var validator = new AclMutationPreconditionValidator(
            observation,
            Policy(),
            new AclMutationPlannerPolicy(TimeSpan.FromSeconds(10)),
            new AdvancingTimeProvider());

        var result = await validator.ValidateAsync(operation.Snapshot);

        Assert.Equal(MutationPreDispatchGuardOutcome.Allowed, result.Outcome);
        Assert.Equal(2, observation.Remaining.Count);
        Assert.Equal(observation.Remaining[0], observation.Remaining[1]);
    }

    [Fact]
    public void Pinned_provider_maps_cluster_via_broker_but_does_not_invent_transactional_id()
    {
        var clusterFilter =
            Kafdeck.Infrastructure.Kafka.ConfluentKafkaAclMapper.ToProviderFilter(
                new KafkaAclBindingFilter(
                    KafkaAclResourceType.Cluster,
                    "kafka-cluster",
                    KafkaAclFilterPatternMode.Literal,
                    "User:alice"));

        Assert.Equal(
            Confluent.Kafka.Admin.ResourceType.Broker,
            clusterFilter.PatternFilter.Type);

        var providerBinding = new Confluent.Kafka.Admin.AclBinding
        {
            Pattern = new Confluent.Kafka.Admin.ResourcePattern
            {
                Type = Confluent.Kafka.Admin.ResourceType.Broker,
                Name = "kafka-cluster",
                ResourcePatternType =
                    Confluent.Kafka.Admin.ResourcePatternType.Literal,
            },
            Entry = new Confluent.Kafka.Admin.AccessControlEntry
            {
                Principal = "User:alice",
                Host = "*",
                Operation = Confluent.Kafka.Admin.AclOperation.Alter,
                PermissionType =
                    Confluent.Kafka.Admin.AclPermissionType.Allow,
            },
        };

        Assert.True(
            Kafdeck.Infrastructure.Kafka.ConfluentKafkaAclMapper.TryFromProvider(
                providerBinding,
                out var mapped));
        Assert.NotNull(mapped);
        Assert.Equal(KafkaAclResourceType.Cluster, mapped!.ResourceType);

        Assert.DoesNotContain(
            "TransactionalId",
            Enum.GetNames<Confluent.Kafka.Admin.ResourceType>());

        Assert.Throws<NotSupportedException>(() =>
            Kafdeck.Infrastructure.Kafka.ConfluentKafkaAclMapper.ToProviderFilter(
                new KafkaAclBindingFilter(
                    KafkaAclResourceType.TransactionalId,
                    "tx-1",
                    KafkaAclFilterPatternMode.Literal,
                    "User:alice")));
    }

    private static KafkaAclBinding Binding(string resource, string principal) =>
        new(
            KafkaAclResourceType.Topic,
            resource,
            KafkaAclPatternType.Literal,
            principal,
            "*",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);

    private static AclServerPolicy PolicyWithLimit(int maxBindingsPerMutation) =>
        new(
            Array.Empty<string>(),
            new[] { "User:alice" },
            new[]
            {
                KafkaAclResourceType.Topic,
                KafkaAclResourceType.Group,
            },
            Enum.GetValues<KafkaAclOperation>(),
            allowPrefixedGrants: true,
            allowWildcardResourceGrants: false,
            allowAllOperationGrants: false,
            maxBindingsPerMutation: maxBindingsPerMutation);

    private static AclServerPolicy Policy(
        params string[] allowedPrincipals) =>
        new(
            Array.Empty<string>(),
            allowedPrincipals.Length == 0
                ? new[] { "User:alice" }
                : allowedPrincipals,
            new[]
            {
                KafkaAclResourceType.Topic,
                KafkaAclResourceType.Group,
            },
            Enum.GetValues<KafkaAclOperation>(),
            allowPrefixedGrants: true,
            allowWildcardResourceGrants: false,
            allowAllOperationGrants: false,
            maxBindingsPerMutation: 25);

    private sealed class MutableObservation : IAclObservationPort
    {
        public IReadOnlyList<KafkaAclBinding> Bindings { get; set; } =
            Array.Empty<KafkaAclBinding>();

        public Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
            string clusterId,
            KafkaAclBindingFilter filter,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            var matches = Bindings
                .Where(binding => AclMutationPolicy.MatchesFilter(binding, filter))
                .ToArray();
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                    Array.AsReadOnly(matches),
                    Observation()));
        }
    }

    private sealed class DeadlineCapturingObservation : IAclObservationPort
    {
        public List<TimeSpan> Remaining { get; } = new();

        public Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
            string clusterId,
            KafkaAclBindingFilter filter,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            Remaining.Add(operation.Remaining(Now));
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                    Array.Empty<KafkaAclBinding>(),
                    Observation()));
        }
    }

    private sealed class AdvancingTimeProvider : TimeProvider
    {
        private int _callCount;

        public override DateTimeOffset GetUtcNow() =>
            Now.AddSeconds(_callCount++);
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static ObservationMetadata Observation() =>
        new(Now, Now, Now, ObservationSource.Live);
}
