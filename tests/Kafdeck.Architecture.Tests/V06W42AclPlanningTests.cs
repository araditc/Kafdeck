using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W42AclPlanningTests
{
    [Fact]
    public async Task Remove_expands_provider_filter_to_exact_bindings_before_preview()
    {
        var existing = new[]
        {
            Binding("payments.orders", KafkaAclOperation.Read, KafkaAclPermissionType.Allow),
            Binding("payments.orders", KafkaAclOperation.Write, KafkaAclPermissionType.Allow),
        };
        var observation = new StubAclObservationPort(existing);
        var planner = new AclMutationPlanner(
            observation,
            Policy(),
            timeProvider: new FixedTimeProvider());

        var result = await planner.PlanRemoveAsync(
            "prod",
            new KafkaAclBindingFilter(
                KafkaAclResourceType.Topic,
                "payments.orders",
                KafkaAclFilterPatternMode.Literal,
                "User:alice"));

        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Plan);
        Assert.Equal(2, result.Plan!.RemoveBindings.Count);
        Assert.All(
            result.Plan.RemoveBindings,
            binding => Assert.Equal("payments.orders", binding.ResourceName));
        Assert.NotNull(result.Plan.SourceFilter);
        Assert.Equal(
            AclMutationPolicy.FingerprintBindings(existing),
            result.Plan.ObservedBindingSetFingerprint);
        Assert.NotNull(result.Intent);
        Assert.Equal(
            result.Plan.RemoveBindings.Count,
            result.Intent!.ResourceKeys.Count);
    }

    [Fact]
    public async Task Replace_rejects_desired_binding_outside_source_filter()
    {
        var observation = new StubAclObservationPort(
            new[]
            {
                Binding("payments.orders", KafkaAclOperation.Read, KafkaAclPermissionType.Allow),
            });
        var planner = new AclMutationPlanner(
            observation,
            Policy(),
            timeProvider: new FixedTimeProvider());

        var result = await planner.PlanReplaceAsync(
            "prod",
            new KafkaAclBindingFilter(
                KafkaAclResourceType.Topic,
                "payments.orders",
                KafkaAclFilterPatternMode.Literal,
                "User:alice"),
            new[]
            {
                Binding("audit.events", KafkaAclOperation.Read, KafkaAclPermissionType.Allow),
            });

        Assert.False(result.IsSuccess);
        Assert.Equal(
            AclMutationPlanningFailureCode.InvalidInput,
            result.Failure!.Code);
    }

    [Fact]
    public async Task Replace_enforces_binding_ceiling_across_combined_delta()
    {
        var observation = new StubAclObservationPort(
            new[]
            {
                Binding("payments.orders", KafkaAclOperation.Read, KafkaAclPermissionType.Allow),
                Binding("payments.orders", KafkaAclOperation.Write, KafkaAclPermissionType.Allow),
                Binding("payments.orders", KafkaAclOperation.Create, KafkaAclPermissionType.Allow),
            });
        var planner = new AclMutationPlanner(
            observation,
            Policy(maxBindingsPerMutation: 4),
            timeProvider: new FixedTimeProvider());

        var result = await planner.PlanReplaceAsync(
            "prod",
            new KafkaAclBindingFilter(
                KafkaAclResourceType.Topic,
                "payments.orders",
                KafkaAclFilterPatternMode.Literal,
                "User:alice"),
            new[]
            {
                Binding("payments.orders", KafkaAclOperation.Delete, KafkaAclPermissionType.Allow),
                Binding("payments.orders", KafkaAclOperation.Alter, KafkaAclPermissionType.Allow),
                Binding("payments.orders", KafkaAclOperation.Describe, KafkaAclPermissionType.Allow),
            });

        Assert.False(result.IsSuccess);
        Assert.Equal(
            AclMutationPlanningFailureCode.TooManyBindings,
            result.Failure!.Code);
    }

    [Fact]
    public async Task Replace_allows_empty_desired_set_as_remove_all()
    {
        var existing = new[]
        {
            Binding(
                "payments.orders",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Allow),
            Binding(
                "payments.orders",
                KafkaAclOperation.Write,
                KafkaAclPermissionType.Allow),
        };
        var observation = new StubAclObservationPort(existing);
        var planner = new AclMutationPlanner(
            observation,
            Policy(),
            timeProvider: new FixedTimeProvider());

        var result = await planner.PlanReplaceAsync(
            "prod",
            new KafkaAclBindingFilter(
                KafkaAclResourceType.Topic,
                "payments.orders",
                KafkaAclFilterPatternMode.Literal,
                "User:alice"),
            Array.Empty<KafkaAclBinding>());

        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Plan);
        Assert.Equal(AclMutationMode.Replace, result.Plan!.Mode);
        Assert.Empty(result.Plan.CreateBindings);
        Assert.Equal(2, result.Plan.RemoveBindings.Count);
        Assert.Equal(
            AclMutationPolicy.FingerprintBindings(existing),
            result.Plan.ObservedBindingSetFingerprint);
    }

    [Fact]
    public async Task Create_observes_each_exact_target_and_omits_existing_binding()
    {
        var existing = Binding(
            "payments.orders",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);
        var missing = Binding(
            "payments.orders",
            KafkaAclOperation.Write,
            KafkaAclPermissionType.Allow);
        var observation = new SelectiveAclObservationPort(existing);
        var planner = new AclMutationPlanner(
            observation,
            Policy(),
            timeProvider: new FixedTimeProvider());

        var result = await planner.PlanCreateAsync(
            "prod",
            new[] { existing, missing });

        Assert.True(result.IsSuccess);
        Assert.Single(result.Plan!.CreateBindings);
        Assert.Equal(missing, result.Plan.CreateBindings[0]);
        Assert.Equal(2, observation.Filters.Count);
        Assert.All(
            observation.Filters,
            filter =>
            {
                Assert.NotNull(filter.ResourceType);
                Assert.NotNull(filter.ResourceName);
                Assert.NotNull(filter.Principal);
                Assert.NotNull(filter.Host);
                Assert.NotNull(filter.Operation);
                Assert.NotNull(filter.PermissionType);
            });
    }

    [Fact]
    public async Task Access_analysis_uses_provider_observation_but_never_claims_effective_access()
    {
        var observation = new StubAclObservationPort(
            new[]
            {
                Binding(
                    "payments.",
                    KafkaAclOperation.Read,
                    KafkaAclPermissionType.Allow,
                    KafkaAclPatternType.Prefixed),
            });
        var service = new AclAccessAnalysisService(
            observation,
            timeProvider: new FixedTimeProvider());

        var result = await service.AnalyzeAsync(
            "prod",
            new AclAccessQuery(
                KafkaAclResourceType.Topic,
                "payments.orders",
                "User:alice",
                "10.0.0.10",
                KafkaAclOperation.Read));

        Assert.True(result.IsSuccess);
        Assert.Equal(
            AclAccessEvidenceState.ObservedAllow,
            result.Value!.EvidenceState);
        Assert.False(result.Value.EffectiveAccessKnown);
        Assert.Single(result.Value.MatchingBindings);
    }

    [Fact]
    public async Task Access_service_observes_exact_and_wildcard_principals_separately()
    {
        var exact = Binding(
            "payments.orders",
            KafkaAclOperation.Read,
            KafkaAclPermissionType.Allow);
        var wildcard = exact with { Principal = "User:*" };
        var observation = new SelectivePrincipalObservationPort(exact, wildcard);
        var service = new AclAccessAnalysisService(
            observation,
            timeProvider: new FixedTimeProvider());

        var result = await service.AnalyzeAsync(
            "prod",
            new AclAccessQuery(
                KafkaAclResourceType.Topic,
                "payments.orders",
                "User:alice",
                "10.0.0.10",
                KafkaAclOperation.Describe));

        Assert.True(result.IsSuccess);
        Assert.Equal(2, observation.Principals.Count);
        Assert.Contains("User:alice", observation.Principals);
        Assert.Contains("User:*", observation.Principals);
        Assert.All(observation.Operations, operation => Assert.Null(operation));
        Assert.Equal(AclAccessEvidenceState.ObservedAllow, result.Value!.EvidenceState);
    }

    [Fact]
    public async Task Create_reuses_one_bounded_deadline_for_all_exact_targets()
    {
        var observation = new DeadlineCapturingObservationPort();
        var planner = new AclMutationPlanner(
            observation,
            Policy(),
            new AclMutationPlannerPolicy(TimeSpan.FromSeconds(10)),
            new AdvancingTimeProvider());

        var result = await planner.PlanCreateAsync(
            "prod",
            new[]
            {
                Binding(
                    "payments.orders",
                    KafkaAclOperation.Read,
                    KafkaAclPermissionType.Allow),
                Binding(
                    "payments.audit",
                    KafkaAclOperation.Write,
                    KafkaAclPermissionType.Allow),
            });

        Assert.True(result.IsSuccess);
        Assert.Equal(2, observation.Remaining.Count);
        Assert.Equal(observation.Remaining[0], observation.Remaining[1]);
    }

    [Fact]
    public void Provider_mapper_uses_typed_acl_filters_and_blocks_unproven_transactional_id()
    {
        var filter = ConfluentKafkaAclMapper.ToProviderFilter(
            new KafkaAclBindingFilter(
                KafkaAclResourceType.Topic,
                "payments.orders",
                KafkaAclFilterPatternMode.Match,
                "User:alice",
                Operation: KafkaAclOperation.Read));

        Assert.Equal(
            Confluent.Kafka.Admin.ResourceType.Topic,
            filter.PatternFilter.Type);
        Assert.Equal(
            Confluent.Kafka.Admin.ResourcePatternType.Match,
            filter.PatternFilter.ResourcePatternType);
        Assert.Equal(
            Confluent.Kafka.Admin.AclOperation.Read,
            filter.EntryFilter.Operation);

        Assert.Throws<NotSupportedException>(() =>
            ConfluentKafkaAclMapper.ToProviderFilter(
                new KafkaAclBindingFilter(
                    KafkaAclResourceType.TransactionalId,
                    "tx-1",
                    KafkaAclFilterPatternMode.Literal,
                    "User:alice")));
    }

    [Fact]
    public void Provider_mapper_preserves_exact_binding_shape_for_mutation()
    {
        var provider = ConfluentKafkaAclMapper.ToProviderBinding(
            Binding(
                "payments.",
                KafkaAclOperation.Write,
                KafkaAclPermissionType.Deny,
                KafkaAclPatternType.Prefixed));

        Assert.Equal(
            Confluent.Kafka.Admin.ResourceType.Topic,
            provider.Pattern.Type);
        Assert.Equal("payments.", provider.Pattern.Name);
        Assert.Equal(
            Confluent.Kafka.Admin.ResourcePatternType.Prefixed,
            provider.Pattern.ResourcePatternType);
        Assert.Equal("User:alice", provider.Entry.Principal);
        Assert.Equal("*", provider.Entry.Host);
        Assert.Equal(
            Confluent.Kafka.Admin.AclOperation.Write,
            provider.Entry.Operation);
        Assert.Equal(
            Confluent.Kafka.Admin.AclPermissionType.Deny,
            provider.Entry.PermissionType);

        Assert.Throws<NotSupportedException>(() =>
            ConfluentKafkaAclMapper.ToProviderBinding(
                new KafkaAclBinding(
                    KafkaAclResourceType.TransactionalId,
                    "tx-1",
                    KafkaAclPatternType.Literal,
                    "User:alice",
                    "*",
                    KafkaAclOperation.Write,
                    KafkaAclPermissionType.Allow)));
    }

    [Fact]
    public void Acl_mutation_port_exposes_only_exact_typed_create_and_remove_batches()
    {
        var methods = typeof(IAclMutationPort)
            .GetMethods()
            .OrderBy(method => method.Name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[] { "CreateAsync", "RemoveAsync" },
            methods.Select(method => method.Name).ToArray());
        Assert.Contains(
            methods,
            method => method.GetParameters()[0].ParameterType == typeof(AclCreateMutation));
        Assert.Contains(
            methods,
            method => method.GetParameters()[0].ParameterType == typeof(AclRemoveMutation));
        Assert.DoesNotContain(
            methods.SelectMany(method => method.GetParameters()),
            parameter => parameter.ParameterType == typeof(KafkaAclBindingFilter));
    }

    private static KafkaAclBinding Binding(
        string resource,
        KafkaAclOperation operation,
        KafkaAclPermissionType permission,
        KafkaAclPatternType pattern = KafkaAclPatternType.Literal) =>
        new(
            KafkaAclResourceType.Topic,
            resource,
            pattern,
            "User:alice",
            "*",
            operation,
            permission);

    private static AclServerPolicy Policy(
        int maxBindingsPerMutation = 64) =>
        new(
            Array.Empty<string>(),
            new[] { "User:alice" },
            new[]
            {
                KafkaAclResourceType.Topic,
                KafkaAclResourceType.Group,
                KafkaAclResourceType.Cluster,
            },
            Enum.GetValues<KafkaAclOperation>(),
            allowPrefixedGrants: true,
            allowWildcardResourceGrants: false,
            allowAllOperationGrants: false,
            maxBindingsPerMutation: maxBindingsPerMutation);

    private sealed class SelectivePrincipalObservationPort : IAclObservationPort
    {
        private readonly KafkaAclBinding _exact;
        private readonly KafkaAclBinding _wildcard;

        public SelectivePrincipalObservationPort(
            KafkaAclBinding exact,
            KafkaAclBinding wildcard)
        {
            _exact = exact;
            _wildcard = wildcard;
        }

        public List<string?> Principals { get; } = new();

        public List<KafkaAclOperation?> Operations { get; } = new();

        public Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
            string clusterId,
            KafkaAclBindingFilter filter,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            Principals.Add(filter.Principal);
            Operations.Add(filter.Operation);
            IReadOnlyList<KafkaAclBinding> values =
                string.Equals(filter.Principal, "User:*", StringComparison.Ordinal)
                    ? new[] { _wildcard }
                    : new[] { _exact };
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                    values,
                    Observation()));
        }
    }

    private sealed class DeadlineCapturingObservationPort : IAclObservationPort
    {
        private static readonly DateTimeOffset ReferenceTime =
            new(2026, 9, 25, 6, 30, 0, TimeSpan.Zero);

        public List<TimeSpan> Remaining { get; } = new();

        public Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
            string clusterId,
            KafkaAclBindingFilter filter,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            Remaining.Add(operation.Remaining(ReferenceTime));
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                    Array.Empty<KafkaAclBinding>(),
                    Observation()));
        }
    }

    private sealed class AdvancingTimeProvider : TimeProvider
    {
        private static readonly DateTimeOffset Start =
            new(2026, 9, 25, 6, 30, 0, TimeSpan.Zero);
        private int _callCount;

        public override DateTimeOffset GetUtcNow() =>
            Start.AddSeconds(_callCount++);
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() =>
            new(2026, 9, 25, 6, 30, 0, TimeSpan.Zero);
    }

    private sealed class StubAclObservationPort : IAclObservationPort
    {
        private readonly IReadOnlyList<KafkaAclBinding> _bindings;

        public StubAclObservationPort(IReadOnlyList<KafkaAclBinding> bindings)
        {
            _bindings = bindings;
        }

        public Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
            string clusterId,
            KafkaAclBindingFilter filter,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            var matches = _bindings
                .Where(binding => AclMutationPolicy.MatchesFilter(binding, filter))
                .ToArray();
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                    Array.AsReadOnly(matches),
                    Observation()));
        }
    }

    private sealed class SelectiveAclObservationPort : IAclObservationPort
    {
        private readonly KafkaAclBinding _existing;

        public SelectiveAclObservationPort(KafkaAclBinding existing)
        {
            _existing = existing;
        }

        public List<KafkaAclBindingFilter> Filters { get; } = new();

        public Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
            string clusterId,
            KafkaAclBindingFilter filter,
            KafkaOperationContext operation,
            CancellationToken cancellationToken = default)
        {
            Filters.Add(filter);
            var values = AclMutationPolicy.MatchesFilter(_existing, filter)
                ? new[] { _existing }
                : Array.Empty<KafkaAclBinding>();
            return Task.FromResult(
                KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                    Array.AsReadOnly(values),
                    Observation()));
        }
    }

    private static ObservationMetadata Observation() =>
        new(
            new DateTimeOffset(2026, 9, 25, 6, 30, 0, TimeSpan.Zero),
            new DateTimeOffset(2026, 9, 25, 6, 30, 0, TimeSpan.Zero),
            new DateTimeOffset(2026, 9, 25, 6, 30, 0, TimeSpan.Zero),
            ObservationSource.Live);
}
