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
        Assert.Equal("acl_precondition_binding_set_changed", result.Code);
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
        Assert.Equal("acl_precondition_binding_set_changed", result.Code);
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
        Assert.Equal("acl_precondition_policy_changed", result.Code);
    }

    [Fact]
    public void Pinned_provider_does_not_invent_cluster_or_transactional_id_acl_resources()
    {
        Assert.Throws<NotSupportedException>(() =>
            Kafdeck.Infrastructure.Kafka.ConfluentKafkaAclMapper.ToProviderFilter(
                new KafkaAclBindingFilter(
                    KafkaAclResourceType.Cluster,
                    "kafka-cluster",
                    KafkaAclFilterPatternMode.Literal,
                    "User:alice")));

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

    private sealed class FixedTimeProvider : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static ObservationMetadata Observation() =>
        new(Now, Now, Now, ObservationSource.Live);
}
