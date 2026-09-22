using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Consumers;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05ConsumerMutationTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 22, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Offset_planner_resolves_all_selectors_before_preview()
    {
        var observation = Observation(
            "orders-workers",
            ConsumerGroupState.Empty,
            Partition("orders", 0, 10, 0, 100, null),
            Partition("orders", 1, 30, 0, 100, null),
            Partition("orders", 2, 10, 0, 100, null),
            Partition("orders", 3, 20, 0, 100, 50),
            Partition("orders", 4, 10, 0, 100, null));

        var port = new FakeObservationPort(observation);
        var planner = new ConsumerMutationPlanner(
            port,
            timeProvider: new FixedTimeProvider(Now));

        var result = await planner.PlanOffsetAlterAsync(
            new ConsumerOffsetAlterRequest(
                "prod",
                "orders-workers",
                new[]
                {
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        0,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            20)),
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        1,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Earliest)),
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        2,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Latest)),
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        3,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Timestamp,
                            TimestampUtc: Now)),
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        4,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.RelativeShift,
                            -5)),
                }));

        Assert.True(result.IsSuccess);
        var plan = result.Plan!;
        Assert.Equal(
            new long[] { 20, 0, 100, 50, 5 },
            plan.Canonical.Targets.Select(target => target.ResolvedOffset));
        Assert.Equal(
            ConsumerOffsetMovement.BackwardReplay,
            plan.Canonical.Targets[4].Movement);
        Assert.Equal(35, plan.Canonical.TotalBackwardDistance);
        Assert.Equal(130, plan.Canonical.TotalForwardDistance);
        Assert.Equal(0, plan.Canonical.MissingCommittedOffsetCount);
        Assert.Equal(MutationRiskClass.Critical, plan.Risk.RiskClass);
        Assert.True(plan.Risk.RequiresIndependentApproval);
        Assert.Equal(MutationConfirmationMode.TypedTarget, plan.Risk.ConfirmationMode);

        Assert.Contains(
            plan.Intent.AuthorizationTargets,
            target =>
                target.Action == AuthorizationAction.ConsumerOffsetAlter &&
                target.ResourceName == "consumer-group/orders-workers");
        Assert.Contains(
            plan.Intent.AuthorizationTargets,
            target =>
                target.Action == AuthorizationAction.ConsumerOffsetAlter &&
                target.ResourceName == "topic/orders");
        Assert.Equal(2, plan.Intent.AuthorizationTargets.Count);
        Assert.Equal(6, plan.Intent.ResourceKeys.Count);
        Assert.Equal(6, plan.Intent.Preconditions.Count);

        Assert.All(
            port.LastTargets!,
            target =>
                Assert.True(
                    target.ResolveTimestampUtc.HasValue ==
                    (target.Partition == 3)));
    }

    [Fact]
    public async Task Planner_never_clamps_invalid_or_unresolvable_offsets()
    {
        var port = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Empty,
                Partition("orders", 0, 10, 0, 100, null)));

        var planner = new ConsumerMutationPlanner(port);

        var relative = await planner.PlanOffsetAlterAsync(
            new ConsumerOffsetAlterRequest(
                "prod",
                "g",
                new[]
                {
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        0,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.RelativeShift,
                            -20)),
                }));
        Assert.False(relative.IsSuccess);
        Assert.Equal(
            ConsumerMutationPlanningFailureCode.OffsetOutOfRange,
            relative.Failure!.Code);

        var absolute = await planner.PlanOffsetAlterAsync(
            new ConsumerOffsetAlterRequest(
                "prod",
                "g",
                new[]
                {
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        0,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            101)),
                }));
        Assert.False(absolute.IsSuccess);
        Assert.Equal(
            ConsumerMutationPlanningFailureCode.OffsetOutOfRange,
            absolute.Failure!.Code);

        var timestamp = await planner.PlanOffsetAlterAsync(
            new ConsumerOffsetAlterRequest(
                "prod",
                "g",
                new[]
                {
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        0,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Timestamp,
                            TimestampUtc: Now)),
                }));
        Assert.False(timestamp.IsSuccess);
        Assert.Equal(
            ConsumerMutationPlanningFailureCode.TimestampUnresolved,
            timestamp.Failure!.Code);
    }

    [Fact]
    public async Task Planner_rejects_active_group_and_relative_missing_commit()
    {
        var activePort = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Stable,
                Partition("orders", 0, 10, 0, 100, null)));

        var active = await new ConsumerMutationPlanner(activePort)
            .PlanOffsetAlterAsync(
                new ConsumerOffsetAlterRequest(
                    "prod",
                    "g",
                    new[]
                    {
                        new ConsumerOffsetAlterTargetInput(
                            "orders",
                            0,
                            new ConsumerOffsetSelector(
                                ConsumerOffsetSelectorKind.Latest)),
                    }));

        Assert.False(active.IsSuccess);
        Assert.Equal(
            ConsumerMutationPlanningFailureCode.GroupStateUnsafe,
            active.Failure!.Code);

        var missingPort = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Empty,
                Partition("orders", 0, null, 0, 100, null)));

        var missing = await new ConsumerMutationPlanner(missingPort)
            .PlanOffsetAlterAsync(
                new ConsumerOffsetAlterRequest(
                    "prod",
                    "g",
                    new[]
                    {
                        new ConsumerOffsetAlterTargetInput(
                            "orders",
                            0,
                            new ConsumerOffsetSelector(
                                ConsumerOffsetSelectorKind.RelativeShift,
                                5)),
                    }));

        Assert.False(missing.IsSuccess);
        Assert.Equal(
            ConsumerMutationPlanningFailureCode.MissingCommittedOffset,
            missing.Failure!.Code);
    }

    [Fact]
    public async Task Delete_planner_distinguishes_group_and_offset_deletion()
    {
        var port = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Empty,
                Partition("orders", 0, 10, 0, 100, null)));
        var planner = new ConsumerMutationPlanner(port);

        var group = await planner.PlanDeleteAsync(
            new ConsumerDeleteRequest(
                "prod",
                "g",
                ConsumerDeleteMode.Group));

        Assert.True(group.IsSuccess);
        Assert.Empty(group.Plan!.Canonical.Targets);
        Assert.Equal(MutationRiskClass.High, group.Plan.Risk.RiskClass);

        port.Current = Observation(
            "g",
            ConsumerGroupState.Empty,
            Partition("orders", 0, null, 0, 100, null));

        var missing = await planner.PlanDeleteAsync(
            new ConsumerDeleteRequest(
                "prod",
                "g",
                ConsumerDeleteMode.Offsets,
                new[] { new ConsumerOffsetDeleteTargetInput("orders", 0) }));

        Assert.False(missing.IsSuccess);
        Assert.Equal(
            ConsumerMutationPlanningFailureCode.MissingCommittedOffset,
            missing.Failure!.Code);
    }

    [Fact]
    public async Task Pre_dispatch_rejects_offset_or_membership_drift()
    {
        var original = Observation(
            "g",
            ConsumerGroupState.Empty,
            Partition("orders", 0, 10, 0, 100, null));
        var port = new FakeObservationPort(original);
        var planner = new ConsumerMutationPlanner(
            port,
            timeProvider: new FixedTimeProvider(Now));

        var plan = await planner.PlanOffsetAlterAsync(
            new ConsumerOffsetAlterRequest(
                "prod",
                "g",
                new[]
                {
                    new ConsumerOffsetAlterTargetInput(
                        "orders",
                        0,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            20)),
                }));
        Assert.True(plan.IsSuccess);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            plan.Plan!.Intent,
            plan.Plan.Risk,
            "w35-test",
            Now.AddMinutes(5),
            Now,
            "consumer-offset");

        port.Current = Observation(
            "g",
            ConsumerGroupState.Empty,
            Partition("orders", 0, 11, 0, 100, null));

        var guard = new ConsumerMutationPreconditionValidator(
            port,
            timeProvider: new FixedTimeProvider(Now));

        var staleOffset = await guard.ValidateAsync(operation.Snapshot);
        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            staleOffset.Outcome);

        port.Current = original with
        {
            Members = new[]
            {
                new ConsumerMemberProjection(
                    "member-1",
                    null,
                    "client",
                    "host",
                    new[]
                    {
                        new ConsumerPartitionAssignment("orders", 0),
                    }),
            },
        };

        var staleMembership = await guard.ValidateAsync(operation.Snapshot);
        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            staleMembership.Outcome);
    }

    [Fact]
    public async Task Execution_verifies_alter_and_group_delete_by_readback()
    {
        var mutations = new FakeMutationPort();
        var observationPort = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Empty,
                Partition("orders", 0, 20, 0, 100, null)));

        var service = new ConsumerMutationExecutionService(
            mutations,
            observationPort,
            new ConsumerMutationVerificationPolicy(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(50)));

        var alter = new ConsumerOffsetAlterCanonicalIntent(
            "prod",
            "g",
            ConsumerGroupState.Empty,
            new string('a', 64),
            0,
            10,
            0,
            new[]
            {
                new ConsumerOffsetCanonicalTarget(
                    0,
                    "orders",
                    0,
                    new ConsumerOffsetCanonicalSelector(
                        ConsumerOffsetSelectorKind.Absolute,
                        20,
                        null),
                    false,
                    10,
                    0,
                    100,
                    20,
                    10,
                    ConsumerOffsetMovement.ForwardSkip),
            });

        var altered = await service.AlterOffsetsAsync(alter);
        Assert.Equal(
            MutationExecutionResultKind.AppliedVerified,
            altered.ResultKind);

        observationPort.Current = new ConsumerMutationObservation(
            "g",
            false,
            ConsumerGroupState.Dead,
            Array.Empty<ConsumerMemberProjection>(),
            Array.Empty<ConsumerMutationPartitionObservation>());

        var deleted = await service.DeleteAsync(
            new ConsumerDeleteCanonicalIntent(
                "prod",
                "g",
                ConsumerDeleteMode.Group,
                ConsumerGroupState.Empty,
                new string('a', 64),
                Array.Empty<ConsumerDeleteCanonicalTarget>()));

        Assert.Equal(
            MutationExecutionResultKind.AppliedVerified,
            deleted.ResultKind);
    }

    [Fact]
    public async Task Malformed_canonical_never_reaches_consumer_mutation_provider()
    {
        var mutations = new FakeMutationPort();
        var observationPort = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Empty,
                Partition("orders", 0, 10, 0, 100, null)));
        var service = new ConsumerMutationExecutionService(
            mutations,
            observationPort);

        var malformed = new ConsumerOffsetAlterCanonicalIntent(
            "prod",
            "g",
            ConsumerGroupState.Empty,
            new string('a', 64),
            0,
            999,
            0,
            new[]
            {
                new ConsumerOffsetCanonicalTarget(
                    0,
                    "orders",
                    0,
                    new ConsumerOffsetCanonicalSelector(
                        ConsumerOffsetSelectorKind.Absolute,
                        20,
                        null),
                    false,
                    10,
                    0,
                    100,
                    20,
                    10,
                    ConsumerOffsetMovement.ForwardSkip),
            });

        var result = await service.AlterOffsetsAsync(malformed);

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            result.ResultKind);
        Assert.Equal(0, mutations.AlterCalls);
        Assert.Equal(0, mutations.DeleteCalls);
    }

    private static ConsumerMutationObservation Observation(
        string groupId,
        ConsumerGroupState state,
        params ConsumerMutationPartitionObservation[] partitions) =>
        new(
            groupId,
            true,
            state,
            Array.Empty<ConsumerMemberProjection>(),
            partitions);

    private static ConsumerMutationPartitionObservation Partition(
        string topic,
        int partition,
        long? committed,
        long low,
        long high,
        long? timestampOffset) =>
        new(
            topic,
            partition,
            committed,
            low,
            high,
            timestampOffset);

    private sealed class FakeObservationPort :
        IConsumerMutationObservationPort
    {
        public FakeObservationPort(ConsumerMutationObservation current)
        {
            Current = current;
        }

        public ConsumerMutationObservation Current { get; set; }
        public IReadOnlyList<ConsumerMutationObservationTarget>? LastTargets { get; private set; }

        public Task<KafkaResult<ConsumerMutationObservation>> ObserveAsync(
            string clusterId,
            string groupId,
            IReadOnlyList<ConsumerMutationObservationTarget> targets,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            LastTargets = targets;
            var now = Now;
            return Task.FromResult(
                KafkaResult<ConsumerMutationObservation>.Success(
                    Current,
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }
    }

    private sealed class FakeMutationPort : IConsumerMutationPort
    {
        public int AlterCalls { get; private set; }
        public int DeleteCalls { get; private set; }

        public Task<MutationProviderResult> AlterOffsetsAsync(
            ConsumerOffsetAlterMutation request,
            CancellationToken cancellationToken = default)
        {
            AlterCalls++;
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.AppliedUnverified,
                    "consumer_offset_alter_accepted"));
        }

        public Task<MutationProviderResult> DeleteAsync(
            ConsumerDeleteMutation request,
            CancellationToken cancellationToken = default)
        {
            DeleteCalls++;
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.AppliedUnverified,
                    "consumer_delete_accepted"));
        }
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
