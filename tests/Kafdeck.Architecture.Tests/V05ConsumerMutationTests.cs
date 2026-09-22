using Confluent.Kafka;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
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

        Assert.NotNull(plan.Intent.AuthorizationTargets);
        var authorizationTargets = plan.Intent.AuthorizationTargets!;
        Assert.Contains(
            authorizationTargets,
            target =>
                target.Action == AuthorizationAction.ConsumerOffsetAlter &&
                target.ResourceName == "consumer-group/orders-workers");
        Assert.Contains(
            authorizationTargets,
            target =>
                target.Action == AuthorizationAction.ConsumerOffsetAlter &&
                target.ResourceName == "topic/orders");
        Assert.Equal(2, authorizationTargets.Count);
        Assert.Equal(6, plan.Intent.ResourceKeys.Count);
        Assert.NotNull(plan.Intent.Preconditions);
        Assert.Equal(6, plan.Intent.Preconditions!.Count);

        Assert.All(
            port.LastTargets!,
            target =>
                Assert.True(
                    target.ResolveTimestampUtc.HasValue ==
                    (target.Partition == 3)));
    }

    [Fact]
    public async Task Planner_deduplicates_sorts_targets_and_enforces_bounds()
    {
        var port = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Empty,
                Partition("a", 0, 5, 0, 100, null),
                Partition("b", 1, 5, 0, 100, null)));
        var planner = new ConsumerMutationPlanner(port);

        var result = await planner.PlanOffsetAlterAsync(
            new ConsumerOffsetAlterRequest(
                "prod",
                "g",
                new[]
                {
                    new ConsumerOffsetAlterTargetInput(
                        "b",
                        1,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            10)),
                    new ConsumerOffsetAlterTargetInput(
                        "a",
                        0,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            10)),
                    new ConsumerOffsetAlterTargetInput(
                        "b",
                        1,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            10)),
                }));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(2, result.Plan!.Canonical.Targets.Count);
        Assert.Equal("a", result.Plan.Canonical.Targets[0].TopicName);
        Assert.Equal(0, result.Plan.Canonical.Targets[0].Partition);
        Assert.Equal("b", result.Plan.Canonical.Targets[1].TopicName);
        Assert.Equal(1, result.Plan.Canonical.Targets[1].Partition);

        var boundedPlanner = new ConsumerMutationPlanner(
            port,
            new ConsumerMutationPolicy(maxTargets: 2));

        var bounded = await boundedPlanner.PlanOffsetAlterAsync(
            new ConsumerOffsetAlterRequest(
                "prod",
                "g",
                new[]
                {
                    new ConsumerOffsetAlterTargetInput(
                        "a",
                        0,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            10)),
                    new ConsumerOffsetAlterTargetInput(
                        "b",
                        1,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            10)),
                    new ConsumerOffsetAlterTargetInput(
                        "c",
                        2,
                        new ConsumerOffsetSelector(
                            ConsumerOffsetSelectorKind.Absolute,
                            10)),
                }));

        Assert.False(bounded.IsSuccess);
        Assert.Equal(
            ConsumerMutationPlanningFailureCode.LimitExceeded,
            bounded.Failure!.Code);
    }

    [Fact]
    public async Task Single_target_mutation_preserves_high_floor_and_typed_confirmation()
    {
        var planner = new ConsumerMutationPlanner(
            new FakeObservationPort(
                Observation(
                    "g",
                    ConsumerGroupState.Empty,
                    Partition("orders", 0, 10, 0, 100, null))));

        var result = await planner.PlanOffsetAlterAsync(
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

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(MutationRiskClass.High, result.Plan!.Risk.RiskClass);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            result.Plan.Risk.ConfirmationMode);
        Assert.False(result.Plan.Risk.RequiresIndependentApproval);
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
    public async Task Execution_preserves_partial_and_ambiguous_provider_outcomes()
    {
        var mutations = new FakeMutationPort();
        var observations = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Empty,
                Partition("orders", 0, 10, 0, 100, null)));
        var service = new ConsumerMutationExecutionService(
            mutations,
            observations);

        var canonical = new ConsumerOffsetAlterCanonicalIntent(
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

        mutations.AlterResult = new MutationProviderResult(
            MutationExecutionResultKind.PartiallyApplied,
            "consumer_offset_alter_partial",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["target.count"] = "1",
                ["applied.count"] = "0",
                ["failed.count"] = "1",
            });

        var partial = await service.AlterOffsetsAsync(canonical);
        Assert.Equal(
            MutationExecutionResultKind.PartiallyApplied,
            partial.ResultKind);
        Assert.Equal(
            "consumer_offset_alter_partial",
            partial.ResultCode);

        mutations.AlterResult = new MutationProviderResult(
            MutationExecutionResultKind.ExecutionUnknown,
            "consumer_offset_alter_ambiguous");

        var unknown = await service.AlterOffsetsAsync(canonical);
        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            unknown.ResultKind);
        Assert.Equal(
            "consumer_offset_alter_ambiguous",
            unknown.ResultCode);
    }

    [Fact]
    public void Mixed_success_and_timeout_is_execution_unknown()
    {
        var result = ConfluentKafkaConsumerMutationAdapter.FromErrors(
            "consumer_offset_alter",
            new[]
            {
                new Error(ErrorCode.RequestTimedOut),
            },
            totalCount: 2,
            successfulCount: 1);

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            result.ResultKind);
        Assert.Equal(
            "consumer_offset_alter_ambiguous",
            result.ResultCode);
        Assert.NotNull(result.SafeEvidence);
        Assert.Equal("1", result.SafeEvidence!["applied.count"]);
        Assert.Contains(
            "kafka_requesttimedout",
            result.SafeEvidence["provider.error.codes"],
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Tampered_selector_semantics_are_rejected_before_dispatch()
    {
        var observations = new FakeObservationPort(
            Observation(
                "g",
                ConsumerGroupState.Empty,
                Partition("orders", 0, 10, 0, 100, null)));
        var planner = new ConsumerMutationPlanner(
            observations,
            timeProvider: new FixedTimeProvider(Now));

        var planned = await planner.PlanOffsetAlterAsync(
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
        Assert.True(planned.IsSuccess, planned.Failure?.SafeMessage);

        var validCanonical = planned.Plan!.Canonical;
        var tamperedTarget = validCanonical.Targets[0] with
        {
            Selector = new ConsumerOffsetCanonicalSelector(
                ConsumerOffsetSelectorKind.Absolute,
                21,
                null),
        };
        var tamperedCanonical = validCanonical with
        {
            Targets = new[] { tamperedTarget },
        };

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Plan.Intent,
            planned.Plan.Risk,
            "w35-selector-test",
            Now.AddMinutes(5),
            Now,
            "selector-tamper");

        var tamperedSnapshot = operation.Snapshot with
        {
            CanonicalIntent =
                ConsumerMutationCanonicalization.Serialize(
                    tamperedCanonical),
        };

        var guard = new ConsumerMutationPreconditionValidator(
            observations,
            timeProvider: new FixedTimeProvider(Now));
        var guardResult = await guard.ValidateAsync(tamperedSnapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            guardResult.Outcome);

        var mutations = new FakeMutationPort();
        var service = new ConsumerMutationExecutionService(
            mutations,
            observations);
        var execution = await service.AlterOffsetsAsync(
            tamperedCanonical);

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            execution.ResultKind);
        Assert.Equal(0, mutations.AlterCalls);
    }

    [Fact]
    public void Consumer_mutation_contract_is_typed_and_has_no_generic_admin_escape_hatch()
    {
        var methods = typeof(IConsumerMutationPort)
            .GetMethods()
            .OrderBy(method => method.Name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(2, methods.Length);
        Assert.Equal(
            new[]
            {
                nameof(IConsumerMutationPort.AlterOffsetsAsync),
                nameof(IConsumerMutationPort.DeleteAsync),
            },
            methods.Select(method => method.Name));

        var forbidden = new[]
        {
            "command",
            "config",
            "script",
            "sql",
            "url",
            "cli",
            "shell",
        };

        foreach (var type in new[]
                 {
                     typeof(ConsumerOffsetAlterMutation),
                     typeof(ConsumerDeleteMutation),
                     typeof(ConsumerOffsetTarget),
                 })
        {
            foreach (var property in type.GetProperties())
            {
                Assert.DoesNotContain(
                    forbidden,
                    term => property.Name.Contains(
                        term,
                        StringComparison.OrdinalIgnoreCase));
            }
        }
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

        public MutationProviderResult AlterResult { get; set; } =
            new(
                MutationExecutionResultKind.AppliedUnverified,
                "consumer_offset_alter_accepted");

        public MutationProviderResult DeleteResult { get; set; } =
            new(
                MutationExecutionResultKind.AppliedUnverified,
                "consumer_delete_accepted");

        public Task<MutationProviderResult> AlterOffsetsAsync(
            ConsumerOffsetAlterMutation request,
            CancellationToken cancellationToken = default)
        {
            AlterCalls++;
            return Task.FromResult(AlterResult);
        }

        public Task<MutationProviderResult> DeleteAsync(
            ConsumerDeleteMutation request,
            CancellationToken cancellationToken = default)
        {
            DeleteCalls++;
            return Task.FromResult(DeleteResult);
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
