using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Purge;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05RecordsPurgeTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 22, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Planner_freezes_absolute_targets_and_enforces_critical_governance()
    {
        var port = new FakeObservationPort(new RecordsPurgeObservation(
            new[]
            {
                Partition("orders", 0, 10, 100, null),
                Partition("orders", 1, 20, 200, null),
            }));
        var planner = new RecordsPurgePlanner(
            port,
            timeProvider: new FixedTimeProvider(Now));

        var result = await planner.PlanAsync(new RecordsPurgeRequest(
            "prod",
            new[]
            {
                new RecordsPurgeTargetInput(
                    "orders",
                    1,
                    new RecordsPurgeSelector(
                        RecordsPurgeSelectorKind.Absolute,
                        BeforeOffset: 150)),
                new RecordsPurgeTargetInput(
                    "orders",
                    0,
                    new RecordsPurgeSelector(
                        RecordsPurgeSelectorKind.Absolute,
                        BeforeOffset: 50)),
            }));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        var plan = result.Plan!;
        Assert.True(plan.Canonical.Irreversible);
        Assert.Collection(
            plan.Canonical.Targets,
            target =>
            {
                Assert.Equal(0, target.Ordinal);
                Assert.Equal("orders", target.TopicName);
                Assert.Equal(0, target.Partition);
                Assert.Equal(50, target.ResolvedBeforeOffset);
                Assert.Equal(10, target.LowWatermark);
                Assert.Equal(100, target.HighWatermark);
            },
            target =>
            {
                Assert.Equal(1, target.Ordinal);
                Assert.Equal("orders", target.TopicName);
                Assert.Equal(1, target.Partition);
                Assert.Equal(150, target.ResolvedBeforeOffset);
                Assert.Equal(20, target.LowWatermark);
                Assert.Equal(200, target.HighWatermark);
            });

        Assert.Equal(MutationOperationKind.RecordsPurge, plan.Intent.Kind);
        Assert.Equal(2, plan.Intent.ResourceKeys.Count);
        var authorization = Assert.Single(plan.Intent.AuthorizationTargets!);
        Assert.Equal(AuthorizationAction.RecordsPurge, authorization.Action);
        Assert.Equal("prod", authorization.ClusterId);
        Assert.Equal("topic/orders", authorization.ResourceName);
        Assert.Equal(2, plan.Intent.Preconditions!.Count);

        Assert.Equal(MutationRiskClass.Critical, plan.Risk.RiskClass);
        Assert.Equal(MutationConfirmationMode.TypedTarget, plan.Risk.ConfirmationMode);
        Assert.True(plan.Risk.RequiresIndependentApproval);
    }

    [Fact]
    public async Task Timestamp_selector_is_resolved_once_and_frozen_into_preview()
    {
        var port = new FakeObservationPort(new RecordsPurgeObservation(
            new[] { Partition("orders", 0, 10, 100, 42) }));
        var planner = new RecordsPurgePlanner(
            port,
            timeProvider: new FixedTimeProvider(Now));

        var result = await planner.PlanAsync(new RecordsPurgeRequest(
            "prod",
            new[]
            {
                new RecordsPurgeTargetInput(
                    "orders",
                    0,
                    new RecordsPurgeSelector(
                        RecordsPurgeSelectorKind.Timestamp,
                        TimestampUtc: Now.AddMinutes(-5))),
            }));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        var target = Assert.Single(result.Plan!.Canonical.Targets);
        Assert.Equal(42, target.ResolvedBeforeOffset);
        Assert.Equal(
            Now.AddMinutes(-5).ToUnixTimeMilliseconds(),
            target.Selector.TimestampUnixMilliseconds);
        Assert.Null(target.Selector.RequestedBeforeOffset);
        Assert.NotNull(port.LastTargets);
        Assert.Equal(
            Now.AddMinutes(-5),
            Assert.Single(port.LastTargets!).ResolveTimestampUtc);
    }

    [Fact]
    public async Task Planner_rejects_unresolved_timestamp_without_clamping()
    {
        var unresolvedPlanner = new RecordsPurgePlanner(
            new FakeObservationPort(new RecordsPurgeObservation(
                new[] { Partition("orders", 0, 10, 100, null) })),
            timeProvider: new FixedTimeProvider(Now));

        var unresolved = await unresolvedPlanner.PlanAsync(new RecordsPurgeRequest(
            "prod",
            new[]
            {
                new RecordsPurgeTargetInput(
                    "orders",
                    0,
                    new RecordsPurgeSelector(
                        RecordsPurgeSelectorKind.Timestamp,
                        TimestampUtc: Now)),
            }));

        Assert.False(unresolved.IsSuccess);
        Assert.Equal(
            RecordsPurgePlanningFailureCode.TimestampUnresolved,
            unresolved.Failure!.Code);

        var rangePlanner = new RecordsPurgePlanner(
            new FakeObservationPort(new RecordsPurgeObservation(
                new[] { Partition("orders", 0, 10, 100, null) })),
            timeProvider: new FixedTimeProvider(Now));

        var belowLow = await rangePlanner.PlanAsync(new RecordsPurgeRequest(
            "prod",
            new[]
            {
                new RecordsPurgeTargetInput(
                    "orders",
                    0,
                    new RecordsPurgeSelector(
                        RecordsPurgeSelectorKind.Absolute,
                        BeforeOffset: 9)),
            }));

        Assert.False(belowLow.IsSuccess);
        Assert.Equal(
            RecordsPurgePlanningFailureCode.OffsetOutOfRange,
            belowLow.Failure!.Code);
    }

    [Fact]
    public async Task Planner_deduplicates_identical_targets_and_rejects_conflicts()
    {
        var planner = new RecordsPurgePlanner(
            new FakeObservationPort(new RecordsPurgeObservation(
                new[] { Partition("orders", 0, 0, 100, null) })),
            timeProvider: new FixedTimeProvider(Now));

        var duplicate = await planner.PlanAsync(new RecordsPurgeRequest(
            "prod",
            new[]
            {
                Target(20),
                Target(20),
            }));

        Assert.True(duplicate.IsSuccess, duplicate.Failure?.SafeMessage);
        Assert.Single(duplicate.Plan!.Canonical.Targets);

        var conflicting = await planner.PlanAsync(new RecordsPurgeRequest(
            "prod",
            new[]
            {
                Target(20),
                Target(30),
            }));

        Assert.False(conflicting.IsSuccess);
        Assert.Equal(
            RecordsPurgePlanningFailureCode.InvalidInput,
            conflicting.Failure!.Code);
    }

    [Fact]
    public async Task Planner_fails_closed_on_unexpected_observation_shape()
    {
        var planner = new RecordsPurgePlanner(
            new FakeObservationPort(new RecordsPurgeObservation(
                new[]
                {
                    Partition("orders", 0, 0, 100, null),
                    Partition("unexpected", 0, 0, 100, null),
                })),
            timeProvider: new FixedTimeProvider(Now));

        var result = await planner.PlanAsync(new RecordsPurgeRequest(
            "prod",
            new[] { Target(20) }));

        Assert.False(result.IsSuccess);
        Assert.Equal(
            RecordsPurgePlanningFailureCode.ObservationFailed,
            result.Failure!.Code);
    }

    private static RecordsPurgeTargetInput Target(long beforeOffset) =>
        new(
            "orders",
            0,
            new RecordsPurgeSelector(
                RecordsPurgeSelectorKind.Absolute,
                BeforeOffset: beforeOffset));

    private static RecordsPurgePartitionObservation Partition(
        string topic,
        int partition,
        long low,
        long high,
        long? timestampOffset) =>
        new(topic, partition, low, high, timestampOffset);

    private sealed class FakeObservationPort : IRecordsPurgeObservationPort
    {
        private readonly RecordsPurgeObservation _observation;

        public FakeObservationPort(RecordsPurgeObservation observation)
        {
            _observation = observation;
        }

        public IReadOnlyList<RecordsPurgeObservationTarget>? LastTargets { get; private set; }

        public Task<KafkaResult<RecordsPurgeObservation>> ObserveAsync(
            string clusterId,
            IReadOnlyList<RecordsPurgeObservationTarget> targets,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            LastTargets = targets;
            return Task.FromResult(KafkaResult<RecordsPurgeObservation>.Success(
                _observation,
                new ObservationMetadata(
                    Now,
                    Now,
                    Now,
                    ObservationSource.Live)));
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
