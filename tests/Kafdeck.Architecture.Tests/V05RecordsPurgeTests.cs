using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05RecordsPurgeTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 22, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Planner_freezes_targets_and_requires_critical_independent_approval()
    {
        var observations = new FakeObservationPort(
            new[]
            {
                Observation("a", 0, 5, 100, null),
                Observation("b", 1, 10, 100, 50),
            });
        var planner = new RecordsPurgePlanner(
            observations,
            timeProvider: new FixedTimeProvider(Now));

        var result = await planner.PlanAsync(
            new RecordsPurgeRequest(
                "prod",
                new[]
                {
                    new RecordsPurgeTargetInput(
                        "b",
                        1,
                        new RecordsPurgeSelector(
                            RecordsPurgeSelectorKind.Timestamp,
                            TimestampUtc: Now)),
                    new RecordsPurgeTargetInput(
                        "a",
                        0,
                        new RecordsPurgeSelector(
                            RecordsPurgeSelectorKind.Absolute,
                            BeforeOffset: 20)),
                    new RecordsPurgeTargetInput(
                        "a",
                        0,
                        new RecordsPurgeSelector(
                            RecordsPurgeSelectorKind.Absolute,
                            BeforeOffset: 20)),
                }));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        var plan = result.Plan!;

        Assert.True(plan.Canonical.Irreversible);
        Assert.False(plan.Canonical.UndoSupported);
        Assert.Equal(
            RecordsPurgeCanonicalization.WarningCode,
            plan.Canonical.WarningCode);
        Assert.Equal(2, plan.Canonical.Targets.Count);
        Assert.Equal("a", plan.Canonical.Targets[0].TopicName);
        Assert.Equal(20, plan.Canonical.Targets[0].BeforeOffset);
        Assert.Equal(15, plan.Canonical.Targets[0].PurgeDistance);
        Assert.Equal("b", plan.Canonical.Targets[1].TopicName);
        Assert.Equal(50, plan.Canonical.Targets[1].BeforeOffset);
        Assert.Equal(40, plan.Canonical.Targets[1].PurgeDistance);
        Assert.Equal(55, plan.Canonical.TotalPurgeDistance);

        Assert.Equal(
            MutationRiskClass.Critical,
            plan.Risk.RiskClass);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            plan.Risk.ConfirmationMode);
        Assert.True(plan.Risk.RequiresIndependentApproval);

        Assert.NotNull(plan.Intent.AuthorizationTargets);
        Assert.Equal(
            2,
            plan.Intent.AuthorizationTargets!.Count);
        Assert.All(
            plan.Intent.AuthorizationTargets,
            target =>
                Assert.Equal(
                    AuthorizationAction.RecordsPurge,
                    target.Action));
        Assert.Equal(2, plan.Intent.ResourceKeys.Count);
        Assert.NotNull(plan.Intent.Preconditions);
        Assert.Equal(2, plan.Intent.Preconditions!.Count);

        Assert.NotNull(observations.LastTargets);
        Assert.Equal(
            Now,
            observations.LastTargets!
                .Single(target => target.Partition == 1)
                .ResolveTimestampUtc);
    }

    [Fact]
    public async Task Planner_rejects_out_of_range_unresolved_and_overbound_targets()
    {
        var observations = new FakeObservationPort(
            new[]
            {
                Observation("orders", 0, 10, 100, null),
            });

        var planner = new RecordsPurgePlanner(observations);

        var belowLow = await planner.PlanAsync(
            Request(
                new RecordsPurgeSelector(
                    RecordsPurgeSelectorKind.Absolute,
                    BeforeOffset: 9)));
        Assert.False(belowLow.IsSuccess);
        Assert.Equal(
            RecordsPurgePlanningFailureCode.OffsetOutOfRange,
            belowLow.Failure!.Code);

        var aboveHigh = await planner.PlanAsync(
            Request(
                new RecordsPurgeSelector(
                    RecordsPurgeSelectorKind.Absolute,
                    BeforeOffset: 101)));
        Assert.False(aboveHigh.IsSuccess);
        Assert.Equal(
            RecordsPurgePlanningFailureCode.OffsetOutOfRange,
            aboveHigh.Failure!.Code);

        var unresolved = await planner.PlanAsync(
            Request(
                new RecordsPurgeSelector(
                    RecordsPurgeSelectorKind.Timestamp,
                    TimestampUtc: Now)));
        Assert.False(unresolved.IsSuccess);
        Assert.Equal(
            RecordsPurgePlanningFailureCode.TimestampUnresolved,
            unresolved.Failure!.Code);

        var bounded = await new RecordsPurgePlanner(
                observations,
                new RecordsPurgePolicy(maxTargets: 1))
            .PlanAsync(
                new RecordsPurgeRequest(
                    "prod",
                    new[]
                    {
                        new RecordsPurgeTargetInput(
                            "orders",
                            0,
                            new RecordsPurgeSelector(
                                RecordsPurgeSelectorKind.Absolute,
                                BeforeOffset: 20)),
                        new RecordsPurgeTargetInput(
                            "other",
                            0,
                            new RecordsPurgeSelector(
                                RecordsPurgeSelectorKind.Absolute,
                                BeforeOffset: 20)),
                    }));

        Assert.False(bounded.IsSuccess);
        Assert.Equal(
            RecordsPurgePlanningFailureCode.LimitExceeded,
            bounded.Failure!.Code);
    }

    [Fact]
    public async Task Pre_dispatch_allows_high_growth_but_rejects_low_watermark_drift()
    {
        var observations = new FakeObservationPort(
            new[]
            {
                Observation("orders", 0, 10, 100, null),
            });
        var planner = new RecordsPurgePlanner(
            observations,
            timeProvider: new FixedTimeProvider(Now));

        var planned = await planner.PlanAsync(
            Request(
                new RecordsPurgeSelector(
                    RecordsPurgeSelectorKind.Absolute,
                    BeforeOffset: 50)));

        Assert.True(planned.IsSuccess, planned.Failure?.SafeMessage);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Plan!.Intent,
            planned.Plan.Risk,
            "w38-test",
            Now.AddMinutes(5),
            Now,
            "purge-precondition");

        var guard = new RecordsPurgePreconditionValidator(
            observations,
            timeProvider: new FixedTimeProvider(Now));

        observations.Current =
            new[]
            {
                Observation("orders", 0, 10, 150, null),
            };

        var allowed = await guard.ValidateAsync(operation.Snapshot);
        Assert.Equal(
            MutationPreDispatchGuardOutcome.Allowed,
            allowed.Outcome);

        observations.Current =
            new[]
            {
                Observation("orders", 0, 11, 150, null),
            };

        var stale = await guard.ValidateAsync(operation.Snapshot);
        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            stale.Outcome);
    }

    [Fact]
    public async Task Execution_verifies_low_watermark_and_respects_outer_deadline()
    {
        var canonical = Canonical(
            new RecordsPurgeCanonicalTarget(
                0,
                "orders",
                0,
                new RecordsPurgeCanonicalSelector(
                    RecordsPurgeSelectorKind.Absolute,
                    50,
                    null),
                10,
                100,
                50,
                40));

        var purge = new FakePurgePort();
        var observations = new FakeObservationPort(
            new[]
            {
                Observation("orders", 0, 50, 100, null),
            });
        var service = new RecordsPurgeExecutionService(
            purge,
            observations,
            timeProvider: new FixedTimeProvider(Now));

        using var material = new MutationExecutionMaterial();
        var context = new MutationExecutionContext(
            Snapshot(canonical, Now),
            material,
            Now.AddSeconds(5));

        var verified = await service.ExecuteAsync(context);

        Assert.Equal(
            MutationExecutionResultKind.AppliedVerified,
            verified.ResultKind);
        Assert.Equal(1, purge.Calls);

        var neverCalled = new FakePurgePort();
        var deadlineService = new RecordsPurgeExecutionService(
            neverCalled,
            observations,
            timeProvider: new FixedTimeProvider(Now));

        using var material2 = new MutationExecutionMaterial();
        var expired = await deadlineService.ExecuteAsync(
            new MutationExecutionContext(
                Snapshot(canonical, Now),
                material2,
                Now.AddSeconds(-1)));

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            expired.ResultKind);
        Assert.Equal(0, neverCalled.Calls);
    }

    [Fact]
    public async Task Partial_readback_after_acceptance_remains_applied_unverified()
    {
        var canonical = Canonical(
            new RecordsPurgeCanonicalTarget(
                0,
                "orders",
                0,
                new RecordsPurgeCanonicalSelector(
                    RecordsPurgeSelectorKind.Absolute,
                    50,
                    null),
                10,
                100,
                50,
                40),
            new RecordsPurgeCanonicalTarget(
                1,
                "orders",
                1,
                new RecordsPurgeCanonicalSelector(
                    RecordsPurgeSelectorKind.Absolute,
                    50,
                    null),
                10,
                100,
                50,
                40));

        var purge = new FakePurgePort();
        var observations = new FakeObservationPort(
            new[]
            {
                Observation("orders", 0, 50, 100, null),
                Observation("orders", 1, 10, 100, null),
            });
        var service = new RecordsPurgeExecutionService(
            purge,
            observations,
            new RecordsPurgeVerificationPolicy(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(50)));

        using var cancellation =
            new CancellationTokenSource(
                TimeSpan.FromMilliseconds(180));
        using var material = new MutationExecutionMaterial();

        var result = await service.ExecuteAsync(
            new MutationExecutionContext(
                Snapshot(
                    canonical,
                    DateTimeOffset.UtcNow),
                material,
                DateTimeOffset.UtcNow.AddSeconds(1)),
            cancellation.Token);

        Assert.Equal(
            MutationExecutionResultKind.AppliedUnverified,
            result.ResultKind);
        Assert.NotNull(result.SafeEvidence);
        Assert.Equal(
            "partial",
            result.SafeEvidence!["verification.state"]);
        Assert.Equal(
            "1",
            result.SafeEvidence["verified.count"]);
        Assert.Equal(
            "2",
            result.SafeEvidence["target.count"]);
    }

    [Fact]
    public async Task Tampered_canonical_never_reaches_delete_records_provider()
    {
        var valid = Canonical(
            new RecordsPurgeCanonicalTarget(
                0,
                "orders",
                0,
                new RecordsPurgeCanonicalSelector(
                    RecordsPurgeSelectorKind.Absolute,
                    50,
                    null),
                10,
                100,
                50,
                40));

        var tampered = valid with
        {
            TotalPurgeDistance = 999,
        };

        var purge = new FakePurgePort();
        var service = new RecordsPurgeExecutionService(
            purge,
            new FakeObservationPort(
                new[]
                {
                    Observation("orders", 0, 50, 100, null),
                }),
            timeProvider: new FixedTimeProvider(Now));

        using var material = new MutationExecutionMaterial();
        var result = await service.ExecuteAsync(
            new MutationExecutionContext(
                Snapshot(tampered, Now),
                material,
                Now.AddSeconds(5)));

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            result.ResultKind);
        Assert.Equal(0, purge.Calls);
    }

    [Fact]
    public void Provider_reports_preserve_partial_and_ambiguous_truth()
    {
        var targets = new[]
        {
            new RecordsPurgeTarget("orders", 0, 50),
            new RecordsPurgeTarget("orders", 1, 50),
        };

        var partial =
            ConfluentKafkaRecordsPurgeAdapter.FromReports(
                new[]
                {
                    new DeleteRecordsReport
                    {
                        Topic = "orders",
                        Partition = new Partition(0),
                        Offset = new Offset(50),
                        Error = new Error(ErrorCode.NoError),
                    },
                    new DeleteRecordsReport
                    {
                        Topic = "orders",
                        Partition = new Partition(1),
                        Offset = new Offset(10),
                        Error = new Error(
                            ErrorCode.TopicAuthorizationFailed),
                    },
                },
                targets);

        Assert.Equal(
            MutationExecutionResultKind.PartiallyApplied,
            partial.ResultKind);

        var ambiguous =
            ConfluentKafkaRecordsPurgeAdapter.FromReports(
                new[]
                {
                    new DeleteRecordsReport
                    {
                        Topic = "orders",
                        Partition = new Partition(0),
                        Offset = new Offset(10),
                        Error = new Error(
                            ErrorCode.UnknownServerError),
                    },
                    new DeleteRecordsReport
                    {
                        Topic = "orders",
                        Partition = new Partition(1),
                        Offset = new Offset(10),
                        Error = new Error(
                            ErrorCode.TopicAuthorizationFailed),
                    },
                },
                targets);

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            ambiguous.ResultKind);
        Assert.Equal(
            "records_purge_ambiguous",
            ambiguous.ResultCode);
    }

    [Fact]
    public void Purge_contract_is_typed_and_has_no_generic_admin_escape_hatch()
    {
        var methods = typeof(IRecordsPurgeMutationPort)
            .GetMethods();

        Assert.Single(methods);
        Assert.Equal(
            nameof(IRecordsPurgeMutationPort.PurgeAsync),
            methods[0].Name);

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
                     typeof(RecordsPurgeMutation),
                     typeof(RecordsPurgeTarget),
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

    private static RecordsPurgeRequest Request(
        RecordsPurgeSelector selector) =>
        new(
            "prod",
            new[]
            {
                new RecordsPurgeTargetInput(
                    "orders",
                    0,
                    selector),
            });

    private static RecordsPurgeCanonicalIntent Canonical(
        params RecordsPurgeCanonicalTarget[] targets) =>
        new(
            "prod",
            Irreversible: true,
            UndoSupported: false,
            RecordsPurgeCanonicalization.WarningCode,
            targets.Sum(target => target.PurgeDistance),
            targets);

    private static MutationOperationSnapshot Snapshot(
        RecordsPurgeCanonicalIntent canonical,
        DateTimeOffset now)
    {
        var resources = canonical.Targets
            .Select(target =>
                RecordsPurgeCanonicalization.ResourceKey(
                    canonical.ClusterId,
                    target.TopicName,
                    target.Partition))
            .ToArray();

        var authorization = canonical.Targets
            .Select(target => target.TopicName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(topic => topic, StringComparer.Ordinal)
            .Select(topic => new MutationAuthorizationTarget(
                AuthorizationAction.RecordsPurge,
                canonical.ClusterId,
                topic))
            .ToArray();

        var intent = new MutationIntentDescriptor(
            MutationOperationKind.RecordsPurge,
            canonical.ClusterId,
            RecordsPurgeCanonicalization.Serialize(canonical),
            resources,
            AuthorizationTargets: authorization);

        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                MutationOperationKind.RecordsPurge,
                canonical.Targets.Count));

        return MutationOperation.CreatePreview(
                "oidc:https://idp.example|tester",
                intent,
                risk,
                "w38-test",
                now.AddMinutes(5),
                now,
                $"purge-{Guid.NewGuid():N}")
            .Snapshot;
    }

    private static RecordsPurgePartitionObservation Observation(
        string topic,
        int partition,
        long low,
        long high,
        long? timestampOffset) =>
        new(
            topic,
            partition,
            low,
            high,
            timestampOffset);

    private sealed class FakeObservationPort :
        IRecordsPurgeObservationPort
    {
        public FakeObservationPort(
            IReadOnlyList<RecordsPurgePartitionObservation> current)
        {
            Current = current;
        }

        public IReadOnlyList<RecordsPurgePartitionObservation> Current { get; set; }
        public IReadOnlyList<RecordsPurgeObservationTarget>? LastTargets { get; private set; }

        public Task<KafkaResult<IReadOnlyList<RecordsPurgePartitionObservation>>>
            ObserveAsync(
                string clusterId,
                IReadOnlyList<RecordsPurgeObservationTarget> targets,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            LastTargets = targets;
            var now = DateTimeOffset.UtcNow;

            return Task.FromResult(
                KafkaResult<IReadOnlyList<RecordsPurgePartitionObservation>>
                    .Success(
                        Current,
                        new ObservationMetadata(
                            now,
                            now,
                            now,
                            ObservationSource.Live)));
        }
    }

    private sealed class FakePurgePort :
        IRecordsPurgeMutationPort
    {
        public int Calls { get; private set; }

        public MutationProviderResult Result { get; set; } =
            new(
                MutationExecutionResultKind.AppliedUnverified,
                "records_purge_accepted");

        public Task<MutationProviderResult> PurgeAsync(
            RecordsPurgeMutation request,
            CancellationToken cancellationToken = default)
        {
            Calls++;
            return Task.FromResult(Result);
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
