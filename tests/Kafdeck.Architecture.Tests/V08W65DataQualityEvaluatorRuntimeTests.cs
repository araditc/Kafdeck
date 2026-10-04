using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W65DataQualityEvaluatorRuntimeTests
{
    [Fact]
    public async Task Evaluator_counts_records_once_and_rules_independently()
    {
        var now =
            new DateTimeOffset(
                2026, 10, 4, 12, 0, 0, TimeSpan.Zero);
        var policy =
            Policy(
                [
                    new DataQualityRule(
                        "customer-required",
                        DataQualityRuleKind.RequiredPath,
                        "/customer/id"),
                    new DataQualityRule(
                        "amount-range",
                        DataQualityRuleKind.NumericRange,
                        "/amount",
                        minimumNumber: 1,
                        maximumNumber: 100),
                ]);

        var input =
            Input(
                now,
                [
                    Record(0, """{"customer":{"id":"a"},"amount":10}"""),
                    Record(1, """{"customer":{},"amount":500}"""),
                    Record(2, """{"customer":{"id":"c"},"amount":500}"""),
                ],
                endOffsetExclusive: 3);

        var result =
            await new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now))
                .EvaluateAsync(
                    policy,
                    input,
                    new KafkaOperationContext(
                        now.AddSeconds(10)),
                    CancellationToken.None);

        Assert.Equal(
            DataQualityEvidenceState.Available,
            result.Evidence.State);
        Assert.Equal(
            3,
            result.Evidence.EvaluatedRecords);
        Assert.Equal(
            2,
            result.Evidence.ViolationCount);
        Assert.Equal(
            1,
            result.Evidence.ViolationsByRule
                .Single(item =>
                    item.RuleId ==
                    "customer-required")
                .Count);
        Assert.Equal(
            2,
            result.Evidence.ViolationsByRule
                .Single(item =>
                    item.RuleId ==
                    "amount-range")
                .Count);
        Assert.Equal(
            DataQualityEvaluationOutcome.Complete,
            result.Progress.Outcome);
        Assert.Equal(
            3,
            result.Progress.NextOffset);
    }

    [Fact]
    public async Task Invalid_json_violates_every_closed_rule_without_persisting_payload()
    {
        var now =
            DateTimeOffset.UtcNow;
        var policy =
            Policy(
                [
                    new DataQualityRule(
                        "required",
                        DataQualityRuleKind.RequiredPath,
                        "/id"),
                    new DataQualityRule(
                        "type",
                        DataQualityRuleKind.ValueType,
                        "/id",
                        expectedType:
                            DataQualityValueType.String),
                ]);
        var raw =
            "{not-json";

        var result =
            await new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now))
                .EvaluateAsync(
                    policy,
                    Input(
                        now,
                        [Record(0, raw)],
                        1),
                    new KafkaOperationContext(
                        now.AddSeconds(10)),
                    CancellationToken.None);

        Assert.Equal(
            1,
            result.Evidence.ViolationCount);
        Assert.All(
            result.Evidence.ViolationsByRule,
            item =>
                Assert.Equal(
                    1,
                    item.Count));

        Assert.DoesNotContain(
            result.Evidence
                .GetType()
                .GetProperties(),
            property =>
                property.Name.Contains(
                    "Payload",
                    StringComparison.OrdinalIgnoreCase) ||
                property.PropertyType ==
                    typeof(KafkaRawRecord));
    }

    [Fact]
    public async Task Missing_or_wrong_typed_values_fail_closed()
    {
        var now =
            DateTimeOffset.UtcNow;
        var policy =
            Policy(
                [
                    new DataQualityRule(
                        "null",
                        DataQualityRuleKind.NullForbidden,
                        "/name"),
                    new DataQualityRule(
                        "type",
                        DataQualityRuleKind.ValueType,
                        "/name",
                        expectedType:
                            DataQualityValueType.String),
                    new DataQualityRule(
                        "length",
                        DataQualityRuleKind.StringLengthRange,
                        "/name",
                        minimumLength: 2,
                        maximumLength: 4),
                ]);

        var result =
            await new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now))
                .EvaluateAsync(
                    policy,
                    Input(
                        now,
                        [
                            Record(0, """{"name":null}"""),
                            Record(1, """{"name":7}"""),
                            Record(2, """{"name":"okay"}"""),
                        ],
                        3),
                    new KafkaOperationContext(
                        now.AddSeconds(10)),
                    CancellationToken.None);

        Assert.Equal(
            2,
            result.Evidence.ViolationCount);
        Assert.Equal(
            1,
            Count(result, "null"));
        Assert.Equal(
            2,
            Count(result, "type"));
        Assert.Equal(
            2,
            Count(result, "length"));
    }

    [Fact]
    public async Task Json_pointer_escape_and_array_index_are_supported()
    {
        var now =
            DateTimeOffset.UtcNow;
        var policy =
            Policy(
                [
                    new DataQualityRule(
                        "escaped",
                        DataQualityRuleKind.RequiredPath,
                        "/a~1b/~0key/0"),
                ]);

        var result =
            await new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now))
                .EvaluateAsync(
                    policy,
                    Input(
                        now,
                        [
                            Record(
                                0,
                                """{"a/b":{"~key":[1]}}"""),
                        ],
                        1),
                    new KafkaOperationContext(
                        now.AddSeconds(10)),
                    CancellationToken.None);

        Assert.Equal(
            0,
            result.Evidence.ViolationCount);
    }

    [Fact]
    public async Task Partial_snapshot_never_claims_complete_progress()
    {
        var now =
            DateTimeOffset.UtcNow;
        var result =
            await new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now))
                .EvaluateAsync(
                    Policy(
                        [
                            new DataQualityRule(
                                "id",
                                DataQualityRuleKind.RequiredPath,
                                "/id"),
                        ]),
                    Input(
                        now,
                        [
                            Record(
                                10,
                                """{"id":"x"}"""),
                        ],
                        endOffsetExclusive: 20,
                        startOffset: 10),
                    new KafkaOperationContext(
                        now.AddSeconds(10)),
                    CancellationToken.None);

        Assert.Equal(
            DataQualityEvidenceState.Partial,
            result.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.RecordLimit,
            result.Progress.Outcome);
        Assert.Equal(
            11,
            result.Progress.NextOffset);
    }

    [Fact]
    public async Task Cancellation_before_evaluation_returns_unknown_zero_counts()
    {
        var now =
            DateTimeOffset.UtcNow;
        using var cts =
            new CancellationTokenSource();
        cts.Cancel();

        var result =
            await new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now))
                .EvaluateAsync(
                    Policy(
                        [
                            new DataQualityRule(
                                "id",
                                DataQualityRuleKind.RequiredPath,
                                "/id"),
                        ]),
                    Input(
                        now,
                        [
                            Record(
                                0,
                                """{"id":"x"}"""),
                        ],
                        1),
                    new KafkaOperationContext(
                        now.AddSeconds(10)),
                    cts.Token);

        Assert.Equal(
            DataQualityEvidenceState.Unknown,
            result.Evidence.State);
        Assert.Equal(
            0,
            result.Evidence.EvaluatedRecords);
        Assert.Equal(
            DataQualityEvaluationOutcome.Cancelled,
            result.Progress.Outcome);
        Assert.Equal(
            0,
            result.Progress.NextOffset);
    }

    [Fact]
    public async Task Evaluator_rejects_input_outside_policy_scope()
    {
        var now =
            DateTimeOffset.UtcNow;
        var evaluator =
            new BoundedDataQualityBatchEvaluator(
                new FixedTimeProvider(now));

        await Assert.ThrowsAsync<ArgumentException>(
            () => evaluator.EvaluateAsync(
                Policy(
                    [
                        new DataQualityRule(
                            "id",
                            DataQualityRuleKind.RequiredPath,
                            "/id"),
                    ]),
                new DataQualityEvaluationInput(
                    "prod",
                    "other-topic",
                    0,
                    now.AddMinutes(-1),
                    now,
                    0,
                    1,
                    [Record(0, """{"id":"x"}""")],
                    new DataQualityEvaluationCycleBudget()),
                new KafkaOperationContext(
                    now.AddSeconds(10)),
                CancellationToken.None));
    }

    private static long Count(
        DataQualityEvaluationResult result,
        string ruleId) =>
        result.Evidence.ViolationsByRule
            .Single(item =>
                item.RuleId == ruleId)
            .Count;

    private static DataQualityPolicyDefinition Policy(
        IReadOnlyList<DataQualityRule> rules) =>
        new(
            "orders-quality",
            1,
            new DataQualityPolicyScope(
                "prod",
                "orders",
                [0]),
            rules);

    private static DataQualityEvaluationInput Input(
        DateTimeOffset now,
        IReadOnlyList<KafkaRawRecord> records,
        long endOffsetExclusive,
        long startOffset = 0) =>
        new(
            "prod",
            "orders",
            0,
            now.AddMinutes(-1),
            now,
            startOffset,
            endOffsetExclusive,
            records,
            new DataQualityEvaluationCycleBudget());

    private static KafkaRawRecord Record(
        long offset,
        string json) =>
        new(
            offset,
            null,
            null,
            Encoding.UTF8.GetBytes(
                json),
            Array.Empty<KafkaRecordHeader>());


    [Fact]
    public async Task Policy_owned_window_and_rate_limits_are_enforced()
    {
        var now =
            DateTimeOffset.UtcNow;
        var evaluator =
            new BoundedDataQualityBatchEvaluator(
                new FixedTimeProvider(now));
        var restrictivePolicy =
            new DataQualityPolicyDefinition(
                "orders-quality",
                1,
                new DataQualityPolicyScope(
                    "prod",
                    "orders",
                    [0]),
                [
                    new DataQualityRule(
                        "id",
                        DataQualityRuleKind.RequiredPath,
                        "/id"),
                ],
                new DataQualityPolicyBudget(
                    recordsPerSecond: 1,
                    bytesPerSecond: 1024,
                    evaluationWindow:
                        TimeSpan.FromSeconds(2)));

        var overWindow =
            new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddSeconds(-3),
                now,
                0,
                1,
                [
                    Record(
                        0,
                        """{"id":"x"}"""),
                ],
                new DataQualityEvaluationCycleBudget());

        await Assert.ThrowsAsync<ArgumentException>(
            () => evaluator.EvaluateAsync(
                restrictivePolicy,
                overWindow,
                new KafkaOperationContext(
                    now.AddSeconds(10)),
                CancellationToken.None));

        var limitedInput =
            new DataQualityEvaluationInput(
                "prod",
                "orders",
                0,
                now.AddSeconds(-1),
                now,
                0,
                3,
                [
                    Record(
                        0,
                        """{"id":"a"}"""),
                    Record(
                        1,
                        """{"id":"b"}"""),
                    Record(
                        2,
                        """{"id":"c"}"""),
                ],
                new DataQualityEvaluationCycleBudget(
                    maxRecords: 10,
                    maxRawBytes:
                        RecordOperationBudget
                            .DefaultMaxRawBytes));

        var result =
            await evaluator.EvaluateAsync(
                restrictivePolicy,
                limitedInput,
                new KafkaOperationContext(
                    now.AddSeconds(10)),
                CancellationToken.None);

        Assert.Equal(
            1,
            result.Evidence.EvaluatedRecords);
        Assert.Equal(
            DataQualityEvidenceState.Partial,
            result.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.RecordLimit,
            result.Progress.Outcome);
        Assert.Equal(
            1,
            result.Progress.NextOffset);
    }

    [Fact]
    public async Task Deadline_crossed_during_record_does_not_commit_that_record()
    {
        var now =
            DateTimeOffset.UtcNow;
        var provider =
            new SequenceTimeProvider(
                now,
                now,
                now.AddSeconds(2),
                now.AddSeconds(2));

        var result =
            await new BoundedDataQualityBatchEvaluator(
                    provider)
                .EvaluateAsync(
                    Policy(
                        [
                            new DataQualityRule(
                                "id",
                                DataQualityRuleKind.RequiredPath,
                                "/id"),
                        ]),
                    Input(
                        now,
                        [
                            Record(
                                0,
                                """{"id":"x"}"""),
                        ],
                        1),
                    new KafkaOperationContext(
                        now.AddSeconds(1)),
                    CancellationToken.None);

        Assert.Equal(
            DataQualityEvidenceState.Unknown,
            result.Evidence.State);
        Assert.Equal(
            0,
            result.Evidence.EvaluatedRecords);
        Assert.Equal(
            DataQualityEvaluationOutcome.DurationLimit,
            result.Progress.Outcome);
        Assert.Equal(
            0,
            result.Progress.NextOffset);
    }

    [Fact]
    public async Task Json_pointer_rejects_leading_zero_array_index()
    {
        var now =
            DateTimeOffset.UtcNow;
        var result =
            await new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now))
                .EvaluateAsync(
                    Policy(
                        [
                            new DataQualityRule(
                                "pointer",
                                DataQualityRuleKind.RequiredPath,
                                "/items/01/id"),
                        ]),
                    Input(
                        now,
                        [
                            Record(
                                0,
                                """{"items":[{"id":"a"},{"id":"b"}]}"""),
                        ],
                        1),
                    new KafkaOperationContext(
                        now.AddSeconds(10)),
                    CancellationToken.None);

        Assert.Equal(
            1,
            result.Evidence.ViolationCount);
        Assert.Equal(
            1,
            Count(
                result,
                "pointer"));
    }

    [Fact]
    public async Task Json_pointer_rejects_invalid_escape_sequence()
    {
        var now =
            DateTimeOffset.UtcNow;
        var result =
            await new BoundedDataQualityBatchEvaluator(
                    new FixedTimeProvider(now))
                .EvaluateAsync(
                    Policy(
                        [
                            new DataQualityRule(
                                "pointer",
                                DataQualityRuleKind.RequiredPath,
                                "/a~2b"),
                        ]),
                    Input(
                        now,
                        [
                            Record(
                                0,
                                """{"a~2b":"present"}"""),
                        ],
                        1),
                    new KafkaOperationContext(
                        now.AddSeconds(10)),
                    CancellationToken.None);

        Assert.Equal(
            1,
            result.Evidence.ViolationCount);
        Assert.Equal(
            1,
            Count(
                result,
                "pointer"));
    }


    private sealed class SequenceTimeProvider :
        TimeProvider
    {
        private readonly Queue<DateTimeOffset>
            _values;
        private DateTimeOffset _last;

        public SequenceTimeProvider(
            params DateTimeOffset[] values)
        {
            _values =
                new Queue<DateTimeOffset>(
                    values);
            _last =
                values.Length == 0
                    ? DateTimeOffset.UtcNow
                    : values[^1];
        }

        public override DateTimeOffset GetUtcNow()
        {
            if (_values.Count > 0)
            {
                _last =
                    _values.Dequeue();
            }

            return _last;
        }
    }

    private sealed class FixedTimeProvider :
        TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(
            DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() =>
            _now;
    }
}
