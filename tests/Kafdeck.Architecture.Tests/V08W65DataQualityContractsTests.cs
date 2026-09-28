using Kafdeck.Core.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W65DataQualityContractsTests
{
    [Fact]
    public void Closed_rule_grammar_rejects_unrelated_operands()
    {
        Assert.Throws<ArgumentException>(
            () => new DataQualityRule(
                "required-customer-id",
                DataQualityRuleKind.RequiredPath,
                "/customer/id",
                expectedType: DataQualityValueType.String));
    }

    [Fact]
    public void Numeric_range_requires_finite_ordered_bounds()
    {
        Assert.Throws<ArgumentException>(
            () => new DataQualityRule(
                "amount-range",
                DataQualityRuleKind.NumericRange,
                "/amount",
                minimumNumber: 100,
                maximumNumber: 10));

        Assert.Throws<ArgumentException>(
            () => new DataQualityRule(
                "amount-range",
                DataQualityRuleKind.NumericRange,
                "/amount",
                minimumNumber: double.NaN));
    }

    [Fact]
    public void Policy_scope_requires_explicit_bounded_partitions()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityPolicyScope(
                "prod",
                "orders",
                Array.Empty<int>()));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityPolicyScope(
                "prod",
                "orders",
                [-1]));
    }

    [Theory]
    [InlineData(" orders")]
    [InlineData("orders ")]
    [InlineData("orders/bad")]
    [InlineData(".")]
    [InlineData("..")]
    public void Policy_scope_rejects_non_kafka_identifiers(
        string topicName)
    {
        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyScope(
                "prod",
                topicName,
                [0]));
    }

    [Fact]
    public void Policy_scope_rejects_unbounded_or_control_cluster_id()
    {
        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyScope(
                new string('c', 257),
                "orders",
                [0]));

        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyScope(
                "prod\nwest",
                "orders",
                [0]));
    }

    [Fact]
    public void Policy_scope_rejects_unbounded_duplicate_partition_input()
    {
        var partitions =
            Enumerable.Repeat(
                    0,
                    DataQualityPolicyScope.MaxPartitions + 1)
                .ToArray();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityPolicyScope(
                "prod",
                "orders",
                partitions));
    }

    [Fact]
    public void Policy_budget_enforces_admitted_hard_caps()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityPolicyBudget(
                recordsPerSecond:
                    DataQualityPolicyBudget.HardMaxRecordsPerSecond + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityPolicyBudget(
                bytesPerSecond:
                    DataQualityPolicyBudget.HardMaxBytesPerSecond + 1));

        Assert.Throws<ArgumentOutOfRangeException>(
            () => new DataQualityPolicyBudget(
                evaluationWindow:
                    DataQualityPolicyBudget.HardMaxEvaluationWindow +
                    TimeSpan.FromSeconds(1)));

        Assert.Equal(
            0,
            DataQualityPolicyBudget.DurableRawPayloadExemplars);
    }

    [Fact]
    public void Policy_rejects_duplicate_rule_identity()
    {
        var scope =
            new DataQualityPolicyScope(
                "prod",
                "orders",
                [0]);
        var rule =
            new DataQualityRule(
                "required-customer-id",
                DataQualityRuleKind.RequiredPath,
                "/customer/id");

        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyDefinition(
                "orders-quality",
                1,
                scope,
                [rule, rule]));
    }

    [Fact]
    public void Unknown_evidence_cannot_fabricate_counts()
    {
        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                DateTimeOffset.UtcNow.AddMinutes(-5),
                DateTimeOffset.UtcNow,
                evaluatedRecords: 1,
                evaluatedBytes: 10,
                violationCount: 0,
                violationsByRule:
                    Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Unknown,
                "record-monitor"));
    }

    [Fact]
    public void Evidence_contract_rejects_unbounded_or_inconsistent_breakdown()
    {
        var start =
            new DateTimeOffset(
                2026,
                9,
                28,
                12,
                0,
                0,
                TimeSpan.Zero);

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                start +
                    DataQualityPolicyBudget.HardMaxEvaluationWindow +
                    TimeSpan.FromSeconds(1),
                0,
                0,
                0,
                Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Available,
                "record-monitor"));

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                start.AddMinutes(5),
                evaluatedRecords: 1,
                evaluatedBytes: 10,
                violationCount: 1,
                violationsByRule:
                [
                    new DataQualityRuleViolationCount(
                        "required-customer-id",
                        2),
                ],
                DataQualityEvidenceState.Available,
                "record-monitor"));

        var tooMany =
            Enumerable.Range(
                    0,
                    DataQualityRule.MaxRulesPerPolicy + 1)
                .Select(
                    index =>
                        new DataQualityRuleViolationCount(
                            $"rule-{index}",
                            0))
                .ToArray();

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                start.AddMinutes(5),
                0,
                0,
                0,
                tooMany,
                DataQualityEvidenceState.Available,
                "record-monitor"));

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                new string(
                    'p',
                    DataQualityPolicyDefinition.MaxPolicyIdLength + 1),
                1,
                start,
                start.AddMinutes(5),
                0,
                0,
                0,
                Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Available,
                "record-monitor"));
    }

    [Fact]
    public void Evidence_totals_cannot_exceed_hard_throughput_budget()
    {
        var start =
            DateTimeOffset.UtcNow.AddMinutes(-5);
        var end =
            start.AddMinutes(5);

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                end,
                evaluatedRecords: long.MaxValue,
                evaluatedBytes: 0,
                violationCount: 0,
                violationsByRule:
                    Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Available,
                "record-monitor"));

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                end,
                evaluatedRecords: 0,
                evaluatedBytes: long.MaxValue,
                violationCount: 0,
                violationsByRule:
                    Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Available,
                "record-monitor"));
    }

    [Fact]
    public void Raw_string_bounds_are_checked_before_normalization()
    {
        Assert.Throws<ArgumentException>(
            () => new DataQualityRule(
                new string(
                    ' ',
                    DataQualityRule.MaxRuleIdLength + 1),
                DataQualityRuleKind.RequiredPath,
                "/customer/id"));

        var scope =
            new DataQualityPolicyScope(
                "prod",
                "orders",
                [0]);
        var rule =
            new DataQualityRule(
                "required-customer-id",
                DataQualityRuleKind.RequiredPath,
                "/customer/id");

        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyDefinition(
                new string(
                    ' ',
                    DataQualityPolicyDefinition.MaxPolicyIdLength + 1),
                1,
                scope,
                [rule]));

        var start =
            DateTimeOffset.UtcNow;
        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                start.AddMinutes(1),
                0,
                0,
                0,
                Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Available,
                new string(
                    ' ',
                    DataQualityAggregateEvidence.MaxSourceLength + 1)));
    }

    [Fact]
    public void Policy_rule_count_is_checked_before_enumeration()
    {
        var scope =
            new DataQualityPolicyScope(
                "prod",
                "orders",
                [0]);
        var oversized =
            new CountOnlyReadOnlyList<DataQualityRule>(
                DataQualityRule.MaxRulesPerPolicy + 1);

        Assert.Throws<ArgumentException>(
            () => new DataQualityPolicyDefinition(
                "orders-quality",
                1,
                scope,
                oversized));
    }

    [Fact]
    public void Evidence_violation_count_is_checked_before_enumeration()
    {
        var oversized =
            new CountOnlyReadOnlyList<DataQualityRuleViolationCount>(
                DataQualityRule.MaxRulesPerPolicy + 1);
        var start =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                start.AddMinutes(1),
                0,
                0,
                0,
                oversized,
                DataQualityEvidenceState.Available,
                "record-monitor"));
    }

    [Fact]
    public void Evidence_throughput_cap_uses_exact_fractional_duration()
    {
        var start =
            DateTimeOffset.UtcNow;

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                start.AddMilliseconds(1001),
                evaluatedRecords: 1002,
                evaluatedBytes: 0,
                violationCount: 0,
                violationsByRule:
                    Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Available,
                "record-monitor"));

        var admitted =
            new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                start.AddMilliseconds(500),
                evaluatedRecords: 500,
                evaluatedBytes: 0,
                violationCount: 0,
                violationsByRule:
                    Array.Empty<DataQualityRuleViolationCount>(),
                DataQualityEvidenceState.Available,
                "record-monitor");

        Assert.Equal(
            500,
            admitted.EvaluatedRecords);
    }

    [Fact]
    public void String_length_range_caps_both_operands()
    {
        Assert.Throws<ArgumentException>(
            () => new DataQualityRule(
                "name-length",
                DataQualityRuleKind.StringLengthRange,
                "/name",
                minimumLength: 1_000_001));
    }

    [Fact]
    public void Evidence_rule_count_cannot_exceed_aggregate_violations()
    {
        var start =
            new DateTimeOffset(
                2026,
                9,
                28,
                12,
                0,
                0,
                TimeSpan.Zero);

        Assert.Throws<ArgumentException>(
            () => new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                start,
                start.AddMinutes(5),
                evaluatedRecords: 10,
                evaluatedBytes: 100,
                violationCount: 1,
                violationsByRule:
                [
                    new DataQualityRuleViolationCount(
                        "required-customer-id",
                        2),
                ],
                DataQualityEvidenceState.Available,
                "record-monitor"));
    }

    [Fact]
    public void Aggregate_evidence_contains_counts_only()
    {
        var evidence =
            new DataQualityAggregateEvidence(
                "orders-quality",
                1,
                new DateTimeOffset(
                    2026, 9, 28, 12, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(
                    2026, 9, 28, 12, 5, 0, TimeSpan.Zero),
                evaluatedRecords: 100,
                evaluatedBytes: 4096,
                violationCount: 3,
                violationsByRule:
                [
                    new DataQualityRuleViolationCount(
                        "required-customer-id",
                        3),
                ],
                DataQualityEvidenceState.Available,
                "record-monitor");

        Assert.Equal(100, evidence.EvaluatedRecords);
        Assert.Equal(3, evidence.ViolationCount);

        var evidenceType = typeof(DataQualityAggregateEvidence);
        Assert.DoesNotContain(
            evidenceType.GetProperties(),
            property =>
                property.PropertyType == typeof(KafkaRawRecord) ||
                property.Name.Contains(
                    "Payload",
                    StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains(
                    "Value",
                    StringComparison.OrdinalIgnoreCase));
    }
    private sealed class CountOnlyReadOnlyList<T> :
        IReadOnlyList<T>
    {
        public CountOnlyReadOnlyList(int count)
        {
            Count = count;
        }

        public int Count { get; }

        public T this[int index] =>
            throw new InvalidOperationException(
                "Oversized input must be rejected before enumeration.");

        public IEnumerator<T> GetEnumerator() =>
            throw new InvalidOperationException(
                "Oversized input must be rejected before enumeration.");

        System.Collections.IEnumerator
            System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();
    }
}
