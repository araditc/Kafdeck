using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W65DataQualityRuntimeEvaluatorTests
{
    [Fact]
    public async Task Evaluator_applies_closed_rules_and_emits_aggregate_only_evidence()
    {
        var now =
            DateTimeOffset.UtcNow;
        var policy =
            new DataQualityPolicyDefinition(
                "orders-quality",
                1,
                new DataQualityPolicyScope(
                    "prod",
                    "orders",
                    [0]),
                [
                    new DataQualityRule(
                        "required-id",
                        DataQualityRuleKind.RequiredPath,
                        "/id"),
                    new DataQualityRule(
                        "non-null-name",
                        DataQualityRuleKind.NullForbidden,
                        "/name"),
                    new DataQualityRule(
                        "amount-range",
                        DataQualityRuleKind.NumericRange,
                        "/amount",
                        minimumNumber: 0,
                        maximumNumber: 100),
                    new DataQualityRule(
                        "name-length",
                        DataQualityRuleKind.StringLengthRange,
                        "/name",
                        minimumLength: 2,
                        maximumLength: 8),
                ]);

        var input =
            Input(
                now,
                [
                    Record(
                        0,
                        """{"id":1,"name":"alice","amount":50}"""),
                    Record(
                        1,
                        """{"id":2,"name":null,"amount":150}"""),
                ]);

        var evaluator =
            new BoundedDataQualityBatchEvaluator(
                new JsonPassThroughDecoder());

        var result =
            await evaluator.EvaluateAsync(
                policy,
                input,
                new KafkaOperationContext(
                    now.AddMinutes(1)),
                CancellationToken.None);

        Assert.Equal(
            DataQualityEvidenceState.Available,
            result.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.Complete,
            result.Progress.Outcome);
        Assert.Equal(
            2,
            result.Evidence.EvaluatedRecords);
        Assert.Equal(
            1,
            result.Evidence.ViolationCount);
        Assert.Equal(
            input.EndOffsetExclusive,
            result.Progress.NextOffset);

        var counts =
            result.Evidence.ViolationsByRule
                .ToDictionary(
                    item => item.RuleId,
                    item => item.Count,
                    StringComparer.Ordinal);

        Assert.Equal(
            0,
            counts["required-id"]);
        Assert.Equal(
            1,
            counts["non-null-name"]);
        Assert.Equal(
            1,
            counts["amount-range"]);
        Assert.Equal(
            1,
            counts["name-length"]);

        var evidenceProperties =
            result.Evidence
                .GetType()
                .GetProperties();

        Assert.DoesNotContain(
            evidenceProperties,
            property =>
                property.Name.Contains(
                    "Payload",
                    StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains(
                    "Value",
                    StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains(
                    "Header",
                    StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Decode_failure_before_any_evidence_is_unavailable_with_zero_counters()
    {
        var now =
            DateTimeOffset.UtcNow;
        var policy =
            Policy();
        var evaluator =
            new BoundedDataQualityBatchEvaluator(
                new AlwaysFailDecoder(
                    RecordSchemaFailureCategory
                        .RegistryNotConfigured));

        var result =
            await evaluator.EvaluateAsync(
                policy,
                Input(
                    now,
                    [
                        Record(
                            0,
                            """{"id":1}"""),
                    ]),
                new KafkaOperationContext(
                    now.AddMinutes(1)),
                CancellationToken.None);

        Assert.Equal(
            DataQualityEvidenceState.Unavailable,
            result.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.SourceUnavailable,
            result.Progress.Outcome);
        Assert.Equal(
            0,
            result.Evidence.EvaluatedRecords);
        Assert.Equal(
            0,
            result.Evidence.ViolationCount);
        Assert.All(
            result.Evidence.ViolationsByRule,
            item =>
                Assert.Equal(
                    0,
                    item.Count));
    }

    [Fact]
    public async Task Decode_failure_after_real_evidence_is_partial_without_fabricating_failed_record_counts()
    {
        var now =
            DateTimeOffset.UtcNow;
        var policy =
            Policy();
        var evaluator =
            new BoundedDataQualityBatchEvaluator(
                new FailAfterDecoder(
                    successfulDecodes: 1));

        var result =
            await evaluator.EvaluateAsync(
                policy,
                Input(
                    now,
                    [
                        Record(
                            0,
                            """{"id":1}"""),
                        Record(
                            1,
                            """{"id":2}"""),
                    ]),
                new KafkaOperationContext(
                    now.AddMinutes(1)),
                CancellationToken.None);

        Assert.Equal(
            DataQualityEvidenceState.Partial,
            result.Evidence.State);
        Assert.Equal(
            DataQualityEvaluationOutcome.SourceUnavailable,
            result.Progress.Outcome);
        Assert.Equal(
            1,
            result.Evidence.EvaluatedRecords);
        Assert.Equal(
            0,
            result.Evidence.ViolationCount);
        Assert.Equal(
            1,
            result.Progress.NextOffset);
    }

    [Fact]
    public async Task Evaluator_rejects_input_outside_exact_policy_scope()
    {
        var now =
            DateTimeOffset.UtcNow;
        var evaluator =
            new BoundedDataQualityBatchEvaluator(
                new JsonPassThroughDecoder());

        await Assert.ThrowsAsync<ArgumentException>(
            () => evaluator.EvaluateAsync(
                Policy(),
                new DataQualityEvaluationInput(
                    "prod",
                    "other-topic",
                    0,
                    now.AddSeconds(-10),
                    now,
                    0,
                    1,
                    [
                        Record(
                            0,
                            """{"id":1}"""),
                    ],
                    new DataQualityEvaluationCycleBudget()),
                new KafkaOperationContext(
                    now.AddMinutes(1)),
                CancellationToken.None));
    }

    [Fact]
    public async Task Null_tombstone_is_evaluated_without_decode_or_payload_persistence()
    {
        var now =
            DateTimeOffset.UtcNow;
        var decoder =
            new CountingDecoder();
        var evaluator =
            new BoundedDataQualityBatchEvaluator(
                decoder);

        var result =
            await evaluator.EvaluateAsync(
                Policy(),
                new DataQualityEvaluationInput(
                    "prod",
                    "orders",
                    0,
                    now.AddSeconds(-10),
                    now,
                    0,
                    1,
                    [
                        new KafkaRawRecord(
                            0,
                            now,
                            null,
                            null,
                            Array.Empty<KafkaRecordHeader>()),
                    ],
                    new DataQualityEvaluationCycleBudget()),
                new KafkaOperationContext(
                    now.AddMinutes(1)),
                CancellationToken.None);

        Assert.Equal(
            0,
            decoder.Calls);
        Assert.Equal(
            1,
            result.Evidence.EvaluatedRecords);
        Assert.Equal(
            1,
            result.Evidence.ViolationCount);
    }

    private static DataQualityPolicyDefinition Policy() =>
        new(
            "orders-quality",
            1,
            new DataQualityPolicyScope(
                "prod",
                "orders",
                [0]),
            [
                new DataQualityRule(
                    "required-id",
                    DataQualityRuleKind.RequiredPath,
                    "/id"),
            ]);

    private static DataQualityEvaluationInput Input(
        DateTimeOffset now,
        IReadOnlyList<KafkaRawRecord> records) =>
        new(
            "prod",
            "orders",
            0,
            now.AddSeconds(-10),
            now,
            0,
            records.Count,
            records,
            new DataQualityEvaluationCycleBudget());

    private static KafkaRawRecord Record(
        long offset,
        string json) =>
        new(
            offset,
            DateTimeOffset.UtcNow,
            null,
            Encoding.UTF8.GetBytes(
                json),
            Array.Empty<KafkaRecordHeader>());

    private sealed class JsonPassThroughDecoder :
        IRecordDecodePort
    {
        public Task<RecordSchemaResult<RecordDecodedValue>>
            DecodeAsync(
                RecordDecodeRequest request,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            using var document =
                JsonDocument.Parse(
                    request.Payload);
            return Task.FromResult(
                RecordSchemaResult<RecordDecodedValue>
                    .Success(
                        new RecordDecodedValue(
                            1,
                            RecordSchemaFormat.JsonSchema,
                            document.RootElement.Clone())));
        }
    }

    private sealed class AlwaysFailDecoder :
        IRecordDecodePort
    {
        private readonly RecordSchemaFailureCategory _category;

        public AlwaysFailDecoder(
            RecordSchemaFailureCategory category)
        {
            _category =
                category;
        }

        public Task<RecordSchemaResult<RecordDecodedValue>>
            DecodeAsync(
                RecordDecodeRequest request,
                KafkaOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                RecordSchemaResult<RecordDecodedValue>
                    .Failed(
                        new RecordSchemaFailure(
                            _category,
                            "test_decode_failure",
                            "Decode unavailable.",
                            true)));
    }

    private sealed class FailAfterDecoder :
        IRecordDecodePort
    {
        private readonly int _successfulDecodes;
        private int _calls;

        public FailAfterDecoder(
            int successfulDecodes)
        {
            _successfulDecodes =
                successfulDecodes;
        }

        public Task<RecordSchemaResult<RecordDecodedValue>>
            DecodeAsync(
                RecordDecodeRequest request,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            if (_calls++ >=
                _successfulDecodes)
            {
                return Task.FromResult(
                    RecordSchemaResult<RecordDecodedValue>
                        .Failed(
                            new RecordSchemaFailure(
                                RecordSchemaFailureCategory
                                    .Unavailable,
                                "test_decode_failure",
                                "Decode unavailable.",
                                true)));
            }

            using var document =
                JsonDocument.Parse(
                    request.Payload);
            return Task.FromResult(
                RecordSchemaResult<RecordDecodedValue>
                    .Success(
                        new RecordDecodedValue(
                            1,
                            RecordSchemaFormat.JsonSchema,
                            document.RootElement.Clone())));
        }
    }

    private sealed class CountingDecoder :
        IRecordDecodePort
    {
        public int Calls { get; private set; }

        public Task<RecordSchemaResult<RecordDecodedValue>>
            DecodeAsync(
                RecordDecodeRequest request,
                KafkaOperationContext operation,
                CancellationToken cancellationToken)
        {
            Calls++;
            throw new InvalidOperationException(
                "Tombstone evaluation must not invoke the decoder.");
        }
    }
}
