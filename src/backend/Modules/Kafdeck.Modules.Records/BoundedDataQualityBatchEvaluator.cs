using System.Globalization;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;

namespace Kafdeck.Modules.Records;

public sealed class BoundedDataQualityBatchEvaluator :
    IDataQualityBatchEvaluator
{
    private const string EvidenceSource =
        "bounded-record-monitor";

    private readonly IRecordDecodePort _decoder;
    private readonly TimeProvider _timeProvider;

    public BoundedDataQualityBatchEvaluator(
        IRecordDecodePort decoder,
        TimeProvider? timeProvider = null)
    {
        _decoder =
            decoder ??
            throw new ArgumentNullException(
                nameof(decoder));
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
    }

    public async Task<DataQualityEvaluationResult> EvaluateAsync(
        DataQualityPolicyDefinition policy,
        DataQualityEvaluationInput input,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(input);

        EnsureScope(policy, input);

        var startedAt =
            _timeProvider.GetUtcNow();
        var cycleDeadline =
            startedAt +
            input.Budget.MaxDuration;
        var deadline =
            operation.DeadlineUtc <= cycleDeadline
                ? operation.DeadlineUtc
                : cycleDeadline;

        var violationCounts =
            policy.Rules.ToDictionary(
                rule => rule.RuleId,
                _ => 0L,
                StringComparer.Ordinal);

        long evaluatedRecords = 0;
        long evaluatedBytes = 0;
        long violatingRecords = 0;
        var nextOffset =
            input.StartOffset;

        foreach (var record in input.Records)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return BuildInterrupted(
                    policy,
                    input,
                    violationCounts,
                    evaluatedRecords,
                    evaluatedBytes,
                    violatingRecords,
                    nextOffset,
                    DataQualityEvaluationOutcome.Cancelled);
            }

            if (_timeProvider.GetUtcNow() >= deadline)
            {
                return BuildInterrupted(
                    policy,
                    input,
                    violationCounts,
                    evaluatedRecords,
                    evaluatedBytes,
                    violatingRecords,
                    nextOffset,
                    DataQualityEvaluationOutcome.DurationLimit);
            }

            if (evaluatedRecords >=
                input.Budget.MaxRecords)
            {
                return BuildPartial(
                    policy,
                    input,
                    violationCounts,
                    evaluatedRecords,
                    evaluatedBytes,
                    violatingRecords,
                    nextOffset,
                    DataQualityEvaluationOutcome.RecordLimit);
            }

            var recordBytes =
                RawByteCount(record);
            if (evaluatedBytes >
                input.Budget.MaxRawBytes -
                recordBytes)
            {
                return BuildPartial(
                    policy,
                    input,
                    violationCounts,
                    evaluatedRecords,
                    evaluatedBytes,
                    violatingRecords,
                    nextOffset,
                    DataQualityEvaluationOutcome.ByteLimit);
            }

            JsonElement structured;
            if (record.Value is null)
            {
                using var nullDocument =
                    JsonDocument.Parse("null");
                structured =
                    nullDocument.RootElement.Clone();
            }
            else
            {
                var decoded =
                    await _decoder.DecodeAsync(
                            new RecordDecodeRequest(
                                input.ClusterId,
                                input.TopicName,
                                input.Partition,
                                record.Offset,
                                isKey: false,
                                record.Value.Value),
                            new KafkaOperationContext(
                                deadline),
                            cancellationToken)
                        .ConfigureAwait(false);

                if (!decoded.IsSuccess ||
                    decoded.Value is null)
                {
                    return BuildDecodeFailure(
                        policy,
                        input,
                        violationCounts,
                        evaluatedRecords,
                        evaluatedBytes,
                        violatingRecords,
                        nextOffset,
                        decoded.Failure);
                }

                structured =
                    decoded.Value.StructuredValue;
            }

            var recordViolated = false;
            foreach (var rule in policy.Rules)
            {
                if (!Passes(
                        rule,
                        structured))
                {
                    violationCounts[rule.RuleId] =
                        checked(
                            violationCounts[rule.RuleId] +
                            1);
                    recordViolated =
                        true;
                }
            }

            evaluatedRecords =
                checked(
                    evaluatedRecords +
                    1);
            evaluatedBytes =
                checked(
                    evaluatedBytes +
                    recordBytes);

            if (recordViolated)
            {
                violatingRecords =
                    checked(
                        violatingRecords +
                        1);
            }

            nextOffset =
                checked(
                    record.Offset +
                    1);
        }

        return Build(
            policy,
            input,
            violationCounts,
            evaluatedRecords,
            evaluatedBytes,
            violatingRecords,
            input.EndOffsetExclusive,
            DataQualityEvidenceState.Available,
            DataQualityEvaluationOutcome.Complete);
    }

    private static void EnsureScope(
        DataQualityPolicyDefinition policy,
        DataQualityEvaluationInput input)
    {
        if (!string.Equals(
                policy.Scope.ClusterId,
                input.ClusterId,
                StringComparison.Ordinal) ||
            !string.Equals(
                policy.Scope.TopicName,
                input.TopicName,
                StringComparison.Ordinal) ||
            !policy.Scope.Partitions.Contains(
                input.Partition))
        {
            throw new ArgumentException(
                "Data-quality evaluator input must remain inside the exact policy scope.",
                nameof(input));
        }

        if (input.Budget.MaxRecords >
                policy.Budget.RecordsPerSecond *
                Math.Max(
                    1d,
                    input.Budget.MaxDuration.TotalSeconds) ||
            input.Budget.MaxRawBytes >
                policy.Budget.BytesPerSecond *
                Math.Max(
                    1d,
                    input.Budget.MaxDuration.TotalSeconds))
        {
            throw new ArgumentException(
                "Data-quality evaluator cycle budget exceeds the policy throughput budget.",
                nameof(input));
        }
    }

    private static DataQualityEvaluationResult
        BuildDecodeFailure(
            DataQualityPolicyDefinition policy,
            DataQualityEvaluationInput input,
            IReadOnlyDictionary<string, long> violationCounts,
            long evaluatedRecords,
            long evaluatedBytes,
            long violatingRecords,
            long nextOffset,
            RecordSchemaFailure? failure)
    {
        var outcome =
            failure?.Category switch
            {
                RecordSchemaFailureCategory.Cancelled =>
                    DataQualityEvaluationOutcome.Cancelled,
                RecordSchemaFailureCategory.Timeout =>
                    DataQualityEvaluationOutcome.DurationLimit,
                _ =>
                    DataQualityEvaluationOutcome.SourceUnavailable,
            };

        return evaluatedRecords > 0
            ? BuildPartial(
                policy,
                input,
                violationCounts,
                evaluatedRecords,
                evaluatedBytes,
                violatingRecords,
                nextOffset,
                outcome)
            : Build(
                policy,
                input,
                ZeroCounts(policy),
                0,
                0,
                0,
                input.StartOffset,
                DataQualityEvidenceState.Unavailable,
                outcome);
    }

    private static DataQualityEvaluationResult
        BuildInterrupted(
            DataQualityPolicyDefinition policy,
            DataQualityEvaluationInput input,
            IReadOnlyDictionary<string, long> violationCounts,
            long evaluatedRecords,
            long evaluatedBytes,
            long violatingRecords,
            long nextOffset,
            DataQualityEvaluationOutcome outcome) =>
        evaluatedRecords > 0
            ? BuildPartial(
                policy,
                input,
                violationCounts,
                evaluatedRecords,
                evaluatedBytes,
                violatingRecords,
                nextOffset,
                outcome)
            : Build(
                policy,
                input,
                ZeroCounts(policy),
                0,
                0,
                0,
                input.StartOffset,
                DataQualityEvidenceState.Unavailable,
                outcome);

    private static DataQualityEvaluationResult BuildPartial(
        DataQualityPolicyDefinition policy,
        DataQualityEvaluationInput input,
        IReadOnlyDictionary<string, long> violationCounts,
        long evaluatedRecords,
        long evaluatedBytes,
        long violatingRecords,
        long nextOffset,
        DataQualityEvaluationOutcome outcome) =>
        Build(
            policy,
            input,
            violationCounts,
            evaluatedRecords,
            evaluatedBytes,
            violatingRecords,
            nextOffset,
            DataQualityEvidenceState.Partial,
            outcome);

    private static DataQualityEvaluationResult Build(
        DataQualityPolicyDefinition policy,
        DataQualityEvaluationInput input,
        IReadOnlyDictionary<string, long> violationCounts,
        long evaluatedRecords,
        long evaluatedBytes,
        long violatingRecords,
        long nextOffset,
        DataQualityEvidenceState state,
        DataQualityEvaluationOutcome outcome)
    {
        var perRule =
            policy.Rules
                .Select(
                    rule =>
                        new DataQualityRuleViolationCount(
                            rule.RuleId,
                            violationCounts[
                                rule.RuleId]))
                .ToArray();

        var evidence =
            new DataQualityAggregateEvidence(
                policy.PolicyId,
                policy.Version,
                input.WindowStartUtc,
                input.WindowEndUtc,
                evaluatedRecords,
                evaluatedBytes,
                violatingRecords,
                Array.AsReadOnly(
                    perRule),
                state,
                EvidenceSource);

        var progress =
            new DataQualityEvaluationProgress(
                policy.PolicyId,
                policy.Version,
                input.ClusterId,
                input.TopicName,
                input.Partition,
                input.WindowStartUtc,
                input.WindowEndUtc,
                input.StartOffset,
                input.EndOffsetExclusive,
                nextOffset,
                evaluatedRecords,
                evaluatedBytes,
                state,
                outcome,
                DateTimeOffset.UtcNow);

        return new DataQualityEvaluationResult(
            evidence,
            progress);
    }

    private static IReadOnlyDictionary<string, long>
        ZeroCounts(
            DataQualityPolicyDefinition policy) =>
        policy.Rules.ToDictionary(
            rule => rule.RuleId,
            _ => 0L,
            StringComparer.Ordinal);

    private static long RawByteCount(
        KafkaRawRecord record)
    {
        long total =
            (record.Key?.Length ?? 0) +
            (record.Value?.Length ?? 0);

        foreach (var header in record.Headers)
        {
            total =
                checked(
                    total +
                    Encoding.UTF8.GetByteCount(
                        header.Name) +
                    header.Value.Length);
        }

        return total;
    }

    private static bool Passes(
        DataQualityRule rule,
        JsonElement root)
    {
        var found =
            TryResolve(
                root,
                rule.JsonPointer,
                out var value);

        return rule.Kind switch
        {
            DataQualityRuleKind.RequiredPath =>
                found,

            DataQualityRuleKind.NullForbidden =>
                found &&
                value.ValueKind !=
                    JsonValueKind.Null,

            DataQualityRuleKind.ValueType =>
                found &&
                MatchesType(
                    value,
                    rule.ExpectedType!.Value),

            DataQualityRuleKind.NumericRange =>
                found &&
                TryFiniteNumber(
                    value,
                    out var number) &&
                (rule.MinimumNumber is null ||
                 number >=
                    rule.MinimumNumber.Value) &&
                (rule.MaximumNumber is null ||
                 number <=
                    rule.MaximumNumber.Value),

            DataQualityRuleKind.StringLengthRange =>
                found &&
                value.ValueKind ==
                    JsonValueKind.String &&
                LengthInRange(
                    value.GetString()?.Length ?? 0,
                    rule.MinimumLength,
                    rule.MaximumLength),

            _ =>
                throw new ArgumentOutOfRangeException(
                    nameof(rule)),
        };
    }

    private static bool MatchesType(
        JsonElement value,
        DataQualityValueType expected) =>
        expected switch
        {
            DataQualityValueType.String =>
                value.ValueKind ==
                    JsonValueKind.String,
            DataQualityValueType.Number =>
                value.ValueKind ==
                    JsonValueKind.Number &&
                TryFiniteNumber(
                    value,
                    out _),
            DataQualityValueType.Boolean =>
                value.ValueKind is
                    JsonValueKind.True or
                    JsonValueKind.False,
            DataQualityValueType.Object =>
                value.ValueKind ==
                    JsonValueKind.Object,
            DataQualityValueType.Array =>
                value.ValueKind ==
                    JsonValueKind.Array,
            _ =>
                false,
        };

    private static bool TryFiniteNumber(
        JsonElement value,
        out double number)
    {
        number = default;
        return value.ValueKind ==
                   JsonValueKind.Number &&
               value.TryGetDouble(
                   out number) &&
               double.IsFinite(
                   number);
    }

    private static bool LengthInRange(
        int length,
        int? minimum,
        int? maximum) =>
        (minimum is null ||
         length >= minimum.Value) &&
        (maximum is null ||
         length <= maximum.Value);

    private static bool TryResolve(
        JsonElement root,
        string pointer,
        out JsonElement value)
    {
        value =
            root;

        foreach (var rawSegment in
                 pointer.Split(
                     '/',
                     StringSplitOptions.None)
                     .Skip(1))
        {
            var segment =
                rawSegment
                    .Replace(
                        "~1",
                        "/",
                        StringComparison.Ordinal)
                    .Replace(
                        "~0",
                        "~",
                        StringComparison.Ordinal);

            if (value.ValueKind ==
                JsonValueKind.Object)
            {
                if (!value.TryGetProperty(
                        segment,
                        out value))
                {
                    return false;
                }

                continue;
            }

            if (value.ValueKind ==
                JsonValueKind.Array)
            {
                if (!int.TryParse(
                        segment,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture,
                        out var index) ||
                    index < 0 ||
                    index >= value.GetArrayLength())
                {
                    return false;
                }

                value =
                    value[index];
                continue;
            }

            return false;
        }

        return true;
    }
}
