using System.Globalization;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Core.Records;

public sealed class BoundedDataQualityBatchEvaluator :
    IDataQualityBatchEvaluator
{
    private const string EvidenceSource =
        "bounded-record-evaluator";

    private readonly TimeProvider _timeProvider;

    public BoundedDataQualityBatchEvaluator(
        TimeProvider? timeProvider = null)
    {
        _timeProvider =
            timeProvider ??
            TimeProvider.System;
    }

    public Task<DataQualityEvaluationResult> EvaluateAsync(
        DataQualityPolicyDefinition policy,
        DataQualityEvaluationInput input,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(input);

        ValidateScope(
            policy,
            input);

        var startedAt =
            _timeProvider.GetUtcNow();
        var evaluationDeadline =
            startedAt +
            input.Budget.MaxDuration;
        if (operation.DeadlineUtc <
            evaluationDeadline)
        {
            evaluationDeadline =
                operation.DeadlineUtc;
        }

        var counts =
            policy.Rules.ToDictionary(
                rule => rule.RuleId,
                _ => 0L,
                StringComparer.Ordinal);

        long evaluatedRecords = 0;
        long evaluatedBytes = 0;
        long violatedRecords = 0;
        var nextOffset =
            input.StartOffset;
        var outcome =
            DataQualityEvaluationOutcome.Complete;
        var state =
            DataQualityEvidenceState.Available;

        for (var index = 0;
             index < input.Records.Count;
             index++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                outcome =
                    DataQualityEvaluationOutcome.Cancelled;
                state =
                    evaluatedRecords == 0
                        ? DataQualityEvidenceState.Unknown
                        : DataQualityEvidenceState.Partial;
                break;
            }

            if (_timeProvider.GetUtcNow() >=
                evaluationDeadline)
            {
                outcome =
                    DataQualityEvaluationOutcome.DurationLimit;
                state =
                    evaluatedRecords == 0
                        ? DataQualityEvidenceState.Unknown
                        : DataQualityEvidenceState.Partial;
                break;
            }

            var record =
                input.Records[index];
            var rawBytes =
                RawBytes(record);

            if (evaluatedBytes >
                input.Budget.MaxRawBytes -
                rawBytes)
            {
                outcome =
                    DataQualityEvaluationOutcome.ByteLimit;
                state =
                    evaluatedRecords == 0
                        ? DataQualityEvidenceState.Unknown
                        : DataQualityEvidenceState.Partial;
                break;
            }

            var recordViolation =
                EvaluateRecord(
                    record,
                    policy.Rules,
                    counts);

            evaluatedRecords++;
            evaluatedBytes +=
                rawBytes;
            if (recordViolation)
            {
                violatedRecords++;
            }

            nextOffset =
                Math.Min(
                    input.EndOffsetExclusive,
                    checked(record.Offset + 1));
        }

        if (outcome ==
                DataQualityEvaluationOutcome.Complete &&
            nextOffset <
                input.EndOffsetExclusive)
        {
            outcome =
                DataQualityEvaluationOutcome.RecordLimit;
            state =
                evaluatedRecords == 0
                    ? DataQualityEvidenceState.Unknown
                    : DataQualityEvidenceState.Partial;
        }

        if (outcome ==
                DataQualityEvaluationOutcome.Complete)
        {
            nextOffset =
                input.EndOffsetExclusive;
        }

        var breakdown =
            policy.Rules
                .Select(rule =>
                    new DataQualityRuleViolationCount(
                        rule.RuleId,
                        counts[rule.RuleId]))
                .ToArray();

        if (state is
                DataQualityEvidenceState.Unknown or
                DataQualityEvidenceState.Unavailable)
        {
            evaluatedRecords = 0;
            evaluatedBytes = 0;
            violatedRecords = 0;
            breakdown =
                policy.Rules
                    .Select(rule =>
                        new DataQualityRuleViolationCount(
                            rule.RuleId,
                            0))
                    .ToArray();
            nextOffset =
                input.StartOffset;
        }

        var evidence =
            new DataQualityAggregateEvidence(
                policy.PolicyId,
                policy.Version,
                input.WindowStartUtc,
                input.WindowEndUtc,
                evaluatedRecords,
                evaluatedBytes,
                violatedRecords,
                Array.AsReadOnly(
                    breakdown),
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
                _timeProvider.GetUtcNow());

        return Task.FromResult(
            new DataQualityEvaluationResult(
                evidence,
                progress));
    }

    private static void ValidateScope(
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
                "Data-quality evaluation input is outside the policy scope.",
                nameof(input));
        }
    }

    private static bool EvaluateRecord(
        KafkaRawRecord record,
        IReadOnlyList<DataQualityRule> rules,
        IDictionary<string, long> counts)
    {
        JsonDocument? document = null;

        try
        {
            if (record.Value is { } value)
            {
                document =
                    JsonDocument.Parse(
                        value);
            }
        }
        catch (JsonException)
        {
            document = null;
        }

        using (document)
        {
            var root =
                document?.RootElement;
            var anyViolation =
                false;

            foreach (var rule in rules)
            {
                var violated =
                    root is null ||
                    !EvaluateRule(
                        root.Value,
                        rule);

                if (!violated)
                {
                    continue;
                }

                counts[rule.RuleId] =
                    checked(
                        counts[rule.RuleId] + 1);
                anyViolation =
                    true;
            }

            return anyViolation;
        }
    }

    private static bool EvaluateRule(
        JsonElement root,
        DataQualityRule rule)
    {
        var found =
            TryResolvePointer(
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
                NumericRangeMatches(
                    value,
                    rule.MinimumNumber,
                    rule.MaximumNumber),
            DataQualityRuleKind.StringLengthRange =>
                found &&
                StringLengthMatches(
                    value,
                    rule.MinimumLength,
                    rule.MaximumLength),
            _ => throw new ArgumentOutOfRangeException(
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
                JsonValueKind.Number,
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
            _ => false,
        };

    private static bool NumericRangeMatches(
        JsonElement value,
        double? minimum,
        double? maximum)
    {
        if (value.ValueKind !=
                JsonValueKind.Number ||
            !value.TryGetDouble(
                out var number) ||
            !double.IsFinite(number))
        {
            return false;
        }

        return (minimum is null ||
                number >= minimum.Value) &&
               (maximum is null ||
                number <= maximum.Value);
    }

    private static bool StringLengthMatches(
        JsonElement value,
        int? minimum,
        int? maximum)
    {
        if (value.ValueKind !=
            JsonValueKind.String)
        {
            return false;
        }

        var length =
            value.GetString()?.Length ?? 0;

        return (minimum is null ||
                length >= minimum.Value) &&
               (maximum is null ||
                length <= maximum.Value);
    }

    private static bool TryResolvePointer(
        JsonElement root,
        string pointer,
        out JsonElement value)
    {
        value = root;
        var segments =
            pointer.Split(
                '/',
                StringSplitOptions.None);

        for (var index = 1;
             index < segments.Length;
             index++)
        {
            var segment =
                DecodePointerSegment(
                    segments[index]);

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
                        out var arrayIndex) ||
                    arrayIndex < 0 ||
                    arrayIndex >=
                        value.GetArrayLength())
                {
                    return false;
                }

                value =
                    value[arrayIndex];
                continue;
            }

            return false;
        }

        return true;
    }

    private static string DecodePointerSegment(
        string segment) =>
        segment
            .Replace(
                "~1",
                "/",
                StringComparison.Ordinal)
            .Replace(
                "~0",
                "~",
                StringComparison.Ordinal);

    private static long RawBytes(
        KafkaRawRecord record)
    {
        long total =
            record.Key?.Length ?? 0;
        total +=
            record.Value?.Length ?? 0;

        foreach (var header in
                 record.Headers)
        {
            total +=
                Encoding.UTF8.GetByteCount(
                    header.Name);
            total +=
                header.Value.Length;
        }

        return total;
    }
}
