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

        var window =
            input.WindowEndUtc -
            input.WindowStartUtc;
        if (window >
            policy.Budget.EvaluationWindow)
        {
            throw new ArgumentException(
                "Data-quality evaluation window exceeds the policy-owned evaluation window.",
                nameof(input));
        }

        var policyRecordLimit =
            checked(
                window.Ticks *
                policy.Budget.RecordsPerSecond /
                TimeSpan.TicksPerSecond);
        var policyByteLimit =
            checked(
                window.Ticks *
                policy.Budget.BytesPerSecond /
                TimeSpan.TicksPerSecond);
        var effectiveRecordLimit =
            Math.Min(
                (long)input.Budget.MaxRecords,
                policyRecordLimit);
        var effectiveByteLimit =
            Math.Min(
                input.Budget.MaxRawBytes,
                policyByteLimit);

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
            if (evaluatedRecords >=
                effectiveRecordLimit)
            {
                outcome =
                    DataQualityEvaluationOutcome.RecordLimit;
                state =
                    evaluatedRecords == 0
                        ? DataQualityEvidenceState.Unknown
                        : DataQualityEvidenceState.Partial;
                break;
            }

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

            if (rawBytes >
                    effectiveByteLimit ||
                evaluatedBytes >
                    effectiveByteLimit -
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

            var violatedRules =
                EvaluateRecord(
                    record,
                    policy.Rules);

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

            evaluatedRecords++;
            evaluatedBytes +=
                rawBytes;

            if (violatedRules.Count > 0)
            {
                violatedRecords++;
                foreach (var ruleId in
                         violatedRules)
                {
                    counts[ruleId] =
                        checked(
                            counts[ruleId] + 1);
                }
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

    private static IReadOnlyList<string> EvaluateRecord(
        KafkaRawRecord record,
        IReadOnlyList<DataQualityRule> rules)
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
            var violations =
                new List<string>();

            foreach (var rule in rules)
            {
                var violated =
                    root is null ||
                    !EvaluateRule(
                        root.Value,
                        rule);

                if (violated)
                {
                    violations.Add(
                        rule.RuleId);
                }
            }

            return Array.AsReadOnly(
                violations.ToArray());
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
            if (!TryDecodePointerSegment(
                    segments[index],
                    out var segment))
            {
                return false;
            }

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
                if (!IsValidArrayIndexToken(
                        segment) ||
                    !int.TryParse(
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

    private static bool TryDecodePointerSegment(
        string segment,
        out string decoded)
    {
        var builder =
            new StringBuilder(
                segment.Length);

        for (var index = 0;
             index < segment.Length;
             index++)
        {
            var character =
                segment[index];
            if (character != '~')
            {
                builder.Append(
                    character);
                continue;
            }

            if (index + 1 >=
                segment.Length)
            {
                decoded =
                    string.Empty;
                return false;
            }

            var escape =
                segment[++index];
            switch (escape)
            {
                case '0':
                    builder.Append('~');
                    break;
                case '1':
                    builder.Append('/');
                    break;
                default:
                    decoded =
                        string.Empty;
                    return false;
            }
        }

        decoded =
            builder.ToString();
        return true;
    }

    private static bool IsValidArrayIndexToken(
        string segment)
    {
        if (segment.Length == 0)
        {
            return false;
        }

        if (segment.Length == 1)
        {
            return segment[0] is
                >= '0' and <= '9';
        }

        if (segment[0] is
            < '1' or > '9')
        {
            return false;
        }

        for (var index = 1;
             index < segment.Length;
             index++)
        {
            if (segment[index] is
                < '0' or > '9')
            {
                return false;
            }
        }

        return true;
    }

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
