namespace Kafdeck.Core.Records;

internal static class DataQualityContractInputBounds
{
    public static void RequireRawString(
        string? value,
        int maxLength,
        string parameterName)
    {
        if (value is null)
        {
            throw new ArgumentNullException(
                parameterName);
        }

        if (value.Length is < 1 ||
            value.Length > maxLength)
        {
            throw new ArgumentException(
                "Data-quality contract input is empty or exceeds its raw length bound.",
                parameterName);
        }
    }
}

public enum DataQualityRuleKind
{
    RequiredPath = 1,
    NullForbidden = 2,
    ValueType = 3,
    NumericRange = 4,
    StringLengthRange = 5,
}

public enum DataQualityValueType
{
    String = 1,
    Number = 2,
    Boolean = 3,
    Object = 4,
    Array = 5,
}

public sealed record DataQualityRule
{
    public const int MaxRuleIdLength = 128;
    public const int MaxRulesPerPolicy = 128;

    public DataQualityRule(
        string ruleId,
        DataQualityRuleKind kind,
        string jsonPointer,
        DataQualityValueType? expectedType = null,
        double? minimumNumber = null,
        double? maximumNumber = null,
        int? minimumLength = null,
        int? maximumLength = null)
    {
        DataQualityContractInputBounds.RequireRawString(
            ruleId,
            MaxRuleIdLength,
            nameof(ruleId));
        DataQualityContractInputBounds.RequireRawString(
            jsonPointer,
            RecordStructuredMaskRule.MaxPathCharacters,
            nameof(jsonPointer));
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleId);
        ArgumentException.ThrowIfNullOrWhiteSpace(jsonPointer);

        RuleId = ruleId.Trim();
        if (RuleId.Length > MaxRuleIdLength ||
            RuleId.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(nameof(ruleId));
        }

        if (!Enum.IsDefined(kind))
        {
            throw new ArgumentOutOfRangeException(nameof(kind));
        }

        if (!jsonPointer.StartsWith("/", StringComparison.Ordinal) ||
            jsonPointer.Length > RecordStructuredMaskRule.MaxPathCharacters ||
            jsonPointer.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Data-quality paths must be bounded JSON-pointer-style paths.",
                nameof(jsonPointer));
        }

        var segments =
            jsonPointer.Split('/', StringSplitOptions.None).Length - 1;
        if (segments is < 1 or > RecordStructuredMaskRule.MaxPathSegments)
        {
            throw new ArgumentOutOfRangeException(nameof(jsonPointer));
        }

        switch (kind)
        {
            case DataQualityRuleKind.RequiredPath:
            case DataQualityRuleKind.NullForbidden:
                RequireNoTypedOperands(
                    expectedType,
                    minimumNumber,
                    maximumNumber,
                    minimumLength,
                    maximumLength);
                break;

            case DataQualityRuleKind.ValueType:
                if (expectedType is null ||
                    !Enum.IsDefined(expectedType.Value) ||
                    minimumNumber is not null ||
                    maximumNumber is not null ||
                    minimumLength is not null ||
                    maximumLength is not null)
                {
                    throw new ArgumentException(
                        "ValueType rule requires exactly one expected type.");
                }
                break;

            case DataQualityRuleKind.NumericRange:
                if (expectedType is not null ||
                    minimumLength is not null ||
                    maximumLength is not null ||
                    (minimumNumber is null && maximumNumber is null) ||
                    (minimumNumber is not null &&
                     !double.IsFinite(minimumNumber.Value)) ||
                    (maximumNumber is not null &&
                     !double.IsFinite(maximumNumber.Value)) ||
                    (minimumNumber is not null &&
                     maximumNumber is not null &&
                     minimumNumber.Value > maximumNumber.Value))
                {
                    throw new ArgumentException(
                        "NumericRange rule requires a finite lower and/or upper numeric bound.");
                }
                break;

            case DataQualityRuleKind.StringLengthRange:
                if (expectedType is not null ||
                    minimumNumber is not null ||
                    maximumNumber is not null ||
                    (minimumLength is null && maximumLength is null) ||
                    minimumLength is < 0 ||
                    maximumLength is < 0 ||
                    (minimumLength is not null &&
                     maximumLength is not null &&
                     minimumLength.Value > maximumLength.Value) ||
                    minimumLength > 1_000_000 ||
                    maximumLength > 1_000_000)
                {
                    throw new ArgumentException(
                        "StringLengthRange rule requires a valid bounded length range.");
                }
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }

        Kind = kind;
        JsonPointer = jsonPointer;
        ExpectedType = expectedType;
        MinimumNumber = minimumNumber;
        MaximumNumber = maximumNumber;
        MinimumLength = minimumLength;
        MaximumLength = maximumLength;
    }

    public string RuleId { get; }
    public DataQualityRuleKind Kind { get; }
    public string JsonPointer { get; }
    public DataQualityValueType? ExpectedType { get; }
    public double? MinimumNumber { get; }
    public double? MaximumNumber { get; }
    public int? MinimumLength { get; }
    public int? MaximumLength { get; }

    private static void RequireNoTypedOperands(
        DataQualityValueType? expectedType,
        double? minimumNumber,
        double? maximumNumber,
        int? minimumLength,
        int? maximumLength)
    {
        if (expectedType is not null ||
            minimumNumber is not null ||
            maximumNumber is not null ||
            minimumLength is not null ||
            maximumLength is not null)
        {
            throw new ArgumentException(
                "This data-quality rule kind does not accept typed operands.");
        }
    }
}

public sealed record DataQualityPolicyScope
{
    public const int MaxPartitions = 128;

    public DataQualityPolicyScope(
        string clusterId,
        string topicName,
        IReadOnlyList<int> partitions)
    {
        DataQualityContractInputBounds.RequireRawString(
            clusterId,
            256,
            nameof(clusterId));
        DataQualityContractInputBounds.RequireRawString(
            topicName,
            249,
            nameof(topicName));
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentNullException.ThrowIfNull(partitions);

        if (!string.Equals(
                clusterId,
                clusterId.Trim(),
                StringComparison.Ordinal) ||
            clusterId.Length > 256 ||
            clusterId.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Data-quality cluster ID must be an exact bounded identifier.",
                nameof(clusterId));
        }

        if (!string.Equals(
                topicName,
                topicName.Trim(),
                StringComparison.Ordinal) ||
            topicName.Length > 249 ||
            topicName is "." or ".." ||
            topicName.Any(
                character =>
                    !(character is >= 'a' and <= 'z' or
                      >= 'A' and <= 'Z' or
                      >= '0' and <= '9' or
                      '.' or '_' or '-')))
        {
            throw new ArgumentException(
                "Data-quality topic name must be an admitted exact Kafka topic identifier.",
                nameof(topicName));
        }

        if (partitions.Count > MaxPartitions)
        {
            throw new ArgumentOutOfRangeException(
                nameof(partitions),
                $"A data-quality policy cannot inspect more than {MaxPartitions} partition entries.");
        }

        var normalized =
            partitions.Distinct().OrderBy(value => value).ToArray();

        if (normalized.Length is < 1 or > MaxPartitions ||
            normalized.Any(value => value < 0))
        {
            throw new ArgumentOutOfRangeException(
                nameof(partitions),
                $"A data-quality policy must target 1..{MaxPartitions} explicit non-negative partitions.");
        }

        ClusterId = clusterId;
        TopicName = topicName;
        Partitions = Array.AsReadOnly(normalized);
    }

    public string ClusterId { get; }
    public string TopicName { get; }
    public IReadOnlyList<int> Partitions { get; }
}

public sealed record DataQualityPolicyBudget
{
    public const int DefaultRecordsPerSecond = 100;
    public const int HardMaxRecordsPerSecond = 1_000;
    public const long DefaultBytesPerSecond = 1L * 1024 * 1024;
    public const long HardMaxBytesPerSecond = 10L * 1024 * 1024;
    public static readonly TimeSpan DefaultEvaluationWindow = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan HardMaxEvaluationWindow = TimeSpan.FromHours(1);
    public const int DefaultActivePoliciesPerCluster = 20;
    public const int HardMaxActivePoliciesPerCluster = 100;
    public const int DefaultConcurrentReadersPerCluster = 2;
    public const int HardMaxConcurrentReadersPerCluster = 8;
    public const int DurableRawPayloadExemplars = 0;

    public DataQualityPolicyBudget(
        int recordsPerSecond = DefaultRecordsPerSecond,
        long bytesPerSecond = DefaultBytesPerSecond,
        TimeSpan? evaluationWindow = null,
        int activePoliciesPerCluster = DefaultActivePoliciesPerCluster,
        int concurrentReadersPerCluster = DefaultConcurrentReadersPerCluster)
    {
        if (recordsPerSecond is < 1 or > HardMaxRecordsPerSecond)
        {
            throw new ArgumentOutOfRangeException(nameof(recordsPerSecond));
        }

        if (bytesPerSecond is < 1 or > HardMaxBytesPerSecond)
        {
            throw new ArgumentOutOfRangeException(nameof(bytesPerSecond));
        }

        var window = evaluationWindow ?? DefaultEvaluationWindow;
        if (window <= TimeSpan.Zero || window > HardMaxEvaluationWindow)
        {
            throw new ArgumentOutOfRangeException(nameof(evaluationWindow));
        }

        if (activePoliciesPerCluster is < 1 or > HardMaxActivePoliciesPerCluster)
        {
            throw new ArgumentOutOfRangeException(nameof(activePoliciesPerCluster));
        }

        if (concurrentReadersPerCluster is < 1 or > HardMaxConcurrentReadersPerCluster)
        {
            throw new ArgumentOutOfRangeException(nameof(concurrentReadersPerCluster));
        }

        RecordsPerSecond = recordsPerSecond;
        BytesPerSecond = bytesPerSecond;
        EvaluationWindow = window;
        ActivePoliciesPerCluster = activePoliciesPerCluster;
        ConcurrentReadersPerCluster = concurrentReadersPerCluster;
    }

    public int RecordsPerSecond { get; }
    public long BytesPerSecond { get; }
    public TimeSpan EvaluationWindow { get; }
    public int ActivePoliciesPerCluster { get; }
    public int ConcurrentReadersPerCluster { get; }
}

public sealed record DataQualityPolicyDefinition
{
    public const int MaxPolicyIdLength = 128;

    public DataQualityPolicyDefinition(
        string policyId,
        int version,
        DataQualityPolicyScope scope,
        IReadOnlyList<DataQualityRule> rules,
        DataQualityPolicyBudget? budget = null)
    {
        DataQualityContractInputBounds.RequireRawString(
            policyId,
            MaxPolicyIdLength,
            nameof(policyId));
        ArgumentException.ThrowIfNullOrWhiteSpace(policyId);
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(rules);

        PolicyId = policyId.Trim();
        if (PolicyId.Length > MaxPolicyIdLength ||
            PolicyId.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(nameof(policyId));
        }

        if (version < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(version));
        }

        if (rules.Count is < 1 or > DataQualityRule.MaxRulesPerPolicy)
        {
            throw new ArgumentException(
                "Data-quality policy requires 1..128 uniquely identified closed rules.",
                nameof(rules));
        }

        var actualRules = rules.ToArray();
        if (actualRules.Any(rule => rule is null) ||
            actualRules
                .GroupBy(rule => rule.RuleId, StringComparer.Ordinal)
                .Any(group => group.Count() != 1))
        {
            throw new ArgumentException(
                "Data-quality policy requires 1..128 uniquely identified closed rules.",
                nameof(rules));
        }

        Version = version;
        Scope = scope;
        Rules = Array.AsReadOnly(actualRules);
        Budget = budget ?? new DataQualityPolicyBudget();
    }

    public string PolicyId { get; }
    public int Version { get; }
    public DataQualityPolicyScope Scope { get; }
    public IReadOnlyList<DataQualityRule> Rules { get; }
    public DataQualityPolicyBudget Budget { get; }
}

public enum DataQualityEvidenceState
{
    Available = 1,
    Partial = 2,
    Unavailable = 3,
    Unknown = 4,
}

public sealed record DataQualityRuleViolationCount(
    string RuleId,
    long Count);

public sealed record DataQualityAggregateEvidence
{
    public const int MaxSourceLength = 128;
    public DataQualityAggregateEvidence(
        string policyId,
        int policyVersion,
        DateTimeOffset windowStartUtc,
        DateTimeOffset windowEndUtc,
        long evaluatedRecords,
        long evaluatedBytes,
        long violationCount,
        IReadOnlyList<DataQualityRuleViolationCount> violationsByRule,
        DataQualityEvidenceState state,
        string source)
    {
        DataQualityContractInputBounds.RequireRawString(
            policyId,
            DataQualityPolicyDefinition.MaxPolicyIdLength,
            nameof(policyId));
        DataQualityContractInputBounds.RequireRawString(
            source,
            MaxSourceLength,
            nameof(source));
        ArgumentException.ThrowIfNullOrWhiteSpace(policyId);
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        ArgumentNullException.ThrowIfNull(violationsByRule);

        var normalizedPolicyId = policyId.Trim();
        if (normalizedPolicyId.Length >
                DataQualityPolicyDefinition.MaxPolicyIdLength ||
            normalizedPolicyId.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                nameof(policyId));
        }

        var normalizedSource = source.Trim();
        if (normalizedSource.Length > MaxSourceLength ||
            normalizedSource.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                nameof(source));
        }

        if (policyVersion < 1 ||
            windowStartUtc == default ||
            windowEndUtc <= windowStartUtc ||
            windowEndUtc - windowStartUtc >
                DataQualityPolicyBudget.HardMaxEvaluationWindow ||
            evaluatedRecords < 0 ||
            evaluatedBytes < 0 ||
            violationCount < 0 ||
            violationCount > evaluatedRecords)
        {
            throw new ArgumentException(
                "Data-quality aggregate evidence counters/window are invalid.");
        }

        if (!Enum.IsDefined(state))
        {
            throw new ArgumentOutOfRangeException(nameof(state));
        }

        var windowTicks =
            (windowEndUtc - windowStartUtc).Ticks;
        var hardMaxEvaluatedRecords =
            checked(
                windowTicks *
                DataQualityPolicyBudget.HardMaxRecordsPerSecond /
                TimeSpan.TicksPerSecond);
        var hardMaxEvaluatedBytes =
            checked(
                windowTicks *
                DataQualityPolicyBudget.HardMaxBytesPerSecond /
                TimeSpan.TicksPerSecond);

        if (evaluatedRecords > hardMaxEvaluatedRecords ||
            evaluatedBytes > hardMaxEvaluatedBytes)
        {
            throw new ArgumentException(
                "Data-quality aggregate evidence exceeds the server-owned hard throughput budget for its window.");
        }

        if (violationsByRule.Count >
            DataQualityRule.MaxRulesPerPolicy)
        {
            throw new ArgumentException(
                "Data-quality aggregate violation counts are invalid or unbounded.",
                nameof(violationsByRule));
        }

        var counts = violationsByRule.ToArray();
        if (
            counts.Any(
                item =>
                    item is null ||
                    item.RuleId is null ||
                    item.RuleId.Length is < 1 or >
                        DataQualityRule.MaxRuleIdLength ||
                    string.IsNullOrWhiteSpace(item.RuleId) ||
                    !string.Equals(
                        item.RuleId,
                        item.RuleId.Trim(),
                        StringComparison.Ordinal) ||
                    item.RuleId.Any(char.IsControl) ||
                    item.Count < 0 ||
                    item.Count > violationCount) ||
            counts
                .GroupBy(item => item.RuleId, StringComparer.Ordinal)
                .Any(group => group.Count() != 1) ||
            counts.Sum(item => item.Count) < violationCount)
        {
            throw new ArgumentException(
                "Data-quality aggregate violation counts are invalid or unbounded.",
                nameof(violationsByRule));
        }

        if (state is DataQualityEvidenceState.Unavailable or
            DataQualityEvidenceState.Unknown)
        {
            if (evaluatedRecords != 0 ||
                evaluatedBytes != 0 ||
                violationCount != 0 ||
                counts.Any(item => item.Count != 0))
            {
                throw new ArgumentException(
                    "Unavailable/unknown data-quality evidence cannot fabricate evaluated or violation counts.");
            }
        }

        PolicyId = normalizedPolicyId;
        PolicyVersion = policyVersion;
        WindowStartUtc = windowStartUtc.ToUniversalTime();
        WindowEndUtc = windowEndUtc.ToUniversalTime();
        EvaluatedRecords = evaluatedRecords;
        EvaluatedBytes = evaluatedBytes;
        ViolationCount = violationCount;
        ViolationsByRule = Array.AsReadOnly(counts);
        State = state;
        Source = normalizedSource;
    }

    public string PolicyId { get; }
    public int PolicyVersion { get; }
    public DateTimeOffset WindowStartUtc { get; }
    public DateTimeOffset WindowEndUtc { get; }
    public long EvaluatedRecords { get; }
    public long EvaluatedBytes { get; }
    public long ViolationCount { get; }
    public IReadOnlyList<DataQualityRuleViolationCount> ViolationsByRule { get; }
    public DataQualityEvidenceState State { get; }
    public string Source { get; }
}
