namespace Kafdeck.Core.Observability;

public enum OperationalTrendState
{
    Available = 1,
    Partial = 2,
    Unavailable = 3,
    Unknown = 4,
}

public sealed record OperationalTrendQuery(
    OperationalMetricKind Metric,
    OperationalResourceIdentity Resource,
    DateTimeOffset FromUtc,
    DateTimeOffset ToUtc,
    int MaxPoints)
{
    public const int HardMaxPoints = 10_000;

    public void Validate()
    {
        if (!Enum.IsDefined(Metric))
        {
            throw new ArgumentOutOfRangeException(nameof(Metric));
        }

        ArgumentNullException.ThrowIfNull(Resource);
        Resource.Validate();
        OperationalMetricCompatibility.Validate(
            Metric,
            Resource.Kind);

        if (FromUtc == default ||
            ToUtc == default ||
            ToUtc <= FromUtc ||
            ToUtc - FromUtc >
                HistoricalMetricQuery.HardMaxRange)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ToUtc));
        }

        if (MaxPoints is < 1 or > HardMaxPoints)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxPoints));
        }
    }
}

public sealed record OperationalTrendPoint(
    DateTimeOffset ObservedAtUtc,
    double Min,
    double Max,
    double Average,
    long Count,
    int ResolutionSeconds,
    OperationalEvidenceState State,
    bool HasKnownCoverage,
    DateTimeOffset? FirstObservedAtUtc = null,
    DateTimeOffset? LastObservedAtUtc = null)
{
    public DateTimeOffset? EffectiveFirstObservedAtUtc =>
        HasKnownCoverage
            ? FirstObservedAtUtc ?? ObservedAtUtc
            : null;

    public DateTimeOffset? EffectiveLastObservedAtUtc =>
        HasKnownCoverage
            ? LastObservedAtUtc ?? ObservedAtUtc
            : null;
}

public sealed record OperationalTrendResult(
    OperationalMetricKind Metric,
    OperationalResourceIdentity Resource,
    IReadOnlyList<OperationalTrendPoint> Points,
    bool Truncated,
    string? LimitReason,
    string Provider,
    OperationalTrendState State)
{
    public void Validate(
        OperationalTrendQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);
        query.Validate();
        ArgumentNullException.ThrowIfNull(Points);
        ArgumentException.ThrowIfNullOrWhiteSpace(Provider);

        if (Metric != query.Metric ||
            Resource != query.Resource ||
            Points.Count > query.MaxPoints ||
            !Enum.IsDefined(State))
        {
            throw new ArgumentException(
                "Operational trend result is inconsistent with its query.");
        }

        if (Truncated !=
            !string.IsNullOrWhiteSpace(LimitReason))
        {
            throw new ArgumentException(
                "Truncated operational trends require an explicit limit reason.");
        }

        if (State is
                OperationalTrendState.Unavailable or
                OperationalTrendState.Unknown &&
            Points.Count != 0)
        {
            throw new ArgumentException(
                "Unavailable/unknown operational trends cannot fabricate points.");
        }

        foreach (var point in Points)
        {
            if (point.ObservedAtUtc == default ||
                !double.IsFinite(point.Min) ||
                !double.IsFinite(point.Max) ||
                !double.IsFinite(point.Average) ||
                point.Min > point.Max ||
                point.Count < 1 ||
                point.ResolutionSeconds < 0 ||
                !Enum.IsDefined(point.State) ||
                ((point.FirstObservedAtUtc is null) !=
                 (point.LastObservedAtUtc is null)) ||
                (point.FirstObservedAtUtc is not null &&
                 (point.FirstObservedAtUtc.Value == default ||
                  point.LastObservedAtUtc!.Value <
                  point.FirstObservedAtUtc.Value)))
            {
                throw new ArgumentException(
                    "Operational trend contains invalid evidence.");
            }
        }
    }
}

public sealed record OperationalSloDefinition(
    string Id,
    OperationalMetricKind Metric,
    OperationalResourceIdentity Resource,
    double MaximumGoodValue,
    double TargetFraction)
{
    public const int MaxIdLength = 128;

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Id);

        if (!string.Equals(
                Id,
                Id.Trim(),
                StringComparison.Ordinal) ||
            Id.Length > MaxIdLength ||
            Id.Any(char.IsControl) ||
            !double.IsFinite(MaximumGoodValue) ||
            MaximumGoodValue < 0 ||
            !double.IsFinite(TargetFraction) ||
            TargetFraction <= 0 ||
            TargetFraction >= 1)
        {
            throw new ArgumentException(
                "Operational SLO definition is invalid.");
        }

        ArgumentNullException.ThrowIfNull(Resource);
        Resource.Validate();
        OperationalMetricCompatibility.Validate(
            Metric,
            Resource.Kind);
    }
}

public sealed record OperationalSloResult(
    OperationalSloDefinition Definition,
    DateTimeOffset FromUtc,
    DateTimeOffset ToUtc,
    int EvaluatedPoints,
    int GoodPoints,
    double? ComplianceFraction,
    double? BurnRate,
    OperationalEvidenceState State,
    string? ReasonCode)
{
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(Definition);
        Definition.Validate();

        if (FromUtc == default ||
            ToUtc <= FromUtc ||
            EvaluatedPoints < 0 ||
            GoodPoints < 0 ||
            GoodPoints > EvaluatedPoints ||
            !Enum.IsDefined(State))
        {
            throw new ArgumentException(
                "Operational SLO result is invalid.");
        }

        if (EvaluatedPoints == 0)
        {
            if (ComplianceFraction is not null ||
                BurnRate is not null)
            {
                throw new ArgumentException(
                    "SLO without evaluated evidence cannot carry numeric compliance.");
            }

            return;
        }

        if (ComplianceFraction is null ||
            BurnRate is null ||
            !double.IsFinite(ComplianceFraction.Value) ||
            !double.IsFinite(BurnRate.Value) ||
            ComplianceFraction.Value is < 0 or > 1 ||
            BurnRate.Value < 0)
        {
            throw new ArgumentException(
                "Operational SLO numeric evidence is invalid.");
        }
    }
}

public static class OperationalMetricHistoryNames
{
    public const string ConsumerLagTotal =
        "consumer.lag.total";

    public static string? TryGet(
        OperationalMetricKind metric) =>
        metric switch
        {
            OperationalMetricKind.ConsumerLagTotal =>
                ConsumerLagTotal,
            _ => null,
        };

    public static string ResourceKind(
        OperationalResourceKind kind) =>
        kind switch
        {
            OperationalResourceKind.ConsumerGroup =>
                "consumer_group",
            OperationalResourceKind.Topic =>
                "topic",
            OperationalResourceKind.Broker =>
                "broker",
            OperationalResourceKind.Operation =>
                "operation",
            _ => throw new ArgumentOutOfRangeException(
                nameof(kind)),
        };
}
