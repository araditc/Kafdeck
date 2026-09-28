namespace Kafdeck.Core.Observability;

public sealed record HistoricalMetricIdentity(
    string MetricName,
    string ClusterId,
    string ResourceKind,
    string ResourceId)
{
    public const int MaxMetricNameLength = 128;
    public const int MaxClusterIdLength = 256;
    public const int MaxResourceKindLength = 64;
    public const int MaxResourceIdLength = 512;

    public void Validate()
    {
        ValidateIdentifier(
            MetricName,
            MaxMetricNameLength,
            nameof(MetricName));
        ValidateText(
            ClusterId,
            MaxClusterIdLength,
            nameof(ClusterId));
        ValidateIdentifier(
            ResourceKind,
            MaxResourceKindLength,
            nameof(ResourceKind));
        ValidateText(
            ResourceId,
            MaxResourceIdLength,
            nameof(ResourceId));
    }

    private static void ValidateIdentifier(
        string value,
        int maxLength,
        string name)
    {
        ValidateText(value, maxLength, name);

        if (value.Any(character =>
                !(char.IsAsciiLetterOrDigit(character) ||
                  character is '.' or '_' or '-')))
        {
            throw new ArgumentException(
                "Historical metric identifiers may contain only ASCII letters, digits, '.', '_' or '-'.",
                name);
        }
    }

    private static void ValidateText(
        string value,
        int maxLength,
        string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, name);

        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"Historical metric {name} must be trimmed, contain no control characters, and not exceed {maxLength} characters.",
                name);
        }
    }
}

public sealed record HistoricalMetricSample(
    HistoricalMetricIdentity Identity,
    DateTimeOffset ObservedAtUtc,
    double Min,
    double Max,
    double Sum,
    long Count,
    int ResolutionSeconds,
    string Source,
    string? State = null)
{
    public const int MaxSourceLength = 128;
    public const int MaxStateLength = 64;
    public const int HardMaxResolutionSeconds = 86_400;

    public double Average => Count == 0 ? 0 : Sum / Count;

    public static HistoricalMetricSample Gauge(
        HistoricalMetricIdentity identity,
        DateTimeOffset observedAtUtc,
        double value,
        string source,
        string? state = null) =>
        new(
            identity,
            observedAtUtc,
            value,
            value,
            value,
            1,
            0,
            source,
            state);

    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(Identity);
        Identity.Validate();

        if (ObservedAtUtc == default)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ObservedAtUtc));
        }

        if (!double.IsFinite(Min) ||
            !double.IsFinite(Max) ||
            !double.IsFinite(Sum) ||
            Count < 1 ||
            Min > Max)
        {
            throw new ArgumentException(
                "Historical metric aggregates must be finite, non-empty, and ordered.");
        }

        if (ResolutionSeconds is < 0 or > HardMaxResolutionSeconds)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ResolutionSeconds));
        }

        ValidateSafeText(
            Source,
            MaxSourceLength,
            nameof(Source));

        if (State is not null)
        {
            ValidateSafeText(
                State,
                MaxStateLength,
                nameof(State));
        }
    }

    private static void ValidateSafeText(
        string value,
        int maxLength,
        string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, name);

        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"Historical metric {name} must be trimmed, contain no control characters, and not exceed {maxLength} characters.",
                name);
        }
    }
}

public sealed record HistoricalMetricQuery(
    string MetricName,
    string ClusterId,
    string ResourceKind,
    string? ResourceId,
    DateTimeOffset FromUtc,
    DateTimeOffset ToUtc,
    int MaxSeries,
    int MaxPoints)
{
    public static readonly TimeSpan HardMaxRange =
        TimeSpan.FromDays(31);
    public const int HardMaxSeries = 10_000;
    public const int HardMaxPoints = 250_000;

    public void Validate()
    {
        new HistoricalMetricIdentity(
                MetricName,
                ClusterId,
                ResourceKind,
                ResourceId ?? "_all")
            .Validate();

        if (FromUtc == default ||
            ToUtc == default ||
            ToUtc <= FromUtc ||
            ToUtc - FromUtc > HardMaxRange)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ToUtc),
                "Historical metric query range must be positive and not exceed 31 days.");
        }

        if (MaxSeries is < 1 or > HardMaxSeries)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxSeries));
        }

        if (MaxPoints is < 1 or > HardMaxPoints)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxPoints));
        }
    }
}

public sealed record HistoricalMetricSeries(
    HistoricalMetricIdentity Identity,
    IReadOnlyList<HistoricalMetricSample> Points);

public sealed record HistoricalMetricQueryResult(
    IReadOnlyList<HistoricalMetricSeries> Series,
    bool Truncated,
    string? LimitReason,
    DateTimeOffset FromUtc,
    DateTimeOffset ToUtc,
    string Provider);

public sealed record HistoricalMetricRetentionResult(
    long RawDeleted,
    long RollupDeleted);

public sealed record HistoricalMetricStorePolicy(
    TimeSpan MaxQueryRange,
    int MaxSeriesPerQuery,
    int MaxPointsPerQuery,
    TimeSpan MaxQueryDuration,
    int MaxConcurrentQueries)
{
    public void Validate()
    {
        if (MaxQueryRange <= TimeSpan.Zero ||
            MaxQueryRange > HistoricalMetricQuery.HardMaxRange)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxQueryRange));
        }

        if (MaxSeriesPerQuery is < 1 or >
            HistoricalMetricQuery.HardMaxSeries)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxSeriesPerQuery));
        }

        if (MaxPointsPerQuery is < 1 or >
            HistoricalMetricQuery.HardMaxPoints)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxPointsPerQuery));
        }

        if (MaxQueryDuration <= TimeSpan.Zero ||
            MaxQueryDuration > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxQueryDuration));
        }

        if (MaxConcurrentQueries is < 1 or > 8)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxConcurrentQueries));
        }
    }

    public void ValidateQuery(
        HistoricalMetricQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);
        query.Validate();

        if (query.ToUtc - query.FromUtc >
            MaxQueryRange)
        {
            throw new ArgumentOutOfRangeException(
                nameof(query),
                "Historical metric query exceeds the configured deployment range.");
        }

        if (query.MaxSeries >
            MaxSeriesPerQuery)
        {
            throw new ArgumentOutOfRangeException(
                nameof(query),
                "Historical metric query exceeds the configured series limit.");
        }

        if (query.MaxPoints >
            MaxPointsPerQuery)
        {
            throw new ArgumentOutOfRangeException(
                nameof(query),
                "Historical metric query exceeds the configured point limit.");
        }
    }
}

public interface IHistoricalMetricStore
{
    Task InitializeAsync(
        CancellationToken cancellationToken = default);

    Task AppendAsync(
        IReadOnlyList<HistoricalMetricSample> samples,
        CancellationToken cancellationToken = default);

    Task<HistoricalMetricQueryResult> QueryAsync(
        HistoricalMetricQuery query,
        CancellationToken cancellationToken = default);

    Task<HistoricalMetricRetentionResult> DeleteExpiredAsync(
        DateTimeOffset rawBeforeUtc,
        DateTimeOffset rollupBeforeUtc,
        CancellationToken cancellationToken = default);
}
