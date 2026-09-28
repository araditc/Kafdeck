using Kafdeck.Core.ReadViews;

namespace Kafdeck.Core.Observability;

public enum OperationalResourceKind
{
    Broker = 1,
    Topic = 2,
    ConsumerGroup = 3,
    Operation = 4,
}

public enum OperationalMetricKind
{
    BrokerBytesInPerSecond = 1,
    BrokerBytesOutPerSecond = 2,
    TopicRecordsInPerSecond = 3,
    TopicRecordsOutPerSecond = 4,
    ConsumerLagTotal = 5,
    ConsumerConsumeRecordsPerSecond = 6,
    OperationDurationMilliseconds = 7,
}

public enum OperationalEvidenceState
{
    Available = 1,
    Partial = 2,
    Stale = 3,
    Unavailable = 4,
    Unknown = 5,
}

public sealed record OperationalResourceIdentity(
    string ClusterId,
    OperationalResourceKind Kind,
    string ResourceId)
{
    public const int MaxClusterIdLength = 256;
    public const int MaxResourceIdLength = 512;

    public void Validate()
    {
        ValidateText(
            ClusterId,
            MaxClusterIdLength,
            nameof(ClusterId));
        ValidateText(
            ResourceId,
            MaxResourceIdLength,
            nameof(ResourceId));

        if (!Enum.IsDefined(
                typeof(OperationalResourceKind),
                Kind))
        {
            throw new ArgumentOutOfRangeException(
                nameof(Kind));
        }
    }

    private static void ValidateText(
        string value,
        int maxLength,
        string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            value,
            name);

        if (!string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"Operational analytics {name} must be trimmed, contain no control characters, and not exceed {maxLength} characters.",
                name);
        }
    }
}

public sealed record OperationalMetricEvidence(
    OperationalMetricKind Metric,
    OperationalResourceIdentity Resource,
    double? Value,
    DateTimeOffset? ObservedAtUtc,
    TimeSpan? Window,
    string Source,
    OperationalEvidenceState State)
{
    public const int MaxSourceLength = 128;
    public static readonly TimeSpan HardMaxWindow =
        TimeSpan.FromHours(24);

    public void Validate()
    {
        if (!Enum.IsDefined(
                typeof(OperationalMetricKind),
                Metric))
        {
            throw new ArgumentOutOfRangeException(
                nameof(Metric));
        }

        ArgumentNullException.ThrowIfNull(
            Resource);
        Resource.Validate();

        ArgumentException.ThrowIfNullOrWhiteSpace(
            Source);

        if (!string.Equals(
                Source,
                Source.Trim(),
                StringComparison.Ordinal) ||
            Source.Length > MaxSourceLength ||
            Source.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"Operational analytics source must be trimmed, contain no control characters, and not exceed {MaxSourceLength} characters.",
                nameof(Source));
        }

        if (!Enum.IsDefined(
                typeof(OperationalEvidenceState),
                State))
        {
            throw new ArgumentOutOfRangeException(
                nameof(State));
        }

        if (State is
            OperationalEvidenceState.Unavailable or
            OperationalEvidenceState.Unknown)
        {
            if (Value is not null)
            {
                throw new ArgumentException(
                    "Unavailable or unknown operational evidence cannot carry a numeric value.",
                    nameof(Value));
            }

            ValidateOptionalWindow();
            return;
        }

        if (Value is null ||
            !double.IsFinite(Value.Value) ||
            Value.Value < 0)
        {
            throw new ArgumentException(
                "Available operational evidence requires a finite non-negative value.",
                nameof(Value));
        }

        if (ObservedAtUtc is null ||
            ObservedAtUtc.Value == default)
        {
            throw new ArgumentException(
                "Available operational evidence requires an observation timestamp.",
                nameof(ObservedAtUtc));
        }

        if (RequiresWindow(Metric))
        {
            if (Window is null ||
                Window.Value <= TimeSpan.Zero ||
                Window.Value > HardMaxWindow)
            {
                throw new ArgumentException(
                    "Rate evidence requires a positive bounded observation window.",
                    nameof(Window));
            }
        }
        else
        {
            ValidateOptionalWindow();
        }
    }

    private void ValidateOptionalWindow()
    {
        if (Window is not null &&
            (Window.Value <= TimeSpan.Zero ||
             Window.Value > HardMaxWindow))
        {
            throw new ArgumentException(
                "Operational evidence window must be positive and bounded when supplied.",
                nameof(Window));
        }
    }

    private static bool RequiresWindow(
        OperationalMetricKind metric) =>
        metric is
            OperationalMetricKind.BrokerBytesInPerSecond or
            OperationalMetricKind.BrokerBytesOutPerSecond or
            OperationalMetricKind.TopicRecordsInPerSecond or
            OperationalMetricKind.TopicRecordsOutPerSecond or
            OperationalMetricKind.ConsumerConsumeRecordsPerSecond;
}

public sealed record OperationalAnalyticsQuery(
    string ClusterId,
    OperationalResourceKind? ResourceKind,
    string? ResourceId,
    IReadOnlyList<OperationalMetricKind> Metrics,
    int MaxItems)
{
    public const int MaxMetricKinds = 16;
    public const int HardMaxItems = 1_000;

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            ClusterId);

        if (!string.Equals(
                ClusterId,
                ClusterId.Trim(),
                StringComparison.Ordinal) ||
            ClusterId.Length >
            OperationalResourceIdentity.MaxClusterIdLength ||
            ClusterId.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Operational analytics cluster ID is invalid.",
                nameof(ClusterId));
        }

        if (ResourceKind is not null &&
            !Enum.IsDefined(
                typeof(OperationalResourceKind),
                ResourceKind.Value))
        {
            throw new ArgumentOutOfRangeException(
                nameof(ResourceKind));
        }

        if (ResourceId is not null)
        {
            if (ResourceKind is null)
            {
                throw new ArgumentException(
                    "A resource ID requires a resource kind.",
                    nameof(ResourceId));
            }

            if (string.IsNullOrWhiteSpace(
                    ResourceId) ||
                !string.Equals(
                    ResourceId,
                    ResourceId.Trim(),
                    StringComparison.Ordinal) ||
                ResourceId.Length >
                OperationalResourceIdentity.MaxResourceIdLength ||
                ResourceId.Any(char.IsControl))
            {
                throw new ArgumentException(
                    "Operational analytics resource ID is invalid.",
                    nameof(ResourceId));
            }
        }

        ArgumentNullException.ThrowIfNull(
            Metrics);

        if (Metrics.Count is < 1 or > MaxMetricKinds ||
            Metrics.Distinct().Count() != Metrics.Count ||
            Metrics.Any(metric =>
                !Enum.IsDefined(
                    typeof(OperationalMetricKind),
                    metric)))
        {
            throw new ArgumentException(
                "Operational analytics metric selection is invalid or unbounded.",
                nameof(Metrics));
        }

        if (MaxItems is < 1 or > HardMaxItems)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxItems));
        }
    }
}

public interface IOperationalAnalyticsObservationPort
{
    Task<ReadViewResult<IReadOnlyList<OperationalMetricEvidence>>>
        QueryAsync(
            OperationalAnalyticsQuery query,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken);
}
