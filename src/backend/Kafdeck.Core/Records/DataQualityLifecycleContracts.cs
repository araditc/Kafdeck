namespace Kafdeck.Core.Records;

public enum DataQualityPolicyLifecycleState
{
    Disabled = 1,
    Active = 2,
    Paused = 3,
    Retired = 4,
}

public sealed record DataQualityPolicyLifecycleSnapshot
{
    public DataQualityPolicyLifecycleSnapshot(
        DataQualityPolicyDefinition definition,
        DataQualityPolicyLifecycleState state,
        long revision,
        DateTimeOffset updatedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(definition);

        if (!Enum.IsDefined(state))
        {
            throw new ArgumentOutOfRangeException(nameof(state));
        }

        if (revision < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(revision));
        }

        if (updatedAtUtc == default)
        {
            throw new ArgumentOutOfRangeException(nameof(updatedAtUtc));
        }

        Definition = definition;
        State = state;
        Revision = revision;
        UpdatedAtUtc = updatedAtUtc.ToUniversalTime();
    }

    public DataQualityPolicyDefinition Definition { get; }
    public DataQualityPolicyLifecycleState State { get; }
    public long Revision { get; }
    public DateTimeOffset UpdatedAtUtc { get; }
}

public sealed record DataQualityPolicyListQuery
{
    public const int DefaultMaxResults = 100;
    public const int HardMaxResults = 500;

    public DataQualityPolicyListQuery(
        string clusterId,
        int maxResults = DefaultMaxResults,
        DataQualityPolicyLifecycleState? state = null,
        string? afterPolicyId = null)
    {
        DataQualityContractInputBounds.RequireRawString(
            clusterId,
            256,
            nameof(clusterId));
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        if (!string.Equals(
                clusterId,
                clusterId.Trim(),
                StringComparison.Ordinal) ||
            clusterId.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Data-quality cluster ID must be an exact bounded identifier.",
                nameof(clusterId));
        }

        if (maxResults is < 1 or > HardMaxResults)
        {
            throw new ArgumentOutOfRangeException(nameof(maxResults));
        }

        if (state is not null &&
            !Enum.IsDefined(state.Value))
        {
            throw new ArgumentOutOfRangeException(nameof(state));
        }

        if (afterPolicyId is not null)
        {
            DataQualityContractInputBounds.RequireRawString(
                afterPolicyId,
                DataQualityPolicyDefinition.MaxPolicyIdLength,
                nameof(afterPolicyId));
            ArgumentException.ThrowIfNullOrWhiteSpace(afterPolicyId);

            if (!string.Equals(
                    afterPolicyId,
                    afterPolicyId.Trim(),
                    StringComparison.Ordinal) ||
                afterPolicyId.Any(char.IsControl))
            {
                throw new ArgumentException(
                    "Data-quality policy cursor must be an exact bounded policy identifier.",
                    nameof(afterPolicyId));
            }
        }

        ClusterId = clusterId;
        MaxResults = maxResults;
        State = state;
        AfterPolicyId = afterPolicyId;
    }

    public string ClusterId { get; }
    public int MaxResults { get; }
    public DataQualityPolicyLifecycleState? State { get; }
    public string? AfterPolicyId { get; }
}

public sealed record DataQualityPolicyPage
{
    public DataQualityPolicyPage(
        IReadOnlyList<DataQualityPolicyLifecycleSnapshot> items,
        bool truncated,
        string? nextPolicyId)
    {
        ArgumentNullException.ThrowIfNull(items);

        if (items.Count > DataQualityPolicyListQuery.HardMaxResults ||
            items.Any(item => item is null))
        {
            throw new ArgumentException(
                "Data-quality policy page is invalid or unbounded.",
                nameof(items));
        }

        if (truncated)
        {
            DataQualityContractInputBounds.RequireRawString(
                nextPolicyId,
                DataQualityPolicyDefinition.MaxPolicyIdLength,
                nameof(nextPolicyId));
            ArgumentException.ThrowIfNullOrWhiteSpace(nextPolicyId);

            if (!string.Equals(
                    nextPolicyId,
                    nextPolicyId.Trim(),
                    StringComparison.Ordinal) ||
                nextPolicyId.Any(char.IsControl))
            {
                throw new ArgumentException(
                    "Data-quality policy continuation must be an exact bounded policy identifier.",
                    nameof(nextPolicyId));
            }
        }
        else if (nextPolicyId is not null)
        {
            throw new ArgumentException(
                "A complete data-quality policy page cannot expose a continuation cursor.",
                nameof(nextPolicyId));
        }

        Items = Array.AsReadOnly(items.ToArray());
        Truncated = truncated;
        NextPolicyId = nextPolicyId;
    }

    public IReadOnlyList<DataQualityPolicyLifecycleSnapshot> Items { get; }
    public bool Truncated { get; }
    public string? NextPolicyId { get; }
}

public sealed record DataQualityEvidenceQuery
{
    public const int DefaultMaxPoints = 250;
    public const int HardMaxPoints = 2_000;
    public static readonly TimeSpan HardMaxRange = TimeSpan.FromDays(31);

    public DataQualityEvidenceQuery(
        string policyId,
        DateTimeOffset fromUtc,
        DateTimeOffset toUtc,
        int maxPoints = DefaultMaxPoints)
    {
        DataQualityContractInputBounds.RequireRawString(
            policyId,
            DataQualityPolicyDefinition.MaxPolicyIdLength,
            nameof(policyId));
        ArgumentException.ThrowIfNullOrWhiteSpace(policyId);

        if (!string.Equals(
                policyId,
                policyId.Trim(),
                StringComparison.Ordinal) ||
            policyId.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Data-quality policy ID must be an exact bounded identifier.",
                nameof(policyId));
        }

        if (fromUtc == default ||
            toUtc <= fromUtc ||
            toUtc - fromUtc > HardMaxRange)
        {
            throw new ArgumentException(
                "Data-quality evidence query range is invalid or exceeds the server-owned maximum.");
        }

        if (maxPoints is < 1 or > HardMaxPoints)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPoints));
        }

        PolicyId = policyId;
        FromUtc = fromUtc.ToUniversalTime();
        ToUtc = toUtc.ToUniversalTime();
        MaxPoints = maxPoints;
    }

    public string PolicyId { get; }
    public DateTimeOffset FromUtc { get; }
    public DateTimeOffset ToUtc { get; }
    public int MaxPoints { get; }
}

public sealed record DataQualityEvidencePoint
{
    public const string BoundedEvaluatorSource =
        "bounded-record-evaluator";

    public DataQualityEvidencePoint(
        DataQualityEvaluationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        if (!string.Equals(
                result.Evidence.Source,
                BoundedEvaluatorSource,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Only closed server-derived data-quality evidence sources may be persisted.",
                nameof(result));
        }

        Evidence = result.Evidence;
        Progress = result.Progress;
    }

    public DataQualityAggregateEvidence Evidence { get; }
    public DataQualityEvaluationProgress Progress { get; }
}

public sealed record DataQualityEvidencePage
{
    public DataQualityEvidencePage(
        IReadOnlyList<DataQualityEvidencePoint> points,
        bool truncated)
    {
        ArgumentNullException.ThrowIfNull(points);

        if (points.Count > DataQualityEvidenceQuery.HardMaxPoints ||
            points.Any(point =>
                point is null ||
                point.Evidence is null ||
                point.Progress is null))
        {
            throw new ArgumentException(
                "Data-quality evidence page is invalid or unbounded.",
                nameof(points));
        }

        Points = Array.AsReadOnly(points.ToArray());
        Truncated = truncated;
    }

    public IReadOnlyList<DataQualityEvidencePoint> Points { get; }
    public bool Truncated { get; }
}

public interface IDataQualityLifecycleStore
{
    Task InitializeAsync(
        CancellationToken cancellationToken = default);

    Task<DataQualityPolicyLifecycleSnapshot?> GetPolicyAsync(
        string policyId,
        CancellationToken cancellationToken = default);

    Task<DataQualityPolicyPage> ListPoliciesAsync(
        DataQualityPolicyListQuery query,
        CancellationToken cancellationToken = default);

    Task<DataQualityPolicyLifecycleSnapshot> CreatePolicyAsync(
        DataQualityPolicyDefinition definition,
        DataQualityPolicyLifecycleState state,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<DataQualityPolicyLifecycleSnapshot?> ReplacePolicyAsync(
        DataQualityPolicyDefinition definition,
        DataQualityPolicyLifecycleState state,
        long expectedRevision,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task<DataQualityPolicyLifecycleSnapshot?> SetPolicyStateAsync(
        string policyId,
        DataQualityPolicyLifecycleState state,
        long expectedRevision,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken = default);

    Task AppendEvidenceAsync(
        DataQualityEvidencePoint point,
        CancellationToken cancellationToken = default);

    Task<DataQualityEvidencePage> QueryEvidenceAsync(
        DataQualityEvidenceQuery query,
        CancellationToken cancellationToken = default);
}
