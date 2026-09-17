using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Topics;

public enum TopicAnomalyState
{
    Healthy = 1,
    Degraded = 2,
    Unknown = 3,
}

public enum PartitionHealthState
{
    Healthy = 1,
    NoLeader = 2,
    UnderReplicated = 3,
}

public sealed record TopicListRequest(
    string? Search = null,
    int PageSize = 50,
    string? Cursor = null);

public sealed record TopicListItem(
    string Name,
    int PartitionCount,
    bool IsInternal,
    int? OfflinePartitionCount,
    int? UnderReplicatedPartitionCount,
    TopicAnomalyState AnomalyState);

public sealed record TopicPage(
    IReadOnlyList<TopicListItem> Items,
    string? NextCursor,
    ObservationMetadata Observation);

public sealed record PartitionProjection(
    int PartitionId,
    int? LeaderBrokerId,
    IReadOnlyList<int> ReplicaBrokerIds,
    IReadOnlyList<int> InSyncReplicaBrokerIds,
    IReadOnlyList<int> OutOfSyncReplicaBrokerIds,
    PartitionHealthState Health,
    IReadOnlyList<string> HealthReasons);

public sealed record TopicDetailProjection(
    string Name,
    bool IsInternal,
    IReadOnlyList<PartitionProjection> Partitions,
    int OfflinePartitionCount,
    int UnderReplicatedPartitionCount,
    TopicAnomalyState AnomalyState,
    ObservationMetadata Observation);

/// <summary>
/// Read-only topic/partition application service over W05 snapshots. List projection is
/// performed entirely in memory after one bounded Kafka list observation, preventing N+1 reads.
/// </summary>
public sealed class TopicExplorerService
{
    public const int DefaultPageSize = 50;
    public const int MaximumPageSize = 200;
    private static readonly TimeSpan ConfigurationTtl = TimeSpan.FromSeconds(30);

    private readonly IKafkaAdministrationPort _kafka;
    private readonly KafkaSnapshotCoordinator _snapshots;
    private readonly KafkaSnapshotPolicy _policy;

    public TopicExplorerService(
        IKafkaAdministrationPort kafka,
        KafkaSnapshotCoordinator snapshots,
        KafkaSnapshotPolicy? policy = null)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _snapshots = snapshots ?? throw new ArgumentNullException(nameof(snapshots));
        _policy = policy ?? new KafkaSnapshotPolicy();
    }

    public async Task<KafkaResult<TopicPage>> ListTopicsAsync(
        string clusterId,
        TopicListRequest? request = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        request ??= new TopicListRequest(PageSize: DefaultPageSize);
        ValidatePageSize(request.PageSize);

        var search = NormalizeSearch(request.Search);
        var snapshot = await _snapshots.ObserveAsync(
                clusterId,
                "topics",
                _policy.TopicMetadataTtl,
                (operation, token) => _kafka.ListTopicsAsync(clusterId, operation, token),
                cancellationToken)
            .ConfigureAwait(false);

        if (!snapshot.IsSuccess || snapshot.Value is null)
        {
            return KafkaResult<TopicPage>.Failed(snapshot.Failure!, snapshot.Observation);
        }

        var filtered = snapshot.Value
            .Where(topic => search.Length == 0 || topic.Name.Contains(search, StringComparison.OrdinalIgnoreCase))
            .OrderBy(topic => topic.Name, StringComparer.Ordinal)
            .ToArray();

        var startIndex = 0;
        if (!string.IsNullOrWhiteSpace(request.Cursor))
        {
            var cursor = DecodeCursor(request.Cursor);
            if (!string.Equals(cursor.Search, search, StringComparison.Ordinal))
            {
                throw new ArgumentException("Cursor does not match the active search query.", nameof(request));
            }

            if (cursor.ObservedAtUtcTicks != snapshot.Observation.ObservedAtUtc.UtcDateTime.Ticks)
            {
                throw new InvalidOperationException(
                    "Cursor no longer matches the active topic snapshot; restart pagination from the first page.");
            }

            var previousIndex = Array.FindIndex(
                filtered,
                topic => string.Equals(topic.Name, cursor.LastTopicName, StringComparison.Ordinal));
            if (previousIndex < 0)
            {
                throw new InvalidOperationException(
                    "Cursor topic is not present in the active snapshot; restart pagination from the first page.");
            }

            startIndex = previousIndex + 1;
        }

        var pageSource = filtered
            .Skip(startIndex)
            .Take(request.PageSize)
            .ToArray();

        var items = pageSource.Select(ProjectSummary).ToArray();
        var hasMore = startIndex + pageSource.Length < filtered.Length;
        var nextCursor = hasMore && pageSource.Length > 0
            ? EncodeCursor(new TopicCursor(
                snapshot.Observation.ObservedAtUtc.UtcDateTime.Ticks,
                search,
                pageSource[^1].Name))
            : null;

        return KafkaResult<TopicPage>.Success(
            new TopicPage(items, nextCursor, snapshot.Observation),
            snapshot.Observation);
    }

    public async Task<KafkaResult<TopicDetailProjection>> GetTopicAsync(
        string clusterId,
        string topicName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        var metadata = await _snapshots.ObserveAsync(
                clusterId,
                $"topic:{topicName}",
                _policy.TopicMetadataTtl,
                (operation, token) => _kafka.GetTopicMetadataAsync(clusterId, topicName, operation, token),
                cancellationToken)
            .ConfigureAwait(false);

        if (!metadata.IsSuccess || metadata.Value is null)
        {
            return KafkaResult<TopicDetailProjection>.Failed(metadata.Failure!, metadata.Observation);
        }

        var partitions = metadata.Value.Partitions
            .OrderBy(partition => partition.PartitionId)
            .Select(ProjectPartition)
            .ToArray();
        var offlineCount = partitions.Count(partition => partition.Health == PartitionHealthState.NoLeader);
        var underReplicatedCount = partitions.Count(partition =>
            partition.InSyncReplicaBrokerIds.Count < partition.ReplicaBrokerIds.Count);
        var anomalyState = offlineCount > 0 || underReplicatedCount > 0
            ? TopicAnomalyState.Degraded
            : TopicAnomalyState.Healthy;

        var projection = new TopicDetailProjection(
            metadata.Value.Name,
            metadata.Value.IsInternal,
            partitions,
            offlineCount,
            underReplicatedCount,
            anomalyState,
            metadata.Observation);

        return KafkaResult<TopicDetailProjection>.Success(projection, metadata.Observation);
    }

    public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
        string clusterId,
        string topicName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        return _snapshots.ObserveAsync(
            clusterId,
            $"topic-config:{topicName}",
            ConfigurationTtl,
            (operation, token) => _kafka.GetTopicConfigurationAsync(clusterId, topicName, operation, token),
            cancellationToken);
    }

    private static TopicListItem ProjectSummary(TopicSummary summary)
    {
        var anomalyState = summary.OfflinePartitionCount.HasValue && summary.UnderReplicatedPartitionCount.HasValue
            ? summary.OfflinePartitionCount.Value > 0 || summary.UnderReplicatedPartitionCount.Value > 0
                ? TopicAnomalyState.Degraded
                : TopicAnomalyState.Healthy
            : TopicAnomalyState.Unknown;

        return new TopicListItem(
            summary.Name,
            summary.PartitionCount,
            summary.IsInternal,
            summary.OfflinePartitionCount,
            summary.UnderReplicatedPartitionCount,
            anomalyState);
    }

    private static PartitionProjection ProjectPartition(PartitionMetadata partition)
    {
        var outOfSync = partition.ReplicaBrokerIds
            .Except(partition.InSyncReplicaBrokerIds)
            .OrderBy(id => id)
            .ToArray();

        var reasons = new List<string>();
        PartitionHealthState health;
        if (!partition.LeaderBrokerId.HasValue)
        {
            health = PartitionHealthState.NoLeader;
            reasons.Add("No partition leader is currently observable.");
            if (outOfSync.Length > 0)
            {
                reasons.Add("One or more replicas are not present in the in-sync replica set.");
            }
        }
        else if (outOfSync.Length > 0)
        {
            health = PartitionHealthState.UnderReplicated;
            reasons.Add("One or more replicas are not present in the in-sync replica set.");
        }
        else
        {
            health = PartitionHealthState.Healthy;
            reasons.Add("Leader and replica evidence is currently consistent.");
        }

        return new PartitionProjection(
            partition.PartitionId,
            partition.LeaderBrokerId,
            partition.ReplicaBrokerIds.OrderBy(id => id).ToArray(),
            partition.InSyncReplicaBrokerIds.OrderBy(id => id).ToArray(),
            outOfSync,
            health,
            reasons);
    }

    private static void ValidatePageSize(int pageSize)
    {
        if (pageSize is < 1 or > MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize),
                $"Page size must be between 1 and {MaximumPageSize}.");
        }
    }

    private static string NormalizeSearch(string? search) => search?.Trim() ?? string.Empty;

    private static string EncodeCursor(TopicCursor cursor)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(cursor);
        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    private static TopicCursor DecodeCursor(string encoded)
    {
        try
        {
            var normalized = encoded
                .Replace('-', '+')
                .Replace('_', '/');
            var padding = normalized.Length % 4;
            if (padding != 0)
            {
                normalized += new string('=', 4 - padding);
            }

            var bytes = Convert.FromBase64String(normalized);
            return JsonSerializer.Deserialize<TopicCursor>(bytes)
                ?? throw new FormatException("Cursor payload is empty.");
        }
        catch (Exception exception) when (exception is FormatException or JsonException)
        {
            throw new ArgumentException("Cursor is invalid.", nameof(encoded), exception);
        }
    }

    private sealed record TopicCursor(
        long ObservedAtUtcTicks,
        string Search,
        string LastTopicName);
}
