using Kafdeck.Core.Consumers;
using Kafdeck.Core.ReadViews;

namespace Kafdeck.Modules.Consumers;

public sealed record ConsumerExplorerPolicy(
    TimeSpan OperationTimeout,
    int MaxItems,
    long MaxResponseBytes)
{
    public static ConsumerExplorerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10), 2_000, 4 * 1024 * 1024);
}

public sealed class ConsumerExplorerService
{
    private readonly IConsumerGroupReadPort _consumerGroups;
    private readonly ConsumerExplorerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public ConsumerExplorerService(
        IConsumerGroupReadPort consumerGroups,
        ConsumerExplorerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _consumerGroups = consumerGroups ?? throw new ArgumentNullException(nameof(consumerGroups));
        _policy = policy ?? ConsumerExplorerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (_policy.OperationTimeout <= TimeSpan.Zero ||
            _policy.OperationTimeout > TimeSpan.FromMinutes(1))
        {
            throw new ArgumentOutOfRangeException(nameof(policy), "Consumer operation timeout must be between zero and one minute.");
        }

        _ = new ReadViewOperationContext(
            _timeProvider.GetUtcNow().Add(_policy.OperationTimeout),
            _policy.MaxItems,
            _policy.MaxResponseBytes);
    }

    public Task<ReadViewResult<IReadOnlyList<ConsumerGroupSummary>>> ListGroupsAsync(
        string clusterId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        return _consumerGroups.ListGroupsAsync(clusterId, Operation(), cancellationToken);
    }

    public Task<ReadViewResult<ConsumerGroupDetail>> GetGroupAsync(
        string clusterId,
        string groupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        return _consumerGroups.GetGroupAsync(clusterId, groupId, Operation(), cancellationToken);
    }

    public async Task<ReadViewResult<ConsumerLagProjection>> GetLagAsync(
        string clusterId,
        string groupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);

        var result = await _consumerGroups.GetOffsetsAsync(
                clusterId,
                groupId,
                Operation(),
                cancellationToken)
            .ConfigureAwait(false);

        if (!result.IsSuccess || result.Value is null)
        {
            return ReadViewResult<ConsumerLagProjection>.Failed(result.Failure!);
        }

        return ReadViewResult<ConsumerLagProjection>.Success(
            BuildLagProjection(groupId, result.Value),
            result.Limitations);
    }

    internal static ConsumerLagProjection BuildLagProjection(
        string groupId,
        IReadOnlyList<ConsumerOffsetProjection> partitions)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentNullException.ThrowIfNull(partitions);

        var observed = partitions
            .Where(partition =>
                partition.State == ConsumerOffsetState.Observed &&
                partition.Lag.HasValue)
            .ToArray();

        var limitations = partitions
            .Where(partition => partition.State != ConsumerOffsetState.Observed)
            .GroupBy(partition => partition.State)
            .OrderBy(group => group.Key)
            .Select(group => new ReadViewLimitation(
                $"consumer_offset_{group.Key.ToString().ToLowerInvariant()}",
                $"{group.Count()} partition observation(s) are {Describe(group.Key)}."))
            .ToArray();

        var isPartial = limitations.Length > 0;
        long? totalLag = observed.Length == 0 && partitions.Count > 0
            ? null
            : observed.Aggregate(0L, (total, partition) => checked(total + partition.Lag!.Value));

        return new ConsumerLagProjection(
            groupId.Trim(),
            partitions,
            totalLag,
            isPartial,
            limitations);
    }

    private ReadViewOperationContext Operation() =>
        new(
            _timeProvider.GetUtcNow().Add(_policy.OperationTimeout),
            _policy.MaxItems,
            _policy.MaxResponseBytes);

    private static string Describe(ConsumerOffsetState state) => state switch
    {
        ConsumerOffsetState.MissingCommittedOffset => "missing a committed offset",
        ConsumerOffsetState.EndOffsetUnavailable => "missing a current end offset",
        ConsumerOffsetState.TopicPartitionUnavailable => "unavailable",
        ConsumerOffsetState.Unauthorized => "not authorized",
        ConsumerOffsetState.CommittedOffsetOutOfRange => "out of the observed log range",
        _ => "not fully observable",
    };
}
