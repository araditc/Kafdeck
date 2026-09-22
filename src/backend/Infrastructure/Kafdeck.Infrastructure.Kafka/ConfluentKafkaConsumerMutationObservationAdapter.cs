using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using CoreConsumerGroupState = Kafdeck.Core.Consumers.ConsumerGroupState;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaConsumerMutationObservationAdapter :
    IConsumerMutationObservationPort,
    IDisposable
{
    private const int HardMaxTargets = 256;

    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaConsumerMutationObservationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<KafkaResult<ConsumerMutationObservation>> ObserveAsync(
        string clusterId,
        string groupId,
        IReadOnlyList<ConsumerMutationObservationTarget> targets,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentNullException.ThrowIfNull(targets);

        if (targets.Count > HardMaxTargets)
            throw new ArgumentOutOfRangeException(nameof(targets));

        var normalizedTargets = targets
            .Select(target =>
            {
                ArgumentNullException.ThrowIfNull(target);
                if (string.IsNullOrWhiteSpace(target.Topic) ||
                    target.Partition < 0)
                {
                    throw new ArgumentException(
                        "Consumer mutation observation target is invalid.",
                        nameof(targets));
                }

                return target with { Topic = target.Topic.Trim() };
            })
            .GroupBy(target => (target.Topic, target.Partition))
            .Select(group =>
            {
                var distinctTimestamps = group
                    .Select(item => item.ResolveTimestampUtc?.ToUniversalTime())
                    .Distinct()
                    .ToArray();
                if (distinctTimestamps.Length != 1)
                {
                    throw new ArgumentException(
                        "A consumer mutation partition cannot request conflicting timestamp observations.",
                        nameof(targets));
                }

                return new ConsumerMutationObservationTarget(
                    group.Key.Topic,
                    group.Key.Partition,
                    distinctTimestamps[0]);
            })
            .OrderBy(target => target.Topic, StringComparer.Ordinal)
            .ThenBy(target => target.Partition)
            .ToArray();

        return ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                ConsumerGroupDescription group;
                try
                {
                    var described = await client.DescribeConsumerGroupsAsync(
                            [groupId],
                            new DescribeConsumerGroupsOptions
                            {
                                RequestTimeout = timeout,
                            })
                        .WaitAsync(token)
                        .ConfigureAwait(false);

                    group = described.ConsumerGroupDescriptions.Single();
                }
                catch (DescribeConsumerGroupsException exception)
                {
                    var report = exception.Results.ConsumerGroupDescriptions.Single();
                    if (report.Error.Code == ErrorCode.GroupIdNotFound)
                    {
                        return new ConsumerMutationObservation(
                            groupId,
                            false,
                            CoreConsumerGroupState.Dead,
                            Array.Empty<ConsumerMemberProjection>(),
                            Array.Empty<ConsumerMutationPartitionObservation>());
                    }

                    throw new KafkaException(report.Error);
                }

                if (group.Error.IsError)
                    throw new KafkaException(group.Error);

                var mappedState =
                    ConfluentKafkaConsumerGroupReadAdapter.MapState(group.State);
                if (mappedState == CoreConsumerGroupState.Dead)
                {
                    // Kafka DescribeGroups represents a non-existent/deleted
                    // consumer group as DEAD. Treat that structured state as
                    // absence so delete verification does not wait forever for
                    // a GroupIdNotFound error that Kafka may never return.
                    return new ConsumerMutationObservation(
                        groupId,
                        false,
                        CoreConsumerGroupState.Dead,
                        Array.Empty<ConsumerMemberProjection>(),
                        Array.Empty<ConsumerMutationPartitionObservation>());
                }

                var members = group.Members
                    .OrderBy(member => member.ConsumerId, StringComparer.Ordinal)
                    .Select(member => new ConsumerMemberProjection(
                        member.ConsumerId,
                        NullIfBlank(member.GroupInstanceId),
                        NullIfBlank(member.ClientId),
                        NullIfBlank(member.Host),
                        member.Assignment.TopicPartitions
                            .OrderBy(item => item.Topic, StringComparer.Ordinal)
                            .ThenBy(item => item.Partition.Value)
                            .Select(item => new ConsumerPartitionAssignment(
                                item.Topic,
                                item.Partition.Value))
                            .ToArray()))
                    .ToArray();

                if (normalizedTargets.Length == 0)
                {
                    return new ConsumerMutationObservation(
                        group.GroupId,
                        true,
                        mappedState,
                        members,
                        Array.Empty<ConsumerMutationPartitionObservation>());
                }

                var partitions = normalizedTargets
                    .Select(target => new TopicPartition(
                        target.Topic,
                        new Partition(target.Partition)))
                    .ToArray();

                var committedResults = await client.ListConsumerGroupOffsetsAsync(
                        [new ConsumerGroupTopicPartitions(groupId, partitions.ToList())],
                        new ListConsumerGroupOffsetsOptions
                        {
                            RequestTimeout = timeout,
                            RequireStableOffsets = true,
                        })
                    .WaitAsync(token)
                    .ConfigureAwait(false);

                var committed = committedResults.Single();
                var committedByPartition = new Dictionary<TopicPartition, long?>();
                foreach (var item in committed.Partitions)
                {
                    if (item.Error.IsError)
                        throw new KafkaException(item.Error);

                    committedByPartition[item.TopicPartition] =
                        item.Offset.Value >= 0 ? item.Offset.Value : null;
                }

                var earliest = await ListOffsetsAsync(
                        client,
                        partitions,
                        OffsetSpec.Earliest(),
                        timeout,
                        token)
                    .ConfigureAwait(false);
                var latest = await ListOffsetsAsync(
                        client,
                        partitions,
                        OffsetSpec.Latest(),
                        timeout,
                        token)
                    .ConfigureAwait(false);

                var timestampOffsets =
                    new Dictionary<TopicPartition, long?>();
                var timestampTargets = normalizedTargets
                    .Where(target => target.ResolveTimestampUtc.HasValue)
                    .ToArray();
                if (timestampTargets.Length > 0)
                {
                    var specs = timestampTargets
                        .Select(target => new TopicPartitionOffsetSpec
                        {
                            TopicPartition = new TopicPartition(
                                target.Topic,
                                new Partition(target.Partition)),
                            OffsetSpec = OffsetSpec.ForTimestamp(
                                target.ResolveTimestampUtc!.Value
                                    .ToUnixTimeMilliseconds()),
                        })
                        .ToArray();

                    var result = await client.ListOffsetsAsync(
                            specs,
                            new ListOffsetsOptions { RequestTimeout = timeout })
                        .WaitAsync(token)
                        .ConfigureAwait(false);

                    foreach (var item in result.ResultInfos)
                    {
                        var offset = item.TopicPartitionOffsetError;
                        if (offset.Error.IsError)
                            throw new KafkaException(offset.Error);

                        timestampOffsets[offset.TopicPartition] =
                            offset.Offset.Value >= 0
                                ? offset.Offset.Value
                                : null;
                    }
                }

                var observations =
                    new List<ConsumerMutationPartitionObservation>(
                        normalizedTargets.Length);
                foreach (var target in normalizedTargets)
                {
                    var partition = new TopicPartition(
                        target.Topic,
                        new Partition(target.Partition));

                    if (!committedByPartition.TryGetValue(
                            partition,
                            out var committedOffset) ||
                        !earliest.TryGetValue(partition, out var low) ||
                        !latest.TryGetValue(partition, out var high))
                    {
                        throw new InvalidOperationException(
                            "Kafka returned an incomplete consumer mutation observation.");
                    }

                    long? timestampOffset = null;
                    if (target.ResolveTimestampUtc.HasValue &&
                        !timestampOffsets.TryGetValue(
                            partition,
                            out timestampOffset))
                    {
                        throw new InvalidOperationException(
                            "Kafka returned an incomplete timestamp offset observation.");
                    }

                    observations.Add(
                        new ConsumerMutationPartitionObservation(
                            target.Topic,
                            target.Partition,
                            committedOffset,
                            low,
                            high,
                            timestampOffset));
                }

                return new ConsumerMutationObservation(
                    group.GroupId,
                    true,
                    mappedState,
                    members,
                    observations);
            });
    }

    public void Dispose() => _clients.Dispose();

    private static async Task<IReadOnlyDictionary<TopicPartition, long>>
        ListOffsetsAsync(
            IAdminClient client,
            IReadOnlyList<TopicPartition> partitions,
            OffsetSpec spec,
            TimeSpan timeout,
            CancellationToken cancellationToken)
    {
        var result = await client.ListOffsetsAsync(
                partitions.Select(partition => new TopicPartitionOffsetSpec
                {
                    TopicPartition = partition,
                    OffsetSpec = spec,
                }),
                new ListOffsetsOptions { RequestTimeout = timeout })
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        var values = new Dictionary<TopicPartition, long>();
        foreach (var info in result.ResultInfos)
        {
            var offset = info.TopicPartitionOffsetError;
            if (offset.Error.IsError)
                throw new KafkaException(offset.Error);
            if (offset.Offset.Value < 0)
            {
                throw new InvalidOperationException(
                    "Kafka returned an invalid watermark offset.");
            }

            values[offset.TopicPartition] = offset.Offset.Value;
        }

        return values;
    }

    private async Task<KafkaResult<T>> ExecuteAsync<T>(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken,
        Func<IAdminClient, TimeSpan, CancellationToken, Task<T>> action)
    {
        if (cancellationToken.IsCancellationRequested)
            return Failed<T>(KafkaFailureMapper.Cancelled());

        var now = _timeProvider.GetUtcNow();
        if (operation.IsExpired(now))
            return Failed<T>(KafkaFailureMapper.DeadlineExceeded());

        if (!_clients.ContainsCluster(clusterId))
            return Failed<T>(KafkaFailureMapper.ClusterNotConfigured());

        try
        {
            var client = _clients.GetClient(clusterId);
            var remaining = operation.Remaining(_timeProvider.GetUtcNow());
            if (remaining <= TimeSpan.Zero)
                return Failed<T>(KafkaFailureMapper.DeadlineExceeded());

            using var deadline =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            deadline.CancelAfter(remaining);

            var value = await action(
                    client,
                    remaining,
                    deadline.Token)
                .ConfigureAwait(false);

            return KafkaResult<T>.Success(value, LiveObservation());
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(KafkaFailureMapper.Cancelled());
        }
        catch (OperationCanceledException)
        {
            return Failed<T>(KafkaFailureMapper.DeadlineExceeded());
        }
        catch (TimeoutException)
        {
            return Failed<T>(KafkaFailureMapper.DeadlineExceeded());
        }
        catch (KafdeckConfigurationException)
        {
            return Failed<T>(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (KeyNotFoundException)
        {
            return Failed<T>(KafkaFailureMapper.ClusterNotConfigured());
        }
        catch (KafkaException exception)
        {
            return Failed<T>(KafkaFailureMapper.FromKafka(exception.Error));
        }
        catch (ArgumentException)
        {
            return Failed<T>(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (InvalidOperationException)
        {
            return Failed<T>(KafkaFailureMapper.Unknown());
        }
        catch
        {
            return Failed<T>(KafkaFailureMapper.Unknown());
        }
    }

    private KafkaResult<T> Failed<T>(KafkaFailure failure) =>
        KafkaResult<T>.Failed(failure, LiveObservation());

    private ObservationMetadata LiveObservation()
    {
        var observedAt = _timeProvider.GetUtcNow();
        return new ObservationMetadata(
            observedAt,
            observedAt,
            observedAt,
            ObservationSource.Live);
    }

    private static string? NullIfBlank(string? value) =>
        string.IsNullOrWhiteSpace(value)
            ? null
            : value.Trim();
}
