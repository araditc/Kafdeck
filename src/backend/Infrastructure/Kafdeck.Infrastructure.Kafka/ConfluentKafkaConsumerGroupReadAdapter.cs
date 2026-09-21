using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;
using CoreConsumerGroupState = Kafdeck.Core.Consumers.ConsumerGroupState;
using KafkaConsumerGroupState = Confluent.Kafka.ConsumerGroupState;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaConsumerGroupReadAdapter : IConsumerGroupReadPort, IDisposable
{
    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaConsumerGroupReadAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));

        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<ReadViewResult<IReadOnlyList<ConsumerGroupSummary>>> ListGroupsAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync<IReadOnlyList<ConsumerGroupSummary>>(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                var result = await client.ListConsumerGroupsAsync(
                        new ListConsumerGroupsOptions { RequestTimeout = timeout })
                    .WaitAsync(token)
                    .ConfigureAwait(false);

                // Confluent.Kafka 2.15.x completes ListConsumerGroupsAsync with
                // ListConsumerGroupsException when any constituent result is in error.
                // Therefore reaching this point means the returned list is complete.
                var groups = result.Valid
                    .OrderBy(group => group.GroupId, StringComparer.Ordinal)
                    .Take(operation.MaxItems + 1)
                    .ToArray();

                if (groups.Length > operation.MaxItems)
                {
                    throw new ResponseBoundExceededException();
                }

                EnsureStringBudget(
                    groups.Select(group => group.GroupId),
                    operation.MaxResponseBytes);

                return groups
                    .Select(group => new ConsumerGroupSummary(
                        group.GroupId,
                        MapState(group.State),
                        null,
                        group.IsSimpleConsumerGroup))
                    .ToArray();
            });

    public Task<ReadViewResult<ConsumerGroupDetail>> GetGroupAsync(
        string clusterId,
        string groupId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);

        return ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                var description = await DescribeGroupAsync(client, groupId, timeout, token)
                    .ConfigureAwait(false);

                if (description.Members.Count > operation.MaxItems)
                {
                    throw new ResponseBoundExceededException();
                }

                var assignmentCount = description.Members.Sum(member => member.Assignment.TopicPartitions.Count);
                if (assignmentCount > operation.MaxItems)
                {
                    throw new ResponseBoundExceededException();
                }

                EnsureStringBudget(
                    description.Members.SelectMany(member => new[]
                    {
                        member.ConsumerId,
                        member.GroupInstanceId ?? string.Empty,
                        member.ClientId ?? string.Empty,
                        member.Host ?? string.Empty,
                    }),
                    operation.MaxResponseBytes);

                var members = description.Members
                    .OrderBy(member => member.ConsumerId, StringComparer.Ordinal)
                    .Select(member => new ConsumerMemberProjection(
                        member.ConsumerId,
                        NullIfBlank(member.GroupInstanceId),
                        NullIfBlank(member.ClientId),
                        NullIfBlank(member.Host),
                        member.Assignment.TopicPartitions
                            .OrderBy(partition => partition.Topic, StringComparer.Ordinal)
                            .ThenBy(partition => partition.Partition.Value)
                            .Select(partition => new ConsumerPartitionAssignment(
                                partition.Topic,
                                partition.Partition.Value))
                            .ToArray()))
                    .ToArray();

                return new ConsumerGroupDetail(
                    description.GroupId,
                    MapState(description.State),
                    description.GroupType.ToString(),
                    NullIfBlank(description.PartitionAssignor),
                    description.Coordinator is null
                        ? null
                        : $"{description.Coordinator.Host}:{description.Coordinator.Port} ({description.Coordinator.Id})",
                    members);
            });
    }

    public Task<ReadViewResult<IReadOnlyList<ConsumerOffsetProjection>>> GetOffsetsAsync(
        string clusterId,
        string groupId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);

        return ExecuteAsync<IReadOnlyList<ConsumerOffsetProjection>>(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                var description = await DescribeGroupAsync(client, groupId, timeout, token)
                    .ConfigureAwait(false);

                var assignedPartitions = description.Members
                    .SelectMany(member => member.Assignment.TopicPartitions)
                    .Distinct()
                    .ToArray();

                var committedResults = await client.ListConsumerGroupOffsetsAsync(
                        [new ConsumerGroupTopicPartitions(groupId, [])],
                        new ListConsumerGroupOffsetsOptions
                        {
                            RequestTimeout = timeout,
                            RequireStableOffsets = true,
                        })
                    .WaitAsync(token)
                    .ConfigureAwait(false);

                var committed = committedResults.Single();
                var committedByPartition = committed.Partitions
                    .GroupBy(partition => partition.TopicPartition)
                    .ToDictionary(group => group.Key, group => group.First());

                var relevantPartitions = committedByPartition.Keys
                    .Concat(assignedPartitions)
                    .Distinct()
                    .OrderBy(partition => partition.Topic, StringComparer.Ordinal)
                    .ThenBy(partition => partition.Partition.Value)
                    .Take(operation.MaxItems + 1)
                    .ToArray();

                if (relevantPartitions.Length > operation.MaxItems)
                {
                    throw new ResponseBoundExceededException();
                }

                EnsureStringBudget(
                    relevantPartitions.Select(partition => partition.Topic),
                    operation.MaxResponseBytes);

                var endOffsets = new Dictionary<TopicPartition, TopicPartitionOffsetError>();
                if (relevantPartitions.Length > 0)
                {
                    var offsetSpecs = relevantPartitions
                        .Select(partition => new TopicPartitionOffsetSpec
                        {
                            TopicPartition = partition,
                            OffsetSpec = OffsetSpec.Latest(),
                        })
                        .ToArray();

                    var endResult = await client.ListOffsetsAsync(
                            offsetSpecs,
                            new ListOffsetsOptions { RequestTimeout = timeout })
                        .WaitAsync(token)
                        .ConfigureAwait(false);

                    foreach (var item in endResult.ResultInfos)
                    {
                        endOffsets[item.TopicPartitionOffsetError.TopicPartition] =
                            item.TopicPartitionOffsetError;
                    }
                }

                return relevantPartitions
                    .Select(partition =>
                    {
                        committedByPartition.TryGetValue(partition, out var committedOffset);
                        return ProjectOffset(partition, committedOffset, endOffsets);
                    })
                    .ToArray();
            });
    }

    public void Dispose() => _clients.Dispose();

    internal static ConsumerOffsetProjection ProjectOffset(
        TopicPartition topicPartition,
        TopicPartitionOffsetError? committed,
        IReadOnlyDictionary<TopicPartition, TopicPartitionOffsetError> endOffsets)
    {
        var topic = topicPartition.Topic;
        var partition = topicPartition.Partition.Value;

        if (committed is not null && committed.Error.IsError)
        {
            return new ConsumerOffsetProjection(
                topic,
                partition,
                null,
                null,
                null,
                IsAuthorization(committed.Error)
                    ? ConsumerOffsetState.Unauthorized
                    : ConsumerOffsetState.TopicPartitionUnavailable);
        }

        if (!endOffsets.TryGetValue(topicPartition, out var end))
        {
            return new ConsumerOffsetProjection(
                topic,
                partition,
                committed is { Offset.Value: >= 0 } ? committed.Offset.Value : null,
                null,
                null,
                committed is null || committed.Offset.Value < 0
                    ? ConsumerOffsetState.MissingCommittedOffset
                    : ConsumerOffsetState.EndOffsetUnavailable);
        }

        if (end.Error.IsError)
        {
            return new ConsumerOffsetProjection(
                topic,
                partition,
                committed is { Offset.Value: >= 0 } ? committed.Offset.Value : null,
                null,
                null,
                IsAuthorization(end.Error)
                    ? ConsumerOffsetState.Unauthorized
                    : ConsumerOffsetState.EndOffsetUnavailable);
        }

        var endValue = end.Offset.Value;
        if (committed is null || committed.Offset.Value < 0)
        {
            return new ConsumerOffsetProjection(
                topic,
                partition,
                null,
                endValue >= 0 ? endValue : null,
                null,
                ConsumerOffsetState.MissingCommittedOffset);
        }

        var committedValue = committed.Offset.Value;
        if (endValue < 0)
        {
            return new ConsumerOffsetProjection(
                topic,
                partition,
                committedValue,
                null,
                null,
                ConsumerOffsetState.EndOffsetUnavailable);
        }

        if (committedValue > endValue)
        {
            return new ConsumerOffsetProjection(
                topic,
                partition,
                committedValue,
                endValue,
                null,
                ConsumerOffsetState.CommittedOffsetOutOfRange);
        }

        return new ConsumerOffsetProjection(
            topic,
            partition,
            committedValue,
            endValue,
            endValue - committedValue,
            ConsumerOffsetState.Observed);
    }

    internal static CoreConsumerGroupState MapState(KafkaConsumerGroupState state) => state switch
    {
        KafkaConsumerGroupState.PreparingRebalance => CoreConsumerGroupState.PreparingRebalance,
        KafkaConsumerGroupState.CompletingRebalance => CoreConsumerGroupState.CompletingRebalance,
        KafkaConsumerGroupState.Stable => CoreConsumerGroupState.Stable,
        KafkaConsumerGroupState.Empty => CoreConsumerGroupState.Empty,
        KafkaConsumerGroupState.Dead => CoreConsumerGroupState.Dead,
        _ => CoreConsumerGroupState.Unknown,
    };

    private static async Task<ConsumerGroupDescription> DescribeGroupAsync(
        IAdminClient client,
        string groupId,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var result = await client.DescribeConsumerGroupsAsync(
                [groupId],
                new DescribeConsumerGroupsOptions { RequestTimeout = timeout })
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        var description = result.ConsumerGroupDescriptions.Single();
        if (description.Error.IsError)
        {
            throw new KafkaException(description.Error);
        }

        return description;
    }

    private async Task<ReadViewResult<T>> ExecuteAsync<T>(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken,
        Func<IAdminClient, TimeSpan, CancellationToken, Task<T>> action)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(
                ReadViewFailureCategory.Cancelled,
                "operation_cancelled",
                "Kafka consumer read operation was cancelled.",
                false);
        }

        var now = _timeProvider.GetUtcNow();
        if (operation.DeadlineUtc <= now)
        {
            return Failed<T>(
                ReadViewFailureCategory.Timeout,
                "deadline_exceeded",
                "Kafka consumer read operation exceeded its deadline.",
                true);
        }

        if (!_clients.ContainsCluster(clusterId))
        {
            return Failed<T>(
                ReadViewFailureCategory.NotConfigured,
                "cluster_not_configured",
                "Kafka cluster profile is not configured.",
                false);
        }

        try
        {
            var client = _clients.GetClient(clusterId);
            var remaining = operation.DeadlineUtc - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return Failed<T>(
                    ReadViewFailureCategory.Timeout,
                    "deadline_exceeded",
                    "Kafka consumer read operation exceeded its deadline.",
                    true);
            }

            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(remaining);

            var value = await action(client, remaining, deadline.Token).ConfigureAwait(false);
            return ReadViewResult<T>.Success(value);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(
                ReadViewFailureCategory.Cancelled,
                "operation_cancelled",
                "Kafka consumer read operation was cancelled.",
                false);
        }
        catch (OperationCanceledException)
        {
            return Failed<T>(
                ReadViewFailureCategory.Timeout,
                "deadline_exceeded",
                "Kafka consumer read operation exceeded its deadline.",
                true);
        }
        catch (TimeoutException)
        {
            return Failed<T>(
                ReadViewFailureCategory.Timeout,
                "deadline_exceeded",
                "Kafka consumer read operation exceeded its deadline.",
                true);
        }
        catch (ResponseBoundExceededException)
        {
            return Failed<T>(
                ReadViewFailureCategory.ResponseTooLarge,
                "consumer_response_bound_exceeded",
                "Kafka consumer observation exceeded the configured bound.",
                false);
        }
        catch (KafdeckConfigurationException)
        {
            return Failed<T>(
                ReadViewFailureCategory.InvalidRequest,
                "invalid_configuration",
                "Kafka client configuration is invalid.",
                false);
        }
        catch (KeyNotFoundException)
        {
            return Failed<T>(
                ReadViewFailureCategory.NotConfigured,
                "cluster_not_configured",
                "Kafka cluster profile is not configured.",
                false);
        }
        catch (KafkaException exception)
        {
            return MapKafkaFailure<T>(KafkaFailureMapper.FromKafka(exception.Error));
        }
        catch (ArgumentException)
        {
            return Failed<T>(
                ReadViewFailureCategory.InvalidRequest,
                "invalid_consumer_request",
                "Kafka consumer read request is invalid.",
                false);
        }
        catch (InvalidOperationException)
        {
            return Failed<T>(
                ReadViewFailureCategory.InvalidResponse,
                "invalid_consumer_response",
                "Kafka returned an invalid consumer observation.",
                false);
        }
        catch (OverflowException)
        {
            return Failed<T>(
                ReadViewFailureCategory.InvalidResponse,
                "consumer_value_overflow",
                "Kafka returned a consumer value outside the supported range.",
                false);
        }
        catch (Exception)
        {
            return Failed<T>(
                ReadViewFailureCategory.Unavailable,
                "consumer_read_failed",
                "Kafka consumer read operation failed.",
                true);
        }
    }

    private static ReadViewResult<T> MapKafkaFailure<T>(KafkaFailure failure)
    {
        var category = failure.Category switch
        {
            KafkaFailureCategory.Unauthorized => ReadViewFailureCategory.Unauthorized,
            KafkaFailureCategory.NotSupported => ReadViewFailureCategory.Unsupported,
            KafkaFailureCategory.Timeout => ReadViewFailureCategory.Timeout,
            KafkaFailureCategory.Cancelled => ReadViewFailureCategory.Cancelled,
            KafkaFailureCategory.InvalidConfiguration => ReadViewFailureCategory.InvalidRequest,
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure or
            KafkaFailureCategory.Unavailable => ReadViewFailureCategory.Unavailable,
            _ => ReadViewFailureCategory.InvalidResponse,
        };

        return Failed<T>(category, failure.Code, failure.SafeMessage, failure.IsRetryable);
    }

    private static ReadViewResult<T> Failed<T>(
        ReadViewFailureCategory category,
        string code,
        string safeMessage,
        bool retryable) =>
        ReadViewResult<T>.Failed(new ReadViewFailure(category, code, safeMessage, retryable));

    private static void EnsureStringBudget(IEnumerable<string> values, long maxBytes)
    {
        long total = 0;
        foreach (var value in values)
        {
            total = checked(total + Encoding.UTF8.GetByteCount(value));
            if (total > maxBytes)
            {
                throw new ResponseBoundExceededException();
            }
        }
    }

    private static bool IsAuthorization(Error error) =>
        error.Code is ErrorCode.GroupAuthorizationFailed or
            ErrorCode.ClusterAuthorizationFailed or
            ErrorCode.TopicAuthorizationFailed;

    private static string? NullIfBlank(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private sealed class ResponseBoundExceededException : Exception;
}
