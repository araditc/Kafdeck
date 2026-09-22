using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaRecordsPurgeObservationAdapter :
    IRecordsPurgeObservationPort,
    IDisposable
{
    private const int HardMaxTargets = 256;

    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaRecordsPurgeObservationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ??
                throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ??
                throw new ArgumentNullException(nameof(secretResolver)));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<KafkaResult<IReadOnlyList<RecordsPurgePartitionObservation>>>
        ObserveAsync(
            string clusterId,
            IReadOnlyList<RecordsPurgeObservationTarget> targets,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentNullException.ThrowIfNull(targets);

        if (targets.Count is < 1 or > HardMaxTargets)
            throw new ArgumentOutOfRangeException(nameof(targets));

        var normalized = NormalizeTargets(targets);

        return ExecuteAsync<IReadOnlyList<RecordsPurgePartitionObservation>>(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                var partitions = normalized
                    .Select(target => new TopicPartition(
                        target.Topic,
                        new Partition(target.Partition)))
                    .ToArray();

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

                var timestampSpecs = normalized
                    .Where(target =>
                        target.ResolveTimestampUtc.HasValue)
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

                if (timestampSpecs.Length > 0)
                {
                    var timestampResult =
                        await client.ListOffsetsAsync(
                                timestampSpecs,
                                new ListOffsetsOptions
                                {
                                    RequestTimeout = timeout,
                                })
                            .WaitAsync(token)
                            .ConfigureAwait(false);

                    foreach (var info in timestampResult.ResultInfos)
                    {
                        var offset =
                            info.TopicPartitionOffsetError;
                        if (offset.Error.IsError)
                            throw new KafkaException(offset.Error);

                        timestampOffsets[offset.TopicPartition] =
                            offset.Offset.Value >= 0
                                ? offset.Offset.Value
                                : null;
                    }
                }

                var result =
                    new List<RecordsPurgePartitionObservation>(
                        normalized.Count);

                foreach (var target in normalized)
                {
                    var partition = new TopicPartition(
                        target.Topic,
                        new Partition(target.Partition));

                    if (!earliest.TryGetValue(
                            partition,
                            out var low) ||
                        !latest.TryGetValue(
                            partition,
                            out var high))
                    {
                        throw new InvalidOperationException(
                            "Kafka returned an incomplete purge observation.");
                    }

                    long? timestampOffset = null;
                    if (target.ResolveTimestampUtc.HasValue &&
                        !timestampOffsets.TryGetValue(
                            partition,
                            out timestampOffset))
                    {
                        throw new InvalidOperationException(
                            "Kafka returned an incomplete purge timestamp observation.");
                    }

                    result.Add(
                        new RecordsPurgePartitionObservation(
                            target.Topic,
                            target.Partition,
                            low,
                            high,
                            timestampOffset));
                }

                return result;
            });
    }

    public void Dispose() => _clients.Dispose();

    private static IReadOnlyList<RecordsPurgeObservationTarget>
        NormalizeTargets(
            IReadOnlyList<RecordsPurgeObservationTarget> targets)
    {
        var normalized =
            new Dictionary<
                (string Topic, int Partition),
                RecordsPurgeObservationTarget>();

        foreach (var target in targets)
        {
            ArgumentNullException.ThrowIfNull(target);

            var topic = target.Topic?.Trim();
            if (string.IsNullOrWhiteSpace(topic) ||
                !string.Equals(
                    topic,
                    target.Topic,
                    StringComparison.Ordinal) ||
                topic.Length > 249 ||
                topic.Any(char.IsControl) ||
                target.Partition < 0)
            {
                throw new ArgumentException(
                    "Records purge observation target is invalid.",
                    nameof(targets));
            }

            var normalizedTimestamp =
                target.ResolveTimestampUtc?.ToUniversalTime();
            var item = new RecordsPurgeObservationTarget(
                topic,
                target.Partition,
                normalizedTimestamp);
            var key = (topic, target.Partition);

            if (normalized.TryGetValue(key, out var existing))
            {
                if (existing.ResolveTimestampUtc !=
                    item.ResolveTimestampUtc)
                {
                    throw new ArgumentException(
                        "Records purge observation targets cannot request conflicting timestamps.",
                        nameof(targets));
                }

                continue;
            }

            normalized.Add(key, item);
        }

        return normalized.Values
            .OrderBy(target => target.Topic, StringComparer.Ordinal)
            .ThenBy(target => target.Partition)
            .ToArray();
    }

    private static async Task<IReadOnlyDictionary<TopicPartition, long>>
        ListOffsetsAsync(
            IAdminClient client,
            IReadOnlyList<TopicPartition> partitions,
            OffsetSpec spec,
            TimeSpan timeout,
            CancellationToken cancellationToken)
    {
        var result = await client.ListOffsetsAsync(
                partitions.Select(partition =>
                    new TopicPartitionOffsetSpec
                    {
                        TopicPartition = partition,
                        OffsetSpec = spec,
                    }),
                new ListOffsetsOptions
                {
                    RequestTimeout = timeout,
                })
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        var values =
            new Dictionary<TopicPartition, long>();

        foreach (var info in result.ResultInfos)
        {
            var offset = info.TopicPartitionOffsetError;
            if (offset.Error.IsError)
                throw new KafkaException(offset.Error);
            if (offset.Offset.Value < 0)
            {
                throw new InvalidOperationException(
                    "Kafka returned an invalid purge watermark.");
            }

            values[offset.TopicPartition] =
                offset.Offset.Value;
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
            var remaining =
                operation.Remaining(_timeProvider.GetUtcNow());

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

            return KafkaResult<T>.Success(
                value,
                LiveObservation());
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
            return Failed<T>(
                KafkaFailureMapper.InvalidConfiguration());
        }
        catch (KeyNotFoundException)
        {
            return Failed<T>(
                KafkaFailureMapper.ClusterNotConfigured());
        }
        catch (KafkaException exception)
        {
            return Failed<T>(
                KafkaFailureMapper.FromKafka(exception.Error));
        }
        catch (ArgumentException)
        {
            return Failed<T>(
                KafkaFailureMapper.InvalidConfiguration());
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
        KafkaResult<T>.Failed(
            failure,
            LiveObservation());

    private ObservationMetadata LiveObservation()
    {
        var observedAt = _timeProvider.GetUtcNow();
        return new ObservationMetadata(
            observedAt,
            observedAt,
            observedAt,
            ObservationSource.Live);
    }
}
