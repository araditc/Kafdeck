using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using CoreBrokerMetadata = Kafdeck.Core.Kafka.BrokerMetadata;
using CoreClusterMetadata = Kafdeck.Core.Kafka.ClusterMetadata;
using CorePartitionMetadata = Kafdeck.Core.Kafka.PartitionMetadata;
using CoreTopicMetadata = Kafdeck.Core.Kafka.TopicMetadata;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaAdministrationAdapter : IKafkaAdministrationPort, IDisposable
{
    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;

    public ConfluentKafkaAdministrationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));

        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<KafkaResult<CoreClusterMetadata>> GetClusterMetadataAsync(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                var result = await client.DescribeClusterAsync(
                        new DescribeClusterOptions { RequestTimeout = timeout })
                    .WaitAsync(token)
                    .ConfigureAwait(false);

                var controllerId = result.Controller?.Id;
                var brokers = result.Nodes
                    .OrderBy(node => node.Id)
                    .Select(node => new CoreBrokerMetadata(
                        node.Id,
                        node.Host,
                        node.Port,
                        node.Rack,
                        controllerId == node.Id))
                    .ToArray();

                return new CoreClusterMetadata(
                    clusterId,
                    result.ClusterId,
                    controllerId,
                    brokers);
            });

    public Task<KafkaResult<IReadOnlyList<TopicSummary>>> ListTopicsAsync(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync<IReadOnlyList<TopicSummary>>(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                var metadata = await Task.Run(
                        () => client.GetMetadata(timeout),
                        CancellationToken.None)
                    .WaitAsync(token)
                    .ConfigureAwait(false);

                var names = metadata.Topics
                    .Where(topic => !topic.Error.IsError)
                    .Select(topic => topic.Topic)
                    .OrderBy(name => name, StringComparer.Ordinal)
                    .ToArray();

                if (names.Length == 0)
                {
                    return Array.Empty<TopicSummary>();
                }

                var described = await client.DescribeTopicsAsync(
                        TopicCollection.OfTopicNames(names),
                        new DescribeTopicsOptions { RequestTimeout = timeout })
                    .WaitAsync(token)
                    .ConfigureAwait(false);

                return described.TopicDescriptions
                    .OrderBy(topic => topic.Name, StringComparer.Ordinal)
                    .Select(topic => new TopicSummary(
                        topic.Name,
                        topic.Partitions.Count,
                        topic.IsInternal))
                    .ToArray();
            });

    public Task<KafkaResult<CoreTopicMetadata>> GetTopicMetadataAsync(
        string clusterId,
        string topicName,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        return ExecuteAsync(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                var result = await client.DescribeTopicsAsync(
                        TopicCollection.OfTopicNames([topicName]),
                        new DescribeTopicsOptions { RequestTimeout = timeout })
                    .WaitAsync(token)
                    .ConfigureAwait(false);

                var topic = result.TopicDescriptions.Single();
                if (topic.Error.IsError)
                {
                    throw new KafkaException(topic.Error);
                }

                var partitions = topic.Partitions
                    .OrderBy(partition => partition.Partition)
                    .Select(partition => new CorePartitionMetadata(
                        partition.Partition,
                        partition.Leader?.Id,
                        partition.Replicas.Select(node => node.Id).OrderBy(id => id).ToArray(),
                        partition.ISR.Select(node => node.Id).OrderBy(id => id).ToArray()))
                    .ToArray();

                return new CoreTopicMetadata(topic.Name, topic.IsInternal, partitions);
            });
    }

    public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetTopicConfigurationAsync(
        string clusterId,
        string topicName,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        return GetConfigurationAsync(
            clusterId,
            new ConfigResource { Type = ResourceType.Topic, Name = topicName },
            operation,
            cancellationToken);
    }

    public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
        string clusterId,
        int brokerId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken) =>
        GetConfigurationAsync(
            clusterId,
            new ConfigResource
            {
                Type = ResourceType.Broker,
                Name = brokerId.ToString(CultureInfo.InvariantCulture),
            },
            operation,
            cancellationToken);

    public async Task<KafkaResult<KafkaCapabilities>> GetCapabilitiesAsync(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        if (!_clients.ContainsCluster(clusterId))
        {
            return Failed<KafkaCapabilities>(KafkaFailureMapper.ClusterNotConfigured());
        }

        var items = new List<KafkaCapabilityStatus>();

        var cluster = await GetClusterMetadataAsync(clusterId, operation, cancellationToken).ConfigureAwait(false);
        items.Add(Status(KafkaCapabilityKind.ClusterMetadata, cluster));
        items.Add(cluster.IsSuccess && cluster.Value!.Brokers.Count > 0
            ? Available(KafkaCapabilityKind.BrokerMetadata)
            : Status(KafkaCapabilityKind.BrokerMetadata, cluster));

        items.Add(cluster.IsSuccess
            ? cluster.Value!.ControllerBrokerId.HasValue
                ? Available(KafkaCapabilityKind.ControllerMetadata)
                : Unknown(KafkaCapabilityKind.ControllerMetadata, "Controller metadata was not observable.")
            : Status(KafkaCapabilityKind.ControllerMetadata, cluster));

        var topics = await ListTopicsAsync(clusterId, operation, cancellationToken).ConfigureAwait(false);
        items.Add(Status(KafkaCapabilityKind.TopicListing, topics));

        if (topics.IsSuccess && topics.Value!.Count > 0)
        {
            var probeTopic = topics.Value[0].Name;

            var topicMetadata = await GetTopicMetadataAsync(
                    clusterId,
                    probeTopic,
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);
            items.Add(Status(KafkaCapabilityKind.TopicMetadata, topicMetadata));

            var topicConfig = await GetTopicConfigurationAsync(
                    clusterId,
                    probeTopic,
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);
            items.Add(Status(KafkaCapabilityKind.TopicConfiguration, topicConfig));
        }
        else if (topics.IsSuccess)
        {
            items.Add(Unknown(KafkaCapabilityKind.TopicMetadata, "No topic exists for a non-mutating capability probe."));
            items.Add(Unknown(KafkaCapabilityKind.TopicConfiguration, "No topic exists for a non-mutating capability probe."));
        }
        else
        {
            items.Add(Status(KafkaCapabilityKind.TopicMetadata, topics));
            items.Add(Status(KafkaCapabilityKind.TopicConfiguration, topics));
        }

        if (cluster.IsSuccess && cluster.Value!.Brokers.Count > 0)
        {
            var probeBroker = cluster.Value.ControllerBrokerId
                ?? cluster.Value.Brokers[0].BrokerId;

            var brokerConfig = await GetBrokerConfigurationAsync(
                    clusterId,
                    probeBroker,
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);
            items.Add(Status(KafkaCapabilityKind.BrokerConfiguration, brokerConfig));
        }
        else
        {
            items.Add(cluster.IsSuccess
                ? Unknown(KafkaCapabilityKind.BrokerConfiguration, "No broker exists for a non-mutating capability probe.")
                : Status(KafkaCapabilityKind.BrokerConfiguration, cluster));
        }

        return KafkaResult<KafkaCapabilities>.Success(
            new KafkaCapabilities(items),
            LiveObservation());
    }

    public void Dispose() => _clients.Dispose();

    private Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetConfigurationAsync(
        string clusterId,
        ConfigResource resource,
        KafkaOperationContext operation,
        CancellationToken cancellationToken) =>
        ExecuteAsync<IReadOnlyList<KafkaConfigurationEntry>>(
            clusterId,
            operation,
            cancellationToken,
            async (client, timeout, token) =>
            {
                var results = await client.DescribeConfigsAsync(
                        [resource],
                        new DescribeConfigsOptions { RequestTimeout = timeout })
                    .WaitAsync(token)
                    .ConfigureAwait(false);

                var result = results.Single();

                return result.Entries.Values
                    .OrderBy(entry => entry.Name, StringComparer.Ordinal)
                    .Select(entry => new KafkaConfigurationEntry(
                        entry.Name,
                        entry.Value,
                        entry.IsSensitive,
                        entry.IsReadOnly,
                        entry.Source.ToString()))
                    .ToArray();
            });

    private async Task<KafkaResult<T>> ExecuteAsync<T>(
        string clusterId,
        KafkaOperationContext operation,
        CancellationToken cancellationToken,
        Func<IAdminClient, TimeSpan, CancellationToken, Task<T>> action)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Failed<T>(KafkaFailureMapper.Cancelled());
        }

        var now = _timeProvider.GetUtcNow();
        if (operation.IsExpired(now))
        {
            return Failed<T>(KafkaFailureMapper.DeadlineExceeded());
        }

        if (!_clients.ContainsCluster(clusterId))
        {
            return Failed<T>(KafkaFailureMapper.ClusterNotConfigured());
        }

        try
        {
            var client = _clients.GetClient(clusterId);
            var remaining = operation.Remaining(_timeProvider.GetUtcNow());
            if (remaining <= TimeSpan.Zero)
            {
                return Failed<T>(KafkaFailureMapper.DeadlineExceeded());
            }

            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(remaining);

            var value = await action(client, remaining, deadline.Token).ConfigureAwait(false);
            return KafkaResult<T>.Success(value, LiveObservation());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
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
            return Failed<T>(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (Exception)
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

    private static KafkaCapabilityStatus Available(KafkaCapabilityKind capability) =>
        new(capability, KafkaCapabilityState.Available);

    private static KafkaCapabilityStatus Unknown(KafkaCapabilityKind capability, string reason) =>
        new(capability, KafkaCapabilityState.Unknown, reason);

    private static KafkaCapabilityStatus Status<T>(
        KafkaCapabilityKind capability,
        KafkaResult<T> result)
    {
        if (result.IsSuccess)
        {
            return Available(capability);
        }

        var failure = result.Failure!;
        var state = failure.Category switch
        {
            KafkaFailureCategory.Unauthorized => KafkaCapabilityState.Unauthorized,
            KafkaFailureCategory.NotSupported => KafkaCapabilityState.Unsupported,
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure => KafkaCapabilityState.Unavailable,
            _ => KafkaCapabilityState.Unknown,
        };

        return new KafkaCapabilityStatus(capability, state, failure.SafeMessage);
    }
}
