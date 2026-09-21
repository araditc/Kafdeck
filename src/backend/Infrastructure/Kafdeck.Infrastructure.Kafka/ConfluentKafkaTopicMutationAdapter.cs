using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaTopicMutationAdapter :
    ITopicMutationPort,
    IDisposable
{
    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeSpan _requestTimeout;
    private readonly TimeSpan _operationTimeout;

    public ConfluentKafkaTopicMutationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeSpan? requestTimeout = null,
        TimeSpan? operationTimeout = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));

        _requestTimeout = ValidateTimeout(
            requestTimeout ?? TimeSpan.FromSeconds(15),
            nameof(requestTimeout));
        _operationTimeout = ValidateTimeout(
            operationTimeout ?? TimeSpan.FromSeconds(10),
            nameof(operationTimeout));
    }

    public Task<MutationProviderResult> CreateTopicAsync(
        TopicCreateMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        return ExecuteAsync(
            request.ClusterId,
            "topic_create",
            cancellationToken,
            async client =>
            {
                await client.CreateTopicsAsync(
                        new[]
                        {
                            new TopicSpecification
                            {
                                Name = request.TopicName,
                                NumPartitions = request.PartitionCount,
                                ReplicationFactor = request.ReplicationFactor,
                                Configs = request.Configurations.ToDictionary(
                                    pair => pair.Key,
                                    pair => pair.Value,
                                    StringComparer.Ordinal),
                            },
                        },
                        new CreateTopicsOptions
                        {
                            RequestTimeout = _requestTimeout,
                            OperationTimeout = _operationTimeout,
                            ValidateOnly = false,
                        })
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);

                return Accepted(
                    "topic_create_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                        ["partition.count"] = request.PartitionCount.ToString(
                            System.Globalization.CultureInfo.InvariantCulture),
                        ["replication.factor"] = request.ReplicationFactor.ToString(
                            System.Globalization.CultureInfo.InvariantCulture),
                    });
            });
    }

    public Task<MutationProviderResult> AlterTopicAsync(
        TopicAlterMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        return ExecuteAsync(
            request.ClusterId,
            "topic_alter",
            cancellationToken,
            async client =>
            {
                var resource = new ConfigResource
                {
                    Type = ResourceType.Topic,
                    Name = request.TopicName,
                };

                var entries = request.Changes
                    .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                    .Select(pair => new ConfigEntry
                    {
                        Name = pair.Key,
                        Value = pair.Value!,
                        IncrementalOperation = pair.Value is null
                            ? AlterConfigOpType.Delete
                            : AlterConfigOpType.Set,
                    })
                    .ToList();

                await client.IncrementalAlterConfigsAsync(
                        new Dictionary<ConfigResource, List<ConfigEntry>>
                        {
                            [resource] = entries,
                        },
                        new IncrementalAlterConfigsOptions
                        {
                            RequestTimeout = _requestTimeout,
                            ValidateOnly = false,
                        })
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);

                return Accepted(
                    "topic_alter_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                    });
            });
    }

    public Task<MutationProviderResult> IncreasePartitionsAsync(
        TopicIncreasePartitionsMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        return ExecuteAsync(
            request.ClusterId,
            "topic_partitions",
            cancellationToken,
            async client =>
            {
                await client.CreatePartitionsAsync(
                        new[]
                        {
                            new PartitionsSpecification
                            {
                                Topic = request.TopicName,
                                IncreaseTo = request.NewPartitionCount,
                            },
                        },
                        new CreatePartitionsOptions
                        {
                            RequestTimeout = _requestTimeout,
                            OperationTimeout = _operationTimeout,
                            ValidateOnly = false,
                        })
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);

                return Accepted(
                    "topic_partitions_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                        ["partition.count"] = request.NewPartitionCount.ToString(
                            System.Globalization.CultureInfo.InvariantCulture),
                    });
            });
    }

    public Task<MutationProviderResult> DeleteTopicAsync(
        TopicDeleteMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        return ExecuteAsync(
            request.ClusterId,
            "topic_delete",
            cancellationToken,
            async client =>
            {
                await client.DeleteTopicsAsync(
                        new[] { request.TopicName },
                        new DeleteTopicsOptions
                        {
                            RequestTimeout = _requestTimeout,
                            OperationTimeout = _operationTimeout,
                        })
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);

                return Accepted(
                    "topic_delete_accepted",
                    new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["provider.accepted"] = "true",
                    });
            });
    }

    public void Dispose() => _clients.Dispose();

    private async Task<MutationProviderResult> ExecuteAsync(
        string clusterId,
        string operationCode,
        CancellationToken cancellationToken,
        Func<IAdminClient, Task<MutationProviderResult>> action)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationCode);
        ArgumentNullException.ThrowIfNull(action);

        if (cancellationToken.IsCancellationRequested)
        {
            return Unknown($"{operationCode}_cancelled");
        }

        if (!_clients.ContainsCluster(clusterId))
        {
            return Failed($"{operationCode}_cluster_not_configured");
        }

        try
        {
            var client = _clients.GetClient(clusterId);
            return await action(client).ConfigureAwait(false);
        }
        catch (CreateTopicsException exception)
        {
            return FromError(operationCode, exception.Results.Single().Error);
        }
        catch (IncrementalAlterConfigsException exception)
        {
            return FromError(operationCode, exception.Results.Single().Error);
        }
        catch (CreatePartitionsException exception)
        {
            return FromError(operationCode, exception.Results.Single().Error);
        }
        catch (DeleteTopicsException exception)
        {
            return FromError(operationCode, exception.Results.Single().Error);
        }
        catch (OperationCanceledException)
        {
            return Unknown($"{operationCode}_cancelled_or_timeout");
        }
        catch (TimeoutException)
        {
            return Unknown($"{operationCode}_timeout");
        }
        catch (KafkaException exception)
        {
            return FromError(operationCode, exception.Error);
        }
        catch (KafdeckConfigurationException)
        {
            return Failed($"{operationCode}_invalid_configuration");
        }
        catch (KeyNotFoundException)
        {
            return Failed($"{operationCode}_cluster_not_configured");
        }
        catch (ArgumentException)
        {
            return Failed($"{operationCode}_invalid_request");
        }
        catch (InvalidOperationException)
        {
            return Failed($"{operationCode}_invalid_operation");
        }
        catch (Exception)
        {
            // After the W32 dispatch marker an unclassified provider-side exception is
            // ambiguous. Do not claim the mutation was not applied.
            return Unknown($"{operationCode}_provider_exception");
        }
    }

    private static MutationProviderResult FromError(
        string operationCode,
        Error error)
    {
        var failure = KafkaFailureMapper.FromKafka(error);
        var code = $"{operationCode}_{failure.Code}";

        return failure.Category is
            Kafdeck.Core.Kafka.KafkaFailureCategory.Timeout or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Unavailable or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Unknown
            ? Unknown(code)
            : Failed(code);
    }

    private static MutationProviderResult Accepted(
        string code,
        IReadOnlyDictionary<string, string> evidence) =>
        new(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            evidence);

    private static MutationProviderResult Failed(string code) =>
        new(
            MutationExecutionResultKind.FailedDefinitive,
            code);

    private static MutationProviderResult Unknown(string code) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code);

    private static TimeSpan ValidateTimeout(TimeSpan value, string parameterName)
    {
        if (value < TimeSpan.FromSeconds(1) || value > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                "Kafka topic mutation timeout must be between one and thirty seconds.");
        }

        return value;
    }
}
