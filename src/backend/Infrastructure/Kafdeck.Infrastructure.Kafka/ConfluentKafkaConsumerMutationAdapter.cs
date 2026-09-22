using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaConsumerMutationAdapter :
    IConsumerMutationPort,
    IDisposable
{
    private const int HardMaxTargets = 256;

    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeSpan _requestTimeout;
    private readonly TimeSpan _operationTimeout;

    public ConfluentKafkaConsumerMutationAdapter(
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

    public Task<MutationProviderResult> AlterOffsetsAsync(
        ConsumerOffsetAlterMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var targets = NormalizeTargets(request.Targets);

        return ExecuteAsync(
            request.ClusterId,
            "consumer_offset_alter",
            cancellationToken,
            async client =>
            {
                try
                {
                    var result = await client.AlterConsumerGroupOffsetsAsync(
                            [
                                new ConsumerGroupTopicPartitionOffsets(
                                    RequireGroupId(request.GroupId),
                                    targets.Select(target =>
                                        new TopicPartitionOffset(
                                            target.TopicName,
                                            new Partition(target.Partition),
                                            new Offset(target.Offset)))
                                        .ToList()),
                            ],
                            new AlterConsumerGroupOffsetsOptions
                            {
                                RequestTimeout = _requestTimeout,
                            })
                        .WaitAsync(cancellationToken)
                        .ConfigureAwait(false);

                    return FromPartitionResults(
                        "consumer_offset_alter",
                        result.Single().Partitions,
                        targets);
                }
                catch (AlterConsumerGroupOffsetsException exception)
                {
                    return FromPartitionReports(
                        "consumer_offset_alter",
                        exception.Results.SelectMany(report => report.Partitions),
                        targets,
                        exception.Results.Select(report => report.Error));
                }
            });
    }

    public Task<MutationProviderResult> DeleteAsync(
        ConsumerDeleteMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var groupId = RequireGroupId(request.GroupId);

        if (request.Offsets is null)
        {
            return ExecuteAsync(
                request.ClusterId,
                "consumer_group_delete",
                cancellationToken,
                async client =>
                {
                    try
                    {
                        await client.DeleteGroupsAsync(
                                [groupId],
                                new DeleteGroupsOptions
                                {
                                    RequestTimeout = _requestTimeout,
                                    OperationTimeout = _operationTimeout,
                                })
                            .WaitAsync(cancellationToken)
                            .ConfigureAwait(false);

                        return Accepted(
                            "consumer_group_delete_accepted",
                            new Dictionary<string, string>(StringComparer.Ordinal)
                            {
                                ["provider.accepted"] = "true",
                                ["group.count"] = "1",
                            });
                    }
                    catch (DeleteGroupsException exception)
                    {
                        var errors = exception.Results
                            .Select(report => report.Error)
                            .ToArray();
                        return FromErrors(
                            "consumer_group_delete",
                            errors,
                            totalCount: 1,
                            successfulCount: errors.Count(error => !error.IsError));
                    }
                });
        }

        var targets = NormalizeTargets(request.Offsets);
        return ExecuteAsync(
            request.ClusterId,
            "consumer_offset_delete",
            cancellationToken,
            async client =>
            {
                try
                {
                    var result = await client.DeleteConsumerGroupOffsetsAsync(
                            groupId,
                            targets.Select(target => new TopicPartition(
                                target.TopicName,
                                new Partition(target.Partition))),
                            new DeleteConsumerGroupOffsetsOptions
                            {
                                RequestTimeout = _requestTimeout,
                                OperationTimeout = _operationTimeout,
                            })
                        .WaitAsync(cancellationToken)
                        .ConfigureAwait(false);

                    return FromDeleteOffsetSuccess(
                        "consumer_offset_delete",
                        result.Partitions,
                        targets);
                }
                catch (DeleteConsumerGroupOffsetsException exception)
                {
                    return FromPartitionReports(
                        "consumer_offset_delete",
                        exception.Result.Partitions,
                        targets,
                        [exception.Result.Error]);
                }
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
            return Unknown($"{operationCode}_cancelled_or_timeout");

        if (!_clients.ContainsCluster(clusterId))
            return Failed($"{operationCode}_cluster_not_configured");

        try
        {
            var client = _clients.GetClient(clusterId);
            return await action(client).ConfigureAwait(false);
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
            var failure = KafkaFailureMapper.FromKafka(exception.Error);
            return IsAmbiguous(failure.Category)
                ? Unknown($"{operationCode}_{failure.Code}")
                : Failed($"{operationCode}_{failure.Code}");
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
        catch
        {
            return Unknown($"{operationCode}_provider_exception");
        }
    }

    private static MutationProviderResult FromPartitionResults(
        string operationCode,
        IEnumerable<TopicPartitionOffsetError> results,
        IReadOnlyList<ConsumerOffsetTarget> expectedTargets) =>
        FromPartitionReports(
            operationCode,
            results,
            expectedTargets,
            Array.Empty<Error>());

    private static MutationProviderResult FromPartitionReports(
        string operationCode,
        IEnumerable<TopicPartitionOffsetError> partitions,
        IReadOnlyList<ConsumerOffsetTarget> expectedTargets,
        IEnumerable<Error> groupErrors)
    {
        var partitionResults = partitions.ToArray();
        var expected = ExpectedPartitions(expectedTargets);
        var groupErrorArray = groupErrors
            .Where(error => error.IsError)
            .ToArray();

        var seen = new HashSet<(string Topic, int Partition)>();
        foreach (var result in partitionResults)
        {
            var key = (
                result.TopicPartition.Topic,
                result.TopicPartition.Partition.Value);
            if (!expected.Contains(key) || !seen.Add(key))
            {
                return Malformed(
                    operationCode,
                    expected.Count,
                    partitionResults.Count(item => !item.Error.IsError));
            }
        }

        var successfulCount =
            partitionResults.Count(result => !result.Error.IsError);
        var partitionErrors = partitionResults
            .Select(result => result.Error)
            .Where(error => error.IsError)
            .ToArray();

        if (partitionResults.Length != expected.Count)
        {
            if (groupErrorArray.Length > 0 && successfulCount == 0)
            {
                return FromErrors(
                    operationCode,
                    groupErrorArray.Concat(partitionErrors).ToArray(),
                    expected.Count,
                    0);
            }

            return Malformed(
                operationCode,
                expected.Count,
                successfulCount);
        }

        var errors = groupErrorArray
            .Concat(partitionErrors)
            .ToArray();

        if (errors.Length == 0)
        {
            return Accepted(
                $"{operationCode}_accepted",
                Evidence(expected.Count, expected.Count, 0));
        }

        if (groupErrorArray.Length > 0 && successfulCount > 0)
        {
            return Malformed(
                operationCode,
                expected.Count,
                successfulCount);
        }

        return FromErrors(
            operationCode,
            errors,
            expected.Count,
            successfulCount);
    }

    private static MutationProviderResult FromDeleteOffsetSuccess(
        string operationCode,
        IEnumerable<TopicPartition> partitions,
        IReadOnlyList<ConsumerOffsetTarget> expectedTargets)
    {
        var expected = ExpectedPartitions(expectedTargets);
        var actual = new HashSet<(string Topic, int Partition)>();

        foreach (var partition in partitions)
        {
            var key = (partition.Topic, partition.Partition.Value);
            if (!expected.Contains(key) || !actual.Add(key))
            {
                return Malformed(
                    operationCode,
                    expected.Count,
                    actual.Count);
            }
        }

        if (!actual.SetEquals(expected))
        {
            return Malformed(
                operationCode,
                expected.Count,
                actual.Count);
        }

        return Accepted(
            $"{operationCode}_accepted",
            Evidence(expected.Count, expected.Count, 0));
    }

    private static HashSet<(string Topic, int Partition)> ExpectedPartitions(
        IReadOnlyList<ConsumerOffsetTarget> expectedTargets) =>
        expectedTargets
            .Select(target => (target.TopicName, target.Partition))
            .ToHashSet();

    private static MutationProviderResult Malformed(
        string operationCode,
        int totalCount,
        int reportedSuccessCount) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            $"{operationCode}_provider_result_invalid",
            Evidence(
                totalCount,
                Math.Clamp(reportedSuccessCount, 0, totalCount),
                Math.Max(0, totalCount - reportedSuccessCount)));

    private static MutationProviderResult FromErrors(
        string operationCode,
        IReadOnlyList<Error> errors,
        int totalCount,
        int successfulCount)
    {
        var failedCount = Math.Max(0, totalCount - successfulCount);
        if (successfulCount > 0)
        {
            return new MutationProviderResult(
                MutationExecutionResultKind.PartiallyApplied,
                $"{operationCode}_partial",
                Evidence(totalCount, successfulCount, failedCount));
        }

        var mapped = errors
            .Where(error => error.IsError)
            .Select(KafkaFailureMapper.FromKafka)
            .ToArray();

        var evidence = Evidence(totalCount, 0, failedCount);
        var safeCodes = mapped
            .Select(failure => failure.Code)
            .Where(code => !string.IsNullOrWhiteSpace(code))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(code => code, StringComparer.Ordinal)
            .ToArray();
        if (safeCodes.Length > 0)
        {
            evidence["provider.error.codes"] = string.Join(",", safeCodes);
        }

        if (mapped.Any(failure => IsAmbiguous(failure.Category)))
        {
            return new MutationProviderResult(
                MutationExecutionResultKind.ExecutionUnknown,
                $"{operationCode}_ambiguous",
                evidence);
        }

        return new MutationProviderResult(
            MutationExecutionResultKind.FailedDefinitive,
            $"{operationCode}_rejected",
            evidence);
    }

    private static IReadOnlyDictionary<string, string> Evidence(
        int total,
        int applied,
        int failed) =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["provider.accepted"] = (applied > 0).ToString().ToLowerInvariant(),
            ["target.count"] = total.ToString(CultureInfo.InvariantCulture),
            ["applied.count"] = applied.ToString(CultureInfo.InvariantCulture),
            ["failed.count"] = failed.ToString(CultureInfo.InvariantCulture),
        };

    private static MutationProviderResult Accepted(
        string code,
        IReadOnlyDictionary<string, string> evidence) =>
        new(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            evidence);

    private static MutationProviderResult Failed(string code) =>
        new(MutationExecutionResultKind.FailedDefinitive, code);

    private static MutationProviderResult Unknown(string code) =>
        new(MutationExecutionResultKind.ExecutionUnknown, code);

    private static bool IsAmbiguous(
        Kafdeck.Core.Kafka.KafkaFailureCategory category) =>
        category is
            Kafdeck.Core.Kafka.KafkaFailureCategory.Timeout or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Unavailable or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Unknown or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Cancelled;

    private static string RequireGroupId(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (!string.Equals(value, normalized, StringComparison.Ordinal) ||
            normalized.Length > 255 ||
            normalized.Any(char.IsControl))
        {
            throw new ArgumentException("Consumer group ID is invalid.", nameof(value));
        }

        return normalized;
    }

    private static IReadOnlyList<ConsumerOffsetTarget> NormalizeTargets(
        IReadOnlyList<ConsumerOffsetTarget> targets)
    {
        ArgumentNullException.ThrowIfNull(targets);
        if (targets.Count is < 1 or > HardMaxTargets)
            throw new ArgumentOutOfRangeException(nameof(targets));

        var normalized =
            new Dictionary<(string Topic, int Partition), ConsumerOffsetTarget>();

        foreach (var target in targets)
        {
            ArgumentNullException.ThrowIfNull(target);
            var topic = target.TopicName?.Trim();
            if (string.IsNullOrWhiteSpace(topic) ||
                !string.Equals(topic, target.TopicName, StringComparison.Ordinal) ||
                topic.Length > 249 ||
                topic.Any(char.IsControl) ||
                target.Partition < 0 ||
                target.Offset < 0)
            {
                throw new ArgumentException(
                    "Consumer offset mutation target is invalid.",
                    nameof(targets));
            }

            if (!normalized.TryAdd(
                    (topic, target.Partition),
                    target with { TopicName = topic }))
            {
                throw new ArgumentException(
                    "Consumer offset mutation targets must be unique.",
                    nameof(targets));
            }
        }

        return normalized.Values
            .OrderBy(target => target.TopicName, StringComparer.Ordinal)
            .ThenBy(target => target.Partition)
            .ToArray();
    }

    private static TimeSpan ValidateTimeout(
        TimeSpan value,
        string parameterName)
    {
        if (value < TimeSpan.FromSeconds(1) ||
            value > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                "Kafka consumer mutation timeout must be between one and thirty seconds.");
        }

        return value;
    }
}
