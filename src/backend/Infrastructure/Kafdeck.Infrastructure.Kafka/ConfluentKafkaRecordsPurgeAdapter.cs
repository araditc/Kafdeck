using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaRecordsPurgeAdapter :
    IRecordsPurgeMutationPort,
    IDisposable
{
    private const int HardMaxTargets = 256;

    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeSpan _requestTimeout;
    private readonly TimeSpan _operationTimeout;

    public ConfluentKafkaRecordsPurgeAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeSpan? requestTimeout = null,
        TimeSpan? operationTimeout = null)
    {
        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ??
                throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ??
                throw new ArgumentNullException(nameof(secretResolver)));

        _requestTimeout = ValidateTimeout(
            requestTimeout ?? TimeSpan.FromSeconds(15),
            nameof(requestTimeout));
        _operationTimeout = ValidateTimeout(
            operationTimeout ?? TimeSpan.FromSeconds(10),
            nameof(operationTimeout));
    }

    public async Task<MutationProviderResult> PurgeAsync(
        RecordsPurgeMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        IReadOnlyList<RecordsPurgeTarget> targets;
        try
        {
            targets = NormalizeTargets(request.Targets);
        }
        catch (ArgumentException)
        {
            return Failed("records_purge_invalid_request");
        }

        if (cancellationToken.IsCancellationRequested)
            return Unknown("records_purge_cancelled_or_timeout");

        if (!_clients.ContainsCluster(request.ClusterId))
            return Failed("records_purge_cluster_not_configured");

        try
        {
            var client = _clients.GetClient(request.ClusterId);
            var result = await client.DeleteRecordsAsync(
                    targets.Select(target =>
                        new TopicPartitionOffset(
                            target.TopicName,
                            new Partition(target.Partition),
                            new Offset(target.BeforeOffset))),
                    new DeleteRecordsOptions
                    {
                        RequestTimeout = _requestTimeout,
                        OperationTimeout = _operationTimeout,
                    })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return FromSuccessResults(
                result,
                targets);
        }
        catch (DeleteRecordsException exception)
        {
            return FromReports(
                exception.Results,
                targets);
        }
        catch (OperationCanceledException)
        {
            return Unknown("records_purge_cancelled_or_timeout");
        }
        catch (TimeoutException)
        {
            return Unknown("records_purge_timeout");
        }
        catch (KafkaException exception)
        {
            var failure =
                KafkaFailureMapper.FromKafka(exception.Error);

            return IsAmbiguous(failure)
                ? Unknown($"records_purge_{failure.Code}")
                : Failed($"records_purge_{failure.Code}");
        }
        catch (KafdeckConfigurationException)
        {
            return Failed("records_purge_invalid_configuration");
        }
        catch (KeyNotFoundException)
        {
            return Failed("records_purge_cluster_not_configured");
        }
        catch (InvalidOperationException)
        {
            return Unknown("records_purge_provider_result_invalid");
        }
        catch
        {
            return Unknown("records_purge_provider_exception");
        }
    }

    public void Dispose() => _clients.Dispose();

    internal static MutationProviderResult FromSuccessResults(
        IEnumerable<DeleteRecordsResult> results,
        IReadOnlyList<RecordsPurgeTarget> expectedTargets)
    {
        var expected = ExpectedTargets(expectedTargets);
        var seen =
            new HashSet<(string Topic, int Partition)>();
        var count = 0;

        foreach (var result in results)
        {
            count++;
            var key = (result.Topic, result.Partition.Value);
            if (!expected.TryGetValue(
                    key,
                    out var target) ||
                !seen.Add(key) ||
                result.Offset.Value < target.BeforeOffset)
            {
                return Malformed(
                    expected.Count,
                    seen.Count);
            }
        }

        if (count != expected.Count ||
            !seen.SetEquals(expected.Keys))
        {
            return Malformed(
                expected.Count,
                seen.Count);
        }

        return Accepted(
            expected.Count,
            expected.Count,
            0);
    }

    internal static MutationProviderResult FromReports(
        IEnumerable<DeleteRecordsReport> reports,
        IReadOnlyList<RecordsPurgeTarget> expectedTargets)
    {
        var expected = ExpectedTargets(expectedTargets);
        var seen =
            new HashSet<(string Topic, int Partition)>();
        var errors = new List<KafkaFailure>();
        var successfulCount = 0;

        foreach (var report in reports)
        {
            var key = (report.Topic, report.Partition.Value);
            if (!expected.TryGetValue(
                    key,
                    out var target) ||
                !seen.Add(key))
            {
                return Malformed(
                    expected.Count,
                    successfulCount);
            }

            if (report.Error.IsError)
            {
                errors.Add(
                    KafkaFailureMapper.FromKafka(
                        report.Error));
                continue;
            }

            if (report.Offset.Value < target.BeforeOffset)
            {
                return Malformed(
                    expected.Count,
                    successfulCount);
            }

            successfulCount++;
        }

        if (seen.Count != expected.Count)
        {
            return Malformed(
                expected.Count,
                successfulCount);
        }

        var evidence = Evidence(
            expected.Count,
            successfulCount,
            expected.Count - successfulCount,
            errors);

        if (errors.Any(IsAmbiguous))
        {
            return new MutationProviderResult(
                MutationExecutionResultKind.ExecutionUnknown,
                "records_purge_ambiguous",
                evidence);
        }

        if (successfulCount > 0 &&
            successfulCount < expected.Count)
        {
            return new MutationProviderResult(
                MutationExecutionResultKind.PartiallyApplied,
                "records_purge_partial",
                evidence);
        }

        if (successfulCount == 0)
        {
            return new MutationProviderResult(
                MutationExecutionResultKind.FailedDefinitive,
                "records_purge_rejected",
                evidence);
        }

        return Accepted(
            expected.Count,
            successfulCount,
            0);
    }

    private static IReadOnlyList<RecordsPurgeTarget>
        NormalizeTargets(
            IReadOnlyList<RecordsPurgeTarget> targets)
    {
        ArgumentNullException.ThrowIfNull(targets);
        if (targets.Count is < 1 or > HardMaxTargets)
            throw new ArgumentOutOfRangeException(nameof(targets));

        var normalized =
            new Dictionary<
                (string Topic, int Partition),
                RecordsPurgeTarget>();

        foreach (var target in targets)
        {
            ArgumentNullException.ThrowIfNull(target);

            var topic = target.TopicName?.Trim();
            if (string.IsNullOrWhiteSpace(topic) ||
                !string.Equals(
                    topic,
                    target.TopicName,
                    StringComparison.Ordinal) ||
                topic.Length > 249 ||
                topic.Any(char.IsControl) ||
                target.Partition < 0 ||
                target.BeforeOffset < 0)
            {
                throw new ArgumentException(
                    "Records purge target is invalid.",
                    nameof(targets));
            }

            var item = target with
            {
                TopicName = topic,
            };

            if (!normalized.TryAdd(
                    (topic, target.Partition),
                    item))
            {
                throw new ArgumentException(
                    "Records purge targets must be unique.",
                    nameof(targets));
            }
        }

        return normalized.Values
            .OrderBy(target => target.TopicName, StringComparer.Ordinal)
            .ThenBy(target => target.Partition)
            .ToArray();
    }

    private static Dictionary<
        (string Topic, int Partition),
        RecordsPurgeTarget> ExpectedTargets(
        IReadOnlyList<RecordsPurgeTarget> targets) =>
        targets.ToDictionary(
            target => (target.TopicName, target.Partition));

    private static MutationProviderResult Accepted(
        int totalCount,
        int appliedCount,
        int failedCount) =>
        new(
            MutationExecutionResultKind.AppliedUnverified,
            "records_purge_accepted",
            Evidence(
                totalCount,
                appliedCount,
                failedCount,
                Array.Empty<KafkaFailure>()));

    private static MutationProviderResult Malformed(
        int totalCount,
        int appliedCount) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            "records_purge_provider_result_invalid",
            Evidence(
                totalCount,
                Math.Clamp(appliedCount, 0, totalCount),
                Math.Max(0, totalCount - appliedCount),
                Array.Empty<KafkaFailure>()));

    private static IReadOnlyDictionary<string, string> Evidence(
        int totalCount,
        int appliedCount,
        int failedCount,
        IReadOnlyList<KafkaFailure> failures)
    {
        var result =
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["provider.accepted"] =
                    (appliedCount > 0)
                    .ToString()
                    .ToLowerInvariant(),
                ["target.count"] =
                    totalCount.ToString(
                        CultureInfo.InvariantCulture),
                ["applied.count"] =
                    appliedCount.ToString(
                        CultureInfo.InvariantCulture),
                ["failed.count"] =
                    failedCount.ToString(
                        CultureInfo.InvariantCulture),
            };

        var codes = failures
            .Select(failure => failure.Code)
            .Where(code => !string.IsNullOrWhiteSpace(code))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(code => code, StringComparer.Ordinal)
            .ToArray();

        if (codes.Length > 0)
            result["provider.error.codes"] =
                string.Join(",", codes);

        return result;
    }

    private static MutationProviderResult Failed(
        string code) =>
        new(
            MutationExecutionResultKind.FailedDefinitive,
            code);

    private static MutationProviderResult Unknown(
        string code) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code);

    private static bool IsAmbiguous(
        KafkaFailure failure)
    {
        if (failure.Category is
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Unknown or
            KafkaFailureCategory.Cancelled)
        {
            return true;
        }

        if (failure.Category ==
            KafkaFailureCategory.ProtocolError)
        {
            return !string.Equals(
                failure.Code,
                "offset_out_of_range",
                StringComparison.Ordinal);
        }

        return false;
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
                "Kafka records purge timeout must be between one and thirty seconds.");
        }

        return value;
    }
}
