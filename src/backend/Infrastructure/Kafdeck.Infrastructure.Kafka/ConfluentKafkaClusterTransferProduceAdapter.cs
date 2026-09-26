using System.Globalization;
using System.Security.Cryptography;
using Confluent.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

/// <summary>
/// W47-only byte-preserving producer. The caller must durably mark transfer
/// dispatch before invoking this adapter. No topic creation, partition choice,
/// retry loop or payload transformation is performed here.
/// </summary>
public sealed class ConfluentKafkaClusterTransferProduceAdapter :
    IClusterTransferProducePort,
    IDisposable
{
    private readonly KafkaProducerRegistry _producers;

    public ConfluentKafkaClusterTransferProduceAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver)
    {
        _producers = new KafkaProducerRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));
    }

    public async Task<MutationProviderResult> ProduceAsync(
        ClusterTransferProduceMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (cancellationToken.IsCancellationRequested)
            return Unknown("cluster_transfer_produce_cancelled_or_timeout");

        if (!_producers.ContainsCluster(request.ClusterId))
            return Failed("cluster_transfer_destination_cluster_not_configured");

        if (!IsExactTopicName(request.TopicName) || request.Partition < 0)
            return Failed("cluster_transfer_invalid_destination");

        byte[]? key = null;
        byte[]? value = null;
        var headerCopies = new List<byte[]>();

        try
        {
            key = request.Key?.ToArray();
            value = request.Value?.ToArray();

            var headers = new Headers();
            foreach (var header in request.Headers)
            {
                if (header.Name.Length > 256 || header.Name.Any(char.IsControl))
                    return Failed("cluster_transfer_invalid_header");

                var headerValue = header.Value.ToArray();
                headerCopies.Add(headerValue);
                headers.Add(header.Name, headerValue);
            }

            var producer = _producers.GetProducer(request.ClusterId);
            var target = new TopicPartition(
                request.TopicName,
                new Partition(request.Partition));

            // The durable W47 checkpoint owns cancellation semantics after its
            // DispatchStarted marker. Do not cancel an enqueued librdkafka send
            // from a request token and then falsely infer non-application.
            var delivery = await producer
                .ProduceAsync(
                    target,
                    new Message<byte[], byte[]>
                    {
                        // Confluent.Kafka models tombstone/null payloads at runtime even though
                        // the generic Message annotations are non-nullable for byte[]. The
                        // null-forgiving operators preserve the actual null value without
                        // substituting an empty array.
                        Key = key!,
                        Value = value!,
                        Headers = headers,
                    },
                    CancellationToken.None)
                .ConfigureAwait(false);

            if (delivery.Status == PersistenceStatus.NotPersisted)
                return Failed("cluster_transfer_not_persisted");

            if (delivery.Status != PersistenceStatus.Persisted ||
                delivery.Partition.Value != request.Partition ||
                delivery.Offset.Value < 0)
                return Unknown("cluster_transfer_produce_outcome_unverified");

            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "cluster_transfer_record_acknowledged",
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["provider.accepted"] = "true",
                    ["partition"] = delivery.Partition.Value.ToString(CultureInfo.InvariantCulture),
                    ["offset"] = delivery.Offset.Value.ToString(CultureInfo.InvariantCulture),
                    ["verification.state"] = "observed",
                });
        }
        catch (ProduceException<byte[], byte[]> exception)
        {
            return FromError(exception.Error);
        }
        catch (OperationCanceledException)
        {
            return Unknown("cluster_transfer_produce_cancelled_or_timeout");
        }
        catch (TimeoutException)
        {
            return Unknown("cluster_transfer_produce_timeout");
        }
        catch (KafkaException exception)
        {
            return FromError(exception.Error);
        }
        catch (KafdeckConfigurationException)
        {
            return Failed("cluster_transfer_invalid_configuration");
        }
        catch (KeyNotFoundException)
        {
            return Failed("cluster_transfer_destination_cluster_not_configured");
        }
        catch (ArgumentException)
        {
            return Failed("cluster_transfer_invalid_request");
        }
        catch (InvalidOperationException)
        {
            return Failed("cluster_transfer_invalid_operation");
        }
        catch
        {
            return Unknown("cluster_transfer_provider_exception");
        }
        finally
        {
            Zero(key);
            Zero(value);
            foreach (var item in headerCopies)
                CryptographicOperations.ZeroMemory(item);
        }
    }

    public void Dispose() => _producers.Dispose();

    private static MutationProviderResult FromError(Error error)
    {
        var failure = KafkaFailureMapper.FromKafka(error);
        return failure.Category is
            Kafdeck.Core.Kafka.KafkaFailureCategory.Timeout or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Unavailable or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Unknown
            ? Unknown($"cluster_transfer_{failure.Code}")
            : Failed($"cluster_transfer_{failure.Code}");
    }

    private static MutationProviderResult Failed(string code) =>
        new(MutationExecutionResultKind.FailedDefinitive, code);

    private static MutationProviderResult Unknown(string code) =>
        new(MutationExecutionResultKind.ExecutionUnknown, code);

    private static bool IsExactTopicName(string value) =>
        !string.IsNullOrWhiteSpace(value) &&
        value.Length <= 249 &&
        string.Equals(value, value.Trim(), StringComparison.Ordinal) &&
        value is not "." and not ".." &&
        value.All(character =>
            character is >= 'a' and <= 'z' or
            >= 'A' and <= 'Z' or
            >= '0' and <= '9' or
            '.' or '_' or '-');

    private static void Zero(byte[]? value)
    {
        if (value is not null)
            CryptographicOperations.ZeroMemory(value);
    }
}
