using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Confluent.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaRecordProduceAdapter :
    IRecordProduceMutationPort,
    IDisposable
{
    private readonly KafkaProducerRegistry _producers;

    public ConfluentKafkaRecordProduceAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver)
    {
        _producers = new KafkaProducerRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));
    }

    public async Task<MutationProviderResult> ProduceAsync(
        RecordProduceMutation request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (cancellationToken.IsCancellationRequested)
            return Unknown("record_produce_cancelled_or_timeout");

        if (!_producers.ContainsCluster(request.ClusterId))
            return Failed("record_produce_cluster_not_configured");

        if (!IsExactTopicName(request.TopicName))
            return Failed("record_produce_invalid_topic");

        byte[]? key = null;
        byte[]? value = null;
        var headerCopies = new List<byte[]>();

        try
        {
            key = request.Key?.ToArray();
            value = request.Value.ToArray();

            var headers = new Headers();
            foreach (var pair in request.Headers.OrderBy(pair => pair.Key, StringComparer.Ordinal))
            {
                if (string.IsNullOrWhiteSpace(pair.Key) ||
                    !string.Equals(pair.Key, pair.Key.Trim(), StringComparison.Ordinal) ||
                    pair.Key.Length > 256 ||
                    pair.Key.Any(char.IsControl))
                {
                    return Failed("record_produce_invalid_header");
                }

                var headerValue = pair.Value.ToArray();
                headerCopies.Add(headerValue);
                headers.Add(pair.Key, headerValue);
            }

            var producer = _producers.GetProducer(request.ClusterId);
            var delivery = await producer
                .ProduceAsync(
                    request.TopicName,
                    new Message<byte[], byte[]>
                    {
                        Key = key!,
                        Value = value,
                        Headers = headers,
                    },
                    cancellationToken)
                .ConfigureAwait(false);

            return delivery.Status switch
            {
                PersistenceStatus.Persisted when
                    delivery.Partition.Value >= 0 &&
                    delivery.Offset.Value >= 0 =>
                    Acknowledged(delivery.Partition.Value, delivery.Offset.Value),
                PersistenceStatus.NotPersisted =>
                    Failed("record_produce_not_persisted"),
                _ =>
                    Unknown("record_produce_possibly_persisted"),
            };
        }
        catch (ProduceException<byte[], byte[]> exception)
        {
            return FromError(exception.Error);
        }
        catch (OperationCanceledException)
        {
            return Unknown("record_produce_cancelled_or_timeout");
        }
        catch (TimeoutException)
        {
            return Unknown("record_produce_timeout");
        }
        catch (KafkaException exception)
        {
            return FromError(exception.Error);
        }
        catch (KafdeckConfigurationException)
        {
            return Failed("record_produce_invalid_configuration");
        }
        catch (KeyNotFoundException)
        {
            return Failed("record_produce_cluster_not_configured");
        }
        catch (ArgumentException)
        {
            return Failed("record_produce_invalid_request");
        }
        catch (InvalidOperationException)
        {
            return Failed("record_produce_invalid_operation");
        }
        catch
        {
            return Unknown("record_produce_provider_exception");
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

    private static MutationProviderResult Acknowledged(int partition, long offset) =>
        new(
            MutationExecutionResultKind.AppliedVerified,
            "record_produce_acknowledged",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["provider.accepted"] = "true",
                ["partition"] = partition.ToString(CultureInfo.InvariantCulture),
                ["offset"] = offset.ToString(CultureInfo.InvariantCulture),
                ["verification.state"] = "observed",
            });

    private static MutationProviderResult FromError(Error error)
    {
        var failure = KafkaFailureMapper.FromKafka(error);
        return failure.Category is
            Kafdeck.Core.Kafka.KafkaFailureCategory.Timeout or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Unavailable or
            Kafdeck.Core.Kafka.KafkaFailureCategory.Unknown
            ? Unknown($"record_produce_{failure.Code}")
            : Failed($"record_produce_{failure.Code}");
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
