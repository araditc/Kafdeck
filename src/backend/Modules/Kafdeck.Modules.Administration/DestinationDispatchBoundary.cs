using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Kafdeck.Modules.Administration;

/// <summary>Trusted provider observation bound durably before dispatch. It carries no payload or credential.</summary>
public sealed record DestinationDispatchBoundary(string ClusterId, string KafkaClusterId, string TopicName,
    string TopicId, int Partition, long LowWatermark, long HighWatermark, long WorkerGeneration, DateTimeOffset ObservedAtUtc)
{
    public DestinationDispatchBoundary Validate()
    {
        foreach (var value in new[] { ClusterId, KafkaClusterId, TopicName })
            if (string.IsNullOrWhiteSpace(value) || value.Length > 256 || value != value.Trim() || value.Any(char.IsControl))
                throw new MutationStateException("Destination observation identity is invalid.");
        if (string.IsNullOrWhiteSpace(TopicId) || TopicId.Length != 32 || !TopicId.All(char.IsAsciiHexDigit) || TopicId.All(c => c == '0') ||
            TopicId != TopicId.ToLowerInvariant() || Partition < 0 || LowWatermark < 0 || HighWatermark < LowWatermark ||
            WorkerGeneration <= 0 || ObservedAtUtc <= DateTimeOffset.UnixEpoch)
            throw new MutationStateException("Destination observation boundary is invalid.");
        return this;
    }

    public bool SameDestinationAndOffsets(DestinationDispatchBoundary other) =>
        ClusterId == other.ClusterId && KafkaClusterId == other.KafkaClusterId && TopicName == other.TopicName &&
        TopicId == other.TopicId && Partition == other.Partition && LowWatermark == other.LowWatermark && HighWatermark == other.HighWatermark;

    public string Fingerprint => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("\n",
        ClusterId, KafkaClusterId, TopicName, TopicId, Partition.ToString(CultureInfo.InvariantCulture),
        LowWatermark.ToString(CultureInfo.InvariantCulture), HighWatermark.ToString(CultureInfo.InvariantCulture),
        WorkerGeneration.ToString(CultureInfo.InvariantCulture), ObservedAtUtc.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture))))).ToLowerInvariant();
}

public sealed record DestinationBoundaryObservation(DestinationDispatchBoundary? Boundary, string Code)
{
    public bool IsSuccess => Boundary is not null;
}

public interface IDestinationDispatchObserver
{
    Task<DestinationBoundaryObservation> ObserveAsync(string clusterId, string topicName, int partition,
        long workerGeneration, CancellationToken cancellationToken = default);
}
