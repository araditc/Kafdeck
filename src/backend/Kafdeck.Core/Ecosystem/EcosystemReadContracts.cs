using Kafdeck.Core.ReadViews;

namespace Kafdeck.Core.Ecosystem;

public sealed record ConnectClusterInfo(string? Version, string? Commit, string? KafkaClusterId);

public sealed record ConnectTaskStatus(int Id, string State, string? WorkerId, string? SafeTrace);

public sealed record ConnectConnectorSummary(string Name);

public sealed record ConnectConnectorDetail(
    string Name,
    string State,
    string? WorkerId,
    IReadOnlyList<ConnectTaskStatus> Tasks,
    IReadOnlyDictionary<string, string?> SafeConfiguration);

public interface IConnectReadPort
{
    Task<ReadViewResult<ConnectClusterInfo>> GetClusterInfoAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);

    Task<ReadViewResult<IReadOnlyList<ConnectConnectorSummary>>> ListConnectorsAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);

    Task<ReadViewResult<ConnectConnectorDetail>> GetConnectorAsync(
        string clusterId,
        string connectorName,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);
}

public sealed record KsqlServerInfo(string? Version, string? KafkaClusterId, string? State);

public sealed record KsqlMetadataItem(
    string Kind,
    string Name,
    string? KafkaTopic,
    string? ValueFormat,
    string? KeyFormat);

public interface IKsqlMetadataReadPort
{
    Task<ReadViewResult<KsqlServerInfo>> GetServerInfoAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);

    Task<ReadViewResult<IReadOnlyList<KsqlMetadataItem>>> ListMetadataAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);
}

public interface IMetricsObservationPort
{
    Task<ReadViewResult<ConsumerRateObservation>> GetConsumerRateAsync(
        string clusterId,
        string groupId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);
}

public interface IHistoryObservationPort
{
    Task<ReadViewResult<IReadOnlyList<ConsumerHistoryObservation>>> GetConsumerHistoryAsync(
        string clusterId,
        string groupId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);
}

public sealed record ConsumerRateObservation(
    double? ProduceRecordsPerSecond,
    double? ConsumeRecordsPerSecond,
    TimeSpan Window,
    DateTimeOffset ObservedAt,
    string Source);

public sealed record ConsumerHistoryObservation(
    DateTimeOffset ObservedAt,
    string State,
    long? TotalLag,
    string Source);
