using Kafdeck.Core.ReadViews;

namespace Kafdeck.Core.Ecosystem;

public sealed record ConnectProfileSummary(
    string Id,
    bool IsDefault,
    string MutationProviderProfile);

public sealed record ConnectClusterInfo(string? Version, string? Commit, string? KafkaClusterId);

public sealed record ConnectTaskStatus(int Id, string State, string? WorkerId, string? SafeTrace);

public sealed record ConnectConnectorSummary(string Name);

public sealed record ConnectPluginSummary(
    string Class,
    string Type,
    string? Version);

public sealed record ConnectPluginValidationField(
    string Name,
    string Type,
    bool Required,
    IReadOnlyList<string> Errors,
    IReadOnlyList<string> RecommendedValues);

public sealed record ConnectPluginValidationResult(
    string ConnectorClass,
    int ErrorCount,
    IReadOnlyList<ConnectPluginValidationField> Fields);

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

    Task<ReadViewResult<IReadOnlyList<ConnectProfileSummary>>> ListProfilesAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        Task.FromResult(
            ReadViewResult<IReadOnlyList<ConnectProfileSummary>>.Failed(
                new ReadViewFailure(
                    ReadViewFailureCategory.Unsupported,
                    "connect_profiles_unsupported",
                    "Kafka Connect profile discovery is unsupported by this provider.",
                    false)));

    Task<ReadViewResult<ConnectClusterInfo>> GetClusterInfoAsync(
        string clusterId,
        string connectProfileId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        string.Equals(connectProfileId, "default", StringComparison.Ordinal)
            ? GetClusterInfoAsync(
                clusterId,
                operation,
                cancellationToken)
            : Task.FromResult(
                ReadViewResult<ConnectClusterInfo>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.NotConfigured,
                        "connect_profile_not_configured",
                        "Kafka Connect profile is not configured for the requested cluster.",
                        false)));

    Task<ReadViewResult<IReadOnlyList<ConnectConnectorSummary>>> ListConnectorsAsync(
        string clusterId,
        string connectProfileId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        string.Equals(connectProfileId, "default", StringComparison.Ordinal)
            ? ListConnectorsAsync(
                clusterId,
                operation,
                cancellationToken)
            : Task.FromResult(
                ReadViewResult<IReadOnlyList<ConnectConnectorSummary>>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.NotConfigured,
                        "connect_profile_not_configured",
                        "Kafka Connect profile is not configured for the requested cluster.",
                        false)));

    Task<ReadViewResult<ConnectConnectorDetail>> GetConnectorAsync(
        string clusterId,
        string connectProfileId,
        string connectorName,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        string.Equals(connectProfileId, "default", StringComparison.Ordinal)
            ? GetConnectorAsync(
                clusterId,
                connectorName,
                operation,
                cancellationToken)
            : Task.FromResult(
                ReadViewResult<ConnectConnectorDetail>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.NotConfigured,
                        "connect_profile_not_configured",
                        "Kafka Connect profile is not configured for the requested cluster.",
                        false)));

    Task<ReadViewResult<IReadOnlyList<ConnectPluginSummary>>> ListPluginsAsync(
        string clusterId,
        string connectProfileId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        Task.FromResult(
            ReadViewResult<IReadOnlyList<ConnectPluginSummary>>.Failed(
                new ReadViewFailure(
                    ReadViewFailureCategory.Unsupported,
                    "connect_plugin_discovery_unsupported",
                    "Kafka Connect plugin discovery is unsupported by the configured provider.",
                    false)));

    Task<ReadViewResult<ConnectPluginValidationResult>> ValidateConfigurationAsync(
        string clusterId,
        string connectProfileId,
        string connectorClass,
        IReadOnlyDictionary<string, string> configuration,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken) =>
        Task.FromResult(
            ReadViewResult<ConnectPluginValidationResult>.Failed(
                new ReadViewFailure(
                    ReadViewFailureCategory.Unsupported,
                    "connect_plugin_validation_unsupported",
                    "Kafka Connect plugin validation is unsupported by the configured provider.",
                    false)));
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
