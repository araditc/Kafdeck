using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Clusters;

public enum ClusterHealthState
{
    Healthy = 1,
    Degraded = 2,
    Unavailable = 3,
    Unknown = 4,
}

public enum ClusterHealthReasonCode
{
    MetadataAvailable = 1,
    StaleObservation = 2,
    NoBrokersObserved = 3,
    KafkaUnavailable = 4,
    KafkaTimeout = 5,
    AuthenticationFailed = 6,
    TlsFailure = 7,
    MetadataUnauthorized = 8,
    MetadataUnsupported = 9,
    MetadataInvalidConfiguration = 10,
    MetadataFailure = 11,
}

public sealed record ClusterHealthReason(
    ClusterHealthReasonCode Code,
    string Description);

public sealed record ClusterAccessLimitation(
    KafkaCapabilityKind? Capability,
    KafkaCapabilityState State,
    string? Reason);

public sealed record BrokerProjection(
    int BrokerId,
    string Host,
    int Port,
    string? Rack,
    bool IsController);

public sealed record ClusterProjection(
    string ClusterId,
    string? KafkaClusterId,
    int? ControllerBrokerId,
    IReadOnlyList<BrokerProjection> Brokers,
    ClusterHealthState Health,
    IReadOnlyList<ClusterHealthReason> HealthReasons,
    IReadOnlyList<ClusterAccessLimitation> Limitations,
    ObservationMetadata Observation,
    KafkaFailure? Failure);

/// <summary>
/// Read-only cluster and broker application service. Kafka-derived values always carry
/// the W03/W05 observation metadata and authorization limitations never masquerade as outage.
/// </summary>
public sealed class ClusterExplorerService
{
    private static readonly TimeSpan CapabilityTtl = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ConfigurationTtl = TimeSpan.FromSeconds(30);

    private readonly IKafkaAdministrationPort _kafka;
    private readonly KafkaSnapshotCoordinator _snapshots;
    private readonly KafkaSnapshotPolicy _policy;

    public ClusterExplorerService(
        IKafkaAdministrationPort kafka,
        KafkaSnapshotCoordinator snapshots,
        KafkaSnapshotPolicy? policy = null)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _snapshots = snapshots ?? throw new ArgumentNullException(nameof(snapshots));
        _policy = policy ?? new KafkaSnapshotPolicy();
    }

    public async Task<IReadOnlyList<ClusterProjection>> ListClustersAsync(
        IEnumerable<string> clusterIds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(clusterIds);

        var ids = clusterIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToArray();

        var results = new List<ClusterProjection>(ids.Length);
        foreach (var clusterId in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();
            results.Add(await GetClusterAsync(clusterId, cancellationToken).ConfigureAwait(false));
        }

        return results;
    }

    public async Task<ClusterProjection> GetClusterAsync(
        string clusterId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);

        var metadata = await _snapshots.ObserveAsync(
                clusterId,
                "cluster-metadata",
                _policy.ClusterMetadataTtl,
                (operation, token) => _kafka.GetClusterMetadataAsync(clusterId, operation, token),
                cancellationToken)
            .ConfigureAwait(false);

        if (!metadata.IsSuccess || metadata.Value is null)
        {
            var failure = metadata.Failure
                ?? new KafkaFailure(
                    KafkaFailureCategory.Unknown,
                    "cluster_metadata_unknown",
                    "Cluster metadata could not be observed.",
                    false);

            return new ClusterProjection(
                clusterId,
                null,
                null,
                Array.Empty<BrokerProjection>(),
                HealthFromFailure(failure),
                [ReasonFromFailure(failure)],
                LimitationsFromFailure(failure),
                metadata.Observation,
                failure);
        }

        var capabilities = await _snapshots.ObserveAsync(
                clusterId,
                "capabilities",
                CapabilityTtl,
                (operation, token) => _kafka.GetCapabilitiesAsync(clusterId, operation, token),
                cancellationToken)
            .ConfigureAwait(false);

        var brokers = metadata.Value.Brokers
            .OrderBy(broker => broker.BrokerId)
            .Select(broker => new BrokerProjection(
                broker.BrokerId,
                broker.Host,
                broker.Port,
                broker.Rack,
                broker.IsController))
            .ToArray();

        var healthReasons = new List<ClusterHealthReason>();
        ClusterHealthState health;

        if (brokers.Length == 0)
        {
            health = ClusterHealthState.Degraded;
            healthReasons.Add(new ClusterHealthReason(
                ClusterHealthReasonCode.NoBrokersObserved,
                "Cluster metadata was available but no brokers were observed."));
        }
        else if (metadata.Observation.Source == ObservationSource.StaleSnapshot)
        {
            health = ClusterHealthState.Degraded;
            healthReasons.Add(new ClusterHealthReason(
                ClusterHealthReasonCode.StaleObservation,
                "Cluster metadata is being served from a stale last-known-good snapshot."));
        }
        else
        {
            health = ClusterHealthState.Healthy;
            healthReasons.Add(new ClusterHealthReason(
                ClusterHealthReasonCode.MetadataAvailable,
                "Cluster metadata is currently observable."));
        }

        return new ClusterProjection(
            clusterId,
            metadata.Value.KafkaClusterId,
            metadata.Value.ControllerBrokerId,
            brokers,
            health,
            healthReasons,
            LimitationsFromCapabilities(capabilities),
            metadata.Observation,
            null);
    }

    public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>> GetBrokerConfigurationAsync(
        string clusterId,
        int brokerId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        if (brokerId < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(brokerId));
        }

        return _snapshots.ObserveAsync(
            clusterId,
            $"broker-config:{brokerId}",
            ConfigurationTtl,
            (operation, token) => _kafka.GetBrokerConfigurationAsync(clusterId, brokerId, operation, token),
            cancellationToken);
    }

    private static ClusterHealthState HealthFromFailure(KafkaFailure failure) => failure.Category switch
    {
        KafkaFailureCategory.Unavailable => ClusterHealthState.Unavailable,
        KafkaFailureCategory.Timeout => ClusterHealthState.Unavailable,
        KafkaFailureCategory.AuthenticationFailed => ClusterHealthState.Unavailable,
        KafkaFailureCategory.TlsFailure => ClusterHealthState.Unavailable,
        KafkaFailureCategory.Unauthorized => ClusterHealthState.Unknown,
        KafkaFailureCategory.NotSupported => ClusterHealthState.Unknown,
        KafkaFailureCategory.InvalidConfiguration => ClusterHealthState.Unknown,
        _ => ClusterHealthState.Unknown,
    };

    private static ClusterHealthReason ReasonFromFailure(KafkaFailure failure) => failure.Category switch
    {
        KafkaFailureCategory.Unavailable => new(
            ClusterHealthReasonCode.KafkaUnavailable,
            "Kafka cluster metadata is currently unavailable."),
        KafkaFailureCategory.Timeout => new(
            ClusterHealthReasonCode.KafkaTimeout,
            "Kafka cluster metadata did not complete within the bounded deadline."),
        KafkaFailureCategory.AuthenticationFailed => new(
            ClusterHealthReasonCode.AuthenticationFailed,
            "Kafka authentication failed."),
        KafkaFailureCategory.TlsFailure => new(
            ClusterHealthReasonCode.TlsFailure,
            "Kafka TLS validation or negotiation failed."),
        KafkaFailureCategory.Unauthorized => new(
            ClusterHealthReasonCode.MetadataUnauthorized,
            "Cluster metadata access was denied; operational health is unknown."),
        KafkaFailureCategory.NotSupported => new(
            ClusterHealthReasonCode.MetadataUnsupported,
            "Cluster metadata is not supported by the active provider capability set."),
        KafkaFailureCategory.InvalidConfiguration => new(
            ClusterHealthReasonCode.MetadataInvalidConfiguration,
            "Cluster metadata cannot be observed because the configured connection is invalid."),
        _ => new(
            ClusterHealthReasonCode.MetadataFailure,
            "Cluster metadata could not be observed; operational health is unknown."),
    };

    private static IReadOnlyList<ClusterAccessLimitation> LimitationsFromFailure(KafkaFailure failure)
    {
        var state = failure.Category switch
        {
            KafkaFailureCategory.Unauthorized => KafkaCapabilityState.Unauthorized,
            KafkaFailureCategory.NotSupported => KafkaCapabilityState.Unsupported,
            KafkaFailureCategory.Unavailable or KafkaFailureCategory.Timeout => KafkaCapabilityState.Unavailable,
            _ => KafkaCapabilityState.Unknown,
        };

        return
        [
            new ClusterAccessLimitation(
                KafkaCapabilityKind.ClusterMetadata,
                state,
                failure.SafeMessage),
        ];
    }

    private static IReadOnlyList<ClusterAccessLimitation> LimitationsFromCapabilities(
        KafkaResult<KafkaCapabilities> capabilities)
    {
        if (!capabilities.IsSuccess || capabilities.Value is null)
        {
            return
            [
                new ClusterAccessLimitation(
                    null,
                    KafkaCapabilityState.Unknown,
                    capabilities.Failure?.SafeMessage ?? "Capability evidence could not be observed."),
            ];
        }

        return capabilities.Value.Items
            .Where(item => item.State != KafkaCapabilityState.Available)
            .OrderBy(item => item.Capability)
            .Select(item => new ClusterAccessLimitation(item.Capability, item.State, item.Reason))
            .ToArray();
    }
}
