using Kafdeck.Core.Ecosystem;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

/// <summary>
/// Common W41 guard that prevents the admitted v0.5 Connect mutation surface
/// from becoming an alternate activation path for managed replication.
///
/// This guard does not admit or implement MM2. It recognizes only the Apache
/// MirrorMaker 2 connector classes whose activation can start/resume replication
/// and fails closed when an activation path cannot classify connector.class from
/// safe canonical/observed configuration evidence.
/// </summary>
public static class ConnectReplicationActivationGuard
{
    private const string ConnectorClassKey = "connector.class";

    private static readonly HashSet<string> ManagedReplicationConnectorClasses =
        new(StringComparer.Ordinal)
        {
            "org.apache.kafka.connect.mirror.MirrorSourceConnector",
            "MirrorSourceConnector",
            "MirrorSource",
            "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
            "MirrorCheckpointConnector",
            "MirrorCheckpoint",
            "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
            "MirrorHeartbeatConnector",
            "MirrorHeartbeat",
        };

    public static MutationPreDispatchGuardResult ValidateCreate(
        IReadOnlyList<ConnectConfigurationCanonicalItem> requestedConfiguration) =>
        ValidateActivationClassification(
            Classify(requestedConfiguration),
            "connect_create");

    public static MutationPreDispatchGuardResult ValidateUpdate(
        IReadOnlyList<ConnectConfigurationCanonicalItem> requestedConfiguration,
        IReadOnlyList<ConnectConfigurationObservationItem> currentConfiguration)
    {
        var requested = Classify(requestedConfiguration);
        var current = Classify(currentConfiguration);

        if (requested == ConnectReplicationClassification.ManagedReplication ||
            current == ConnectReplicationClassification.ManagedReplication)
        {
            return ManagedReplicationBlocked("connect_update");
        }

        if (requested == ConnectReplicationClassification.Unavailable ||
            current == ConnectReplicationClassification.Unavailable)
        {
            return ClassificationUnavailable("connect_update");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    public static MutationPreDispatchGuardResult ValidateControl(
        ConnectControlAction action,
        IReadOnlyList<ConnectConfigurationObservationItem> currentConfiguration)
    {
        // Pause cannot start/resume replication and remains on the existing
        // typed Connect control path. Resume/restart/task-restart can activate
        // external effects and therefore must pass replication classification.
        if (action == ConnectControlAction.Pause)
        {
            return MutationPreDispatchGuardResult.Allowed;
        }

        if (action is not (ConnectControlAction.Resume or ConnectControlAction.Restart))
        {
            return new MutationPreDispatchGuardResult(
                MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                "connect_activation_action_not_admitted");
        }

        return ValidateActivationClassification(
            Classify(currentConfiguration),
            action == ConnectControlAction.Resume
                ? "connect_resume"
                : "connect_restart");
    }

    internal static bool IsManagedReplicationConnectorClass(string connectorClass) =>
        ManagedReplicationConnectorClasses.Contains(connectorClass);

    private static ConnectReplicationClassification Classify(
        IReadOnlyList<ConnectConfigurationCanonicalItem> configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var matches = configuration
            .Where(item => string.Equals(
                item.Key,
                ConnectorClassKey,
                StringComparison.Ordinal))
            .ToArray();

        return matches.Length == 1
            ? ClassifySafeValue(matches[0].SafeValue)
            : ConnectReplicationClassification.Unavailable;
    }

    private static ConnectReplicationClassification Classify(
        IReadOnlyList<ConnectConfigurationObservationItem> configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var matches = configuration
            .Where(item => string.Equals(
                item.Key,
                ConnectorClassKey,
                StringComparison.Ordinal))
            .ToArray();

        return matches.Length == 1
            ? ClassifySafeValue(matches[0].SafeValue)
            : ConnectReplicationClassification.Unavailable;
    }

    private static ConnectReplicationClassification ClassifySafeValue(string? safeValue)
    {
        if (string.IsNullOrWhiteSpace(safeValue) ||
            string.Equals(safeValue, "[REDACTED]", StringComparison.Ordinal))
        {
            return ConnectReplicationClassification.Unavailable;
        }

        return ManagedReplicationConnectorClasses.Contains(safeValue)
            ? ConnectReplicationClassification.ManagedReplication
            : ConnectReplicationClassification.NonReplication;
    }

    private static MutationPreDispatchGuardResult ValidateActivationClassification(
        ConnectReplicationClassification classification,
        string path) =>
        classification switch
        {
            ConnectReplicationClassification.NonReplication =>
                MutationPreDispatchGuardResult.Allowed,

            ConnectReplicationClassification.ManagedReplication =>
                ManagedReplicationBlocked(path),

            ConnectReplicationClassification.Unavailable =>
                ClassificationUnavailable(path),

            _ => new MutationPreDispatchGuardResult(
                MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                "connect_activation_classification_invalid"),
        };

    private static MutationPreDispatchGuardResult ManagedReplicationBlocked(
        string path) =>
        new(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            $"{path}_replication_managed_activation_not_admitted");

    private static MutationPreDispatchGuardResult ClassificationUnavailable(
        string path) =>
        new(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            $"{path}_replication_classification_unavailable");

    private enum ConnectReplicationClassification
    {
        NonReplication = 1,
        ManagedReplication = 2,
        Unavailable = 3,
    }
}
