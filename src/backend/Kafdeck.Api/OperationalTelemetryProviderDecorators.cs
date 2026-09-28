using Kafdeck.Core.Consumers;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Observability;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;

namespace Kafdeck.Api;

internal static class OperationalTelemetryObservation
{
    public static async Task<KafkaResult<T>>
        ObserveKafkaAsync<T>(
            IKafdeckOperationalTelemetry telemetry,
            KafdeckOperationalFamily family,
            Func<Task<KafkaResult<T>>> action)
    {
        using var scope = telemetry.Start(
            KafdeckOperationalKind.Provider,
            family);

        try
        {
            var result =
                await action().ConfigureAwait(false);
            scope.Complete(
                result.Failure is null
                    ? KafdeckOperationalOutcome.Success
                    : MapKafkaFailure(
                        result.Failure.Category));
            return result;
        }
        catch (OperationCanceledException)
        {
            scope.Complete(
                KafdeckOperationalOutcome.Cancelled);
            throw;
        }
        catch
        {
            scope.Complete(
                KafdeckOperationalOutcome.Failed);
            throw;
        }
    }

    public static async Task<ReadViewResult<T>>
        ObserveReadViewAsync<T>(
            IKafdeckOperationalTelemetry telemetry,
            KafdeckOperationalFamily family,
            Func<Task<ReadViewResult<T>>> action)
    {
        using var scope = telemetry.Start(
            KafdeckOperationalKind.Provider,
            family);

        try
        {
            var result =
                await action().ConfigureAwait(false);
            scope.Complete(
                result.Failure is null
                    ? KafdeckOperationalOutcome.Success
                    : MapReadViewFailure(
                        result.Failure.Category));
            return result;
        }
        catch (OperationCanceledException)
        {
            scope.Complete(
                KafdeckOperationalOutcome.Cancelled);
            throw;
        }
        catch
        {
            scope.Complete(
                KafdeckOperationalOutcome.Failed);
            throw;
        }
    }

    public static async Task<RecordSchemaResult<T>>
        ObserveSchemaAsync<T>(
            IKafdeckOperationalTelemetry telemetry,
            Func<Task<RecordSchemaResult<T>>> action)
    {
        using var scope = telemetry.Start(
            KafdeckOperationalKind.Provider,
            KafdeckOperationalFamily.SchemaRegistryRead);

        try
        {
            var result =
                await action().ConfigureAwait(false);
            scope.Complete(
                result.Failure is null
                    ? KafdeckOperationalOutcome.Success
                    : MapSchemaFailure(
                        result.Failure.Category));
            return result;
        }
        catch (OperationCanceledException)
        {
            scope.Complete(
                KafdeckOperationalOutcome.Cancelled);
            throw;
        }
        catch
        {
            scope.Complete(
                KafdeckOperationalOutcome.Failed);
            throw;
        }
    }

    private static KafdeckOperationalOutcome
        MapKafkaFailure(
            KafkaFailureCategory category) =>
        category switch
        {
            KafkaFailureCategory.Unauthorized or
            KafkaFailureCategory.AuthenticationFailed =>
                KafdeckOperationalOutcome.Denied,
            KafkaFailureCategory.NotSupported =>
                KafdeckOperationalOutcome.Unsupported,
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.TlsFailure =>
                KafdeckOperationalOutcome.Unavailable,
            KafkaFailureCategory.Timeout =>
                KafdeckOperationalOutcome.Timeout,
            KafkaFailureCategory.Cancelled =>
                KafdeckOperationalOutcome.Cancelled,
            KafkaFailureCategory.InvalidConfiguration =>
                KafdeckOperationalOutcome.Invalid,
            _ =>
                KafdeckOperationalOutcome.Failed,
        };

    private static KafdeckOperationalOutcome
        MapReadViewFailure(
            ReadViewFailureCategory category) =>
        category switch
        {
            ReadViewFailureCategory.NotConfigured =>
                KafdeckOperationalOutcome.Unconfigured,
            ReadViewFailureCategory.Unsupported =>
                KafdeckOperationalOutcome.Unsupported,
            ReadViewFailureCategory.Unauthorized =>
                KafdeckOperationalOutcome.Denied,
            ReadViewFailureCategory.Unavailable =>
                KafdeckOperationalOutcome.Unavailable,
            ReadViewFailureCategory.Timeout =>
                KafdeckOperationalOutcome.Timeout,
            ReadViewFailureCategory.Cancelled =>
                KafdeckOperationalOutcome.Cancelled,
            ReadViewFailureCategory.InvalidRequest or
            ReadViewFailureCategory.InvalidResponse or
            ReadViewFailureCategory.ResponseTooLarge =>
                KafdeckOperationalOutcome.Invalid,
            _ =>
                KafdeckOperationalOutcome.Failed,
        };

    private static KafdeckOperationalOutcome
        MapSchemaFailure(
            RecordSchemaFailureCategory category) =>
        category switch
        {
            RecordSchemaFailureCategory.RegistryNotConfigured =>
                KafdeckOperationalOutcome.Unconfigured,
            RecordSchemaFailureCategory.ProviderUnsupported or
            RecordSchemaFailureCategory.UnsupportedFormat =>
                KafdeckOperationalOutcome.Unsupported,
            RecordSchemaFailureCategory.Unauthorized =>
                KafdeckOperationalOutcome.Denied,
            RecordSchemaFailureCategory.Unavailable =>
                KafdeckOperationalOutcome.Unavailable,
            RecordSchemaFailureCategory.Timeout =>
                KafdeckOperationalOutcome.Timeout,
            RecordSchemaFailureCategory.Cancelled =>
                KafdeckOperationalOutcome.Cancelled,
            RecordSchemaFailureCategory.InvalidResponse or
            RecordSchemaFailureCategory.DecodeFailed =>
                KafdeckOperationalOutcome.Invalid,
            _ =>
                KafdeckOperationalOutcome.Failed,
        };
}

public sealed class TelemetryKafkaAdministrationPort
    : IKafkaAdministrationPort
{
    private readonly IKafkaAdministrationPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetryKafkaAdministrationPort(
        IKafkaAdministrationPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<KafkaResult<ClusterMetadata>>
        GetClusterMetadataAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveKafkaAsync(
            _telemetry,
            KafdeckOperationalFamily.KafkaMetadataRead,
            () => _inner.GetClusterMetadataAsync(
                clusterId,
                operation,
                cancellationToken));

    public Task<KafkaResult<IReadOnlyList<TopicSummary>>>
        ListTopicsAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveKafkaAsync(
            _telemetry,
            KafdeckOperationalFamily.KafkaMetadataRead,
            () => _inner.ListTopicsAsync(
                clusterId,
                operation,
                cancellationToken));

    public Task<KafkaResult<TopicMetadata>>
        GetTopicMetadataAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveKafkaAsync(
            _telemetry,
            KafdeckOperationalFamily.KafkaMetadataRead,
            () => _inner.GetTopicMetadataAsync(
                clusterId,
                topicName,
                operation,
                cancellationToken));

    public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>
        GetTopicConfigurationAsync(
            string clusterId,
            string topicName,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveKafkaAsync(
            _telemetry,
            KafdeckOperationalFamily.KafkaMetadataRead,
            () => _inner.GetTopicConfigurationAsync(
                clusterId,
                topicName,
                operation,
                cancellationToken));

    public Task<KafkaResult<IReadOnlyList<KafkaConfigurationEntry>>>
        GetBrokerConfigurationAsync(
            string clusterId,
            int brokerId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveKafkaAsync(
            _telemetry,
            KafdeckOperationalFamily.KafkaMetadataRead,
            () => _inner.GetBrokerConfigurationAsync(
                clusterId,
                brokerId,
                operation,
                cancellationToken));

    public Task<KafkaResult<KafkaCapabilities>>
        GetCapabilitiesAsync(
            string clusterId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveKafkaAsync(
            _telemetry,
            KafdeckOperationalFamily.KafkaMetadataRead,
            () => _inner.GetCapabilitiesAsync(
                clusterId,
                operation,
                cancellationToken));
}

public sealed class TelemetryKafkaRecordReadPort
    : IKafkaRecordReadPort
{
    private readonly IKafkaRecordReadPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetryKafkaRecordReadPort(
        IKafkaRecordReadPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<KafkaResult<RecordReadBatch>>
        ReadPageAsync(
            RecordReadRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveKafkaAsync(
            _telemetry,
            KafdeckOperationalFamily.KafkaRecordRead,
            () => _inner.ReadPageAsync(
                request,
                operation,
                cancellationToken));
}

public sealed class TelemetryConsumerGroupReadPort
    : IConsumerGroupReadPort
{
    private readonly IConsumerGroupReadPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetryConsumerGroupReadPort(
        IConsumerGroupReadPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<ReadViewResult<IReadOnlyList<ConsumerGroupSummary>>>
        ListGroupsAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.ConsumerGroupRead,
            () => _inner.ListGroupsAsync(
                clusterId,
                operation,
                cancellationToken));

    public Task<ReadViewResult<ConsumerGroupDetail>>
        GetGroupAsync(
            string clusterId,
            string groupId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.ConsumerGroupRead,
            () => _inner.GetGroupAsync(
                clusterId,
                groupId,
                operation,
                cancellationToken));

    public Task<ReadViewResult<IReadOnlyList<ConsumerOffsetProjection>>>
        GetOffsetsAsync(
            string clusterId,
            string groupId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.ConsumerGroupRead,
            () => _inner.GetOffsetsAsync(
                clusterId,
                groupId,
                operation,
                cancellationToken));
}

public sealed class TelemetryRecordSchemaReadPort
    : IRecordSchemaReadPort
{
    private readonly IRecordSchemaReadPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetryRecordSchemaReadPort(
        IRecordSchemaReadPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<RecordSchemaResult<RecordSchemaDocument>>
        GetSchemaByIdAsync(
            string clusterId,
            int schemaId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveSchemaAsync(
            _telemetry,
            () => _inner.GetSchemaByIdAsync(
                clusterId,
                schemaId,
                operation,
                cancellationToken));

    public Task<RecordSchemaResult<RecordSchemaDocument>>
        GetSchemaBySubjectVersionAsync(
            string clusterId,
            string subject,
            int version,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveSchemaAsync(
            _telemetry,
            () => _inner.GetSchemaBySubjectVersionAsync(
                clusterId,
                subject,
                version,
                operation,
                cancellationToken));
}

public sealed class TelemetrySchemaCatalogReadPort
    : ISchemaCatalogReadPort
{
    private readonly ISchemaCatalogReadPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetrySchemaCatalogReadPort(
        ISchemaCatalogReadPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>>
        ListSubjectsAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.SchemaRegistryRead,
            () => _inner.ListSubjectsAsync(
                clusterId,
                operation,
                cancellationToken));

    public Task<ReadViewResult<IReadOnlyList<SchemaVersionSummary>>>
        ListVersionsAsync(
            string clusterId,
            string subject,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.SchemaRegistryRead,
            () => _inner.ListVersionsAsync(
                clusterId,
                subject,
                operation,
                cancellationToken));

    public Task<ReadViewResult<SchemaVersionDetail>>
        GetVersionAsync(
            string clusterId,
            string subject,
            int version,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.SchemaRegistryRead,
            () => _inner.GetVersionAsync(
                clusterId,
                subject,
                version,
                operation,
                cancellationToken));

    public Task<ReadViewResult<SchemaCompatibilityObservation>>
        GetCompatibilityAsync(
            string clusterId,
            string subject,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.SchemaRegistryRead,
            () => _inner.GetCompatibilityAsync(
                clusterId,
                subject,
                operation,
                cancellationToken));

    public Task<ReadViewResult<SchemaGlobalCompatibilityObservation>>
        GetGlobalCompatibilityAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.SchemaRegistryRead,
            () => _inner.GetGlobalCompatibilityAsync(
                clusterId,
                operation,
                cancellationToken));
}

public sealed class TelemetryConnectReadPort
    : IConnectReadPort
{
    private readonly IConnectReadPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetryConnectReadPort(
        IConnectReadPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<ReadViewResult<ConnectClusterInfo>>
        GetClusterInfoAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.GetClusterInfoAsync(
            clusterId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<IReadOnlyList<ConnectConnectorSummary>>>
        ListConnectorsAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.ListConnectorsAsync(
            clusterId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<ConnectConnectorDetail>>
        GetConnectorAsync(
            string clusterId,
            string connectorName,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.GetConnectorAsync(
            clusterId,
            connectorName,
            operation,
            cancellationToken));

    public Task<ReadViewResult<IReadOnlyList<ConnectProfileSummary>>>
        ListProfilesAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.ListProfilesAsync(
            clusterId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<ConnectClusterInfo>>
        GetClusterInfoAsync(
            string clusterId,
            string connectProfileId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.GetClusterInfoAsync(
            clusterId,
            connectProfileId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<IReadOnlyList<ConnectConnectorSummary>>>
        ListConnectorsAsync(
            string clusterId,
            string connectProfileId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.ListConnectorsAsync(
            clusterId,
            connectProfileId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<ConnectConnectorDetail>>
        GetConnectorAsync(
            string clusterId,
            string connectProfileId,
            string connectorName,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.GetConnectorAsync(
            clusterId,
            connectProfileId,
            connectorName,
            operation,
            cancellationToken));

    public Task<ReadViewResult<IReadOnlyList<ConnectPluginSummary>>>
        ListPluginsAsync(
            string clusterId,
            string connectProfileId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.ListPluginsAsync(
            clusterId,
            connectProfileId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<ConnectPluginValidationResult>>
        ValidateConfigurationAsync(
            string clusterId,
            string connectProfileId,
            string connectorClass,
            IReadOnlyDictionary<string, string> configuration,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.ValidateConfigurationAsync(
            clusterId,
            connectProfileId,
            connectorClass,
            configuration,
            operation,
            cancellationToken));

    private Task<ReadViewResult<T>> Observe<T>(
        Func<Task<ReadViewResult<T>>> action) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.KafkaConnectRead,
            action);
}

public sealed class TelemetryKsqlMetadataReadPort
    : IKsqlMetadataReadPort
{
    private readonly IKsqlMetadataReadPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetryKsqlMetadataReadPort(
        IKsqlMetadataReadPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<ReadViewResult<KsqlServerInfo>>
        GetServerInfoAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.GetServerInfoAsync(
            clusterId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<IReadOnlyList<KsqlMetadataItem>>>
        ListMetadataAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.ListMetadataAsync(
            clusterId,
            operation,
            cancellationToken));

    private Task<ReadViewResult<T>> Observe<T>(
        Func<Task<ReadViewResult<T>>> action) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.KsqlMetadataRead,
            action);
}

public sealed class TelemetryKsqlQueryPort
    : IKsqlQueryPort
{
    private readonly IKsqlQueryPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetryKsqlQueryPort(
        IKsqlQueryPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<ReadViewResult<KsqlQueryResult>>
        ExecuteQueryAsync(
            string clusterId,
            string statement,
            KsqlQueryLimits limits,
            CancellationToken cancellationToken) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.KsqlQuery,
            () => _inner.ExecuteQueryAsync(
                clusterId,
                statement,
                limits,
                cancellationToken));
}

public sealed class TelemetryStreamsTelemetryReadPort
    : IStreamsTelemetryReadPort
{
    private readonly IStreamsTelemetryReadPort _inner;
    private readonly IKafdeckOperationalTelemetry _telemetry;

    public TelemetryStreamsTelemetryReadPort(
        IStreamsTelemetryReadPort inner,
        IKafdeckOperationalTelemetry telemetry)
    {
        _inner = inner ??
            throw new ArgumentNullException(nameof(inner));
        _telemetry = telemetry ??
            throw new ArgumentNullException(nameof(telemetry));
    }

    public Task<ReadViewResult<IReadOnlyList<StreamsApplicationSummary>>>
        ListApplicationsAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.ListApplicationsAsync(
            clusterId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<StreamsTopologyObservation>>
        GetTopologyAsync(
            string clusterId,
            string applicationId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.GetTopologyAsync(
            clusterId,
            applicationId,
            operation,
            cancellationToken));

    public Task<ReadViewResult<StreamsStateStoreObservation>>
        GetStateStoresAsync(
            string clusterId,
            string applicationId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
        Observe(() => _inner.GetStateStoresAsync(
            clusterId,
            applicationId,
            operation,
            cancellationToken));

    private Task<ReadViewResult<T>> Observe<T>(
        Func<Task<ReadViewResult<T>>> action) =>
        OperationalTelemetryObservation.ObserveReadViewAsync(
            _telemetry,
            KafdeckOperationalFamily.StreamsTelemetryRead,
            action);
}
