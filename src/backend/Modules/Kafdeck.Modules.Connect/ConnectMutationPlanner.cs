using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

public sealed class ConnectMutationPlanner
{
    private readonly IConnectMutationObservationPort _observations;
    private readonly IMutationMaterialDigestService _digest;
    private readonly ConnectMutationPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public ConnectMutationPlanner(
        IConnectMutationObservationPort observations,
        IMutationMaterialDigestService digest,
        ConnectMutationPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _digest = digest ?? throw new ArgumentNullException(nameof(digest));
        _policy = policy ?? ConnectMutationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<ConnectMutationPlanningResult<ConnectCreateCanonicalIntent>>
        PlanCreateAsync(
            ConnectCreateRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string connectorName;
        IReadOnlyDictionary<string, string> configuration;
        try
        {
            clusterId = ConnectMutationCanonicalization.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);
            connectorName =
                ConnectMutationCanonicalization.RequireConnectorName(
                    request.ConnectorName);
            configuration =
                ConnectMutationCanonicalization.NormalizeConfiguration(
                    request.Configuration,
                    _policy);
        }
        catch (ArgumentException exception)
        {
            return Failed<ConnectCreateCanonicalIntent>(
                ConnectMutationPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var capabilities = await GetCapabilitiesAsync(
                clusterId,
                cancellationToken)
            .ConfigureAwait(false);
        if (!capabilities.IsSuccess)
            return Failed<ConnectCreateCanonicalIntent>(capabilities.Failure!);
        if (!capabilities.Value!.SupportsCreate)
        {
            return Failed<ConnectCreateCanonicalIntent>(
                ConnectMutationPlanningFailureCode.ProviderUnsupported,
                "Configured Kafka Connect provider does not admit connector creation.");
        }

        var observed = await ObserveAsync(
                clusterId,
                connectorName,
                cancellationToken)
            .ConfigureAwait(false);
        if (!observed.IsSuccess)
            return Failed<ConnectCreateCanonicalIntent>(observed.Failure!);
        if (observed.Value!.Exists)
        {
            return Failed<ConnectCreateCanonicalIntent>(
                ConnectMutationPlanningFailureCode.ConnectorAlreadyExists,
                "Kafka Connect connector already exists.");
        }

        return BuildCreate(
            clusterId,
            connectorName,
            configuration,
            observed.Value);
    }

    public async Task<ConnectMutationPlanningResult<ConnectUpdateCanonicalIntent>>
        PlanUpdateAsync(
            ConnectUpdateRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string connectorName;
        IReadOnlyDictionary<string, string> configuration;
        try
        {
            clusterId = ConnectMutationCanonicalization.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);
            connectorName =
                ConnectMutationCanonicalization.RequireConnectorName(
                    request.ConnectorName);
            configuration =
                ConnectMutationCanonicalization.NormalizeConfiguration(
                    request.Configuration,
                    _policy);
        }
        catch (ArgumentException exception)
        {
            return Failed<ConnectUpdateCanonicalIntent>(
                ConnectMutationPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var capabilities = await GetCapabilitiesAsync(
                clusterId,
                cancellationToken)
            .ConfigureAwait(false);
        if (!capabilities.IsSuccess)
            return Failed<ConnectUpdateCanonicalIntent>(capabilities.Failure!);
        if (!capabilities.Value!.SupportsUpdate)
        {
            return Failed<ConnectUpdateCanonicalIntent>(
                ConnectMutationPlanningFailureCode.ProviderUnsupported,
                "Configured Kafka Connect provider does not admit connector update.");
        }

        var observed = await ObserveAsync(
                clusterId,
                connectorName,
                cancellationToken)
            .ConfigureAwait(false);
        if (!observed.IsSuccess)
            return Failed<ConnectUpdateCanonicalIntent>(observed.Failure!);
        if (!observed.Value!.Exists)
        {
            return Failed<ConnectUpdateCanonicalIntent>(
                ConnectMutationPlanningFailureCode.ConnectorNotFound,
                "Kafka Connect connector does not exist.");
        }

        var projected =
            ConnectMutationCanonicalization.ProjectConfiguration(
                configuration);
        var requestedFingerprint =
            ConnectMutationCanonicalization.ConfigurationFingerprint(
                projected);
        var diff =
            ConnectMutationCanonicalization.Diff(
                observed.Value.Configuration,
                projected);

        if (diff.Count == 0 &&
            string.Equals(
                observed.Value.ConfigurationFingerprint,
                requestedFingerprint,
                StringComparison.Ordinal))
        {
            return Failed<ConnectUpdateCanonicalIntent>(
                ConnectMutationPlanningFailureCode.NoChange,
                "Requested connector configuration already matches observed state.");
        }

        var materialName = "connect/configuration";
        var materialBytes =
            ConnectMutationCanonicalization.EncodeConfiguration(
                configuration);

        try
        {
            var stateFingerprint =
                ConnectMutationCanonicalization.ObservationFingerprint(
                    observed.Value);
            var canonical =
                new ConnectUpdateCanonicalIntent(
                    ConnectAlterIntentKind.ConfigurationUpdate,
                    clusterId,
                    connectorName,
                    materialName,
                    observed.Value.ConfigurationFingerprint,
                    requestedFingerprint,
                    projected,
                    diff,
                    observed.Value.State,
                    stateFingerprint);

            var intent = BuildIntent(
                MutationOperationKind.ConnectAlter,
                clusterId,
                connectorName,
                ConnectMutationCanonicalization.Serialize(canonical),
                stateFingerprint,
                AuthorizationAction.ConnectAlter,
                materialName,
                materialBytes);

            var risk =
                MutationRiskClassifier.Classify(
                    new MutationRiskInput(
                        MutationOperationKind.ConnectAlter));

            var material =
                new MutationExecutionMaterial(
                    new Dictionary<string, ReadOnlyMemory<byte>>(
                        StringComparer.Ordinal)
                    {
                        [materialName] = materialBytes,
                    });

            return ConnectMutationPlanningResult<ConnectUpdateCanonicalIntent>
                .Success(
                    new ConnectMutationPlan<ConnectUpdateCanonicalIntent>(
                        canonical,
                        intent,
                        risk),
                    material);
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations
                .ZeroMemory(materialBytes);
        }
    }

    public async Task<ConnectMutationPlanningResult<ConnectControlCanonicalIntent>>
        PlanControlAsync(
            ConnectControlRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string connectorName;
        try
        {
            clusterId = ConnectMutationCanonicalization.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);
            connectorName =
                ConnectMutationCanonicalization.RequireConnectorName(
                    request.ConnectorName);

            if (!Enum.IsDefined(request.Action))
                throw new ArgumentOutOfRangeException(nameof(request.Action));

            if (request.Action is
                    ConnectControlAction.Pause or
                    ConnectControlAction.Resume)
            {
                if (request.TaskId is not null)
                    throw new ArgumentException(
                        "Pause/resume cannot target an individual task.");
            }
            else if (request.TaskId is < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(request.TaskId));
            }
        }
        catch (ArgumentException exception)
        {
            return Failed<ConnectControlCanonicalIntent>(
                ConnectMutationPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var capabilities = await GetCapabilitiesAsync(
                clusterId,
                cancellationToken)
            .ConfigureAwait(false);
        if (!capabilities.IsSuccess)
            return Failed<ConnectControlCanonicalIntent>(capabilities.Failure!);

        var supported = request.Action switch
        {
            ConnectControlAction.Pause =>
                capabilities.Value!.SupportsPause,
            ConnectControlAction.Resume =>
                capabilities.Value!.SupportsResume,
            ConnectControlAction.Restart when request.TaskId.HasValue =>
                capabilities.Value!.SupportsTaskRestart,
            ConnectControlAction.Restart =>
                capabilities.Value!.SupportsRestart,
            _ => false,
        };

        if (!supported)
        {
            return Failed<ConnectControlCanonicalIntent>(
                ConnectMutationPlanningFailureCode.ProviderUnsupported,
                "Configured Kafka Connect provider does not admit the requested lifecycle operation.");
        }

        var observed = await ObserveAsync(
                clusterId,
                connectorName,
                cancellationToken)
            .ConfigureAwait(false);
        if (!observed.IsSuccess)
            return Failed<ConnectControlCanonicalIntent>(observed.Failure!);
        if (!observed.Value!.Exists)
        {
            return Failed<ConnectControlCanonicalIntent>(
                ConnectMutationPlanningFailureCode.ConnectorNotFound,
                "Kafka Connect connector does not exist.");
        }

        string? taskState = null;
        if (request.TaskId.HasValue)
        {
            var task = observed.Value.Tasks
                .SingleOrDefault(item => item.Id == request.TaskId.Value);
            if (task is null)
            {
                return Failed<ConnectControlCanonicalIntent>(
                    ConnectMutationPlanningFailureCode.TaskNotFound,
                    "Kafka Connect task does not exist.");
            }

            taskState = task.State;
        }

        if (request.Action == ConnectControlAction.Pause &&
            string.Equals(
                observed.Value.State,
                "PAUSED",
                StringComparison.Ordinal))
        {
            return Failed<ConnectControlCanonicalIntent>(
                ConnectMutationPlanningFailureCode.NoChange,
                "Kafka Connect connector is already paused.");
        }

        if (request.Action == ConnectControlAction.Resume &&
            string.Equals(
                observed.Value.State,
                "RUNNING",
                StringComparison.Ordinal))
        {
            return Failed<ConnectControlCanonicalIntent>(
                ConnectMutationPlanningFailureCode.NoChange,
                "Kafka Connect connector is already running.");
        }

        var stateFingerprint =
            ConnectMutationCanonicalization.ObservationFingerprint(
                observed.Value);
        var canonical =
            new ConnectControlCanonicalIntent(
                ConnectAlterIntentKind.Control,
                clusterId,
                connectorName,
                request.Action,
                request.TaskId,
                observed.Value.State,
                taskState,
                stateFingerprint);

        var canonicalJson =
            ConnectMutationCanonicalization.Serialize(canonical);
        var intent = BuildIntent(
            MutationOperationKind.ConnectAlter,
            clusterId,
            connectorName,
            canonicalJson,
            stateFingerprint,
            AuthorizationAction.ConnectAlter);

        var risk =
            MutationRiskClassifier.Classify(
                new MutationRiskInput(
                    MutationOperationKind.ConnectAlter));

        return ConnectMutationPlanningResult<ConnectControlCanonicalIntent>
            .Success(
                new ConnectMutationPlan<ConnectControlCanonicalIntent>(
                    canonical,
                    intent,
                    risk));
    }

    public async Task<ConnectMutationPlanningResult<ConnectDeleteCanonicalIntent>>
        PlanDeleteAsync(
            ConnectDeleteRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string connectorName;
        try
        {
            clusterId = ConnectMutationCanonicalization.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);
            connectorName =
                ConnectMutationCanonicalization.RequireConnectorName(
                    request.ConnectorName);
        }
        catch (ArgumentException exception)
        {
            return Failed<ConnectDeleteCanonicalIntent>(
                ConnectMutationPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var capabilities = await GetCapabilitiesAsync(
                clusterId,
                cancellationToken)
            .ConfigureAwait(false);
        if (!capabilities.IsSuccess)
            return Failed<ConnectDeleteCanonicalIntent>(capabilities.Failure!);
        if (!capabilities.Value!.SupportsDelete)
        {
            return Failed<ConnectDeleteCanonicalIntent>(
                ConnectMutationPlanningFailureCode.ProviderUnsupported,
                "Configured Kafka Connect provider does not admit connector deletion.");
        }

        var observed = await ObserveAsync(
                clusterId,
                connectorName,
                cancellationToken)
            .ConfigureAwait(false);
        if (!observed.IsSuccess)
            return Failed<ConnectDeleteCanonicalIntent>(observed.Failure!);
        if (!observed.Value!.Exists)
        {
            return Failed<ConnectDeleteCanonicalIntent>(
                ConnectMutationPlanningFailureCode.ConnectorNotFound,
                "Kafka Connect connector does not exist.");
        }

        var stateFingerprint =
            ConnectMutationCanonicalization.ObservationFingerprint(
                observed.Value);
        var canonical =
            new ConnectDeleteCanonicalIntent(
                clusterId,
                connectorName,
                observed.Value.State,
                observed.Value.ConfigurationFingerprint,
                stateFingerprint);

        var intent = BuildIntent(
            MutationOperationKind.ConnectDelete,
            clusterId,
            connectorName,
            ConnectMutationCanonicalization.Serialize(canonical),
            stateFingerprint,
            AuthorizationAction.ConnectDelete);

        var risk =
            MutationRiskClassifier.Classify(
                new MutationRiskInput(
                    MutationOperationKind.ConnectDelete));

        return ConnectMutationPlanningResult<ConnectDeleteCanonicalIntent>
            .Success(
                new ConnectMutationPlan<ConnectDeleteCanonicalIntent>(
                    canonical,
                    intent,
                    risk));
    }

    private ConnectMutationPlanningResult<ConnectCreateCanonicalIntent>
        BuildCreate(
            string clusterId,
            string connectorName,
            IReadOnlyDictionary<string, string> configuration,
            ConnectMutationObservation observed)
    {
        var projected =
            ConnectMutationCanonicalization.ProjectConfiguration(
                configuration);
        var requestedFingerprint =
            ConnectMutationCanonicalization.ConfigurationFingerprint(
                projected);
        var stateFingerprint =
            ConnectMutationCanonicalization.ObservationFingerprint(
                observed);
        var materialName = "connect/configuration";
        var materialBytes =
            ConnectMutationCanonicalization.EncodeConfiguration(
                configuration);

        try
        {
            var canonical =
                new ConnectCreateCanonicalIntent(
                    clusterId,
                    connectorName,
                    materialName,
                    requestedFingerprint,
                    projected,
                    stateFingerprint);

            var intent = BuildIntent(
                MutationOperationKind.ConnectCreate,
                clusterId,
                connectorName,
                ConnectMutationCanonicalization.Serialize(canonical),
                stateFingerprint,
                AuthorizationAction.ConnectCreate,
                materialName,
                materialBytes);

            var risk =
                MutationRiskClassifier.Classify(
                    new MutationRiskInput(
                        MutationOperationKind.ConnectCreate));

            var material =
                new MutationExecutionMaterial(
                    new Dictionary<string, ReadOnlyMemory<byte>>(
                        StringComparer.Ordinal)
                    {
                        [materialName] = materialBytes,
                    });

            return ConnectMutationPlanningResult<ConnectCreateCanonicalIntent>
                .Success(
                    new ConnectMutationPlan<ConnectCreateCanonicalIntent>(
                        canonical,
                        intent,
                        risk),
                    material);
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations
                .ZeroMemory(materialBytes);
        }
    }

    private MutationIntentDescriptor BuildIntent(
        MutationOperationKind kind,
        string clusterId,
        string connectorName,
        string canonicalIntent,
        string stateFingerprint,
        AuthorizationAction action,
        string? materialName = null,
        ReadOnlySpan<byte> material = default)
    {
        IReadOnlyList<MutationMaterialDigest>? materialDigests = null;
        if (materialName is not null)
        {
            materialDigests =
            [
                new MutationMaterialDigest(
                    materialName,
                    _digest.ComputeDigest(material)),
            ];
        }

        return new MutationIntentDescriptor(
            kind,
            clusterId,
            canonicalIntent,
            new[]
            {
                ConnectMutationCanonicalization.ResourceKey(
                    clusterId,
                    connectorName),
            },
            new[]
            {
                new MutationPrecondition(
                    "connect.connector",
                    stateFingerprint),
            },
            materialDigests,
            new[]
            {
                new MutationAuthorizationTarget(
                    action,
                    clusterId,
                    ConnectMutationCanonicalization.AuthorizationResource(
                        connectorName)),
            });
    }

    private async Task<ConnectObservationResult<ConnectMutationCapabilities>>
        GetCapabilitiesAsync(
            string clusterId,
            CancellationToken cancellationToken)
    {
        var result = await _observations.GetCapabilitiesAsync(
                clusterId,
                Observation(),
                cancellationToken)
            .ConfigureAwait(false);

        return result.IsSuccess && result.Value is not null
            ? ConnectObservationResult<ConnectMutationCapabilities>.Success(
                result.Value)
            : ConnectObservationResult<ConnectMutationCapabilities>.Failed(
                MapObservationFailure(result.Failure));
    }

    internal async Task<ConnectObservationResult<ConnectMutationObservation>>
        ObserveAsync(
            string clusterId,
            string connectorName,
            CancellationToken cancellationToken)
    {
        var result = await _observations.ObserveConnectorAsync(
                clusterId,
                connectorName,
                Observation(),
                cancellationToken)
            .ConfigureAwait(false);

        if (!result.IsSuccess || result.Value is null)
        {
            return ConnectObservationResult<ConnectMutationObservation>.Failed(
                MapObservationFailure(result.Failure));
        }

        if (result.Value.Tasks.Count > _policy.MaxTasks ||
            !string.Equals(
                result.Value.ConnectorName,
                connectorName,
                StringComparison.Ordinal))
        {
            return ConnectObservationResult<ConnectMutationObservation>.Failed(
                new ConnectMutationPlanningFailure(
                    ConnectMutationPlanningFailureCode.ObservationFailed,
                    "Kafka Connect returned an invalid connector observation."));
        }

        return ConnectObservationResult<ConnectMutationObservation>.Success(
            result.Value);
    }

    private ReadViewOperationContext Observation() =>
        new(
            _timeProvider.GetUtcNow().Add(
                _policy.ObservationTimeout),
            maxItems:
                Math.Max(
                    _policy.MaxConfigurationItems,
                    _policy.MaxTasks),
            maxResponseBytes:
                ConnectMutationPolicy.HardMaxConfigurationBytes * 2L);

    private static ConnectMutationPlanningFailure MapObservationFailure(
        ConnectMutationObservationFailure? failure)
    {
        if (failure is null)
        {
            return new(
                ConnectMutationPlanningFailureCode.ObservationFailed,
                "Kafka Connect state could not be safely observed.");
        }

        return failure.Category switch
        {
            ConnectMutationObservationFailureCategory.NotConfigured =>
                new(
                    ConnectMutationPlanningFailureCode.ProviderNotConfigured,
                    "Kafka Connect is not configured for the requested cluster."),
            ConnectMutationObservationFailureCategory.Unauthorized =>
                new(
                    ConnectMutationPlanningFailureCode.ProviderUnauthorized,
                    "Kafka Connect denied the required observation."),
            ConnectMutationObservationFailureCategory.Unsupported =>
                new(
                    ConnectMutationPlanningFailureCode.ProviderUnsupported,
                    "Kafka Connect does not support the required capability."),
            ConnectMutationObservationFailureCategory.Unavailable or
            ConnectMutationObservationFailureCategory.Timeout or
            ConnectMutationObservationFailureCategory.Cancelled =>
                new(
                    ConnectMutationPlanningFailureCode.ProviderUnavailable,
                    "Kafka Connect state is currently unavailable."),
            _ =>
                new(
                    ConnectMutationPlanningFailureCode.ObservationFailed,
                    "Kafka Connect state could not be safely observed."),
        };
    }

    private static ConnectMutationPlanningResult<T> Failed<T>(
        ConnectMutationPlanningFailureCode code,
        string message)
        where T : class =>
        ConnectMutationPlanningResult<T>.Failed(
            new ConnectMutationPlanningFailure(
                code,
                message));

    private static ConnectMutationPlanningResult<T> Failed<T>(
        ConnectMutationPlanningFailure failure)
        where T : class =>
        ConnectMutationPlanningResult<T>.Failed(failure);

    internal sealed record ConnectObservationResult<T>
    {
        private ConnectObservationResult(
            T? value,
            ConnectMutationPlanningFailure? failure)
        {
            Value = value;
            Failure = failure;
        }

        public T? Value { get; }
        public ConnectMutationPlanningFailure? Failure { get; }
        public bool IsSuccess =>
            Value is not null && Failure is null;

        public static ConnectObservationResult<T> Success(
            T value) =>
            new(value, null);

        public static ConnectObservationResult<T> Failed(
            ConnectMutationPlanningFailure failure) =>
            new(default, failure);
    }
}
