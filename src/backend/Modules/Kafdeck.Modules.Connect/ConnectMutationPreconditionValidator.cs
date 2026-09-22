using System.Text.Json;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

public sealed class ConnectMutationPreconditionValidator
{
    private readonly ConnectMutationPlanner _planner;

    public ConnectMutationPreconditionValidator(
        ConnectMutationPlanner planner)
    {
        _planner = planner ?? throw new ArgumentNullException(nameof(planner));
    }

    public Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        return operation.OperationKind switch
        {
            MutationOperationKind.ConnectCreate =>
                ValidateCreateAsync(
                    operation,
                    cancellationToken),

            MutationOperationKind.ConnectAlter =>
                ValidateAlterAsync(
                    operation,
                    cancellationToken),

            MutationOperationKind.ConnectDelete =>
                ValidateDeleteAsync(
                    operation,
                    cancellationToken),

            _ => Task.FromResult(
                new MutationPreDispatchGuardResult(
                    MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                    "connect_precondition_operation_not_supported")),
        };
    }

    private async Task<MutationPreDispatchGuardResult> ValidateCreateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        ConnectCreateCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectCreateCanonicalIntent>(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale(
                "connect_create_precondition_intent_invalid");
        }

        if (!ValidateCreateShape(canonical) ||
            !BindingsMatch(
                operation,
                MutationOperationKind.ConnectCreate,
                canonical.ClusterId,
                canonical.ConnectorName,
                AuthorizationAction.ConnectCreate) ||
            !MaterialBindingMatches(
                operation,
                canonical.MaterialName))
        {
            return Stale(
                "connect_create_precondition_binding_changed");
        }

        var observed = await _planner.ObserveAsync(
                canonical.ClusterId,
                canonical.ConnectorName,
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess ||
            observed.Value is null)
        {
            return Unsupported(
                "connect_create_precondition_observation_unavailable");
        }

        var fingerprint =
            ConnectMutationCanonicalization.ObservationFingerprint(
                observed.Value);

        if (observed.Value.Exists ||
            !string.Equals(
                fingerprint,
                canonical.StateFingerprint,
                StringComparison.Ordinal) ||
            !PreconditionMatches(
                operation,
                fingerprint))
        {
            return Stale(
                "connect_create_precondition_changed");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private async Task<MutationPreDispatchGuardResult> ValidateAlterAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        if (!TryReadAlterKind(
                operation.CanonicalIntent,
                out var kind))
        {
            return Stale(
                "connect_alter_precondition_intent_invalid");
        }

        return kind switch
        {
            ConnectAlterIntentKind.ConfigurationUpdate =>
                await ValidateUpdateAsync(
                        operation,
                        cancellationToken)
                    .ConfigureAwait(false),

            ConnectAlterIntentKind.Control =>
                await ValidateControlAsync(
                        operation,
                        cancellationToken)
                    .ConfigureAwait(false),

            _ => Stale(
                "connect_alter_precondition_intent_invalid"),
        };
    }

    private async Task<MutationPreDispatchGuardResult> ValidateUpdateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        ConnectUpdateCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectUpdateCanonicalIntent>(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale(
                "connect_update_precondition_intent_invalid");
        }

        if (canonical.AlterKind !=
                ConnectAlterIntentKind.ConfigurationUpdate ||
            !ValidateUpdateShape(canonical) ||
            !BindingsMatch(
                operation,
                MutationOperationKind.ConnectAlter,
                canonical.ClusterId,
                canonical.ConnectorName,
                AuthorizationAction.ConnectAlter) ||
            !MaterialBindingMatches(
                operation,
                canonical.MaterialName))
        {
            return Stale(
                "connect_update_precondition_binding_changed");
        }

        var observed = await _planner.ObserveAsync(
                canonical.ClusterId,
                canonical.ConnectorName,
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess ||
            observed.Value is null)
        {
            return Unsupported(
                "connect_update_precondition_observation_unavailable");
        }

        var fingerprint =
            ConnectMutationCanonicalization.ObservationFingerprint(
                observed.Value);

        if (!observed.Value.Exists ||
            !string.Equals(
                observed.Value.ConfigurationFingerprint,
                canonical.CurrentConfigurationFingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                observed.Value.State,
                canonical.CurrentState,
                StringComparison.Ordinal) ||
            !string.Equals(
                fingerprint,
                canonical.StateFingerprint,
                StringComparison.Ordinal) ||
            !PreconditionMatches(
                operation,
                fingerprint))
        {
            return Stale(
                "connect_update_precondition_changed");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private async Task<MutationPreDispatchGuardResult> ValidateControlAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        ConnectControlCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectControlCanonicalIntent>(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale(
                "connect_control_precondition_intent_invalid");
        }

        if (canonical.AlterKind != ConnectAlterIntentKind.Control ||
            !ValidateControlShape(canonical) ||
            !BindingsMatch(
                operation,
                MutationOperationKind.ConnectAlter,
                canonical.ClusterId,
                canonical.ConnectorName,
                AuthorizationAction.ConnectAlter))
        {
            return Stale(
                "connect_control_precondition_binding_changed");
        }

        var observed = await _planner.ObserveAsync(
                canonical.ClusterId,
                canonical.ConnectorName,
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess ||
            observed.Value is null)
        {
            return Unsupported(
                "connect_control_precondition_observation_unavailable");
        }

        var fingerprint =
            ConnectMutationCanonicalization.ObservationFingerprint(
                observed.Value);

        if (!observed.Value.Exists ||
            !string.Equals(
                observed.Value.State,
                canonical.CurrentState,
                StringComparison.Ordinal) ||
            !string.Equals(
                fingerprint,
                canonical.StateFingerprint,
                StringComparison.Ordinal) ||
            !PreconditionMatches(
                operation,
                fingerprint))
        {
            return Stale(
                "connect_control_precondition_changed");
        }

        if (canonical.TaskId.HasValue)
        {
            var task = observed.Value.Tasks
                .SingleOrDefault(item =>
                    item.Id == canonical.TaskId.Value);
            if (task is null ||
                !string.Equals(
                    task.State,
                    canonical.CurrentTaskState,
                    StringComparison.Ordinal))
            {
                return Stale(
                    "connect_control_precondition_task_changed");
            }
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private async Task<MutationPreDispatchGuardResult> ValidateDeleteAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        ConnectDeleteCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectDeleteCanonicalIntent>(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale(
                "connect_delete_precondition_intent_invalid");
        }

        if (!ValidateDeleteShape(canonical) ||
            !BindingsMatch(
                operation,
                MutationOperationKind.ConnectDelete,
                canonical.ClusterId,
                canonical.ConnectorName,
                AuthorizationAction.ConnectDelete))
        {
            return Stale(
                "connect_delete_precondition_binding_changed");
        }

        var observed = await _planner.ObserveAsync(
                canonical.ClusterId,
                canonical.ConnectorName,
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess ||
            observed.Value is null)
        {
            return Unsupported(
                "connect_delete_precondition_observation_unavailable");
        }

        var fingerprint =
            ConnectMutationCanonicalization.ObservationFingerprint(
                observed.Value);

        if (!observed.Value.Exists ||
            !string.Equals(
                observed.Value.State,
                canonical.CurrentState,
                StringComparison.Ordinal) ||
            !string.Equals(
                observed.Value.ConfigurationFingerprint,
                canonical.CurrentConfigurationFingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                fingerprint,
                canonical.StateFingerprint,
                StringComparison.Ordinal) ||
            !PreconditionMatches(
                operation,
                fingerprint))
        {
            return Stale(
                "connect_delete_precondition_changed");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private static bool ValidateCreateShape(
        ConnectCreateCanonicalIntent canonical) =>
        ValidateCanonicalIdentity(
            canonical.ClusterId,
            canonical.ConnectorName,
            canonical.StateFingerprint) &&
        ValidateMaterialCanonical(
            canonical.MaterialName,
            canonical.RequestedConfigurationFingerprint,
            canonical.RequestedConfiguration);

    private static bool ValidateUpdateShape(
        ConnectUpdateCanonicalIntent canonical) =>
        ValidateCanonicalIdentity(
            canonical.ClusterId,
            canonical.ConnectorName,
            canonical.StateFingerprint) &&
        ValidateFingerprint(
            canonical.CurrentConfigurationFingerprint) &&
        ValidateMaterialCanonical(
            canonical.MaterialName,
            canonical.RequestedConfigurationFingerprint,
            canonical.RequestedConfiguration) &&
        canonical.Diff.Count > 0 &&
        canonical.Diff.All(item =>
            Enum.IsDefined(item.ChangeKind));

    private static bool ValidateControlShape(
        ConnectControlCanonicalIntent canonical)
    {
        if (!ValidateCanonicalIdentity(
                canonical.ClusterId,
                canonical.ConnectorName,
                canonical.StateFingerprint) ||
            !Enum.IsDefined(canonical.Action))
        {
            return false;
        }

        return canonical.Action switch
        {
            ConnectControlAction.Pause or
            ConnectControlAction.Resume =>
                canonical.TaskId is null &&
                canonical.CurrentTaskState is null,

            ConnectControlAction.Restart
                when canonical.TaskId.HasValue =>
                canonical.TaskId.Value >= 0 &&
                !string.IsNullOrWhiteSpace(
                    canonical.CurrentTaskState),

            ConnectControlAction.Restart =>
                canonical.TaskId is null &&
                canonical.CurrentTaskState is null,

            _ => false,
        };
    }

    private static bool ValidateDeleteShape(
        ConnectDeleteCanonicalIntent canonical) =>
        ValidateCanonicalIdentity(
            canonical.ClusterId,
            canonical.ConnectorName,
            canonical.StateFingerprint) &&
        ValidateFingerprint(
            canonical.CurrentConfigurationFingerprint);

    private static bool ValidateMaterialCanonical(
        string materialName,
        string requestedFingerprint,
        IReadOnlyList<ConnectConfigurationCanonicalItem> items)
    {
        try
        {
            _ = ConnectMutationCanonicalization.RequireIdentifier(
                materialName,
                "Connect material name",
                256);
        }
        catch (ArgumentException)
        {
            return false;
        }

        if (!ValidateFingerprint(requestedFingerprint) ||
            items is null ||
            items.Count is < 1 or >
                ConnectMutationPolicy.HardMaxConfigurationItems ||
            items.Any(item =>
                item is null ||
                !ValidateFingerprint(item.ValueSha256)))
        {
            return false;
        }

        var keys = new HashSet<string>(
            StringComparer.Ordinal);
        foreach (var item in items)
        {
            try
            {
                var key =
                    ConnectMutationCanonicalization.RequireIdentifier(
                        item.Key,
                        "Connector configuration key",
                        ConnectMutationPolicy
                            .HardMaxConfigurationKeyCharacters);
                if (!keys.Add(key))
                    return false;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        return string.Equals(
            ConnectMutationCanonicalization.ConfigurationFingerprint(
                items),
            requestedFingerprint,
            StringComparison.Ordinal);
    }

    private static bool ValidateCanonicalIdentity(
        string clusterId,
        string connectorName,
        string stateFingerprint)
    {
        try
        {
            _ = ConnectMutationCanonicalization.RequireIdentifier(
                clusterId,
                "Cluster ID",
                256);
            _ = ConnectMutationCanonicalization.RequireConnectorName(
                connectorName);
            return ValidateFingerprint(stateFingerprint);
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool ValidateFingerprint(string value) =>
        value.Length == 64 &&
        value.All(char.IsAsciiHexDigit);

    private static bool BindingsMatch(
        MutationOperationSnapshot operation,
        MutationOperationKind kind,
        string clusterId,
        string connectorName,
        AuthorizationAction action) =>
        operation.OperationKind == kind &&
        string.Equals(
            operation.ClusterId,
            clusterId,
            StringComparison.Ordinal) &&
        operation.ResourceKeys.Count == 1 &&
        string.Equals(
            operation.ResourceKeys[0],
            ConnectMutationCanonicalization.ResourceKey(
                clusterId,
                connectorName),
            StringComparison.Ordinal) &&
        operation.AuthorizationTargets.Count == 1 &&
        operation.AuthorizationTargets[0].Action == action &&
        string.Equals(
            operation.AuthorizationTargets[0].ClusterId,
            clusterId,
            StringComparison.Ordinal) &&
        string.Equals(
            operation.AuthorizationTargets[0].ResourceName,
            ConnectMutationCanonicalization.AuthorizationResource(
                connectorName),
            StringComparison.Ordinal);

    private static bool MaterialBindingMatches(
        MutationOperationSnapshot operation,
        string materialName) =>
        operation.MaterialDigests.Count == 1 &&
        string.Equals(
            operation.MaterialDigests[0].Name,
            materialName,
            StringComparison.Ordinal);

    private static bool PreconditionMatches(
        MutationOperationSnapshot operation,
        string fingerprint) =>
        operation.Preconditions.Count == 1 &&
        string.Equals(
            operation.Preconditions[0].Key,
            "connect.connector",
            StringComparison.Ordinal) &&
        string.Equals(
            operation.Preconditions[0].Fingerprint,
            fingerprint,
            StringComparison.Ordinal);

    private static bool TryReadAlterKind(
        string canonicalIntent,
        out ConnectAlterIntentKind kind)
    {
        kind = default;
        try
        {
            using var document =
                JsonDocument.Parse(canonicalIntent);
            if (document.RootElement.ValueKind !=
                    JsonValueKind.Object ||
                !document.RootElement.TryGetProperty(
                    "alterKind",
                    out var element) ||
                !element.TryGetInt32(out var raw) ||
                !Enum.IsDefined(
                    (ConnectAlterIntentKind)raw))
            {
                return false;
            }

            kind = (ConnectAlterIntentKind)raw;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static MutationPreDispatchGuardResult Stale(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.StalePreview,
            code);

    private static MutationPreDispatchGuardResult Unsupported(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.CapabilityUnsupported,
            code);
}
