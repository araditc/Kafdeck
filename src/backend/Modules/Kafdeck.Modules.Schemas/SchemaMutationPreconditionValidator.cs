using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Core.Schemas;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Schemas;

public sealed class SchemaMutationPreconditionValidator
{
    private readonly SchemaMutationPlanner _planner;
    private readonly ISchemaCatalogReadPort _catalog;
    private readonly ISchemaMutationObservationPort _mutationObservations;
    private readonly SchemaMutationPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public SchemaMutationPreconditionValidator(
        SchemaMutationPlanner planner,
        ISchemaCatalogReadPort catalog,
        ISchemaMutationObservationPort mutationObservations,
        SchemaMutationPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _planner = planner ?? throw new ArgumentNullException(nameof(planner));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _mutationObservations =
            mutationObservations ?? throw new ArgumentNullException(nameof(mutationObservations));
        _policy = policy ?? SchemaMutationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        return operation.OperationKind switch
        {
            MutationOperationKind.SchemaCreate =>
                ValidateCreateAsync(operation, cancellationToken),
            MutationOperationKind.SchemaAlter =>
                ValidateCompatibilityAsync(operation, cancellationToken),
            MutationOperationKind.SchemaDelete =>
                ValidateDeleteAsync(operation, cancellationToken),
            _ => Task.FromResult(
                new MutationPreDispatchGuardResult(
                    MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                    "schema_precondition_operation_not_supported")),
        };
    }

    private async Task<MutationPreDispatchGuardResult> ValidateCreateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        SchemaCreateCanonicalIntent canonical;
        try
        {
            canonical =
                SchemaMutationCanonicalization.Deserialize<
                    SchemaCreateCanonicalIntent>(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale("schema_create_precondition_intent_invalid");
        }

        if (!ValidateCreateShape(canonical) ||
            !BindingsMatch(
                operation,
                MutationOperationKind.SchemaCreate,
                canonical.ClusterId,
                SchemaMutationCanonicalization.ResourceKey(
                    canonical.ClusterId,
                    canonical.Subject),
                AuthorizationAction.SchemaCreate,
                SchemaMutationCanonicalization.AuthorizationResource(
                    canonical.Subject)) ||
            operation.MaterialDigests.Count != 1 ||
            !string.Equals(
                operation.MaterialDigests[0].Name,
                canonical.MaterialName,
                StringComparison.Ordinal))
        {
            return Stale("schema_create_precondition_binding_changed");
        }

        if (!TryGetUniquePreconditions(
                operation.Preconditions,
                out var preconditions) ||
            preconditions!.Count != canonical.References.Count + 1 ||
            !preconditions.TryGetValue(
                "schema.subject",
                out var expectedSubjectFingerprint))
        {
            return Stale("schema_create_precondition_shape_changed");
        }

        var subjectState = await _planner.ObserveSubjectStateAsync(
                canonical.ClusterId,
                canonical.Subject,
                cancellationToken)
            .ConfigureAwait(false);
        if (!subjectState.IsSuccess ||
            subjectState.Value is null)
        {
            return Unsupported(
                "schema_create_precondition_observation_unavailable");
        }

        if (!string.Equals(
                expectedSubjectFingerprint,
                subjectState.Value.Fingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                canonical.SubjectFingerprint,
                subjectState.Value.Fingerprint,
                StringComparison.Ordinal) ||
            canonical.SubjectExists !=
                subjectState.Value.Exists ||
            canonical.LatestVersion !=
                subjectState.Value.LatestVersion ||
            canonical.CompatibilityMode !=
                subjectState.Value.CompatibilityMode ||
            canonical.CompatibilityInherited !=
                subjectState.Value.CompatibilityInherited)
        {
            return Stale(
                "schema_create_precondition_subject_changed");
        }

        for (var ordinal = 0;
             ordinal < canonical.References.Count;
             ordinal++)
        {
            var reference = canonical.References[ordinal];
            var detail = await _catalog.GetVersionAsync(
                    canonical.ClusterId,
                    reference.Subject,
                    reference.Version,
                    Observation(),
                    cancellationToken)
                .ConfigureAwait(false);

            if (!detail.IsSuccess || detail.Value is null)
            {
                return Stale(
                    "schema_create_precondition_reference_changed");
            }

            var fingerprint =
                SchemaMutationPlanner.FingerprintVersion(
                    detail.Value);
            var key = $"schema.reference/{ordinal:D4}";
            if (!preconditions.TryGetValue(key, out var expected) ||
                !string.Equals(
                    expected,
                    fingerprint,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    reference.TargetFingerprint,
                    fingerprint,
                    StringComparison.Ordinal))
            {
                return Stale(
                    "schema_create_precondition_reference_changed");
            }
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private async Task<MutationPreDispatchGuardResult>
        ValidateCompatibilityAsync(
            MutationOperationSnapshot operation,
            CancellationToken cancellationToken)
    {
        SchemaCompatibilityCanonicalIntent canonical;
        try
        {
            canonical =
                SchemaMutationCanonicalization.Deserialize<
                    SchemaCompatibilityCanonicalIntent>(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale(
                "schema_compatibility_precondition_intent_invalid");
        }

        if (!Enum.IsDefined(canonical.Scope) ||
            canonical.RequestedMode ==
                SchemaCompatibilityMode.Unknown ||
            !Enum.IsDefined(canonical.RequestedMode))
        {
            return Stale(
                "schema_compatibility_precondition_intent_invalid");
        }

        var authResource =
            canonical.Scope == SchemaCompatibilityScope.Subject &&
            canonical.Subject is not null
                ? SchemaMutationCanonicalization.AuthorizationResource(
                    canonical.Subject)
                : canonical.Scope == SchemaCompatibilityScope.Global &&
                  canonical.Subject is null
                    ? SchemaMutationCanonicalization
                        .GlobalCompatibilityResource
                    : null;

        var resourceKey =
            canonical.Scope == SchemaCompatibilityScope.Subject &&
            canonical.Subject is not null
                ? SchemaMutationCanonicalization.ResourceKey(
                    canonical.ClusterId,
                    canonical.Subject)
                : canonical.Scope == SchemaCompatibilityScope.Global &&
                  canonical.Subject is null
                    ? $"cluster/{canonical.ClusterId}/schema-compatibility/global"
                    : null;

        if (authResource is null ||
            resourceKey is null ||
            !BindingsMatch(
                operation,
                MutationOperationKind.SchemaAlter,
                canonical.ClusterId,
                resourceKey,
                AuthorizationAction.SchemaAlter,
                authResource) ||
            !TryGetSinglePrecondition(
                operation.Preconditions,
                "schema.compatibility",
                out var expectedFingerprint))
        {
            return Stale(
                "schema_compatibility_precondition_binding_changed");
        }

        string observedFingerprint;
        SchemaCompatibilityMode observedMode;
        bool observedInherited;

        if (canonical.Scope == SchemaCompatibilityScope.Subject)
        {
            var state = await _planner.ObserveSubjectStateAsync(
                    canonical.ClusterId,
                    canonical.Subject!,
                    cancellationToken)
                .ConfigureAwait(false);

            if (!state.IsSuccess ||
                state.Value is null ||
                !state.Value.Exists)
            {
                return Stale(
                    "schema_compatibility_precondition_subject_changed");
            }

            observedFingerprint =
                state.Value.Fingerprint;
            observedMode =
                state.Value.CompatibilityMode;
            observedInherited =
                state.Value.CompatibilityInherited;
        }
        else
        {
            var global =
                await _catalog.GetGlobalCompatibilityAsync(
                        canonical.ClusterId,
                        Observation(),
                        cancellationToken)
                    .ConfigureAwait(false);

            if (!global.IsSuccess ||
                global.Value is null)
            {
                return Unsupported(
                    "schema_compatibility_precondition_observation_unavailable");
            }

            observedMode = global.Value.Mode;
            observedInherited = false;
            observedFingerprint =
                SchemaMutationPlanner.FingerprintCompatibility(
                    SchemaCompatibilityScope.Global,
                    subject: null,
                    observedMode,
                    inherited: false);
        }

        if (!string.Equals(
                expectedFingerprint,
                observedFingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                canonical.StateFingerprint,
                observedFingerprint,
                StringComparison.Ordinal) ||
            canonical.CurrentMode != observedMode ||
            canonical.CurrentInherited != observedInherited ||
            canonical.RequestedMode == observedMode)
        {
            return Stale(
                "schema_compatibility_precondition_changed");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private async Task<MutationPreDispatchGuardResult> ValidateDeleteAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        SchemaDeleteCanonicalIntent canonical;
        try
        {
            canonical =
                SchemaMutationCanonicalization.Deserialize<
                    SchemaDeleteCanonicalIntent>(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale(
                "schema_delete_precondition_intent_invalid");
        }

        if (canonical.Version is <= 0)
        {
            return Stale(
                "schema_delete_precondition_intent_invalid");
        }

        var authResource =
            canonical.Version.HasValue
                ? $"schema/{canonical.Subject}/version/{canonical.Version.Value}"
                : SchemaMutationCanonicalization.AuthorizationResource(
                    canonical.Subject);
        var resourceKey =
            canonical.Version.HasValue
                ? $"cluster/{canonical.ClusterId}/schema/{canonical.Subject}/version/{canonical.Version.Value}"
                : SchemaMutationCanonicalization.ResourceKey(
                    canonical.ClusterId,
                    canonical.Subject);

        if (!BindingsMatch(
                operation,
                MutationOperationKind.SchemaDelete,
                canonical.ClusterId,
                resourceKey,
                AuthorizationAction.SchemaDelete,
                authResource) ||
            !TryGetSinglePrecondition(
                operation.Preconditions,
                "schema.delete-target",
                out var expectedFingerprint))
        {
            return Stale(
                "schema_delete_precondition_binding_changed");
        }

        var observed =
            await _mutationObservations.ObserveDeleteTargetAsync(
                    canonical.ClusterId,
                    new SchemaDeleteObservationRequest(
                        canonical.Subject,
                        canonical.Version),
                    Observation(),
                    cancellationToken)
                .ConfigureAwait(false);

        if (!observed.IsSuccess ||
            observed.Value is null)
        {
            return Unsupported(
                "schema_delete_precondition_observation_unavailable");
        }

        var fingerprint =
            SchemaMutationPlanner.FingerprintDeleteTarget(
                canonical.Subject,
                canonical.Version,
                observed.Value);

        if (!string.Equals(
                expectedFingerprint,
                fingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                canonical.TargetFingerprint,
                fingerprint,
                StringComparison.Ordinal) ||
            canonical.ExistsActive !=
                observed.Value.ExistsActive ||
            canonical.ExistsIncludingDeleted !=
                observed.Value.ExistsIncludingDeleted ||
            canonical.IsSoftDeleted !=
                observed.Value.IsSoftDeleted)
        {
            return Stale(
                "schema_delete_precondition_changed");
        }

        if (canonical.Permanent)
        {
            if (!observed.Value.IsSoftDeleted ||
                observed.Value.ExistsActive ||
                !observed.Value.ExistsIncludingDeleted)
            {
                return Stale(
                    "schema_delete_precondition_permanent_not_ready");
            }
        }
        else if (!observed.Value.ExistsActive)
        {
            return Stale(
                "schema_delete_precondition_target_missing");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private static bool ValidateCreateShape(
        SchemaCreateCanonicalIntent canonical)
    {
        if (!Enum.IsDefined(canonical.Format) ||
            canonical.SchemaBytes is < 1 or >
                SchemaMutationPolicy.HardMaxSchemaBytes ||
            canonical.SchemaSha256.Length != 64 ||
            !canonical.SchemaSha256.All(char.IsAsciiHexDigit) ||
            canonical.References.Count >
                SchemaMutationPolicy.HardMaxReferences ||
            canonical.CompatibilityMode ==
                SchemaCompatibilityMode.Unknown)
        {
            return false;
        }

        try
        {
            _ = SchemaMutationCanonicalization.RequireIdentifier(
                canonical.ClusterId,
                "Cluster ID",
                256);
            _ = SchemaMutationCanonicalization.RequireSubject(
                canonical.Subject);
            _ = SchemaMutationCanonicalization.RequireIdentifier(
                canonical.MaterialName,
                "Schema material name",
                256);

            var names = new HashSet<string>(
                StringComparer.Ordinal);
            foreach (var reference in canonical.References)
            {
                if (reference.Version <= 0 ||
                    reference.TargetFingerprint.Length != 64 ||
                    !reference.TargetFingerprint.All(
                        char.IsAsciiHexDigit))
                {
                    return false;
                }

                var name =
                    SchemaMutationCanonicalization
                        .RequireReferenceName(reference.Name);
                _ = SchemaMutationCanonicalization.RequireSubject(
                    reference.Subject);
                if (!names.Add(name))
                    return false;
            }

            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool BindingsMatch(
        MutationOperationSnapshot operation,
        MutationOperationKind kind,
        string clusterId,
        string resourceKey,
        AuthorizationAction action,
        string authResource) =>
        operation.OperationKind == kind &&
        string.Equals(
            operation.ClusterId,
            clusterId,
            StringComparison.Ordinal) &&
        operation.ResourceKeys.Count == 1 &&
        string.Equals(
            operation.ResourceKeys[0],
            resourceKey,
            StringComparison.Ordinal) &&
        operation.AuthorizationTargets.Count == 1 &&
        operation.AuthorizationTargets[0].Action == action &&
        string.Equals(
            operation.AuthorizationTargets[0].ClusterId,
            clusterId,
            StringComparison.Ordinal) &&
        string.Equals(
            operation.AuthorizationTargets[0].ResourceName,
            authResource,
            StringComparison.Ordinal);

    private static bool TryGetSinglePrecondition(
        IReadOnlyList<MutationPrecondition> values,
        string key,
        out string fingerprint)
    {
        fingerprint = string.Empty;
        var matches = values
            .Where(value =>
                string.Equals(
                    value.Key,
                    key,
                    StringComparison.Ordinal))
            .Take(2)
            .ToArray();

        if (matches.Length != 1 ||
            values.Count != 1)
        {
            return false;
        }

        fingerprint = matches[0].Fingerprint;
        return true;
    }

    private static bool TryGetUniquePreconditions(
        IReadOnlyList<MutationPrecondition> values,
        out IReadOnlyDictionary<string, string>? result)
    {
        var dictionary = new Dictionary<string, string>(
            StringComparer.Ordinal);
        foreach (var value in values)
        {
            if (value is null ||
                !dictionary.TryAdd(
                    value.Key,
                    value.Fingerprint))
            {
                result = null;
                return false;
            }
        }

        result = dictionary;
        return true;
    }

    private ReadViewOperationContext Observation() =>
        new(
            _timeProvider.GetUtcNow().Add(
                _policy.ObservationTimeout),
            maxItems: 1_000,
            maxResponseBytes: 4 * 1024 * 1024);

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
