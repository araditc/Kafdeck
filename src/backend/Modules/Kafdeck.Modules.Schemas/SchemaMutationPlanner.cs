using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Core.Schemas;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Schemas;

public sealed class SchemaMutationPlanner
{
    private readonly ISchemaCatalogReadPort _catalog;
    private readonly ISchemaMutationObservationPort _mutationObservations;
    private readonly IMutationMaterialDigestService _digest;
    private readonly SchemaMutationPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public SchemaMutationPlanner(
        ISchemaCatalogReadPort catalog,
        ISchemaMutationObservationPort mutationObservations,
        IMutationMaterialDigestService digest,
        SchemaMutationPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _mutationObservations =
            mutationObservations ?? throw new ArgumentNullException(nameof(mutationObservations));
        _digest = digest ?? throw new ArgumentNullException(nameof(digest));
        _policy = policy ?? SchemaMutationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<SchemaMutationPlanningResult<SchemaCreateCanonicalIntent>>
        PlanCreateAsync(
            SchemaRegistrationRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string subject;
        IReadOnlyList<RecordSchemaReference> references;
        try
        {
            clusterId = SchemaMutationCanonicalization.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);
            subject = SchemaMutationCanonicalization.RequireSubject(
                request.Subject);

            if (!Enum.IsDefined(request.Format))
                throw new ArgumentOutOfRangeException(nameof(request.Format));

            references = SchemaMutationCanonicalization.NormalizeReferences(
                request.References,
                _policy.MaxReferences);

            var byteCount = Encoding.UTF8.GetByteCount(request.Schema);
            if (byteCount is < 1 || byteCount > _policy.MaxSchemaBytes)
            {
                return Failed<SchemaCreateCanonicalIntent>(
                    SchemaMutationPlanningFailureCode.LimitExceeded,
                    "Schema exceeds the configured byte ceiling.");
            }
        }
        catch (ArgumentException exception)
        {
            return Failed<SchemaCreateCanonicalIntent>(
                SchemaMutationPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var operation = Observation();
        var capabilities = await _mutationObservations.GetCapabilitiesAsync(
                clusterId,
                operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (!capabilities.IsSuccess || capabilities.Value is null)
        {
            return Failed<SchemaCreateCanonicalIntent>(
                MapObservationFailure(capabilities.Failure));
        }

        if (!capabilities.Value.SupportsRegistration)
        {
            return Failed<SchemaCreateCanonicalIntent>(
                SchemaMutationPlanningFailureCode.ProviderUnsupported,
                "Configured Schema Registry provider does not admit schema registration.");
        }

        var subjectState = await ObserveSubjectStateAsync(
                clusterId,
                subject,
                cancellationToken)
            .ConfigureAwait(false);
        if (!subjectState.IsSuccess)
        {
            return Failed<SchemaCreateCanonicalIntent>(
                subjectState.Failure!);
        }

        var canonicalReferences =
            new List<SchemaCanonicalReference>(references.Count);
        var preconditions =
            new List<MutationPrecondition>(references.Count + 1)
            {
                new(
                    "schema.subject",
                    subjectState.Value!.Fingerprint),
            };

        for (var ordinal = 0; ordinal < references.Count; ordinal++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var reference = references[ordinal];

            var detail = await _catalog.GetVersionAsync(
                    clusterId,
                    reference.Subject,
                    reference.Version,
                    Observation(),
                    cancellationToken)
                .ConfigureAwait(false);

            if (!detail.IsSuccess || detail.Value is null)
            {
                var failure = detail.Failure;
                if (IsResourceNotFound(failure))
                {
                    return Failed<SchemaCreateCanonicalIntent>(
                        SchemaMutationPlanningFailureCode.ReferenceNotFound,
                        "A referenced schema subject/version does not exist.");
                }

                return Failed<SchemaCreateCanonicalIntent>(
                    MapReadFailure(failure));
            }

            var fingerprint = FingerprintVersion(detail.Value);
            canonicalReferences.Add(
                new SchemaCanonicalReference(
                    reference.Name,
                    reference.Subject,
                    reference.Version,
                    fingerprint));
            preconditions.Add(
                new MutationPrecondition(
                    $"schema.reference/{ordinal:D4}",
                    fingerprint));
        }

        var compatibilityValidated = false;
        var compatibilityResultCode =
            subjectState.Value!.Exists
                ? "schema_compatibility_not_checked"
                : "schema_new_subject_no_prior_version";

        if (subjectState.Value.Exists)
        {
            if (!capabilities.Value.SupportsCompatibilityValidation)
            {
                return Failed<SchemaCreateCanonicalIntent>(
                    SchemaMutationPlanningFailureCode.ProviderUnsupported,
                    "Configured Schema Registry provider does not admit compatibility validation.");
            }

            var compatibility =
                await _mutationObservations.TestCompatibilityAsync(
                        clusterId,
                        new SchemaCompatibilityCheckRequest(
                            subject,
                            request.Format,
                            request.Schema,
                            references),
                        Observation(),
                        cancellationToken)
                    .ConfigureAwait(false);

            if (!compatibility.IsSuccess ||
                compatibility.Value is null)
            {
                return Failed<SchemaCreateCanonicalIntent>(
                    MapObservationFailure(
                        compatibility.Failure,
                        SchemaMutationPlanningFailureCode.CompatibilityUnavailable));
            }

            compatibilityValidated = true;
            compatibilityResultCode =
                SchemaMutationCanonicalization.RequireIdentifier(
                    compatibility.Value.ResultCode,
                    "Schema compatibility result code",
                    128);

            if (!compatibility.Value.IsCompatible)
            {
                return Failed<SchemaCreateCanonicalIntent>(
                    SchemaMutationPlanningFailureCode.SchemaIncompatible,
                    "Schema Registry rejected the candidate as incompatible.");
            }
        }

        byte[] schemaBytes;
        try
        {
            schemaBytes =
                SchemaMutationCanonicalization.EncodeSchema(
                    request.Schema,
                    _policy.MaxSchemaBytes);
        }
        catch (ArgumentException exception)
        {
            return Failed<SchemaCreateCanonicalIntent>(
                SchemaMutationPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        try
        {
            var materialName = "schema/source";
            var schemaSha =
                SchemaMutationCanonicalization.Sha256(schemaBytes);
            var canonical = new SchemaCreateCanonicalIntent(
                clusterId,
                subject,
                request.Format,
                materialName,
                schemaBytes.Length,
                schemaSha,
                Array.AsReadOnly(canonicalReferences.ToArray()),
                subjectState.Value.Exists,
                subjectState.Value.LatestVersion,
                subjectState.Value.Fingerprint,
                subjectState.Value.CompatibilityMode,
                subjectState.Value.CompatibilityInherited,
                compatibilityValidated,
                compatibilityResultCode);

            var canonicalJson =
                SchemaMutationCanonicalization.Serialize(canonical);
            if (canonicalJson.Length >
                MutationLimits.MaxCanonicalIntentCharacters)
            {
                return Failed<SchemaCreateCanonicalIntent>(
                    SchemaMutationPlanningFailureCode.LimitExceeded,
                    "Schema mutation preview exceeds the canonical intent ceiling.");
            }

            var materialDigest = new MutationMaterialDigest(
                materialName,
                _digest.ComputeDigest(schemaBytes));

            var intent = new MutationIntentDescriptor(
                MutationOperationKind.SchemaCreate,
                clusterId,
                canonicalJson,
                new[]
                {
                    SchemaMutationCanonicalization.ResourceKey(
                        clusterId,
                        subject),
                },
                Array.AsReadOnly(preconditions.ToArray()),
                new[] { materialDigest },
                new[]
                {
                    new MutationAuthorizationTarget(
                        AuthorizationAction.SchemaCreate,
                        clusterId,
                        SchemaMutationCanonicalization.AuthorizationResource(
                            subject)),
                });

            var risk = MutationRiskClassifier.Classify(
                new MutationRiskInput(
                    MutationOperationKind.SchemaCreate));

            var material = new MutationExecutionMaterial(
                new Dictionary<string, ReadOnlyMemory<byte>>(
                    StringComparer.Ordinal)
                {
                    [materialName] = schemaBytes,
                });

            return SchemaMutationPlanningResult<SchemaCreateCanonicalIntent>
                .Success(
                    new SchemaMutationPlan<SchemaCreateCanonicalIntent>(
                        canonical,
                        intent,
                        risk),
                    material);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(schemaBytes);
        }
    }

    public async Task<SchemaMutationPlanningResult<SchemaCompatibilityCanonicalIntent>>
        PlanCompatibilityAsync(
            SchemaCompatibilityAlterRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string? subject = null;
        try
        {
            clusterId = SchemaMutationCanonicalization.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);

            if (!Enum.IsDefined(request.Scope) ||
                request.RequestedMode == SchemaCompatibilityMode.Unknown ||
                !Enum.IsDefined(request.RequestedMode))
            {
                throw new ArgumentOutOfRangeException(nameof(request));
            }

            if (request.Scope == SchemaCompatibilityScope.Subject)
            {
                subject = SchemaMutationCanonicalization.RequireSubject(
                    request.Subject ?? string.Empty);
            }
            else if (request.Subject is not null)
            {
                throw new ArgumentException(
                    "Global schema compatibility mutation cannot include a subject.");
            }
        }
        catch (ArgumentException exception)
        {
            return Failed<SchemaCompatibilityCanonicalIntent>(
                SchemaMutationPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var capabilities = await _mutationObservations.GetCapabilitiesAsync(
                clusterId,
                Observation(),
                cancellationToken)
            .ConfigureAwait(false);
        if (!capabilities.IsSuccess || capabilities.Value is null)
        {
            return Failed<SchemaCompatibilityCanonicalIntent>(
                MapObservationFailure(capabilities.Failure));
        }

        if (!capabilities.Value.SupportsCompatibilityMutation)
        {
            return Failed<SchemaCompatibilityCanonicalIntent>(
                SchemaMutationPlanningFailureCode.ProviderUnsupported,
                "Configured Schema Registry provider does not admit compatibility mutation.");
        }

        SchemaCompatibilityMode currentMode;
        bool currentInherited;
        string stateFingerprint;

        if (request.Scope == SchemaCompatibilityScope.Subject)
        {
            var subjectState = await ObserveSubjectStateAsync(
                    clusterId,
                    subject!,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!subjectState.IsSuccess)
            {
                return Failed<SchemaCompatibilityCanonicalIntent>(
                    subjectState.Failure!);
            }

            if (!subjectState.Value!.Exists)
            {
                return Failed<SchemaCompatibilityCanonicalIntent>(
                    SchemaMutationPlanningFailureCode.SubjectNotFound,
                    "Schema subject does not exist.");
            }

            currentMode = subjectState.Value.CompatibilityMode;
            currentInherited = subjectState.Value.CompatibilityInherited;
            stateFingerprint = subjectState.Value.Fingerprint;
        }
        else
        {
            var global = await _catalog.GetGlobalCompatibilityAsync(
                    clusterId,
                    Observation(),
                    cancellationToken)
                .ConfigureAwait(false);
            if (!global.IsSuccess || global.Value is null)
            {
                return Failed<SchemaCompatibilityCanonicalIntent>(
                    MapReadFailure(global.Failure));
            }

            currentMode = global.Value.Mode;
            currentInherited = false;
            stateFingerprint =
                FingerprintCompatibility(
                    SchemaCompatibilityScope.Global,
                    subject: null,
                    currentMode,
                    currentInherited);
        }

        if (currentMode == request.RequestedMode)
        {
            return Failed<SchemaCompatibilityCanonicalIntent>(
                SchemaMutationPlanningFailureCode.NoChange,
                "Requested compatibility mode already matches the observed provider state.");
        }

        var canonical = new SchemaCompatibilityCanonicalIntent(
            clusterId,
            request.Scope,
            subject,
            currentMode,
            currentInherited,
            request.RequestedMode,
            stateFingerprint);

        var canonicalJson =
            SchemaMutationCanonicalization.Serialize(canonical);
        var authResource =
            request.Scope == SchemaCompatibilityScope.Subject
                ? SchemaMutationCanonicalization.AuthorizationResource(
                    subject!)
                : SchemaMutationCanonicalization.GlobalCompatibilityResource;

        var resourceKey =
            request.Scope == SchemaCompatibilityScope.Subject
                ? SchemaMutationCanonicalization.ResourceKey(
                    clusterId,
                    subject!)
                : $"cluster/{clusterId}/schema-compatibility/global";

        var intent = new MutationIntentDescriptor(
            MutationOperationKind.SchemaAlter,
            clusterId,
            canonicalJson,
            new[] { resourceKey },
            new[]
            {
                new MutationPrecondition(
                    "schema.compatibility",
                    stateFingerprint),
            },
            AuthorizationTargets:
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.SchemaAlter,
                    clusterId,
                    authResource),
            });

        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                MutationOperationKind.SchemaAlter));

        return SchemaMutationPlanningResult<SchemaCompatibilityCanonicalIntent>
            .Success(
                new SchemaMutationPlan<SchemaCompatibilityCanonicalIntent>(
                    canonical,
                    intent,
                    risk));
    }

    public async Task<SchemaMutationPlanningResult<SchemaDeleteCanonicalIntent>>
        PlanDeleteAsync(
            SchemaDeleteRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string subject;
        try
        {
            clusterId = SchemaMutationCanonicalization.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);
            subject = SchemaMutationCanonicalization.RequireSubject(
                request.Subject);
            if (request.Version is <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(request.Version));
            }
        }
        catch (ArgumentException exception)
        {
            return Failed<SchemaDeleteCanonicalIntent>(
                SchemaMutationPlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var capabilities = await _mutationObservations.GetCapabilitiesAsync(
                clusterId,
                Observation(),
                cancellationToken)
            .ConfigureAwait(false);
        if (!capabilities.IsSuccess || capabilities.Value is null)
        {
            return Failed<SchemaDeleteCanonicalIntent>(
                MapObservationFailure(capabilities.Failure));
        }

        if (request.Permanent)
        {
            if (!capabilities.Value.SupportsPermanentDelete)
            {
                return Failed<SchemaDeleteCanonicalIntent>(
                    SchemaMutationPlanningFailureCode.ProviderUnsupported,
                    "Configured Schema Registry provider does not admit permanent delete.");
            }
        }
        else if (!capabilities.Value.SupportsSoftDelete)
        {
            return Failed<SchemaDeleteCanonicalIntent>(
                SchemaMutationPlanningFailureCode.ProviderUnsupported,
                "Configured Schema Registry provider does not admit soft delete.");
        }

        var observed = await _mutationObservations.ObserveDeleteTargetAsync(
                clusterId,
                new SchemaDeleteObservationRequest(
                    subject,
                    request.Version),
                Observation(),
                cancellationToken)
            .ConfigureAwait(false);
        if (!observed.IsSuccess || observed.Value is null)
        {
            return Failed<SchemaDeleteCanonicalIntent>(
                MapObservationFailure(observed.Failure));
        }

        if (request.Permanent)
        {
            if (!observed.Value.IsSoftDeleted ||
                observed.Value.ExistsActive ||
                !observed.Value.ExistsIncludingDeleted)
            {
                return Failed<SchemaDeleteCanonicalIntent>(
                    SchemaMutationPlanningFailureCode.PermanentDeleteRequiresSoftDelete,
                    "Permanent schema deletion requires a previously soft-deleted target.");
            }
        }
        else if (!observed.Value.ExistsActive)
        {
            return Failed<SchemaDeleteCanonicalIntent>(
                request.Version.HasValue
                    ? SchemaMutationPlanningFailureCode.VersionNotFound
                    : SchemaMutationPlanningFailureCode.SubjectNotFound,
                request.Version.HasValue
                    ? "Schema version does not exist as an active target."
                    : "Schema subject does not exist as an active target.");
        }

        var fingerprint = FingerprintDeleteTarget(
            subject,
            request.Version,
            observed.Value);

        var canonical = new SchemaDeleteCanonicalIntent(
            clusterId,
            subject,
            request.Version,
            request.Permanent,
            observed.Value.ExistsActive,
            observed.Value.ExistsIncludingDeleted,
            observed.Value.IsSoftDeleted,
            fingerprint);

        var canonicalJson =
            SchemaMutationCanonicalization.Serialize(canonical);
        var resource =
            request.Version.HasValue
                ? $"schema/{subject}/version/{request.Version.Value}"
                : SchemaMutationCanonicalization.AuthorizationResource(
                    subject);
        var resourceKey =
            request.Version.HasValue
                ? $"cluster/{clusterId}/schema/{subject}/version/{request.Version.Value}"
                : SchemaMutationCanonicalization.ResourceKey(
                    clusterId,
                    subject);

        var intent = new MutationIntentDescriptor(
            MutationOperationKind.SchemaDelete,
            clusterId,
            canonicalJson,
            new[] { resourceKey },
            new[]
            {
                new MutationPrecondition(
                    "schema.delete-target",
                    fingerprint),
            },
            AuthorizationTargets:
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.SchemaDelete,
                    clusterId,
                    resource),
            },
            RiskContext:
            new MutationRiskContext(
                PermanentDelete: request.Permanent));

        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                MutationOperationKind.SchemaDelete,
                PermanentDelete: request.Permanent));

        return SchemaMutationPlanningResult<SchemaDeleteCanonicalIntent>
            .Success(
                new SchemaMutationPlan<SchemaDeleteCanonicalIntent>(
                    canonical,
                    intent,
                    risk));
    }

    internal async Task<SchemaSubjectStateResult>
        ObserveSubjectStateAsync(
            string clusterId,
            string subject,
            CancellationToken cancellationToken)
    {
        var versions = await _catalog.ListVersionsAsync(
                clusterId,
                subject,
                Observation(),
                cancellationToken)
            .ConfigureAwait(false);

        if (!versions.IsSuccess || versions.Value is null)
        {
            if (!IsResourceNotFound(versions.Failure))
            {
                return SchemaSubjectStateResult.Failed(
                    MapReadFailure(versions.Failure));
            }

            var global = await _catalog.GetGlobalCompatibilityAsync(
                    clusterId,
                    Observation(),
                    cancellationToken)
                .ConfigureAwait(false);
            if (!global.IsSuccess || global.Value is null)
            {
                return SchemaSubjectStateResult.Failed(
                    MapReadFailure(global.Failure));
            }

            var absentFingerprint = FingerprintSubject(
                subject,
                Array.Empty<SchemaVersionSummary>(),
                global.Value.Mode,
                compatibilityInherited: true,
                exists: false);

            return SchemaSubjectStateResult.Success(
                new SchemaSubjectState(
                    Exists: false,
                    Versions:
                        Array.Empty<SchemaVersionSummary>(),
                    LatestVersion: null,
                    CompatibilityMode: global.Value.Mode,
                    CompatibilityInherited: true,
                    Fingerprint: absentFingerprint));
        }

        var compatibility = await _catalog.GetCompatibilityAsync(
                clusterId,
                subject,
                Observation(),
                cancellationToken)
            .ConfigureAwait(false);
        if (!compatibility.IsSuccess ||
            compatibility.Value is null)
        {
            return SchemaSubjectStateResult.Failed(
                MapReadFailure(compatibility.Failure));
        }

        var normalizedVersions = versions.Value
            .OrderBy(item => item.Version)
            .ToArray();
        var fingerprint = FingerprintSubject(
            subject,
            normalizedVersions,
            compatibility.Value.Mode,
            compatibility.Value.IsInherited,
            exists: true);

        return SchemaSubjectStateResult.Success(
            new SchemaSubjectState(
                Exists: true,
                Versions:
                    Array.AsReadOnly(normalizedVersions),
                LatestVersion:
                    normalizedVersions.Length == 0
                        ? null
                        : normalizedVersions[^1].Version,
                CompatibilityMode:
                    compatibility.Value.Mode,
                CompatibilityInherited:
                    compatibility.Value.IsInherited,
                Fingerprint:
                    fingerprint));
    }

    internal static string FingerprintVersion(
        SchemaVersionDetail detail)
    {
        ArgumentNullException.ThrowIfNull(detail);
        var values =
            new List<KeyValuePair<string, string>>
            {
                new("subject", detail.Subject),
                new(
                    "version",
                    detail.Version.ToString(
                        System.Globalization.CultureInfo.InvariantCulture)),
                new(
                    "schema-id",
                    detail.Schema.Id.ToString(
                        System.Globalization.CultureInfo.InvariantCulture)),
                new(
                    "format",
                    ((int)detail.Schema.Format).ToString(
                        System.Globalization.CultureInfo.InvariantCulture)),
                new(
                    "schema-sha256",
                    SchemaMutationCanonicalization.Sha256(
                        Encoding.UTF8.GetBytes(
                            detail.Schema.SchemaText))),
            };

        foreach (var reference in detail.Schema.References
                     .OrderBy(item => item.Name, StringComparer.Ordinal)
                     .ThenBy(item => item.Subject, StringComparer.Ordinal)
                     .ThenBy(item => item.Version))
        {
            values.Add(
                new(
                    "reference",
                    $"{reference.Name}|{reference.Subject}|{reference.Version}"));
        }

        return SchemaMutationCanonicalization.FingerprintText(values);
    }

    internal static string FingerprintDeleteTarget(
        string subject,
        int? version,
        SchemaDeleteTargetObservation observation)
    {
        var values =
            new List<KeyValuePair<string, string>>
            {
                new("subject", subject),
                new(
                    "version",
                    version?.ToString(
                        System.Globalization.CultureInfo.InvariantCulture)
                    ?? "subject"),
                new(
                    "active",
                    observation.ExistsActive ? "1" : "0"),
                new(
                    "including-deleted",
                    observation.ExistsIncludingDeleted ? "1" : "0"),
                new(
                    "soft-deleted",
                    observation.IsSoftDeleted ? "1" : "0"),
                new(
                    "active-versions",
                    string.Join(
                        ",",
                        observation.ActiveVersions.OrderBy(value => value))),
                new(
                    "all-versions",
                    string.Join(
                        ",",
                        observation.VersionsIncludingDeleted
                            .OrderBy(value => value))),
            };

        return SchemaMutationCanonicalization.FingerprintText(values);
    }

    internal static string FingerprintCompatibility(
        SchemaCompatibilityScope scope,
        string? subject,
        SchemaCompatibilityMode mode,
        bool inherited) =>
        SchemaMutationCanonicalization.FingerprintText(
            new[]
            {
                new KeyValuePair<string, string>(
                    "scope",
                    ((int)scope).ToString(
                        System.Globalization.CultureInfo.InvariantCulture)),
                new(
                    "subject",
                    subject ?? "global"),
                new(
                    "mode",
                    ((int)mode).ToString(
                        System.Globalization.CultureInfo.InvariantCulture)),
                new(
                    "inherited",
                    inherited ? "1" : "0"),
            });

    private static string FingerprintSubject(
        string subject,
        IReadOnlyList<SchemaVersionSummary> versions,
        SchemaCompatibilityMode mode,
        bool compatibilityInherited,
        bool exists)
    {
        var values =
            new List<KeyValuePair<string, string>>
            {
                new("subject", subject),
                new("exists", exists ? "1" : "0"),
                new(
                    "compatibility",
                    ((int)mode).ToString(
                        System.Globalization.CultureInfo.InvariantCulture)),
                new(
                    "compatibility-inherited",
                    compatibilityInherited ? "1" : "0"),
            };

        foreach (var version in versions
                     .OrderBy(item => item.Version))
        {
            values.Add(
                new(
                    "version",
                    $"{version.Version}|{version.SchemaId}|{(int)version.Format}"));

            foreach (var reference in version.References
                         .OrderBy(item => item.Name, StringComparer.Ordinal)
                         .ThenBy(item => item.Subject, StringComparer.Ordinal)
                         .ThenBy(item => item.Version))
            {
                values.Add(
                    new(
                        "version-reference",
                        $"{version.Version}|{reference.Name}|{reference.Subject}|{reference.Version}"));
            }
        }

        return SchemaMutationCanonicalization.FingerprintText(values);
    }

    private ReadViewOperationContext Observation() =>
        new(
            _timeProvider.GetUtcNow().Add(
                _policy.ObservationTimeout),
            maxItems: 1_000,
            maxResponseBytes: 4 * 1024 * 1024);

    private static bool IsResourceNotFound(
        ReadViewFailure? failure) =>
        failure?.Code is
            "schema_registry_resource_not_found" or
            "schema_not_found";

    private static SchemaMutationPlanningFailure MapReadFailure(
        ReadViewFailure? failure)
    {
        if (failure is null)
        {
            return new(
                SchemaMutationPlanningFailureCode.ObservationFailed,
                "Schema Registry state could not be safely observed.");
        }

        return failure.Category switch
        {
            ReadViewFailureCategory.NotConfigured =>
                new(
                    SchemaMutationPlanningFailureCode.ProviderNotConfigured,
                    "Schema Registry is not configured for the requested cluster."),
            ReadViewFailureCategory.Unauthorized =>
                new(
                    SchemaMutationPlanningFailureCode.ProviderUnauthorized,
                    "Schema Registry denied the required observation."),
            ReadViewFailureCategory.Unsupported =>
                new(
                    SchemaMutationPlanningFailureCode.ProviderUnsupported,
                    "Schema Registry does not support the required observation."),
            ReadViewFailureCategory.Unavailable or
            ReadViewFailureCategory.Timeout or
            ReadViewFailureCategory.Cancelled =>
                new(
                    SchemaMutationPlanningFailureCode.ProviderUnavailable,
                    "Schema Registry state is currently unavailable."),
            _ =>
                new(
                    SchemaMutationPlanningFailureCode.ObservationFailed,
                    "Schema Registry state could not be safely observed."),
        };
    }

    private static SchemaMutationPlanningFailure MapObservationFailure(
        SchemaMutationObservationFailure? failure,
        SchemaMutationPlanningFailureCode unavailableCode =
            SchemaMutationPlanningFailureCode.ProviderUnavailable)
    {
        if (failure is null)
        {
            return new(
                SchemaMutationPlanningFailureCode.ObservationFailed,
                "Schema Registry mutation capability could not be safely observed.");
        }

        return failure.Category switch
        {
            SchemaMutationObservationFailureCategory.NotConfigured =>
                new(
                    SchemaMutationPlanningFailureCode.ProviderNotConfigured,
                    "Schema Registry is not configured for the requested cluster."),
            SchemaMutationObservationFailureCategory.Unauthorized =>
                new(
                    SchemaMutationPlanningFailureCode.ProviderUnauthorized,
                    "Schema Registry denied the required operation."),
            SchemaMutationObservationFailureCategory.Unsupported =>
                new(
                    SchemaMutationPlanningFailureCode.ProviderUnsupported,
                    "Schema Registry provider does not support the required capability."),
            SchemaMutationObservationFailureCategory.Unavailable or
            SchemaMutationObservationFailureCategory.Timeout or
            SchemaMutationObservationFailureCategory.Cancelled =>
                new(
                    unavailableCode,
                    "Schema Registry mutation capability is currently unavailable."),
            _ =>
                new(
                    SchemaMutationPlanningFailureCode.ObservationFailed,
                    "Schema Registry mutation capability could not be safely observed."),
        };
    }

    private static SchemaMutationPlanningResult<T> Failed<T>(
        SchemaMutationPlanningFailureCode code,
        string message)
        where T : class =>
        SchemaMutationPlanningResult<T>.Failed(
            new SchemaMutationPlanningFailure(
                code,
                message));

    private static SchemaMutationPlanningResult<T> Failed<T>(
        SchemaMutationPlanningFailure failure)
        where T : class =>
        SchemaMutationPlanningResult<T>.Failed(failure);

    internal sealed record SchemaSubjectState(
        bool Exists,
        IReadOnlyList<SchemaVersionSummary> Versions,
        int? LatestVersion,
        SchemaCompatibilityMode CompatibilityMode,
        bool CompatibilityInherited,
        string Fingerprint);

    internal sealed record SchemaSubjectStateResult
    {
        private SchemaSubjectStateResult(
            SchemaSubjectState? value,
            SchemaMutationPlanningFailure? failure)
        {
            Value = value;
            Failure = failure;
        }

        public SchemaSubjectState? Value { get; }
        public SchemaMutationPlanningFailure? Failure { get; }
        public bool IsSuccess => Failure is null && Value is not null;

        public static SchemaSubjectStateResult Success(
            SchemaSubjectState value) =>
            new(value, null);

        public static SchemaSubjectStateResult Failed(
            SchemaMutationPlanningFailure failure) =>
            new(null, failure);
    }
}
