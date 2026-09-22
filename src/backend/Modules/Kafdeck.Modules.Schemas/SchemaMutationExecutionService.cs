using System.Globalization;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Schemas;

public sealed record SchemaMutationVerificationPolicy
{
    public SchemaMutationVerificationPolicy(
        TimeSpan timeout,
        TimeSpan pollInterval)
    {
        if (timeout < TimeSpan.FromSeconds(1) ||
            timeout > TimeSpan.FromSeconds(15))
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }

        if (pollInterval < TimeSpan.FromMilliseconds(50) ||
            pollInterval > TimeSpan.FromSeconds(2) ||
            pollInterval >= timeout)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }

        Timeout = timeout;
        PollInterval = pollInterval;
    }

    public TimeSpan Timeout { get; }
    public TimeSpan PollInterval { get; }

    public static SchemaMutationVerificationPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(250));
}

public sealed class SchemaMutationExecutionService
{
    private static readonly TimeSpan ExecutionCompletionReserve =
        TimeSpan.FromMilliseconds(250);

    private readonly ISchemaMutationPort _mutations;
    private readonly ISchemaCatalogReadPort _catalog;
    private readonly ISchemaMutationObservationPort _observations;
    private readonly SchemaMutationVerificationPolicy _verification;
    private readonly TimeProvider _timeProvider;

    public SchemaMutationExecutionService(
        ISchemaMutationPort mutations,
        ISchemaCatalogReadPort catalog,
        ISchemaMutationObservationPort observations,
        SchemaMutationVerificationPolicy? verification = null,
        TimeProvider? timeProvider = null)
    {
        _mutations = mutations ?? throw new ArgumentNullException(nameof(mutations));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _verification =
            verification ?? SchemaMutationVerificationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> CreateAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Operation.OperationKind !=
            MutationOperationKind.SchemaCreate)
        {
            return Unknown(
                "schema_create_operation_mismatch");
        }

        SchemaCreateCanonicalIntent canonical;
        try
        {
            canonical =
                SchemaMutationCanonicalization.Deserialize<
                    SchemaCreateCanonicalIntent>(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return Unknown(
                "schema_create_canonical_invalid");
        }

        if (!TryBuildCreateMutation(
                canonical,
                context.Material,
                out var mutation))
        {
            return Unknown(
                "schema_create_material_invalid");
        }

        MutationProviderResult accepted;
        try
        {
            accepted = await _mutations.CreateAsync(
                    mutation!,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unknown(
                "schema_create_cancelled_or_timeout");
        }
        catch
        {
            return Unknown(
                "schema_create_provider_exception");
        }

        if (accepted.ResultKind !=
            MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        var schemaId = ParsePositiveEvidenceInt(
            accepted.SafeEvidence,
            "schema.id");

        var verification = await VerifyUntilAsync(
                async (verificationDeadlineUtc, token) =>
                {
                    var versions = await _catalog.ListVersionsAsync(
                            canonical.ClusterId,
                            canonical.Subject,
                            Observation(verificationDeadlineUtc),
                            token)
                        .ConfigureAwait(false);

                    if (!versions.IsSuccess ||
                        versions.Value is null)
                    {
                        return VerificationObservation.NotVerified();
                    }

                    var candidates = versions.Value
                        .Where(version =>
                            version.Format == canonical.Format &&
                            (!schemaId.HasValue ||
                             version.SchemaId == schemaId.Value))
                        .OrderByDescending(version => version.Version)
                        .ToArray();

                    foreach (var candidate in candidates)
                    {
                        var detail = await _catalog.GetVersionAsync(
                                canonical.ClusterId,
                                canonical.Subject,
                                candidate.Version,
                                Observation(verificationDeadlineUtc),
                                token)
                            .ConfigureAwait(false);

                        if (!detail.IsSuccess ||
                            detail.Value is null)
                        {
                            continue;
                        }

                        var sourceBytes =
                            System.Text.Encoding.UTF8.GetBytes(
                                detail.Value.Schema.SchemaText);
                        try
                        {
                            var digest =
                                SchemaMutationCanonicalization.Sha256(
                                    sourceBytes);
                            if (!string.Equals(
                                    digest,
                                    canonical.SchemaSha256,
                                    StringComparison.Ordinal))
                            {
                                continue;
                            }
                        }
                        finally
                        {
                            System.Security.Cryptography
                                .CryptographicOperations
                                .ZeroMemory(sourceBytes);
                        }

                        if (!ReferencesMatch(
                                canonical.References,
                                detail.Value.Schema.References))
                        {
                            continue;
                        }

                        return VerificationObservation.Observed(
                            candidate.Version,
                            candidate.SchemaId);
                    }

                    return VerificationObservation.NotVerified();
                },
                cancellationToken,
                context.ExecutionDeadlineUtc)
            .ConfigureAwait(false);

        if (verification.Verified)
        {
            var evidence = MergeEvidence(
                accepted.SafeEvidence);
            evidence["verification.state"] = "observed";
            if (verification.Version.HasValue)
            {
                evidence["schema.version"] =
                    verification.Version.Value.ToString(
                        CultureInfo.InvariantCulture);
            }

            if (verification.SchemaId.HasValue)
            {
                evidence["schema.id"] =
                    verification.SchemaId.Value.ToString(
                        CultureInfo.InvariantCulture);
            }

            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "schema_create_verified",
                evidence);
        }

        return AppliedUnverified(
            "schema_create_verification_inconclusive",
            accepted);
    }

    public async Task<MutationProviderResult> AlterCompatibilityAsync(
        SchemaCompatibilityCanonicalIntent canonical,
        CancellationToken cancellationToken = default,
        DateTimeOffset? executionDeadlineUtc = null)
    {
        ArgumentNullException.ThrowIfNull(canonical);

        if (!TryBuildCompatibilityMutation(
                canonical,
                out var mutation))
        {
            return Unknown(
                "schema_compatibility_canonical_invalid");
        }

        MutationProviderResult accepted;
        try
        {
            accepted =
                await _mutations.AlterCompatibilityAsync(
                        mutation!,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unknown(
                "schema_compatibility_cancelled_or_timeout");
        }
        catch
        {
            return Unknown(
                "schema_compatibility_provider_exception");
        }

        if (accepted.ResultKind !=
            MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        var verified = await VerifyUntilAsync(
                async (verificationDeadlineUtc, token) =>
                {
                    SchemaCompatibilityMode observed;

                    if (canonical.Scope ==
                        SchemaCompatibilityScope.Subject)
                    {
                        var result =
                            await _catalog.GetCompatibilityAsync(
                                    canonical.ClusterId,
                                    canonical.Subject!,
                                    Observation(verificationDeadlineUtc),
                                    token)
                                .ConfigureAwait(false);

                        if (!result.IsSuccess ||
                            result.Value is null)
                        {
                            return VerificationObservation.NotVerified();
                        }

                        observed = result.Value.Mode;
                    }
                    else
                    {
                        var result =
                            await _catalog.GetGlobalCompatibilityAsync(
                                    canonical.ClusterId,
                                    Observation(verificationDeadlineUtc),
                                    token)
                                .ConfigureAwait(false);

                        if (!result.IsSuccess ||
                            result.Value is null)
                        {
                            return VerificationObservation.NotVerified();
                        }

                        observed = result.Value.Mode;
                    }

                    return observed == canonical.RequestedMode
                        ? VerificationObservation.Observed()
                        : VerificationObservation.NotVerified();
                },
                cancellationToken,
                executionDeadlineUtc)
            .ConfigureAwait(false);

        if (verified.Verified)
        {
            var evidence = MergeEvidence(
                accepted.SafeEvidence);
            evidence["verification.state"] = "observed";
            evidence["compatibility.mode"] =
                SchemaMutationCanonicalization
                    .CompatibilityName(
                        canonical.RequestedMode);

            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                "schema_compatibility_verified",
                evidence);
        }

        return AppliedUnverified(
            "schema_compatibility_verification_inconclusive",
            accepted);
    }

    public async Task<MutationProviderResult> DeleteAsync(
        SchemaDeleteCanonicalIntent canonical,
        CancellationToken cancellationToken = default,
        DateTimeOffset? executionDeadlineUtc = null)
    {
        ArgumentNullException.ThrowIfNull(canonical);

        if (!TryBuildDeleteMutation(
                canonical,
                out var mutation))
        {
            return Unknown(
                "schema_delete_canonical_invalid");
        }

        MutationProviderResult accepted;
        try
        {
            accepted = await _mutations.DeleteAsync(
                    mutation!,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unknown(
                "schema_delete_cancelled_or_timeout");
        }
        catch
        {
            return Unknown(
                "schema_delete_provider_exception");
        }

        if (accepted.ResultKind !=
            MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        var verified = await VerifyUntilAsync(
                async (verificationDeadlineUtc, token) =>
                {
                    var observed =
                        await _observations.ObserveDeleteTargetAsync(
                                canonical.ClusterId,
                                new SchemaDeleteObservationRequest(
                                    canonical.Subject,
                                    canonical.Version),
                                Observation(verificationDeadlineUtc),
                                token)
                            .ConfigureAwait(false);

                    if (!observed.IsSuccess ||
                        observed.Value is null)
                    {
                        return VerificationObservation.NotVerified();
                    }

                    var isVerified = canonical.Permanent
                        ? !observed.Value.ExistsActive &&
                          !observed.Value.ExistsIncludingDeleted
                        : !observed.Value.ExistsActive &&
                          observed.Value.ExistsIncludingDeleted &&
                          observed.Value.IsSoftDeleted;

                    return isVerified
                        ? VerificationObservation.Observed()
                        : VerificationObservation.NotVerified();
                },
                cancellationToken,
                executionDeadlineUtc)
            .ConfigureAwait(false);

        if (verified.Verified)
        {
            var evidence = MergeEvidence(
                accepted.SafeEvidence);
            evidence["verification.state"] = "observed";
            evidence["delete.permanent"] =
                canonical.Permanent ? "true" : "false";

            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedVerified,
                canonical.Permanent
                    ? "schema_delete_permanent_verified"
                    : "schema_delete_soft_verified",
                evidence);
        }

        return AppliedUnverified(
            canonical.Permanent
                ? "schema_delete_permanent_verification_inconclusive"
                : "schema_delete_soft_verification_inconclusive",
            accepted);
    }

    private static bool TryBuildCreateMutation(
        SchemaCreateCanonicalIntent canonical,
        MutationExecutionMaterial material,
        out SchemaCreateMutation? mutation)
    {
        mutation = null;

        if (canonical.SchemaBytes is < 1 or >
                SchemaMutationPolicy.HardMaxSchemaBytes ||
            canonical.SchemaSha256.Length != 64 ||
            !canonical.SchemaSha256.All(char.IsAsciiHexDigit) ||
            canonical.References.Count >
                SchemaMutationPolicy.HardMaxReferences ||
            !Enum.IsDefined(canonical.Format))
        {
            return false;
        }

        try
        {
            var cluster =
                SchemaMutationCanonicalization.RequireIdentifier(
                    canonical.ClusterId,
                    "Cluster ID",
                    256);
            var subject =
                SchemaMutationCanonicalization.RequireSubject(
                    canonical.Subject);
            var materialName =
                SchemaMutationCanonicalization.RequireIdentifier(
                    canonical.MaterialName,
                    "Schema material name",
                    256);

            var source = material.GetRequired(materialName);
            if (source.Length != canonical.SchemaBytes ||
                !string.Equals(
                    SchemaMutationCanonicalization.Sha256(
                        source.Span),
                    canonical.SchemaSha256,
                    StringComparison.Ordinal))
            {
                return false;
            }

            var schema =
                SchemaMutationCanonicalization.DecodeSchema(
                    source);

            var names = new HashSet<string>(
                StringComparer.Ordinal);
            var references =
                new List<SchemaMutationReference>(
                    canonical.References.Count);

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
                    SchemaMutationCanonicalization.RequireReferenceName(
                        reference.Name);
                var referenceSubject =
                    SchemaMutationCanonicalization.RequireSubject(
                        reference.Subject);
                if (!names.Add(name))
                    return false;

                references.Add(
                    new SchemaMutationReference(
                        name,
                        referenceSubject,
                        reference.Version));
            }

            mutation = new SchemaCreateMutation(
                cluster,
                subject,
                canonical.Format,
                schema,
                Array.AsReadOnly(
                    references.ToArray()));
            return true;
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                MutationStateException or
                KeyNotFoundException)
        {
            return false;
        }
    }

    private static bool TryBuildCompatibilityMutation(
        SchemaCompatibilityCanonicalIntent canonical,
        out SchemaAlterMutation? mutation)
    {
        mutation = null;

        if (!Enum.IsDefined(canonical.Scope) ||
            canonical.RequestedMode ==
                SchemaCompatibilityMode.Unknown ||
            !Enum.IsDefined(canonical.RequestedMode) ||
            canonical.StateFingerprint.Length != 64 ||
            !canonical.StateFingerprint.All(
                char.IsAsciiHexDigit))
        {
            return false;
        }

        try
        {
            var cluster =
                SchemaMutationCanonicalization.RequireIdentifier(
                    canonical.ClusterId,
                    "Cluster ID",
                    256);
            string? subject = null;

            if (canonical.Scope ==
                SchemaCompatibilityScope.Subject)
            {
                subject =
                    SchemaMutationCanonicalization.RequireSubject(
                        canonical.Subject ?? string.Empty);
            }
            else if (canonical.Subject is not null)
            {
                return false;
            }

            mutation = new SchemaAlterMutation(
                cluster,
                canonical.Scope,
                subject,
                SchemaMutationCanonicalization
                    .CompatibilityName(
                        canonical.RequestedMode));
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool TryBuildDeleteMutation(
        SchemaDeleteCanonicalIntent canonical,
        out SchemaDeleteMutation? mutation)
    {
        mutation = null;

        if (canonical.Version is <= 0 ||
            canonical.TargetFingerprint.Length != 64 ||
            !canonical.TargetFingerprint.All(
                char.IsAsciiHexDigit))
        {
            return false;
        }

        if (canonical.Permanent)
        {
            if (canonical.ExistsActive ||
                !canonical.ExistsIncludingDeleted ||
                !canonical.IsSoftDeleted)
            {
                return false;
            }
        }
        else if (!canonical.ExistsActive)
        {
            return false;
        }

        try
        {
            var cluster =
                SchemaMutationCanonicalization.RequireIdentifier(
                    canonical.ClusterId,
                    "Cluster ID",
                    256);
            var subject =
                SchemaMutationCanonicalization.RequireSubject(
                    canonical.Subject);

            mutation = new SchemaDeleteMutation(
                cluster,
                subject,
                canonical.Version,
                canonical.Permanent);
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private async Task<VerificationObservation> VerifyUntilAsync(
        Func<DateTimeOffset, CancellationToken, Task<VerificationObservation>> observe,
        CancellationToken cancellationToken,
        DateTimeOffset? executionDeadlineUtc = null)
    {
        var now = _timeProvider.GetUtcNow();
        var deadline = now.Add(_verification.Timeout);

        if (executionDeadlineUtc.HasValue)
        {
            var safeExecutionDeadline =
                executionDeadlineUtc.Value - ExecutionCompletionReserve;
            if (safeExecutionDeadline < deadline)
            {
                deadline = safeExecutionDeadline;
            }
        }

        if (deadline <= now || cancellationToken.IsCancellationRequested)
        {
            return VerificationObservation.NotVerified();
        }

        using var verificationCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        verificationCancellation.CancelAfter(deadline - now);
        var verificationToken = verificationCancellation.Token;

        while (_timeProvider.GetUtcNow() < deadline)
        {
            if (verificationToken.IsCancellationRequested)
            {
                return VerificationObservation.NotVerified();
            }

            try
            {
                var result = await observe(
                        deadline,
                        verificationToken)
                    .ConfigureAwait(false);
                if (result.Verified)
                    return result;
            }
            catch (OperationCanceledException)
            {
                return VerificationObservation.NotVerified();
            }
            catch
            {
                // Provider acceptance is already known. Readback errors are
                // evidence gaps, never proof that the mutation failed.
            }

            var remaining =
                deadline - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
                break;

            var delay =
                remaining < _verification.PollInterval
                    ? remaining
                    : _verification.PollInterval;

            try
            {
                await Task.Delay(
                        delay,
                        _timeProvider,
                        verificationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return VerificationObservation.NotVerified();
            }
        }

        return VerificationObservation.NotVerified();
    }

    private static ReadViewOperationContext Observation(
        DateTimeOffset deadlineUtc) =>
        new(
            deadlineUtc,
            maxItems: 1_000,
            maxResponseBytes: 4 * 1024 * 1024);

    private static int? ParsePositiveEvidenceInt(
        IReadOnlyDictionary<string, string>? evidence,
        string key)
    {
        if (evidence is null ||
            !evidence.TryGetValue(key, out var value) ||
            !int.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed <= 0)
        {
            return null;
        }

        return parsed;
    }

    private static bool ReferencesMatch(
        IReadOnlyList<SchemaCanonicalReference> expected,
        IReadOnlyList<RecordSchemaReference> observed)
    {
        if (expected.Count != observed.Count)
            return false;

        var expectedValues = expected
            .Select(item =>
                (item.Name, item.Subject, item.Version))
            .OrderBy(item => item.Name, StringComparer.Ordinal)
            .ThenBy(item => item.Subject, StringComparer.Ordinal)
            .ThenBy(item => item.Version)
            .ToArray();

        var observedValues = observed
            .Select(item =>
                (item.Name, item.Subject, item.Version))
            .OrderBy(item => item.Name, StringComparer.Ordinal)
            .ThenBy(item => item.Subject, StringComparer.Ordinal)
            .ThenBy(item => item.Version)
            .ToArray();

        return expectedValues.SequenceEqual(observedValues);
    }

    private static MutationProviderResult AppliedUnverified(
        string code,
        MutationProviderResult accepted)
    {
        var evidence = MergeEvidence(
            accepted.SafeEvidence);
        evidence["verification.state"] = "inconclusive";

        return new MutationProviderResult(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            evidence);
    }

    private static MutationProviderResult Unknown(
        string code) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code);

    private static Dictionary<string, string> MergeEvidence(
        IReadOnlyDictionary<string, string>? source)
    {
        var result = new Dictionary<string, string>(
            StringComparer.Ordinal);

        if (source is not null)
        {
            foreach (var pair in source)
                result[pair.Key] = pair.Value;
        }

        return result;
    }

    private sealed record VerificationObservation(
        bool Verified,
        int? Version,
        int? SchemaId)
    {
        public static VerificationObservation Observed(
            int? version = null,
            int? schemaId = null) =>
            new(true, version, schemaId);

        public static VerificationObservation NotVerified() =>
            new(false, null, null);
    }
}

public sealed class SchemaCreateExecutionHandler :
    IMutationExecutionHandler
{
    private readonly SchemaMutationExecutionService _service;

    public SchemaCreateExecutionHandler(
        SchemaMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.SchemaCreate;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default) =>
        _service.CreateAsync(
            context,
            cancellationToken);
}

public sealed class SchemaAlterExecutionHandler :
    IMutationExecutionHandler
{
    private readonly SchemaMutationExecutionService _service;

    public SchemaAlterExecutionHandler(
        SchemaMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.SchemaAlter;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != OperationKind)
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "schema_compatibility_operation_mismatch"));
        }

        SchemaCompatibilityCanonicalIntent canonical;
        try
        {
            canonical =
                SchemaMutationCanonicalization.Deserialize<
                    SchemaCompatibilityCanonicalIntent>(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "schema_compatibility_canonical_invalid"));
        }

        return _service.AlterCompatibilityAsync(
            canonical,
            cancellationToken,
            context.ExecutionDeadlineUtc);
    }
}

public sealed class SchemaDeleteExecutionHandler :
    IMutationExecutionHandler
{
    private readonly SchemaMutationExecutionService _service;

    public SchemaDeleteExecutionHandler(
        SchemaMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.SchemaDelete;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != OperationKind)
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "schema_delete_operation_mismatch"));
        }

        SchemaDeleteCanonicalIntent canonical;
        try
        {
            canonical =
                SchemaMutationCanonicalization.Deserialize<
                    SchemaDeleteCanonicalIntent>(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "schema_delete_canonical_invalid"));
        }

        return _service.DeleteAsync(
            canonical,
            cancellationToken,
            context.ExecutionDeadlineUtc);
    }
}
