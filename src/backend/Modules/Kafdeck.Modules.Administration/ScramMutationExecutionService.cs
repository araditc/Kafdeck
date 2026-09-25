using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

public sealed record ScramMutationExecutionPolicy
{
    public ScramMutationExecutionPolicy(
        TimeSpan providerCallTimeout,
        TimeSpan verificationTimeout,
        TimeSpan pollInterval)
    {
        if (providerCallTimeout < TimeSpan.FromSeconds(1) ||
            providerCallTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(providerCallTimeout));
        }

        if (verificationTimeout < TimeSpan.FromSeconds(1) ||
            verificationTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(verificationTimeout));
        }

        if (pollInterval < TimeSpan.FromMilliseconds(50) ||
            pollInterval > TimeSpan.FromSeconds(2) ||
            pollInterval >= verificationTimeout)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }

        ProviderCallTimeout = providerCallTimeout;
        VerificationTimeout = verificationTimeout;
        PollInterval = pollInterval;
    }

    public TimeSpan ProviderCallTimeout { get; }
    public TimeSpan VerificationTimeout { get; }
    public TimeSpan PollInterval { get; }

    public static ScramMutationExecutionPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(250));
}

/// <summary>
/// W43 execution coordinator. It is an IMutationExecutionHandler dependency only
/// and does not own approval, idempotency, leases, public routes or worker
/// authority. Provider I/O remains behind the existing common mutation executor.
/// </summary>
public sealed class ScramMutationExecutionService
{
    private const string UpsertStep = "scram-upsert";
    private const string DeleteStep = "scram-delete";

    private readonly IScramMutationPort _mutations;
    private readonly IScramObservationPort _observations;
    private readonly IFleetMutationStateStore _fleetState;
    private readonly IScramEffectAuthorizationGuard _authorization;
    private readonly ScramMutationPreconditionValidator _preconditions;
    private readonly ScramServerPolicy _serverPolicy;
    private readonly ScramMutationExecutionPolicy _executionPolicy;
    private readonly TimeProvider _timeProvider;

    public ScramMutationExecutionService(
        IScramMutationPort mutations,
        IScramObservationPort observations,
        IFleetMutationStateStore fleetState,
        IScramEffectAuthorizationGuard authorization,
        ScramMutationPreconditionValidator preconditions,
        ScramServerPolicy serverPolicy,
        ScramMutationExecutionPolicy? executionPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _mutations = mutations ??
            throw new ArgumentNullException(nameof(mutations));
        _observations = observations ??
            throw new ArgumentNullException(nameof(observations));
        _fleetState = fleetState ??
            throw new ArgumentNullException(nameof(fleetState));
        _authorization = authorization ??
            throw new ArgumentNullException(nameof(authorization));
        _preconditions = preconditions ??
            throw new ArgumentNullException(nameof(preconditions));
        _serverPolicy = serverPolicy ??
            throw new ArgumentNullException(nameof(serverPolicy));
        _executionPolicy = executionPolicy ??
            ScramMutationExecutionPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != MutationOperationKind.ScramAlter)
        {
            return Failed("scram_execution_operation_not_supported");
        }

        ScramMutationPlan plan;
        try
        {
            plan = ScramMutationContract.ValidateBoundOperation(
                context.Operation);
        }
        catch (Exception exception)
            when (exception is ArgumentException or MutationStateException)
        {
            return Failed("scram_execution_canonical_invalid");
        }

        return plan.Mode switch
        {
            ScramMutationMode.Upsert => await ExecuteUpsertAsync(
                    context,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false),
            ScramMutationMode.Delete => await ExecuteDeleteAsync(
                    context,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false),
            _ => Failed("scram_execution_mode_not_supported"),
        };
    }

    private async Task<MutationProviderResult> ExecuteUpsertAsync(
        MutationExecutionContext context,
        ScramMutationPlan plan,
        CancellationToken cancellationToken)
    {
        ReadOnlyMemory<byte> envelope;
        try
        {
            envelope = context.Material.GetRequired(
                ScramMutationPlanner.MaterialName);
        }
        catch (Exception exception)
            when (exception is
                KeyNotFoundException or
                ObjectDisposedException or
                ArgumentException)
        {
            return Failed("scram_upsert_material_missing");
        }

        ScramDecodedCredentialMaterial decoded;
        try
        {
            decoded = ScramCredentialMaterialCodec.Decode(envelope);
        }
        catch (Exception exception)
            when (exception is
                MutationStateException or
                ArgumentException)
        {
            return Failed("scram_upsert_material_invalid");
        }

        using (decoded)
        {
            var expectedContext =
                ScramMutationContract.BuildUpsertMaterialContext(
                    context.Operation,
                    plan);
            if (decoded.Context != expectedContext)
            {
                return Failed("scram_upsert_material_context_mismatch");
            }

            var existing = await FindExistingBlockingEffectAsync(
                    context.Operation,
                    plan,
                    UpsertStep,
                    cancellationToken)
                .ConfigureAwait(false);
            if (existing.Result is not null)
            {
                return existing.Result;
            }

            if (existing.SameEffect)
            {
                // Never redispatch an already-admitted SCRAM upsert. Safe
                // metadata cannot establish password equality after ambiguity.
                return Unknown(
                    "scram_upsert_existing_effect_unresolved");
            }

            var newEffectGate = await ValidateBeforeNewEffectAsync(
                    context.Operation,
                    cancellationToken)
                .ConfigureAwait(false);
            if (newEffectGate is not null)
            {
                return newEffectGate;
            }

            try
            {
                _serverPolicy.ValidateMutation(
                    plan.Credential.User,
                    plan.Credential.Mechanism,
                    plan.Credential.Iterations);
            }
            catch (ScramPolicyException)
            {
                return Failed("scram_upsert_policy_changed");
            }

            var admitted = await AdmitObligationAsync(
                    context.Operation,
                    plan,
                    UpsertStep,
                    cancellationToken)
                .ConfigureAwait(false);
            if (admitted.Result is not null)
            {
                return admitted.Result;
            }

            var obligation = admitted.Obligation!;
            if (admitted.ExistingSameEffect)
            {
                // A concurrent coordinator admitted the same effect after the
                // preflight lookup. Do not redispatch it.
                return Unknown(
                    "scram_upsert_existing_effect_unresolved");
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return await ResolveNonApplicationAsync(
                        obligation,
                        "scram_upsert_cancelled_before_provider",
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            var currentAuthorization = await _authorization
                .ValidateCurrentRequesterAsync(
                    context.Operation,
                    cancellationToken)
                .ConfigureAwait(false);
            if (currentAuthorization.Outcome !=
                MutationPreDispatchGuardOutcome.Allowed)
            {
                return await ResolveNonApplicationAsync(
                        obligation,
                        "scram_upsert_current_authorization_denied",
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            MutationProviderResult provider;
            try
            {
                provider = await _mutations.UpsertAsync(
                        new ScramUpsertMutation(
                            plan.Credential.ClusterId,
                            plan.Credential.User,
                            plan.Credential.Mechanism,
                            plan.Credential.Iterations),
                        decoded.Password,
                        ProviderOperation(context),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                return Unknown("scram_upsert_provider_exception");
            }

            if (provider.ResultKind ==
                MutationExecutionResultKind.FailedDefinitive)
            {
                return await ResolveNonApplicationAsync(
                        obligation,
                        provider.ResultCode,
                        cancellationToken,
                        provider)
                    .ConfigureAwait(false);
            }

            if (provider.ResultKind is
                MutationExecutionResultKind.ExecutionUnknown or
                MutationExecutionResultKind.PartiallyApplied)
            {
                // Even matching metadata after an ambiguous rotation cannot
                // prove which password is installed.
                return Unknown(
                    "scram_upsert_outcome_unknown",
                    provider);
            }

            if (provider.ResultKind is not (
                MutationExecutionResultKind.AppliedUnverified or
                MutationExecutionResultKind.AppliedVerified))
            {
                return Unknown(
                    "scram_upsert_invalid_provider_result",
                    provider);
            }

            var metadataMatches = await VerifyUntilAsync(
                    context,
                    plan,
                    shouldExist: true,
                    cancellationToken)
                .ConfigureAwait(false);

            if (metadataMatches)
            {
                if (!await ResolveTerminalAsync(
                        obligation,
                        "scram-upsert-metadata-observed",
                        plan,
                        cancellationToken)
                    .ConfigureAwait(false))
                {
                    return Unknown(
                        "scram_upsert_obligation_resolution_failed",
                        provider);
                }
            }

            // A successful provider response may be reported as applied, but
            // credential equality is never observable. Keep the parent result
            // explicitly unverified even when safe metadata postconditions match.
            return new MutationProviderResult(
                MutationExecutionResultKind.AppliedUnverified,
                metadataMatches
                    ? "scram_upsert_applied_metadata_observed"
                    : "scram_upsert_applied_metadata_unverified",
                MergeEvidence(
                    provider.SafeEvidence,
                    metadataMatches ? "observed" : "inconclusive"));
        }
    }

    private async Task<MutationProviderResult> ExecuteDeleteAsync(
        MutationExecutionContext context,
        ScramMutationPlan plan,
        CancellationToken cancellationToken)
    {
        var existing = await FindExistingBlockingEffectAsync(
                context.Operation,
                plan,
                DeleteStep,
                cancellationToken)
            .ConfigureAwait(false);
        if (existing.Result is not null)
        {
            return existing.Result;
        }

        if (existing.SameEffect)
        {
            if (await VerifyUntilAsync(
                    context,
                    plan,
                    shouldExist: false,
                    cancellationToken)
                .ConfigureAwait(false) &&
                await ResolveTerminalAsync(
                    existing.Obligation!,
                    "scram-delete-absence-observed",
                    plan,
                    cancellationToken)
                .ConfigureAwait(false))
            {
                return Verified(
                    "scram_delete_recovered_by_observation");
            }

            return Unknown(
                "scram_delete_existing_effect_unresolved");
        }

        var newEffectGate = await ValidateBeforeNewEffectAsync(
                context.Operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (newEffectGate is not null)
        {
            return newEffectGate;
        }

        try
        {
            _serverPolicy.ValidateDelete(
                plan.Credential.User,
                plan.Credential.Mechanism);
        }
        catch (ScramPolicyException)
        {
            return Failed("scram_delete_policy_changed");
        }

        var admitted = await AdmitObligationAsync(
                context.Operation,
                plan,
                DeleteStep,
                cancellationToken)
            .ConfigureAwait(false);
        if (admitted.Result is not null)
        {
            return admitted.Result;
        }

        var obligation = admitted.Obligation!;
        if (admitted.ExistingSameEffect)
        {
            if (await VerifyUntilAsync(
                    context,
                    plan,
                    shouldExist: false,
                    cancellationToken)
                .ConfigureAwait(false) &&
                await ResolveTerminalAsync(
                    obligation,
                    "scram-delete-absence-observed",
                    plan,
                    cancellationToken)
                .ConfigureAwait(false))
            {
                return Verified(
                    "scram_delete_recovered_by_observation");
            }

            return Unknown("scram_delete_existing_effect_unresolved");
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return await ResolveNonApplicationAsync(
                    obligation,
                    "scram_delete_cancelled_before_provider",
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var currentAuthorization = await _authorization
            .ValidateCurrentRequesterAsync(
                context.Operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (currentAuthorization.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return await ResolveNonApplicationAsync(
                    obligation,
                    "scram_delete_current_authorization_denied",
                    cancellationToken)
                .ConfigureAwait(false);
        }

        MutationProviderResult provider;
        try
        {
            provider = await _mutations.DeleteAsync(
                    new ScramDeleteMutation(
                        plan.Credential.ClusterId,
                        plan.Credential.User,
                        plan.Credential.Mechanism),
                    ProviderOperation(context),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            return Unknown("scram_delete_provider_exception");
        }

        if (provider.ResultKind ==
            MutationExecutionResultKind.FailedDefinitive)
        {
            return await ResolveNonApplicationAsync(
                    obligation,
                    provider.ResultCode,
                    cancellationToken,
                    provider)
                .ConfigureAwait(false);
        }

        var absent = await VerifyUntilAsync(
                context,
                plan,
                shouldExist: false,
                cancellationToken)
            .ConfigureAwait(false);
        if (absent &&
            await ResolveTerminalAsync(
                obligation,
                "scram-delete-absence-observed",
                plan,
                cancellationToken)
            .ConfigureAwait(false))
        {
            return Verified(
                "scram_delete_verified",
                provider);
        }

        return provider.ResultKind is
            MutationExecutionResultKind.AppliedVerified or
            MutationExecutionResultKind.AppliedUnverified
            ? new MutationProviderResult(
                MutationExecutionResultKind.AppliedUnverified,
                "scram_delete_applied_unverified",
                MergeEvidence(
                    provider.SafeEvidence,
                    "inconclusive"))
            : Unknown(
                "scram_delete_outcome_unknown",
                provider);
    }

    private async Task<(
        bool SameEffect,
        FleetConflictObligationSnapshot? Obligation,
        MutationProviderResult? Result)> FindExistingBlockingEffectAsync(
        MutationOperationSnapshot operation,
        ScramMutationPlan plan,
        string stepId,
        CancellationToken cancellationToken)
    {
        var conflictKey =
            ScramMutationContract.ConflictResource(plan.Credential);

        FleetConflictObligationSnapshot? existing;
        try
        {
            existing = await _fleetState
                .FindBlockingConflictObligationAsync(
                    conflictKey,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            return (
                false,
                null,
                Unknown("scram_obligation_store_unavailable"));
        }

        if (existing is null)
        {
            return (false, null, null);
        }

        var expectedFingerprint = EffectFingerprint(
            operation.PreviewHash,
            stepId,
            conflictKey);
        if (existing.OperationId == operation.OperationId &&
            string.Equals(
                existing.StepId,
                stepId,
                StringComparison.Ordinal) &&
            string.Equals(
                existing.EffectFingerprint,
                expectedFingerprint,
                StringComparison.Ordinal))
        {
            return (true, existing, null);
        }

        return (
            false,
            existing,
            Failed("scram_effect_conflict_obligation_blocked"));
    }

    private async Task<MutationProviderResult?> ValidateBeforeNewEffectAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        var preconditions = await _preconditions.ValidateAsync(
                operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (preconditions.Outcome !=
            MutationPreDispatchGuardOutcome.Allowed)
        {
            return Failed(preconditions.ResultCode);
        }

        var authorization = await _authorization
            .ValidateCurrentRequesterAsync(
                operation,
                cancellationToken)
            .ConfigureAwait(false);
        return authorization.Outcome ==
               MutationPreDispatchGuardOutcome.Allowed
            ? null
            : Failed("scram_current_authorization_denied");
    }

    private async Task<(
        FleetConflictObligationSnapshot? Obligation,
        bool ExistingSameEffect,
        MutationProviderResult? Result)> AdmitObligationAsync(
        MutationOperationSnapshot operation,
        ScramMutationPlan plan,
        string stepId,
        CancellationToken cancellationToken)
    {
        var conflictKey =
            ScramMutationContract.ConflictResource(plan.Credential);
        var requested = FleetConflictObligation.Create(
            operation.OperationId,
            stepId,
            conflictKey,
            EffectFingerprint(
                operation.PreviewHash,
                stepId,
                conflictKey),
            _timeProvider.GetUtcNow()).Snapshot;

        FleetConflictObligationBatchCreateResult admitted;
        try
        {
            admitted = await _fleetState.CreateConflictObligationsAsync(
                    new[] { requested },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            return (
                null,
                false,
                Unknown("scram_obligation_store_unavailable"));
        }

        if (admitted.Outcome is
            FleetConflictObligationBatchCreateOutcome.Created or
            FleetConflictObligationBatchCreateOutcome.ExistingSameEffects)
        {
            var obligation = admitted.Obligations.SingleOrDefault();
            if (obligation is null)
            {
                return (
                    null,
                    false,
                    Unknown("scram_obligation_admission_incomplete"));
            }

            return (
                obligation,
                admitted.Outcome ==
                    FleetConflictObligationBatchCreateOutcome.ExistingSameEffects,
                null);
        }

        return admitted.Outcome switch
        {
            FleetConflictObligationBatchCreateOutcome.FleetConflictScopeConflict =>
                (null, false, Failed("scram_effect_conflict_obligation_blocked")),
            FleetConflictObligationBatchCreateOutcome.LegacyResourceClaimConflict =>
                (null, false, Failed("scram_effect_legacy_claim_conflict")),
            FleetConflictObligationBatchCreateOutcome.ExistingDifferentEffect =>
                (null, false, Unknown("scram_effect_identity_conflict")),
            FleetConflictObligationBatchCreateOutcome.ParentOperationNotFound =>
                (null, false, Unknown("scram_effect_parent_state_missing")),
            _ => (null, false, Unknown("scram_obligation_admission_failed")),
        };
    }

    private async Task<bool> VerifyUntilAsync(
        MutationExecutionContext context,
        ScramMutationPlan plan,
        bool shouldExist,
        CancellationToken cancellationToken)
    {
        var deadline = Deadline(
            context,
            _executionPolicy.VerificationTimeout);

        while (_timeProvider.GetUtcNow() < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                var observed = await _observations.DescribeUserAsync(
                        plan.Credential.ClusterId,
                        plan.Credential.User,
                        new KafkaOperationContext(deadline),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (observed.IsSuccess && observed.Value is not null)
                {
                    var matching = observed.Value.Any(item =>
                        item.Mechanism == plan.Credential.Mechanism &&
                        item.Iterations == plan.Credential.Iterations);
                    if (matching == shouldExist)
                    {
                        return true;
                    }
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // Observation gaps never become proof of application or
                // non-application.
            }

            var remaining = deadline - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                break;
            }

            var delay = remaining < _executionPolicy.PollInterval
                ? remaining
                : _executionPolicy.PollInterval;
            await Task.Delay(
                    delay,
                    _timeProvider,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        return false;
    }

    private async Task<MutationProviderResult> ResolveNonApplicationAsync(
        FleetConflictObligationSnapshot snapshot,
        string resultCode,
        CancellationToken cancellationToken,
        MutationProviderResult? source = null)
    {
        var resolved = await ResolveAsync(
                snapshot,
                FleetUncertaintyDispositionOutcome.ObservedNonApplication,
                Sha256("scram-nonapplication\n" + snapshot.EffectFingerprint),
                cancellationToken)
            .ConfigureAwait(false);

        return resolved
            ? source ?? Failed(resultCode)
            : Unknown(
                "scram_nonapplication_resolution_failed",
                source);
    }

    private async Task<bool> ResolveTerminalAsync(
        FleetConflictObligationSnapshot snapshot,
        string state,
        ScramMutationPlan plan,
        CancellationToken cancellationToken) =>
        await ResolveAsync(
                snapshot,
                FleetUncertaintyDispositionOutcome.ObservedTerminalEffect,
                Sha256(
                    string.Join(
                        '\n',
                        state,
                        ScramMutationContract.ConflictResource(plan.Credential),
                        ((int)plan.Credential.Mechanism).ToString(
                            CultureInfo.InvariantCulture),
                        plan.Credential.Iterations.ToString(
                            CultureInfo.InvariantCulture))),
                cancellationToken)
            .ConfigureAwait(false);

    private async Task<bool> ResolveAsync(
        FleetConflictObligationSnapshot snapshot,
        FleetUncertaintyDispositionOutcome outcome,
        string evidenceHash,
        CancellationToken cancellationToken)
    {
        if (snapshot.State is
            FleetConflictObligationState.ObservedNonApplication or
            FleetConflictObligationState.ObservedTerminalEffect)
        {
            return true;
        }

        var obligation = FleetConflictObligation.Restore(snapshot);
        try
        {
            obligation.ApplyObservedResolution(
                outcome,
                evidenceHash,
                _timeProvider.GetUtcNow());
        }
        catch (MutationStateException)
        {
            return false;
        }

        var saved = await _fleetState.TrySaveConflictObligationAsync(
                obligation.Snapshot,
                snapshot.Version,
                cancellationToken)
            .ConfigureAwait(false);
        return saved.Outcome ==
                   FleetConflictObligationSaveOutcome.Saved ||
               (saved.Obligation is not null &&
                saved.Obligation.State == obligation.Snapshot.State &&
                string.Equals(
                    saved.Obligation.SafeResolutionEvidenceHash,
                    evidenceHash,
                    StringComparison.Ordinal));
    }

    private KafkaOperationContext ProviderOperation(
        MutationExecutionContext context) =>
        new(Deadline(
            context,
            _executionPolicy.ProviderCallTimeout));

    private DateTimeOffset Deadline(
        MutationExecutionContext context,
        TimeSpan localBudget)
    {
        var local = _timeProvider.GetUtcNow().Add(localBudget);
        return context.ExecutionDeadlineUtc is { } outer && outer < local
            ? outer
            : local;
    }

    private static string EffectFingerprint(
        string previewHash,
        string stepId,
        string conflictKey) =>
        Sha256(
            string.Join(
                '\n',
                previewHash,
                stepId,
                conflictKey));

    private static string Sha256(string value) =>
        "sha256:" +
        Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private static MutationProviderResult Verified(
        string code,
        MutationProviderResult? source = null) =>
        new(
            MutationExecutionResultKind.AppliedVerified,
            code,
            MergeEvidence(source?.SafeEvidence, "observed"));

    private static MutationProviderResult Failed(string code) =>
        new(
            MutationExecutionResultKind.FailedDefinitive,
            code);

    private static MutationProviderResult Unknown(
        string code,
        MutationProviderResult? source = null) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code,
            MergeEvidence(source?.SafeEvidence, "inconclusive"));

    private static IReadOnlyDictionary<string, string> MergeEvidence(
        IReadOnlyDictionary<string, string>? source,
        string verificationState)
    {
        var result = new Dictionary<string, string>(
            StringComparer.Ordinal);
        if (source is not null)
        {
            foreach (var pair in source)
            {
                result[pair.Key] = pair.Value;
            }
        }

        result["verification.state"] = verificationState;
        return result;
    }
}

public sealed class ScramAlterExecutionHandler : IMutationExecutionHandler
{
    private readonly ScramMutationExecutionService _service;

    public ScramAlterExecutionHandler(
        ScramMutationExecutionService service)
    {
        _service = service ??
            throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.ScramAlter;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default) =>
        _service.ExecuteAsync(context, cancellationToken);
}
