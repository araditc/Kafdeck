using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Kafka;

namespace Kafdeck.Modules.Administration;

public sealed record AclMutationExecutionPolicy
{
    public AclMutationExecutionPolicy(
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

    public static AclMutationExecutionPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(250));
}

/// <summary>
/// W42 ACL execution coordinator. It runs only inside the existing mutation
/// executor and does not own approval, idempotency, leases or worker authority.
/// No production handler registration is introduced by this workstream.
/// </summary>
public sealed class AclMutationExecutionService
{
    private const string CreateStep = "acl-create";
    private const string RemoveStep = "acl-remove";

    private readonly IAclMutationPort _mutations;
    private readonly IAclObservationPort _observations;
    private readonly IFleetMutationStateStore _fleetState;
    private readonly IAclEffectAuthorizationGuard _authorization;
    private readonly AclMutationPreconditionValidator _preconditions;
    private readonly AclServerPolicy _serverPolicy;
    private readonly AclMutationExecutionPolicy _executionPolicy;
    private readonly TimeProvider _timeProvider;

    public AclMutationExecutionService(
        IAclMutationPort mutations,
        IAclObservationPort observations,
        IFleetMutationStateStore fleetState,
        IAclEffectAuthorizationGuard authorization,
        AclMutationPreconditionValidator preconditions,
        AclServerPolicy serverPolicy,
        AclMutationExecutionPolicy? executionPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _mutations = mutations ?? throw new ArgumentNullException(nameof(mutations));
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _fleetState = fleetState ?? throw new ArgumentNullException(nameof(fleetState));
        _authorization = authorization ?? throw new ArgumentNullException(nameof(authorization));
        _preconditions = preconditions ?? throw new ArgumentNullException(nameof(preconditions));
        _serverPolicy = serverPolicy ?? throw new ArgumentNullException(nameof(serverPolicy));
        _executionPolicy = executionPolicy ?? AclMutationExecutionPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != MutationOperationKind.AclAlter)
        {
            return Failed("acl_execution_operation_not_supported");
        }

        AclMutationPlan plan;
        try
        {
            plan = AclMutationPolicy.DeserializePlan(
                context.Operation.CanonicalIntent);
        }
        catch (Exception exception) when (
            exception is ArgumentException or MutationStateException)
        {
            return Failed("acl_execution_canonical_invalid");
        }

        var initialPrecondition = await _preconditions.ValidateAsync(
                context.Operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (initialPrecondition.Outcome != MutationPreDispatchGuardOutcome.Allowed)
        {
            return Failed(initialPrecondition.ResultCode);
        }

        return plan.Mode switch
        {
            AclMutationMode.Create => await ExecuteStandaloneCreateAsync(
                    context,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false),
            AclMutationMode.Remove => await ExecuteStandaloneRemoveAsync(
                    context,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false),
            AclMutationMode.Replace => await ExecuteReplaceAsync(
                    context,
                    plan,
                    cancellationToken)
                .ConfigureAwait(false),
            _ => Failed("acl_execution_mode_not_supported"),
        };
    }

    private async Task<MutationProviderResult> ExecuteStandaloneCreateAsync(
        MutationExecutionContext context,
        AclMutationPlan plan,
        CancellationToken cancellationToken)
    {
        if (plan.CreateBindings.Count == 0 || plan.RemoveBindings.Count != 0)
        {
            return Failed("acl_create_plan_invalid");
        }

        return await ExecuteEffectAsync(
                context,
                plan,
                isCreate: true,
                plan.CreateBindings,
                verify: operation => ExactTargetsMatchAsync(
                    plan.ClusterId,
                    plan.CreateBindings,
                    shouldExist: true,
                    operation,
                    cancellationToken),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<MutationProviderResult> ExecuteStandaloneRemoveAsync(
        MutationExecutionContext context,
        AclMutationPlan plan,
        CancellationToken cancellationToken)
    {
        if (plan.RemoveBindings.Count == 0 ||
            plan.CreateBindings.Count != 0 ||
            plan.SourceFilter is null)
        {
            return Failed("acl_remove_plan_invalid");
        }

        return await ExecuteEffectAsync(
                context,
                plan,
                isCreate: false,
                plan.RemoveBindings,
                verify: operation => ExactTargetsMatchAsync(
                    plan.ClusterId,
                    plan.RemoveBindings,
                    shouldExist: false,
                    operation,
                    cancellationToken),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<MutationProviderResult> ExecuteReplaceAsync(
        MutationExecutionContext context,
        AclMutationPlan plan,
        CancellationToken cancellationToken)
    {
        if (plan.SourceFilter is null ||
            (plan.CreateBindings.Count == 0 && plan.RemoveBindings.Count == 0))
        {
            return Failed("acl_replace_plan_invalid");
        }

        var initial = await ObserveInventoryOnceAsync(
                context,
                plan.ClusterId,
                plan.SourceFilter,
                cancellationToken)
            .ConfigureAwait(false);
        if (initial is null ||
            !string.Equals(
                AclMutationPolicy.FingerprintBindings(initial),
                plan.ObservedBindingSetFingerprint,
                StringComparison.Ordinal))
        {
            return Failed("acl_replace_precondition_changed_before_effect");
        }

        var current = initial;
        if (plan.RemoveBindings.Count > 0)
        {
            var expectedIntermediate = current
                .Except(plan.RemoveBindings)
                .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
                .ToArray();

            var remove = await ExecuteEffectAsync(
                    context,
                    plan,
                    isCreate: false,
                    plan.RemoveBindings,
                    verify: operation => InventoryMatchesAsync(
                        plan.ClusterId,
                        plan.SourceFilter,
                        expectedIntermediate,
                        operation,
                        cancellationToken),
                    cancellationToken)
                .ConfigureAwait(false);

            if (remove.ResultKind != MutationExecutionResultKind.AppliedVerified)
            {
                return remove;
            }

            current = expectedIntermediate;
        }

        if (plan.CreateBindings.Count == 0)
        {
            return VerifiedResult(
                "acl_replace_verified",
                current.Count,
                source: null);
        }

        // Close the race between remove verification and the next external
        // effect. The source inventory must still equal the server-derived
        // intermediate state before create obligations are admitted.
        var stillIntermediate = await ObserveInventoryOnceAsync(
                context,
                plan.ClusterId,
                plan.SourceFilter,
                cancellationToken)
            .ConfigureAwait(false);
        if (stillIntermediate is null ||
            !SameBindings(stillIntermediate, current))
        {
            return plan.RemoveBindings.Count == 0
                ? Failed("acl_replace_intermediate_state_changed")
                : PartiallyAppliedResult(
                    "acl_replace_intermediate_state_changed",
                    plan.RemoveBindings.Count);
        }

        var create = await ExecuteEffectAsync(
                context,
                plan,
                isCreate: true,
                plan.CreateBindings,
                verify: operation => InventoryMatchesAsync(
                    plan.ClusterId,
                    plan.SourceFilter,
                    current
                        .Concat(plan.CreateBindings)
                        .Distinct()
                        .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
                        .ToArray(),
                    operation,
                    cancellationToken),
                cancellationToken)
            .ConfigureAwait(false);

        return create.ResultKind switch
        {
            MutationExecutionResultKind.AppliedVerified =>
                VerifiedResult(
                    "acl_replace_verified",
                    plan.RemoveBindings.Count + plan.CreateBindings.Count,
                    create),
            MutationExecutionResultKind.FailedDefinitive
                when plan.RemoveBindings.Count == 0 =>
                create,
            MutationExecutionResultKind.FailedDefinitive =>
                PartiallyAppliedResult(
                    "acl_replace_create_not_applied",
                    plan.RemoveBindings.Count),
            MutationExecutionResultKind.PartiallyApplied or
            MutationExecutionResultKind.AppliedUnverified or
            MutationExecutionResultKind.ExecutionUnknown =>
                UnknownResult(
                    "acl_replace_create_outcome_unknown",
                    create),
            _ => UnknownResult(
                "acl_replace_invalid_create_result",
                create),
        };
    }

    private async Task<MutationProviderResult> ExecuteEffectAsync(
        MutationExecutionContext context,
        AclMutationPlan plan,
        bool isCreate,
        IReadOnlyList<KafkaAclBinding> bindings,
        Func<KafkaOperationContext, Task<bool>> verify,
        CancellationToken cancellationToken)
    {
        var authorization = await _authorization.ValidateCurrentRequesterAsync(
                context.Operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (authorization.Outcome != MutationPreDispatchGuardOutcome.Allowed)
        {
            return Failed(
                isCreate
                    ? "acl_create_current_authorization_denied"
                    : "acl_remove_current_authorization_denied");
        }

        IReadOnlyList<KafkaAclBinding> normalized;
        try
        {
            normalized = isCreate
                ? AclMutationPolicy.ValidateCreates(bindings, _serverPolicy)
                : AclMutationPolicy.ValidateRemovals(bindings, _serverPolicy);
        }
        catch (AclPolicyException)
        {
            return Failed(
                isCreate
                    ? "acl_create_policy_changed"
                    : "acl_remove_policy_changed");
        }

        var stepId = isCreate ? CreateStep : RemoveStep;
        var requestedObligations = normalized
            .Select(binding => FleetConflictObligation.Create(
                context.Operation.OperationId,
                stepId,
                AclBindingIdentity.FleetConflictKey(
                    plan.ClusterId,
                    binding),
                EffectFingerprint(
                    context.Operation.PreviewHash,
                    stepId,
                    binding),
                _timeProvider.GetUtcNow()).Snapshot)
            .ToArray();

        FleetConflictObligationBatchCreateResult admitted;
        try
        {
            admitted = await _fleetState.CreateConflictObligationsAsync(
                    requestedObligations,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            return UnknownResult(
                isCreate
                    ? "acl_create_obligation_store_unavailable"
                    : "acl_remove_obligation_store_unavailable");
        }

        if (admitted.Outcome ==
            FleetConflictObligationBatchCreateOutcome.ExistingSameEffects)
        {
            // The same parent/effect was already admitted. Never redispatch it.
            // Only bounded observation may recover a stronger terminal result.
            if (await VerifyUntilAsync(
                    context,
                    verify,
                    cancellationToken)
                .ConfigureAwait(false))
            {
                return await ResolveAppliedAsync(
                        admitted.Obligations,
                        isCreate,
                        normalized,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            return UnknownResult(
                isCreate
                    ? "acl_create_existing_effect_unresolved"
                    : "acl_remove_existing_effect_unresolved");
        }

        if (admitted.Outcome !=
            FleetConflictObligationBatchCreateOutcome.Created)
        {
            return admitted.Outcome switch
            {
                FleetConflictObligationBatchCreateOutcome.FleetConflictScopeConflict =>
                    Failed("acl_effect_conflict_obligation_blocked"),
                FleetConflictObligationBatchCreateOutcome.ParentOperationNotFound =>
                    UnknownResult("acl_effect_parent_state_missing"),
                FleetConflictObligationBatchCreateOutcome.LegacyResourceClaimConflict =>
                    Failed("acl_effect_legacy_claim_conflict"),
                FleetConflictObligationBatchCreateOutcome.ExistingDifferentEffect =>
                    UnknownResult("acl_effect_identity_conflict"),
                _ => UnknownResult("acl_effect_obligation_admission_failed"),
            };
        }

        MutationProviderResult provider;
        try
        {
            var operation = ProviderOperation(context);
            provider = isCreate
                ? await _mutations.CreateAsync(
                        new AclCreateMutation(plan.ClusterId, normalized),
                        operation,
                        cancellationToken)
                    .ConfigureAwait(false)
                : await _mutations.RemoveAsync(
                        new AclRemoveMutation(plan.ClusterId, normalized),
                        operation,
                        cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            // The adapter call crossed the durable dispatch obligation boundary.
            // Unclassified exceptions are not proof of non-application.
            return UnknownResult(
                isCreate
                    ? "acl_create_provider_exception"
                    : "acl_remove_provider_exception");
        }

        if (provider.ResultKind == MutationExecutionResultKind.FailedDefinitive)
        {
            return await ResolveNonApplicationAsync(
                    admitted.Obligations,
                    isCreate,
                    normalized,
                    provider,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        if (provider.ResultKind == MutationExecutionResultKind.FailedBeforeDispatch)
        {
            return UnknownResult(
                isCreate
                    ? "acl_create_invalid_provider_result"
                    : "acl_remove_invalid_provider_result",
                provider);
        }

        if (await VerifyUntilAsync(
                context,
                verify,
                cancellationToken)
            .ConfigureAwait(false))
        {
            return await ResolveAppliedAsync(
                    admitted.Obligations,
                    isCreate,
                    normalized,
                    cancellationToken,
                    provider)
                .ConfigureAwait(false);
        }

        // v0.6 does not promote provider acceptance or partial provider reports
        // to a terminal aggregate state without bounded readback proof.
        return UnknownResult(
            isCreate
                ? "acl_create_verification_inconclusive"
                : "acl_remove_verification_inconclusive",
            provider);
    }

    private async Task<MutationProviderResult> ResolveAppliedAsync(
        IReadOnlyList<FleetConflictObligationSnapshot> obligations,
        bool isCreate,
        IReadOnlyList<KafkaAclBinding> bindings,
        CancellationToken cancellationToken,
        MutationProviderResult? source = null)
    {
        var evidenceHash = ResolutionEvidenceHash(
            isCreate ? "create-present" : "remove-absent",
            bindings);

        if (!await ResolveObligationsAsync(
                obligations,
                FleetUncertaintyDispositionOutcome.ObservedTerminalEffect,
                evidenceHash,
                cancellationToken)
            .ConfigureAwait(false))
        {
            return UnknownResult(
                isCreate
                    ? "acl_create_obligation_resolution_failed"
                    : "acl_remove_obligation_resolution_failed",
                source,
                verificationState: "observed");
        }

        return VerifiedResult(
            isCreate ? "acl_create_verified" : "acl_remove_verified",
            bindings.Count,
            source);
    }

    private async Task<MutationProviderResult> ResolveNonApplicationAsync(
        IReadOnlyList<FleetConflictObligationSnapshot> obligations,
        bool isCreate,
        IReadOnlyList<KafkaAclBinding> bindings,
        MutationProviderResult source,
        CancellationToken cancellationToken)
    {
        var evidenceHash = ResolutionEvidenceHash(
            isCreate ? "create-not-applied" : "remove-not-applied",
            bindings);

        if (!await ResolveObligationsAsync(
                obligations,
                FleetUncertaintyDispositionOutcome.ObservedNonApplication,
                evidenceHash,
                cancellationToken)
            .ConfigureAwait(false))
        {
            return UnknownResult(
                isCreate
                    ? "acl_create_nonapplication_resolution_failed"
                    : "acl_remove_nonapplication_resolution_failed",
                source);
        }

        return source;
    }

    private async Task<bool> ResolveObligationsAsync(
        IReadOnlyList<FleetConflictObligationSnapshot> obligations,
        FleetUncertaintyDispositionOutcome outcome,
        string evidenceHash,
        CancellationToken cancellationToken)
    {
        foreach (var snapshot in obligations)
        {
            if (snapshot.State is
                FleetConflictObligationState.ObservedNonApplication or
                FleetConflictObligationState.ObservedTerminalEffect)
            {
                continue;
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

            if (saved.Outcome == FleetConflictObligationSaveOutcome.Saved)
            {
                continue;
            }

            if (saved.Obligation is not null &&
                saved.Obligation.State == obligation.Snapshot.State &&
                string.Equals(
                    saved.Obligation.SafeResolutionEvidenceHash,
                    evidenceHash,
                    StringComparison.Ordinal))
            {
                continue;
            }

            return false;
        }

        return true;
    }

    private async Task<IReadOnlyList<KafkaAclBinding>?> ObserveInventoryOnceAsync(
        MutationExecutionContext context,
        string clusterId,
        KafkaAclBindingFilter filter,
        CancellationToken cancellationToken)
    {
        var operation = ProviderOperation(context);
        var observed = await _observations.DescribeAsync(
                clusterId,
                AclMutationPolicy.NormalizeMutationFilter(filter),
                operation,
                cancellationToken)
            .ConfigureAwait(false);

        return observed.IsSuccess && observed.Value is not null
            ? observed.Value
                .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
                .ToArray()
            : null;
    }

    private async Task<bool> InventoryMatchesAsync(
        string clusterId,
        KafkaAclBindingFilter filter,
        IReadOnlyList<KafkaAclBinding> expected,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        var observed = await _observations.DescribeAsync(
                clusterId,
                AclMutationPolicy.NormalizeMutationFilter(filter),
                operation,
                cancellationToken)
            .ConfigureAwait(false);
        return observed.IsSuccess &&
               observed.Value is not null &&
               SameBindings(observed.Value, expected);
    }

    private async Task<bool> ExactTargetsMatchAsync(
        string clusterId,
        IReadOnlyList<KafkaAclBinding> expected,
        bool shouldExist,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        foreach (var binding in expected)
        {
            var observed = await _observations.DescribeAsync(
                    clusterId,
                    AclMutationPolicy.ExactFilter(binding),
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!observed.IsSuccess || observed.Value is null)
            {
                return false;
            }

            var contains = observed.Value.Any(item => item == binding);
            if (contains != shouldExist)
            {
                return false;
            }
        }

        return true;
    }

    private async Task<bool> VerifyUntilAsync(
        MutationExecutionContext context,
        Func<KafkaOperationContext, Task<bool>> verify,
        CancellationToken cancellationToken)
    {
        var deadline = Deadline(
            context,
            _executionPolicy.VerificationTimeout);

        while (_timeProvider.GetUtcNow() < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var operation = new KafkaOperationContext(deadline);
            try
            {
                if (await verify(operation).ConfigureAwait(false))
                {
                    return true;
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // Verification gaps never prove the provider effect absent.
            }

            var remaining = deadline - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                break;
            }

            var delay = remaining < _executionPolicy.PollInterval
                ? remaining
                : _executionPolicy.PollInterval;
            await Task.Delay(delay, _timeProvider, cancellationToken)
                .ConfigureAwait(false);
        }

        return false;
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

    private static bool SameBindings(
        IEnumerable<KafkaAclBinding> left,
        IEnumerable<KafkaAclBinding> right) =>
        left.OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
            .SequenceEqual(
                right.OrderBy(
                    AclBindingIdentity.Canonical,
                    StringComparer.Ordinal));

    private static string EffectFingerprint(
        string previewHash,
        string stepId,
        KafkaAclBinding binding) =>
        Sha256(
            string.Join(
                '\n',
                previewHash,
                stepId,
                AclBindingIdentity.Hash(binding)));

    private static string ResolutionEvidenceHash(
        string state,
        IReadOnlyList<KafkaAclBinding> bindings) =>
        Sha256(
            string.Join(
                '\n',
                new[] { state }
                    .Concat(bindings
                        .Select(AclBindingIdentity.Hash)
                        .OrderBy(value => value, StringComparer.Ordinal))));

    private static string Sha256(string value) =>
        "sha256:" +
        Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private static MutationProviderResult VerifiedResult(
        string code,
        int acknowledgedCount,
        MutationProviderResult? source)
    {
        var evidence = MergeEvidence(source?.SafeEvidence);
        evidence["acknowledged.count"] = acknowledgedCount.ToString(
            CultureInfo.InvariantCulture);
        evidence["verification.state"] = "observed";
        return new MutationProviderResult(
            MutationExecutionResultKind.AppliedVerified,
            code,
            evidence);
    }

    private static MutationProviderResult PartiallyAppliedResult(
        string code,
        int acknowledgedCount) =>
        new(
            MutationExecutionResultKind.PartiallyApplied,
            code,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["acknowledged.count"] = acknowledgedCount.ToString(
                    CultureInfo.InvariantCulture),
                ["verification.state"] = "observed",
            });

    private static MutationProviderResult Failed(string code) =>
        new(
            MutationExecutionResultKind.FailedDefinitive,
            code);

    private static MutationProviderResult UnknownResult(
        string code,
        MutationProviderResult? source = null,
        string verificationState = "inconclusive")
    {
        var evidence = MergeEvidence(source?.SafeEvidence);
        evidence["verification.state"] = verificationState;
        return new MutationProviderResult(
            MutationExecutionResultKind.ExecutionUnknown,
            code,
            evidence);
    }

    private static Dictionary<string, string> MergeEvidence(
        IReadOnlyDictionary<string, string>? source)
    {
        var evidence = new Dictionary<string, string>(StringComparer.Ordinal);
        if (source is null)
        {
            return evidence;
        }

        foreach (var pair in source)
        {
            evidence[pair.Key] = pair.Value;
        }

        return evidence;
    }
}

public sealed class AclAlterExecutionHandler : IMutationExecutionHandler
{
    private readonly AclMutationExecutionService _service;

    public AclAlterExecutionHandler(AclMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.AclAlter;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default) =>
        _service.ExecuteAsync(context, cancellationToken);
}
