using System.Security.Claims;
using Kafdeck.Core.Security;
using Kafdeck.Core.Governance;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public enum MutationAdmissionOutcome
{
    Created = 1,
    ExistingSameIntent = 2,
    IdempotencyConflict = 3,
    Unauthenticated = 4,
    Forbidden = 5,
    InvalidRequest = 6,
    Unavailable = 7,
}

public sealed record MutationAdmissionResult(
    MutationAdmissionOutcome Outcome,
    MutationOperationSnapshot? Operation = null,
    string? Code = null);

public sealed class MutationAdmissionService
{
    public const string PolicyVersion = "v0.5-rfc0005-w39";

    private readonly IMutationOperationRepository _repository;
    private readonly MutationRequestAuthorizationService _authorization;
    private readonly MutationOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly IGovernancePolicyStore? _governance;

    public MutationAdmissionService(
        IMutationOperationRepository repository,
        MutationRequestAuthorizationService authorization,
        KafdeckOptions options,
        TimeProvider? timeProvider = null,
        IGovernancePolicyStore? governance = null)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _authorization = authorization ?? throw new ArgumentNullException(nameof(authorization));
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Administration?.Mutations ??
            throw new InvalidOperationException(
                "Mutation admission service requires configured mutation options.");
        _timeProvider = timeProvider ?? TimeProvider.System;
        _governance = governance;
    }

    public Task<MutationAdmissionResult> AdmitAsync(
        ClaimsPrincipal? principal,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        string? idempotencyKey,
        CancellationToken cancellationToken = default) =>
        AdmitAsync(
            principal,
            Guid.Empty,
            intent,
            risk,
            idempotencyKey,
            cancellationToken);

    public async Task<MutationAdmissionResult> AdmitAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        string? idempotencyKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentNullException.ThrowIfNull(risk);

        if (!MutationActor.TryCreate(principal, out var actor) || actor is null)
        {
            return new MutationAdmissionResult(MutationAdmissionOutcome.Unauthenticated);
        }

        if (string.IsNullOrWhiteSpace(idempotencyKey))
        {
            return new MutationAdmissionResult(
                MutationAdmissionOutcome.InvalidRequest,
                Code: "idempotency_key_required");
        }

        IReadOnlyList<MutationAuthorizationTarget> targets;
        try
        {
            _ = MutationIdempotency.HashKey(idempotencyKey);
            targets = MutationAuthorizationRequirements.Normalize(
                intent.Kind,
                intent.ClusterId,
                intent.AuthorizationTargets,
                intent.ResourceKeys);
        }
        catch (Exception exception)
            when (exception is ArgumentException or MutationStateException)
        {
            return new MutationAdmissionResult(
                MutationAdmissionOutcome.InvalidRequest,
                Code: "mutation_admission_invalid");
        }

        var requesterPrincipalId =
            actor.PrincipalId;
        var effectiveOperationId = operationId;
        try
        {
            if (intent.Kind == MutationOperationKind.ScramAlter)
            {
                var derivedOperationId =
                    MutationIdempotency.DeriveOperationId(
                        requesterPrincipalId,
                        intent.ClusterId,
                        intent.Kind,
                        idempotencyKey);
                if (operationId != Guid.Empty &&
                    operationId != derivedOperationId)
                {
                    return new MutationAdmissionResult(
                        MutationAdmissionOutcome.InvalidRequest,
                        Code: "mutation_operation_identity_mismatch");
                }

                effectiveOperationId = derivedOperationId;
                _ = ScramMutationContract.ValidateAdmissionIntent(
                    intent,
                    effectiveOperationId,
                    requesterPrincipalId,
                    PolicyVersion);
            }
            else if (effectiveOperationId == Guid.Empty)
            {
                effectiveOperationId = Guid.NewGuid();
            }
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                MutationStateException or
                OverflowException)
        {
            return new MutationAdmissionResult(
                MutationAdmissionOutcome.InvalidRequest,
                Code: "mutation_admission_invalid");
        }

        var admissionTargets = intent.Kind == MutationOperationKind.DeclarativeNotificationSubscription
            ? targets.Where(t => t.Action is Kafdeck.Core.Security.AuthorizationAction.DeclarativeRead or
                Kafdeck.Core.Security.AuthorizationAction.DeclarativePlan or Kafdeck.Core.Security.AuthorizationAction.DeclarativeAdopt or
                Kafdeck.Core.Security.AuthorizationAction.NotificationRead).ToArray() : targets;
        var authorization = intent.Kind is MutationOperationKind.FleetUncertaintyDisposition or MutationOperationKind.DeclarativeNotificationSubscription or MutationOperationKind.GovernancePolicyVersion or MutationOperationKind.GovernancePolicyActivation
            ? _authorization.AuthorizeProofTargets(principal, admissionTargets)
            : _authorization.AuthorizeTargets(principal, targets);
        if (authorization == KafdeckAuthorizationOutcome.Unauthenticated)
        {
            return new MutationAdmissionResult(MutationAdmissionOutcome.Unauthenticated);
        }

        if (authorization != KafdeckAuthorizationOutcome.Allowed)
        {
            return new MutationAdmissionResult(MutationAdmissionOutcome.Forbidden);
        }

        // Bind every physical authorization scope, including cross-cluster transfer destinations.
        // Never-used disabled governance changes neither legacy material nor its fingerprint.
        if (_governance is not null)
        {
            try
            {
                var conditions = (intent.Preconditions ?? Array.Empty<MutationPrecondition>()).ToList();
                if (conditions.Any(p => p.Key.StartsWith("v09.governance:", StringComparison.Ordinal)))
                    return new(MutationAdmissionOutcome.InvalidRequest, Code: "GovernancePreconditionReserved");
                foreach (var cluster in targets.Select(t => t.ClusterId).Append(intent.ClusterId).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal))
                {
                    var current = await _governance.GetStateAsync(cluster, cancellationToken).ConfigureAwait(false);
                    if (current.Digest is not null) conditions.Add(new("v09.governance:" + cluster, current.Digest));
                }
                intent = intent with { Preconditions = conditions };
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception) { return new(MutationAdmissionOutcome.Unavailable, Code: "GovernanceUnavailable"); }
        }
        var now = _timeProvider.GetUtcNow();
        MutationOperation operation;
        try
        {
            operation = MutationOperation.CreatePreview(
                effectiveOperationId,
                requesterPrincipalId,
                intent with { AuthorizationTargets = targets },
                risk,
                _authorization.PolicyVersionFor(principal, intent.Kind),
                now.Add(_options.PreviewTtl),
                now,
                idempotencyKey);
            operation.OpenForConfirmation(now);
        }
        catch (Exception exception)
            when (exception is ArgumentException or MutationStateException or OverflowException)
        {
            return new MutationAdmissionResult(
                MutationAdmissionOutcome.InvalidRequest,
                Code: "mutation_admission_invalid");
        }

        var created = await _repository
            .CreateAsync(operation.Snapshot, cancellationToken)
            .ConfigureAwait(false);

        return created.Outcome switch
        {
            MutationCreateOutcome.Created =>
                new MutationAdmissionResult(
                    MutationAdmissionOutcome.Created,
                    created.Operation),
            MutationCreateOutcome.ExistingSameIntent =>
                new MutationAdmissionResult(
                    MutationAdmissionOutcome.ExistingSameIntent,
                    created.Operation),
            MutationCreateOutcome.IdempotencyConflict =>
                new MutationAdmissionResult(
                    MutationAdmissionOutcome.IdempotencyConflict,
                    created.Operation,
                    "idempotency_key_conflict"),
            _ => throw new InvalidOperationException(
                "Unsupported mutation create outcome."),
        };
    }
}
