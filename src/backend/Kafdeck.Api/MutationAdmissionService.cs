using System.Security.Claims;
using Kafdeck.Core.Security;
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

    public MutationAdmissionService(
        IMutationOperationRepository repository,
        MutationRequestAuthorizationService authorization,
        KafdeckOptions options,
        TimeProvider? timeProvider = null)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _authorization = authorization ?? throw new ArgumentNullException(nameof(authorization));
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Administration?.Mutations ??
            throw new InvalidOperationException(
                "Mutation admission service requires configured mutation options.");
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public Task<MutationAdmissionResult> AdmitAsync(
        ClaimsPrincipal? principal,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        string? idempotencyKey,
        CancellationToken cancellationToken = default) =>
        AdmitAsync(
            principal,
            Guid.NewGuid(),
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

        if (!OperatorSessionContextFactory.TryCreate(principal, out var session) ||
            session is null)
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
            targets = MutationAuthorization.NormalizeTargets(
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

        var authorization = _authorization.AuthorizeTargets(
            principal,
            targets);
        if (authorization == KafdeckAuthorizationOutcome.Unauthenticated)
        {
            return new MutationAdmissionResult(MutationAdmissionOutcome.Unauthenticated);
        }

        if (authorization != KafdeckAuthorizationOutcome.Allowed)
        {
            return new MutationAdmissionResult(MutationAdmissionOutcome.Forbidden);
        }

        var now = _timeProvider.GetUtcNow();
        MutationOperation operation;
        try
        {
            operation = MutationOperation.CreatePreview(
                operationId,
                SecurityAuditPrincipal.FromOperator(session.Identity),
                intent with { AuthorizationTargets = targets },
                risk,
                PolicyVersion,
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
