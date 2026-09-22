using System.Security.Claims;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public enum MutationCommandOutcome
{
    Saved = 1,
    NotFound = 2,
    Unauthenticated = 3,
    Forbidden = 4,
    InvalidRequest = 5,
    InvalidState = 6,
    VersionConflict = 7,
}

public sealed record MutationCommandResult(
    MutationCommandOutcome Outcome,
    MutationOperationSnapshot? Operation = null,
    string? Code = null);

public sealed class MutationCommandService
{
    private readonly IMutationOperationRepository _repository;
    private readonly MutationApprovalAuthorizer _approvalAuthorizer;
    private readonly MutationRequestAuthorizationService _requestAuthorization;

    public MutationCommandService(
        IMutationOperationRepository repository,
        MutationApprovalAuthorizer approvalAuthorizer,
        MutationRequestAuthorizationService requestAuthorization)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _approvalAuthorizer = approvalAuthorizer ?? throw new ArgumentNullException(nameof(approvalAuthorizer));
        _requestAuthorization = requestAuthorization ?? throw new ArgumentNullException(nameof(requestAuthorization));
    }

    public async Task<MutationCommandResult> ConfirmAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        string? previewHash,
        string? typedTargetChallenge,
        CancellationToken cancellationToken = default)
    {
        if (!IsPreviewHashValid(previewHash) ||
            !IsOptionalChallengeValid(typedTargetChallenge))
        {
            return new MutationCommandResult(
                MutationCommandOutcome.InvalidRequest,
                Code: "mutation_confirmation_request_invalid");
        }

        if (!TryGetSession(principal, out var session))
        {
            return new MutationCommandResult(MutationCommandOutcome.Unauthenticated);
        }

        var snapshot = await _repository
            .GetAsync(operationId, cancellationToken)
            .ConfigureAwait(false);
        if (snapshot is null)
        {
            return new MutationCommandResult(MutationCommandOutcome.NotFound);
        }

        var principalId = SecurityAuditPrincipal.FromOperator(session!.Identity);
        if (!string.Equals(
                principalId,
                snapshot.RequesterPrincipalId,
                StringComparison.Ordinal))
        {
            return new MutationCommandResult(MutationCommandOutcome.Forbidden);
        }

        if (_requestAuthorization.AuthorizeForDispatch(principal, snapshot) !=
            KafdeckAuthorizationOutcome.Allowed)
        {
            return new MutationCommandResult(MutationCommandOutcome.Forbidden);
        }

        var operation = MutationOperation.Restore(snapshot);
        try
        {
            operation.Confirm(
                principalId,
                previewHash!,
                DateTimeOffset.UtcNow,
                typedTargetChallenge);
        }
        catch (MutationStateException)
        {
            return new MutationCommandResult(
                MutationCommandOutcome.InvalidState,
                snapshot,
                "mutation_confirmation_rejected");
        }

        return await SaveAsync(
                operation.Snapshot,
                snapshot.Version,
                cancellationToken)
            .ConfigureAwait(false);
    }

    public Task<MutationCommandResult> ApproveAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        string? previewHash,
        CancellationToken cancellationToken = default) =>
        ReviewAsync(
            principal,
            operationId,
            previewHash,
            approve: true,
            cancellationToken);

    public Task<MutationCommandResult> RejectAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        string? previewHash,
        CancellationToken cancellationToken = default) =>
        ReviewAsync(
            principal,
            operationId,
            previewHash,
            approve: false,
            cancellationToken);

    public async Task<MutationCommandResult> CancelAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        string? previewHash,
        CancellationToken cancellationToken = default)
    {
        if (!IsPreviewHashValid(previewHash))
        {
            return new MutationCommandResult(
                MutationCommandOutcome.InvalidRequest,
                Code: "mutation_cancel_request_invalid");
        }

        if (!TryGetSession(principal, out var session))
        {
            return new MutationCommandResult(MutationCommandOutcome.Unauthenticated);
        }

        var snapshot = await _repository
            .GetAsync(operationId, cancellationToken)
            .ConfigureAwait(false);
        if (snapshot is null)
        {
            return new MutationCommandResult(MutationCommandOutcome.NotFound);
        }

        var principalId = SecurityAuditPrincipal.FromOperator(session!.Identity);
        if (!string.Equals(
                principalId,
                snapshot.RequesterPrincipalId,
                StringComparison.Ordinal))
        {
            return new MutationCommandResult(MutationCommandOutcome.Forbidden);
        }

        if (!string.Equals(
                previewHash,
                snapshot.PreviewHash,
                StringComparison.Ordinal))
        {
            return new MutationCommandResult(
                MutationCommandOutcome.InvalidState,
                snapshot,
                "mutation_preview_mismatch");
        }

        var operation = MutationOperation.Restore(snapshot);
        try
        {
            operation.Cancel(DateTimeOffset.UtcNow);
        }
        catch (MutationStateException)
        {
            return new MutationCommandResult(
                MutationCommandOutcome.InvalidState,
                snapshot,
                "mutation_cancel_rejected");
        }

        return await SaveAsync(
                operation.Snapshot,
                snapshot.Version,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<MutationCommandResult> ReviewAsync(
        ClaimsPrincipal? principal,
        Guid operationId,
        string? previewHash,
        bool approve,
        CancellationToken cancellationToken)
    {
        if (!IsPreviewHashValid(previewHash))
        {
            return new MutationCommandResult(
                MutationCommandOutcome.InvalidRequest,
                Code: approve
                    ? "mutation_approval_request_invalid"
                    : "mutation_rejection_request_invalid");
        }

        if (!TryGetSession(principal, out var session))
        {
            return new MutationCommandResult(MutationCommandOutcome.Unauthenticated);
        }

        var snapshot = await _repository
            .GetAsync(operationId, cancellationToken)
            .ConfigureAwait(false);
        if (snapshot is null)
        {
            return new MutationCommandResult(MutationCommandOutcome.NotFound);
        }

        MutationApprovalAuthorizationEvidence evidence;
        try
        {
            evidence = _approvalAuthorizer.Authorize(
                session!.Identity,
                snapshot);
        }
        catch (MutationStateException)
        {
            return new MutationCommandResult(MutationCommandOutcome.Forbidden);
        }

        var operation = MutationOperation.Restore(snapshot);
        try
        {
            if (approve)
            {
                operation.Approve(
                    evidence,
                    previewHash!,
                    DateTimeOffset.UtcNow);
            }
            else
            {
                operation.Reject(
                    evidence,
                    previewHash!,
                    DateTimeOffset.UtcNow);
            }
        }
        catch (MutationStateException)
        {
            return new MutationCommandResult(
                MutationCommandOutcome.InvalidState,
                snapshot,
                approve
                    ? "mutation_approval_rejected"
                    : "mutation_rejection_rejected");
        }

        return await SaveAsync(
                operation.Snapshot,
                snapshot.Version,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<MutationCommandResult> SaveAsync(
        MutationOperationSnapshot snapshot,
        long expectedVersion,
        CancellationToken cancellationToken)
    {
        var saved = await _repository
            .TrySaveAsync(snapshot, expectedVersion, cancellationToken)
            .ConfigureAwait(false);

        return saved.Outcome switch
        {
            MutationSaveOutcome.Saved =>
                new MutationCommandResult(
                    MutationCommandOutcome.Saved,
                    saved.Operation ?? snapshot),
            MutationSaveOutcome.NotFound =>
                new MutationCommandResult(MutationCommandOutcome.NotFound),
            MutationSaveOutcome.VersionConflict =>
                new MutationCommandResult(
                    MutationCommandOutcome.VersionConflict,
                    saved.Operation,
                    "mutation_version_conflict"),
            _ => throw new InvalidOperationException(
                "Unsupported mutation save outcome."),
        };
    }

    private static bool IsPreviewHashValid(string? previewHash) =>
        !string.IsNullOrWhiteSpace(previewHash) &&
        previewHash.Length == 64 &&
        previewHash.All(char.IsAsciiHexDigit);

    private static bool IsOptionalChallengeValid(string? challenge) =>
        challenge is null ||
        (!string.IsNullOrWhiteSpace(challenge) &&
         challenge.Length <= 512 &&
         !challenge.Any(char.IsControl));

    private static bool TryGetSession(
        ClaimsPrincipal? principal,
        out OperatorSessionContext? session) =>
        OperatorSessionContextFactory.TryCreate(principal, out session) &&
        session is not null;
}
