namespace Kafdeck.Modules.Administration;

public sealed class MutationOperation
{
    private MutationOperation(MutationOperationSnapshot snapshot)
    {
        Snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
    }

    public MutationOperationSnapshot Snapshot { get; private set; }

    public static MutationOperation CreatePreview(
        string requesterPrincipalId,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        string policyVersion,
        DateTimeOffset previewExpiresAtUtc,
        DateTimeOffset nowUtc,
        string idempotencyKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requesterPrincipalId);
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentNullException.ThrowIfNull(risk);
        ArgumentException.ThrowIfNullOrWhiteSpace(policyVersion);

        var clusterId = RequireBounded(intent.ClusterId, nameof(intent.ClusterId), 256);
        var canonicalIntent = RequireBounded(
            intent.CanonicalIntent,
            nameof(intent.CanonicalIntent),
            MutationLimits.MaxCanonicalIntentCharacters);
        var requester = RequireBounded(requesterPrincipalId, nameof(requesterPrincipalId), 4096);
        var resources = MutationPreviewHasher.NormalizeResources(intent.ResourceKeys);
        var preconditions = MutationPreviewHasher.NormalizePreconditions(intent.Preconditions);
        var digests = MutationPreviewHasher.NormalizeDigests(intent.MaterialDigests);
        var authorizationTargets = MutationAuthorization.NormalizeTargets(
            intent.Kind,
            clusterId,
            intent.AuthorizationTargets,
            resources);

        if (previewExpiresAtUtc <= nowUtc)
        {
            throw new ArgumentOutOfRangeException(nameof(previewExpiresAtUtc), "Preview expiry must be in the future.");
        }

        var normalizedIntent = intent with
        {
            ClusterId = clusterId,
            CanonicalIntent = canonicalIntent,
            ResourceKeys = resources,
            Preconditions = preconditions,
            MaterialDigests = digests,
            AuthorizationTargets = authorizationTargets,
        };
        var riskContext = intent.RiskContext ?? new MutationRiskContext();
        var effectiveRisk = MutationRiskClassifier.EnforceBuiltInFloor(
            new MutationRiskInput(
                intent.Kind,
                resources.Count,
                riskContext.PermanentDelete,
                riskContext.DurabilitySensitiveChange),
            risk);

        var confirmationChallenge = MutationAuthorization.BuildConfirmationChallenge(
            effectiveRisk.ConfirmationMode,
            authorizationTargets);
        var previewHash = MutationPreviewHasher.ComputeHash(
            normalizedIntent,
            effectiveRisk,
            policyVersion,
            previewExpiresAtUtc);

        var snapshot = new MutationOperationSnapshot
        {
            OperationId = Guid.NewGuid(),
            RequesterPrincipalId = requester,
            ClusterId = clusterId,
            OperationKind = intent.Kind,
            Risk = effectiveRisk,
            State = MutationOperationState.Previewed,
            CanonicalIntent = canonicalIntent,
            CanonicalIntentHash = MutationIdempotency.HashAdmittedIntent(
                normalizedIntent,
                effectiveRisk,
                policyVersion),
            ResourceKeys = resources,
            Preconditions = preconditions,
            MaterialDigests = digests,
            AuthorizationTargets = authorizationTargets,
            ConfirmationChallenge = confirmationChallenge,
            PreviewHash = previewHash,
            PreviewExpiresAtUtc = previewExpiresAtUtc,
            PolicyVersion = RequireBounded(policyVersion, nameof(policyVersion), 256),
            IdempotencyScope = MutationIdempotency.BuildScope(requester, clusterId, intent.Kind),
            IdempotencyKeyHash = MutationIdempotency.HashKey(idempotencyKey),
            Version = 0,
            ExecutionClaimGeneration = 0,
            CreatedAtUtc = nowUtc,
            UpdatedAtUtc = nowUtc,
        };

        return new MutationOperation(snapshot);
    }

    public static MutationOperation Restore(MutationOperationSnapshot snapshot) =>
        new(snapshot ?? throw new ArgumentNullException(nameof(snapshot)));

    public void OpenForConfirmation(DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.Previewed);
        RequireNotExpired(nowUtc);
        Transition(MutationOperationState.AwaitingConfirmation, nowUtc);
    }

    public void Confirm(
        string principalId,
        string previewHash,
        DateTimeOffset nowUtc,
        string? typedTargetChallenge = null)
    {
        RequireState(MutationOperationState.AwaitingConfirmation);
        RequireNotExpired(nowUtc);
        RequirePreviewHash(previewHash);

        var principal = RequireBounded(principalId, nameof(principalId), 4096);
        if (!string.Equals(principal, Snapshot.RequesterPrincipalId, StringComparison.Ordinal))
        {
            throw new MutationStateException("Only the requesting principal may confirm this mutation.");
        }

        string? confirmedChallenge = null;
        if (Snapshot.Risk.ConfirmationMode == MutationConfirmationMode.TypedTarget)
        {
            if (string.IsNullOrWhiteSpace(Snapshot.ConfirmationChallenge))
            {
                throw new MutationStateException(
                    "Typed-target confirmation is required but no server challenge is available.");
            }

            if (string.IsNullOrWhiteSpace(typedTargetChallenge))
            {
                throw new MutationStateException(
                    "Typed-target confirmation requires the server-derived target challenge.");
            }

            confirmedChallenge = RequireBounded(
                typedTargetChallenge,
                nameof(typedTargetChallenge),
                512);
            if (!string.Equals(
                    confirmedChallenge,
                    Snapshot.ConfirmationChallenge,
                    StringComparison.Ordinal))
            {
                throw new MutationStateException(
                    "Typed-target confirmation does not match the server-derived target challenge.");
            }
        }

        Snapshot = Snapshot with
        {
            ConfirmedByPrincipalId = principal,
            ConfirmedChallenge = confirmedChallenge,
            ConfirmedAtUtc = nowUtc,
        };
        Transition(
            Snapshot.Risk.RequiresIndependentApproval
                ? MutationOperationState.AwaitingApproval
                : MutationOperationState.Ready,
            nowUtc);
    }

    public void Approve(
        MutationApprovalAuthorizationEvidence evidence,
        string previewHash,
        DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.AwaitingApproval);
        RequireNotExpired(nowUtc);
        RequirePreviewHash(previewHash);
        RequireApprovalEvidence(evidence);

        if (string.Equals(
                evidence.PrincipalId,
                Snapshot.RequesterPrincipalId,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "An independent approval must come from a distinct authorized principal.");
        }

        Snapshot = Snapshot with
        {
            ApprovedByPrincipalId = evidence.PrincipalId,
            ApprovalAuthorizationEvidenceHash = evidence.EvidenceHash,
            ApprovedAtUtc = nowUtc,
        };
        Transition(MutationOperationState.Ready, nowUtc);
    }

    public void Reject(
        MutationApprovalAuthorizationEvidence evidence,
        string previewHash,
        DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.AwaitingApproval);
        RequireNotExpired(nowUtc);
        RequirePreviewHash(previewHash);
        RequireApprovalEvidence(evidence);

        if (string.Equals(
                evidence.PrincipalId,
                Snapshot.RequesterPrincipalId,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "The requesting principal cannot act as the independent rejector.");
        }

        Snapshot = Snapshot with
        {
            RejectedByPrincipalId = evidence.PrincipalId,
            RejectionAuthorizationEvidenceHash = evidence.EvidenceHash,
            RejectedAtUtc = nowUtc,
        };
        Transition(MutationOperationState.Rejected, nowUtc);
    }

    public long ClaimExecution(DateTimeOffset nowUtc, DateTimeOffset claimExpiresAtUtc)
    {
        RequireState(MutationOperationState.Ready);
        RequireNotExpired(nowUtc);
        if (claimExpiresAtUtc <= nowUtc)
        {
            throw new ArgumentOutOfRangeException(
                nameof(claimExpiresAtUtc),
                "Execution claim expiry must be in the future.");
        }

        var generation = checked(Snapshot.ExecutionClaimGeneration + 1);
        Snapshot = Snapshot with
        {
            ExecutionClaimGeneration = generation,
            ExecutionClaimExpiresAtUtc = claimExpiresAtUtc,
        };
        Transition(MutationOperationState.Executing, nowUtc);
        return generation;
    }

    public void MarkDispatchStarted(DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.Executing);
        if (Snapshot.DispatchStartedAtUtc is not null)
        {
            throw new MutationStateException("External dispatch has already been marked as started.");
        }

        Snapshot = Snapshot with
        {
            DispatchStartedAtUtc = nowUtc,
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };
    }

    public void Complete(MutationExecutionResultKind result, string resultCode, DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.Executing);
        ArgumentException.ThrowIfNullOrWhiteSpace(resultCode);

        if (result != MutationExecutionResultKind.FailedBeforeDispatch &&
            Snapshot.DispatchStartedAtUtc is null)
        {
            throw new MutationStateException(
                "A post-dispatch mutation result cannot be recorded before dispatch starts.");
        }

        var target = result switch
        {
            MutationExecutionResultKind.AppliedVerified => MutationOperationState.AppliedVerified,
            MutationExecutionResultKind.AppliedUnverified => MutationOperationState.AppliedUnverified,
            MutationExecutionResultKind.PartiallyApplied => MutationOperationState.PartiallyApplied,
            MutationExecutionResultKind.ExecutionUnknown => MutationOperationState.ExecutionUnknown,
            MutationExecutionResultKind.FailedBeforeDispatch when Snapshot.DispatchStartedAtUtc is null =>
                MutationOperationState.FailedBeforeDispatch,
            MutationExecutionResultKind.FailedBeforeDispatch =>
                throw new MutationStateException("A pre-dispatch failure cannot be recorded after dispatch started."),
            MutationExecutionResultKind.FailedDefinitive => MutationOperationState.FailedDefinitive,
            _ => throw new ArgumentOutOfRangeException(nameof(result), result, "Unsupported execution result."),
        };

        Snapshot = Snapshot with { ResultCode = RequireBounded(resultCode, nameof(resultCode), 256) };
        Transition(target, nowUtc);
    }

    public void MarkStaleBeforeDispatch(DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.Executing);
        if (Snapshot.DispatchStartedAtUtc is not null)
        {
            throw new MutationStateException("A dispatched mutation cannot be reclassified as a stale preview.");
        }

        Transition(MutationOperationState.StalePreview, nowUtc);
    }

    public void MarkStalePreview(DateTimeOffset nowUtc)
    {
        if (Snapshot.State is not (
            MutationOperationState.Previewed or
            MutationOperationState.AwaitingConfirmation or
            MutationOperationState.AwaitingApproval or
            MutationOperationState.Ready))
        {
            throw new MutationStateException($"State '{Snapshot.State}' cannot transition to stale preview.");
        }

        Transition(MutationOperationState.StalePreview, nowUtc);
    }

    public void Cancel(DateTimeOffset nowUtc)
    {
        if (Snapshot.State is not (
            MutationOperationState.Previewed or
            MutationOperationState.AwaitingConfirmation or
            MutationOperationState.AwaitingApproval or
            MutationOperationState.Ready))
        {
            throw new MutationStateException("Mutation cannot be cancelled after external execution has begun or after it is terminal.");
        }

        Transition(MutationOperationState.Cancelled, nowUtc);
    }

    public void Expire(DateTimeOffset nowUtc)
    {
        if (nowUtc < Snapshot.PreviewExpiresAtUtc)
        {
            throw new MutationStateException("Mutation preview has not expired.");
        }

        if (Snapshot.State is not (
            MutationOperationState.Previewed or
            MutationOperationState.AwaitingConfirmation or
            MutationOperationState.AwaitingApproval or
            MutationOperationState.Ready))
        {
            throw new MutationStateException("Only a non-executing mutation may expire.");
        }

        Transition(MutationOperationState.Expired, nowUtc);
    }

    private void Transition(MutationOperationState state, DateTimeOffset nowUtc)
    {
        Snapshot = Snapshot with
        {
            State = state,
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };
    }

    private void RequireApprovalEvidence(
        MutationApprovalAuthorizationEvidence evidence)
    {
        ArgumentNullException.ThrowIfNull(evidence);

        if (evidence.OperationId != Snapshot.OperationId ||
            !string.Equals(evidence.PreviewHash, Snapshot.PreviewHash, StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(evidence.PrincipalId) ||
            string.IsNullOrWhiteSpace(evidence.EvidenceHash))
        {
            throw new MutationStateException(
                "Approval authorization evidence does not match this mutation operation.");
        }
    }

    private void RequireState(MutationOperationState expected)
    {
        if (Snapshot.State != expected)
        {
            throw new MutationStateException($"Mutation is in state '{Snapshot.State}', expected '{expected}'.");
        }
    }

    private void RequireNotExpired(DateTimeOffset nowUtc)
    {
        if (nowUtc >= Snapshot.PreviewExpiresAtUtc)
        {
            throw new MutationStateException("Mutation preview has expired and must be regenerated.");
        }
    }

    private void RequirePreviewHash(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(value, Snapshot.PreviewHash, StringComparison.Ordinal))
        {
            throw new MutationStateException("Confirmation or approval does not match the admitted preview.");
        }
    }

    private static string RequireBounded(string value, string parameterName, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);
        var normalized = value.Trim();
        if (normalized.Length > maxLength || normalized.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                $"Value must be at most {maxLength} characters and contain no control characters.");
        }

        return normalized;
    }
}
