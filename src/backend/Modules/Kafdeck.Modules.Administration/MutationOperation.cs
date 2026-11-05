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
        string idempotencyKey) =>
        CreatePreview(
            Guid.NewGuid(),
            requesterPrincipalId,
            intent,
            risk,
            policyVersion,
            previewExpiresAtUtc,
            nowUtc,
            idempotencyKey);

    /// <summary>
    /// Creates a preview using a server-generated operation identity allocated
    /// before operation-bound material planning. The legacy overload above
    /// retains the existing v0.5 identity allocation and hash behavior.
    /// </summary>
    public static MutationOperation CreatePreview(
        Guid operationId,
        string requesterPrincipalId,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        string policyVersion,
        DateTimeOffset previewExpiresAtUtc,
        DateTimeOffset nowUtc,
        string idempotencyKey)
    {
        if (operationId == Guid.Empty)
        {
            throw new ArgumentException(
                "Mutation operation identity must be non-empty.",
                nameof(operationId));
        }

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
        var authorizationTargets = MutationAuthorizationRequirements.Normalize(
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
        // ACL alterations and finite cluster transfers have admitted,
        // operation-specific blast-radius classifiers. Reapplying the generic
        // multi-resource escalation here would make every narrow transfer
        // CRITICAL merely because one transfer necessarily owns a pair key plus
        // source/destination partition keys. Their specific classifiers can
        // still propose CRITICAL and EnforceBuiltInFloor never lowers it.
        var floorTargetCount = intent.Kind is
            MutationOperationKind.AclAlter or
            MutationOperationKind.ClusterTransfer
                ? 1
                : resources.Count;
        var effectiveRisk = MutationRiskClassifier.EnforceBuiltInFloor(
            new MutationRiskInput(
                intent.Kind,
                floorTargetCount,
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
            OperationId = operationId,
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

    public void RenewExecutionLease(DateTimeOffset nowUtc, DateTimeOffset newExpiresAtUtc)
    {
        RequireState(MutationOperationState.Executing);
        if (Snapshot.DispatchStartedAtUtc is not null)
        {
            throw new MutationStateException(
                "Execution lease cannot be renewed through the pre-dispatch path after dispatch starts.");
        }

        if (Snapshot.ExecutionClaimExpiresAtUtc is not { } currentExpiry ||
            currentExpiry <= nowUtc)
        {
            throw new MutationStateException(
                "Execution lease has expired and the mutation must not be dispatched.");
        }

        if (newExpiresAtUtc <= nowUtc || newExpiresAtUtc < currentExpiry)
        {
            throw new ArgumentOutOfRangeException(
                nameof(newExpiresAtUtc),
                "Renewed execution lease must be live and must not shorten the current lease.");
        }

        Snapshot = Snapshot with
        {
            ExecutionClaimExpiresAtUtc = newExpiresAtUtc,
            Version = checked(Snapshot.Version + 1),
            UpdatedAtUtc = nowUtc,
        };
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

    public void Complete(
        MutationExecutionResultKind result,
        string resultCode,
        DateTimeOffset nowUtc,
        IReadOnlyDictionary<string, string>? safeProviderEvidence = null)
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

        Snapshot = Snapshot with
        {
            ResultCode = RequireBounded(resultCode, nameof(resultCode), 256),
            SafeProviderEvidence = MutationProviderEvidence.Normalize(safeProviderEvidence),
        };
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

    // A positive backend transaction receipt resolves acknowledgement loss.
    public void ReconcileVerifiedGovernanceCommit(Guid receiptOperationId,int receiptKind,string materialHash,string receiptPreviewHash,
        string versionId,string contentHash,long revision,DateTimeOffset committedAtUtc,DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.ExecutionUnknown);
        if(Snapshot.OperationKind is not(MutationOperationKind.GovernancePolicyVersion or MutationOperationKind.GovernancePolicyActivation)||
            receiptKind!=(int)Snapshot.OperationKind||receiptOperationId!=Snapshot.OperationId||Snapshot.DispatchStartedAtUtc is null||
            receiptPreviewHash!=Snapshot.PreviewHash||materialHash!=Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(Snapshot.CanonicalIntent))).ToLowerInvariant()||
            versionId is not {Length:64}||contentHash is not {Length:64}||revision<0||committedAtUtc<Snapshot.CreatedAtUtc||committedAtUtc>nowUtc)
            throw new MutationStateException("Matching positive governance commit evidence is required.");
        Snapshot=Snapshot with{ResultCode="GovernanceCommitReceiptRecovered",SafeProviderEvidence=MutationProviderEvidence.Normalize(new Dictionary<string,string>
        { ["policy.version"]=versionId,["policy.hash"]=contentHash,["policy.revision"]=revision.ToString(System.Globalization.CultureInfo.InvariantCulture),["policy.receipt"]=receiptOperationId.ToString("D") })};
        Transition(MutationOperationState.AppliedVerified,nowUtc);
    }

    // A positive backend transaction receipt resolves acknowledgement loss.
    // This never authorizes another dispatch or infers success from absence.
    public void ReconcileVerifiedDeclarativeCommit(Guid receiptOperationId, string planFingerprint,
        long ownershipRevision, long subscriptionRevision, DateTimeOffset committedAtUtc, DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.ExecutionUnknown);
        if (Snapshot.OperationKind != MutationOperationKind.DeclarativeNotificationSubscription ||
            receiptOperationId != Snapshot.OperationId || Snapshot.DispatchStartedAtUtc is null ||
            planFingerprint is not { Length: 64 } || !planFingerprint.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f') ||
            Snapshot.Preconditions.Count(p => p.Key == "declarative.plan" && p.Fingerprint == planFingerprint) != 1 ||
            ownershipRevision < 1 || subscriptionRevision < 1 || committedAtUtc == default || committedAtUtc > nowUtc)
            throw new MutationStateException("A matching durable declarative commit receipt is required.");
        Snapshot = Snapshot with
        {
            ResultCode = "declarative_commit_receipt_recovered",
            SafeProviderEvidence = MutationProviderEvidence.Normalize(new Dictionary<string, string>
            {
                ["declarative.plan"] = receiptOperationId.ToString("D"),
                ["declarative.plan-fingerprint"] = planFingerprint,
                ["declarative.ownership-revision"] = ownershipRevision.ToString(System.Globalization.CultureInfo.InvariantCulture),
                ["declarative.subscription-revision"] = subscriptionRevision.ToString(System.Globalization.CultureInfo.InvariantCulture)
            })
        };
        Transition(MutationOperationState.AppliedVerified, nowUtc);
    }

    // A catalog DB transaction receipt proves a completed metadata effect, never permission to dispatch again.
    public void ReconcileVerifiedCatalogCommit(Guid receiptOperationId, string materialHash, int changedCount,
        DateTimeOffset committedAtUtc, DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.ExecutionUnknown);
        if (Snapshot.OperationKind != MutationOperationKind.CatalogMutation || receiptOperationId != Snapshot.OperationId ||
            Snapshot.DispatchStartedAtUtc is null || materialHash is not { Length: 64 } ||
            !materialHash.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f') ||
            Snapshot.Preconditions.Count(p => p.Key == "catalog.material" && p.Fingerprint == materialHash) != 1 ||
            changedCount is < 1 or > 500 || changedCount != Snapshot.ResourceKeys.Count || committedAtUtc == default ||
            committedAtUtc < Snapshot.CreatedAtUtc || committedAtUtc > Snapshot.PreviewExpiresAtUtc || committedAtUtc > nowUtc)
            throw new MutationStateException("A matching durable catalog commit receipt is required.");
        Snapshot = Snapshot with
        {
            ResultCode = "catalog_commit_receipt_recovered",
            SafeProviderEvidence = MutationProviderEvidence.Normalize(new Dictionary<string, string>
            {
                ["catalog.plan"] = receiptOperationId.ToString("D"), ["catalog.content-hash"] = materialHash,
                ["catalog.changed"] = changedCount.ToString(System.Globalization.CultureInfo.InvariantCulture)
            })
        };
        Transition(MutationOperationState.AppliedVerified, nowUtc);
    }

    // Only immutable same-transaction request/rate-card receipts may resolve acknowledgement loss.
    // Current authority, outside generation and guarded CAS are separately checked by the owning store.
    public void ReconcileVerifiedWorkflowCommit(Guid receiptOperationId,int receiptKind,string materialHash,string receiptPreviewHash,
        string entityId,string contentHash,long revision,DateTimeOffset committedAtUtc,DateTimeOffset nowUtc)
    {
        RequireState(MutationOperationState.ExecutionUnknown);
        var request=Snapshot.OperationKind is MutationOperationKind.ServiceRequestChange or MutationOperationKind.ServiceRequestDecision;
        var rate=Snapshot.OperationKind is MutationOperationKind.RateCardVersion or MutationOperationKind.RateCardActivation;
        static bool Hash(string value)=>value is {Length:64}&&value.All(c=>c is >= '0' and <= '9' or >= 'a' and <= 'f');
        var requestId=Guid.Empty;
        if((!request&&!rate)||receiptKind!=(int)Snapshot.OperationKind||receiptOperationId!=Snapshot.OperationId||Snapshot.DispatchStartedAtUtc is null||
            receiptPreviewHash!=Snapshot.PreviewHash||!Hash(contentHash)||revision<(request?1:0)||
            materialHash!=Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(Snapshot.CanonicalIntent))).ToLowerInvariant()||
            request&&(!Guid.TryParseExact(entityId,"N",out requestId)||requestId==Guid.Empty||entityId!=entityId.ToLowerInvariant())||
            rate&&!Hash(entityId)||committedAtUtc<Snapshot.CreatedAtUtc||committedAtUtc>nowUtc)
            throw new MutationStateException("Matching positive workflow commit evidence is required.");
        var prefix=request?"request":"rate-card";
        Snapshot=Snapshot with{ResultCode="WorkflowCommitReceiptRecovered",SafeProviderEvidence=MutationProviderEvidence.Normalize(new Dictionary<string,string>
        {
            [prefix+(request?".id":".version")]=request?requestId.ToString("D"):entityId,
            [prefix+".hash"]=contentHash,[prefix+".revision"]=revision.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [prefix+".receipt"]=receiptOperationId.ToString("D")
        })};
        Transition(MutationOperationState.AppliedVerified,nowUtc);
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
