using Kafdeck.Api;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68DeclarativeCommitRecoveryTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.UtcNow;

    [Fact]
    public void Matching_positive_receipt_resolves_unknown_without_claiming_or_dispatching_again()
    {
        var operation = Unknown(); var before = operation.Snapshot;
        var proof = Assert.Single(before.Preconditions, p => p.Key == "declarative.plan").Fingerprint;
        operation.ReconcileVerifiedDeclarativeCommit(before.OperationId, proof, 2, 3, Now.AddSeconds(4), Now.AddSeconds(5));
        var after = operation.Snapshot;
        Assert.Equal(MutationOperationState.AppliedVerified, after.State);
        Assert.Equal("declarative_commit_receipt_recovered", after.ResultCode);
        Assert.Equal(before.Version + 1, after.Version);
        Assert.Equal(before.ExecutionClaimGeneration, after.ExecutionClaimGeneration);
        Assert.Equal(before.DispatchStartedAtUtc, after.DispatchStartedAtUtc);
        Assert.Equal(before.CanonicalIntent, after.CanonicalIntent);
        Assert.Equal(before.RequesterPrincipalId, after.RequesterPrincipalId);
        Assert.Equal(before.ApprovedByPrincipalId, after.ApprovedByPrincipalId);
        Assert.Equal(before.ApprovalAuthorizationEvidenceHash, after.ApprovalAuthorizationEvidenceHash);
        Assert.Equal(proof, after.SafeProviderEvidence["declarative.plan-fingerprint"]);
        Assert.Equal("2", after.SafeProviderEvidence["declarative.ownership-revision"]);
        Assert.Equal("3", after.SafeProviderEvidence["declarative.subscription-revision"]);
        Assert.Throws<MutationStateException>(() => operation.ClaimExecution(Now.AddSeconds(6), Now.AddMinutes(1)));
        Assert.Throws<MutationStateException>(() => operation.MarkDispatchStarted(Now.AddSeconds(6)));
        Assert.Throws<MutationStateException>(() => operation.ReconcileVerifiedDeclarativeCommit(before.OperationId, proof, 2, 3, Now.AddSeconds(4), Now.AddSeconds(6)));
        Assert.Equal(after, operation.Snapshot);
    }

    [Theory]
    [InlineData("kind")][InlineData("operation")][InlineData("proof")][InlineData("missing-proof")]
    [InlineData("no-dispatch")][InlineData("owner-revision")][InlineData("subscription-revision")]
    [InlineData("default-time")][InlineData("future-time")]
    public void Nonmatching_or_nonpositive_receipt_never_changes_unknown_state(string failure)
    {
        var operation = Unknown(); var original = operation.Snapshot;
        var proof = Assert.Single(original.Preconditions, p => p.Key == "declarative.plan").Fingerprint;
        if (failure == "kind") operation = MutationOperation.Restore(original with { OperationKind = MutationOperationKind.TopicCreate });
        if (failure == "no-dispatch") operation = MutationOperation.Restore(original with { DispatchStartedAtUtc = null });
        if (failure == "missing-proof") operation = MutationOperation.Restore(original with { Preconditions = [] });
        var before = operation.Snapshot;
        Assert.Throws<MutationStateException>(() => operation.ReconcileVerifiedDeclarativeCommit(
            failure == "operation" ? Guid.NewGuid() : before.OperationId,
            failure == "proof" ? new string('0', 64) : proof,
            failure == "owner-revision" ? 0 : 2,
            failure == "subscription-revision" ? -1 : 3,
            failure == "default-time" ? default : failure == "future-time" ? Now.AddMinutes(1) : Now.AddSeconds(4),
            Now.AddSeconds(5)));
        Assert.Equal(before, operation.Snapshot);
        Assert.Equal(MutationOperationState.ExecutionUnknown, operation.Snapshot.State);
    }

    [Theory]
    [InlineData(MutationOperationState.Ready)]
    [InlineData(MutationOperationState.Executing)]
    [InlineData(MutationOperationState.AppliedVerified)]
    [InlineData(MutationOperationState.Cancelled)]
    [InlineData(MutationOperationState.Expired)]
    [InlineData(MutationOperationState.FailedBeforeDispatch)]
    public void Receipt_recovery_is_not_a_general_terminal_state_override(MutationOperationState state)
    {
        var operation = MutationOperation.Restore(Unknown().Snapshot with { State = state });
        var before = operation.Snapshot;
        var proof = Assert.Single(before.Preconditions, p => p.Key == "declarative.plan").Fingerprint;
        Assert.Throws<MutationStateException>(() => operation.ReconcileVerifiedDeclarativeCommit(
            before.OperationId, proof, 1, 1, Now.AddSeconds(4), Now.AddSeconds(5)));
        Assert.Equal(before, operation.Snapshot);
    }

    private static MutationOperation Unknown()
    {
        var plan = V08W68GovernedApplyBoundaryTests.Plan();
        var operation = MutationOperation.CreatePreview(plan.PlanId, plan.RequesterPrincipalId,
            DeclarativeSubscriptionMutation.Intent(plan), MutationRiskClassifier.Classify(new(MutationOperationKind.DeclarativeNotificationSubscription)),
            "test-policy", Now.AddMinutes(5), Now, Guid.NewGuid().ToString("N"));
        operation.OpenForConfirmation(Now);
        operation.Confirm(plan.RequesterPrincipalId, operation.Snapshot.PreviewHash, Now, operation.Snapshot.ConfirmationChallenge);
        operation.Approve(new(operation.Snapshot.OperationId, "oidc:https://idp.example|distinct-approver",
            operation.Snapshot.PreviewHash, new string('b', 64)), operation.Snapshot.PreviewHash, Now);
        operation.ClaimExecution(Now, Now.AddMinutes(1));
        operation.MarkDispatchStarted(Now.AddSeconds(1));
        operation.Complete(MutationExecutionResultKind.ExecutionUnknown, "declarative_commit_unknown", Now.AddSeconds(3));
        return operation;
    }
}
