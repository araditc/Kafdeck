namespace Kafdeck.Core.Declarative;

// Candidate-only immutable server observation proof. Construction does not
// register an owner, authorize a principal, or grant any GitOps mutation.
internal sealed record CandidateOwnershipEvidence(
    string DeploymentId,
    string ResourceKind,
    string ResourceId,
    string RegisteredOwnerId,
    string RegisteredSourceId,
    long OwnershipRevision,
    DateTimeOffset ObservedAtUtc,
    DateTimeOffset ExpiresAtUtc,
    bool ServerVerified,
    bool HasCompetingClaim);

internal static class CandidateOwnershipEvidenceGuard
{
    private const string SubscriptionKind = "NotificationSubscription";

    // Reject absent, expired, mismatched or client-authored evidence. In a future
    // admitted runtime, the source must be a trusted backend ownership registry,
    // NEVER manifest.ownerRef or data provided by the GitOps runner.
    internal static bool IsCurrentAndConsistent(
        CandidateOwnershipEvidence? evidence,
        string expectedDeploymentId,
        string expectedSubscriptionId,
        string expectedRegisteredOwnerId,
        string expectedRegisteredSourceId,
        long expectedOwnershipRevision,
        DateTimeOffset serverNowUtc)
    {
        if (evidence is null || !evidence.ServerVerified || evidence.HasCompetingClaim ||
            expectedOwnershipRevision < 1 || evidence.OwnershipRevision != expectedOwnershipRevision ||
            serverNowUtc == default ||
            evidence.ObservedAtUtc == default || evidence.ExpiresAtUtc == default ||
            evidence.ObservedAtUtc > serverNowUtc || evidence.ExpiresAtUtc <= serverNowUtc ||
            evidence.ExpiresAtUtc <= evidence.ObservedAtUtc)
            return false;

        return string.Equals(evidence.DeploymentId, expectedDeploymentId, StringComparison.Ordinal) &&
               string.Equals(evidence.ResourceKind, SubscriptionKind, StringComparison.Ordinal) &&
               string.Equals(evidence.ResourceId, expectedSubscriptionId, StringComparison.Ordinal) &&
               string.Equals(evidence.RegisteredOwnerId, expectedRegisteredOwnerId, StringComparison.Ordinal) &&
               string.Equals(evidence.RegisteredSourceId, expectedRegisteredSourceId, StringComparison.Ordinal) &&
               !string.IsNullOrWhiteSpace(expectedDeploymentId) &&
               !string.IsNullOrWhiteSpace(expectedSubscriptionId) &&
               !string.IsNullOrWhiteSpace(expectedRegisteredOwnerId) &&
               !string.IsNullOrWhiteSpace(expectedRegisteredSourceId);
    }
}
