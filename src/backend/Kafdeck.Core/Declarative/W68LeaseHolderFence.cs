namespace Kafdeck.Core.Declarative;

// Pure admission predicate only. Caller must load and lock the authoritative
// lease row in the SAME DB transaction that conditionally modifies ownership.
internal sealed record W68AuthoritativeLeaseEvidence(
    string DeploymentId,
    string LeaseScopeId,
    string HolderId,
    long Epoch,
    DateTimeOffset ExpiresAtUtc,
    bool FromAuthoritativeLockedRow);

internal static class W68LeaseHolderFence
{
    internal static bool CanIssueOwnershipCas(
        W68AuthoritativeLeaseEvidence? lease,
        string expectedDeployment,
        string expectedLeaseScope,
        string authenticatedHolderId,
        long acquiredEpoch,
        DateTimeOffset authoritativeDatabaseNowUtc)
    {
        if (lease is null || !lease.FromAuthoritativeLockedRow ||
            string.IsNullOrWhiteSpace(expectedDeployment) ||
            string.IsNullOrWhiteSpace(expectedLeaseScope) ||
            string.IsNullOrWhiteSpace(authenticatedHolderId) ||
            acquiredEpoch < 1 || lease.Epoch != acquiredEpoch ||
            authoritativeDatabaseNowUtc == default ||
            lease.ExpiresAtUtc <= authoritativeDatabaseNowUtc)
            return false;

        return string.Equals(lease.DeploymentId, expectedDeployment, StringComparison.Ordinal) &&
               string.Equals(lease.LeaseScopeId, expectedLeaseScope, StringComparison.Ordinal) &&
               string.Equals(lease.HolderId, authenticatedHolderId, StringComparison.Ordinal);
    }
}
