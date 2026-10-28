namespace Kafdeck.Infrastructure.Persistence;

// Pure, unregistered admission predicate. Not evidence of an actual DB lock.
internal static class W68LeaseEligibility
{
    internal static bool CanRenew(
        W68StoredControllerLease? stored,
        string deploymentId, string scopeId, string authenticatedHolderId,
        long acquiredEpoch, DateTimeOffset databaseNowUtc)
    {
        return stored is not null &&
            Valid(deploymentId) && Valid(scopeId) && Valid(authenticatedHolderId) &&
            acquiredEpoch > 0 &&
            databaseNowUtc != default && databaseNowUtc.Offset == TimeSpan.Zero &&
            stored.ExpiresAtUtc.Offset == TimeSpan.Zero &&
            stored.ExpiresAtUtc > databaseNowUtc &&
            stored.Epoch == acquiredEpoch &&
            string.Equals(stored.DeploymentId, deploymentId, StringComparison.Ordinal) &&
            string.Equals(stored.ScopeId, scopeId, StringComparison.Ordinal) &&
            string.Equals(stored.HolderId, authenticatedHolderId, StringComparison.Ordinal);
    }
    private static bool Valid(string value) =>
        !string.IsNullOrWhiteSpace(value) && value.Length <= 128 &&
        string.Equals(value, value.Trim(), StringComparison.Ordinal);
}
