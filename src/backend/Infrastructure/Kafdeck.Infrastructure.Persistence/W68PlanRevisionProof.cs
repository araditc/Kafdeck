namespace Kafdeck.Infrastructure.Persistence;

// Compare-only evidence contract; never allows Apply or Adopt by itself.
internal sealed record W68PlanRevisionEvidence(
    string DeploymentId, string ResourceKind, string ResourceId,
    long ObservedRevision, string PlanSha256, string DesiredSha256, bool AuthorizedObservation);
internal static class W68PlanRevisionProof
{
    internal static bool CanUseObservation(W68PlanRevisionEvidence? proof,
        string deployment, string kind, string resource, long expectedRevision,
        string expectedPlanSha256, string expectedDesiredSha256)
    {
        if (proof is null || !proof.AuthorizedObservation || expectedRevision < 0 ||
            !Valid(deployment) || !Valid(kind) || !Valid(resource) ||
            !Sha(expectedPlanSha256) || !Sha(expectedDesiredSha256) ||
            !Sha(proof.PlanSha256) || !Sha(proof.DesiredSha256))
            return false;
        return proof.ObservedRevision == expectedRevision &&
            string.Equals(proof.DeploymentId,deployment,StringComparison.Ordinal) &&
            string.Equals(proof.ResourceKind,kind,StringComparison.Ordinal) &&
            string.Equals(proof.ResourceId,resource,StringComparison.Ordinal) &&
            string.Equals(proof.PlanSha256,expectedPlanSha256,StringComparison.Ordinal) &&
            string.Equals(proof.DesiredSha256,expectedDesiredSha256,StringComparison.Ordinal);
    }
    private static bool Valid(string? x) => !string.IsNullOrWhiteSpace(x) &&
        x.Length <= 128 && x == x.Trim();
    private static bool Sha(string? x) => x is { Length: 64 } &&
        x.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
}
