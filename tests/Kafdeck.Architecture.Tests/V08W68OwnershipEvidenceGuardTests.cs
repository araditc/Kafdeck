using Kafdeck.Core.Declarative;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68OwnershipEvidenceGuardTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 23, 12, 0, 0, TimeSpan.Zero);

    private static CandidateOwnershipEvidence Evidence() =>
        new("production-a", "NotificationSubscription", "ops-lag",
            "registered-owner", "registered-source", 3, Now.AddSeconds(-5),
            Now.AddMinutes(1), ServerVerified: true, HasCompetingClaim: false);

    private static bool Check(CandidateOwnershipEvidence? evidence,
        string owner = "registered-owner", string source = "registered-source") =>
        CandidateOwnershipEvidenceGuard.IsCurrentAndConsistent(evidence,
            "production-a", "ops-lag", owner, source, Now);

    [Fact]
    public void Verified_current_exact_owner_and_source_evidence_is_eligible_for_read_only_comparison() =>
        Assert.True(Check(Evidence()));

    [Fact]
    public void Missing_unverified_expired_future_or_conflicting_evidence_fails_closed()
    {
        var proof = Evidence();
        Assert.False(Check(null));
        Assert.False(Check(proof with { ServerVerified = false }));
        Assert.False(Check(proof with { HasCompetingClaim = true }));
        Assert.False(Check(proof with { OwnershipRevision = 0 }));
        Assert.False(Check(proof with { ExpiresAtUtc = Now }));
        Assert.False(Check(proof with { ObservedAtUtc = Now.AddSeconds(1) }));
        Assert.False(Check(proof with { ExpiresAtUtc = proof.ObservedAtUtc }));
    }

    [Fact]
    public void A_different_deployment_resource_kind_owner_or_registered_source_is_not_adopted()
    {
        var proof = Evidence();
        Assert.False(Check(proof with { DeploymentId = "production-b" }));
        Assert.False(Check(proof with { ResourceKind = "KafkaTopic" }));
        Assert.False(Check(proof with { ResourceId = "other-subscription" }));
        Assert.False(Check(proof with { RegisteredOwnerId = "other-owner" }));
        Assert.False(Check(proof with { RegisteredSourceId = "other-repository-path" }));
        Assert.False(Check(proof, owner: "manifest-claims-owner"));
        Assert.False(Check(proof, source: "different-source"));
        Assert.False(Check(proof, owner: ""));
    }

    [Fact]
    public void Proof_record_does_not_supply_apply_adopt_destroy_or_owner_creation()
    {
        Assert.DoesNotContain(typeof(CandidateOwnershipEvidenceGuard).GetMethods(),
            method => method.Name is "Apply" or "Adopt" or "Destroy" or "Acquire");
    }
}
