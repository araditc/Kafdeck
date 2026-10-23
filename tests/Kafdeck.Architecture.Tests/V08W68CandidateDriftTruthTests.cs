using Kafdeck.Core.Declarative;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68CandidateDriftTruthTests
{
    private static readonly string Desired = new('a', 64);
    private static readonly string Other = new('b', 64);

    [Fact]
    public void Authorized_fresh_matching_evidence_reports_in_sync() =>
        Assert.Equal(CandidateDriftStatus.InSync, Check(CandidateObservationTruth.AuthorizedCurrent, Desired));

    [Fact]
    public void Authorized_fresh_mismatching_evidence_reports_out_of_sync() =>
        Assert.Equal(CandidateDriftStatus.OutOfSync, Check(CandidateObservationTruth.AuthorizedCurrent, Other));

    [Fact]
    public void Authorized_fresh_missing_evidence_reports_missing() =>
        Assert.Equal(CandidateDriftStatus.Missing, Check(CandidateObservationTruth.AuthorizedMissing));

    [Theory]
    [InlineData(CandidateObservationTruth.AccessDenied, CandidateDriftStatus.Inaccessible)]
    [InlineData(CandidateObservationTruth.StaleOrUnavailable, CandidateDriftStatus.Unknown)]
    [InlineData(CandidateObservationTruth.OwnershipConflict, CandidateDriftStatus.Conflict)]
    [InlineData(CandidateObservationTruth.FamilyUnsupported, CandidateDriftStatus.Unsupported)]
    public void Observation_truth_never_turns_denial_uncertainty_conflict_or_unsupported_into_missing(
        CandidateObservationTruth truth, CandidateDriftStatus expected) =>
        Assert.Equal(expected, Check(truth));

    [Fact]
    public void Stale_or_uncertain_ownership_never_proves_in_sync_missing_or_out_of_sync()
    {
        Assert.Equal(CandidateDriftStatus.Unknown,
            CandidateSubscriptionDriftClassifier.Classify(Desired, new(CandidateObservationTruth.AuthorizedCurrent, Desired)));
        Assert.Equal(CandidateDriftStatus.Unknown,
            CandidateSubscriptionDriftClassifier.Classify(Desired, new(CandidateObservationTruth.AuthorizedMissing)));
        Assert.Equal(CandidateDriftStatus.Conflict,
            CandidateSubscriptionDriftClassifier.Classify(Desired,
                new(CandidateObservationTruth.AuthorizedCurrent, Desired, IsRevisionCurrent: true)));
        Assert.Equal(CandidateDriftStatus.Conflict,
            CandidateSubscriptionDriftClassifier.Classify(Desired,
                new(CandidateObservationTruth.AuthorizedMissing, IsRevisionCurrent: true)));
    }

    [Theory]
    [InlineData("")]
    [InlineData("short")]
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")]
    public void Untrusted_or_corrupted_evidence_never_creates_a_false_comparison(string invalid)
    {
        Assert.Equal(CandidateDriftStatus.Unknown,
            CandidateSubscriptionDriftClassifier.Classify(Desired,
                new(CandidateObservationTruth.AuthorizedCurrent, invalid, true, true)));
        Assert.Equal(CandidateDriftStatus.Unknown,
            CandidateSubscriptionDriftClassifier.Classify(invalid,
                new(CandidateObservationTruth.AuthorizedCurrent, Desired, true, true)));
    }

    [Fact]
    public void Unauthorized_evidence_is_not_inspected_and_exposes_no_resource_details()
    {
        var inaccessible = CandidateSubscriptionDriftClassifier.Classify("not-a-fingerprint",
            new(CandidateObservationTruth.AccessDenied, Other, true, true));
        Assert.Equal(CandidateDriftStatus.Inaccessible, inaccessible);
        Assert.DoesNotContain("a", inaccessible.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Classification_is_pure_and_declares_no_effect_or_ownership_methods()
    {
        Assert.DoesNotContain(typeof(CandidateSubscriptionDriftClassifier).GetMethods(),
            method => method.Name is "Apply" or "Adopt" or "Execute" or "Destroy" or "Acquire");
    }

    private static CandidateDriftStatus Check(CandidateObservationTruth truth, string? observed = null) =>
        CandidateSubscriptionDriftClassifier.Classify(Desired,
            new(truth, observed, IsRevisionCurrent: true, IsOwnershipConsistent: true));
}
