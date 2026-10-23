namespace Kafdeck.Core.Declarative;

// Pure, candidate-only drift classification. This is NOT a current-state reader,
// GitOps controller, ownership claim, plan, or permission to apply mutations.
public enum CandidateDriftStatus
{
    InSync,
    OutOfSync,
    Missing,
    Unknown,
    Inaccessible,
    Conflict,
    Unsupported
}

public enum CandidateObservationTruth
{
    AuthorizedCurrent,
    AuthorizedMissing,
    StaleOrUnavailable,
    AccessDenied,
    OwnershipConflict,
    FamilyUnsupported
}

// All evidence must already have been fetched and authorized by the backend.
// No observed resource identifier or secret is ever accepted or returned here.
public sealed record CandidateDriftEvidence(
    CandidateObservationTruth Truth,
    string? CurrentFingerprint = null,
    bool IsRevisionCurrent = false,
    bool IsOwnershipConsistent = false);

public static class CandidateSubscriptionDriftClassifier
{
    public static CandidateDriftStatus Classify(
        string desiredFingerprint,
        CandidateDriftEvidence observation)
    {
        ArgumentNullException.ThrowIfNull(observation);
        // Even a denied or inaccessible observation cannot disclose a desired
        // resource identity via an invalid fingerprint error; authorization
        // uncertainty is decided ahead of inspecting any fingerprint value.
        if (observation.Truth == CandidateObservationTruth.AccessDenied)
            return CandidateDriftStatus.Inaccessible;
        if (observation.Truth == CandidateObservationTruth.FamilyUnsupported)
            return CandidateDriftStatus.Unsupported;
        if (observation.Truth == CandidateObservationTruth.StaleOrUnavailable)
            return CandidateDriftStatus.Unknown;
        if (observation.Truth == CandidateObservationTruth.OwnershipConflict)
            return CandidateDriftStatus.Conflict;

        if (observation.Truth is not (
            CandidateObservationTruth.AuthorizedCurrent or
            CandidateObservationTruth.AuthorizedMissing))
            return CandidateDriftStatus.Unknown;

        // Missing must be backed by an authorized, current, consistent exact
        // lookup, never an empty inaccessible/stale list.
        if (!observation.IsRevisionCurrent)
            return CandidateDriftStatus.Unknown;
        if (!observation.IsOwnershipConsistent)
            return CandidateDriftStatus.Conflict;
        if (observation.Truth == CandidateObservationTruth.AuthorizedMissing)
            return CandidateDriftStatus.Missing;

        if (!IsFingerprint(desiredFingerprint) ||
            !IsFingerprint(observation.CurrentFingerprint))
            return CandidateDriftStatus.Unknown;

        return string.Equals(desiredFingerprint, observation.CurrentFingerprint, StringComparison.Ordinal)
            ? CandidateDriftStatus.InSync
            : CandidateDriftStatus.OutOfSync;
    }

    private static bool IsFingerprint(string? value) =>
        value is { Length: 64 } &&
        value.All(ch => ch is >= '0' and <= '9' or >= 'a' and <= 'f');
}
