namespace Kafdeck.Infrastructure.Persistence;

// Candidate-only guard; cannot mint ownership authority or attest a DB lock.
internal sealed record W68OwnershipCasEvidence(
    bool AuthenticatedPrincipal, bool RegisteredSource, bool AuthorizedOwner,
    bool ExactResourceAuthorization, bool LockedLeaseInSameTransaction,
    bool DatabaseClockVerified, bool DurableAuditPrepared,
    long CurrentRevision, long ExpectedRevision,
    long LockedLeaseEpoch, long AcquiredLeaseEpoch);

internal static class W68OwnershipCasAdmission
{
    internal static bool CanAttempt(W68OwnershipCasEvidence? e) =>
        e is not null && e.AuthenticatedPrincipal && e.RegisteredSource &&
        e.AuthorizedOwner && e.ExactResourceAuthorization &&
        e.LockedLeaseInSameTransaction && e.DatabaseClockVerified &&
        e.DurableAuditPrepared && e.CurrentRevision >= 0 &&
        e.ExpectedRevision == e.CurrentRevision &&
        e.AcquiredLeaseEpoch > 0 && e.LockedLeaseEpoch == e.AcquiredLeaseEpoch &&
        e.ExpectedRevision < long.MaxValue;
}
