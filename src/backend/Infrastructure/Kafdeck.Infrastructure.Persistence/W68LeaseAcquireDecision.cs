namespace Kafdeck.Infrastructure.Persistence;

// Pure candidate policy only. Real Acquire requires a locked DB row and high-water CAS.
internal sealed record W68AcquireEvidence(bool AuthenticatedController, bool SourceRegistered,
    bool SameDatabaseTransaction, bool HighWaterLocked, bool LeaseRowLocked,
    bool ExistingLeaseActive, long PreviousEpoch, long ExpectedNextEpoch);

internal static class W68LeaseAcquireDecision
{
    internal static bool CanAttempt(W68AcquireEvidence? e) =>
        e is not null && e.AuthenticatedController && e.SourceRegistered &&
        e.SameDatabaseTransaction && e.HighWaterLocked && e.LeaseRowLocked &&
        !e.ExistingLeaseActive && e.PreviousEpoch >= 0 &&
        e.PreviousEpoch < long.MaxValue && e.ExpectedNextEpoch == e.PreviousEpoch + 1;
}
