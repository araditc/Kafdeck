namespace Kafdeck.Infrastructure.Persistence;
// Internal readiness predicate; it is NOT an API route or feature flag.
internal sealed record W68RuntimePrerequisites(bool SchemaMigrated,bool VerifiedControllerIdentity,
 bool SourceRegistered,bool AuditDurable,bool DbTimeLeaseAcquired,bool MonotonicEpochFenced,
 bool OwnerCasAtomic,bool ProtectedResourceAuthorized,bool RiskGovernanceAdmitted,bool HaRecoveryProven);
internal static class W68RuntimeActivationGate
{
    internal static bool MayAdmit(W68RuntimePrerequisites? p) =>
        p is not null && p.SchemaMigrated && p.VerifiedControllerIdentity &&
        p.SourceRegistered && p.AuditDurable && p.DbTimeLeaseAcquired &&
        p.MonotonicEpochFenced && p.OwnerCasAtomic && p.ProtectedResourceAuthorized &&
        p.RiskGovernanceAdmitted && p.HaRecoveryProven;
}
