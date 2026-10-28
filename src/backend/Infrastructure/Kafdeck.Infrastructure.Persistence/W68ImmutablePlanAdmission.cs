namespace Kafdeck.Infrastructure.Persistence;

// Internal pure candidate. Does not authenticate, lock storage or grant runtime mutation.
internal sealed record W68ImmutablePlanEvidence(string DeploymentId,string ResourceId,string PlanHash,string DesiredHash,long ObservedRevision,bool ReadAuthorized,bool OwnerRegistered,bool ExpiresValid,bool RiskApproved);
internal static class W68ImmutablePlanGate {
 internal static bool Allows(W68ImmutablePlanEvidence? e,string deployment,string resource,string plan,string desired,long revision) =>
 e is not null && e.ReadAuthorized && e.OwnerRegistered && e.ExpiresValid && e.RiskApproved &&
 revision>=0 && e.ObservedRevision==revision && Canon(e.DeploymentId) && Canon(e.ResourceId) &&
 string.Equals(e.DeploymentId,deployment,StringComparison.Ordinal) &&
 string.Equals(e.ResourceId,resource,StringComparison.Ordinal) &&
 Sha(e.PlanHash) && Sha(e.DesiredHash) && Sha(plan) && Sha(desired) &&
 string.Equals(e.PlanHash,plan,StringComparison.Ordinal) &&
 string.Equals(e.DesiredHash,desired,StringComparison.Ordinal);
 private static bool Canon(string? v)=>!string.IsNullOrWhiteSpace(v)&&v.Length<=128&&v==v.Trim();
 private static bool Sha(string? v)=>v is {Length:64}&&v.All(c=>c is >= '0' and <= '9' or >= 'a' and <= 'f');
}
