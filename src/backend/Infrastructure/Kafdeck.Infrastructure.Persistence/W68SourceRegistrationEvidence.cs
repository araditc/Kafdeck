namespace Kafdeck.Infrastructure.Persistence;

// Internal pure candidate. Does not authenticate, lock storage or grant runtime mutation.
internal sealed record W68SourceRegistrationEvidence(string DeploymentId,string SourceId,Guid Generation,bool AuthenticatedPrincipal,bool Registered,bool Revoked,string RevisionSha256);
internal static class W68SourceRegistrationGate {
 internal static bool Allows(W68SourceRegistrationEvidence? e,string deployment,string source,Guid generation,string revision) =>
 e is not null && e.AuthenticatedPrincipal && e.Registered && !e.Revoked &&
 e.Generation!=Guid.Empty && e.Generation==generation && Valid(deployment) && Valid(source) &&
 string.Equals(e.DeploymentId,deployment,StringComparison.Ordinal) &&
 string.Equals(e.SourceId,source,StringComparison.Ordinal) &&
 Hash(e.RevisionSha256) && Hash(revision) &&
 string.Equals(e.RevisionSha256,revision,StringComparison.Ordinal);
 private static bool Valid(string? v)=>!string.IsNullOrWhiteSpace(v)&&v.Length<=128&&v==v.Trim();
 private static bool Hash(string? v)=>v is {Length:64}&&v.All(c=>c is >= '0' and <= '9' or >= 'a' and <= 'f');
}
