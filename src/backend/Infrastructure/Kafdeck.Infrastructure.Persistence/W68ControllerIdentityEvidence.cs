namespace Kafdeck.Infrastructure.Persistence;

// Internal pure candidate. Does not authenticate, lock storage or grant runtime mutation.
internal sealed record W68ControllerIdentityEvidence(string DeploymentId,string ScopeId,string PrincipalId,string ControllerId,bool Authenticated,bool ControllerRegistered,bool PrincipalMayControlScope);
internal static class W68ControllerIdentityGate {
 internal static bool Allows(W68ControllerIdentityEvidence? e,string deployment,string scope,string principal) =>
 e is not null && e.Authenticated && e.ControllerRegistered && e.PrincipalMayControlScope &&
 Valid(e.DeploymentId) && Valid(e.ScopeId) && Valid(e.PrincipalId) && Valid(e.ControllerId) &&
 string.Equals(e.DeploymentId,deployment,StringComparison.Ordinal) &&
 string.Equals(e.ScopeId,scope,StringComparison.Ordinal) &&
 string.Equals(e.PrincipalId,principal,StringComparison.Ordinal);
 private static bool Valid(string? v) => !string.IsNullOrWhiteSpace(v) && v.Length<=128 && v==v.Trim();
}
