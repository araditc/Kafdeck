using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class W68ControllerIdentityEvidenceTests {
 [Fact] public void Rejects_stale_and_unauthorized_evidence(){var e=new W68ControllerIdentityEvidence("prod","writer","principal","controller",true,true,true);
Assert.True(W68ControllerIdentityGate.Allows(e,"prod","writer","principal"));
Assert.False(W68ControllerIdentityGate.Allows(e with {Authenticated=false},"prod","writer","principal"));
Assert.False(W68ControllerIdentityGate.Allows(e with {ControllerRegistered=false},"prod","writer","principal"));
Assert.False(W68ControllerIdentityGate.Allows(e with {PrincipalMayControlScope=false},"prod","writer","principal"));
Assert.False(W68ControllerIdentityGate.Allows(e,"prod","other","principal"));}
}
