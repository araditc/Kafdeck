using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class W68SourceRegistrationEvidenceTests {
 [Fact] public void Rejects_stale_and_unauthorized_evidence(){var gen=Guid.NewGuid();var hash=new string('a',64);
var e=new W68SourceRegistrationEvidence("prod","git",gen,true,true,false,hash);
Assert.True(W68SourceRegistrationGate.Allows(e,"prod","git",gen,hash));
Assert.False(W68SourceRegistrationGate.Allows(e with {Revoked=true},"prod","git",gen,hash));
Assert.False(W68SourceRegistrationGate.Allows(e,"prod","git",Guid.NewGuid(),hash));
Assert.False(W68SourceRegistrationGate.Allows(e,"prod","git",gen,new string('A',64)));
Assert.False(W68SourceRegistrationGate.Allows(e,"other","git",gen,hash));}
}
