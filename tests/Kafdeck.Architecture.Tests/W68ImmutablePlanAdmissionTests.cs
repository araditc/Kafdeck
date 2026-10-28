using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class W68ImmutablePlanAdmissionTests {
 [Fact] public void Rejects_stale_and_unauthorized_evidence(){var h=new string('a',64);var d=new string('b',64);
var e=new W68ImmutablePlanEvidence("prod","subscription",h,d,4,true,true,true,true);
Assert.True(W68ImmutablePlanGate.Allows(e,"prod","subscription",h,d,4));
Assert.False(W68ImmutablePlanGate.Allows(e with {RiskApproved=false},"prod","subscription",h,d,4));
Assert.False(W68ImmutablePlanGate.Allows(e with {OwnerRegistered=false},"prod","subscription",h,d,4));
Assert.False(W68ImmutablePlanGate.Allows(e,"prod","subscription",h,d,3));
Assert.False(W68ImmutablePlanGate.Allows(e,"prod","subscription",new string('c',64),d,4));}
}
