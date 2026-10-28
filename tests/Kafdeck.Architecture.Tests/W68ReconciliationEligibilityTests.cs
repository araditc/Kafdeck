using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class W68ReconciliationEligibilityTests {
 [Fact] public void Rejects_stale_and_unauthorized_evidence(){var e=new W68ReconciliationEvidence(W68ReconciliationStatus.OutOfSync,true,true,true,true,true,true);
Assert.True(W68ReconciliationGate.CanSchedule(e));
Assert.False(W68ReconciliationGate.CanSchedule(e with {FreshLease=false}));
Assert.False(W68ReconciliationGate.CanSchedule(e with {RegisteredOwner=false}));
Assert.False(W68ReconciliationGate.CanSchedule(e with {Status=W68ReconciliationStatus.Inaccessible}));
Assert.False(W68ReconciliationGate.CanSchedule(e with {Status=W68ReconciliationStatus.Conflict}));
Assert.False(W68ReconciliationGate.CanSchedule(e with {GoverningApproval=false}));}
}
