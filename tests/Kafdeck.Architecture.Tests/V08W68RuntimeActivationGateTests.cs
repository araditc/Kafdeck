using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68RuntimeActivationGateTests
{
    [Fact] public void Each_missing_prerequisite_denies_activation()
    {
        var p=new W68RuntimePrerequisites(true,true,true,true,true,true,true,true,true,true);
        Assert.True(W68RuntimeActivationGate.MayAdmit(p));
        Assert.False(W68RuntimeActivationGate.MayAdmit(null));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {SchemaMigrated=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {VerifiedControllerIdentity=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {SourceRegistered=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {AuditDurable=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {DbTimeLeaseAcquired=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {MonotonicEpochFenced=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {OwnerCasAtomic=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {ProtectedResourceAuthorized=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {RiskGovernanceAdmitted=false}));
        Assert.False(W68RuntimeActivationGate.MayAdmit(p with {HaRecoveryProven=false}));
    }
}
