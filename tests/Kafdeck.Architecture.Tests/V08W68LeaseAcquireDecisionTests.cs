using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68LeaseAcquireDecisionTests
{
    [Fact]
    public void Stale_or_unauthenticated_acquisition_fails_closed()
    {
        var e = new W68AcquireEvidence(true,true,true,true,true,false,8,9);
        Assert.True(W68LeaseAcquireDecision.CanAttempt(e));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(null));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(e with {AuthenticatedController=false}));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(e with {SourceRegistered=false}));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(e with {SameDatabaseTransaction=false}));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(e with {HighWaterLocked=false}));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(e with {LeaseRowLocked=false}));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(e with {ExistingLeaseActive=true}));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(e with {ExpectedNextEpoch=8}));
        Assert.False(W68LeaseAcquireDecision.CanAttempt(e with {PreviousEpoch=long.MaxValue}));
    }
}
