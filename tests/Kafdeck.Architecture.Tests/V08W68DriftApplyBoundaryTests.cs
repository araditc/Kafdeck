using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68DriftApplyBoundaryTests
{
    [Fact]
    public void Unowned_missing_and_inaccessible_resources_cannot_become_apply()
    {
        var e = new W68DriftAuthorityEvidence(W68DriftObservation.OutOfSync,true,true,true,true,true,true);
        Assert.True(W68DriftApplyBoundary.CanEnterGovernedApply(e));
        Assert.False(W68DriftApplyBoundary.CanEnterGovernedApply(null));
        Assert.False(W68DriftApplyBoundary.CanEnterGovernedApply(e with {RegisteredOwner=false, Status=W68DriftObservation.Missing}));
        Assert.False(W68DriftApplyBoundary.CanEnterGovernedApply(e with {AuthorizedCurrentRead=false}));
        Assert.False(W68DriftApplyBoundary.CanEnterGovernedApply(e with {ImmutablePlan=false}));
        Assert.False(W68DriftApplyBoundary.CanEnterGovernedApply(e with {BackendRiskReviewSatisfied=false}));
        foreach(var status in new [] { W68DriftObservation.Unknown,W68DriftObservation.Inaccessible,
            W68DriftObservation.Conflict,W68DriftObservation.Unsupported,W68DriftObservation.InSync })
            Assert.False(W68DriftApplyBoundary.CanEnterGovernedApply(e with {Status=status}));
    }
}
