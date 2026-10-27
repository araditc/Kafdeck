using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68PlanRevisionProofTests
{
    [Fact]
    public void Immutable_hashes_and_current_revision_must_match_exactly()
    {
        var hash = new string('a',64);
        var desired = new string('b',64);
        var p = new W68PlanRevisionEvidence("prod","NotificationSubscription","sub",5,hash,desired,true);
        bool Check(W68PlanRevisionEvidence? value, string deployment="prod",long revision=5,
          string plan="",string target="") =>
            W68PlanRevisionProof.CanUseObservation(value,deployment,"NotificationSubscription","sub",
                revision,plan==""?hash:plan,target==""?desired:target);
        Assert.True(Check(p));
        Assert.False(Check(null));
        Assert.False(Check(p with { AuthorizedObservation=false }));
        Assert.False(Check(p,deployment:"other"));
        Assert.False(Check(p,revision:4));
        Assert.False(Check(p,plan:new string('c',64)));
        Assert.False(Check(p,target:"INVALID"));
    }
}
