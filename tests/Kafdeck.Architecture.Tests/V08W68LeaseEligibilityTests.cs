using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68LeaseEligibilityTests
{
    private static readonly DateTimeOffset Now = new(2026,10,27,12,0,0,TimeSpan.Zero);
    private static W68StoredControllerLease Row() => new("prod","writer","holder",12,Now.AddSeconds(30));
    [Fact]
    public void Exact_evidence_passes_but_denials_are_closed()
    {
        bool Check(W68StoredControllerLease? row,string dep="prod",string scope="writer",
          string holder="holder",long epoch=12,DateTimeOffset? now=null) =>
            W68LeaseEligibility.CanRenew(row,dep,scope,holder,epoch,now??Now);
        Assert.True(Check(Row()));
        Assert.False(Check(null));
        Assert.False(Check(Row(),dep:"other"));
        Assert.False(Check(Row(),scope:"other"));
        Assert.False(Check(Row(),holder:"impostor"));
        Assert.False(Check(Row(),epoch:11));
        Assert.False(Check(Row(),epoch:0));
        Assert.False(Check(Row(),now:Now.AddSeconds(30)));
        Assert.False(Check(Row(),now:Now.AddSeconds(31)));
        Assert.False(Check(Row(),dep:" prod "));
        Assert.False(Check(Row() with { Epoch=13 }));
    }
}
