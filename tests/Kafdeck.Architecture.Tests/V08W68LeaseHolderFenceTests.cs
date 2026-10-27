using Kafdeck.Core.Declarative;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68LeaseHolderFenceTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 27, 8, 0, 0, TimeSpan.Zero);
    private static W68AuthoritativeLeaseEvidence Lease() =>
        new("prod", "ownership-writer", "controller-b", 7, Now.AddMinutes(1), true);

    private static bool Check(W68AuthoritativeLeaseEvidence? lease, string holder = "controller-b",
        long epoch = 7, string deployment = "prod", string scope = "ownership-writer") =>
        W68LeaseHolderFence.CanIssueOwnershipCas(lease, deployment, scope, holder, epoch, Now);

    [Fact]
    public void Current_unexpired_authoritative_holder_and_epoch_can_pass_the_candidate_fence() =>
        Assert.True(Check(Lease()));

    [Fact]
    public void No_locked_authoritative_row_or_expired_lease_does_not_pass()
    {
        Assert.False(Check(null));
        Assert.False(Check(Lease() with { FromAuthoritativeLockedRow = false }));
        Assert.False(Check(Lease() with { ExpiresAtUtc = Now }));
        Assert.False(Check(Lease() with { ExpiresAtUtc = Now.AddMinutes(-1) }));
    }

    [Fact]
    public void A_stale_or_impostor_controller_cannot_reuse_current_epoch()
    {
        Assert.False(Check(Lease(), holder: "controller-a"));
        Assert.False(Check(Lease(), epoch: 6));
        Assert.False(Check(Lease(), epoch: 8));
        Assert.False(Check(Lease() with { HolderId = "controller-a" }));
        Assert.False(Check(Lease(), holder: ""));
    }

    [Fact]
    public void Cross_deployment_or_scope_claim_cannot_pass()
    {
        Assert.False(Check(Lease(), deployment: "other"));
        Assert.False(Check(Lease(), scope: "other"));
        Assert.False(Check(Lease() with { DeploymentId = "other" }));
        Assert.False(Check(Lease() with { LeaseScopeId = "other" }));
        Assert.False(Check(Lease(), deployment: ""));
    }
}
