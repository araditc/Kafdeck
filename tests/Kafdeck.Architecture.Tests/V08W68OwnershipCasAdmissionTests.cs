using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68OwnershipCasAdmissionTests
{
    [Fact]
    public void Rejects_unverified_lease_stale_revision_and_unaudited_mutations()
    {
        var e = new W68OwnershipCasEvidence(true,true,true,true,true,true,true,4,4,9,9);
        Assert.True(W68OwnershipCasAdmission.CanAttempt(e));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(null));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {AuthenticatedPrincipal=false}));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {RegisteredSource=false}));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {AuthorizedOwner=false}));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {ExactResourceAuthorization=false}));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {LockedLeaseInSameTransaction=false}));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {DatabaseClockVerified=false}));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {DurableAuditPrepared=false}));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {ExpectedRevision=3}));
        Assert.False(W68OwnershipCasAdmission.CanAttempt(e with {AcquiredLeaseEpoch=8}));
    }
}
