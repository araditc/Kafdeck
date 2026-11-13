using System.Net;
using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedSetupRegressionTests
{
    [Fact]
    public async Task Expired_code_cannot_reserve_or_dispatch_first_owner_creation()
    {
        await using var f=await V1ManagedWorkflowLab.Start(setup:true);
        var code=f.Store.IssueCode();var state=f.Store.Read();f.Store.Write(state with {CodeExpires=DateTimeOffset.UtcNow.AddSeconds(-1)});
        using var result=await f.Setup(code);
        Assert.Equal(HttpStatusCode.Forbidden,result.StatusCode);Assert.EndsWith(":setup-code",await V1ManagedWorkflowLab.Problem(result));
        Assert.Equal(0,f.Provider.Creates);Assert.Null(f.Store.Read().OwnerId);Assert.False(f.Store.Read().Provisioning);
    }

    [Fact]
    public async Task Replacement_invalidates_old_code_and_success_consumes_the_new_code_once()
    {
        await using var f=await V1ManagedWorkflowLab.Start(setup:true);
        var old=f.Store.IssueCode();var current=f.Store.IssueCode();Assert.NotEqual(old,current);
        Assert.DoesNotContain(current,File.ReadAllText(f.Identity.StatePath),StringComparison.Ordinal);
        using var denied=await f.Setup(old);Assert.Equal(HttpStatusCode.Forbidden,denied.StatusCode);Assert.Equal(0,f.Provider.Creates);
        using var created=await f.Setup(current);Assert.Equal(HttpStatusCode.NoContent,created.StatusCode);
        var state=f.Store.Read();var owner=Assert.Single(state.Users);
        Assert.Equal("owner",owner.Role);Assert.Equal("first.owner@example.test",owner.Email);Assert.Equal(owner.Id,state.OwnerId);
        Assert.Null(state.CodeHash);Assert.Equal(default,state.CodeExpires);Assert.False(state.Provisioning);
        using var replay=await f.Setup(current);Assert.Equal(HttpStatusCode.Conflict,replay.StatusCode);
        Assert.Throws<InvalidOperationException>(()=>f.Store.IssueCode());Assert.Equal(1,f.Provider.Creates);
        Assert.Equal("initial_owner_created",Assert.Single(f.Identity.Audit.Events).ReasonCategory);
    }

    [Fact]
    public async Task Missing_setup_CSRF_does_not_consume_a_valid_host_code()
    {
        await using var f=await V1ManagedWorkflowLab.Start(setup:true);var code=f.Store.IssueCode();
        f.Http.DefaultRequestHeaders.Remove(KafdeckAntiforgeryExtensions.HeaderName);
        using var denied=await f.Setup(code);Assert.Equal(HttpStatusCode.Forbidden,denied.StatusCode);
        Assert.True(ManagedIdentityStore.MatchesCode(f.Store.Read(),code));Assert.Equal(0,f.Provider.Creates);
    }

    [Fact]
    public async Task Concurrent_first_owner_requests_have_one_provider_dispatch_and_one_owner()
    {
        await using var f=await V1ManagedWorkflowLab.Start(setup:true);var code=f.Store.IssueCode();
        var entered=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        f.Provider.CreateGate=async()=>{entered.TrySetResult();await release.Task;};
        var first=f.Setup(code);
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(f.Store.Read().Provisioning);Assert.Null(f.Store.Read().CodeHash);
            using var second=await f.Setup(code);Assert.Equal(HttpStatusCode.Conflict,second.StatusCode);
            Assert.EndsWith(":identity-busy",await V1ManagedWorkflowLab.Problem(second));
        }
        finally {release.TrySetResult();}
        using var result=await first;Assert.Equal(HttpStatusCode.NoContent,result.StatusCode);
        Assert.Single(f.Store.Read().Users);Assert.Single(f.Provider.Accounts);Assert.Single(f.Identity.Audit.Events);Assert.Equal(1,f.Provider.Creates);
    }

    [Fact]
    public async Task Committed_but_lost_first_owner_reply_keeps_the_durable_quarantine_closed()
    {
        await using var f=await V1ManagedWorkflowLab.Start(setup:true);var code=f.Store.IssueCode();f.Provider.LoseCommittedReply=true;
        using var unknown=await f.Setup(code);Assert.Equal(HttpStatusCode.BadGateway,unknown.StatusCode);
        var reopened=new ManagedIdentityStore(f.Identity.StatePath,ManagedProvisioningFixture.Issuer);
        Assert.Equal("recovery",ManagedIdentityEndpoints.SetupState(reopened.Read()));Assert.Empty(reopened.Read().Users);Assert.Null(reopened.Read().CodeHash);
        Assert.Throws<InvalidOperationException>(()=>reopened.IssueCode());
        using var retry=await f.Setup(code);Assert.Equal(HttpStatusCode.Conflict,retry.StatusCode);
        Assert.Single(f.Provider.Accounts);Assert.Equal(1,f.Provider.Creates);Assert.Empty(f.Identity.Audit.Events);
    }

    [Theory]
    [InlineData(400,false)]
    [InlineData(409,false)]
    [InlineData(503,true)]
    public async Task Only_a_definite_provider_rejection_can_leave_setup_reissuable(int status,bool quarantined)
    {
        await using var f=await V1ManagedWorkflowLab.Start(setup:true);var code=f.Store.IssueCode();f.Provider.CreateStatus=(HttpStatusCode)status;
        using var rejected=await f.Setup(code);Assert.NotEqual(HttpStatusCode.NoContent,rejected.StatusCode);
        var state=f.Store.Read();Assert.Equal(quarantined,state.Provisioning);Assert.Null(state.OwnerId);Assert.Null(state.CodeHash);
        Assert.False(ManagedIdentityStore.MatchesCode(state,code));Assert.Empty(f.Provider.Accounts);
        if(quarantined)Assert.Throws<InvalidOperationException>(()=>f.Store.IssueCode());else {var replacement=f.Store.IssueCode();Assert.True(ManagedIdentityStore.MatchesCode(f.Store.Read(),replacement));}
    }

    [Fact]
    public async Task An_existing_foreign_realm_user_blocks_first_owner_without_consuming_the_code()
    {
        await using var f=await V1ManagedWorkflowLab.Start(setup:true);var code=f.Store.IssueCode();f.Provider.ExistingForeignUser=true;
        using var rejected=await f.Setup(code);Assert.Equal(HttpStatusCode.Conflict,rejected.StatusCode);
        Assert.EndsWith(":setup-realm-in-use",await V1ManagedWorkflowLab.Problem(rejected));Assert.Equal(0,f.Provider.Creates);
        Assert.True(ManagedIdentityStore.MatchesCode(f.Store.Read(),code));Assert.False(f.Store.Read().Provisioning);
    }
}
