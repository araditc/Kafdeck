using System.Net;
using System.Net.Http.Json;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedAccessRegressionTests
{
    [Theory]
    [InlineData("viewer",AuthorizationAction.ConsumerRead,false)]
    [InlineData("operator",AuthorizationAction.ConsumerRead,true)]
    [InlineData("viewer",AuthorizationAction.RecordProduce,false)]
    [InlineData("operator",AuthorizationAction.RecordProduce,false)]
    [InlineData("manager",AuthorizationAction.RecordProduce,true)]
    [InlineData("manager",AuthorizationAction.SchemaCreate,true)]
    public async Task Existing_role_rules_are_current_cluster_scoped_and_keep_write_prerequisites(string role,AuthorizationAction action,bool permitted)
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();var id=f.AddUser(role);
        var session=Session(id);
        Assert.Equal(permitted,ManagedIdentityEndpoints.Allows(f.Store,session,action,"connection-a",registeredWrites:true,schemaRegistryConfigured:true));
        Assert.False(ManagedIdentityEndpoints.Allows(f.Store,session,action,"connection-b",registeredWrites:true,schemaRegistryConfigured:true));
        if(action is AuthorizationAction.RecordProduce or AuthorizationAction.SchemaCreate)
            Assert.False(ManagedIdentityEndpoints.Allows(f.Store,session,action,"connection-a",registeredWrites:false,schemaRegistryConfigured:true));
        if(action==AuthorizationAction.SchemaCreate)
            Assert.False(ManagedIdentityEndpoints.Allows(f.Store,session,action,"connection-a",registeredWrites:true,schemaRegistryConfigured:false));
    }

    [Theory]
    [InlineData("role")]
    [InlineData("cluster")]
    [InlineData("disabled")]
    [InlineData("session")]
    public async Task Previously_valid_session_cannot_keep_authority_after_state_changes(string changed)
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();var id=f.AddUser();var session=Session(id);
        Assert.True(ManagedIdentityEndpoints.Allows(f.Store,session,AuthorizationAction.RecordProduce,"connection-a",registeredWrites:true));
        f.ChangeUser(id,u=>changed switch {"role"=>u with {Role="viewer"},"cluster"=>u with {ClusterIds=[]},"disabled"=>u with {Enabled=false},_=>u with {SessionsValidFrom=DateTimeOffset.UtcNow.AddMinutes(1)}});
        Assert.False(ManagedIdentityEndpoints.Allows(f.Store,session,AuthorizationAction.RecordProduce,"connection-a",registeredWrites:true));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Provider_outage_cannot_leave_local_disable_open_or_enable_a_disabled_user(bool enabling)
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();var id=f.AddUser(enabled:!enabling);var old=Session(id);f.Provider.FailUserChanges=true;
        using var response=await f.Http.PutAsJsonAsync($"/api/v1/identity/users/{id:D}",new {role="manager",clusterIds=new[]{"connection-a"},enabled=enabling});
        Assert.Equal(HttpStatusCode.BadGateway,response.StatusCode);Assert.Equal(1,f.Provider.EnableChanges);
        var user=f.Store.Read().Users.Single(u=>u.Id==id);Assert.False(user.Enabled);
        Assert.False(ManagedIdentityEndpoints.Allows(f.Store,old,AuthorizationAction.ClusterRead,"connection-a"));
        Assert.Empty(f.Identity.Audit.Events);
    }

    [Fact]
    public async Task Disable_and_reenable_require_a_fresh_session_and_logout_the_disabled_account()
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();var id=f.AddUser();var old=Session(id);
        using var disabled=await f.Http.PutAsJsonAsync($"/api/v1/identity/users/{id:D}",new {role="manager",clusterIds=new[]{"connection-a"},enabled=false});
        Assert.Equal(HttpStatusCode.OK,disabled.StatusCode);Assert.Equal(1,f.Provider.Logouts);
        using var enabled=await f.Http.PutAsJsonAsync($"/api/v1/identity/users/{id:D}",new {role="manager",clusterIds=new[]{"connection-a"},enabled=true});
        Assert.Equal(HttpStatusCode.OK,enabled.StatusCode);
        Assert.False(ManagedIdentityEndpoints.Allows(f.Store,old,AuthorizationAction.ClusterRead,"connection-a"));
        Assert.True(ManagedIdentityEndpoints.Allows(f.Store,Session(id,DateTimeOffset.UtcNow),AuthorizationAction.ClusterRead,"connection-a"));
        Assert.Equal(2,f.Identity.Audit.Events.Count);
    }

    [Fact]
    public async Task First_owner_cannot_be_demoted_or_disabled_through_ordinary_user_update()
    {
        await using var f=await V1ManagedWorkflowLab.Start();
        using var rejected=await f.Http.PutAsJsonAsync($"/api/v1/identity/users/{f.Identity.OwnerId:D}",new {role="viewer",clusterIds=Array.Empty<string>(),enabled=false});
        Assert.Equal(HttpStatusCode.Conflict,rejected.StatusCode);Assert.EndsWith(":identity-owner",await V1ManagedWorkflowLab.Problem(rejected));
        var owner=Assert.Single(f.Store.Read().Users);Assert.Equal("owner",owner.Role);Assert.True(owner.Enabled);Assert.Equal(0,f.Provider.EnableChanges);
    }

    private static OperatorSessionContext Session(Guid id,DateTimeOffset? at=null)=>new(new(new(ManagedProvisioningFixture.Issuer,id.ToString("D"))),new SessionId(Guid.NewGuid()),at??DateTimeOffset.UtcNow.AddMinutes(-1));
}
