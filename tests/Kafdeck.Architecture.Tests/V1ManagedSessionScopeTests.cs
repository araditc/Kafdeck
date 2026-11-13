using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedSessionScopeTests
{
    [Fact]
    public void Managed_authority_is_a_valid_stable_hash_instead_of_invalid_role_suffix()
    {
        using var f=new ManagedProvisioningFixture();var user=f.Store.Read().Users.Single();
        var principal=V1ManagedWorkflowLab.Browser(f.OwnerId);
        Assert.True(OperatorSessionContextFactory.TryCreate(principal,out var session));
        var policy=new string('a',64);var authority=OperatorSessionUiScope.ManagedAuthority(policy,user);
        Assert.Matches("^[a-f0-9]{64}$",authority!);
        var scope=OperatorSessionUiScope.Create(session!,authority);Assert.NotNull(scope);
        Assert.Equal(scope,OperatorSessionUiScope.Create(session!,OperatorSessionUiScope.ManagedAuthority(policy,user with {Email="cosmetic@example.test"})));
        Assert.Null(OperatorSessionUiScope.Create(session!,policy+":owner:"));
    }
    [Fact]
    public void Current_authority_changes_invalidate_the_UI_scope_but_ordering_does_not()
    {
        using var f=new ManagedProvisioningFixture();var user=f.Store.Read().Users.Single() with {ClusterIds=["a","b"]};var policy=new string('a',64);
        var before=OperatorSessionUiScope.ManagedAuthority(policy,user);
        foreach(var changed in new[]{user with {Role="viewer"},user with {Enabled=false},user with {ClusterIds=["a"]},user with {SessionsValidFrom=DateTimeOffset.UtcNow.AddMinutes(1)}})
            Assert.NotEqual(before,OperatorSessionUiScope.ManagedAuthority(policy,changed));
        Assert.NotEqual(before,OperatorSessionUiScope.ManagedAuthority(new string('b',64),user));
        Assert.Equal(before,OperatorSessionUiScope.ManagedAuthority(policy,user with {ClusterIds=["b","a"]}));
        Assert.Null(OperatorSessionUiScope.ManagedAuthority(null,user));
    }
}
