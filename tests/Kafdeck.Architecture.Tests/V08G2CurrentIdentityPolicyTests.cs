using Kafdeck.Api;
using Kafdeck.Core.Security;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G2CurrentIdentityPolicyTests
{
    [Fact]
    public void Current_policy_reload_revokes_both_request_authority_and_preview_fingerprint()
    {
        var config = Configuration();
        using var configLifetime = (IDisposable)config;
        using var policies = new ReloadingAuthorizationPolicy(config);
        var evaluator = new AuthorizationPolicyEvaluator(policies.Current, () => policies.Current);
        var key = new OperatorIdentityKey("https://issuer.example/realms/g2", "subject");
        var request = new AuthorizationRequest(AuthorizationAction.SystemRead);
        Assert.True(evaluator.EvaluateDirectSubject(key, request).IsAllowed);
        var fingerprint = evaluator.Fingerprint;
        config["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "ClusterRead";
        config.Reload();
        Assert.False(evaluator.EvaluateDirectSubject(key, request).IsAllowed);
        Assert.NotEqual(fingerprint, evaluator.Fingerprint);
        config["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "SystemRead";
        config.Reload();
        Assert.True(evaluator.EvaluateDirectSubject(key, request).IsAllowed);
        Assert.Equal(fingerprint, evaluator.Fingerprint);
    }

    [Fact]
    public void Malformed_reload_denies_every_previous_grant_until_valid_configuration_returns()
    {
        var config = Configuration();
        using var configLifetime = (IDisposable)config;
        using var policies = new ReloadingAuthorizationPolicy(config);
        var evaluator = new AuthorizationPolicyEvaluator(policies.Current, () => policies.Current);
        var key = new OperatorIdentityKey("https://issuer.example/realms/g2", "subject");
        Assert.True(evaluator.EvaluateDirectSubject(key, new(AuthorizationAction.SystemRead)).IsAllowed);
        config["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "arbitrary-invalid-action";
        config.Reload();
        Assert.False(evaluator.EvaluateDirectSubject(key, new(AuthorizationAction.SystemRead)).IsAllowed);
        Assert.False(evaluator.HasApplicableDirectSubjectPermission(key, AuthorizationAction.SystemRead, null));
        Assert.False(evaluator.HasApplicablePermission(new(key), AuthorizationAction.SystemRead));
        config["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "SystemRead";
        config.Reload();
        Assert.True(evaluator.EvaluateDirectSubject(key, new(AuthorizationAction.SystemRead)).IsAllowed);
    }

    [Fact]
    public void Signing_key_revocation_rechecks_current_configuration_and_invalid_lists_fail_closed()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Kafdeck:NonBrowserRead:Enabled"]="true",["Kafdeck:NonBrowserRead:Issuer"]="https://issuer.example/realms/g2",
            ["Kafdeck:NonBrowserRead:ApiAudience"]="api",["Kafdeck:NonBrowserRead:Registrations:0:Id"]="service",
            ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"]="owner",["Kafdeck:NonBrowserRead:Registrations:0:Subject"]="subject",
            ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"]="client",["Kafdeck:NonBrowserRead:Registrations:0:Enabled"]="true",
            ["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"]=DateTimeOffset.UtcNow.AddMinutes(5).ToString("O"),
            ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"]="SystemRead",
        }).Build();
        using var configLifetime = (IDisposable)config;
        var registry = new NonBrowserReadRegistry(config);
        Assert.True(registry.IsSigningKeyAllowed("old-key"));
        config["Kafdeck:NonBrowserRead:RevokedSigningKeyIds:0"]="old-key";
        Assert.False(registry.IsSigningKeyAllowed("old-key"));
        Assert.True(registry.IsSigningKeyAllowed("new-key"));
        config["Kafdeck:NonBrowserRead:RevokedSigningKeyIds:1"]="old-key";
        Assert.False(registry.IsSigningKeyAllowed("new-key"));
    }

    private static IConfigurationRoot Configuration() => new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Kafdeck:Authorization:Roles:0:Id"]="reader",["Kafdeck:Authorization:Roles:0:Permissions:0:Action"]="SystemRead",
        ["Kafdeck:Authorization:SubjectBindings:0:Issuer"]="https://issuer.example/realms/g2",
        ["Kafdeck:Authorization:SubjectBindings:0:Subject"]="subject",["Kafdeck:Authorization:SubjectBindings:0:RoleIds:0"]="reader",
    }).Build();
}
