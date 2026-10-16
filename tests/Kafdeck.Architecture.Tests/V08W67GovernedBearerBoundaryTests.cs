using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67GovernedBearerBoundaryTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("Cookie")]
    [InlineData("X-Kafdeck-Access-Token")]
    public async Task Dedicated_mutation_body_is_bounded_before_binding_even_without_bearer_header(string? credentialHeader)
    {
        var context = new DefaultHttpContext();
        context.SetEndpoint(new Endpoint(_ => Task.CompletedTask,
            new EndpointMetadataCollection(new EndpointNameMetadata("v08-bearer-topic-create-preview")), "preview"));
        if (credentialHeader is not null) context.Request.Headers[credentialHeader] = "credential-sentinel";
        var feature = new MutableBodyLimit();
        context.Features.Set<IHttpMaxRequestBodySizeFeature>(feature);
        var reachedNext = false;
        var middleware = new NonBrowserReadBoundaryMiddleware(_ =>
        {
            Assert.Equal(64 * 1024, feature.MaxRequestBodySize);
            reachedNext = true;
            return Task.CompletedTask;
        });
        await middleware.InvokeAsync(context, null!, null!);
        Assert.True(reachedNext);
    }

    private sealed class MutableBodyLimit : IHttpMaxRequestBodySizeFeature
    {
        public bool IsReadOnly => false;
        public long? MaxRequestBodySize { get; set; } = 30_000_000;
    }

    [Fact]
    public void Mutation_requires_separate_global_activation_registration_action_and_current_exact_rbac()
    {
        var configuration = Configuration();
        var evaluator = Evaluator("orders*");
        var identity = Identity();
        var options = new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:8080", null, AccessMode.Local, null), []);
        var registry = new NonBrowserReadRegistry(configuration);
        var authorization = new KafdeckAuthorizationService(options, evaluator, registry);
        var principal = new ClaimsPrincipal(identity);
        Assert.Equal(KafdeckAuthorizationOutcome.Allowed, authorization.Authorize(principal, new(AuthorizationAction.TopicCreate, "allowed", "orders-new")));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.Authorize(principal, new(AuthorizationAction.TopicCreate, "denied", "orders-new")));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.Authorize(principal, new(AuthorizationAction.TopicCreate, "allowed", "secret-topic")));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.Authorize(principal, new(AuthorizationAction.TopicDelete, "allowed", "orders-new")));
        configuration["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"] = "false";
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.Authorize(principal, new(AuthorizationAction.TopicCreate, "allowed", "orders-new")));
        configuration["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"] = "true";
        configuration["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "false";
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, authorization.Authorize(principal, new(AuthorizationAction.TopicCreate, "allowed", "orders-new")));
    }

    [Fact]
    public void Actor_keeps_machine_and_human_attribution_without_manufacturing_browser_session()
    {
        var machine = Identity();
        Assert.True(MutationActor.TryCreate(new ClaimsPrincipal(machine), out var actor));
        Assert.Null(actor!.HumanKey);
        Assert.Equal(machine.AuditId, actor.PrincipalId);
        Assert.False(OperatorSessionContextFactory.TryCreate(new ClaimsPrincipal(machine), out _));
        var human = new NonBrowserReadIdentity("human-registration", machine.Evidence, NonBrowserPrincipalKind.Human);
        Assert.True(MutationActor.TryCreate(new ClaimsPrincipal(human), out var humanActor));
        Assert.NotNull(humanActor!.HumanKey);
        Assert.True(MutationActor.IsSameHuman(humanActor.PrincipalId, new("https://sso.test/realms/kafdeck", "subject")));
        Assert.False(MutationActor.IsSameHuman(humanActor.PrincipalId, new("https://sso.test/realms/kafdeck", "another-human")));
        Assert.False(MutationActor.IsSameHuman(actor.PrincipalId, new("https://sso.test/realms/kafdeck", "subject")));
    }

    [Fact]
    public void Current_policy_fingerprint_changes_for_resource_scope_and_subject_binding_changes()
    {
        Assert.Equal(Evaluator("orders*").Fingerprint, Evaluator("orders*").Fingerprint);
        Assert.NotEqual(Evaluator("orders*").Fingerprint, Evaluator("different*").Fingerprint);
    }

    private static NonBrowserReadIdentity Identity() => new("machine", new("https://sso.test/realms/kafdeck", "subject", "client", "api", DateTimeOffset.UtcNow.AddMinutes(2)));
    private static AuthorizationPolicyEvaluator Evaluator(string pattern) => new(AuthorizationPolicyCompiler.Compile(new(
        [new("creator", [new(AuthorizationAction.TopicCreate, ["allowed"], [pattern])])],
        [new("https://sso.test/realms/kafdeck", "subject", ["creator"])], [])));
    private static IConfigurationRoot Configuration() => new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Kafdeck:NonBrowserRead:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Issuer"] = "https://sso.test/realms/kafdeck",
        ["Kafdeck:NonBrowserRead:ApiAudience"] = "api", ["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"] = "true",
        ["Kafdeck:NonBrowserRead:Registrations:0:Id"] = "machine", ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"] = "owner",
        ["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = "subject", ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = "client",
        ["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O"),
        ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"] = "SystemRead", ["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:0"] = "TopicCreate",
    }).Build();
}
