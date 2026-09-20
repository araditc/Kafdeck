using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class AuthorizationPolicyTests
{
    [Fact]
    public void Unauthenticated_requests_are_denied()
    {
        var evaluator = CreateEvaluator(
            roles: new[]
            {
                Role("viewer", Permission(AuthorizationAction.ClusterRead)),
            });

        var decision = evaluator.Evaluate(null, new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod"));

        Assert.False(decision.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.Unauthenticated, decision.Reason);
    }

    [Fact]
    public void Identity_without_binding_is_denied()
    {
        var evaluator = CreateEvaluator(
            roles: new[]
            {
                Role("viewer", Permission(AuthorizationAction.ClusterRead)),
            });

        var identity = Identity("https://idp.example", "alice");
        var decision = evaluator.Evaluate(identity, new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod"));

        Assert.False(decision.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.NoMatchingBinding, decision.Reason);
    }

    [Fact]
    public void Exact_subject_binding_allows_only_configured_cluster()
    {
        var evaluator = CreateEvaluator(
            roles: new[]
            {
                Role(
                    "prod-viewer",
                    new AuthorizationPermissionDefinition(
                        AuthorizationAction.ClusterRead,
                        new[] { "prod" })),
            },
            subjects: new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "prod-viewer" }),
            });

        var identity = Identity("https://idp.example", "alice");

        var allowed = evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod"));
        var denied = evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "secret"));

        Assert.True(allowed.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.Allowed, allowed.Reason);
        Assert.False(denied.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.ResourceDenied, denied.Reason);
    }

    [Fact]
    public void Same_subject_from_different_issuer_does_not_match_binding()
    {
        var evaluator = CreateEvaluator(
            roles: new[]
            {
                Role("viewer", Permission(AuthorizationAction.ClusterRead)),
            },
            subjects: new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "viewer" }),
            });

        var decision = evaluator.Evaluate(
            Identity("https://other.example", "alice"),
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod"));

        Assert.False(decision.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.NoMatchingBinding, decision.Reason);
    }

    [Fact]
    public void Exact_external_group_binding_grants_configured_role()
    {
        var evaluator = CreateEvaluator(
            roles: new[]
            {
                Role("viewer", Permission(AuthorizationAction.TopicList)),
            },
            groups: new[]
            {
                new AuthorizationGroupBindingDefinition("kafka-readers", new[] { "viewer" }),
            });

        var identity = new OperatorIdentity(
            new OperatorIdentityKey("https://idp.example", "alice"),
            externalGroups: new[] { "unrelated", "kafka-readers" });

        var decision = evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.TopicList, "prod"));

        Assert.True(decision.IsAllowed);
        Assert.Contains("viewer", decision.MatchedRoleIds);
    }

    [Fact]
    public void Topic_resource_glob_is_bounded_and_enforced()
    {
        var evaluator = CreateEvaluator(
            roles: new[]
            {
                Role(
                    "payments-reader",
                    new AuthorizationPermissionDefinition(
                        AuthorizationAction.TopicRead,
                        new[] { "prod" },
                        new[] { "payments.*" })),
            },
            subjects: new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "payments-reader" }),
            });

        var identity = Identity("https://idp.example", "alice");

        Assert.True(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.TopicRead, "prod", "payments.created")).IsAllowed);

        var denied = evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.TopicRead, "prod", "audit.events"));

        Assert.False(denied.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.ResourceDenied, denied.Reason);
    }

    [Fact]
    public void Topic_read_does_not_imply_topic_configuration_read()
    {
        var evaluator = CreateEvaluator(
            roles: new[]
            {
                Role("viewer", Permission(AuthorizationAction.TopicRead)),
            },
            subjects: new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "viewer" }),
            });

        var decision = evaluator.Evaluate(
            Identity("https://idp.example", "alice"),
            new AuthorizationRequest(AuthorizationAction.TopicConfigRead, "prod", "payments"));

        Assert.False(decision.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.ActionDenied, decision.Reason);
    }

    [Fact]
    public void Unknown_role_reference_fails_policy_compilation()
    {
        var definition = new AuthorizationPolicyDefinition(
            Array.Empty<AuthorizationRoleDefinition>(),
            new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "missing" }),
            },
            Array.Empty<AuthorizationGroupBindingDefinition>());

        Assert.Throws<AuthorizationPolicyException>(() => AuthorizationPolicyCompiler.Compile(definition));
    }

    [Fact]
    public void Duplicate_role_id_fails_policy_compilation()
    {
        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                Role("viewer", Permission(AuthorizationAction.ClusterRead)),
                Role("viewer", Permission(AuthorizationAction.TopicRead)),
            },
            Array.Empty<AuthorizationSubjectBindingDefinition>(),
            Array.Empty<AuthorizationGroupBindingDefinition>());

        Assert.Throws<AuthorizationPolicyException>(() => AuthorizationPolicyCompiler.Compile(definition));
    }

    [Fact]
    public void Resource_pattern_wildcard_count_is_bounded()
    {
        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                Role(
                    "viewer",
                    new AuthorizationPermissionDefinition(
                        AuthorizationAction.TopicRead,
                        ResourcePatterns: new[] { "*a*b*c*d*e*f*g*h*i*" })),
            },
            Array.Empty<AuthorizationSubjectBindingDefinition>(),
            Array.Empty<AuthorizationGroupBindingDefinition>());

        Assert.Throws<AuthorizationPolicyException>(() => AuthorizationPolicyCompiler.Compile(definition));
    }

    [Fact]
    public void Permission_vocabulary_contains_no_write_or_payload_actions()
    {
        var forbidden = new[] { "write", "create", "delete", "alter", "produce", "consume", "payload", "record" };

        foreach (var action in Enum.GetNames<AuthorizationAction>())
        {
            Assert.DoesNotContain(
                forbidden,
                term => action.Contains(term, StringComparison.OrdinalIgnoreCase));
        }
    }

    [Fact]
    public void Configuration_loader_builds_compilable_default_deny_policy()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Authorization:Roles:0:Id"] = "viewer",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "cluster.read",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:ClusterIds:0"] = "prod",
            ["Kafdeck:Authorization:SubjectBindings:0:Issuer"] = "https://idp.example",
            ["Kafdeck:Authorization:SubjectBindings:0:Subject"] = "alice",
            ["Kafdeck:Authorization:SubjectBindings:0:RoleIds:0"] = "viewer",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        var definition = AuthorizationPolicyConfigurationLoader.Load(configuration);
        var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(definition));

        Assert.True(evaluator.Evaluate(
            Identity("https://idp.example", "alice"),
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod")).IsAllowed);

        Assert.False(evaluator.Evaluate(
            Identity("https://idp.example", "bob"),
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod")).IsAllowed);
    }

    private static AuthorizationPolicyEvaluator CreateEvaluator(
        IReadOnlyList<AuthorizationRoleDefinition> roles,
        IReadOnlyList<AuthorizationSubjectBindingDefinition>? subjects = null,
        IReadOnlyList<AuthorizationGroupBindingDefinition>? groups = null)
    {
        var definition = new AuthorizationPolicyDefinition(
            roles,
            subjects ?? Array.Empty<AuthorizationSubjectBindingDefinition>(),
            groups ?? Array.Empty<AuthorizationGroupBindingDefinition>());

        return new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(definition));
    }

    private static AuthorizationRoleDefinition Role(
        string id,
        params AuthorizationPermissionDefinition[] permissions) =>
        new(id, permissions);

    private static AuthorizationPermissionDefinition Permission(AuthorizationAction action) =>
        new(action);

    private static OperatorIdentity Identity(string issuer, string subject) =>
        new(new OperatorIdentityKey(issuer, subject));
}
