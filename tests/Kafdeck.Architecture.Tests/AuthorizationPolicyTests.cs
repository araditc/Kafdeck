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
    public void Topic_read_does_not_imply_record_read()
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
            new AuthorizationRequest(AuthorizationAction.RecordRead, "prod", "payments"));

        Assert.False(decision.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.ActionDenied, decision.Reason);
    }

    [Fact]
    public void Record_read_does_not_imply_record_export()
    {
        var evaluator = CreateEvaluator(
            roles: new[]
            {
                Role(
                    "record-reader",
                    new AuthorizationPermissionDefinition(
                        AuthorizationAction.RecordRead,
                        new[] { "prod" },
                        new[] { "payments.*" })),
            },
            subjects: new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "record-reader" }),
            });

        var identity = Identity("https://idp.example", "alice");

        Assert.True(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.RecordRead, "prod", "payments.created")).IsAllowed);

        var export = evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.RecordExport, "prod", "payments.created"));

        Assert.False(export.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.ActionDenied, export.Reason);
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
    public void Permission_vocabulary_exposes_only_explicit_v05_mutation_actions()
    {
        var names = Enum.GetNames<AuthorizationAction>();

        Assert.Contains(nameof(AuthorizationAction.RecordRead), names);
        Assert.Contains(nameof(AuthorizationAction.RecordExport), names);

        var expectedMutations = new[]
        {
            nameof(AuthorizationAction.TopicCreate),
            nameof(AuthorizationAction.TopicAlter),
            nameof(AuthorizationAction.TopicDelete),
            nameof(AuthorizationAction.RecordProduce),
            nameof(AuthorizationAction.ConsumerOffsetAlter),
            nameof(AuthorizationAction.ConsumerDelete),
            nameof(AuthorizationAction.SchemaCreate),
            nameof(AuthorizationAction.SchemaAlter),
            nameof(AuthorizationAction.SchemaDelete),
            nameof(AuthorizationAction.ConnectCreate),
            nameof(AuthorizationAction.ConnectAlter),
            nameof(AuthorizationAction.ConnectDelete),
            nameof(AuthorizationAction.RecordsPurge),
        };

        Assert.All(expectedMutations, action => Assert.Contains(action, names));
        Assert.DoesNotContain(names, action => string.Equals(action, "Admin", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(names, action => string.Equals(action, "Write", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Read_permission_does_not_imply_mutation_permission()
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
            new AuthorizationRequest(AuthorizationAction.TopicDelete, "prod", "payments"));

        Assert.False(decision.IsAllowed);
        Assert.Equal(AuthorizationDecisionReason.ActionDenied, decision.Reason);
    }

    [Fact]
    public void Configuration_loader_parses_record_permissions()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Authorization:Roles:0:Id"] = "records",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "record.read",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:ClusterIds:0"] = "prod",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:ResourcePatterns:0"] = "payments.*",
            ["Kafdeck:Authorization:Roles:0:Permissions:1:Action"] = "record.export",
            ["Kafdeck:Authorization:Roles:0:Permissions:1:ClusterIds:0"] = "prod",
            ["Kafdeck:Authorization:Roles:0:Permissions:1:ResourcePatterns:0"] = "payments.audit",
            ["Kafdeck:Authorization:SubjectBindings:0:Issuer"] = "https://idp.example",
            ["Kafdeck:Authorization:SubjectBindings:0:Subject"] = "alice",
            ["Kafdeck:Authorization:SubjectBindings:0:RoleIds:0"] = "records",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        var evaluator = new AuthorizationPolicyEvaluator(
            AuthorizationPolicyCompiler.Compile(AuthorizationPolicyConfigurationLoader.Load(configuration)));
        var identity = Identity("https://idp.example", "alice");

        Assert.True(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.RecordRead, "prod", "payments.created")).IsAllowed);
        Assert.True(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.RecordExport, "prod", "payments.audit")).IsAllowed);
        Assert.False(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.RecordExport, "prod", "payments.created")).IsAllowed);
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
