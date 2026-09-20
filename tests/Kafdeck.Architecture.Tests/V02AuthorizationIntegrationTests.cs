using System.Diagnostics;
using Kafdeck.Core.Security;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V02AuthorizationIntegrationTests
{
    [Fact]
    public void Subject_and_group_bindings_compose_without_expanding_ungranted_actions()
    {
        var policy = AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition("cluster", new[]
                {
                    new AuthorizationPermissionDefinition(AuthorizationAction.ClusterRead, new[] { "prod" }),
                }),
                new AuthorizationRoleDefinition("topics", new[]
                {
                    new AuthorizationPermissionDefinition(
                        AuthorizationAction.TopicRead,
                        new[] { "prod" },
                        new[] { "payments.*" }),
                }),
            },
            new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example", "alice", new[] { "cluster" }),
            },
            new[]
            {
                new AuthorizationGroupBindingDefinition("payments-team", new[] { "topics" }),
            }));

        var evaluator = new AuthorizationPolicyEvaluator(policy);
        var identity = new OperatorIdentity(
            new OperatorIdentityKey("https://idp.example", "alice"),
            externalGroups: new[] { "payments-team" });

        Assert.True(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod")).IsAllowed);
        Assert.True(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.TopicRead, "prod", "payments.created")).IsAllowed);
        Assert.False(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.TopicConfigRead, "prod", "payments.created")).IsAllowed);
        Assert.False(evaluator.Evaluate(
            identity,
            new AuthorizationRequest(AuthorizationAction.TopicRead, "secret", "payments.created")).IsAllowed);
    }

    [Fact]
    public void Authorization_evaluator_has_bounded_release_readiness_latency()
    {
        var permissions = Enumerable.Range(0, 64)
            .Select(index => new AuthorizationPermissionDefinition(
                AuthorizationAction.TopicRead,
                new[] { "prod" },
                new[] { $"topic-{index}-*" }))
            .ToArray();

        var policy = AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition(
            new[] { new AuthorizationRoleDefinition("reader", permissions) },
            new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example", "perf-user", new[] { "reader" }),
            },
            Array.Empty<AuthorizationGroupBindingDefinition>()));

        var evaluator = new AuthorizationPolicyEvaluator(policy);
        var identity = new OperatorIdentity(new OperatorIdentityKey("https://idp.example", "perf-user"));
        var request = new AuthorizationRequest(AuthorizationAction.TopicRead, "prod", "topic-63-orders");

        for (var i = 0; i < 1_000; i++)
        {
            Assert.True(evaluator.Evaluate(identity, request).IsAllowed);
        }

        const int iterations = 20_000;
        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; i < iterations; i++)
        {
            if (!evaluator.Evaluate(identity, request).IsAllowed)
            {
                throw new InvalidOperationException("Expected authorization request to remain allowed.");
            }
        }

        stopwatch.Stop();
        var averageMicroseconds = stopwatch.Elapsed.TotalMilliseconds * 1000d / iterations;

        // This is a deliberately loose regression ceiling, not a benchmark claim.
        // It detects accidental blocking/I/O or algorithmic explosions while remaining CI-host tolerant.
        Assert.True(
            averageMicroseconds < 1_000d,
            $"Authorization evaluation averaged {averageMicroseconds:F2} us, exceeding the 1000 us readiness ceiling.");
    }
}
