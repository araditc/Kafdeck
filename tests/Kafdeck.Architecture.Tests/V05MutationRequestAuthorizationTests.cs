using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05MutationRequestAuthorizationTests
{
    [Fact]
    public void Dispatch_recheck_requires_every_frozen_authorization_target()
    {
        var service = CreateService(
            new AuthorizationPermissionDefinition(
                AuthorizationAction.TopicDelete,
                new[] { "prod" },
                new[] { "payments.*" }));

        var operation = CreateOperation(
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.TopicDelete,
                    "prod",
                    "payments.events"),
                new MutationAuthorizationTarget(
                    AuthorizationAction.TopicDelete,
                    "prod",
                    "payments.audit"),
            });

        var outcome = service.AuthorizeForDispatch(
            CreateOperatorPrincipal("alice"),
            operation);

        Assert.Equal(KafdeckAuthorizationOutcome.Allowed, outcome);
    }

    [Fact]
    public void Dispatch_recheck_fails_closed_when_any_frozen_target_is_not_authorized()
    {
        var service = CreateService(
            new AuthorizationPermissionDefinition(
                AuthorizationAction.TopicDelete,
                new[] { "prod" },
                new[] { "payments.allowed" }));

        var operation = CreateOperation(
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.TopicDelete,
                    "prod",
                    "payments.allowed"),
                new MutationAuthorizationTarget(
                    AuthorizationAction.TopicDelete,
                    "prod",
                    "payments.denied"),
            });

        var outcome = service.AuthorizeForDispatch(
            CreateOperatorPrincipal("alice"),
            operation);

        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, outcome);
    }

    [Fact]
    public void Dispatch_recheck_does_not_rehydrate_missing_current_identity()
    {
        var service = CreateService(
            new AuthorizationPermissionDefinition(
                AuthorizationAction.TopicDelete,
                new[] { "prod" },
                new[] { "*" }));

        var operation = CreateOperation(
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.TopicDelete,
                    "prod",
                    "payments.events"),
            });

        var outcome = service.AuthorizeForDispatch(null, operation);

        Assert.Equal(KafdeckAuthorizationOutcome.Unauthenticated, outcome);
    }

    [Fact]
    public void Consumer_direct_route_preflight_requires_exact_group_and_every_explicit_topic()
    {
        var targets = MutationDirectRouteAuthorization.ConsumerOffsetAlter(
            "prod",
            "payments-worker",
            new[]
            {
                new ConsumerOffsetAlterTargetInput(
                    "payments.allowed",
                    0,
                    new ConsumerOffsetSelector(
                        ConsumerOffsetSelectorKind.Absolute,
                        Value: 10)),
                new ConsumerOffsetAlterTargetInput(
                    "payments.denied",
                    1,
                    new ConsumerOffsetSelector(
                        ConsumerOffsetSelectorKind.Latest)),
            });

        Assert.NotNull(targets);
        Assert.Contains(targets!, target =>
            target.ResourceName == "consumer-group/payments-worker");
        Assert.Contains(targets!, target =>
            target.ResourceName == "topic/payments.allowed");
        Assert.Contains(targets!, target =>
            target.ResourceName == "topic/payments.denied");

        var service = CreateService(
            new AuthorizationPermissionDefinition(
                AuthorizationAction.ConsumerOffsetAlter,
                new[] { "prod" },
                new[]
                {
                    "consumer-group/payments-worker",
                    "topic/payments.allowed",
                }));

        var outcome = service.AuthorizeTargets(
            CreateOperatorPrincipal("alice"),
            targets!);

        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, outcome);
    }

    [Fact]
    public void Whole_group_delete_preflight_requires_the_exact_group_before_inventory_observation()
    {
        var targets = MutationDirectRouteAuthorization.ConsumerDelete(
            "prod",
            "payments-worker",
            ConsumerDeleteMode.Group,
            targets: null);

        var target = Assert.Single(targets!);
        Assert.Equal(AuthorizationAction.ConsumerDelete, target.Action);
        Assert.Equal("prod", target.ClusterId);
        Assert.Equal("consumer-group/payments-worker", target.ResourceName);
    }

    [Fact]
    public void Purge_direct_route_preflight_requires_every_exact_topic_before_watermark_observation()
    {
        var targets = MutationDirectRouteAuthorization.RecordsPurge(
            "prod",
            new[]
            {
                new RecordsPurgeTargetInput(
                    "payments.allowed",
                    0,
                    new RecordsPurgeSelector(
                        RecordsPurgeSelectorKind.Absolute,
                        BeforeOffset: 10)),
                new RecordsPurgeTargetInput(
                    "payments.denied",
                    1,
                    new RecordsPurgeSelector(
                        RecordsPurgeSelectorKind.Absolute,
                        BeforeOffset: 20)),
            });

        Assert.NotNull(targets);
        Assert.Equal(
            new[] { "payments.allowed", "payments.denied" },
            targets!.Select(target => target.ResourceName).ToArray());

        var service = CreateService(
            new AuthorizationPermissionDefinition(
                AuthorizationAction.RecordsPurge,
                new[] { "prod" },
                new[] { "payments.allowed" }));

        var outcome = service.AuthorizeTargets(
            CreateOperatorPrincipal("alice"),
            targets);

        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, outcome);
    }

    [Fact]
    public void Malformed_direct_route_targets_do_not_produce_authorization_targets()
    {
        Assert.Null(MutationDirectRouteAuthorization.ConsumerOffsetAlter(
            "prod",
            "payments-worker",
            new[]
            {
                new ConsumerOffsetAlterTargetInput(
                    "../invalid",
                    0,
                    new ConsumerOffsetSelector(
                        ConsumerOffsetSelectorKind.Latest)),
            }));

        Assert.Null(MutationDirectRouteAuthorization.RecordsPurge(
            "prod",
            new[]
            {
                new RecordsPurgeTargetInput(
                    "bad/topic",
                    0,
                    new RecordsPurgeSelector(
                        RecordsPurgeSelectorKind.Absolute,
                        BeforeOffset: 10)),
            }));
    }

    private static MutationRequestAuthorizationService CreateService(
        params AuthorizationPermissionDefinition[] permissions)
    {
        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition(
                    "mutation-operator",
                    permissions),
            },
            new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "mutation-operator" }),
            },
            Array.Empty<AuthorizationGroupBindingDefinition>());

        var evaluator = new AuthorizationPolicyEvaluator(
            AuthorizationPolicyCompiler.Compile(definition));
        var options = new KafdeckOptions(
            new DeploymentOptions(
                "http://127.0.0.1:8080",
                null,
                AccessMode.Oidc,
                null),
            Array.Empty<ClusterProfile>());

        return new MutationRequestAuthorizationService(
            new KafdeckAuthorizationService(options, evaluator));
    }

    private static MutationOperationSnapshot CreateOperation(
        IReadOnlyList<MutationAuthorizationTarget> targets)
    {
        var intent = new MutationIntentDescriptor(
            MutationOperationKind.TopicDelete,
            "prod",
            "{\"operation\":\"topic-delete\"}",
            targets.Select(target => $"topic/{target.ResourceName}").ToArray(),
            Preconditions:
                new[] { new MutationPrecondition("metadata", "sha256:abc") },
            AuthorizationTargets: targets);

        return MutationOperation.CreatePreview(
                "oidc:https://idp.example|alice",
                intent,
                MutationRiskClassifier.Classify(
                    new MutationRiskInput(MutationOperationKind.TopicDelete)),
                "v0.5-w39",
                DateTimeOffset.UtcNow.AddMinutes(5),
                DateTimeOffset.UtcNow,
                "dispatch-recheck")
            .Snapshot;
    }

    private static ClaimsPrincipal CreateOperatorPrincipal(string subject)
    {
        var external = new ClaimsPrincipal(
            new ClaimsIdentity(
                new[]
                {
                    new Claim("sub", subject),
                    new Claim("name", subject),
                },
                authenticationType: "oidc"));

        return OidcIdentityNormalizer.Normalize(
            external,
            "https://idp.example",
            groupClaim: null,
            DateTimeOffset.UtcNow);
    }
}
