using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
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

    private static MutationRequestAuthorizationService CreateService(
        AuthorizationPermissionDefinition permission)
    {
        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition(
                    "mutation-operator",
                    new[] { permission }),
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
