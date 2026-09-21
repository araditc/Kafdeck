using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class AuthorizationEnforcementTests
{
    [Theory]
    [InlineData(AccessMode.Local)]
    [InlineData(AccessMode.Token)]
    public void Legacy_modes_preserve_existing_boundary_without_operator_identity(AccessMode mode)
    {
        var service = CreateService(mode);

        var outcome = service.Authorize(
            null,
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod"));

        Assert.Equal(KafdeckAuthorizationOutcome.Allowed, outcome);
    }

    [Fact]
    public void Oidc_mode_fails_unauthenticated_requests_closed()
    {
        var service = CreateService(AccessMode.Oidc);

        var outcome = service.Authorize(
            null,
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod"));

        Assert.Equal(KafdeckAuthorizationOutcome.Unauthenticated, outcome);
    }

    [Fact]
    public void Arbitrary_authenticated_principal_cannot_bypass_oidc_operator_identity()
    {
        var service = CreateService(AccessMode.Oidc);
        var principal = new ClaimsPrincipal(new ClaimsIdentity(
            new[] { new Claim(ClaimTypes.Name, "deployment-token") },
            authenticationType: "legacy-token"));

        var outcome = service.Authorize(
            principal,
            new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod"));

        Assert.Equal(KafdeckAuthorizationOutcome.Unauthenticated, outcome);
    }

    [Fact]
    public void Oidc_operator_is_allowed_only_for_granted_cluster()
    {
        var service = CreateService(AccessMode.Oidc);
        var principal = CreateOperatorPrincipal("alice");

        Assert.Equal(
            KafdeckAuthorizationOutcome.Allowed,
            service.Authorize(
                principal,
                new AuthorizationRequest(AuthorizationAction.ClusterRead, "prod")));

        Assert.Equal(
            KafdeckAuthorizationOutcome.Forbidden,
            service.Authorize(
                principal,
                new AuthorizationRequest(AuthorizationAction.ClusterRead, "secret")));
    }

    [Fact]
    public void Dedicated_configuration_permission_is_enforced()
    {
        var service = CreateService(AccessMode.Oidc);
        var principal = CreateOperatorPrincipal("alice");

        var outcome = service.Authorize(
            principal,
            new AuthorizationRequest(AuthorizationAction.TopicConfigRead, "prod", "payments"));

        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, outcome);
    }


    [Fact]
    public void Collection_preflight_requires_an_applicable_action_in_the_cluster()
    {
        var service = CreateService(AccessMode.Oidc);
        var principal = CreateOperatorPrincipal("alice");

        Assert.Equal(
            KafdeckAuthorizationOutcome.Allowed,
            service.AuthorizeCollection(
                principal,
                AuthorizationAction.ConsumerRead,
                "prod"));

        Assert.Equal(
            KafdeckAuthorizationOutcome.Forbidden,
            service.AuthorizeCollection(
                principal,
                AuthorizationAction.ConsumerRead,
                "secret"));

        Assert.Equal(
            KafdeckAuthorizationOutcome.Forbidden,
            service.AuthorizeCollection(
                principal,
                AuthorizationAction.SchemaRead,
                "prod"));
    }

    [Fact]
    public void Collection_preflight_does_not_bypass_per_resource_patterns()
    {
        var service = CreateService(AccessMode.Oidc);
        var principal = CreateOperatorPrincipal("alice");

        Assert.Equal(
            KafdeckAuthorizationOutcome.Allowed,
            service.AuthorizeCollection(
                principal,
                AuthorizationAction.ConsumerRead,
                "prod"));

        Assert.Equal(
            KafdeckAuthorizationOutcome.Allowed,
            service.Authorize(
                principal,
                new AuthorizationRequest(
                    AuthorizationAction.ConsumerRead,
                    "prod",
                    "payments-worker")));

        Assert.Equal(
            KafdeckAuthorizationOutcome.Forbidden,
            service.Authorize(
                principal,
                new AuthorizationRequest(
                    AuthorizationAction.ConsumerRead,
                    "prod",
                    "audit-worker")));
    }

    private static KafdeckAuthorizationService CreateService(AccessMode mode)
    {
        var definition = new AuthorizationPolicyDefinition(
            new[]
            {
                new AuthorizationRoleDefinition(
                    "viewer",
                    new[]
                    {
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.ClusterRead,
                            new[] { "prod" }),
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.TopicRead,
                            new[] { "prod" },
                            new[] { "payments*" }),
                        new AuthorizationPermissionDefinition(
                            AuthorizationAction.ConsumerRead,
                            new[] { "prod" },
                            new[] { "payments*" }),
                    }),
            },
            new[]
            {
                new AuthorizationSubjectBindingDefinition(
                    "https://idp.example",
                    "alice",
                    new[] { "viewer" }),
            },
            Array.Empty<AuthorizationGroupBindingDefinition>());

        var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(definition));
        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null, mode, null),
            Array.Empty<ClusterProfile>());

        return new KafdeckAuthorizationService(options, evaluator);
    }

    private static ClaimsPrincipal CreateOperatorPrincipal(string subject)
    {
        var external = new ClaimsPrincipal(new ClaimsIdentity(
            new[] { new Claim("sub", subject), new Claim("name", subject) },
            authenticationType: "oidc"));

        return OidcIdentityNormalizer.Normalize(
            external,
            "https://idp.example",
            groupClaim: null,
            DateTimeOffset.UtcNow);
    }
}
