using System.Security.Claims;
using Kafdeck.Api;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W65DataQualityApiTests
{
    [Fact]
    public void Valid_sqlite_data_quality_configuration_is_admitted()
    {
        var options =
            Options(
                new DataQualityOptions(
                    Enabled: true,
                    ManagementEnabled: true,
                    Provider:
                        DataQualityPersistenceProvider.Sqlite,
                    ExecutionMode:
                        DataQualityExecutionMode.Standalone,
                    SqliteDatabasePath:
                        Path.Combine(
                            Path.GetTempPath(),
                            "kafdeck-data-quality.db"),
                    ConnectionString: null),
                AccessMode.Oidc);

        KafdeckConfigurationValidator
            .ValidateAndThrow(
                options);
    }

    [Fact]
    public void Data_quality_management_requires_oidc()
    {
        var options =
            Options(
                new DataQualityOptions(
                    Enabled: true,
                    ManagementEnabled: true,
                    Provider:
                        DataQualityPersistenceProvider.Sqlite,
                    ExecutionMode:
                        DataQualityExecutionMode.Standalone,
                    SqliteDatabasePath:
                        Path.Combine(
                            Path.GetTempPath(),
                            "kafdeck-data-quality.db"),
                    ConnectionString: null),
                AccessMode.Local);

        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () =>
                    KafdeckConfigurationValidator
                        .ValidateAndThrow(
                            options));

        Assert.Contains(
            "requires OIDC access mode",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Disabled_data_quality_rejects_persistence_material()
    {
        var options =
            Options(
                new DataQualityOptions(
                    Enabled: false,
                    ManagementEnabled: false,
                    Provider:
                        DataQualityPersistenceProvider.Sqlite,
                    ExecutionMode:
                        DataQualityExecutionMode.Standalone,
                    SqliteDatabasePath:
                        Path.Combine(
                            Path.GetTempPath(),
                            "must-not-be-used.db"),
                    ConnectionString: null),
                AccessMode.Local);

        var exception =
            Assert.Throws<KafdeckConfigurationException>(
                () =>
                    KafdeckConfigurationValidator
                        .ValidateAndThrow(
                            options));

        Assert.Contains(
            "must not be configured while data-quality is disabled",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Read_only_activation_maps_only_bounded_get_surface()
    {
        var app =
            BuildApp(
                managementEnabled: false);

        var routes =
            Routes(app);

        Assert.Equal(
            3,
            routes.Length);
        Assert.All(
            routes,
            route =>
                Assert.Equal(
                    "GET",
                    route.Method));
        Assert.Contains(
            routes,
            route =>
                route.Pattern ==
                "/api/v1/clusters/{clusterId}/data-quality/policies");
        Assert.Contains(
            routes,
            route =>
                route.Pattern ==
                "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}");
        Assert.Contains(
            routes,
            route =>
                route.Pattern ==
                "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}/evidence");
    }

    [Fact]
    public void Managed_activation_adds_only_revision_guarded_put_surface()
    {
        var app =
            BuildApp(
                managementEnabled: true);

        var routes =
            Routes(app);

        Assert.Equal(
            5,
            routes.Length);
        Assert.Equal(
            3,
            routes.Count(
                route =>
                    route.Method == "GET"));
        Assert.Equal(
            2,
            routes.Count(
                route =>
                    route.Method == "PUT"));
        Assert.Contains(
            routes,
            route =>
                route.Method == "PUT" &&
                route.Pattern ==
                "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}");
        Assert.Contains(
            routes,
            route =>
                route.Method == "PUT" &&
                route.Pattern ==
                "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}/state");
    }

    [Fact]
    public void Upsert_request_binds_policy_identity_and_cluster_from_route()
    {
        var request =
            new DataQualityPolicyUpsertRequest(
                Version: 2,
                TopicName: "orders",
                Partitions: [0, 1],
                Rules:
                [
                    new DataQualityRuleRequest(
                        "id-required",
                        DataQualityRuleKind.RequiredPath,
                        "/id",
                        null,
                        null,
                        null,
                        null,
                        null),
                ],
                Budget:
                    new DataQualityBudgetRequest(
                        100,
                        1024,
                        60,
                        20,
                        2),
                State:
                    DataQualityPolicyLifecycleState.Active,
                ExpectedRevision: 1);

        var definition =
            request.BuildDefinition(
                "prod",
                "orders-quality");

        Assert.Equal(
            "orders-quality",
            definition.PolicyId);
        Assert.Equal(
            "prod",
            definition.Scope.ClusterId);
        Assert.Equal(
            "orders",
            definition.Scope.TopicName);
        Assert.Equal(
            [0, 1],
            definition.Scope.Partitions);
        Assert.Equal(
            2,
            definition.Version);
    }

    [Fact]
    public void Underlying_topic_visibility_is_independent_from_policy_visibility()
    {
        var definition =
            new AuthorizationPolicyDefinition(
                [
                    new AuthorizationRoleDefinition(
                        "quality-reader",
                        [
                            new AuthorizationPermissionDefinition(
                                AuthorizationAction.ClusterRead,
                                ["prod"]),
                            new AuthorizationPermissionDefinition(
                                AuthorizationAction.DataQualityRead,
                                ["prod"],
                                ["*"]),
                            new AuthorizationPermissionDefinition(
                                AuthorizationAction.DataQualityManage,
                                ["prod"],
                                ["*"]),
                            new AuthorizationPermissionDefinition(
                                AuthorizationAction.TopicRead,
                                ["prod"],
                                ["payments*"]),
                        ]),
                ],
                [
                    new AuthorizationSubjectBindingDefinition(
                        "https://idp.example",
                        "alice",
                        ["quality-reader"]),
                ],
                Array.Empty<
                    AuthorizationGroupBindingDefinition>());

        var authorization =
            new KafdeckAuthorizationService(
                new KafdeckOptions(
                    new DeploymentOptions(
                        "http://127.0.0.1:8080",
                        null,
                        AccessMode.Oidc,
                        null),
                    Array.Empty<ClusterProfile>()),
                new AuthorizationPolicyEvaluator(
                    AuthorizationPolicyCompiler.Compile(
                        definition)));
        var principal =
            CreateOperatorPrincipal(
                "alice");

        Assert.Equal(
            KafdeckAuthorizationOutcome.Allowed,
            authorization.Authorize(
                principal,
                new AuthorizationRequest(
                    AuthorizationAction.DataQualityRead,
                    "prod",
                    "secret-policy")));

        Assert.Equal(
            KafdeckAuthorizationOutcome.Allowed,
            authorization.Authorize(
                principal,
                new AuthorizationRequest(
                    AuthorizationAction.TopicRead,
                    "prod",
                    "payments")));

        Assert.Equal(
            KafdeckAuthorizationOutcome.Forbidden,
            authorization.Authorize(
                principal,
                new AuthorizationRequest(
                    AuthorizationAction.TopicRead,
                    "prod",
                    "secret-topic")));
    }

    [Fact]
    public void Data_quality_authorization_actions_are_explicit_and_distinct()
    {
        Assert.NotEqual(
            AuthorizationAction.DataQualityRead,
            AuthorizationAction.DataQualityManage);
        Assert.True(
            Enum.IsDefined(
                AuthorizationAction.DataQualityRead));
        Assert.True(
            Enum.IsDefined(
                AuthorizationAction.DataQualityManage));
    }

    private static WebApplication BuildApp(
        bool managementEnabled)
    {
        var builder =
            WebApplication.CreateBuilder();

        var options =
            Options(
                new DataQualityOptions(
                    Enabled: true,
                    ManagementEnabled:
                        managementEnabled,
                    Provider:
                        DataQualityPersistenceProvider.Sqlite,
                    ExecutionMode:
                        DataQualityExecutionMode.Standalone,
                    SqliteDatabasePath:
                        Path.Combine(
                            Path.GetTempPath(),
                            "kafdeck-data-quality-api.db"),
                    ConnectionString: null),
                managementEnabled
                    ? AccessMode.Oidc
                    : AccessMode.Local);

        builder.Services.AddSingleton(
            new KafdeckAuthorizationService(
                options,
                new AuthorizationPolicyEvaluator(
                    AuthorizationPolicyCompiler.Compile(
                        new AuthorizationPolicyDefinition(
                            Array.Empty<
                                AuthorizationRoleDefinition>(),
                            Array.Empty<
                                AuthorizationSubjectBindingDefinition>(),
                            Array.Empty<
                                AuthorizationGroupBindingDefinition>())))));
        builder.Services.AddAntiforgery();

        var app =
            builder.Build();
        app.MapKafdeckV08DataQuality(
            options);
        return app;
    }

    private static ApiRoute[] Routes(
        WebApplication app) =>
        ((IEndpointRouteBuilder)app)
        .DataSources
        .SelectMany(
            source =>
                source.Endpoints)
        .OfType<RouteEndpoint>()
        .Where(
            endpoint =>
                endpoint.RoutePattern.RawText?
                    .Contains(
                        "/data-quality/",
                        StringComparison.Ordinal) ==
                true)
        .Select(
            endpoint =>
                new ApiRoute(
                    endpoint.Metadata
                        .GetMetadata<HttpMethodMetadata>()?
                        .HttpMethods
                        .Single() ??
                    string.Empty,
                    endpoint.RoutePattern.RawText ??
                    string.Empty))
        .OrderBy(
            route =>
                route.Pattern,
            StringComparer.Ordinal)
        .ThenBy(
            route =>
                route.Method,
            StringComparer.Ordinal)
        .ToArray();

    private static KafdeckOptions Options(
        DataQualityOptions dataQuality,
        AccessMode accessMode)
    {
        var deployment =
            accessMode == AccessMode.Oidc
                ? new DeploymentOptions(
                    "http://127.0.0.1:8080",
                    null,
                    AccessMode.Oidc,
                    new OidcProfile(
                        "http://127.0.0.1:5555",
                        "kafdeck-tests",
                        null,
                        null,
                        ["openid"]))
                : new DeploymentOptions(
                    "http://127.0.0.1:8080",
                    null);

        return new KafdeckOptions(
            deployment,
            Array.Empty<ClusterProfile>(),
            DataQuality:
                dataQuality);
    }

    private static ClaimsPrincipal CreateOperatorPrincipal(
        string subject)
    {
        var external =
            new ClaimsPrincipal(
                new ClaimsIdentity(
                    [
                        new Claim(
                            "sub",
                            subject),
                        new Claim(
                            "name",
                            subject),
                    ],
                    authenticationType:
                        "oidc"));

        return OidcIdentityNormalizer.Normalize(
            external,
            "https://idp.example",
            groupClaim: null,
            DateTimeOffset.UtcNow);
    }

    private sealed record ApiRoute(
        string Method,
        string Pattern);
}
