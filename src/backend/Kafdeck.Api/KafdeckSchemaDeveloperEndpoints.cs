using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Schemas;

namespace Kafdeck.Api;

public sealed record SchemaMockRequest(
    string Subject,
    int Version,
    int Count,
    int? Seed);

public static class KafdeckSchemaDeveloperEndpoints
{
    public static WebApplication MapKafdeckV07SchemaDeveloperTools(
        this WebApplication app,
        KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(options);

        app.MapGet(
                "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/references",
                async (
                    string clusterId,
                    string subject,
                    int version,
                    HttpContext context,
                    KafdeckAuthorizationService authorization,
                    SchemaDeveloperService tooling,
                    CancellationToken cancellationToken) =>
                {
                    if (!IsConfiguredCluster(options, clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(clusterId));
                    }

                    var result = await tooling
                        .BuildReferenceGraphAsync(
                            clusterId,
                            subject,
                            version,
                            cancellationToken,
                            candidateSubject =>
                                authorization.Authorize(
                                    context.User,
                                    new AuthorizationRequest(
                                        AuthorizationAction.SchemaRead,
                                        clusterId,
                                        candidateSubject)) ==
                                KafdeckAuthorizationOutcome.Allowed)
                        .ConfigureAwait(false);

                    return ToReadViewResult(result);
                })
            .WithName("v07-schema-reference-graph")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SchemaRead,
                "clusterId",
                "subject");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/compatibility/explanation",
                async (
                    string clusterId,
                    string subject,
                    SchemaDeveloperService tooling,
                    CancellationToken cancellationToken) =>
                {
                    if (!IsConfiguredCluster(options, clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(clusterId));
                    }

                    var result = await tooling
                        .ExplainCompatibilityAsync(
                            clusterId,
                            subject,
                            cancellationToken)
                        .ConfigureAwait(false);

                    return ToReadViewResult(result);
                })
            .WithName("v07-schema-compatibility-explanation")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SchemaRead,
                "clusterId",
                "subject");

        var mock = app.MapPost(
                "/api/v1/clusters/{clusterId}/schemas/mock",
                async (
                    string clusterId,
                    SchemaMockRequest request,
                    HttpContext context,
                    KafdeckAuthorizationService authorization,
                    ISecurityAuditSink audit,
                    SchemaDeveloperService tooling,
                    CancellationToken cancellationToken) =>
                {
                    if (!IsConfiguredCluster(options, clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(clusterId));
                    }

                    if (string.IsNullOrWhiteSpace(request.Subject))
                    {
                        return ApiResults.Problem(
                            new ApiProblemDefinition(
                                StatusCodes.Status400BadRequest,
                                "urn:kafdeck:problem:invalid-schema-mock-request",
                                "Invalid schema mock request",
                                "A non-empty schema subject is required.",
                                "invalid_schema_mock_request"));
                    }

                    var normalizedSubject = request.Subject.Trim();
                    var outcome = authorization.Authorize(
                        context.User,
                        new AuthorizationRequest(
                            AuthorizationAction.SchemaRead,
                            clusterId,
                            normalizedSubject));

                    if (outcome != KafdeckAuthorizationOutcome.Allowed)
                    {
                        if (outcome == KafdeckAuthorizationOutcome.Forbidden)
                        {
                            var principal =
                                OperatorSessionContextFactory.TryCreate(
                                    context.User,
                                    out var session) &&
                                session is not null
                                    ? SecurityAuditPrincipal.FromOperator(
                                        session.Identity)
                                    : SecurityAuditPrincipal.Anonymous;

                            await audit
                                .WriteAsync(
                                    new SecurityAuditEvent(
                                        DateTimeOffset.UtcNow,
                                        SecurityAuditEventType.AuthorizationDenied,
                                        principal,
                                        session?.SessionId.Value.ToString("N"),
                                        clusterId,
                                        normalizedSubject,
                                        SecurityAuditOutcome.Denied,
                                        "rbac_denied_schema_mock"),
                                    cancellationToken)
                                .ConfigureAwait(false);
                        }

                        return outcome ==
                               KafdeckAuthorizationOutcome.Unauthenticated
                            ? Results.Problem(
                                statusCode:
                                    StatusCodes.Status401Unauthorized,
                                type:
                                    "urn:kafdeck:problem:operator-authentication-required",
                                title: "Authentication required",
                                detail:
                                    "An authenticated operator session is required.")
                            : Results.Problem(
                                statusCode:
                                    StatusCodes.Status403Forbidden,
                                type:
                                    "urn:kafdeck:problem:operator-authorization-denied",
                                title: "Forbidden",
                                detail:
                                    "The authenticated operator is not authorized to use schema developer tooling for this subject.");
                    }

                    var result = await tooling
                        .GenerateMockAsync(
                            clusterId,
                            normalizedSubject,
                            request.Version,
                            request.Count,
                            request.Seed,
                            cancellationToken)
                        .ConfigureAwait(false);

                    return ToReadViewResult(result);
                })
            .WithName("v07-schema-mock");

        if (options.Deployment.Mode == AccessMode.Oidc)
        {
            mock.RequireKafdeckAntiforgery();
        }

        return app;
    }

    private static IResult ToReadViewResult<T>(
        ReadViewResult<T> result)
    {
        if (!result.IsSuccess || result.Value is null)
        {
            return ApiResults.Problem(
                ApiProblemMapper.FromReadView(result.Failure!));
        }

        return Results.Ok(ReadViewApiMapper.Envelope(result));
    }

    private static bool IsConfiguredCluster(
        KafdeckOptions options,
        string clusterId) =>
        options.Clusters.Any(
            cluster => string.Equals(
                cluster.Id,
                clusterId,
                StringComparison.Ordinal));
}
