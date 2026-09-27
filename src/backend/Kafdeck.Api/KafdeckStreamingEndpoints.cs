using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

public sealed record KsqlQueryLimitRequest(
    int? MaxRows,
    long? MaxBytes,
    int? MaxDurationSeconds);

public sealed record KsqlQueryRequestBody(
    string Statement,
    KsqlQueryLimitRequest? Limits = null);

public static class KafdeckStreamingEndpoints
{
    public static WebApplication MapKafdeckV07Streaming(
        this WebApplication app,
        KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(options);

        var query = app.MapPost(
                "/api/v1/clusters/{clusterId}/ksql/query",
                async (
                    string clusterId,
                    KsqlQueryRequestBody request,
                    IKsqlQueryPort ksql,
                    CancellationToken cancellationToken) =>
                {
                    if (!options.Clusters.Any(cluster =>
                            string.Equals(
                                cluster.Id,
                                clusterId,
                                StringComparison.Ordinal)))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(clusterId));
                    }

                    if (string.IsNullOrWhiteSpace(request.Statement))
                    {
                        return ApiResults.Problem(
                            new ApiProblemDefinition(
                                StatusCodes.Status400BadRequest,
                                "urn:kafdeck:problem:invalid-ksql-query-request",
                                "Invalid ksqlDB query request",
                                "A non-empty SELECT statement is required.",
                                "ksql_statement_required"));
                    }

                    var limits = BuildLimits(request.Limits);
                    var result = await ksql
                        .ExecuteQueryAsync(
                            clusterId,
                            request.Statement,
                            limits,
                            cancellationToken)
                        .ConfigureAwait(false);

                    return ToReadViewResult(result);
                })
            .WithName("v07-ksql-query")
            .RequireKafdeckCollectionAuthorization(
                AuthorizationAction.KsqlRead,
                "clusterId");

        if (options.Deployment.Mode == AccessMode.Oidc)
        {
            query.RequireKafdeckAntiforgery();
        }

        return app;
    }

    private static KsqlQueryLimits BuildLimits(
        KsqlQueryLimitRequest? request)
    {
        var defaults = KsqlQueryLimits.Default;
        return new KsqlQueryLimits(
            request?.MaxRows ?? defaults.MaxRows,
            request?.MaxBytes ?? defaults.MaxBytes,
            TimeSpan.FromSeconds(
                request?.MaxDurationSeconds ??
                (int)defaults.MaxDuration.TotalSeconds));
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
}
