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

        app.MapGet(
                "/api/v1/clusters/{clusterId}/streams/applications",
                async (
                    string clusterId,
                    HttpContext context,
                    KafdeckAuthorizationService authorization,
                    IStreamsTelemetryReadPort streams,
                    CancellationToken cancellationToken) =>
                {
                    if (!IsConfiguredCluster(options, clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(clusterId));
                    }

                    var result = await streams
                        .ListApplicationsAsync(
                            clusterId,
                            EcosystemOperation(),
                            cancellationToken)
                        .ConfigureAwait(false);

                    if (!result.IsSuccess || result.Value is null)
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.FromReadView(result.Failure!));
                    }

                    var visible = result.Value
                        .Where(application =>
                            authorization.Authorize(
                                context.User,
                                new AuthorizationRequest(
                                    AuthorizationAction.StreamsRead,
                                    clusterId,
                                    application.ApplicationId)) ==
                            KafdeckAuthorizationOutcome.Allowed)
                        .ToArray();

                    var limitations = AddAuthorizationFilterLimitation(
                        result.Limitations,
                        result.Value.Count,
                        visible.Length);

                    return Results.Ok(
                        ReadViewApiMapper.Envelope(
                            visible,
                            limitations));
                })
            .WithName("v07-streams-applications")
            .RequireKafdeckCollectionAuthorization(
                AuthorizationAction.StreamsRead,
                "clusterId");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/streams/applications/{applicationId}/topology",
                async (
                    string clusterId,
                    string applicationId,
                    IStreamsTelemetryReadPort streams,
                    CancellationToken cancellationToken) =>
                {
                    if (!IsConfiguredCluster(options, clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(clusterId));
                    }

                    var result = await streams
                        .GetTopologyAsync(
                            clusterId,
                            applicationId,
                            EcosystemOperation(),
                            cancellationToken)
                        .ConfigureAwait(false);
                    return ToReadViewResult(result);
                })
            .WithName("v07-streams-topology")
            .RequireKafdeckAuthorization(
                AuthorizationAction.StreamsRead,
                "clusterId",
                "applicationId");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/streams/applications/{applicationId}/state-stores",
                async (
                    string clusterId,
                    string applicationId,
                    IStreamsTelemetryReadPort streams,
                    CancellationToken cancellationToken) =>
                {
                    if (!IsConfiguredCluster(options, clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(clusterId));
                    }

                    var result = await streams
                        .GetStateStoresAsync(
                            clusterId,
                            applicationId,
                            EcosystemOperation(),
                            cancellationToken)
                        .ConfigureAwait(false);
                    return ToReadViewResult(result);
                })
            .WithName("v07-streams-state-stores")
            .RequireKafdeckAuthorization(
                AuthorizationAction.StreamsRead,
                "clusterId",
                "applicationId");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/lineage",
                async (
                    string clusterId,
                    HttpContext context,
                    KafdeckAuthorizationService authorization,
                    ILineageReadPort lineage,
                    CancellationToken cancellationToken) =>
                {
                    if (!IsConfiguredCluster(options, clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(clusterId));
                    }

                    var result = await lineage
                        .GetLineageAsync(
                            clusterId,
                            EcosystemOperation(),
                            cancellationToken)
                        .ConfigureAwait(false);

                    if (!result.IsSuccess || result.Value is null)
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.FromReadView(result.Failure!));
                    }

                    var visibleEdges = result.Value.Edges
                        .Where(edge =>
                            IsLineageEntityAllowed(
                                authorization,
                                context,
                                clusterId,
                                edge.Source) &&
                            IsLineageEntityAllowed(
                                authorization,
                                context,
                                clusterId,
                                edge.Destination))
                        .ToArray();

                    var limitations = AddAuthorizationFilterLimitation(
                        result.Limitations,
                        result.Value.Edges.Count,
                        visibleEdges.Length);

                    var filtered = result.Value with
                    {
                        Edges = visibleEdges,
                        Partial = result.Value.Partial ||
                                  visibleEdges.Length <
                                  result.Value.Edges.Count,
                        Limitations = limitations,
                    };

                    return Results.Ok(
                        ReadViewApiMapper.Envelope(
                            filtered,
                            limitations));
                })
            .WithName("v07-lineage")
            .RequireKafdeckCollectionAuthorization(
                AuthorizationAction.LineageRead,
                "clusterId");

        return app;
    }

    private static bool IsConfiguredCluster(
        KafdeckOptions options,
        string clusterId) =>
        options.Clusters.Any(cluster =>
            string.Equals(
                cluster.Id,
                clusterId,
                StringComparison.Ordinal));

    private static ReadViewOperationContext EcosystemOperation() =>
        new(
            DateTimeOffset.UtcNow.AddSeconds(10),
            maxItems: 2_000,
            maxResponseBytes: 4 * 1024 * 1024);

    private static bool IsLineageEntityAllowed(
        KafdeckAuthorizationService authorization,
        HttpContext context,
        string clusterId,
        LineageEntity entity) =>
        authorization.Authorize(
            context.User,
            new AuthorizationRequest(
                AuthorizationAction.LineageRead,
                clusterId,
                $"{entity.Kind}:{entity.Id}")) ==
        KafdeckAuthorizationOutcome.Allowed;

    private static IReadOnlyList<ReadViewLimitation>
        AddAuthorizationFilterLimitation(
            IReadOnlyList<ReadViewLimitation> limitations,
            int upstreamCount,
            int visibleCount)
    {
        if (visibleCount >= upstreamCount)
        {
            return limitations;
        }

        var combined = new List<ReadViewLimitation>(
            limitations.Count + 1);
        combined.AddRange(limitations);
        combined.Add(
            new ReadViewLimitation(
                "authorization_filtered",
                "One or more resources were omitted because the authenticated operator is not authorized to view them."));
        return combined;
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
