using Kafdeck.Core.Observability;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

public static class KafdeckOperationalAnalyticsEndpoints
{
    public static IEndpointRouteBuilder
        MapKafdeckV08OperationalAnalytics(
            this IEndpointRouteBuilder endpoints,
            KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(options);

        endpoints.MapGet(
                "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/analytics/live",
                async (
                    string clusterId,
                    string groupId,
                    OperationalAnalyticsRuntimeService analytics,
                    CancellationToken cancellationToken) =>
                {
                    if (!options.Clusters.Any(cluster =>
                            string.Equals(
                                cluster.Id,
                                clusterId,
                                StringComparison.Ordinal)))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper
                                .InvalidClusterId(
                                    clusterId));
                    }

                    var query =
                        new OperationalAnalyticsQuery(
                            clusterId,
                            OperationalResourceKind.ConsumerGroup,
                            groupId,
                            [
                                OperationalMetricKind.ConsumerLagTotal,
                                OperationalMetricKind.ConsumerConsumeRecordsPerSecond,
                            ],
                            MaxItems: 8);
                    var result =
                        await analytics
                            .QueryAsync(
                                query,
                                new ReadViewOperationContext(
                                    DateTimeOffset.UtcNow
                                        .AddSeconds(10),
                                    maxItems: 8,
                                    maxResponseBytes:
                                        256 * 1024),
                                cancellationToken)
                            .ConfigureAwait(false);

                    return result.IsSuccess &&
                           result.Value is not null
                        ? Results.Ok(result.Value)
                        : ApiResults.Problem(
                            ApiProblemMapper.FromReadView(
                                result.Failure!));
                })
            .WithName("v08-consumer-operational-analytics-live")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ConsumerRead,
                "clusterId",
                "groupId");

        endpoints.MapGet(
                "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/analytics/trend",
                async (
                    string clusterId,
                    string groupId,
                    DateTimeOffset? from,
                    DateTimeOffset? to,
                    int? maxPoints,
                    OperationalTrendService trends,
                    CancellationToken cancellationToken) =>
                {
                    if (!options.Clusters.Any(cluster =>
                            string.Equals(
                                cluster.Id,
                                clusterId,
                                StringComparison.Ordinal)))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper
                                .InvalidClusterId(
                                    clusterId));
                    }

                    var toUtc =
                        to ??
                        DateTimeOffset.UtcNow;
                    var fromUtc =
                        from ??
                        toUtc.AddHours(-1);

                    try
                    {
                        var result =
                            await trends.QueryAsync(
                                    new OperationalTrendQuery(
                                        OperationalMetricKind
                                            .ConsumerLagTotal,
                                        new OperationalResourceIdentity(
                                            clusterId,
                                            OperationalResourceKind
                                                .ConsumerGroup,
                                            groupId),
                                        fromUtc,
                                        toUtc,
                                        maxPoints ?? 1_000),
                                    cancellationToken)
                                .ConfigureAwait(false);
                        return Results.Ok(result);
                    }
                    catch (ArgumentException exception)
                    {
                        return Results.BadRequest(
                            new
                            {
                                code =
                                    "invalid_operational_trend_query",
                                message =
                                    exception.Message,
                            });
                    }
                })
            .WithName("v08-consumer-operational-analytics-trend")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ConsumerRead,
                "clusterId",
                "groupId");

        endpoints.MapGet(
                "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/analytics/slo",
                async (
                    string clusterId,
                    string groupId,
                    double threshold,
                    double target,
                    DateTimeOffset? from,
                    DateTimeOffset? to,
                    int? maxPoints,
                    OperationalTrendService trends,
                    CancellationToken cancellationToken) =>
                {
                    if (!options.Clusters.Any(cluster =>
                            string.Equals(
                                cluster.Id,
                                clusterId,
                                StringComparison.Ordinal)))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper
                                .InvalidClusterId(
                                    clusterId));
                    }

                    var toUtc =
                        to ??
                        DateTimeOffset.UtcNow;
                    var fromUtc =
                        from ??
                        toUtc.AddHours(-1);

                    try
                    {
                        var result =
                            await trends.EvaluateSloAsync(
                                    new OperationalSloDefinition(
                                        "consumer-lag",
                                        OperationalMetricKind
                                            .ConsumerLagTotal,
                                        new OperationalResourceIdentity(
                                            clusterId,
                                            OperationalResourceKind
                                                .ConsumerGroup,
                                            groupId),
                                        threshold,
                                        target),
                                    fromUtc,
                                    toUtc,
                                    maxPoints ?? 1_000,
                                    cancellationToken)
                                .ConfigureAwait(false);
                        return Results.Ok(result);
                    }
                    catch (ArgumentException exception)
                    {
                        return Results.BadRequest(
                            new
                            {
                                code =
                                    "invalid_operational_slo_query",
                                message =
                                    exception.Message,
                            });
                    }
                })
            .WithName("v08-consumer-operational-slo")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ConsumerRead,
                "clusterId",
                "groupId");

        return endpoints;
    }
}
