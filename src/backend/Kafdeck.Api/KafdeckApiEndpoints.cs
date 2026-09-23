using System.Reflection;
using Kafdeck.Core;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Clusters;
using Kafdeck.Modules.Topics;

namespace Kafdeck.Api;

public static class KafdeckApiEndpoints
{
    private static readonly TimeSpan CapabilityTtl = TimeSpan.FromSeconds(30);

    public static string ResolveKafkaAdministrationMode(KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.Administration?.Mutations.Enabled == true
            ? "controlledMutations"
            : "readOnly";
    }

    public static WebApplication MapKafdeckV01(this WebApplication app, KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(options);

        app.MapGet("/openapi/v1.json", () =>
                Results.Text(V01ApiContract.OpenApiJson, "application/json; charset=utf-8"))
            .WithName("openapi-v01");

        app.MapGet("/api/v1/system/info", () => Results.Ok(new
            {
                name = ProductIdentity.Name,
                description = ProductIdentity.Description,
                version = Assembly.GetEntryAssembly()?.GetName().Version?.ToString() ?? "unknown",
                apiVersion = "v1",
                kafkaAdministrationMode = ResolveKafkaAdministrationMode(options),
            }))
            .WithName("v01-system-info")
            .RequireKafdeckAuthorization(AuthorizationAction.SystemRead);

        app.MapGet("/api/v1/system/health", () => Results.Ok(new
            {
                status = "ok",
                product = ProductIdentity.Name,
                configuredClusterCount = options.Clusters.Count,
            }))
            .WithName("v01-system-health")
            .RequireKafdeckAuthorization(AuthorizationAction.SystemRead);

        app.MapGet("/api/v1/clusters", async (
                HttpContext context,
                [Microsoft.AspNetCore.Mvc.FromServices] KafdeckAuthorizationService authorization,
                ClusterExplorerService clusters,
                CancellationToken cancellationToken) =>
            {
                var authorizedClusterIds = options.Clusters
                    .Select(cluster => cluster.Id)
                    .Where(clusterId => authorization.Authorize(
                        context.User,
                        new AuthorizationRequest(AuthorizationAction.ClusterRead, clusterId)) ==
                        KafdeckAuthorizationOutcome.Allowed)
                    .ToArray();

                var projections = await clusters.ListClustersAsync(
                        authorizedClusterIds,
                        cancellationToken)
                    .ConfigureAwait(false);

                return Results.Ok(new
                {
                    data = projections.Select(projection => ToClusterEnvelope(projection, authorization, context.User)).ToArray(),
                });
            })
            .WithName("v01-clusters-list");

        app.MapGet("/api/v1/clusters/{clusterId}", async (
                string clusterId,
                ClusterExplorerService clusters,
                [Microsoft.AspNetCore.Mvc.FromServices] KafdeckAuthorizationService authorization,
                HttpContext context,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var projection = await clusters.GetClusterAsync(clusterId, cancellationToken).ConfigureAwait(false);
                return Results.Ok(ToClusterEnvelope(projection, authorization, context.User));
            })
            .WithName("v01-clusters-detail")
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/health", async (
                string clusterId,
                ClusterExplorerService clusters,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var projection = await clusters.GetClusterAsync(clusterId, cancellationToken).ConfigureAwait(false);
                var limitations = ApiObservationMapper.FromCluster(projection.Limitations);
                var data = new ClusterHealthData(
                    projection.Health,
                    projection.HealthReasons,
                    projection.Failure?.Code);

                return Results.Ok(new ApiEnvelope<ClusterHealthData>(
                    data,
                    ApiObservationMapper.Create(projection.Observation, limitations.Count > 0),
                    limitations));
            })
            .WithName("v01-clusters-health")
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/capabilities", async (
                string clusterId,
                IKafkaAdministrationPort kafka,
                KafkaSnapshotCoordinator snapshots,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await snapshots.ObserveAsync(
                        clusterId,
                        "capabilities",
                        CapabilityTtl,
                        (operation, token) => kafka.GetCapabilitiesAsync(clusterId, operation, token),
                        cancellationToken)
                    .ConfigureAwait(false);

                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromKafka(result.Failure!));
                }

                var limitations = ApiObservationMapper.FromCapabilities(result.Value);
                var data = result.Value.Items
                    .OrderBy(item => item.Capability)
                    .ToDictionary(
                        item => CapabilityName(item.Capability),
                        item => ApiObservationMapper.CapabilityState(item.State),
                        StringComparer.Ordinal);

                return Results.Ok(new ApiEnvelope<IReadOnlyDictionary<string, string>>(
                    data,
                    ApiObservationMapper.Create(result.Observation, limitations.Count > 0),
                    limitations));
            })
            .WithName("v01-clusters-capabilities")
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/brokers", async (
                string clusterId,
                ClusterExplorerService clusters,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var projection = await clusters.GetClusterAsync(clusterId, cancellationToken).ConfigureAwait(false);
                if (projection.Failure is not null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromKafka(projection.Failure));
                }

                var limitations = ApiObservationMapper.FromCluster(projection.Limitations);
                return Results.Ok(new ApiEnvelope<IReadOnlyList<BrokerProjection>>(
                    projection.Brokers,
                    ApiObservationMapper.Create(projection.Observation, limitations.Count > 0),
                    limitations));
            })
            .WithName("v01-brokers-list")
            .RequireKafdeckAuthorization(AuthorizationAction.BrokerRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/brokers/{brokerId:int}", async (
                string clusterId,
                int brokerId,
                ClusterExplorerService clusters,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var projection = await clusters.GetClusterAsync(clusterId, cancellationToken).ConfigureAwait(false);
                if (projection.Failure is not null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromKafka(projection.Failure));
                }

                var broker = projection.Brokers.SingleOrDefault(item => item.BrokerId == brokerId);
                if (broker is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.BrokerNotFound(brokerId));
                }

                var limitations = ApiObservationMapper.FromCluster(projection.Limitations);
                return Results.Ok(new ApiEnvelope<BrokerProjection>(
                    broker,
                    ApiObservationMapper.Create(projection.Observation, limitations.Count > 0),
                    limitations));
            })
            .WithName("v01-brokers-detail")
            .RequireKafdeckAuthorization(AuthorizationAction.BrokerRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/brokers/{brokerId:int}/configuration", async (
                string clusterId,
                int brokerId,
                ClusterExplorerService clusters,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                if (brokerId < 0)
                {
                    return ApiResults.Problem(ApiProblemMapper.BrokerNotFound(brokerId));
                }

                var result = await clusters.GetBrokerConfigurationAsync(clusterId, brokerId, cancellationToken)
                    .ConfigureAwait(false);
                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromKafka(result.Failure!));
                }

                var data = result.Value.Select(ApiConfigurationMapper.Create).ToArray();
                return Results.Ok(new ApiEnvelope<IReadOnlyList<ConfigurationEntryData>>(
                    data,
                    ApiObservationMapper.Create(result.Observation, false),
                    Array.Empty<ApiLimitation>()));
            })
            .WithName("v01-brokers-configuration")
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId")
            .RequireKafdeckAuthorization(AuthorizationAction.BrokerRead, "clusterId")
            .RequireKafdeckAuthorization(AuthorizationAction.BrokerConfigRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/topics", async (
                string clusterId,
                string? q,
                string? cursor,
                int? pageSize,
                TopicExplorerService topics,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                KafkaResult<TopicPage> result;
                try
                {
                    result = await topics.ListTopicsAsync(
                            clusterId,
                            new TopicListRequest(q, pageSize ?? TopicExplorerService.DefaultPageSize, cursor),
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (ArgumentOutOfRangeException)
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidPageSize());
                }
                catch (ArgumentException exception)
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidCursor(exception.Message));
                }
                catch (InvalidOperationException exception)
                {
                    return ApiResults.Problem(ApiProblemMapper.StaleCursor(exception.Message));
                }

                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromKafka(result.Failure!));
                }

                var data = new TopicPageData(result.Value.Items, result.Value.NextCursor);
                return Results.Ok(new ApiEnvelope<TopicPageData>(
                    data,
                    ApiObservationMapper.Create(result.Observation, false),
                    Array.Empty<ApiLimitation>()));
            })
            .WithName("v01-topics-list")
            .RequireKafdeckAuthorization(AuthorizationAction.TopicList, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/topics/{topicName}", async (
                string clusterId,
                string topicName,
                TopicExplorerService topics,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await topics.GetTopicAsync(clusterId, topicName, cancellationToken).ConfigureAwait(false);
                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromKafka(result.Failure!));
                }

                var data = new TopicDetailData(
                    result.Value.Name,
                    result.Value.IsInternal,
                    result.Value.Partitions,
                    result.Value.OfflinePartitionCount,
                    result.Value.UnderReplicatedPartitionCount,
                    result.Value.AnomalyState);

                return Results.Ok(new ApiEnvelope<TopicDetailData>(
                    data,
                    ApiObservationMapper.Create(result.Observation, false),
                    Array.Empty<ApiLimitation>()));
            })
            .WithName("v01-topics-detail")
            .RequireKafdeckAuthorization(AuthorizationAction.TopicRead, "clusterId", "topicName");

        app.MapGet("/api/v1/clusters/{clusterId}/topics/{topicName}/partitions", async (
                string clusterId,
                string topicName,
                TopicExplorerService topics,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await topics.GetTopicAsync(clusterId, topicName, cancellationToken).ConfigureAwait(false);
                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromKafka(result.Failure!));
                }

                return Results.Ok(new ApiEnvelope<IReadOnlyList<PartitionProjection>>(
                    result.Value.Partitions,
                    ApiObservationMapper.Create(result.Observation, false),
                    Array.Empty<ApiLimitation>()));
            })
            .WithName("v01-topics-partitions")
            .RequireKafdeckAuthorization(AuthorizationAction.TopicRead, "clusterId", "topicName");

        app.MapGet("/api/v1/clusters/{clusterId}/topics/{topicName}/configuration", async (
                string clusterId,
                string topicName,
                TopicExplorerService topics,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await topics.GetTopicConfigurationAsync(clusterId, topicName, cancellationToken)
                    .ConfigureAwait(false);
                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromKafka(result.Failure!));
                }

                var data = result.Value.Select(ApiConfigurationMapper.Create).ToArray();
                return Results.Ok(new ApiEnvelope<IReadOnlyList<ConfigurationEntryData>>(
                    data,
                    ApiObservationMapper.Create(result.Observation, false),
                    Array.Empty<ApiLimitation>()));
            })
            .WithName("v01-topics-configuration")
            .RequireKafdeckAuthorization(AuthorizationAction.ClusterRead, "clusterId")
            .RequireKafdeckAuthorization(AuthorizationAction.TopicRead, "clusterId", "topicName")
            .RequireKafdeckAuthorization(AuthorizationAction.TopicConfigRead, "clusterId", "topicName");

        return app;
    }

    private static ApiEnvelope<ClusterData> ToClusterEnvelope(
        ClusterProjection projection,
        KafdeckAuthorizationService? authorization = null,
        System.Security.Claims.ClaimsPrincipal? principal = null)
    {
        var limitations = ApiObservationMapper.FromCluster(projection.Limitations);
        var brokers = authorization is null ||
            authorization.Authorize(
                principal,
                new AuthorizationRequest(AuthorizationAction.BrokerRead, projection.ClusterId)) ==
                KafdeckAuthorizationOutcome.Allowed
            ? projection.Brokers
            : Array.Empty<BrokerProjection>();
        var data = new ClusterData(
            projection.ClusterId,
            projection.KafkaClusterId,
            projection.ControllerBrokerId,
            brokers,
            projection.Health,
            projection.HealthReasons,
            projection.Failure?.Code);

        return new ApiEnvelope<ClusterData>(
            data,
            ApiObservationMapper.Create(projection.Observation, limitations.Count > 0),
            limitations);
    }

    private static bool IsConfiguredCluster(KafdeckOptions options, string clusterId) =>
        options.Clusters.Any(cluster => string.Equals(cluster.Id, clusterId, StringComparison.Ordinal));

    private static string CapabilityName(KafkaCapabilityKind capability) => capability switch
    {
        KafkaCapabilityKind.ClusterMetadata => "clusterMetadataRead",
        KafkaCapabilityKind.BrokerMetadata => "brokerMetadataRead",
        KafkaCapabilityKind.ControllerMetadata => "controllerMetadataRead",
        KafkaCapabilityKind.TopicListing => "topicListRead",
        KafkaCapabilityKind.TopicMetadata => "topicMetadataRead",
        KafkaCapabilityKind.TopicConfiguration => "topicConfigRead",
        KafkaCapabilityKind.BrokerConfiguration => "brokerConfigRead",
        _ => "unknown",
    };

    private sealed record ClusterData(
        string ClusterId,
        string? KafkaClusterId,
        int? ControllerBrokerId,
        IReadOnlyList<BrokerProjection> Brokers,
        ClusterHealthState Health,
        IReadOnlyList<ClusterHealthReason> HealthReasons,
        string? FailureCode);

    private sealed record ClusterHealthData(
        ClusterHealthState Health,
        IReadOnlyList<ClusterHealthReason> Reasons,
        string? FailureCode);

    private sealed record TopicPageData(
        IReadOnlyList<TopicListItem> Items,
        string? NextCursor);

    private sealed record TopicDetailData(
        string Name,
        bool IsInternal,
        IReadOnlyList<PartitionProjection> Partitions,
        int OfflinePartitionCount,
        int UnderReplicatedPartitionCount,
        TopicAnomalyState AnomalyState);
}
