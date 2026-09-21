using System.Security.Claims;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Schemas;

namespace Kafdeck.Api;

public static class KafdeckReadViewEndpoints
{
    private static readonly TimeSpan EcosystemOperationTimeout = TimeSpan.FromSeconds(10);
    private const int EcosystemMaxItems = 500;
    private const long EcosystemMaxResponseBytes = 4 * 1024 * 1024;

    public static WebApplication MapKafdeckV04ReadViews(
        this WebApplication app,
        KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(options);

        app.MapGet("/openapi/v0.4.json", () =>
                Results.Text(V04ReadViewApiContract.OpenApiJson, "application/json; charset=utf-8"))
            .WithName("openapi-v04");

        MapConsumers(app, options);
        MapSchemas(app, options);
        MapConnect(app, options);
        MapKsql(app, options);
        MapCatalog(app, options);

        return app;
    }

    private static void MapConsumers(WebApplication app, KafdeckOptions options)
    {
        app.MapGet("/api/v1/clusters/{clusterId}/consumer-groups", async (
                string clusterId,
                HttpContext context,
                KafdeckAuthorizationService authorization,
                ConsumerExplorerService consumers,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await consumers.ListGroupsAsync(clusterId, cancellationToken)
                    .ConfigureAwait(false);
                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromReadView(result.Failure!));
                }

                var visible = result.Value
                    .Where(group => IsAllowed(
                        authorization,
                        context.User,
                        AuthorizationAction.ConsumerRead,
                        clusterId,
                        group.GroupId))
                    .ToArray();

                return Results.Ok(ReadViewApiMapper.Envelope(visible, result.Limitations));
            })
            .WithName("v04-consumer-groups-list")
            .RequireKafdeckCollectionAuthorization(AuthorizationAction.ConsumerRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}", async (
                string clusterId,
                string groupId,
                ConsumerExplorerService consumers,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await consumers.GetGroupAsync(clusterId, groupId, cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-consumer-groups-detail")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ConsumerRead,
                "clusterId",
                "groupId");

        app.MapGet("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/lag", async (
                string clusterId,
                string groupId,
                ConsumerExplorerService consumers,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await consumers.GetLagAsync(clusterId, groupId, cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-consumer-groups-lag")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ConsumerRead,
                "clusterId",
                "groupId");

        app.MapGet("/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/diagnostics", async (
                string clusterId,
                string groupId,
                ConsumerDiagnosticsService diagnostics,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await diagnostics.DiagnoseAsync(clusterId, groupId, cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-consumer-groups-diagnostics")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ConsumerRead,
                "clusterId",
                "groupId");
    }

    private static void MapSchemas(WebApplication app, KafdeckOptions options)
    {
        app.MapGet("/api/v1/clusters/{clusterId}/schemas/subjects", async (
                string clusterId,
                HttpContext context,
                KafdeckAuthorizationService authorization,
                SchemaExplorerService schemas,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await schemas.ListSubjectsAsync(clusterId, cancellationToken)
                    .ConfigureAwait(false);
                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromReadView(result.Failure!));
                }

                var visible = result.Value
                    .Where(subject => IsAllowed(
                        authorization,
                        context.User,
                        AuthorizationAction.SchemaRead,
                        clusterId,
                        subject.Subject))
                    .ToArray();

                return Results.Ok(ReadViewApiMapper.Envelope(visible, result.Limitations));
            })
            .WithName("v04-schema-subjects-list")
            .RequireKafdeckCollectionAuthorization(AuthorizationAction.SchemaRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/versions", async (
                string clusterId,
                string subject,
                SchemaExplorerService schemas,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await schemas.ListVersionsAsync(clusterId, subject, cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-schema-versions-list")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SchemaRead,
                "clusterId",
                "subject");

        app.MapGet("/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/versions/{version:int}", async (
                string clusterId,
                string subject,
                int version,
                SchemaExplorerService schemas,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await schemas.GetVersionAsync(
                        clusterId,
                        subject,
                        version,
                        cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-schema-version-detail")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SchemaRead,
                "clusterId",
                "subject");

        app.MapGet("/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/compatibility", async (
                string clusterId,
                string subject,
                SchemaExplorerService schemas,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await schemas.GetCompatibilityAsync(
                        clusterId,
                        subject,
                        cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-schema-compatibility")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SchemaRead,
                "clusterId",
                "subject");

        app.MapGet("/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/diff", async (
                string clusterId,
                string subject,
                int leftVersion,
                int rightVersion,
                SchemaExplorerService schemas,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await schemas.DiffAsync(
                        clusterId,
                        subject,
                        leftVersion,
                        rightVersion,
                        cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-schema-diff")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SchemaRead,
                "clusterId",
                "subject");
    }

    private static void MapConnect(WebApplication app, KafdeckOptions options)
    {
        app.MapGet("/api/v1/clusters/{clusterId}/connect", async (
                string clusterId,
                IConnectReadPort connect,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await connect.GetClusterInfoAsync(
                        clusterId,
                        EcosystemOperation(),
                        cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-connect-info")
            .RequireKafdeckCollectionAuthorization(AuthorizationAction.ConnectRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/connect/connectors", async (
                string clusterId,
                HttpContext context,
                KafdeckAuthorizationService authorization,
                IConnectReadPort connect,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await connect.ListConnectorsAsync(
                        clusterId,
                        EcosystemOperation(),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (!result.IsSuccess || result.Value is null)
                {
                    return ApiResults.Problem(ApiProblemMapper.FromReadView(result.Failure!));
                }

                var visible = result.Value
                    .Where(connector => IsAllowed(
                        authorization,
                        context.User,
                        AuthorizationAction.ConnectRead,
                        clusterId,
                        connector.Name))
                    .ToArray();

                return Results.Ok(ReadViewApiMapper.Envelope(visible, result.Limitations));
            })
            .WithName("v04-connect-connectors-list")
            .RequireKafdeckCollectionAuthorization(AuthorizationAction.ConnectRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}", async (
                string clusterId,
                string connectorName,
                IConnectReadPort connect,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await connect.GetConnectorAsync(
                        clusterId,
                        connectorName,
                        EcosystemOperation(),
                        cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-connect-connectors-detail")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ConnectRead,
                "clusterId",
                "connectorName");
    }

    private static void MapKsql(WebApplication app, KafdeckOptions options)
    {
        app.MapGet("/api/v1/clusters/{clusterId}/ksql", async (
                string clusterId,
                IKsqlMetadataReadPort ksql,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await ksql.GetServerInfoAsync(
                        clusterId,
                        EcosystemOperation(),
                        cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-ksql-info")
            .RequireKafdeckCollectionAuthorization(AuthorizationAction.KsqlRead, "clusterId");

        app.MapGet("/api/v1/clusters/{clusterId}/ksql/metadata", async (
                string clusterId,
                IKsqlMetadataReadPort ksql,
                CancellationToken cancellationToken) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var result = await ksql.ListMetadataAsync(
                        clusterId,
                        EcosystemOperation(),
                        cancellationToken)
                    .ConfigureAwait(false);
                return ToReadViewResult(result);
            })
            .WithName("v04-ksql-metadata")
            .RequireKafdeckCollectionAuthorization(AuthorizationAction.KsqlRead, "clusterId");
    }

    private static void MapCatalog(WebApplication app, KafdeckOptions options)
    {
        app.MapGet("/api/v1/clusters/{clusterId}/catalog/topics/{topicName}", (
                string clusterId,
                string topicName,
                ITopicCatalogProvider catalog) =>
            {
                if (!IsConfiguredCluster(options, clusterId))
                {
                    return ApiResults.Problem(ApiProblemMapper.InvalidClusterId(clusterId));
                }

                var entry = catalog.GetTopic(clusterId, topicName);
                return entry is null
                    ? ApiResults.Problem(new ApiProblemDefinition(
                        StatusCodes.Status404NotFound,
                        "urn:kafdeck:problem:catalog-entry-not-found",
                        "Catalog entry not found",
                        "No topic catalog metadata is configured for the requested topic.",
                        "catalog_entry_not_found"))
                    : Results.Ok(ReadViewApiMapper.Envelope(entry));
            })
            .WithName("v04-topic-catalog-detail")
            .RequireKafdeckAuthorization(
                AuthorizationAction.CatalogRead,
                "clusterId",
                "topicName");
    }

    private static IResult ToReadViewResult<T>(ReadViewResult<T> result)
    {
        if (!result.IsSuccess || result.Value is null)
        {
            return ApiResults.Problem(ApiProblemMapper.FromReadView(result.Failure!));
        }

        return Results.Ok(ReadViewApiMapper.Envelope(result));
    }

    private static bool IsAllowed(
        KafdeckAuthorizationService authorization,
        ClaimsPrincipal principal,
        AuthorizationAction action,
        string clusterId,
        string resourceName) =>
        authorization.Authorize(
            principal,
            new AuthorizationRequest(action, clusterId, resourceName)) ==
        KafdeckAuthorizationOutcome.Allowed;

    private static bool IsConfiguredCluster(KafdeckOptions options, string clusterId) =>
        options.Clusters.Any(cluster =>
            string.Equals(cluster.Id, clusterId, StringComparison.Ordinal));

    private static ReadViewOperationContext EcosystemOperation() =>
        new(
            DateTimeOffset.UtcNow.Add(EcosystemOperationTimeout),
            EcosystemMaxItems,
            EcosystemMaxResponseBytes);
}
