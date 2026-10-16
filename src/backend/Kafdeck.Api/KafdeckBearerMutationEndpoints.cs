using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Topics;
using System.Text.Json.Serialization;

namespace Kafdeck.Api;

/// <summary>Explicit bearer-only TopicCreate lifecycle using the existing durable mutation engine.</summary>
public static class KafdeckBearerMutationEndpoints
{
    public static WebApplication MapKafdeckBearerMutations(this WebApplication app)
    {
        app.MapPost("/api/v1/automation/clusters/{clusterId}/topics/{topicName}/create/preview", async (
            string clusterId, string topicName, BearerTopicCreateRequest request, HttpContext context,
            TopicMutationPlanner planner, MutationAdmissionService admission, CancellationToken cancellationToken) =>
        {
            var planning = await planner.PlanCreateAsync(new TopicCreateMutation(clusterId, topicName, request.PartitionCount,
                request.ReplicationFactor, request.Configurations ?? new Dictionary<string, string>(StringComparer.Ordinal)), cancellationToken);
            return planning.IsSuccess && planning.Plan is not null
                ? await KafdeckTopicMutationEndpoints.AdmitAsync(context, admission, planning.Plan.Intent, planning.Plan.Risk,
                    cancellationToken, id => $"/api/v1/automation/mutations/{id:D}/status")
                : KafdeckTopicMutationEndpoints.PlanningProblem(planning.Failure!);
        }).WithName("v08-bearer-topic-create-preview").RequireLiveBearerMutation()
            .RequireKafdeckAuthorization(AuthorizationAction.TopicCreate, "clusterId", "topicName");

        app.MapPost("/api/v1/automation/clusters/{clusterId}/topics/{topicName}/delete/preview", async (
            string clusterId, string topicName, HttpContext context, TopicMutationPlanner planner,
            MutationAdmissionService admission, CancellationToken cancellationToken) =>
        {
            var planning = await planner.PlanDeleteAsync(new TopicDeleteMutation(clusterId, topicName), cancellationToken);
            return planning.IsSuccess && planning.Plan is not null
                ? await KafdeckTopicMutationEndpoints.AdmitAsync(context, admission, planning.Plan.Intent, planning.Plan.Risk,
                    cancellationToken, id => $"/api/v1/automation/mutations/{id:D}/status")
                : KafdeckTopicMutationEndpoints.PlanningProblem(planning.Failure!);
        }).WithName("v08-bearer-topic-delete-preview").RequireLiveBearerMutation().RequireEmptyBody()
            .RequireKafdeckAuthorization(AuthorizationAction.TopicDelete, "clusterId", "topicName");

        app.MapGet("/api/v1/automation/mutations/{operationId:guid}/status", async (
            Guid operationId, HttpContext context, IMutationOperationRepository repository,
            MutationRequestAuthorizationService authorization, CancellationToken cancellationToken) =>
        {
            var checkedOperation = await CheckOwnedAsync(operationId, context, repository, authorization, cancellationToken);
            return checkedOperation.Error ?? Results.Ok(MutationStatusData.From(checkedOperation.Operation!));
        }).WithName("v08-bearer-mutation-status").RequireLiveBearerMutation();

        app.MapPost("/api/v1/automation/mutations/{operationId:guid}/confirm", async (
            Guid operationId, BearerMutationConfirmRequest request, HttpContext context, IMutationOperationRepository repository,
            MutationRequestAuthorizationService authorization, MutationCommandService commands, CancellationToken cancellationToken) =>
        {
            var checkedOperation = await CheckOwnedAsync(operationId, context, repository, authorization, cancellationToken);
            return checkedOperation.Error ?? KafdeckMutationEndpoints.MapCommandResult(await commands.ConfirmAsync(context.User,
                operationId, request.PreviewHash, request.TypedTargetChallenge, cancellationToken));
        }).WithName("v08-bearer-mutation-confirm").RequireLiveBearerMutation();

        app.MapPost("/api/v1/automation/mutations/{operationId:guid}/execute", async (
            Guid operationId, HttpContext context, IMutationOperationRepository repository,
            MutationRequestAuthorizationService authorization, MutationDispatchService dispatch, CancellationToken cancellationToken) =>
        {
            var checkedOperation = await CheckOwnedAsync(operationId, context, repository, authorization, cancellationToken);
            return checkedOperation.Error ?? KafdeckMutationEndpoints.MapDispatchResult(await dispatch.ExecuteTopicAsync(
                context.User, operationId, cancellationToken));
        }).WithName("v08-bearer-mutation-execute").RequireLiveBearerMutation().RequireEmptyBody();

        app.MapGet("/api/v1/automation/mutations/{operationId:guid}/approval", async (
            Guid operationId, HttpContext context, IMutationOperationRepository repository,
            MutationRequestAuthorizationService authorization, CancellationToken cancellationToken) =>
        {
            var checkedOperation = await CheckApprovalAsync(operationId, context, repository, authorization, cancellationToken);
            return checkedOperation.Error ?? Results.Ok(MutationStatusData.From(checkedOperation.Operation!));
        }).WithName("v08-bearer-mutation-approval").RequireLiveBearerApproval();

        app.MapPost("/api/v1/automation/mutations/{operationId:guid}/approve", async (
            Guid operationId, BearerMutationApprovalRequest request, HttpContext context, IMutationOperationRepository repository,
            MutationRequestAuthorizationService authorization, MutationCommandService commands, CancellationToken cancellationToken) =>
        {
            var checkedOperation = await CheckApprovalAsync(operationId, context, repository, authorization, cancellationToken);
            return checkedOperation.Error ?? KafdeckMutationEndpoints.MapCommandResult(await commands.ApproveAsync(context.User,
                operationId, request.PreviewHash, cancellationToken));
        }).WithName("v08-bearer-mutation-approve").RequireLiveBearerApproval();
        return app;
    }

    private static RouteHandlerBuilder RequireLiveBearerMutation(this RouteHandlerBuilder builder) => builder.AddEndpointFilter(async (context, next) =>
    {
        var http = context.HttpContext;
        if (http.User.Identity is not NonBrowserReadIdentity identity ||
            !(http.RequestServices.GetRequiredService<NonBrowserReadRegistry>().IsGovernedMutationAllowed(identity, AuthorizationAction.TopicCreate) ||
              http.RequestServices.GetRequiredService<NonBrowserReadRegistry>().IsGovernedMutationAllowed(identity, AuthorizationAction.TopicDelete)))
        {
            http.Response.Headers.WWWAuthenticate = "Bearer";
            return Results.Problem(statusCode: 401, title: "A live registered bearer mutation identity is required.");
        }
        return await next(context);
    });

    private static async Task<(MutationOperationSnapshot? Operation, IResult? Error)> CheckOwnedAsync(Guid id, HttpContext context,
        IMutationOperationRepository repository, MutationRequestAuthorizationService authorization, CancellationToken token)
    {
        if (!MutationActor.TryCreate(context.User, out var actor) || actor is null)
            return (null, Results.Problem(statusCode: 401, title: "Authentication required"));
        var operation = await repository.GetAsync(id, token);
        if (operation is null) return (null, Results.Problem(statusCode: 404, title: "Mutation not found"));
        if (operation.OperationKind is not (MutationOperationKind.TopicCreate or MutationOperationKind.TopicDelete) || operation.RequesterPrincipalId != actor.PrincipalId ||
            authorization.AuthorizeForDispatch(context.User, operation) != KafdeckAuthorizationOutcome.Allowed)
            return (null, Results.Problem(statusCode: 403, title: "Mutation access denied"));
        return (operation, null);
    }

    private static RouteHandlerBuilder RequireEmptyBody(this RouteHandlerBuilder builder) => builder.AddEndpointFilter((context, next) =>
        context.HttpContext.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpRequestBodyDetectionFeature>()?.CanHaveBody == true
            ? ValueTask.FromResult<object?>(Results.Problem(statusCode: 400, title: "This named operation accepts no request body."))
            : next(context));

    private static RouteHandlerBuilder RequireLiveBearerApproval(this RouteHandlerBuilder builder) => builder.AddEndpointFilter((context, next) =>
    {
        var http = context.HttpContext;
        if (http.User.Identity is not NonBrowserReadIdentity identity ||
            !http.RequestServices.GetRequiredService<NonBrowserReadRegistry>().IsGovernedApprovalAllowed(identity, AuthorizationAction.TopicDelete))
        {
            http.Response.Headers.WWWAuthenticate = "Bearer";
            return ValueTask.FromResult<object?>(Results.Problem(statusCode: 401, title: "A live registered human approval identity is required."));
        }
        return next(context);
    });

    private static async Task<(MutationOperationSnapshot? Operation, IResult? Error)> CheckApprovalAsync(Guid id, HttpContext context,
        IMutationOperationRepository repository, MutationRequestAuthorizationService authorization, CancellationToken token)
    {
        var operation = await repository.GetAsync(id, token);
        if (operation is null) return (null, Results.Problem(statusCode: 404, title: "Mutation not found"));
        return authorization.IsApprovalAllowed(context.User, operation)
            ? (operation, null) : (null, Results.Problem(statusCode: 403, title: "Independent human approval access denied"));
    }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record BearerTopicCreateRequest(int PartitionCount, short ReplicationFactor, IReadOnlyDictionary<string, string>? Configurations);

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record BearerMutationConfirmRequest(string PreviewHash, string? TypedTargetChallenge);

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record BearerMutationApprovalRequest(string PreviewHash);
