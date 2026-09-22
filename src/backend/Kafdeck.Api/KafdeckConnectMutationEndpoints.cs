using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;

namespace Kafdeck.Api;

public static class KafdeckConnectMutationEndpoints
{
    private const string IdempotencyHeader = "Idempotency-Key";

    public static WebApplication MapKafdeckConnectMutationEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapPost(
                "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/create/preview",
                async (
                    string clusterId,
                    string connectorName,
                    ConnectConfigurationPreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    ConnectMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var denied = Preflight(
                        context,
                        authorization,
                        AuthorizationAction.ConnectCreate,
                        clusterId,
                        connectorName);
                    if (denied is not null)
                    {
                        return denied;
                    }

                    if (request.Configuration is null)
                    {
                        return InvalidRequest("A bounded connector configuration is required.");
                    }

                    var planning = await planner
                        .PlanCreateAsync(
                            new ConnectCreateRequest(
                                clusterId,
                                connectorName,
                                request.Configuration),
                            cancellationToken)
                        .ConfigureAwait(false);
                    using var material = planning.ExecutionMaterial;

                    return planning.IsSuccess && planning.Plan is not null
                        ? await AdmitAsync(
                                context,
                                admission,
                                planning.Plan.Intent,
                                planning.Plan.Risk,
                                cancellationToken)
                            .ConfigureAwait(false)
                        : PlanningProblem(planning.Failure!);
                })
            .WithName("v05-connect-create-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/update/preview",
                async (
                    string clusterId,
                    string connectorName,
                    ConnectConfigurationPreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    ConnectMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var denied = Preflight(
                        context,
                        authorization,
                        AuthorizationAction.ConnectAlter,
                        clusterId,
                        connectorName);
                    if (denied is not null)
                    {
                        return denied;
                    }

                    if (request.Configuration is null)
                    {
                        return InvalidRequest("A bounded connector configuration is required.");
                    }

                    var planning = await planner
                        .PlanUpdateAsync(
                            new ConnectUpdateRequest(
                                clusterId,
                                connectorName,
                                request.Configuration),
                            cancellationToken)
                        .ConfigureAwait(false);
                    using var material = planning.ExecutionMaterial;

                    return planning.IsSuccess && planning.Plan is not null
                        ? await AdmitAsync(
                                context,
                                admission,
                                planning.Plan.Intent,
                                planning.Plan.Risk,
                                cancellationToken)
                            .ConfigureAwait(false)
                        : PlanningProblem(planning.Failure!);
                })
            .WithName("v05-connect-update-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/control/preview",
                async (
                    string clusterId,
                    string connectorName,
                    ConnectControlPreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    ConnectMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var denied = Preflight(
                        context,
                        authorization,
                        AuthorizationAction.ConnectAlter,
                        clusterId,
                        connectorName);
                    if (denied is not null)
                    {
                        return denied;
                    }

                    var planning = await planner
                        .PlanControlAsync(
                            new ConnectControlRequest(
                                clusterId,
                                connectorName,
                                request.Action,
                                request.TaskId),
                            cancellationToken)
                        .ConfigureAwait(false);

                    return planning.IsSuccess && planning.Plan is not null
                        ? await AdmitAsync(
                                context,
                                admission,
                                planning.Plan.Intent,
                                planning.Plan.Risk,
                                cancellationToken)
                            .ConfigureAwait(false)
                        : PlanningProblem(planning.Failure!);
                })
            .WithName("v05-connect-control-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/delete/preview",
                async (
                    string clusterId,
                    string connectorName,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    ConnectMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var denied = Preflight(
                        context,
                        authorization,
                        AuthorizationAction.ConnectDelete,
                        clusterId,
                        connectorName);
                    if (denied is not null)
                    {
                        return denied;
                    }

                    var planning = await planner
                        .PlanDeleteAsync(
                            new ConnectDeleteRequest(clusterId, connectorName),
                            cancellationToken)
                        .ConfigureAwait(false);

                    return planning.IsSuccess && planning.Plan is not null
                        ? await AdmitAsync(
                                context,
                                admission,
                                planning.Plan.Intent,
                                planning.Plan.Risk,
                                cancellationToken)
                            .ConfigureAwait(false)
                        : PlanningProblem(planning.Failure!);
                })
            .WithName("v05-connect-delete-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/mutations/{operationId:guid}/connect-configuration/execute",
                async (
                    Guid operationId,
                    ConnectConfigurationExecuteRequest request,
                    HttpContext context,
                    MutationDispatchService dispatch,
                    CancellationToken cancellationToken) =>
                {
                    var result = await dispatch
                        .ExecuteConnectConfigurationAsync(
                            context.User,
                            operationId,
                            request.Configuration,
                            cancellationToken)
                        .ConfigureAwait(false);
                    return MapDispatchResult(result);
                })
            .WithName("v05-connect-configuration-execute")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/mutations/{operationId:guid}/connect/execute",
                async (
                    Guid operationId,
                    HttpContext context,
                    MutationDispatchService dispatch,
                    CancellationToken cancellationToken) =>
                {
                    var result = await dispatch
                        .ExecuteConnectWithoutMaterialAsync(
                            context.User,
                            operationId,
                            cancellationToken)
                        .ConfigureAwait(false);
                    return MapDispatchResult(result);
                })
            .WithName("v05-connect-control-delete-execute")
            .RequireKafdeckAntiforgery();

        return app;
    }

    private static IResult? Preflight(
        HttpContext context,
        MutationRequestAuthorizationService authorization,
        AuthorizationAction action,
        string clusterId,
        string connectorName)
    {
        var outcome = authorization.AuthorizeTargets(
            context.User,
            new[]
            {
                new MutationAuthorizationTarget(
                    action,
                    clusterId,
                    $"connector/{connectorName}"),
            });

        return outcome switch
        {
            KafdeckAuthorizationOutcome.Allowed => null,
            KafdeckAuthorizationOutcome.Unauthenticated => Results.Problem(
                statusCode: StatusCodes.Status401Unauthorized,
                type: "urn:kafdeck:problem:operator-authentication-required",
                title: "Operator authentication required"),
            _ => Results.Problem(
                statusCode: StatusCodes.Status403Forbidden,
                type: "urn:kafdeck:problem:mutation-authorization-denied",
                title: "Kafka Connect mutation access denied"),
        };
    }

    private static async Task<IResult> AdmitAsync(
        HttpContext context,
        MutationAdmissionService admission,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        CancellationToken cancellationToken)
    {
        var result = await admission
            .AdmitAsync(
                context.User,
                intent,
                risk,
                context.Request.Headers[IdempotencyHeader].ToString(),
                cancellationToken)
            .ConfigureAwait(false);

        return result.Outcome switch
        {
            MutationAdmissionOutcome.Created when result.Operation is not null =>
                Results.Created(
                    $"/api/v1/mutations/{result.Operation.OperationId:D}",
                    MutationStatusData.From(result.Operation)),
            MutationAdmissionOutcome.ExistingSameIntent when result.Operation is not null =>
                Results.Ok(MutationStatusData.From(result.Operation)),
            MutationAdmissionOutcome.IdempotencyConflict => Results.Problem(
                statusCode: StatusCodes.Status409Conflict,
                type: "urn:kafdeck:problem:idempotency-key-conflict",
                title: "Idempotency key conflict"),
            MutationAdmissionOutcome.Unauthenticated => Results.Problem(
                statusCode: StatusCodes.Status401Unauthorized,
                type: "urn:kafdeck:problem:operator-authentication-required",
                title: "Operator authentication required"),
            MutationAdmissionOutcome.Forbidden => Results.Problem(
                statusCode: StatusCodes.Status403Forbidden,
                type: "urn:kafdeck:problem:mutation-authorization-denied",
                title: "Kafka Connect mutation access denied"),
            MutationAdmissionOutcome.InvalidRequest => Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                type: "urn:kafdeck:problem:mutation-admission-invalid",
                title: "Kafka Connect mutation admission request is invalid",
                detail: result.Code == "idempotency_key_required"
                    ? "A valid Idempotency-Key header is required."
                    : null),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-admission-failed",
                title: "Kafka Connect mutation admission failed"),
        };
    }

    private static IResult MapDispatchResult(MutationDispatchResult result) =>
        result.Outcome switch
        {
            MutationDispatchOutcome.Executed when result.Operation is not null =>
                Results.Ok(MutationStatusData.From(result.Operation)),
            MutationDispatchOutcome.NotFound => Results.Problem(
                statusCode: StatusCodes.Status404NotFound,
                type: "urn:kafdeck:problem:mutation-operation-not-found",
                title: "Mutation operation not found"),
            MutationDispatchOutcome.Unauthenticated => Results.Problem(
                statusCode: StatusCodes.Status401Unauthorized,
                type: "urn:kafdeck:problem:operator-authentication-required",
                title: "Operator authentication required"),
            MutationDispatchOutcome.Forbidden => Results.Problem(
                statusCode: StatusCodes.Status403Forbidden,
                type: "urn:kafdeck:problem:mutation-authorization-denied",
                title: "Kafka Connect mutation access denied"),
            MutationDispatchOutcome.NotReady => Results.Problem(
                statusCode: StatusCodes.Status409Conflict,
                type: "urn:kafdeck:problem:mutation-not-ready",
                title: "Mutation is not ready for execution"),
            MutationDispatchOutcome.CapabilityUnsupported => Results.Problem(
                statusCode: StatusCodes.Status501NotImplemented,
                type: "urn:kafdeck:problem:mutation-handler-not-admitted",
                title: "Mutation execution is not admitted for this operation kind"),
            MutationDispatchOutcome.InvalidExecutionMaterial => Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                type: "urn:kafdeck:problem:execution-material-invalid",
                title: "Kafka Connect execution material is invalid"),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-dispatch-failed",
                title: "Mutation dispatch failed"),
        };

    private static IResult PlanningProblem(
        ConnectMutationPlanningFailure failure)
    {
        var (status, type) = failure.Code switch
        {
            ConnectMutationPlanningFailureCode.InvalidInput or
            ConnectMutationPlanningFailureCode.LimitExceeded or
            ConnectMutationPlanningFailureCode.NoChange =>
                (StatusCodes.Status400BadRequest,
                    "urn:kafdeck:problem:connect-mutation-plan-invalid"),
            ConnectMutationPlanningFailureCode.ConnectorAlreadyExists =>
                (StatusCodes.Status409Conflict,
                    "urn:kafdeck:problem:connect-connector-already-exists"),
            ConnectMutationPlanningFailureCode.ConnectorNotFound or
            ConnectMutationPlanningFailureCode.TaskNotFound =>
                (StatusCodes.Status404NotFound,
                    "urn:kafdeck:problem:connect-mutation-target-not-found"),
            ConnectMutationPlanningFailureCode.ProviderUnauthorized =>
                (StatusCodes.Status403Forbidden,
                    "urn:kafdeck:problem:mutation-provider-authorization-denied"),
            ConnectMutationPlanningFailureCode.ProviderUnsupported or
            ConnectMutationPlanningFailureCode.ProviderNotConfigured =>
                (StatusCodes.Status501NotImplemented,
                    "urn:kafdeck:problem:mutation-provider-unsupported"),
            ConnectMutationPlanningFailureCode.ProviderUnavailable =>
                (StatusCodes.Status503ServiceUnavailable,
                    "urn:kafdeck:problem:mutation-provider-unavailable"),
            _ =>
                (StatusCodes.Status502BadGateway,
                    "urn:kafdeck:problem:mutation-planning-observation-failed"),
        };

        return Results.Problem(
            statusCode: status,
            type: type,
            title: "Kafka Connect mutation preview could not be created",
            detail: failure.SafeMessage,
            extensions: new Dictionary<string, object?>
            {
                ["code"] = failure.Code.ToString(),
            });
    }

    private static IResult InvalidRequest(string detail) =>
        Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            type: "urn:kafdeck:problem:connect-mutation-request-invalid",
            title: "Kafka Connect mutation request is invalid",
            detail: detail);
}

public sealed record ConnectConfigurationPreviewRequest(
    IReadOnlyDictionary<string, string>? Configuration);

public sealed record ConnectControlPreviewRequest(
    ConnectControlAction Action,
    int? TaskId = null);

public sealed record ConnectConfigurationExecuteRequest(
    IReadOnlyDictionary<string, string>? Configuration);
