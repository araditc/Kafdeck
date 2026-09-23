using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Schemas;

namespace Kafdeck.Api;

public static class KafdeckSchemaMutationEndpoints
{
    private const string IdempotencyHeader = "Idempotency-Key";

    public static WebApplication MapKafdeckSchemaMutationEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapPost(
                "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/register/preview",
                async (
                    string clusterId,
                    string subject,
                    SchemaRegistrationPreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    SchemaMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var denied = Preflight(
                        context,
                        authorization,
                        AuthorizationAction.SchemaCreate,
                        clusterId,
                        $"schema/{subject}");
                    if (denied is not null)
                    {
                        return denied;
                    }

                    if (string.IsNullOrWhiteSpace(request.Schema))
                    {
                        return InvalidRequest("A non-empty schema source is required.");
                    }

                    var planning = await planner
                        .PlanCreateAsync(
                            new SchemaRegistrationRequest(
                                clusterId,
                                subject,
                                request.Format,
                                request.Schema,
                                request.References ?? Array.Empty<RecordSchemaReference>()),
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
            .WithName("v05-schema-register-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/compatibility/preview",
                async (
                    string clusterId,
                    string subject,
                    SchemaCompatibilityPreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    SchemaMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var denied = Preflight(
                        context,
                        authorization,
                        AuthorizationAction.SchemaAlter,
                        clusterId,
                        $"schema/{subject}");
                    if (denied is not null)
                    {
                        return denied;
                    }

                    var planning = await planner
                        .PlanCompatibilityAsync(
                            new SchemaCompatibilityAlterRequest(
                                clusterId,
                                SchemaCompatibilityScope.Subject,
                                subject,
                                request.RequestedMode),
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
            .WithName("v05-schema-subject-compatibility-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/schemas/mutations/compatibility/preview",
                async (
                    string clusterId,
                    SchemaCompatibilityPreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    SchemaMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var denied = Preflight(
                        context,
                        authorization,
                        AuthorizationAction.SchemaAlter,
                        clusterId,
                        "schema-compatibility/global");
                    if (denied is not null)
                    {
                        return denied;
                    }

                    var planning = await planner
                        .PlanCompatibilityAsync(
                            new SchemaCompatibilityAlterRequest(
                                clusterId,
                                SchemaCompatibilityScope.Global,
                                null,
                                request.RequestedMode),
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
            .WithName("v05-schema-global-compatibility-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/delete/preview",
                async (
                    string clusterId,
                    string subject,
                    SchemaDeletePreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    SchemaMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var resource = request.Version.HasValue
                        ? $"schema/{subject}/version/{request.Version.Value}"
                        : $"schema/{subject}";
                    var denied = Preflight(
                        context,
                        authorization,
                        AuthorizationAction.SchemaDelete,
                        clusterId,
                        resource);
                    if (denied is not null)
                    {
                        return denied;
                    }

                    var planning = await planner
                        .PlanDeleteAsync(
                            new SchemaDeleteRequest(
                                clusterId,
                                subject,
                                request.Version,
                                request.Permanent),
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
            .WithName("v05-schema-delete-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/mutations/{operationId:guid}/schema-create/execute",
                async (
                    Guid operationId,
                    SchemaCreateExecuteRequest request,
                    HttpContext context,
                    MutationDispatchService dispatch,
                    CancellationToken cancellationToken) =>
                {
                    var result = await dispatch
                        .ExecuteSchemaCreateAsync(
                            context.User,
                            operationId,
                            request.Schema,
                            cancellationToken)
                        .ConfigureAwait(false);
                    return MapDispatchResult(result);
                })
            .WithName("v05-schema-create-execute")
            .RequireKafdeckAntiforgery();

        return app;
    }

    private static IResult? Preflight(
        HttpContext context,
        MutationRequestAuthorizationService authorization,
        AuthorizationAction action,
        string clusterId,
        string resource)
    {
        var outcome = authorization.AuthorizeTargets(
            context.User,
            new[]
            {
                new MutationAuthorizationTarget(action, clusterId, resource),
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
                title: "Schema mutation access denied"),
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
                title: "Schema mutation access denied"),
            MutationAdmissionOutcome.InvalidRequest => Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                type: "urn:kafdeck:problem:mutation-admission-invalid",
                title: "Schema mutation admission request is invalid",
                detail: result.Code == "idempotency_key_required"
                    ? "A valid Idempotency-Key header is required."
                    : null),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-admission-failed",
                title: "Schema mutation admission failed"),
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
                title: "Schema mutation access denied"),
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
                title: "Schema execution material is invalid"),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-dispatch-failed",
                title: "Mutation dispatch failed"),
        };

    private static IResult PlanningProblem(SchemaMutationPlanningFailure failure)
    {
        var (status, type) = failure.Code switch
        {
            SchemaMutationPlanningFailureCode.InvalidInput or
            SchemaMutationPlanningFailureCode.LimitExceeded or
            SchemaMutationPlanningFailureCode.SchemaIncompatible or
            SchemaMutationPlanningFailureCode.NoChange or
            SchemaMutationPlanningFailureCode.PermanentDeleteRequiresSoftDelete =>
                (StatusCodes.Status400BadRequest,
                    "urn:kafdeck:problem:schema-mutation-plan-invalid"),
            SchemaMutationPlanningFailureCode.SubjectNotFound or
            SchemaMutationPlanningFailureCode.VersionNotFound or
            SchemaMutationPlanningFailureCode.ReferenceNotFound =>
                (StatusCodes.Status404NotFound,
                    "urn:kafdeck:problem:schema-mutation-target-not-found"),
            SchemaMutationPlanningFailureCode.ProviderUnauthorized =>
                (StatusCodes.Status403Forbidden,
                    "urn:kafdeck:problem:mutation-provider-authorization-denied"),
            SchemaMutationPlanningFailureCode.ProviderUnsupported or
            SchemaMutationPlanningFailureCode.ProviderNotConfigured =>
                (StatusCodes.Status501NotImplemented,
                    "urn:kafdeck:problem:mutation-provider-unsupported"),
            SchemaMutationPlanningFailureCode.ProviderUnavailable or
            SchemaMutationPlanningFailureCode.CompatibilityUnavailable =>
                (StatusCodes.Status503ServiceUnavailable,
                    "urn:kafdeck:problem:mutation-provider-unavailable"),
            _ =>
                (StatusCodes.Status502BadGateway,
                    "urn:kafdeck:problem:mutation-planning-observation-failed"),
        };

        return Results.Problem(
            statusCode: status,
            type: type,
            title: "Schema mutation preview could not be created",
            detail: failure.SafeMessage,
            extensions: new Dictionary<string, object?>
            {
                ["code"] = failure.Code.ToString(),
            });
    }

    private static IResult InvalidRequest(string detail) =>
        Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            type: "urn:kafdeck:problem:schema-mutation-request-invalid",
            title: "Schema mutation request is invalid",
            detail: detail);
}

public sealed record SchemaRegistrationPreviewRequest(
    RecordSchemaFormat Format,
    string? Schema,
    IReadOnlyList<RecordSchemaReference>? References = null);

public sealed record SchemaCompatibilityPreviewRequest(
    SchemaCompatibilityMode RequestedMode);

public sealed record SchemaDeletePreviewRequest(
    int? Version,
    bool Permanent);

public sealed record SchemaCreateExecuteRequest(string? Schema);
