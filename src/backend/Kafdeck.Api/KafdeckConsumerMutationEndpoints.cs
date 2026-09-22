using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Consumers;

namespace Kafdeck.Api;

public static class KafdeckConsumerMutationEndpoints
{
    private const string IdempotencyHeader = "Idempotency-Key";

    public static WebApplication MapKafdeckConsumerMutationEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapPost(
                "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/mutations/offsets/preview",
                async (
                    string clusterId,
                    string groupId,
                    ConsumerOffsetAlterPreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    ConsumerMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var directTargets =
                        MutationDirectRouteAuthorization.ConsumerOffsetAlter(
                            clusterId,
                            groupId,
                            request.Targets);
                    if (directTargets is not null)
                    {
                        var denied = Preflight(
                            context,
                            authorization,
                            directTargets,
                            "Consumer offset mutation access denied");
                        if (denied is not null)
                        {
                            return denied;
                        }
                    }

                    var planning = await planner
                        .PlanOffsetAlterAsync(
                            new ConsumerOffsetAlterRequest(
                                clusterId,
                                groupId,
                                request.Targets ?? Array.Empty<ConsumerOffsetAlterTargetInput>()),
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
            .WithName("v05-consumer-offset-alter-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/mutations/delete/preview",
                async (
                    string clusterId,
                    string groupId,
                    ConsumerDeletePreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    ConsumerMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var directTargets =
                        MutationDirectRouteAuthorization.ConsumerDelete(
                            clusterId,
                            groupId,
                            request.Mode,
                            request.Targets);
                    if (directTargets is not null)
                    {
                        var denied = Preflight(
                            context,
                            authorization,
                            directTargets,
                            "Consumer delete mutation access denied");
                        if (denied is not null)
                        {
                            return denied;
                        }
                    }

                    var planning = await planner
                        .PlanDeleteAsync(
                            new ConsumerDeleteRequest(
                                clusterId,
                                groupId,
                                request.Mode,
                                request.Targets),
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
            .WithName("v05-consumer-delete-preview")
            .RequireKafdeckAntiforgery();

        return app;
    }

    private static IResult? Preflight(
        HttpContext context,
        MutationRequestAuthorizationService authorization,
        IReadOnlyList<MutationAuthorizationTarget> targets,
        string forbiddenTitle)
    {
        var outcome = authorization.AuthorizeTargets(
            context.User,
            targets);

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
                title: forbiddenTitle),
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
                title: "Idempotency key conflict",
                detail: "The Idempotency-Key is already bound to a different mutation intent in this scope."),
            MutationAdmissionOutcome.Unauthenticated => Results.Problem(
                statusCode: StatusCodes.Status401Unauthorized,
                type: "urn:kafdeck:problem:operator-authentication-required",
                title: "Operator authentication required"),
            MutationAdmissionOutcome.Forbidden => Results.Problem(
                statusCode: StatusCodes.Status403Forbidden,
                type: "urn:kafdeck:problem:mutation-authorization-denied",
                title: "Mutation access denied"),
            MutationAdmissionOutcome.InvalidRequest => Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                type: "urn:kafdeck:problem:mutation-admission-invalid",
                title: "Mutation admission request is invalid",
                detail: result.Code == "idempotency_key_required"
                    ? "A valid Idempotency-Key header is required."
                    : "The mutation could not be admitted from the supplied request."),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-admission-failed",
                title: "Mutation admission failed"),
        };
    }

    private static IResult PlanningProblem(
        ConsumerMutationPlanningFailure failure)
    {
        var (status, type) = failure.Code switch
        {
            ConsumerMutationPlanningFailureCode.InvalidInput or
            ConsumerMutationPlanningFailureCode.LimitExceeded or
            ConsumerMutationPlanningFailureCode.MissingCommittedOffset or
            ConsumerMutationPlanningFailureCode.OffsetOutOfRange or
            ConsumerMutationPlanningFailureCode.TimestampUnresolved =>
                (StatusCodes.Status400BadRequest,
                    "urn:kafdeck:problem:consumer-mutation-plan-invalid"),
            ConsumerMutationPlanningFailureCode.GroupNotFound or
            ConsumerMutationPlanningFailureCode.TargetNotFound =>
                (StatusCodes.Status404NotFound,
                    "urn:kafdeck:problem:consumer-mutation-target-not-found"),
            ConsumerMutationPlanningFailureCode.GroupStateUnsafe =>
                (StatusCodes.Status409Conflict,
                    "urn:kafdeck:problem:consumer-group-state-unsafe"),
            ConsumerMutationPlanningFailureCode.ProviderUnauthorized =>
                (StatusCodes.Status403Forbidden,
                    "urn:kafdeck:problem:mutation-provider-authorization-denied"),
            ConsumerMutationPlanningFailureCode.ProviderUnsupported =>
                (StatusCodes.Status501NotImplemented,
                    "urn:kafdeck:problem:mutation-provider-unsupported"),
            ConsumerMutationPlanningFailureCode.ProviderUnavailable =>
                (StatusCodes.Status503ServiceUnavailable,
                    "urn:kafdeck:problem:mutation-provider-unavailable"),
            _ =>
                (StatusCodes.Status502BadGateway,
                    "urn:kafdeck:problem:mutation-planning-observation-failed"),
        };

        return Results.Problem(
            statusCode: status,
            type: type,
            title: "Consumer mutation preview could not be created",
            detail: failure.SafeMessage,
            extensions: new Dictionary<string, object?>
            {
                ["code"] = failure.Code.ToString(),
                ["targetOrdinal"] = failure.TargetOrdinal,
            });
    }
}

public sealed record ConsumerOffsetAlterPreviewRequest(
    IReadOnlyList<ConsumerOffsetAlterTargetInput>? Targets);

public sealed record ConsumerDeletePreviewRequest(
    ConsumerDeleteMode Mode,
    IReadOnlyList<ConsumerOffsetDeleteTargetInput>? Targets = null);
