using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

public static class KafdeckRecordsPurgeEndpoints
{
    private const string IdempotencyHeader = "Idempotency-Key";

    public static WebApplication MapKafdeckRecordsPurgeEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapPost(
                "/api/v1/clusters/{clusterId}/records/purge/preview",
                async (
                    string clusterId,
                    RecordsPurgePreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    RecordsPurgePlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var directTargets =
                        MutationDirectRouteAuthorization.RecordsPurge(
                            clusterId,
                            request.Targets);
                    if (directTargets is not null)
                    {
                        var outcome = authorization.AuthorizeTargets(
                            context.User,
                            directTargets);
                        if (outcome == KafdeckAuthorizationOutcome.Unauthenticated)
                        {
                            return Results.Problem(
                                statusCode: StatusCodes.Status401Unauthorized,
                                type: "urn:kafdeck:problem:operator-authentication-required",
                                title: "Operator authentication required");
                        }

                        if (outcome != KafdeckAuthorizationOutcome.Allowed)
                        {
                            return Results.Problem(
                                statusCode: StatusCodes.Status403Forbidden,
                                type: "urn:kafdeck:problem:mutation-authorization-denied",
                                title: "Records purge access denied");
                        }
                    }

                    var planning = await planner
                        .PlanAsync(
                            new RecordsPurgeRequest(
                                clusterId,
                                request.Targets ??
                                Array.Empty<RecordsPurgeTargetInput>()),
                            cancellationToken)
                        .ConfigureAwait(false);

                    if (!planning.IsSuccess || planning.Plan is null)
                    {
                        return PlanningProblem(planning.Failure!);
                    }

                    var result = await admission
                        .AdmitAsync(
                            context.User,
                            planning.Plan.Intent,
                            planning.Plan.Risk,
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
                            title: "Records purge access denied"),
                        MutationAdmissionOutcome.InvalidRequest => Results.Problem(
                            statusCode: StatusCodes.Status400BadRequest,
                            type: "urn:kafdeck:problem:mutation-admission-invalid",
                            title: "Records purge admission request is invalid",
                            detail: result.Code == "idempotency_key_required"
                                ? "A valid Idempotency-Key header is required."
                                : "The destructive mutation could not be admitted from the supplied request."),
                        _ => Results.Problem(
                            statusCode: StatusCodes.Status500InternalServerError,
                            type: "urn:kafdeck:problem:mutation-admission-failed",
                            title: "Records purge admission failed"),
                    };
                })
            .WithName("v05-records-purge-preview")
            .RequireKafdeckAntiforgery();

        return app;
    }

    private static IResult PlanningProblem(
        RecordsPurgePlanningFailure failure)
    {
        var (status, type) = failure.Code switch
        {
            RecordsPurgePlanningFailureCode.InvalidInput or
            RecordsPurgePlanningFailureCode.LimitExceeded or
            RecordsPurgePlanningFailureCode.OffsetOutOfRange or
            RecordsPurgePlanningFailureCode.TimestampUnresolved =>
                (StatusCodes.Status400BadRequest,
                    "urn:kafdeck:problem:records-purge-plan-invalid"),
            RecordsPurgePlanningFailureCode.TargetNotFound =>
                (StatusCodes.Status404NotFound,
                    "urn:kafdeck:problem:records-purge-target-not-found"),
            RecordsPurgePlanningFailureCode.ProviderUnauthorized =>
                (StatusCodes.Status403Forbidden,
                    "urn:kafdeck:problem:mutation-provider-authorization-denied"),
            RecordsPurgePlanningFailureCode.ProviderUnsupported =>
                (StatusCodes.Status501NotImplemented,
                    "urn:kafdeck:problem:mutation-provider-unsupported"),
            RecordsPurgePlanningFailureCode.ProviderUnavailable =>
                (StatusCodes.Status503ServiceUnavailable,
                    "urn:kafdeck:problem:mutation-provider-unavailable"),
            _ =>
                (StatusCodes.Status502BadGateway,
                    "urn:kafdeck:problem:mutation-planning-observation-failed"),
        };

        return Results.Problem(
            statusCode: status,
            type: type,
            title: "Records purge preview could not be created",
            detail: failure.SafeMessage,
            extensions: new Dictionary<string, object?>
            {
                ["code"] = failure.Code.ToString(),
                ["targetOrdinal"] = failure.TargetOrdinal,
                ["irreversible"] = true,
                ["undoSupported"] = false,
            });
    }
}

public sealed record RecordsPurgePreviewRequest(
    IReadOnlyList<RecordsPurgeTargetInput>? Targets);
