using System.Security.Cryptography;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

public static class KafdeckRecordProductionEndpoints
{
    private const string IdempotencyHeader = "Idempotency-Key";

    public static WebApplication MapKafdeckRecordProductionEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapPost(
                "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/produce/preview",
                async (
                    string clusterId,
                    string topicName,
                    RecordProductionPreviewRequest request,
                    HttpContext context,
                    RecordProductionPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    try
                    {
                        if (!TryMapRecords(request.Records, out var records))
                        {
                            return InvalidPayloadShape();
                        }

                        using var planning = await planner
                            .PlanAsync(
                                new RecordProductionRequest(
                                    clusterId,
                                    topicName,
                                    records!,
                                    request.SchemaValidation),
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

                        return MapAdmissionResult(result);
                    }
                    finally
                    {
                        ZeroRequestMaterial(request.Records);
                    }
                })
            .WithName("v05-record-production-preview")
            .RequireKafdeckAuthorization(
                AuthorizationAction.RecordProduce,
                "clusterId",
                "topicName")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/mutations/{operationId:guid}/record-production/execute",
                async (
                    Guid operationId,
                    RecordProductionExecuteRequest request,
                    HttpContext context,
                    MutationDispatchService dispatch,
                    CancellationToken cancellationToken) =>
                {
                    try
                    {
                        if (!TryMapRecords(request.Records, out var records))
                        {
                            return InvalidPayloadShape();
                        }

                        var result = await dispatch
                            .ExecuteRecordProductionAsync(
                                context.User,
                                operationId,
                                records,
                                cancellationToken)
                            .ConfigureAwait(false);

                        return MapDispatchResult(result);
                    }
                    finally
                    {
                        ZeroRequestMaterial(request.Records);
                    }
                })
            .WithName("v05-record-production-execute")
            .RequireKafdeckAntiforgery();

        return app;
    }

    private static bool TryMapRecords(
        IReadOnlyList<RecordProductionApiRecord?>? source,
        out IReadOnlyList<RecordProductionRecordInput>? records)
    {
        records = null;
        if (source is null)
        {
            return false;
        }

        var mapped = new List<RecordProductionRecordInput>(source.Count);
        foreach (var record in source)
        {
            if (record?.Value is null)
            {
                return false;
            }

            var headers = new List<RecordProductionHeaderInput>(
                record.Headers?.Count ?? 0);
            if (record.Headers is not null)
            {
                foreach (var header in record.Headers)
                {
                    if (header?.Name is null || header.Value is null)
                    {
                        return false;
                    }

                    headers.Add(
                        new RecordProductionHeaderInput(
                            header.Name,
                            header.Value));
                }
            }

            mapped.Add(
                new RecordProductionRecordInput(
                    record.Key,
                    record.Value,
                    headers));
        }

        records = mapped;
        return true;
    }

    private static IResult MapAdmissionResult(MutationAdmissionResult result) =>
        result.Outcome switch
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
                title: "Record production access denied"),
            MutationAdmissionOutcome.InvalidRequest => Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                type: "urn:kafdeck:problem:mutation-admission-invalid",
                title: "Record production admission request is invalid",
                detail: result.Code == "idempotency_key_required"
                    ? "A valid Idempotency-Key header is required."
                    : "The mutation could not be admitted from the supplied request."),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-admission-failed",
                title: "Record production admission failed"),
        };

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
                title: "Record production access denied"),
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
                title: "Execution material is invalid",
                detail: "Re-submit the exact bounded record material associated with the admitted preview."),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-dispatch-failed",
                title: "Mutation dispatch failed"),
        };

    private static IResult PlanningProblem(
        RecordProductionPlanningFailure failure)
    {
        var (status, type) = failure.Code switch
        {
            RecordProductionPlanningFailureCode.InvalidInput or
            RecordProductionPlanningFailureCode.LimitExceeded or
            RecordProductionPlanningFailureCode.SchemaValidationFailed or
            RecordProductionPlanningFailureCode.TemplateInvalid or
            RecordProductionPlanningFailureCode.PolicyApprovalUnsupported =>
                (StatusCodes.Status400BadRequest,
                    "urn:kafdeck:problem:record-production-plan-invalid"),
            RecordProductionPlanningFailureCode.TopicNotFound =>
                (StatusCodes.Status404NotFound,
                    "urn:kafdeck:problem:record-production-topic-not-found"),
            RecordProductionPlanningFailureCode.ProviderUnauthorized =>
                (StatusCodes.Status403Forbidden,
                    "urn:kafdeck:problem:mutation-provider-authorization-denied"),
            RecordProductionPlanningFailureCode.ProviderUnsupported =>
                (StatusCodes.Status501NotImplemented,
                    "urn:kafdeck:problem:mutation-provider-unsupported"),
            RecordProductionPlanningFailureCode.ProviderUnavailable or
            RecordProductionPlanningFailureCode.SchemaValidationUnavailable =>
                (StatusCodes.Status503ServiceUnavailable,
                    "urn:kafdeck:problem:mutation-provider-unavailable"),
            _ =>
                (StatusCodes.Status502BadGateway,
                    "urn:kafdeck:problem:mutation-planning-observation-failed"),
        };

        return Results.Problem(
            statusCode: status,
            type: type,
            title: "Record production preview could not be created",
            detail: failure.SafeMessage,
            extensions: new Dictionary<string, object?>
            {
                ["code"] = failure.Code.ToString(),
                ["recordOrdinal"] = failure.RecordOrdinal,
            });
    }

    private static IResult InvalidPayloadShape() =>
        Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            type: "urn:kafdeck:problem:record-production-payload-invalid",
            title: "Record production payload is invalid",
            detail: "Records require a value and every supplied header requires a name and value.");

    private static void ZeroRequestMaterial(
        IReadOnlyList<RecordProductionApiRecord?>? records)
    {
        if (records is null)
        {
            return;
        }

        foreach (var record in records)
        {
            if (record is null)
            {
                continue;
            }

            if (record.Key is { Length: > 0 })
            {
                CryptographicOperations.ZeroMemory(record.Key);
            }

            if (record.Value is { Length: > 0 })
            {
                CryptographicOperations.ZeroMemory(record.Value);
            }

            if (record.Headers is null)
            {
                continue;
            }

            foreach (var header in record.Headers)
            {
                if (header?.Value is { Length: > 0 })
                {
                    CryptographicOperations.ZeroMemory(header.Value);
                }
            }
        }
    }
}

public sealed record RecordProductionPreviewRequest(
    IReadOnlyList<RecordProductionApiRecord?>? Records,
    RecordProductionSchemaRequest? SchemaValidation = null);

public sealed record RecordProductionExecuteRequest(
    IReadOnlyList<RecordProductionApiRecord?>? Records);

public sealed record RecordProductionApiRecord(
    byte[]? Key,
    byte[]? Value,
    IReadOnlyList<RecordProductionApiHeader?>? Headers = null);

public sealed record RecordProductionApiHeader(
    string? Name,
    byte[]? Value);
