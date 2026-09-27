using Kafdeck.Modules.Records;

namespace Kafdeck.Api;

public static class KafdeckDataJobEndpoints
{
    private const string IdempotencyHeader =
        "Idempotency-Key";

    public static WebApplication MapKafdeckDataJobEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        MapPreview(
            app,
            "/api/v1/data-jobs/replay/preview",
            "v07-data-job-replay-preview",
            GovernedDataJobKind.Replay);

        MapPreview(
            app,
            "/api/v1/data-jobs/forward/preview",
            "v07-data-job-forward-preview",
            GovernedDataJobKind.Forward);

        MapPreview(
            app,
            "/api/v1/data-jobs/reprocess/preview",
            "v07-data-job-reprocess-preview",
            GovernedDataJobKind.Reprocess);

        MapPreview(
            app,
            "/api/v1/data-jobs/dlq-forward/preview",
            "v07-data-job-dlq-forward-preview",
            GovernedDataJobKind.DlqForward);

        return app;
    }

    private static void MapPreview(
        WebApplication app,
        string pattern,
        string name,
        GovernedDataJobKind kind)
    {
        app.MapPost(
                pattern,
                async (
                    DataJobPreviewRequest request,
                    HttpContext context,
                    GovernedDataJobPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var planningRequest =
                        TryMapRequest(
                            kind,
                            request,
                            out var error);

                    if (planningRequest is null)
                    {
                        return InvalidRequest(error);
                    }

                    var planning =
                        await planner.PlanAsync(
                                planningRequest,
                                cancellationToken)
                            .ConfigureAwait(false);

                    if (!planning.IsSuccess ||
                        planning.Intent is null ||
                        planning.Risk is null)
                    {
                        return PlanningProblem(
                            planning.Failure!);
                    }

                    var admitted =
                        await admission.AdmitAsync(
                                context.User,
                                planning.Intent,
                                planning.Risk,
                                context.Request.Headers[
                                    IdempotencyHeader]
                                    .ToString(),
                                cancellationToken)
                            .ConfigureAwait(false);

                    return MapAdmissionResult(admitted);
                })
            .WithName(name)
            .RequireKafdeckAntiforgery();
    }

    private static GovernedDataJobPlanningRequest?
        TryMapRequest(
            GovernedDataJobKind kind,
            DataJobPreviewRequest request,
            out string? error)
    {
        error = null;

        if (request is null ||
            string.IsNullOrWhiteSpace(
                request.SourceClusterId) ||
            string.IsNullOrWhiteSpace(
                request.SourceProfileVersion) ||
            string.IsNullOrWhiteSpace(
                request.DestinationClusterId) ||
            string.IsNullOrWhiteSpace(
                request.DestinationProfileVersion) ||
            request.Ranges is null ||
            request.Ranges.Count == 0)
        {
            error =
                "Source, destination and at least one finite range are required.";
            return null;
        }

        ClusterTransferBudget budget;
        try
        {
            var input = request.Budget;
            budget = new ClusterTransferBudget(
                input?.MaxBatchRecords ??
                    ClusterTransferBudget
                        .DefaultMaxBatchRecords,
                input?.MaxBatchBytes ??
                    ClusterTransferBudget
                        .DefaultMaxBatchBytes,
                input?.MaxTotalRecords ??
                    ClusterTransferBudget
                        .DefaultMaxTotalRecords,
                input?.MaxTotalBytes ??
                    ClusterTransferBudget
                        .DefaultMaxTotalBytes,
                TimeSpan.FromSeconds(
                    input?.MaxDurationSeconds ??
                    (long)ClusterTransferBudget
                        .DefaultMaxDuration.TotalSeconds),
                input?.MaxRecordsPerSecond ??
                    ClusterTransferBudget
                        .DefaultMaxRecordsPerSecond,
                input?.MaxBytesPerSecond ??
                    ClusterTransferBudget
                        .DefaultMaxBytesPerSecond);
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                OverflowException)
        {
            error =
                "Data-job budget is outside admitted finite bounds.";
            return null;
        }

        GovernedDataTransform transform;
        if (kind ==
            GovernedDataJobKind.Reprocess)
        {
            if (request.Transform is null)
            {
                error =
                    "Reprocess preview requires an explicit closed transform.";
                return null;
            }

            if (!Enum.TryParse<
                    GovernedDataTransformKind>(
                    request.Transform.Kind,
                    ignoreCase: true,
                    out var transformKind))
            {
                error =
                    "Data-job transform kind is unsupported.";
                return null;
            }

            transform =
                new GovernedDataTransform(
                    transformKind,
                    request.Transform.SerdeFormat,
                    request.Transform
                        .ProjectedFields);
        }
        else
        {
            if (request.Transform is not null)
            {
                error =
                    "Replay/forward previews do not accept an arbitrary transform.";
                return null;
            }

            transform =
                new GovernedDataTransform(
                    GovernedDataTransformKind
                        .BytePreserving);
        }

        var ranges = request.Ranges
            .Select(range =>
                new ClusterTransferMappingRequest(
                    range.SourceTopic,
                    range.SourcePartition,
                    range.DestinationTopic,
                    range.DestinationPartition,
                    range.StartInclusive,
                    range.EndExclusive))
            .ToArray();

        return new GovernedDataJobPlanningRequest(
            kind,
            request.SourceClusterId,
            request.SourceProfileVersion,
            request.DestinationClusterId,
            request.DestinationProfileVersion,
            ranges,
            budget,
            transform);
    }

    private static IResult PlanningProblem(
        GovernedDataJobPlanningFailure failure)
    {
        var status = failure.Code switch
        {
            GovernedDataJobPlanningFailureCode
                .InvalidInput =>
                StatusCodes.Status400BadRequest,
            GovernedDataJobPlanningFailureCode
                .TransformUnsupported =>
                StatusCodes.Status501NotImplemented,
            GovernedDataJobPlanningFailureCode
                .TransferPlanningFailed
                when failure.TransferFailure ==
                    ClusterTransferPlanningFailureCode
                        .ProviderUnauthorized =>
                StatusCodes.Status403Forbidden,
            GovernedDataJobPlanningFailureCode
                .TransferPlanningFailed
                when failure.TransferFailure is
                    ClusterTransferPlanningFailureCode
                        .ProviderUnsupported =>
                StatusCodes.Status501NotImplemented,
            GovernedDataJobPlanningFailureCode
                .TransferPlanningFailed
                when failure.TransferFailure is
                    ClusterTransferPlanningFailureCode
                        .ProviderUnavailable =>
                StatusCodes.Status503ServiceUnavailable,
            GovernedDataJobPlanningFailureCode
                .TransferPlanningFailed
                when failure.TransferFailure is
                    ClusterTransferPlanningFailureCode
                        .SamePhysicalCluster or
                    ClusterTransferPlanningFailureCode
                        .InternalTopicUnsupported or
                    ClusterTransferPlanningFailureCode
                        .PartitionUnavailable or
                    ClusterTransferPlanningFailureCode
                        .MaskingPolicyUnsupported =>
                StatusCodes.Status409Conflict,
            _ =>
                StatusCodes.Status502BadGateway,
        };

        return Results.Problem(
            statusCode: status,
            type:
                "urn:kafdeck:problem:data-job-preview-failed",
            title:
                "Data-job preview could not be created",
            detail: failure.SafeMessage,
            extensions:
                new Dictionary<string, object?>
                {
                    ["code"] =
                        failure.Code.ToString(),
                    ["transferFailure"] =
                        failure.TransferFailure?
                            .ToString(),
                });
    }

    private static IResult MapAdmissionResult(
        MutationAdmissionResult result) =>
        result.Outcome switch
        {
            MutationAdmissionOutcome.Created
                when result.Operation is not null =>
                Results.Created(
                    $"/api/v1/mutations/{result.Operation.OperationId:D}",
                    MutationStatusData.From(
                        result.Operation)),

            MutationAdmissionOutcome
                .ExistingSameIntent
                when result.Operation is not null =>
                Results.Ok(
                    MutationStatusData.From(
                        result.Operation)),

            MutationAdmissionOutcome
                .IdempotencyConflict =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status409Conflict,
                    type:
                        "urn:kafdeck:problem:idempotency-key-conflict",
                    title:
                        "Idempotency key conflict"),

            MutationAdmissionOutcome
                .Unauthenticated =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status401Unauthorized,
                    type:
                        "urn:kafdeck:problem:operator-authentication-required",
                    title:
                        "Operator authentication required"),

            MutationAdmissionOutcome.Forbidden =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status403Forbidden,
                    type:
                        "urn:kafdeck:problem:mutation-authorization-denied",
                    title:
                        "Data-job authorization denied"),

            MutationAdmissionOutcome.InvalidRequest =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status400BadRequest,
                    type:
                        "urn:kafdeck:problem:mutation-admission-invalid",
                    title:
                        "Data-job admission request is invalid",
                    detail:
                        result.Code ==
                            "idempotency_key_required"
                            ? "A valid Idempotency-Key header is required."
                            : null),

            _ => Results.Problem(
                statusCode:
                    StatusCodes
                        .Status500InternalServerError,
                type:
                    "urn:kafdeck:problem:mutation-admission-failed",
                title:
                    "Data-job admission failed"),
        };

    private static IResult InvalidRequest(
        string? detail) =>
        Results.Problem(
            statusCode:
                StatusCodes.Status400BadRequest,
            type:
                "urn:kafdeck:problem:data-job-request-invalid",
            title:
                "Data-job preview request is invalid",
            detail:
                detail ??
                "The supplied data-job request is invalid.");
}

public sealed record DataJobPreviewRequest(
    string SourceClusterId,
    string SourceProfileVersion,
    string DestinationClusterId,
    string DestinationProfileVersion,
    IReadOnlyList<DataJobRangeRequest>? Ranges,
    DataJobBudgetRequest? Budget = null,
    DataJobTransformRequest? Transform = null);

public sealed record DataJobRangeRequest(
    string SourceTopic,
    int SourcePartition,
    string DestinationTopic,
    int DestinationPartition,
    long StartInclusive,
    long EndExclusive);

public sealed record DataJobBudgetRequest(
    int? MaxBatchRecords = null,
    long? MaxBatchBytes = null,
    long? MaxTotalRecords = null,
    long? MaxTotalBytes = null,
    long? MaxDurationSeconds = null,
    int? MaxRecordsPerSecond = null,
    long? MaxBytesPerSecond = null);

public sealed record DataJobTransformRequest(
    string Kind,
    string? SerdeFormat = null,
    IReadOnlyList<string>? ProjectedFields = null);
