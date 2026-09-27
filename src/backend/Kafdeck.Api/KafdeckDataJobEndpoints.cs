using Kafdeck.Modules.Administration;
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

        app.MapPost(
                "/api/v1/data-jobs/{operationId:guid}/start",
                async (
                    Guid operationId,
                    HttpContext context,
                    MutationDispatchService dispatch,
                    CancellationToken cancellationToken) =>
                {
                    var result =
                        await dispatch.ExecuteWithoutMaterialAsync(
                                context.User,
                                operationId,
                                cancellationToken)
                            .ConfigureAwait(false);

                    return MapDispatchResult(result);
                })
            .WithName("v07-data-job-start")
            .RequireKafdeckAntiforgery();

        app.MapGet(
                "/api/v1/data-jobs/{operationId:guid}",
                async (
                    Guid operationId,
                    HttpContext context,
                    IMutationOperationRepository repository,
                    IFleetMutationStateStore fleet,
                    MutationRequestAuthorizationService authorization,
                    CancellationToken cancellationToken) =>
                {
                    var operation =
                        await repository.GetAsync(
                                operationId,
                                cancellationToken)
                            .ConfigureAwait(false);

                    if (operation is null ||
                        operation.OperationKind !=
                            MutationOperationKind.DataJob)
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status404NotFound,
                            type:
                                "urn:kafdeck:problem:data-job-not-found",
                            title:
                                "Data job not found");
                    }

                    var authorized =
                        authorization.AuthorizeForDispatch(
                            context.User,
                            operation);

                    if (authorized ==
                        KafdeckAuthorizationOutcome
                            .Unauthenticated)
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status401Unauthorized,
                            type:
                                "urn:kafdeck:problem:operator-authentication-required",
                            title:
                                "Operator authentication required");
                    }

                    if (authorized !=
                        KafdeckAuthorizationOutcome.Allowed)
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status403Forbidden,
                            type:
                                "urn:kafdeck:problem:mutation-authorization-denied",
                            title:
                                "Data-job access denied");
                    }

                    GovernedDataJobPlan plan;
                    try
                    {
                        plan =
                            GovernedDataJobPolicy.DeserializePlan(
                                operation.CanonicalIntent);
                    }
                    catch
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status409Conflict,
                            type:
                                "urn:kafdeck:problem:data-job-state-invalid",
                            title:
                                "Data-job state is invalid");
                    }

                    var progress =
                        await fleet.GetProgressAsync(
                                operationId,
                                cancellationToken)
                            .ConfigureAwait(false);

                    return Results.Ok(
                        DataJobStatusData.From(
                            operation,
                            plan,
                            progress));
                })
            .WithName("v07-data-job-status");

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

    private static IResult MapDispatchResult(
        MutationDispatchResult result) =>
        result.Outcome switch
        {
            MutationDispatchOutcome.Executed
                when result.Operation is not null =>
                Results.Ok(
                    MutationStatusData.From(
                        result.Operation)),

            MutationDispatchOutcome.NotFound =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status404NotFound,
                    type:
                        "urn:kafdeck:problem:mutation-operation-not-found",
                    title:
                        "Data-job mutation not found"),

            MutationDispatchOutcome.Unauthenticated =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status401Unauthorized,
                    type:
                        "urn:kafdeck:problem:operator-authentication-required",
                    title:
                        "Operator authentication required"),

            MutationDispatchOutcome.Forbidden =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status403Forbidden,
                    type:
                        "urn:kafdeck:problem:mutation-authorization-denied",
                    title:
                        "Data-job activation denied"),

            MutationDispatchOutcome.NotReady =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status409Conflict,
                    type:
                        "urn:kafdeck:problem:mutation-not-ready",
                    title:
                        "Data-job mutation is not ready"),

            MutationDispatchOutcome.CapabilityUnsupported =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status501NotImplemented,
                    type:
                        "urn:kafdeck:problem:mutation-handler-not-admitted",
                    title:
                        "Data-job activation is not admitted"),

            _ =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status500InternalServerError,
                    type:
                        "urn:kafdeck:problem:mutation-dispatch-failed",
                    title:
                        "Data-job activation failed"),
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

public sealed record DataJobRangeStatusData(
    int RangeIndex,
    string SourceTopic,
    int SourcePartition,
    string DestinationTopic,
    int DestinationPartition,
    long StartInclusive,
    long EndExclusive,
    long? NextSourceOffset);

public sealed record DataJobPendingBatchStatusData(
    Guid BatchId,
    int RangeIndex,
    long SourceOffset,
    int DestinationPartition,
    long RawBytes,
    string State,
    DateTimeOffset ReservedAtUtc,
    DateTimeOffset? DispatchStartedAtUtc);

public sealed record DataJobStatusData(
    Guid OperationId,
    string Kind,
    string MutationState,
    string? ResultCode,
    string PlanFingerprint,
    string SourceClusterId,
    string DestinationClusterId,
    string ProgressPhase,
    long WorkerGeneration,
    long AcknowledgedRecords,
    long AcknowledgedBytes,
    long ActiveRuntimeMilliseconds,
    IReadOnlyList<DataJobRangeStatusData> Ranges,
    DataJobPendingBatchStatusData? PendingBatch)
{
    public static DataJobStatusData From(
        MutationOperationSnapshot operation,
        GovernedDataJobPlan plan,
        FleetOperationProgressSnapshot? progress)
    {
        var checkpoints =
            progress?.Transfer?.Checkpoints ??
            Array.Empty<FleetTransferCheckpoint>();

        var ranges = plan.Ranges
            .Select((range, index) =>
                new DataJobRangeStatusData(
                    index,
                    range.SourceTopic,
                    range.SourcePartition,
                    range.DestinationTopic,
                    range.DestinationPartition,
                    range.StartInclusive,
                    range.EndExclusive,
                    index < checkpoints.Count
                        ? checkpoints[index]
                            .NextSourceOffset
                        : null))
            .ToArray();

        var pending =
            progress?.Transfer?.PendingBatch;
        var pendingData =
            pending is null
                ? null
                : new DataJobPendingBatchStatusData(
                    pending.BatchId,
                    pending.MappingIndex,
                    pending.SourceOffset,
                    pending.DestinationPartition,
                    pending.RawBytes,
                    pending.State.ToString(),
                    pending.ReservedAtUtc,
                    pending.DispatchStartedAtUtc);

        return new DataJobStatusData(
            operation.OperationId,
            plan.Kind.ToString(),
            operation.State.ToString(),
            operation.ResultCode,
            plan.PlanFingerprint,
            plan.Source.ClusterId,
            plan.Destination.ClusterId,
            progress?.Phase.ToString() ??
                "NotActivated",
            progress?.WorkerGeneration ?? 0,
            progress?.Transfer?
                .AcknowledgedRecords ?? 0,
            progress?.Transfer?
                .AcknowledgedBytes ?? 0,
            (long)(progress?
                .ActiveObservationElapsed
                .TotalMilliseconds ?? 0),
            ranges,
            pendingData);
    }
}

public sealed record DataJobTransformRequest(
    string Kind,
    string? SerdeFormat = null,
    IReadOnlyList<string>? ProjectedFields = null);
