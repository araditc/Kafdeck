using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Generator;

namespace Kafdeck.Api;

public static class KafdeckDataGeneratorEndpoints
{
    private const string IdempotencyHeader =
        "Idempotency-Key";

    public static WebApplication MapKafdeckDataGeneratorEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapPost(
                "/api/v1/data-jobs/generator/preview",
                async (
                    DataGeneratorPreviewRequest request,
                    HttpContext context,
                    DataGeneratorPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var mapped =
                        TryMapRequest(
                            request,
                            out var error);

                    if (mapped is null)
                    {
                        return InvalidRequest(error);
                    }

                    var planning =
                        await planner.PlanAsync(
                                mapped,
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
            .WithName("v07-data-generator-preview")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/data-jobs/generator/{operationId:guid}/start",
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
            .WithName("v07-data-generator-start")
            .RequireKafdeckAntiforgery();

        app.MapGet(
                "/api/v1/data-jobs/generator/{operationId:guid}",
                async (
                    Guid operationId,
                    HttpContext context,
                    IMutationOperationRepository repository,
                    IFleetMutationStateStore fleet,
                    MutationRequestAuthorizationService authorization,
                    CancellationToken cancellationToken) =>
                {
                    var access =
                        await GetAuthorizedGeneratorAsync(
                                operationId,
                                context,
                                repository,
                                authorization,
                                requireReconcilePermission: false,
                                cancellationToken)
                            .ConfigureAwait(false);

                    if (access.Error is not null)
                    {
                        return access.Error;
                    }

                    var progress =
                        await fleet.GetProgressAsync(
                                operationId,
                                cancellationToken)
                            .ConfigureAwait(false);

                    return Results.Ok(
                        DataGeneratorStatusData.From(
                            access.Operation!,
                            access.Plan!,
                            progress));
                })
            .WithName("v07-data-generator-status");

        app.MapPost(
                "/api/v1/data-jobs/generator/{operationId:guid}/cancel",
                async (
                    Guid operationId,
                    HttpContext context,
                    IMutationOperationRepository repository,
                    MutationRequestAuthorizationService authorization,
                    DataGeneratorStateCoordinator state,
                    IMutationAuditSink audit,
                    CancellationToken cancellationToken) =>
                {
                    var access =
                        await GetAuthorizedGeneratorAsync(
                                operationId,
                                context,
                                repository,
                                authorization,
                                requireReconcilePermission: false,
                                cancellationToken)
                            .ConfigureAwait(false);

                    if (access.Error is not null)
                    {
                        return access.Error;
                    }

                    var result =
                        await state.CancelAndFenceAsync(
                                operationId,
                                access.Plan!,
                                DateTimeOffset.UtcNow,
                                cancellationToken)
                            .ConfigureAwait(false);

                    if (result.Outcome ==
                        DataGeneratorStateOutcome.Applied)
                    {
                        await WriteLifecycleAuditAsync(
                                context,
                                audit,
                                access.Operation!,
                                MutationAuditEventType.Cancelled,
                                result.Code,
                                cancellationToken)
                            .ConfigureAwait(false);
                    }

                    return MapStateResult(
                        access.Operation!,
                        access.Plan!,
                        result,
                        "cancel");
                })
            .WithName("v07-data-generator-cancel")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/data-jobs/generator/{operationId:guid}/reconcile",
                async (
                    Guid operationId,
                    DataGeneratorReconcileRequest request,
                    HttpContext context,
                    IMutationOperationRepository repository,
                    MutationRequestAuthorizationService authorization,
                    DataGeneratorStateCoordinator state,
                    IMutationAuditSink audit,
                    CancellationToken cancellationToken) =>
                {
                    if (request.BatchId == Guid.Empty ||
                        !string.Equals(
                            request.Disposition,
                            "provenNonApplication",
                            StringComparison.OrdinalIgnoreCase))
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status400BadRequest,
                            type:
                                "urn:kafdeck:problem:data-generator-reconcile-request-invalid",
                            title:
                                "Generator reconciliation request is invalid",
                            detail:
                                "Only an exact batchId with disposition 'provenNonApplication' is admitted.");
                    }

                    var access =
                        await GetAuthorizedGeneratorAsync(
                                operationId,
                                context,
                                repository,
                                authorization,
                                requireReconcilePermission: true,
                                cancellationToken)
                            .ConfigureAwait(false);

                    if (access.Error is not null)
                    {
                        return access.Error;
                    }

                    var result =
                        await state.ReconcileProvenNonApplicationAsync(
                                operationId,
                                access.Plan!,
                                request.BatchId,
                                DateTimeOffset.UtcNow,
                                cancellationToken)
                            .ConfigureAwait(false);

                    if (result.Outcome ==
                        DataGeneratorStateOutcome.Applied)
                    {
                        await WriteLifecycleAuditAsync(
                                context,
                                audit,
                                access.Operation!,
                                MutationAuditEventType.Reconciled,
                                result.Code,
                                cancellationToken)
                            .ConfigureAwait(false);
                    }

                    return MapStateResult(
                        access.Operation!,
                        access.Plan!,
                        result,
                        "reconcile");
                })
            .WithName("v07-data-generator-reconcile")
            .RequireKafdeckAntiforgery();

        return app;
    }

    private static DataGeneratorPlanningRequest? TryMapRequest(
        DataGeneratorPreviewRequest request,
        out string? error)
    {
        error = null;

        if (request is null ||
            string.IsNullOrWhiteSpace(
                request.DestinationClusterId) ||
            string.IsNullOrWhiteSpace(
                request.DestinationProfileVersion) ||
            string.IsNullOrWhiteSpace(
                request.DestinationTopic) ||
            request.RecordCount <= 0 ||
            request.Source is null)
        {
            error =
                "Exact destination, finite record count and one source are required.";
            return null;
        }

        DataGeneratorBudget budget;
        try
        {
            var input = request.Budget;
            budget =
                new DataGeneratorBudget(
                    input?.MaxBatchRecords ??
                        DataGeneratorBudget
                            .DefaultMaxBatchRecords,
                    input?.MaxBatchBytes ??
                        DataGeneratorBudget
                            .DefaultMaxBatchBytes,
                    input?.MaxTotalRecords ??
                        DataGeneratorBudget
                            .DefaultMaxTotalRecords,
                    input?.MaxTotalBytes ??
                        DataGeneratorBudget
                            .DefaultMaxTotalBytes,
                    TimeSpan.FromSeconds(
                        input?.MaxDurationSeconds ??
                        (long)DataGeneratorBudget
                            .DefaultMaxDuration
                            .TotalSeconds),
                    input?.MaxRecordsPerSecond ??
                        DataGeneratorBudget
                            .DefaultMaxRecordsPerSecond,
                    input?.MaxBytesPerSecond ??
                        DataGeneratorBudget
                            .DefaultMaxBytesPerSecond);
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                OverflowException)
        {
            error =
                "Generator budget is outside admitted finite bounds.";
            return null;
        }

        if (!Enum.TryParse<DataGeneratorSourceKind>(
                request.Source.Kind,
                ignoreCase: true,
                out var kind))
        {
            error =
                "Generator source kind is unsupported.";
            return null;
        }

        DataGeneratorBuiltInTemplate? template = null;
        if (!string.IsNullOrWhiteSpace(
                request.Source.Template))
        {
            if (!Enum.TryParse<
                    DataGeneratorBuiltInTemplate>(
                    request.Source.Template,
                    ignoreCase: true,
                    out var parsed))
            {
                error =
                    "Generator built-in template is unsupported.";
                return null;
            }

            template = parsed;
        }

        return new DataGeneratorPlanningRequest(
            request.DestinationClusterId,
            request.DestinationProfileVersion,
            request.DestinationTopic,
            request.DestinationPartition,
            request.RecordCount,
            request.Seed ?? 0,
            budget,
            new DataGeneratorPlanningSourceRequest(
                kind,
                request.Source.SchemaSubject,
                request.Source.SchemaVersion,
                template));
    }

    private sealed record AuthorizedGenerator(
        MutationOperationSnapshot? Operation,
        DataGeneratorPlan? Plan,
        IResult? Error);

    private static async Task<AuthorizedGenerator>
        GetAuthorizedGeneratorAsync(
            Guid operationId,
            HttpContext context,
            IMutationOperationRepository repository,
            MutationRequestAuthorizationService authorization,
            bool requireReconcilePermission,
            CancellationToken cancellationToken)
    {
        var operation =
            await repository.GetAsync(
                    operationId,
                    cancellationToken)
                .ConfigureAwait(false);

        if (operation is null ||
            operation.OperationKind !=
                MutationOperationKind.DataGenerator)
        {
            return new(
                null,
                null,
                Results.Problem(
                    statusCode:
                        StatusCodes.Status404NotFound,
                    type:
                        "urn:kafdeck:problem:data-generator-not-found",
                    title:
                        "Data generator job not found"));
        }

        var authorized =
            authorization.AuthorizeForDispatch(
                context.User,
                operation);

        if (authorized ==
            KafdeckAuthorizationOutcome.Unauthenticated)
        {
            return new(
                null,
                null,
                Results.Problem(
                    statusCode:
                        StatusCodes.Status401Unauthorized,
                    type:
                        "urn:kafdeck:problem:operator-authentication-required",
                    title:
                        "Operator authentication required"));
        }

        if (authorized !=
            KafdeckAuthorizationOutcome.Allowed)
        {
            return new(
                null,
                null,
                Results.Problem(
                    statusCode:
                        StatusCodes.Status403Forbidden,
                    type:
                        "urn:kafdeck:problem:mutation-authorization-denied",
                    title:
                        "Generator access denied"));
        }

        DataGeneratorPlan plan;
        try
        {
            plan =
                DataGeneratorPolicy.DeserializePlan(
                    operation.CanonicalIntent);
        }
        catch
        {
            return new(
                null,
                null,
                Results.Problem(
                    statusCode:
                        StatusCodes.Status409Conflict,
                    type:
                        "urn:kafdeck:problem:data-generator-state-invalid",
                    title:
                        "Generator state is invalid"));
        }

        if (requireReconcilePermission)
        {
            var reconcile =
                authorization.AuthorizeTargets(
                    context.User,
                    new[]
                    {
                        new MutationAuthorizationTarget(
                            AuthorizationAction.MutationReconcile,
                            plan.Destination.ClusterId,
                            $"data-generator/{plan.PlanFingerprint}"),
                    });

            if (reconcile ==
                KafdeckAuthorizationOutcome.Unauthenticated)
            {
                return new(
                    null,
                    null,
                    Results.Problem(
                        statusCode:
                            StatusCodes.Status401Unauthorized,
                        type:
                            "urn:kafdeck:problem:operator-authentication-required",
                        title:
                            "Operator authentication required"));
            }

            if (reconcile !=
                KafdeckAuthorizationOutcome.Allowed)
            {
                return new(
                    null,
                    null,
                    Results.Problem(
                        statusCode:
                            StatusCodes.Status403Forbidden,
                        type:
                            "urn:kafdeck:problem:data-generator-reconcile-authorization-denied",
                        title:
                            "Generator reconciliation denied"));
            }
        }

        return new(
            operation,
            plan,
            null);
    }

    private static async Task WriteLifecycleAuditAsync(
        HttpContext context,
        IMutationAuditSink audit,
        MutationOperationSnapshot operation,
        MutationAuditEventType eventType,
        string outcomeCode,
        CancellationToken cancellationToken)
    {
        var principal =
            OperatorSessionContextFactory.TryCreate(
                context.User,
                out var session) &&
            session is not null
                ? SecurityAuditPrincipal.FromOperator(
                    session.Identity)
                : SecurityAuditPrincipal.LegacyDeployment;

        await audit.WriteAsync(
                new MutationAuditEvent(
                    DateTimeOffset.UtcNow,
                    eventType,
                    operation.OperationId,
                    principal,
                    operation.ClusterId,
                    operation.OperationKind,
                    operation.Risk.RiskClass,
                    operation.State,
                    operation.ResourceKeys,
                    operation.PreviewHash,
                    outcomeCode),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static IResult MapStateResult(
        MutationOperationSnapshot operation,
        DataGeneratorPlan plan,
        DataGeneratorStateResult result,
        string action)
    {
        if (result.Outcome ==
                DataGeneratorStateOutcome.Applied &&
            result.Progress is not null)
        {
            return Results.Ok(
                DataGeneratorStatusData.From(
                    operation,
                    plan,
                    result.Progress));
        }

        return Results.Problem(
            statusCode:
                StatusCodes.Status409Conflict,
            type:
                $"urn:kafdeck:problem:data-generator-{action}-blocked",
            title:
                $"Generator {action} could not be applied",
            detail: result.Code,
            extensions:
                new Dictionary<string, object?>
                {
                    ["code"] = result.Code,
                });
    }

    private static IResult PlanningProblem(
        DataGeneratorPlanningFailure failure)
    {
        var status =
            failure.Code switch
            {
                DataGeneratorPlanningFailureCode
                    .PolicyDenied =>
                    StatusCodes.Status403Forbidden,

                DataGeneratorPlanningFailureCode
                    .DestinationUnauthorized =>
                    StatusCodes.Status403Forbidden,

                DataGeneratorPlanningFailureCode
                    .DestinationUnsupported or
                DataGeneratorPlanningFailureCode
                    .SourceUnsupported =>
                    StatusCodes.Status501NotImplemented,

                DataGeneratorPlanningFailureCode
                    .DestinationUnavailable or
                DataGeneratorPlanningFailureCode
                    .SourceUnavailable =>
                    StatusCodes.Status503ServiceUnavailable,

                _ =>
                    StatusCodes.Status400BadRequest,
            };

        return Results.Problem(
            statusCode: status,
            type:
                "urn:kafdeck:problem:data-generator-preview-failed",
            title:
                "Generator preview could not be created",
            detail: failure.SafeMessage,
            extensions:
                new Dictionary<string, object?>
                {
                    ["code"] =
                        failure.Code.ToString(),
                    ["sourceCode"] =
                        failure.SourceCode,
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
                        "Generator authorization denied"),

            MutationAdmissionOutcome.InvalidRequest =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status400BadRequest,
                    type:
                        "urn:kafdeck:problem:mutation-admission-invalid",
                    title:
                        "Generator admission request is invalid",
                    detail:
                        result.Code ==
                            "idempotency_key_required"
                            ? "A valid Idempotency-Key header is required."
                            : null),

            _ =>
                Results.Problem(
                    statusCode:
                        StatusCodes
                            .Status500InternalServerError,
                    type:
                        "urn:kafdeck:problem:mutation-admission-failed",
                    title:
                        "Generator admission failed"),
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
                        "Generator mutation not found"),

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
                        "Generator activation denied"),

            MutationDispatchOutcome.NotReady =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status409Conflict,
                    type:
                        "urn:kafdeck:problem:mutation-not-ready",
                    title:
                        "Generator mutation is not ready"),

            MutationDispatchOutcome.CapabilityUnsupported =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status501NotImplemented,
                    type:
                        "urn:kafdeck:problem:mutation-handler-not-admitted",
                    title:
                        "Generator activation is not admitted"),

            _ =>
                Results.Problem(
                    statusCode:
                        StatusCodes
                            .Status500InternalServerError,
                    type:
                        "urn:kafdeck:problem:mutation-dispatch-failed",
                    title:
                        "Generator activation failed"),
        };

    private static IResult InvalidRequest(
        string? detail) =>
        Results.Problem(
            statusCode:
                StatusCodes.Status400BadRequest,
            type:
                "urn:kafdeck:problem:data-generator-request-invalid",
            title:
                "Generator preview request is invalid",
            detail:
                detail ??
                "The supplied generator request is invalid.");
}

public sealed record DataGeneratorPreviewRequest(
    string DestinationClusterId,
    string DestinationProfileVersion,
    string DestinationTopic,
    int DestinationPartition,
    int RecordCount,
    int? Seed,
    DataGeneratorSourceRequest Source,
    DataGeneratorBudgetRequest? Budget = null);

public sealed record DataGeneratorSourceRequest(
    string Kind,
    string? SchemaSubject = null,
    int? SchemaVersion = null,
    string? Template = null);

public sealed record DataGeneratorBudgetRequest(
    int? MaxBatchRecords = null,
    long? MaxBatchBytes = null,
    int? MaxTotalRecords = null,
    long? MaxTotalBytes = null,
    long? MaxDurationSeconds = null,
    int? MaxRecordsPerSecond = null,
    long? MaxBytesPerSecond = null);

public sealed record DataGeneratorReconcileRequest(
    Guid BatchId,
    string Disposition);

public sealed record DataGeneratorStatusData(
    Guid OperationId,
    string MutationState,
    string? ResultCode,
    string PlanFingerprint,
    string DestinationClusterId,
    string DestinationTopic,
    int DestinationPartition,
    string SourceKind,
    int RecordCount,
    int Seed,
    string ProgressPhase,
    long WorkerGeneration,
    long NextRecordIndex,
    long AcknowledgedRecords,
    long AcknowledgedBytes,
    long ActiveRuntimeMilliseconds,
    DataGeneratorPendingStatusData? PendingBatch)
{
    public static DataGeneratorStatusData From(
        MutationOperationSnapshot operation,
        DataGeneratorPlan plan,
        FleetOperationProgressSnapshot? progress)
    {
        var pending =
            progress?.Generator?.PendingBatch;

        return new DataGeneratorStatusData(
            operation.OperationId,
            operation.State.ToString(),
            operation.ResultCode,
            plan.PlanFingerprint,
            plan.Destination.ClusterId,
            plan.Destination.TopicName,
            plan.Destination.Partition,
            plan.Source.Kind.ToString(),
            plan.RecordCount,
            plan.Seed,
            progress?.Phase.ToString() ??
                "NotActivated",
            progress?.WorkerGeneration ?? 0,
            progress?.Generator?.NextRecordIndex ?? 0,
            progress?.Generator?.AcknowledgedRecords ?? 0,
            progress?.Generator?.AcknowledgedBytes ?? 0,
            (long)(progress?
                .ActiveObservationElapsed
                .TotalMilliseconds ?? 0),
            pending is null
                ? null
                : new DataGeneratorPendingStatusData(
                    pending.BatchId,
                    pending.RecordIndex,
                    pending.DestinationPartition,
                    pending.RawBytes,
                    pending.State.ToString(),
                    pending.ReservedAtUtc,
                    pending.DispatchStartedAtUtc));
    }
}

public sealed record DataGeneratorPendingStatusData(
    Guid BatchId,
    long RecordIndex,
    int DestinationPartition,
    long RawBytes,
    string State,
    DateTimeOffset ReservedAtUtc,
    DateTimeOffset? DispatchStartedAtUtc);
