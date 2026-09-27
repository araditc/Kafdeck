using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;

namespace Kafdeck.Api;

public static class KafdeckConnectAutoRestartEndpoints
{
    private const string IdempotencyHeader = "Idempotency-Key";

    public static WebApplication MapKafdeckConnectAutoRestartEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet(
                "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/restart-policy",
                async (
                    string clusterId,
                    string connectProfileId,
                    string connectorName,
                    int? taskId,
                    HttpContext context,
                    KafdeckAuthorizationService authorization,
                    IConnectAutoRestartStateStore store,
                    IConnectAutoRestartRuntimePolicyProvider runtimePolicy,
                    CancellationToken cancellationToken) =>
                {
                    if (!TryTarget(
                            clusterId,
                            connectProfileId,
                            connectorName,
                            taskId,
                            out var target))
                    {
                        return InvalidRequest(
                            "Connect auto-restart target is invalid.");
                    }

                    var authorized = authorization.Authorize(
                        context.User,
                        new AuthorizationRequest(
                            AuthorizationAction.ConnectRead,
                            target!.ClusterId,
                            target.AuthorizationResource));

                    if (authorized != KafdeckAuthorizationOutcome.Allowed)
                    {
                        return AuthorizationProblem(authorized);
                    }

                    var current = await store
                        .GetActiveByTargetAsync(
                            target,
                            cancellationToken)
                        .ConfigureAwait(false);
                    var runtime = await runtimePolicy
                        .GetCurrentAsync(
                            target,
                            cancellationToken)
                        .ConfigureAwait(false);

                    return Results.Ok(
                        ConnectAutoRestartPolicyStatusData.From(
                            target,
                            runtime,
                            current));
                })
            .WithName("v07-connect-auto-restart-policy-read");

        app.MapPut(
                "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/restart-policy/preview",
                async (
                    string clusterId,
                    string connectProfileId,
                    string connectorName,
                    ConnectAutoRestartPolicyPreviewRequest request,
                    HttpContext context,
                    MutationRequestAuthorizationService authorization,
                    ConnectAutoRestartPolicyPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    if (!TryTarget(
                            clusterId,
                            connectProfileId,
                            connectorName,
                            request.TaskId,
                            out var target))
                    {
                        return InvalidRequest(
                            "Connect auto-restart target is invalid.");
                    }

                    if (!OperatorSessionContextFactory.TryCreate(
                            context.User,
                            out var session) ||
                        session is null)
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status401Unauthorized,
                            type:
                                "urn:kafdeck:problem:operator-authentication-required",
                            title:
                                "Operator authentication required");
                    }

                    var requirements =
                        PreviewAuthorizationRequirements(
                            target!,
                            request.Enabled);

                    var preflight = authorization.AuthorizeTargets(
                        context.User,
                        requirements);
                    if (preflight != KafdeckAuthorizationOutcome.Allowed)
                    {
                        return AuthorizationProblem(preflight);
                    }

                    var planning = await planner
                        .PlanAsync(
                            new ConnectAutoRestartPolicyRequest(
                                clusterId,
                                connectProfileId,
                                connectorName,
                                request.Enabled,
                                request.TaskId,
                                request.MaxAttempts,
                                request.InitialBackoffSeconds,
                                request.MaxBackoffSeconds,
                                request.ActivationLifetimeSeconds,
                                request.MaxActivePoliciesPerProfile,
                                request.JitterBasisPoints),
                            SecurityAuditPrincipal.FromOperator(
                                session.Identity),
                            cancellationToken)
                        .ConfigureAwait(false);

                    if (!planning.IsSuccess ||
                        planning.Plan is null)
                    {
                        return PlanningProblem(
                            planning.Failure!);
                    }

                    var admitted = await admission
                        .AdmitAsync(
                            context.User,
                            planning.Plan.Intent,
                            planning.Plan.Risk,
                            context.Request.Headers[
                                IdempotencyHeader].ToString(),
                            cancellationToken)
                        .ConfigureAwait(false);

                    return AdmissionResult(admitted);
                })
            .WithName("v07-connect-auto-restart-policy-preview")
            .RequireKafdeckAntiforgery();

        app.MapPut(
                "/api/v1/clusters/{clusterId}/connect/profiles/{connectProfileId}/connectors/{connectorName}/restart-policy",
                async (
                    string clusterId,
                    string connectProfileId,
                    string connectorName,
                    ConnectAutoRestartPolicyApplyRequest request,
                    HttpContext context,
                    IMutationOperationRepository repository,
                    MutationDispatchService dispatch,
                    CancellationToken cancellationToken) =>
                {
                    if (!TryTarget(
                            clusterId,
                            connectProfileId,
                            connectorName,
                            request.TaskId,
                            out var target) ||
                        request.OperationId == Guid.Empty)
                    {
                        return InvalidRequest(
                            "Connect auto-restart apply request is invalid.");
                    }

                    var operation = await repository
                        .GetAsync(
                            request.OperationId,
                            cancellationToken)
                        .ConfigureAwait(false);

                    if (operation is null)
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status404NotFound,
                            type:
                                "urn:kafdeck:problem:mutation-operation-not-found",
                            title:
                                "Mutation operation not found");
                    }

                    if (!OperationMatchesTarget(
                            operation,
                            target!))
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status409Conflict,
                            type:
                                "urn:kafdeck:problem:auto-restart-operation-target-mismatch",
                            title:
                                "Auto-restart policy operation target mismatch");
                    }

                    var result = await dispatch
                        .ExecuteWithoutMaterialAsync(
                            context.User,
                            request.OperationId,
                            cancellationToken)
                        .ConfigureAwait(false);

                    return DispatchResult(result);
                })
            .WithName("v07-connect-auto-restart-policy-apply")
            .RequireKafdeckAntiforgery();

        return app;
    }

    private static bool OperationMatchesTarget(
        MutationOperationSnapshot operation,
        ConnectAutoRestartTarget target)
    {
        if (operation.OperationKind !=
            MutationOperationKind.ConnectAutoRestartPolicy)
        {
            return false;
        }

        try
        {
            var canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectAutoRestartPolicyCanonicalIntent>(
                    operation.CanonicalIntent);

            return canonical.Target == target &&
                   operation.ResourceKeys.Count == 1 &&
                   string.Equals(
                       operation.ResourceKeys[0],
                       target.CanonicalKey,
                       StringComparison.Ordinal);
        }
        catch
        {
            return false;
        }
    }

    private static IReadOnlyList<MutationAuthorizationTarget>
        PreviewAuthorizationRequirements(
            ConnectAutoRestartTarget target,
            bool enabled)
    {
        var items =
            new List<MutationAuthorizationTarget>
            {
                new(
                    AuthorizationAction.ConnectAutoRestartManage,
                    target.ClusterId,
                    target.AuthorizationResource),
            };

        if (enabled)
        {
            items.Add(
                new MutationAuthorizationTarget(
                    AuthorizationAction.ConnectRead,
                    target.ClusterId,
                    target.AuthorizationResource));
        }

        return items;
    }

    private static bool TryTarget(
        string clusterId,
        string connectProfileId,
        string connectorName,
        int? taskId,
        out ConnectAutoRestartTarget? target)
    {
        target = null;

        if (string.IsNullOrWhiteSpace(clusterId) ||
            clusterId.Trim().Length > 256 ||
            string.IsNullOrWhiteSpace(connectProfileId) ||
            connectProfileId.Trim().Length >
                Kafdeck.Infrastructure.Configuration
                    .KafkaConnectProfileSet.MaxProfileIdLength ||
            string.IsNullOrWhiteSpace(connectorName) ||
            connectorName.Trim().Length >
                ConnectMutationPolicy.HardMaxConnectorNameCharacters ||
            taskId is < 0)
        {
            return false;
        }

        target = new ConnectAutoRestartTarget(
            clusterId.Trim(),
            connectProfileId.Trim(),
            connectorName.Trim(),
            taskId);
        return true;
    }

    private static IResult AdmissionResult(
        MutationAdmissionResult result) =>
        result.Outcome switch
        {
            MutationAdmissionOutcome.Created
                when result.Operation is not null =>
                Results.Created(
                    $"/api/v1/mutations/{result.Operation.OperationId:D}",
                    MutationStatusData.From(
                        result.Operation)),

            MutationAdmissionOutcome.ExistingSameIntent
                when result.Operation is not null =>
                Results.Ok(
                    MutationStatusData.From(
                        result.Operation)),

            MutationAdmissionOutcome.IdempotencyConflict =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status409Conflict,
                    type:
                        "urn:kafdeck:problem:idempotency-key-conflict",
                    title:
                        "Idempotency key conflict"),

            MutationAdmissionOutcome.Unauthenticated =>
                AuthorizationProblem(
                    KafdeckAuthorizationOutcome.Unauthenticated),

            MutationAdmissionOutcome.Forbidden =>
                AuthorizationProblem(
                    KafdeckAuthorizationOutcome.Forbidden),

            MutationAdmissionOutcome.InvalidRequest =>
                InvalidRequest(
                    result.Code ??
                    "Mutation admission request is invalid."),

            _ =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status500InternalServerError,
                    type:
                        "urn:kafdeck:problem:mutation-admission-failed",
                    title:
                        "Auto-restart policy admission failed"),
        };

    private static IResult PlanningProblem(
        ConnectAutoRestartPolicyPlanningFailure failure)
    {
        var status = failure.Code switch
        {
            ConnectAutoRestartPolicyPlanningFailureCode.InvalidInput =>
                StatusCodes.Status400BadRequest,

            ConnectAutoRestartPolicyPlanningFailureCode.NoChange =>
                StatusCodes.Status409Conflict,

            ConnectAutoRestartPolicyPlanningFailureCode.TargetNotFound or
            ConnectAutoRestartPolicyPlanningFailureCode.TaskNotFound =>
                StatusCodes.Status404NotFound,

            ConnectAutoRestartPolicyPlanningFailureCode.AutomationAuthorizationDenied =>
                StatusCodes.Status403Forbidden,

            ConnectAutoRestartPolicyPlanningFailureCode.DeploymentPolicyDisabled or
            ConnectAutoRestartPolicyPlanningFailureCode.ReplicationBlocked =>
                StatusCodes.Status409Conflict,

            ConnectAutoRestartPolicyPlanningFailureCode.ObservationUnavailable or
            ConnectAutoRestartPolicyPlanningFailureCode.PersistenceUnavailable =>
                StatusCodes.Status503ServiceUnavailable,

            _ =>
                StatusCodes.Status500InternalServerError,
        };

        return Results.Problem(
            statusCode: status,
            type:
                "urn:kafdeck:problem:auto-restart-policy-planning-failed",
            title:
                "Connect auto-restart policy preview could not be created",
            detail:
                failure.SafeMessage,
            extensions:
                new Dictionary<string, object?>
                {
                    ["code"] =
                        failure.Code.ToString(),
                });
    }

    private static IResult DispatchResult(
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
                        "Mutation operation not found"),

            MutationDispatchOutcome.Unauthenticated =>
                AuthorizationProblem(
                    KafdeckAuthorizationOutcome.Unauthenticated),

            MutationDispatchOutcome.Forbidden =>
                AuthorizationProblem(
                    KafdeckAuthorizationOutcome.Forbidden),

            MutationDispatchOutcome.NotReady =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status409Conflict,
                    type:
                        "urn:kafdeck:problem:mutation-not-ready",
                    title:
                        "Mutation is not ready for execution"),

            MutationDispatchOutcome.CapabilityUnsupported =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status501NotImplemented,
                    type:
                        "urn:kafdeck:problem:mutation-handler-not-admitted",
                    title:
                        "Mutation handler is not admitted"),

            _ =>
                Results.Problem(
                    statusCode:
                        StatusCodes.Status500InternalServerError,
                    type:
                        "urn:kafdeck:problem:auto-restart-policy-dispatch-failed",
                    title:
                        "Auto-restart policy dispatch failed"),
        };

    private static IResult AuthorizationProblem(
        KafdeckAuthorizationOutcome outcome) =>
        outcome ==
        KafdeckAuthorizationOutcome.Unauthenticated
            ? Results.Problem(
                statusCode:
                    StatusCodes.Status401Unauthorized,
                type:
                    "urn:kafdeck:problem:operator-authentication-required",
                title:
                    "Operator authentication required")
            : Results.Problem(
                statusCode:
                    StatusCodes.Status403Forbidden,
                type:
                    "urn:kafdeck:problem:operator-authorization-denied",
                title:
                    "Auto-restart policy access denied");

    private static IResult InvalidRequest(
        string detail) =>
        Results.Problem(
            statusCode:
                StatusCodes.Status400BadRequest,
            type:
                "urn:kafdeck:problem:auto-restart-policy-request-invalid",
            title:
                "Connect auto-restart policy request is invalid",
            detail:
                detail);
}

public sealed record ConnectAutoRestartPolicyPreviewRequest(
    bool Enabled,
    int? TaskId = null,
    int? MaxAttempts = null,
    int? InitialBackoffSeconds = null,
    int? MaxBackoffSeconds = null,
    int? ActivationLifetimeSeconds = null,
    int? MaxActivePoliciesPerProfile = null,
    int? JitterBasisPoints = null);

public sealed record ConnectAutoRestartPolicyApplyRequest(
    Guid OperationId,
    int? TaskId = null);

public sealed record ConnectAutoRestartPolicyStatusData(
    string ClusterId,
    string ConnectProfileId,
    string ConnectorName,
    int? TaskId,
    bool DeploymentEnabled,
    bool Active,
    Guid? ActivationId,
    string? CircuitState,
    int AttemptsUsed,
    int MaxAttempts,
    DateTimeOffset? ActivatedAtUtc,
    DateTimeOffset? DeadlineUtc,
    DateTimeOffset? NextAttemptUtc,
    string? TerminalReason,
    bool HasUnresolvedDispatch,
    long? Version)
{
    public static ConnectAutoRestartPolicyStatusData From(
        ConnectAutoRestartTarget target,
        ConnectAutoRestartRuntimePolicySnapshot runtime,
        ConnectAutoRestartActivation? activation) =>
        new(
            target.ClusterId,
            target.ConnectProfileId,
            target.ConnectorName,
            target.TaskId,
            runtime.Enabled,
            activation is not null,
            activation?.ActivationId,
            activation?.CircuitState.ToString(),
            activation?.AttemptsUsed ?? 0,
            activation?.Policy.MaxAttempts ??
                runtime.Policy.MaxAttempts,
            activation?.ActivatedAtUtc,
            activation?.DeadlineUtc,
            activation?.NextAttemptUtc,
            activation?.TerminalReason,
            activation?.HasUnresolvedDispatch ?? false,
            activation?.Version);
}
