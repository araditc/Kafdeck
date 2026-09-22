using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public static class KafdeckMutationEndpoints
{
    public static WebApplication MapKafdeckMutationEndpoints(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet("/api/v1/mutations/{operationId:guid}", async (
                Guid operationId,
                HttpContext context,
                IMutationOperationRepository repository,
                MutationRequestAuthorizationService authorization,
                CancellationToken cancellationToken) =>
            {
                var operation = await repository
                    .GetAsync(operationId, cancellationToken)
                    .ConfigureAwait(false);

                if (operation is null)
                {
                    return ProblemNotFound();
                }

                var outcome = authorization.AuthorizeForDispatch(
                    context.User,
                    operation);

                if (outcome == KafdeckAuthorizationOutcome.Unauthenticated)
                {
                    return ProblemUnauthenticated();
                }

                if (outcome != KafdeckAuthorizationOutcome.Allowed)
                {
                    return ProblemForbidden();
                }

                return Results.Ok(MutationStatusData.From(operation));
            })
            .WithName("v05-mutation-status");

        app.MapPost("/api/v1/mutations/{operationId:guid}/confirm", async (
                Guid operationId,
                MutationConfirmRequest request,
                HttpContext context,
                MutationCommandService commands,
                CancellationToken cancellationToken) =>
            {
                var result = await commands
                    .ConfirmAsync(
                        context.User,
                        operationId,
                        request.PreviewHash,
                        request.TypedTargetChallenge,
                        cancellationToken)
                    .ConfigureAwait(false);

                return MapCommandResult(result);
            })
            .WithName("v05-mutation-confirm")
            .RequireKafdeckAntiforgery();

        app.MapPost("/api/v1/mutations/{operationId:guid}/approve", async (
                Guid operationId,
                MutationReviewRequest request,
                HttpContext context,
                MutationCommandService commands,
                CancellationToken cancellationToken) =>
            {
                var result = await commands
                    .ApproveAsync(
                        context.User,
                        operationId,
                        request.PreviewHash,
                        cancellationToken)
                    .ConfigureAwait(false);

                return MapCommandResult(result);
            })
            .WithName("v05-mutation-approve")
            .RequireKafdeckAntiforgery();

        app.MapPost("/api/v1/mutations/{operationId:guid}/reject", async (
                Guid operationId,
                MutationReviewRequest request,
                HttpContext context,
                MutationCommandService commands,
                CancellationToken cancellationToken) =>
            {
                var result = await commands
                    .RejectAsync(
                        context.User,
                        operationId,
                        request.PreviewHash,
                        cancellationToken)
                    .ConfigureAwait(false);

                return MapCommandResult(result);
            })
            .WithName("v05-mutation-reject")
            .RequireKafdeckAntiforgery();

        app.MapPost("/api/v1/mutations/{operationId:guid}/cancel", async (
                Guid operationId,
                MutationCancelRequest request,
                HttpContext context,
                MutationCommandService commands,
                CancellationToken cancellationToken) =>
            {
                var result = await commands
                    .CancelAsync(
                        context.User,
                        operationId,
                        request.PreviewHash,
                        cancellationToken)
                    .ConfigureAwait(false);

                return MapCommandResult(result);
            })
            .WithName("v05-mutation-cancel")
            .RequireKafdeckAntiforgery();

        app.MapPost("/api/v1/mutations/{operationId:guid}/execute", async (
                Guid operationId,
                HttpContext context,
                MutationDispatchService dispatch,
                CancellationToken cancellationToken) =>
            {
                var result = await dispatch
                    .ExecuteTopicAsync(
                        context.User,
                        operationId,
                        cancellationToken)
                    .ConfigureAwait(false);

                return MapDispatchResult(result);
            })
            .WithName("v05-mutation-execute")
            .RequireKafdeckAntiforgery();

        app.MapKafdeckMutationApprovalEndpoints();
        app.MapKafdeckV05OpenApi();
        return app;
    }

    private static IResult MapCommandResult(MutationCommandResult result) =>
        result.Outcome switch
        {
            MutationCommandOutcome.Saved when result.Operation is not null =>
                Results.Ok(MutationStatusData.From(result.Operation)),
            MutationCommandOutcome.NotFound => ProblemNotFound(),
            MutationCommandOutcome.Unauthenticated => ProblemUnauthenticated(),
            MutationCommandOutcome.Forbidden => ProblemForbidden(),
            MutationCommandOutcome.InvalidRequest => Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                type: "urn:kafdeck:problem:mutation-command-invalid",
                title: "Mutation command request is invalid",
                detail: result.Code ?? "The mutation command request is invalid."),
            MutationCommandOutcome.InvalidState => Results.Problem(
                statusCode: StatusCodes.Status409Conflict,
                type: "urn:kafdeck:problem:mutation-state-conflict",
                title: "Mutation state conflict",
                detail: result.Code ?? "The requested transition is not valid for the current mutation state."),
            MutationCommandOutcome.VersionConflict => Results.Problem(
                statusCode: StatusCodes.Status409Conflict,
                type: "urn:kafdeck:problem:mutation-version-conflict",
                title: "Mutation version conflict",
                detail: "The mutation changed concurrently. Refresh its status before retrying a safe pre-dispatch action."),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-command-failed",
                title: "Mutation command failed"),
        };

    private static IResult MapDispatchResult(MutationDispatchResult result) =>
        result.Outcome switch
        {
            MutationDispatchOutcome.Executed when result.Operation is not null =>
                Results.Ok(MutationStatusData.From(result.Operation)),
            MutationDispatchOutcome.NotFound => ProblemNotFound(),
            MutationDispatchOutcome.Unauthenticated => ProblemUnauthenticated(),
            MutationDispatchOutcome.Forbidden => ProblemForbidden(),
            MutationDispatchOutcome.NotReady => Results.Problem(
                statusCode: StatusCodes.Status409Conflict,
                type: "urn:kafdeck:problem:mutation-not-ready",
                title: "Mutation is not ready for execution",
                detail: "Refresh the mutation status and complete its governed confirmation/approval workflow before execution."),
            MutationDispatchOutcome.CapabilityUnsupported => Results.Problem(
                statusCode: StatusCodes.Status501NotImplemented,
                type: "urn:kafdeck:problem:mutation-handler-not-admitted",
                title: "Mutation execution is not admitted for this operation kind",
                detail: "This mutation kind remains fail-closed at the current W39 integration boundary."),
            _ => Results.Problem(
                statusCode: StatusCodes.Status500InternalServerError,
                type: "urn:kafdeck:problem:mutation-dispatch-failed",
                title: "Mutation dispatch failed"),
        };

    private static IResult ProblemNotFound() =>
        Results.Problem(
            statusCode: StatusCodes.Status404NotFound,
            type: "urn:kafdeck:problem:mutation-operation-not-found",
            title: "Mutation operation not found",
            detail: "The requested mutation operation does not exist.");

    private static IResult ProblemUnauthenticated() =>
        Results.Problem(
            statusCode: StatusCodes.Status401Unauthorized,
            type: "urn:kafdeck:problem:operator-authentication-required",
            title: "Operator authentication required");

    private static IResult ProblemForbidden() =>
        Results.Problem(
            statusCode: StatusCodes.Status403Forbidden,
            type: "urn:kafdeck:problem:mutation-authorization-denied",
            title: "Mutation access denied",
            detail: "The current operator is not authorized for this mutation action.");
}

public sealed record MutationConfirmRequest(
    string? PreviewHash,
    string? TypedTargetChallenge);

public sealed record MutationReviewRequest(string? PreviewHash);

public sealed record MutationCancelRequest(string? PreviewHash);

public sealed record MutationStatusData(
    Guid OperationId,
    string ClusterId,
    MutationOperationKind OperationKind,
    MutationRiskClass RiskClass,
    MutationConfirmationMode ConfirmationMode,
    bool RequiresIndependentApproval,
    MutationOperationState State,
    string PreviewHash,
    DateTimeOffset PreviewExpiresAtUtc,
    string? ConfirmationChallenge,
    string? ResultCode,
    IReadOnlyDictionary<string, string> SafeProviderEvidence,
    long Version,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset UpdatedAtUtc)
{
    public static MutationStatusData From(MutationOperationSnapshot operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        return new MutationStatusData(
            operation.OperationId,
            operation.ClusterId,
            operation.OperationKind,
            operation.Risk.RiskClass,
            operation.Risk.ConfirmationMode,
            operation.Risk.RequiresIndependentApproval,
            operation.State,
            operation.PreviewHash,
            operation.PreviewExpiresAtUtc,
            operation.ConfirmationChallenge,
            operation.ResultCode,
            operation.SafeProviderEvidence,
            operation.Version,
            operation.CreatedAtUtc,
            operation.UpdatedAtUtc);
    }
}
