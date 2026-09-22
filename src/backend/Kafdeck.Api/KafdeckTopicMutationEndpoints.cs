using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Topics;

namespace Kafdeck.Api;

public static class KafdeckTopicMutationEndpoints
{
    private const string IdempotencyHeader = "Idempotency-Key";

    public static WebApplication MapKafdeckTopicMutationEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapPost(
                "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/create/preview",
                async (
                    string clusterId,
                    string topicName,
                    TopicCreatePreviewRequest request,
                    HttpContext context,
                    TopicMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var planning = await planner.PlanCreateAsync(
                            new TopicCreateMutation(
                                clusterId,
                                topicName,
                                request.PartitionCount,
                                request.ReplicationFactor,
                                request.Configurations ??
                                new Dictionary<string, string>(StringComparer.Ordinal)),
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
            .WithName("v05-topic-create-preview")
            .RequireKafdeckAuthorization(
                AuthorizationAction.TopicCreate,
                "clusterId",
                "topicName")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/alter/preview",
                async (
                    string clusterId,
                    string topicName,
                    TopicAlterPreviewRequest request,
                    HttpContext context,
                    TopicMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var planning = await planner.PlanAlterAsync(
                            new TopicAlterMutation(
                                clusterId,
                                topicName,
                                request.Changes ??
                                new Dictionary<string, string?>(StringComparer.Ordinal)),
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
            .WithName("v05-topic-alter-preview")
            .RequireKafdeckAuthorization(
                AuthorizationAction.TopicAlter,
                "clusterId",
                "topicName")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/increase-partitions/preview",
                async (
                    string clusterId,
                    string topicName,
                    TopicIncreasePartitionsPreviewRequest request,
                    HttpContext context,
                    TopicMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var planning = await planner.PlanIncreasePartitionsAsync(
                            new TopicIncreasePartitionsMutation(
                                clusterId,
                                topicName,
                                request.NewPartitionCount),
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
            .WithName("v05-topic-increase-partitions-preview")
            .RequireKafdeckAuthorization(
                AuthorizationAction.TopicAlter,
                "clusterId",
                "topicName")
            .RequireKafdeckAntiforgery();

        app.MapPost(
                "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/delete/preview",
                async (
                    string clusterId,
                    string topicName,
                    HttpContext context,
                    TopicMutationPlanner planner,
                    MutationAdmissionService admission,
                    CancellationToken cancellationToken) =>
                {
                    var planning = await planner.PlanDeleteAsync(
                            new TopicDeleteMutation(clusterId, topicName),
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
            .WithName("v05-topic-delete-preview")
            .RequireKafdeckAuthorization(
                AuthorizationAction.TopicDelete,
                "clusterId",
                "topicName")
            .RequireKafdeckAntiforgery();

        return app;
    }

    private static async Task<IResult> AdmitAsync(
        HttpContext context,
        MutationAdmissionService admission,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        CancellationToken cancellationToken)
    {
        var idempotencyKey = context.Request.Headers[IdempotencyHeader].ToString();
        var result = await admission
            .AdmitAsync(
                context.User,
                intent,
                risk,
                idempotencyKey,
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
            MutationAdmissionOutcome.IdempotencyConflict =>
                Results.Problem(
                    statusCode: StatusCodes.Status409Conflict,
                    type: "urn:kafdeck:problem:idempotency-key-conflict",
                    title: "Idempotency key conflict",
                    detail: "The Idempotency-Key is already bound to a different mutation intent in this scope."),
            MutationAdmissionOutcome.Unauthenticated =>
                Results.Problem(
                    statusCode: StatusCodes.Status401Unauthorized,
                    type: "urn:kafdeck:problem:operator-authentication-required",
                    title: "Operator authentication required"),
            MutationAdmissionOutcome.Forbidden =>
                Results.Problem(
                    statusCode: StatusCodes.Status403Forbidden,
                    type: "urn:kafdeck:problem:mutation-authorization-denied",
                    title: "Mutation access denied"),
            MutationAdmissionOutcome.InvalidRequest =>
                Results.Problem(
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

    private static IResult PlanningProblem(TopicMutationPlanningFailure failure)
    {
        var (status, type) = failure.Code switch
        {
            TopicMutationPlanningFailureCode.InvalidInput or
            TopicMutationPlanningFailureCode.InvalidTopicName or
            TopicMutationPlanningFailureCode.InvalidPartitionCount or
            TopicMutationPlanningFailureCode.InvalidReplicationFactor or
            TopicMutationPlanningFailureCode.ConfigurationKeyUnsupported or
            TopicMutationPlanningFailureCode.InvalidConfigurationValue or
            TopicMutationPlanningFailureCode.ConfigurationReadOnly or
            TopicMutationPlanningFailureCode.ReplicationFactorUnsatisfied =>
                (StatusCodes.Status400BadRequest,
                    "urn:kafdeck:problem:mutation-plan-invalid"),
            TopicMutationPlanningFailureCode.TopicAlreadyExists =>
                (StatusCodes.Status409Conflict,
                    "urn:kafdeck:problem:topic-already-exists"),
            TopicMutationPlanningFailureCode.TopicNotFound =>
                (StatusCodes.Status404NotFound,
                    "urn:kafdeck:problem:topic-not-found"),
            TopicMutationPlanningFailureCode.InternalTopicUnsupported =>
                (StatusCodes.Status403Forbidden,
                    "urn:kafdeck:problem:internal-topic-mutation-forbidden"),
            TopicMutationPlanningFailureCode.ProviderUnsupported =>
                (StatusCodes.Status501NotImplemented,
                    "urn:kafdeck:problem:mutation-provider-unsupported"),
            TopicMutationPlanningFailureCode.ProviderUnavailable =>
                (StatusCodes.Status503ServiceUnavailable,
                    "urn:kafdeck:problem:mutation-provider-unavailable"),
            _ =>
                (StatusCodes.Status502BadGateway,
                    "urn:kafdeck:problem:mutation-planning-observation-failed"),
        };

        return Results.Problem(
            statusCode: status,
            type: type,
            title: "Mutation preview could not be created",
            detail: failure.SafeMessage,
            extensions: new Dictionary<string, object?>
            {
                ["code"] = failure.Code.ToString(),
            });
    }
}

public sealed record TopicCreatePreviewRequest(
    int PartitionCount,
    short ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configurations);

public sealed record TopicAlterPreviewRequest(
    IReadOnlyDictionary<string, string?>? Changes);

public sealed record TopicIncreasePartitionsPreviewRequest(
    int NewPartitionCount);
