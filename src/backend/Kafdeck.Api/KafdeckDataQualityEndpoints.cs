using System.Security.Claims;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

public static class KafdeckDataQualityEndpoints
{
    public static WebApplication MapKafdeckV08DataQuality(
        this WebApplication app,
        KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(options);

        app.MapGet(
                "/api/v1/clusters/{clusterId}/data-quality/policies",
                async (
                    string clusterId,
                    DataQualityPolicyLifecycleState? state,
                    int? maxResults,
                    string? afterPolicyId,
                    HttpContext context,
                    KafdeckAuthorizationService authorization,
                    [FromServices] IDataQualityLifecycleStore store,
                    CancellationToken cancellationToken) =>
                {
                    if (!ClusterExists(
                            options,
                            clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(
                                clusterId));
                    }

                    try
                    {
                        var page =
                            await store.ListPoliciesAsync(
                                    new DataQualityPolicyListQuery(
                                        clusterId,
                                        maxResults ??
                                        DataQualityPolicyListQuery
                                            .DefaultMaxResults,
                                        state,
                                        afterPolicyId),
                                    cancellationToken)
                                .ConfigureAwait(false);

                        var visible =
                            page.Items
                                .Where(
                                    item =>
                                        IsAllowed(
                                            authorization,
                                            context.User,
                                            AuthorizationAction
                                                .DataQualityRead,
                                            clusterId,
                                            item.Definition.PolicyId) &&
                                        IsAllowed(
                                            authorization,
                                            context.User,
                                            AuthorizationAction
                                                .TopicRead,
                                            clusterId,
                                            item.Definition.Scope.TopicName))
                                .ToArray();

                        var authorizationFiltered =
                            visible.Length !=
                            page.Items.Count;
                        var safeNextPolicyId =
                            page.Truncated &&
                            visible.Length > 0
                                ? visible[^1]
                                    .Definition.PolicyId
                                : null;

                        return Results.Ok(
                            new DataQualityPolicyPageData(
                                visible
                                    .Select(
                                        DataQualityPolicyData.From)
                                    .ToArray(),
                                page.Truncated,
                                safeNextPolicyId,
                                AuthorizationFiltered:
                                    authorizationFiltered));
                    }
                    catch (ArgumentException exception)
                    {
                        return InvalidRequest(
                            exception.Message);
                    }
                })
            .WithName("v08-data-quality-policy-list")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ClusterRead,
                "clusterId")
            .RequireKafdeckCollectionAuthorization(
                AuthorizationAction.DataQualityRead,
                "clusterId")
            .RequireKafdeckCollectionAuthorization(
                AuthorizationAction.TopicRead,
                "clusterId");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}",
                async (
                    string clusterId,
                    string policyId,
                    HttpContext context,
                    KafdeckAuthorizationService authorization,
                    [FromServices] IDataQualityLifecycleStore store,
                    CancellationToken cancellationToken) =>
                {
                    if (!ClusterExists(
                            options,
                            clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(
                                clusterId));
                    }

                    try
                    {
                        var snapshot =
                            await store.GetPolicyAsync(
                                    policyId,
                                    cancellationToken)
                                .ConfigureAwait(false);

                        if (!PolicyBelongsToCluster(
                                snapshot,
                                clusterId))
                        {
                            return PolicyNotFound();
                        }

                        var topicAuthorization =
                            await RequireTopicVisibilityAsync(
                                    context,
                                    authorization,
                                    clusterId,
                                    snapshot!.Definition.Scope.TopicName)
                                .ConfigureAwait(false);
                        if (topicAuthorization is not null)
                        {
                            return topicAuthorization;
                        }

                        return Results.Ok(
                            DataQualityPolicyData.From(
                                snapshot));
                    }
                    catch (ArgumentException exception)
                    {
                        return InvalidRequest(
                            exception.Message);
                    }
                })
            .WithName("v08-data-quality-policy-detail")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ClusterRead,
                "clusterId")
            .RequireKafdeckAuthorization(
                AuthorizationAction.DataQualityRead,
                "clusterId",
                "policyId");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}/evidence",
                async (
                    string clusterId,
                    string policyId,
                    DateTimeOffset? from,
                    DateTimeOffset? to,
                    int? maxPoints,
                    HttpContext context,
                    KafdeckAuthorizationService authorization,
                    [FromServices] IDataQualityLifecycleStore store,
                    CancellationToken cancellationToken) =>
                {
                    if (!ClusterExists(
                            options,
                            clusterId))
                    {
                        return ApiResults.Problem(
                            ApiProblemMapper.InvalidClusterId(
                                clusterId));
                    }

                    try
                    {
                        var snapshot =
                            await store.GetPolicyAsync(
                                    policyId,
                                    cancellationToken)
                                .ConfigureAwait(false);
                        if (!PolicyBelongsToCluster(
                                snapshot,
                                clusterId))
                        {
                            return PolicyNotFound();
                        }

                        var topicAuthorization =
                            await RequireTopicVisibilityAsync(
                                    context,
                                    authorization,
                                    clusterId,
                                    snapshot!.Definition.Scope.TopicName)
                                .ConfigureAwait(false);
                        if (topicAuthorization is not null)
                        {
                            return topicAuthorization;
                        }

                        var toUtc =
                            (to ??
                             DateTimeOffset.UtcNow)
                            .ToUniversalTime();
                        var fromUtc =
                            (from ??
                             toUtc.AddHours(-1))
                            .ToUniversalTime();

                        var page =
                            await store.QueryEvidenceAsync(
                                    new DataQualityEvidenceQuery(
                                        policyId,
                                        fromUtc,
                                        toUtc,
                                        maxPoints ??
                                        DataQualityEvidenceQuery
                                            .DefaultMaxPoints),
                                    cancellationToken)
                                .ConfigureAwait(false);

                        if (page.Points.Any(
                                point =>
                                    !string.Equals(
                                        point.Progress.ClusterId,
                                        clusterId,
                                        StringComparison.Ordinal)))
                        {
                            return Results.Problem(
                                statusCode:
                                    StatusCodes
                                        .Status500InternalServerError,
                                type:
                                    "urn:kafdeck:problem:data-quality-evidence-scope-corrupt",
                                title:
                                    "Data-quality evidence scope is inconsistent");
                        }

                        var visibleEvidence =
                            await FilterEvidenceByTopicVisibilityAsync(
                                    page.Points,
                                    context,
                                    authorization,
                                    clusterId)
                                .ConfigureAwait(false);

                        return Results.Ok(
                            new DataQualityEvidencePageData(
                                visibleEvidence
                                    .Select(
                                        DataQualityEvidenceData.From)
                                    .ToArray(),
                                page.Truncated,
                                AuthorizationFiltered:
                                    visibleEvidence.Count !=
                                    page.Points.Count));
                    }
                    catch (ArgumentException exception)
                    {
                        return InvalidRequest(
                            exception.Message);
                    }
                })
            .WithName("v08-data-quality-evidence")
            .RequireKafdeckAuthorization(
                AuthorizationAction.ClusterRead,
                "clusterId")
            .RequireKafdeckAuthorization(
                AuthorizationAction.DataQualityRead,
                "clusterId",
                "policyId");

        if (options.DataQuality?.ManagementEnabled == true)
        {
            app.MapPut(
                    "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}",
                    async (
                        string clusterId,
                        string policyId,
                        DataQualityPolicyUpsertRequest request,
                        HttpContext context,
                        KafdeckAuthorizationService authorization,
                        [FromServices] IDataQualityLifecycleStore store,
                        CancellationToken cancellationToken) =>
                    {
                        if (!ClusterExists(
                                options,
                                clusterId))
                        {
                            return ApiResults.Problem(
                                ApiProblemMapper.InvalidClusterId(
                                    clusterId));
                        }

                        try
                        {
                            var existing =
                                await store.GetPolicyAsync(
                                        policyId,
                                        cancellationToken)
                                    .ConfigureAwait(false);
                            if (existing is not null &&
                                !string.Equals(
                                    existing.Definition.Scope.ClusterId,
                                    clusterId,
                                    StringComparison.Ordinal))
                            {
                                return PolicyNotFound();
                            }

                            if (existing is not null)
                            {
                                var existingTopicAuthorization =
                                    await RequireTopicVisibilityAsync(
                                            context,
                                            authorization,
                                            clusterId,
                                            existing.Definition.Scope.TopicName)
                                        .ConfigureAwait(false);
                                if (existingTopicAuthorization is not null)
                                {
                                    return existingTopicAuthorization;
                                }
                            }

                            var definition =
                                request.BuildDefinition(
                                    clusterId,
                                    policyId);

                            var targetTopicAuthorization =
                                await RequireTopicVisibilityAsync(
                                        context,
                                        authorization,
                                        clusterId,
                                        definition.Scope.TopicName)
                                    .ConfigureAwait(false);
                            if (targetTopicAuthorization is not null)
                            {
                                return targetTopicAuthorization;
                            }

                            if (request.ExpectedRevision is null)
                            {
                                if (existing is not null)
                                {
                                    return PolicyConflict(
                                        "The data-quality policy already exists. Refresh the policy and retry with its current revision.");
                                }

                                try
                                {
                                    var created =
                                        await store.CreatePolicyAsync(
                                                definition,
                                                request.State,
                                                DateTimeOffset.UtcNow,
                                                cancellationToken)
                                            .ConfigureAwait(false);

                                    return Results.Created(
                                        $"/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}",
                                        DataQualityPolicyData.From(
                                            created));
                                }
                                catch (InvalidOperationException)
                                {
                                    return PolicyConflict(
                                        "The data-quality policy was created concurrently. Refresh before retrying.");
                                }
                            }

                            if (existing is null)
                            {
                                return PolicyNotFound();
                            }

                            if (request.ExpectedRevision.Value !=
                                existing.Revision)
                            {
                                return PolicyConflict(
                                    "The data-quality policy revision does not match the authorized snapshot. Refresh before retrying.");
                            }

                            var replaced =
                                await store.ReplacePolicyAsync(
                                        definition,
                                        request.State,
                                        request.ExpectedRevision.Value,
                                        DateTimeOffset.UtcNow,
                                        cancellationToken)
                                    .ConfigureAwait(false);

                            return replaced is null
                                ? PolicyConflict(
                                    "The data-quality policy changed concurrently. Refresh before retrying.")
                                : Results.Ok(
                                    DataQualityPolicyData.From(
                                        replaced));
                        }
                        catch (ArgumentException exception)
                        {
                            return InvalidRequest(
                                exception.Message);
                        }
                    })
                .WithName("v08-data-quality-policy-upsert")
                .RequireKafdeckAuthorization(
                    AuthorizationAction.ClusterRead,
                    "clusterId")
                .RequireKafdeckAuthorization(
                    AuthorizationAction.DataQualityManage,
                    "clusterId",
                    "policyId")
                .RequireKafdeckAntiforgery();

            app.MapPut(
                    "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}/state",
                    async (
                        string clusterId,
                        string policyId,
                        DataQualityPolicyStateRequest request,
                        HttpContext context,
                        KafdeckAuthorizationService authorization,
                        [FromServices] IDataQualityLifecycleStore store,
                        CancellationToken cancellationToken) =>
                    {
                        if (!ClusterExists(
                                options,
                                clusterId))
                        {
                            return ApiResults.Problem(
                                ApiProblemMapper.InvalidClusterId(
                                    clusterId));
                        }

                        try
                        {
                            var existing =
                                await store.GetPolicyAsync(
                                        policyId,
                                        cancellationToken)
                                    .ConfigureAwait(false);
                            if (!PolicyBelongsToCluster(
                                    existing,
                                    clusterId))
                            {
                                return PolicyNotFound();
                            }

                            var topicAuthorization =
                                await RequireTopicVisibilityAsync(
                                        context,
                                        authorization,
                                        clusterId,
                                        existing!.Definition.Scope.TopicName)
                                    .ConfigureAwait(false);
                            if (topicAuthorization is not null)
                            {
                                return topicAuthorization;
                            }

                            if (request.ExpectedRevision !=
                                existing!.Revision)
                            {
                                return PolicyConflict(
                                    "The data-quality policy revision does not match the authorized snapshot. Refresh before retrying.");
                            }

                            var updated =
                                await store.SetPolicyStateAsync(
                                        policyId,
                                        request.State,
                                        request.ExpectedRevision,
                                        DateTimeOffset.UtcNow,
                                        cancellationToken)
                                    .ConfigureAwait(false);

                            return updated is null
                                ? PolicyConflict(
                                    "The data-quality policy changed concurrently. Refresh before retrying.")
                                : Results.Ok(
                                    DataQualityPolicyData.From(
                                        updated));
                        }
                        catch (ArgumentException exception)
                        {
                            return InvalidRequest(
                                exception.Message);
                        }
                    })
                .WithName("v08-data-quality-policy-state")
                .RequireKafdeckAuthorization(
                    AuthorizationAction.ClusterRead,
                    "clusterId")
                .RequireKafdeckAuthorization(
                    AuthorizationAction.DataQualityManage,
                    "clusterId",
                    "policyId")
                .RequireKafdeckAntiforgery();
        }

        return app;
    }

    private static bool IsAllowed(
        KafdeckAuthorizationService authorization,
        ClaimsPrincipal principal,
        AuthorizationAction action,
        string clusterId,
        string resourceName) =>
        authorization.Authorize(
            principal,
            new AuthorizationRequest(
                action,
                clusterId,
                resourceName)) ==
        KafdeckAuthorizationOutcome.Allowed;

    private static async Task<IResult?>
        RequireTopicVisibilityAsync(
            HttpContext context,
            KafdeckAuthorizationService authorization,
            string clusterId,
            string topicName)
    {
        var outcome =
            authorization.Authorize(
                context.User,
                new AuthorizationRequest(
                    AuthorizationAction.TopicRead,
                    clusterId,
                    topicName));

        if (outcome ==
            KafdeckAuthorizationOutcome.Allowed)
        {
            return null;
        }

        if (outcome ==
            KafdeckAuthorizationOutcome.Forbidden)
        {
            await AuditTopicDenialAsync(
                    context,
                    clusterId,
                    topicName,
                    "rbac_denied_data_quality_topic")
                .ConfigureAwait(false);
        }

        return outcome ==
               KafdeckAuthorizationOutcome.Unauthenticated
            ? Results.Problem(
                statusCode:
                    StatusCodes.Status401Unauthorized,
                title:
                    "Authentication required",
                detail:
                    "An authenticated operator session is required.")
            : Results.Problem(
                statusCode:
                    StatusCodes.Status403Forbidden,
                type:
                    "urn:kafdeck:problem:operator-authorization-denied",
                title:
                    "Forbidden",
                detail:
                    "The authenticated operator is not authorized to access the underlying topic.");
    }

    private static async Task<IReadOnlyList<
            DataQualityEvidencePoint>>
        FilterEvidenceByTopicVisibilityAsync(
            IReadOnlyList<DataQualityEvidencePoint> points,
            HttpContext context,
            KafdeckAuthorizationService authorization,
            string clusterId)
    {
        var visibility =
            new Dictionary<string, bool>(
                StringComparer.Ordinal);

        foreach (var topicName in
                 points
                     .Select(
                         point =>
                             point.Progress.TopicName)
                     .Distinct(
                         StringComparer.Ordinal))
        {
            var outcome =
                authorization.Authorize(
                    context.User,
                    new AuthorizationRequest(
                        AuthorizationAction.TopicRead,
                        clusterId,
                        topicName));
            var allowed =
                outcome ==
                KafdeckAuthorizationOutcome.Allowed;
            visibility[topicName] =
                allowed;

            if (!allowed &&
                outcome ==
                KafdeckAuthorizationOutcome.Forbidden)
            {
                await AuditTopicDenialAsync(
                        context,
                        clusterId,
                        topicName,
                        "rbac_filtered_data_quality_evidence_topic")
                    .ConfigureAwait(false);
            }
        }

        return Array.AsReadOnly(
            points
                .Where(
                    point =>
                        visibility.TryGetValue(
                            point.Progress.TopicName,
                            out var allowed) &&
                        allowed)
                .ToArray());
    }

    private static async Task AuditTopicDenialAsync(
        HttpContext context,
        string clusterId,
        string topicName,
        string reasonCategory)
    {
        var audit =
            context.RequestServices
                .GetRequiredService<ISecurityAuditSink>();
        var principal =
            OperatorSessionContextFactory.TryCreate(
                context.User,
                out var session) &&
            session is not null
                ? SecurityAuditPrincipal.FromOperator(
                    session.Identity)
                : SecurityAuditPrincipal.Anonymous;

        await audit.WriteAsync(
                new SecurityAuditEvent(
                    DateTimeOffset.UtcNow,
                    SecurityAuditEventType.AuthorizationDenied,
                    principal,
                    session?.SessionId.Value.ToString("N"),
                    clusterId,
                    topicName,
                    SecurityAuditOutcome.Denied,
                    reasonCategory),
                context.RequestAborted)
            .ConfigureAwait(false);
    }

    private static bool ClusterExists(
        KafdeckOptions options,
        string clusterId) =>
        options.Clusters.Any(
            cluster =>
                string.Equals(
                    cluster.Id,
                    clusterId,
                    StringComparison.Ordinal));

    private static bool PolicyBelongsToCluster(
        DataQualityPolicyLifecycleSnapshot? snapshot,
        string clusterId) =>
        snapshot is not null &&
        string.Equals(
            snapshot.Definition.Scope.ClusterId,
            clusterId,
            StringComparison.Ordinal);

    private static IResult PolicyNotFound() =>
        Results.Problem(
            statusCode: StatusCodes.Status404NotFound,
            type:
                "urn:kafdeck:problem:data-quality-policy-not-found",
            title:
                "Data-quality policy not found");

    private static IResult PolicyConflict(
        string detail) =>
        Results.Problem(
            statusCode: StatusCodes.Status409Conflict,
            type:
                "urn:kafdeck:problem:data-quality-policy-version-conflict",
            title:
                "Data-quality policy conflict",
            detail:
                detail);

    private static IResult InvalidRequest(
        string detail) =>
        Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            type:
                "urn:kafdeck:problem:data-quality-request-invalid",
            title:
                "Data-quality request is invalid",
            detail:
                detail);
}

public sealed record DataQualityPolicyUpsertRequest(
    int Version,
    string TopicName,
    IReadOnlyList<int>? Partitions,
    IReadOnlyList<DataQualityRuleRequest>? Rules,
    DataQualityBudgetRequest? Budget,
    DataQualityPolicyLifecycleState State,
    long? ExpectedRevision)
{
    public DataQualityPolicyDefinition BuildDefinition(
        string clusterId,
        string policyId)
    {
        var rules =
            (Rules ??
             Array.Empty<DataQualityRuleRequest>())
            .Select(
                rule =>
                    rule.ToRule())
            .ToArray();

        var budget =
            Budget?.ToBudget() ??
            new DataQualityPolicyBudget();

        return new DataQualityPolicyDefinition(
            policyId,
            Version,
            new DataQualityPolicyScope(
                clusterId,
                TopicName,
                Partitions ??
                Array.Empty<int>()),
            rules,
            budget);
    }
}

public sealed record DataQualityRuleRequest(
    string RuleId,
    DataQualityRuleKind Kind,
    string JsonPointer,
    DataQualityValueType? ExpectedType,
    double? MinimumNumber,
    double? MaximumNumber,
    int? MinimumLength,
    int? MaximumLength)
{
    public DataQualityRule ToRule() =>
        new(
            RuleId,
            Kind,
            JsonPointer,
            ExpectedType,
            MinimumNumber,
            MaximumNumber,
            MinimumLength,
            MaximumLength);
}

public sealed record DataQualityBudgetRequest(
    int RecordsPerSecond,
    long BytesPerSecond,
    int EvaluationWindowSeconds,
    int ActivePoliciesPerCluster,
    int ConcurrentReadersPerCluster)
{
    public DataQualityPolicyBudget ToBudget() =>
        new(
            RecordsPerSecond,
            BytesPerSecond,
            TimeSpan.FromSeconds(
                EvaluationWindowSeconds),
            ActivePoliciesPerCluster,
            ConcurrentReadersPerCluster);
}

public sealed record DataQualityPolicyStateRequest(
    DataQualityPolicyLifecycleState State,
    long ExpectedRevision);

public sealed record DataQualityPolicyPageData(
    IReadOnlyList<DataQualityPolicyData> Items,
    bool Truncated,
    string? NextPolicyId,
    bool AuthorizationFiltered);

public sealed record DataQualityPolicyData(
    string PolicyId,
    int Version,
    string ClusterId,
    string TopicName,
    IReadOnlyList<int> Partitions,
    IReadOnlyList<DataQualityRule> Rules,
    DataQualityPolicyBudget Budget,
    DataQualityPolicyLifecycleState State,
    long Revision,
    DateTimeOffset UpdatedAtUtc)
{
    public static DataQualityPolicyData From(
        DataQualityPolicyLifecycleSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        return new DataQualityPolicyData(
            snapshot.Definition.PolicyId,
            snapshot.Definition.Version,
            snapshot.Definition.Scope.ClusterId,
            snapshot.Definition.Scope.TopicName,
            snapshot.Definition.Scope.Partitions,
            snapshot.Definition.Rules,
            snapshot.Definition.Budget,
            snapshot.State,
            snapshot.Revision,
            snapshot.UpdatedAtUtc);
    }
}

public sealed record DataQualityEvidencePageData(
    IReadOnlyList<DataQualityEvidenceData> Points,
    bool Truncated,
    bool AuthorizationFiltered);

public sealed record DataQualityEvidenceData(
    string PolicyId,
    int PolicyVersion,
    DateTimeOffset WindowStartUtc,
    DateTimeOffset WindowEndUtc,
    long EvaluatedRecords,
    long EvaluatedBytes,
    long ViolationCount,
    IReadOnlyList<DataQualityRuleViolationCount> ViolationsByRule,
    DataQualityEvidenceState State,
    string Source,
    string ClusterId,
    string TopicName,
    int Partition,
    long StartOffset,
    long EndOffsetExclusive,
    long NextOffset,
    DataQualityEvaluationOutcome Outcome,
    DateTimeOffset UpdatedAtUtc)
{
    public static DataQualityEvidenceData From(
        DataQualityEvidencePoint point)
    {
        ArgumentNullException.ThrowIfNull(point);

        return new DataQualityEvidenceData(
            point.Evidence.PolicyId,
            point.Evidence.PolicyVersion,
            point.Evidence.WindowStartUtc,
            point.Evidence.WindowEndUtc,
            point.Evidence.EvaluatedRecords,
            point.Evidence.EvaluatedBytes,
            point.Evidence.ViolationCount,
            point.Evidence.ViolationsByRule,
            point.Evidence.State,
            point.Evidence.Source,
            point.Progress.ClusterId,
            point.Progress.TopicName,
            point.Progress.Partition,
            point.Progress.StartOffset,
            point.Progress.EndOffsetExclusive,
            point.Progress.NextOffset,
            point.Progress.Outcome,
            point.Progress.UpdatedAtUtc);
    }
}
