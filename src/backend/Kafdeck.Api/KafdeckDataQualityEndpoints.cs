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

                        return Results.Ok(
                            new DataQualityPolicyPageData(
                                page.Items
                                    .Select(
                                        DataQualityPolicyData.From)
                                    .ToArray(),
                                page.Truncated,
                                page.NextPolicyId));
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
            .RequireKafdeckAuthorization(
                AuthorizationAction.DataQualityRead,
                "clusterId");

        app.MapGet(
                "/api/v1/clusters/{clusterId}/data-quality/policies/{policyId}",
                async (
                    string clusterId,
                    string policyId,
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

                        return PolicyBelongsToCluster(
                                snapshot,
                                clusterId)
                            ? Results.Ok(
                                DataQualityPolicyData.From(
                                    snapshot!))
                            : PolicyNotFound();
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

                        return Results.Ok(
                            new DataQualityEvidencePageData(
                                page.Points
                                    .Select(
                                        DataQualityEvidenceData.From)
                                    .ToArray(),
                                page.Truncated));
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

                            var definition =
                                request.BuildDefinition(
                                    clusterId,
                                    policyId);

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
    string? NextPolicyId);

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
    bool Truncated);

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
