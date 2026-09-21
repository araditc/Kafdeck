using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Topics;

public sealed record TopicBulkMutationPlanningFailure(
    string? TopicName,
    TopicMutationPlanningFailureCode Code,
    string SafeMessage);

public sealed record TopicBulkMutationPlan<TMutation>(
    IReadOnlyList<TopicMutationPlan<TMutation>> Items,
    IReadOnlyList<string> ResourceKeys,
    IReadOnlyList<MutationAuthorizationTarget> AuthorizationTargets,
    MutationRiskDecision Risk)
    where TMutation : class;

public sealed record TopicBulkMutationPlanningResult<TMutation>(
    TopicBulkMutationPlan<TMutation>? Plan,
    TopicBulkMutationPlanningFailure? Failure)
    where TMutation : class
{
    public bool IsSuccess => Plan is not null && Failure is null;

    public static TopicBulkMutationPlanningResult<TMutation> Success(
        TopicBulkMutationPlan<TMutation> plan) =>
        new(plan ?? throw new ArgumentNullException(nameof(plan)), null);

    public static TopicBulkMutationPlanningResult<TMutation> Failed(
        TopicBulkMutationPlanningFailure failure) =>
        new(null, failure ?? throw new ArgumentNullException(nameof(failure)));
}

/// <summary>
/// Materializes a finite, explicit set of same-kind topic mutation previews.
/// It is deliberately planning-only: no target is executed unless every target
/// can be normalized and previewed successfully.
/// </summary>
public sealed class TopicBulkMutationMaterializer
{
    private readonly TopicMutationPlanner _planner;

    public TopicBulkMutationMaterializer(TopicMutationPlanner planner)
    {
        _planner = planner ?? throw new ArgumentNullException(nameof(planner));
    }

    public Task<TopicBulkMutationPlanningResult<TopicCreateMutation>> PlanCreatesAsync(
        IReadOnlyList<TopicCreateMutation> requests,
        CancellationToken cancellationToken = default) =>
        PlanAsync(
            requests,
            TopicMutationPolicy.NormalizeCreate,
            _planner.PlanCreateAsync,
            cancellationToken);

    public Task<TopicBulkMutationPlanningResult<TopicAlterMutation>> PlanAltersAsync(
        IReadOnlyList<TopicAlterMutation> requests,
        CancellationToken cancellationToken = default) =>
        PlanAsync(
            requests,
            TopicMutationPolicy.NormalizeAlter,
            _planner.PlanAlterAsync,
            cancellationToken);

    public Task<TopicBulkMutationPlanningResult<TopicIncreasePartitionsMutation>>
        PlanPartitionIncreasesAsync(
            IReadOnlyList<TopicIncreasePartitionsMutation> requests,
            CancellationToken cancellationToken = default) =>
        PlanAsync(
            requests,
            TopicMutationPolicy.NormalizeIncreasePartitions,
            _planner.PlanIncreasePartitionsAsync,
            cancellationToken);

    public Task<TopicBulkMutationPlanningResult<TopicDeleteMutation>> PlanDeletesAsync(
        IReadOnlyList<TopicDeleteMutation> requests,
        CancellationToken cancellationToken = default) =>
        PlanAsync(
            requests,
            TopicMutationPolicy.NormalizeDelete,
            _planner.PlanDeleteAsync,
            cancellationToken);

    private static TopicBulkMutationPlanningResult<TMutation> Invalid<TMutation>(
        string safeMessage,
        string? topicName = null)
        where TMutation : class =>
        TopicBulkMutationPlanningResult<TMutation>.Failed(
            new TopicBulkMutationPlanningFailure(
                topicName,
                TopicMutationPlanningFailureCode.InvalidInput,
                safeMessage));

    private async Task<TopicBulkMutationPlanningResult<TMutation>> PlanAsync<TMutation>(
        IReadOnlyList<TMutation> requests,
        Func<TMutation, TMutation> normalize,
        Func<TMutation, CancellationToken, Task<TopicMutationPlanningResult<TMutation>>> plan,
        CancellationToken cancellationToken)
        where TMutation : class
    {
        ArgumentNullException.ThrowIfNull(requests);
        ArgumentNullException.ThrowIfNull(normalize);
        ArgumentNullException.ThrowIfNull(plan);

        if (requests.Count is < 1 or > TopicMutationPolicy.MaxBulkTopics)
        {
            return Invalid<TMutation>(
                $"Bulk topic mutation requires between 1 and {TopicMutationPolicy.MaxBulkTopics} explicit targets.");
        }

        var normalizedByTopic = new SortedDictionary<string, TMutation>(StringComparer.Ordinal);
        string? clusterId = null;

        foreach (var request in requests)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (request is null)
            {
                return Invalid<TMutation>("Bulk topic mutation contains a null target.");
            }

            TMutation normalized;
            try
            {
                normalized = normalize(request);
            }
            catch (TopicMutationPolicyException exception)
            {
                return TopicBulkMutationPlanningResult<TMutation>.Failed(
                    new TopicBulkMutationPlanningFailure(
                        TopicNameOrNull(request),
                        exception.Code,
                        exception.Message));
            }
            catch (ArgumentException)
            {
                return Invalid<TMutation>(
                    "Bulk topic mutation contains an invalid target.",
                    TopicNameOrNull(request));
            }

            var targetCluster = ClusterId(normalized);
            var topicName = TopicName(normalized);
            clusterId ??= targetCluster;

            if (!string.Equals(clusterId, targetCluster, StringComparison.Ordinal))
            {
                return Invalid<TMutation>(
                    "A bulk topic mutation must target exactly one configured Kafka cluster.",
                    topicName);
            }

            if (normalizedByTopic.TryGetValue(topicName, out var existing))
            {
                if (!string.Equals(
                        TopicMutationPolicy.Serialize(existing),
                        TopicMutationPolicy.Serialize(normalized),
                        StringComparison.Ordinal))
                {
                    return Invalid<TMutation>(
                        "Conflicting duplicate topic targets are not allowed in a bulk mutation.",
                        topicName);
                }

                continue;
            }

            normalizedByTopic.Add(topicName, normalized);
        }

        var items = new List<TopicMutationPlan<TMutation>>(normalizedByTopic.Count);
        foreach (var pair in normalizedByTopic)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = await plan(pair.Value, cancellationToken).ConfigureAwait(false);

            if (!result.IsSuccess || result.Plan is null)
            {
                var failure = result.Failure ??
                              new TopicMutationPlanningFailure(
                                  TopicMutationPlanningFailureCode.ObservationFailed,
                                  "Topic mutation planning failed without safe evidence.");
                return TopicBulkMutationPlanningResult<TMutation>.Failed(
                    new TopicBulkMutationPlanningFailure(
                        pair.Key,
                        failure.Code,
                        failure.SafeMessage));
            }

            items.Add(result.Plan);
        }

        if (items.Count == 0)
        {
            return Invalid<TMutation>("Bulk topic mutation materialized no targets.");
        }

        var resourceKeys = items
            .SelectMany(item => item.Intent.ResourceKeys)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();

        var authorizationTargets = items
            .SelectMany(item => item.Intent.AuthorizationTargets ?? Array.Empty<MutationAuthorizationTarget>())
            .Distinct()
            .OrderBy(item => (int)item.Action)
            .ThenBy(item => item.ClusterId, StringComparer.Ordinal)
            .ThenBy(item => item.ResourceName, StringComparer.Ordinal)
            .ToArray();

        if (resourceKeys.Length != items.Count ||
            authorizationTargets.Length != items.Count)
        {
            return Invalid<TMutation>(
                "Bulk topic mutation target materialization was not one-to-one and is rejected.");
        }

        var firstIntent = items[0].Intent;
        if (items.Any(item => item.Intent.Kind != firstIntent.Kind))
        {
            return Invalid<TMutation>("A bulk topic mutation cannot mix operation kinds.");
        }

        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                firstIntent.Kind,
                items.Count,
                PermanentDelete: items.Any(
                    item => item.Intent.RiskContext?.PermanentDelete == true),
                DurabilitySensitiveChange: items.Any(
                    item => item.Intent.RiskContext?.DurabilitySensitiveChange == true)));

        return TopicBulkMutationPlanningResult<TMutation>.Success(
            new TopicBulkMutationPlan<TMutation>(
                Array.AsReadOnly(items.ToArray()),
                Array.AsReadOnly(resourceKeys),
                Array.AsReadOnly(authorizationTargets),
                risk));
    }

    private static string ClusterId<TMutation>(TMutation mutation) =>
        mutation switch
        {
            TopicCreateMutation value => value.ClusterId,
            TopicAlterMutation value => value.ClusterId,
            TopicIncreasePartitionsMutation value => value.ClusterId,
            TopicDeleteMutation value => value.ClusterId,
            _ => throw new ArgumentOutOfRangeException(
                nameof(mutation),
                typeof(TMutation).Name,
                "Unsupported topic mutation type."),
        };

    private static string TopicName<TMutation>(TMutation mutation) =>
        mutation switch
        {
            TopicCreateMutation value => value.TopicName,
            TopicAlterMutation value => value.TopicName,
            TopicIncreasePartitionsMutation value => value.TopicName,
            TopicDeleteMutation value => value.TopicName,
            _ => throw new ArgumentOutOfRangeException(
                nameof(mutation),
                typeof(TMutation).Name,
                "Unsupported topic mutation type."),
        };

    private static string? TopicNameOrNull<TMutation>(TMutation? mutation) =>
        mutation switch
        {
            TopicCreateMutation value => value.TopicName,
            TopicAlterMutation value => value.TopicName,
            TopicIncreasePartitionsMutation value => value.TopicName,
            TopicDeleteMutation value => value.TopicName,
            _ => null,
        };
}
