using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Topics;

public enum TopicMutationPlanningFailureCode
{
    InvalidInput = 1,
    InvalidTopicName = 2,
    InvalidPartitionCount = 3,
    InvalidReplicationFactor = 4,
    ConfigurationKeyUnsupported = 5,
    InvalidConfigurationValue = 6,
    ConfigurationReadOnly = 7,
    TopicAlreadyExists = 8,
    TopicNotFound = 9,
    InternalTopicUnsupported = 10,
    ReplicationFactorUnsatisfied = 11,
    ProviderUnauthorized = 12,
    ProviderUnsupported = 13,
    ProviderUnavailable = 14,
    ObservationFailed = 15,
}

public sealed record TopicMutationPlanningFailure(
    TopicMutationPlanningFailureCode Code,
    string SafeMessage);

public sealed record TopicMutationPlan<TMutation>(
    TMutation Mutation,
    MutationIntentDescriptor Intent,
    MutationRiskDecision Risk)
    where TMutation : class;

public sealed record TopicMutationPlanningResult<TMutation>
    where TMutation : class
{
    private TopicMutationPlanningResult(
        TopicMutationPlan<TMutation>? plan,
        TopicMutationPlanningFailure? failure)
    {
        Plan = plan;
        Failure = failure;
    }

    public TopicMutationPlan<TMutation>? Plan { get; }
    public TopicMutationPlanningFailure? Failure { get; }
    public bool IsSuccess => Plan is not null && Failure is null;

    public static TopicMutationPlanningResult<TMutation> Success(
        TopicMutationPlan<TMutation> plan) =>
        new(plan ?? throw new ArgumentNullException(nameof(plan)), null);

    public static TopicMutationPlanningResult<TMutation> Failed(
        TopicMutationPlanningFailure failure) =>
        new(null, failure ?? throw new ArgumentNullException(nameof(failure)));
}

public sealed record TopicMutationPlannerPolicy
{
    public TopicMutationPlannerPolicy(TimeSpan observationTimeout)
    {
        if (observationTimeout < TimeSpan.FromSeconds(1) ||
            observationTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(observationTimeout));
        }

        ObservationTimeout = observationTimeout;
    }

    public TimeSpan ObservationTimeout { get; }

    public static TopicMutationPlannerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10));
}

public sealed class TopicMutationPlanner
{
    private readonly IKafkaAdministrationPort _kafka;
    private readonly TopicMutationPlannerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public TopicMutationPlanner(
        IKafkaAdministrationPort kafka,
        TopicMutationPlannerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _policy = policy ?? TopicMutationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<TopicMutationPlanningResult<TopicCreateMutation>> PlanCreateAsync(
        TopicCreateMutation request,
        CancellationToken cancellationToken = default)
    {
        TopicCreateMutation mutation;
        try
        {
            mutation = TopicMutationPolicy.NormalizeCreate(request);
        }
        catch (TopicMutationPolicyException exception)
        {
            return Failed<TopicCreateMutation>(exception.Code, exception.Message);
        }

        var operation = Operation();
        var cluster = await _kafka.GetClusterMetadataAsync(
                mutation.ClusterId,
                operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (!cluster.IsSuccess || cluster.Value is null)
        {
            return Failed<TopicCreateMutation>(MapFailure(cluster.Failure!));
        }

        if (mutation.ReplicationFactor > cluster.Value.Brokers.Count)
        {
            return Failed<TopicCreateMutation>(
                TopicMutationPlanningFailureCode.ReplicationFactorUnsatisfied,
                "Requested replication factor exceeds the currently observed broker count.");
        }

        var topics = await _kafka.ListTopicsAsync(
                mutation.ClusterId,
                operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (!topics.IsSuccess || topics.Value is null)
        {
            return Failed<TopicCreateMutation>(MapFailure(topics.Failure!));
        }

        if (topics.Value.Any(
                topic => string.Equals(
                    topic.Name,
                    mutation.TopicName,
                    StringComparison.Ordinal)))
        {
            return Failed<TopicCreateMutation>(
                TopicMutationPlanningFailureCode.TopicAlreadyExists,
                "The requested topic already exists.");
        }

        var preconditions = new[]
        {
            new MutationPrecondition(
                "topic.absence",
                TopicMutationPolicy.FingerprintCreateAbsence(
                    cluster.Value,
                    mutation.TopicName)),
        };

        return Success(
            mutation,
            TopicMutationPolicy.BuildIntent(mutation, preconditions));
    }

    public async Task<TopicMutationPlanningResult<TopicAlterMutation>> PlanAlterAsync(
        TopicAlterMutation request,
        CancellationToken cancellationToken = default)
    {
        TopicAlterMutation mutation;
        try
        {
            mutation = TopicMutationPolicy.NormalizeAlter(request);
        }
        catch (TopicMutationPolicyException exception)
        {
            return Failed<TopicAlterMutation>(exception.Code, exception.Message);
        }

        var operation = Operation();
        var metadata = await _kafka.GetTopicMetadataAsync(
                mutation.ClusterId,
                mutation.TopicName,
                operation,
                cancellationToken)
            .ConfigureAwait(false);
        var metadataFailure = RequireMutableTopic<TopicAlterMutation>(metadata);
        if (metadataFailure is not null)
        {
            return metadataFailure;
        }

        var configurations = await _kafka.GetTopicConfigurationAsync(
                mutation.ClusterId,
                mutation.TopicName,
                operation,
                cancellationToken)
            .ConfigureAwait(false);
        if (!configurations.IsSuccess || configurations.Value is null)
        {
            return Failed<TopicAlterMutation>(MapFailure(configurations.Failure!));
        }

        var byName = configurations.Value.ToDictionary(
            entry => entry.Name,
            StringComparer.Ordinal);

        foreach (var key in mutation.Changes.Keys)
        {
            if (!byName.TryGetValue(key, out var entry))
            {
                return Failed<TopicAlterMutation>(
                    TopicMutationPlanningFailureCode.ConfigurationKeyUnsupported,
                    $"Topic configuration '{key}' is not observable on this provider.");
            }

            if (entry.IsReadOnly || entry.IsSensitive)
            {
                return Failed<TopicAlterMutation>(
                    TopicMutationPlanningFailureCode.ConfigurationReadOnly,
                    $"Topic configuration '{key}' cannot be safely mutated.");
            }
        }

        var preconditions = new[]
        {
            new MutationPrecondition(
                "topic.metadata",
                TopicMutationPolicy.FingerprintTopicMetadata(metadata.Value!)),
            new MutationPrecondition(
                "topic.configuration",
                TopicMutationPolicy.FingerprintConfigurations(
                    mutation.TopicName,
                    configurations.Value,
                    mutation.Changes.Keys)),
        };

        return Success(
            mutation,
            TopicMutationPolicy.BuildIntent(mutation, preconditions));
    }

    public async Task<TopicMutationPlanningResult<TopicIncreasePartitionsMutation>>
        PlanIncreasePartitionsAsync(
            TopicIncreasePartitionsMutation request,
            CancellationToken cancellationToken = default)
    {
        TopicIncreasePartitionsMutation mutation;
        try
        {
            mutation = TopicMutationPolicy.NormalizeIncreasePartitions(request);
        }
        catch (TopicMutationPolicyException exception)
        {
            return Failed<TopicIncreasePartitionsMutation>(
                exception.Code,
                exception.Message);
        }

        var metadata = await _kafka.GetTopicMetadataAsync(
                mutation.ClusterId,
                mutation.TopicName,
                Operation(),
                cancellationToken)
            .ConfigureAwait(false);
        var metadataFailure =
            RequireMutableTopic<TopicIncreasePartitionsMutation>(metadata);
        if (metadataFailure is not null)
        {
            return metadataFailure;
        }

        var currentCount = metadata.Value!.Partitions.Count;
        if (mutation.NewPartitionCount <= currentCount)
        {
            return Failed<TopicIncreasePartitionsMutation>(
                TopicMutationPlanningFailureCode.InvalidPartitionCount,
                "Target partition count must be greater than the current partition count.");
        }

        var preconditions = new[]
        {
            new MutationPrecondition(
                "topic.metadata",
                TopicMutationPolicy.FingerprintTopicMetadata(metadata.Value)),
        };

        return Success(
            mutation,
            TopicMutationPolicy.BuildIntent(mutation, preconditions));
    }

    public async Task<TopicMutationPlanningResult<TopicDeleteMutation>> PlanDeleteAsync(
        TopicDeleteMutation request,
        CancellationToken cancellationToken = default)
    {
        TopicDeleteMutation mutation;
        try
        {
            mutation = TopicMutationPolicy.NormalizeDelete(request);
        }
        catch (TopicMutationPolicyException exception)
        {
            return Failed<TopicDeleteMutation>(exception.Code, exception.Message);
        }

        var metadata = await _kafka.GetTopicMetadataAsync(
                mutation.ClusterId,
                mutation.TopicName,
                Operation(),
                cancellationToken)
            .ConfigureAwait(false);
        var metadataFailure = RequireMutableTopic<TopicDeleteMutation>(metadata);
        if (metadataFailure is not null)
        {
            return metadataFailure;
        }

        var preconditions = new[]
        {
            new MutationPrecondition(
                "topic.metadata",
                TopicMutationPolicy.FingerprintTopicMetadata(metadata.Value!)),
        };

        return Success(
            mutation,
            TopicMutationPolicy.BuildIntent(mutation, preconditions));
    }

    private TopicMutationPlanningResult<TMutation>? RequireMutableTopic<TMutation>(
        KafkaResult<TopicMetadata> metadata)
        where TMutation : class
    {
        if (!metadata.IsSuccess || metadata.Value is null)
        {
            return Failed<TMutation>(MapFailure(metadata.Failure!));
        }

        if (metadata.Value.IsInternal)
        {
            return Failed<TMutation>(
                TopicMutationPlanningFailureCode.InternalTopicUnsupported,
                "Internal Kafka topics are not mutable through W33.");
        }

        return null;
    }

    private TopicMutationPlanningResult<TMutation> Success<TMutation>(
        TMutation mutation,
        MutationIntentDescriptor intent)
        where TMutation : class
    {
        var riskContext = intent.RiskContext ?? new MutationRiskContext();
        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                intent.Kind,
                intent.ResourceKeys.Count,
                riskContext.PermanentDelete,
                riskContext.DurabilitySensitiveChange));

        return TopicMutationPlanningResult<TMutation>.Success(
            new TopicMutationPlan<TMutation>(
                mutation,
                intent,
                risk));
    }

    private KafkaOperationContext Operation() =>
        new(_timeProvider.GetUtcNow().Add(_policy.ObservationTimeout));

    private static TopicMutationPlanningResult<TMutation> Failed<TMutation>(
        TopicMutationPlanningFailureCode code,
        string safeMessage)
        where TMutation : class =>
        TopicMutationPlanningResult<TMutation>.Failed(
            new TopicMutationPlanningFailure(code, safeMessage));

    private static TopicMutationPlanningResult<TMutation> Failed<TMutation>(
        TopicMutationPlanningFailure failure)
        where TMutation : class =>
        TopicMutationPlanningResult<TMutation>.Failed(failure);

    private static TopicMutationPlanningFailure MapFailure(KafkaFailure failure)
    {
        if (failure.Code is
            "kafka_unknowntopicorpart" or
            "kafka_local_unknowntopic" or
            "kafka_resourcenotfound")
        {
            return new TopicMutationPlanningFailure(
                TopicMutationPlanningFailureCode.TopicNotFound,
                "The requested topic was not found.");
        }

        return failure.Category switch
        {
            KafkaFailureCategory.Unauthorized =>
                new(
                    TopicMutationPlanningFailureCode.ProviderUnauthorized,
                    "Kafka denied access to the requested topic operation."),
            KafkaFailureCategory.NotSupported =>
                new(
                    TopicMutationPlanningFailureCode.ProviderUnsupported,
                    "Kafka does not support the required topic capability."),
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure =>
                new(
                    TopicMutationPlanningFailureCode.ProviderUnavailable,
                    "Kafka is not currently observable for safe mutation planning."),
            _ =>
                new(
                    TopicMutationPlanningFailureCode.ObservationFailed,
                    "Kafka topic state could not be safely observed."),
        };
    }
}
