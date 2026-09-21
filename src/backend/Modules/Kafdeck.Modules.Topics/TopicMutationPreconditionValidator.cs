using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Topics;

/// <summary>
/// Re-observes topic-specific preview preconditions immediately before dispatch.
/// This validator intentionally does not implement IMutationPreDispatchGuard:
/// requester authorization evidence must be supplied by a later admitted integration
/// layer before W33 handlers can be registered in the production mutation runtime.
/// </summary>
public sealed class TopicMutationPreconditionValidator
{
    private readonly IKafkaAdministrationPort _kafka;
    private readonly TopicMutationPlannerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public TopicMutationPreconditionValidator(
        IKafkaAdministrationPort kafka,
        TopicMutationPlannerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _policy = policy ?? TopicMutationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            return operation.OperationKind switch
            {
                MutationOperationKind.TopicCreate =>
                    await ValidateCreateAsync(operation, cancellationToken).ConfigureAwait(false),
                MutationOperationKind.TopicAlter =>
                    await ValidateAlterAsync(operation, cancellationToken).ConfigureAwait(false),
                MutationOperationKind.TopicIncreasePartitions =>
                    await ValidateMetadataAsync<TopicIncreasePartitionsMutation>(
                        operation,
                        cancellationToken).ConfigureAwait(false),
                MutationOperationKind.TopicDelete =>
                    await ValidateMetadataAsync<TopicDeleteMutation>(
                        operation,
                        cancellationToken).ConfigureAwait(false),
                _ => Unsupported("topic_precondition_operation_not_supported"),
            };
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception) when (
            operation.OperationKind is
                MutationOperationKind.TopicCreate or
                MutationOperationKind.TopicAlter or
                MutationOperationKind.TopicIncreasePartitions or
                MutationOperationKind.TopicDelete)
        {
            return Unsupported("topic_precondition_validation_failed");
        }
    }

    private async Task<MutationPreDispatchGuardResult> ValidateCreateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        var mutation = TopicMutationPolicy.Deserialize<TopicCreateMutation>(
            operation.CanonicalIntent);
        var binding = ValidateBinding(operation, mutation.ClusterId, mutation.TopicName);
        if (binding is not null)
        {
            return binding;
        }

        var context = Operation();
        var cluster = await _kafka.GetClusterMetadataAsync(
                mutation.ClusterId,
                context,
                cancellationToken)
            .ConfigureAwait(false);
        if (!cluster.IsSuccess || cluster.Value is null)
        {
            return Unobservable(cluster.Failure);
        }

        var topics = await _kafka.ListTopicsAsync(
                mutation.ClusterId,
                context,
                cancellationToken)
            .ConfigureAwait(false);
        if (!topics.IsSuccess || topics.Value is null)
        {
            return Unobservable(topics.Failure);
        }

        if (topics.Value.Any(
                item => string.Equals(item.Name, mutation.TopicName, StringComparison.Ordinal)))
        {
            return Stale("topic_precondition_topic_now_exists");
        }

        var expected = RequiredPrecondition(operation, "topic.absence");
        if (expected is null)
        {
            return Stale("topic_precondition_missing");
        }

        var observed = TopicMutationPolicy.FingerprintCreateAbsence(
            cluster.Value,
            mutation.TopicName);
        return string.Equals(expected.Fingerprint, observed, StringComparison.Ordinal)
            ? MutationPreDispatchGuardResult.Allowed
            : Stale("topic_precondition_changed");
    }

    private async Task<MutationPreDispatchGuardResult> ValidateAlterAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        var mutation = TopicMutationPolicy.Deserialize<TopicAlterMutation>(
            operation.CanonicalIntent);
        var binding = ValidateBinding(operation, mutation.ClusterId, mutation.TopicName);
        if (binding is not null)
        {
            return binding;
        }

        var context = Operation();
        var metadata = await _kafka.GetTopicMetadataAsync(
                mutation.ClusterId,
                mutation.TopicName,
                context,
                cancellationToken)
            .ConfigureAwait(false);
        if (!metadata.IsSuccess || metadata.Value is null)
        {
            return IsTopicMissing(metadata.Failure)
                ? Stale("topic_precondition_topic_missing")
                : Unobservable(metadata.Failure);
        }

        if (metadata.Value.IsInternal)
        {
            return Stale("topic_precondition_internal_topic");
        }

        var configuration = await _kafka.GetTopicConfigurationAsync(
                mutation.ClusterId,
                mutation.TopicName,
                context,
                cancellationToken)
            .ConfigureAwait(false);
        if (!configuration.IsSuccess || configuration.Value is null)
        {
            return Unobservable(configuration.Failure);
        }

        var metadataPrecondition = RequiredPrecondition(operation, "topic.metadata");
        var configurationPrecondition = RequiredPrecondition(operation, "topic.configuration");
        if (metadataPrecondition is null || configurationPrecondition is null)
        {
            return Stale("topic_precondition_missing");
        }

        var metadataFingerprint =
            TopicMutationPolicy.FingerprintTopicMetadata(metadata.Value);
        var configurationFingerprint =
            TopicMutationPolicy.FingerprintConfigurations(
                mutation.TopicName,
                configuration.Value,
                mutation.Changes.Keys);

        return
            string.Equals(
                metadataPrecondition.Fingerprint,
                metadataFingerprint,
                StringComparison.Ordinal) &&
            string.Equals(
                configurationPrecondition.Fingerprint,
                configurationFingerprint,
                StringComparison.Ordinal)
                ? MutationPreDispatchGuardResult.Allowed
                : Stale("topic_precondition_changed");
    }

    private async Task<MutationPreDispatchGuardResult> ValidateMetadataAsync<TMutation>(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
        where TMutation : class
    {
        var mutation = TopicMutationPolicy.Deserialize<TMutation>(
            operation.CanonicalIntent);
        var clusterId = mutation switch
        {
            TopicIncreasePartitionsMutation value => value.ClusterId,
            TopicDeleteMutation value => value.ClusterId,
            _ => throw new MutationStateException("Unsupported topic metadata validation type."),
        };
        var topicName = mutation switch
        {
            TopicIncreasePartitionsMutation value => value.TopicName,
            TopicDeleteMutation value => value.TopicName,
            _ => throw new MutationStateException("Unsupported topic metadata validation type."),
        };

        var binding = ValidateBinding(operation, clusterId, topicName);
        if (binding is not null)
        {
            return binding;
        }

        var metadata = await _kafka.GetTopicMetadataAsync(
                clusterId,
                topicName,
                Operation(),
                cancellationToken)
            .ConfigureAwait(false);
        if (!metadata.IsSuccess || metadata.Value is null)
        {
            return IsTopicMissing(metadata.Failure)
                ? Stale("topic_precondition_topic_missing")
                : Unobservable(metadata.Failure);
        }

        if (metadata.Value.IsInternal)
        {
            return Stale("topic_precondition_internal_topic");
        }

        var expected = RequiredPrecondition(operation, "topic.metadata");
        if (expected is null)
        {
            return Stale("topic_precondition_missing");
        }

        var observed = TopicMutationPolicy.FingerprintTopicMetadata(metadata.Value);
        return string.Equals(expected.Fingerprint, observed, StringComparison.Ordinal)
            ? MutationPreDispatchGuardResult.Allowed
            : Stale("topic_precondition_changed");
    }

    private static MutationPreDispatchGuardResult? ValidateBinding(
        MutationOperationSnapshot operation,
        string clusterId,
        string topicName)
    {
        var expectedResource = TopicMutationPolicy.ResourceKey(clusterId, topicName);
        var expectedAction = MutationAuthorization.ExpectedAction(operation.OperationKind);

        if (!string.Equals(operation.ClusterId, clusterId, StringComparison.Ordinal) ||
            operation.ResourceKeys.Count != 1 ||
            !string.Equals(operation.ResourceKeys[0], expectedResource, StringComparison.Ordinal) ||
            operation.AuthorizationTargets.Count != 1)
        {
            return Stale("topic_precondition_binding_changed");
        }

        var target = operation.AuthorizationTargets[0];
        return
            target.Action == expectedAction &&
            string.Equals(target.ClusterId, clusterId, StringComparison.Ordinal) &&
            string.Equals(target.ResourceName, topicName, StringComparison.Ordinal)
                ? null
                : Stale("topic_precondition_binding_changed");
    }

    private KafkaOperationContext Operation() =>
        new(_timeProvider.GetUtcNow().Add(_policy.ObservationTimeout));

    private static MutationPrecondition? RequiredPrecondition(
        MutationOperationSnapshot operation,
        string key)
    {
        var matches = operation.Preconditions
            .Where(item => string.Equals(item.Key, key, StringComparison.Ordinal))
            .Take(2)
            .ToArray();

        return matches.Length == 1 ? matches[0] : null;
    }

    private static MutationPreDispatchGuardResult Stale(string code) =>
        new(MutationPreDispatchGuardOutcome.StalePreview, code);

    private static MutationPreDispatchGuardResult Unsupported(string code) =>
        new(MutationPreDispatchGuardOutcome.CapabilityUnsupported, code);

    private static MutationPreDispatchGuardResult Unobservable(KafkaFailure? failure) =>
        Unsupported(
            failure?.Category switch
            {
                KafkaFailureCategory.Unauthorized => "topic_precondition_authorization_unavailable",
                KafkaFailureCategory.NotSupported => "topic_precondition_capability_unsupported",
                _ => "topic_precondition_observation_unavailable",
            });

    private static bool IsTopicMissing(KafkaFailure? failure) =>
        failure?.Code is
            "kafka_unknowntopicorpart" or
            "kafka_local_unknowntopic" or
            "kafka_resourcenotfound";
}
