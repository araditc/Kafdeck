using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Consumers;

public sealed class ConsumerMutationPreconditionValidator
{
    private readonly IConsumerMutationObservationPort _observations;
    private readonly ConsumerMutationPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public ConsumerMutationPreconditionValidator(
        IConsumerMutationObservationPort observations,
        ConsumerMutationPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _policy = policy ?? ConsumerMutationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        return operation.OperationKind switch
        {
            MutationOperationKind.ConsumerOffsetAlter =>
                await ValidateAlterAsync(operation, cancellationToken)
                    .ConfigureAwait(false),
            MutationOperationKind.ConsumerDelete =>
                await ValidateDeleteAsync(operation, cancellationToken)
                    .ConfigureAwait(false),
            _ => new(
                MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                "consumer_precondition_operation_not_supported"),
        };
    }

    private async Task<MutationPreDispatchGuardResult> ValidateAlterAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        ConsumerOffsetAlterCanonicalIntent canonical;
        try
        {
            canonical =
                ConsumerMutationCanonicalization
                    .Deserialize<ConsumerOffsetAlterCanonicalIntent>(
                        operation.CanonicalIntent);
        }
        catch
        {
            return Stale("consumer_offset_intent_invalid");
        }

        if (!BindingsMatch(
                operation,
                canonical.ClusterId,
                canonical.GroupId,
                canonical.Targets.Select(
                    target => (target.TopicName, target.Partition))))
        {
            return Stale("consumer_offset_binding_changed");
        }

        var observationTargets = canonical.Targets
            .OrderBy(target => target.Ordinal)
            .Select(target => new ConsumerMutationObservationTarget(
                target.TopicName,
                target.Partition,
                target.Selector.TimestampUnixMilliseconds.HasValue
                    ? DateTimeOffset.FromUnixTimeMilliseconds(
                        target.Selector.TimestampUnixMilliseconds.Value)
                    : null))
            .ToArray();

        return await ValidateObservedStateAsync(
                operation,
                canonical.GroupId,
                observationTargets,
                canonical.Targets.Select(
                    target => (target.Ordinal, target.TopicName, target.Partition)),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<MutationPreDispatchGuardResult> ValidateDeleteAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken)
    {
        ConsumerDeleteCanonicalIntent canonical;
        try
        {
            canonical =
                ConsumerMutationCanonicalization
                    .Deserialize<ConsumerDeleteCanonicalIntent>(
                        operation.CanonicalIntent);
        }
        catch
        {
            return Stale("consumer_delete_intent_invalid");
        }

        if (!Enum.IsDefined(canonical.Mode) ||
            !BindingsMatch(
                operation,
                canonical.ClusterId,
                canonical.GroupId,
                canonical.Targets.Select(
                    target => (target.TopicName, target.Partition))))
        {
            return Stale("consumer_delete_binding_changed");
        }

        var observationTargets = canonical.Targets
            .OrderBy(target => target.Ordinal)
            .Select(target => new ConsumerMutationObservationTarget(
                target.TopicName,
                target.Partition))
            .ToArray();

        return await ValidateObservedStateAsync(
                operation,
                canonical.GroupId,
                observationTargets,
                canonical.Targets.Select(
                    target => (target.Ordinal, target.TopicName, target.Partition)),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<MutationPreDispatchGuardResult> ValidateObservedStateAsync(
        MutationOperationSnapshot operation,
        string groupId,
        IReadOnlyList<ConsumerMutationObservationTarget> observationTargets,
        IEnumerable<(int Ordinal, string Topic, int Partition)> canonicalTargets,
        CancellationToken cancellationToken)
    {
        var observed = await _observations.ObserveAsync(
                operation.ClusterId,
                groupId,
                observationTargets,
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow().Add(_policy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess || observed.Value is null)
        {
            return new(
                MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                "consumer_precondition_observation_unavailable");
        }

        if (!observed.Value.Exists ||
            !string.Equals(
                observed.Value.GroupId,
                groupId,
                StringComparison.Ordinal))
        {
            return Stale("consumer_precondition_group_changed");
        }

        if (_policy.RequireEmptyGroup &&
            observed.Value.State != ConsumerGroupState.Empty)
        {
            return Stale("consumer_precondition_group_active");
        }

        if (!TryGetUniquePreconditions(
                operation.Preconditions,
                out var preconditions))
        {
            return Stale("consumer_precondition_shape_changed");
        }

        if (!preconditions!.TryGetValue(
                "consumer.group",
                out var expectedGroupFingerprint) ||
            !string.Equals(
                expectedGroupFingerprint,
                ConsumerMutationCanonicalization.GroupPreconditionFingerprint(
                    observed.Value),
                StringComparison.Ordinal))
        {
            return Stale("consumer_precondition_group_changed");
        }

        var byPartition = new Dictionary<
            (string Topic, int Partition),
            ConsumerMutationPartitionObservation>();
        foreach (var partition in observed.Value.Partitions)
        {
            if (partition is null ||
                !byPartition.TryAdd(
                    (partition.Topic, partition.Partition),
                    partition))
            {
                return Stale("consumer_precondition_observation_invalid");
            }
        }

        foreach (var target in canonicalTargets.OrderBy(item => item.Ordinal))
        {
            if (!byPartition.TryGetValue(
                    (target.Topic, target.Partition),
                    out var partition))
            {
                return Stale("consumer_precondition_partition_missing");
            }

            var key = $"consumer.partition/{target.Ordinal:D4}";
            if (!preconditions.TryGetValue(key, out var expected) ||
                !string.Equals(
                    expected,
                    ConsumerMutationCanonicalization
                        .PartitionPreconditionFingerprint(partition),
                    StringComparison.Ordinal))
            {
                return Stale("consumer_precondition_partition_changed");
            }
        }

        if (preconditions.Count != observationTargets.Count + 1)
        {
            return Stale("consumer_precondition_shape_changed");
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private static bool BindingsMatch(
        MutationOperationSnapshot operation,
        string clusterId,
        string groupId,
        IEnumerable<(string Topic, int Partition)> targets)
    {
        if (!string.Equals(operation.ClusterId, clusterId, StringComparison.Ordinal))
            return false;

        var normalizedTargets = targets
            .Distinct()
            .OrderBy(item => item.Topic, StringComparer.Ordinal)
            .ThenBy(item => item.Partition)
            .ToArray();

        var expectedResources = new[]
            {
                ConsumerMutationCanonicalization.GroupResourceKey(
                    clusterId,
                    groupId),
            }
            .Concat(
                normalizedTargets.Select(target =>
                    ConsumerMutationCanonicalization.PartitionResourceKey(
                        clusterId,
                        target.Topic,
                        target.Partition)))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();

        if (!operation.ResourceKeys
                .OrderBy(value => value, StringComparer.Ordinal)
                .SequenceEqual(expectedResources, StringComparer.Ordinal))
        {
            return false;
        }

        var action = MutationAuthorization.ExpectedAction(operation.OperationKind);
        var expectedAuthorization = new[]
            {
                new MutationAuthorizationTarget(
                    action,
                    clusterId,
                    ConsumerMutationCanonicalization.GroupAuthorizationResource(
                        groupId)),
            }
            .Concat(
                normalizedTargets
                    .Select(target => target.Topic)
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(topic => topic, StringComparer.Ordinal)
                    .Select(topic => new MutationAuthorizationTarget(
                        action,
                        clusterId,
                        ConsumerMutationCanonicalization.TopicAuthorizationResource(
                            topic))))
            .OrderBy(target => target.ResourceName, StringComparer.Ordinal)
            .ToArray();

        return operation.AuthorizationTargets
            .OrderBy(target => target.ResourceName, StringComparer.Ordinal)
            .SequenceEqual(expectedAuthorization);
    }

    private static bool TryGetUniquePreconditions(
        IReadOnlyList<MutationPrecondition> values,
        out IReadOnlyDictionary<string, string>? result)
    {
        var dictionary = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var value in values)
        {
            if (value is null ||
                !dictionary.TryAdd(value.Key, value.Fingerprint))
            {
                result = null;
                return false;
            }
        }

        result = dictionary;
        return true;
    }

    private static MutationPreDispatchGuardResult Stale(string code) =>
        new(MutationPreDispatchGuardOutcome.StalePreview, code);
}
