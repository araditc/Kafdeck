using System.Collections.ObjectModel;
using System.Globalization;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Kafka;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Consumers;

public sealed class ConsumerMutationPlanner
{
    private readonly IConsumerMutationObservationPort _observations;
    private readonly ConsumerMutationPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public ConsumerMutationPlanner(
        IConsumerMutationObservationPort observations,
        ConsumerMutationPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _policy = policy ?? ConsumerMutationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>>
        PlanOffsetAlterAsync(
            ConsumerOffsetAlterRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (!TryNormalizeIdentity(
                request.ClusterId,
                request.GroupId,
                out var clusterId,
                out var groupId,
                out var identityFailure))
        {
            return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
                .Failed(identityFailure!);
        }

        var normalizedTargets = NormalizeAlterTargets(request.Targets, out var targetFailure);
        if (targetFailure is not null)
        {
            return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
                .Failed(targetFailure);
        }

        var observation = await ObserveAsync(
                clusterId!,
                groupId!,
                normalizedTargets!
                    .Select(target => new ConsumerMutationObservationTarget(
                        target.TopicName,
                        target.Partition,
                        target.Selector.Kind == ConsumerOffsetSelectorKind.Timestamp
                            ? target.Selector.TimestampUtc!.Value.ToUniversalTime()
                            : null))
                    .ToArray(),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observation.IsSuccess || observation.Value is null)
        {
            return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
                .Failed(MapObservationFailure(observation.Failure));
        }

        var commonFailure = ValidateGroupObservation(groupId!, observation.Value);
        if (commonFailure is not null)
        {
            return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
                .Failed(commonFailure);
        }

        var observedByTarget = IndexObservedPartitions(
            observation.Value.Partitions,
            out var observationFailure);
        if (observationFailure is not null)
        {
            return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
                .Failed(observationFailure);
        }

        var canonicalTargets = new List<ConsumerOffsetCanonicalTarget>(
            normalizedTargets!.Count);
        long totalBackwardDistance = 0;
        long totalForwardDistance = 0;
        var missingCommittedOffsetCount = 0;
        var preconditions = new List<MutationPrecondition>(
            normalizedTargets.Count + 1)
        {
            new(
                "consumer.group",
                ConsumerMutationCanonicalization.GroupPreconditionFingerprint(
                    observation.Value)),
        };

        for (var ordinal = 0; ordinal < normalizedTargets.Count; ordinal++)
        {
            var target = normalizedTargets[ordinal];
            if (!observedByTarget!.TryGetValue(
                    (target.TopicName, target.Partition),
                    out var observed))
            {
                return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
                    .Failed(new ConsumerMutationPlanningFailure(
                        ConsumerMutationPlanningFailureCode.TargetNotFound,
                        "A consumer offset target could not be observed.",
                        ordinal));
            }

            var resolved = ResolveAlterTarget(target, observed, ordinal);
            if (resolved.Failure is not null)
            {
                return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
                    .Failed(resolved.Failure);
            }

            var canonicalTarget = resolved.Target!;
            canonicalTargets.Add(canonicalTarget);
            try
            {
                if (!canonicalTarget.DeltaFromCommitted.HasValue)
                {
                    missingCommittedOffsetCount++;
                }
                else if (canonicalTarget.DeltaFromCommitted.Value < 0)
                {
                    totalBackwardDistance = checked(
                        totalBackwardDistance -
                        canonicalTarget.DeltaFromCommitted.Value);
                }
                else if (canonicalTarget.DeltaFromCommitted.Value > 0)
                {
                    totalForwardDistance = checked(
                        totalForwardDistance +
                        canonicalTarget.DeltaFromCommitted.Value);
                }
            }
            catch (OverflowException)
            {
                return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
                    .Failed(new ConsumerMutationPlanningFailure(
                        ConsumerMutationPlanningFailureCode.OffsetOutOfRange,
                        "Aggregate consumer offset movement exceeds the supported range.",
                        ordinal));
            }

            preconditions.Add(
                new MutationPrecondition(
                    $"consumer.partition/{ordinal:D4}",
                    ConsumerMutationCanonicalization.PartitionPreconditionFingerprint(
                        observed)));
        }

        var canonical = new ConsumerOffsetAlterCanonicalIntent(
            clusterId!,
            groupId!,
            observation.Value.State,
            ConsumerMutationCanonicalization.GroupPreconditionFingerprint(
                observation.Value),
            totalBackwardDistance,
            totalForwardDistance,
            missingCommittedOffsetCount,
            Array.AsReadOnly(canonicalTargets.ToArray()));

        var intent = BuildIntent(
            MutationOperationKind.ConsumerOffsetAlter,
            clusterId!,
            groupId!,
            ConsumerMutationCanonicalization.Serialize(canonical),
            canonicalTargets
                .Select(target => (target.TopicName, target.Partition))
                .ToArray(),
            preconditions);

        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                MutationOperationKind.ConsumerOffsetAlter,
                canonicalTargets.Count));

        return ConsumerMutationPlanningResult<ConsumerOffsetAlterCanonicalIntent>
            .Success(new ConsumerMutationPlan<ConsumerOffsetAlterCanonicalIntent>(
                canonical,
                intent,
                risk));
    }

    public async Task<ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>>
        PlanDeleteAsync(
            ConsumerDeleteRequest request,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (!TryNormalizeIdentity(
                request.ClusterId,
                request.GroupId,
                out var clusterId,
                out var groupId,
                out var identityFailure))
        {
            return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                .Failed(identityFailure!);
        }

        if (!Enum.IsDefined(request.Mode))
        {
            return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                .Failed(Invalid("Consumer delete mode is invalid."));
        }

        IReadOnlyList<ConsumerOffsetDeleteTargetInput> normalizedTargets;
        if (request.Mode == ConsumerDeleteMode.Group)
        {
            if (request.Targets is { Count: > 0 })
            {
                return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                    .Failed(Invalid(
                        "Whole-group deletion cannot also contain offset targets."));
            }

            normalizedTargets = Array.Empty<ConsumerOffsetDeleteTargetInput>();
        }
        else
        {
            normalizedTargets = NormalizeDeleteTargets(
                request.Targets,
                out var targetFailure) ??
                Array.Empty<ConsumerOffsetDeleteTargetInput>();
            if (targetFailure is not null)
            {
                return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                    .Failed(targetFailure);
            }
        }

        var observation = await ObserveAsync(
                clusterId!,
                groupId!,
                normalizedTargets
                    .Select(target => new ConsumerMutationObservationTarget(
                        target.TopicName,
                        target.Partition))
                    .ToArray(),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observation.IsSuccess || observation.Value is null)
        {
            return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                .Failed(MapObservationFailure(observation.Failure));
        }

        var commonFailure = ValidateGroupObservation(groupId!, observation.Value);
        if (commonFailure is not null)
        {
            return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                .Failed(commonFailure);
        }

        var observedByTarget = IndexObservedPartitions(
            observation.Value.Partitions,
            out var observationFailure);
        if (observationFailure is not null)
        {
            return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                .Failed(observationFailure);
        }

        var canonicalTargets = new List<ConsumerDeleteCanonicalTarget>(
            normalizedTargets.Count);
        var preconditions = new List<MutationPrecondition>(
            normalizedTargets.Count + 1)
        {
            new(
                "consumer.group",
                ConsumerMutationCanonicalization.GroupPreconditionFingerprint(
                    observation.Value)),
        };

        for (var ordinal = 0; ordinal < normalizedTargets.Count; ordinal++)
        {
            var target = normalizedTargets[ordinal];
            if (!observedByTarget!.TryGetValue(
                    (target.TopicName, target.Partition),
                    out var observed))
            {
                return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                    .Failed(new ConsumerMutationPlanningFailure(
                        ConsumerMutationPlanningFailureCode.TargetNotFound,
                        "A consumer offset deletion target could not be observed.",
                        ordinal));
            }

            if (observed.CommittedOffset is null)
            {
                return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                    .Failed(new ConsumerMutationPlanningFailure(
                        ConsumerMutationPlanningFailureCode.MissingCommittedOffset,
                        "A consumer offset deletion target has no committed offset.",
                        ordinal));
            }

            var rangeFailure = ValidateObservedRange(observed, ordinal);
            if (rangeFailure is not null)
            {
                return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
                    .Failed(rangeFailure);
            }

            canonicalTargets.Add(
                new ConsumerDeleteCanonicalTarget(
                    ordinal,
                    target.TopicName,
                    target.Partition,
                    false,
                    observed.CommittedOffset,
                    observed.LowWatermark,
                    observed.HighWatermark));

            preconditions.Add(
                new MutationPrecondition(
                    $"consumer.partition/{ordinal:D4}",
                    ConsumerMutationCanonicalization.PartitionPreconditionFingerprint(
                        observed)));
        }

        var canonical = new ConsumerDeleteCanonicalIntent(
            clusterId!,
            groupId!,
            request.Mode,
            observation.Value.State,
            ConsumerMutationCanonicalization.GroupPreconditionFingerprint(
                observation.Value),
            Array.AsReadOnly(canonicalTargets.ToArray()));

        var resourceTargets = canonicalTargets
            .Select(target => (target.TopicName, target.Partition))
            .ToArray();

        var intent = BuildIntent(
            MutationOperationKind.ConsumerDelete,
            clusterId!,
            groupId!,
            ConsumerMutationCanonicalization.Serialize(canonical),
            resourceTargets,
            preconditions);

        var riskTargetCount = Math.Max(1, canonicalTargets.Count);
        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                MutationOperationKind.ConsumerDelete,
                riskTargetCount));

        return ConsumerMutationPlanningResult<ConsumerDeleteCanonicalIntent>
            .Success(new ConsumerMutationPlan<ConsumerDeleteCanonicalIntent>(
                canonical,
                intent,
                risk));
    }

    private async Task<KafkaResult<ConsumerMutationObservation>> ObserveAsync(
        string clusterId,
        string groupId,
        IReadOnlyList<ConsumerMutationObservationTarget> targets,
        CancellationToken cancellationToken) =>
        await _observations.ObserveAsync(
                clusterId,
                groupId,
                targets,
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow().Add(_policy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);

    private ConsumerMutationPlanningFailure? ValidateGroupObservation(
        string expectedGroupId,
        ConsumerMutationObservation observation)
    {
        if (!observation.Exists)
        {
            return new(
                ConsumerMutationPlanningFailureCode.GroupNotFound,
                "The consumer group does not exist.");
        }

        if (!string.Equals(
                observation.GroupId,
                expectedGroupId,
                StringComparison.Ordinal))
        {
            return new(
                ConsumerMutationPlanningFailureCode.ObservationFailed,
                "Kafka returned a consumer group observation for an unexpected group.");
        }

        if (_policy.RequireEmptyGroup &&
            observation.State != ConsumerGroupState.Empty)
        {
            return new(
                ConsumerMutationPlanningFailureCode.GroupStateUnsafe,
                "Consumer administration requires an empty group under the admitted provider policy.");
        }

        return null;
    }

    private IReadOnlyList<ConsumerOffsetAlterTargetInput>? NormalizeAlterTargets(
        IReadOnlyList<ConsumerOffsetAlterTargetInput>? targets,
        out ConsumerMutationPlanningFailure? failure)
    {
        failure = null;
        if (targets is null ||
            targets.Count < 1 ||
            targets.Count > _policy.MaxTargets)
        {
            failure = new(
                ConsumerMutationPlanningFailureCode.LimitExceeded,
                $"Consumer offset alteration requires between 1 and {_policy.MaxTargets} targets.");
            return null;
        }

        var byPartition = new Dictionary<
            (string Topic, int Partition),
            ConsumerOffsetAlterTargetInput>();

        foreach (var input in targets)
        {
            if (input is null || input.Selector is null)
            {
                failure = Invalid("Consumer offset alteration contains a null target.");
                return null;
            }

            string topic;
            try
            {
                topic = ConsumerMutationCanonicalization.RequireTopicName(
                    input.TopicName);
            }
            catch (ArgumentException exception)
            {
                failure = Invalid(exception.Message);
                return null;
            }

            if (input.Partition < 0)
            {
                failure = Invalid("Consumer offset partition must be non-negative.");
                return null;
            }

            var selector = NormalizeSelector(input.Selector, out failure);
            if (failure is not null)
            {
                return null;
            }

            var normalized = new ConsumerOffsetAlterTargetInput(
                topic,
                input.Partition,
                selector!);
            var key = (topic, input.Partition);

            if (byPartition.TryGetValue(key, out var existing))
            {
                if (existing.Selector != normalized.Selector)
                {
                    failure = Invalid(
                        "A consumer offset partition cannot have conflicting selectors.");
                    return null;
                }

                continue;
            }

            byPartition.Add(key, normalized);
        }

        return Array.AsReadOnly(
            byPartition.Values
                .OrderBy(item => item.TopicName, StringComparer.Ordinal)
                .ThenBy(item => item.Partition)
                .ToArray());
    }

    private IReadOnlyList<ConsumerOffsetDeleteTargetInput>? NormalizeDeleteTargets(
        IReadOnlyList<ConsumerOffsetDeleteTargetInput>? targets,
        out ConsumerMutationPlanningFailure? failure)
    {
        failure = null;
        if (targets is null ||
            targets.Count < 1 ||
            targets.Count > _policy.MaxTargets)
        {
            failure = new(
                ConsumerMutationPlanningFailureCode.LimitExceeded,
                $"Consumer offset deletion requires between 1 and {_policy.MaxTargets} targets.");
            return null;
        }

        var normalized = new Dictionary<
            (string Topic, int Partition),
            ConsumerOffsetDeleteTargetInput>();

        foreach (var input in targets)
        {
            if (input is null)
            {
                failure = Invalid("Consumer offset deletion contains a null target.");
                return null;
            }

            string topic;
            try
            {
                topic = ConsumerMutationCanonicalization.RequireTopicName(
                    input.TopicName);
            }
            catch (ArgumentException exception)
            {
                failure = Invalid(exception.Message);
                return null;
            }

            if (input.Partition < 0)
            {
                failure = Invalid("Consumer offset partition must be non-negative.");
                return null;
            }

            normalized.TryAdd(
                (topic, input.Partition),
                new ConsumerOffsetDeleteTargetInput(topic, input.Partition));
        }

        return Array.AsReadOnly(
            normalized.Values
                .OrderBy(item => item.TopicName, StringComparer.Ordinal)
                .ThenBy(item => item.Partition)
                .ToArray());
    }

    private static ConsumerOffsetSelector? NormalizeSelector(
        ConsumerOffsetSelector selector,
        out ConsumerMutationPlanningFailure? failure)
    {
        failure = null;
        if (!Enum.IsDefined(selector.Kind))
        {
            failure = Invalid("Consumer offset selector is invalid.");
            return null;
        }

        switch (selector.Kind)
        {
            case ConsumerOffsetSelectorKind.Absolute:
                if (selector.Value is null or < 0 || selector.TimestampUtc is not null)
                {
                    failure = Invalid(
                        "Absolute offset selector requires one non-negative offset.");
                    return null;
                }
                break;

            case ConsumerOffsetSelectorKind.RelativeShift:
                if (selector.Value is null || selector.TimestampUtc is not null)
                {
                    failure = Invalid(
                        "Relative offset selector requires exactly one signed shift.");
                    return null;
                }
                break;

            case ConsumerOffsetSelectorKind.Timestamp:
                if (selector.Value is not null || selector.TimestampUtc is null)
                {
                    failure = Invalid(
                        "Timestamp offset selector requires exactly one timestamp.");
                    return null;
                }

                selector = selector with
                {
                    TimestampUtc = selector.TimestampUtc.Value.ToUniversalTime(),
                };
                break;

            case ConsumerOffsetSelectorKind.Earliest:
            case ConsumerOffsetSelectorKind.Latest:
                if (selector.Value is not null || selector.TimestampUtc is not null)
                {
                    failure = Invalid(
                        "Earliest/latest offset selectors do not accept a value or timestamp.");
                    return null;
                }
                break;

            default:
                failure = Invalid("Consumer offset selector is invalid.");
                return null;
        }

        return selector;
    }

    private static (
        ConsumerOffsetCanonicalTarget? Target,
        ConsumerMutationPlanningFailure? Failure)
        ResolveAlterTarget(
            ConsumerOffsetAlterTargetInput target,
            ConsumerMutationPartitionObservation observed,
            int ordinal)
    {
        var rangeFailure = ValidateObservedRange(observed, ordinal);
        if (rangeFailure is not null)
        {
            return (null, rangeFailure);
        }

        long resolved;
        try
        {
            resolved = target.Selector.Kind switch
            {
                ConsumerOffsetSelectorKind.Absolute =>
                    target.Selector.Value!.Value,
                ConsumerOffsetSelectorKind.Earliest =>
                    observed.LowWatermark,
                ConsumerOffsetSelectorKind.Latest =>
                    observed.HighWatermark,
                ConsumerOffsetSelectorKind.Timestamp =>
                    observed.TimestampResolvedOffset ??
                    throw new TimestampResolutionException(),
                ConsumerOffsetSelectorKind.RelativeShift when
                    observed.CommittedOffset.HasValue =>
                    checked(
                        observed.CommittedOffset.Value +
                        target.Selector.Value!.Value),
                ConsumerOffsetSelectorKind.RelativeShift =>
                    throw new MissingCommittedOffsetException(),
                _ => throw new InvalidOperationException(),
            };
        }
        catch (TimestampResolutionException)
        {
            return (
                null,
                new ConsumerMutationPlanningFailure(
                    ConsumerMutationPlanningFailureCode.TimestampUnresolved,
                    "Kafka could not resolve the requested timestamp to an explicit offset.",
                    ordinal));
        }
        catch (MissingCommittedOffsetException)
        {
            return (
                null,
                new ConsumerMutationPlanningFailure(
                    ConsumerMutationPlanningFailureCode.MissingCommittedOffset,
                    "Relative offset movement requires an existing committed offset.",
                    ordinal));
        }
        catch (OverflowException)
        {
            return (
                null,
                new ConsumerMutationPlanningFailure(
                    ConsumerMutationPlanningFailureCode.OffsetOutOfRange,
                    "Resolved consumer offset is outside the supported range.",
                    ordinal));
        }

        if (resolved < observed.LowWatermark ||
            resolved > observed.HighWatermark)
        {
            return (
                null,
                new ConsumerMutationPlanningFailure(
                    ConsumerMutationPlanningFailureCode.OffsetOutOfRange,
                    "Resolved consumer offset is outside the observed log range.",
                    ordinal));
        }

        long? delta = observed.CommittedOffset.HasValue
            ? checked(resolved - observed.CommittedOffset.Value)
            : null;
        var movement = delta switch
        {
            null => ConsumerOffsetMovement.MissingCurrent,
            < 0 => ConsumerOffsetMovement.BackwardReplay,
            > 0 => ConsumerOffsetMovement.ForwardSkip,
            _ => ConsumerOffsetMovement.Unchanged,
        };

        return (
            new ConsumerOffsetCanonicalTarget(
                ordinal,
                target.TopicName,
                target.Partition,
                new ConsumerOffsetCanonicalSelector(
                    target.Selector.Kind,
                    target.Selector.Kind is
                        ConsumerOffsetSelectorKind.Absolute or
                        ConsumerOffsetSelectorKind.RelativeShift
                        ? target.Selector.Value
                        : null,
                    target.Selector.Kind == ConsumerOffsetSelectorKind.Timestamp
                        ? target.Selector.TimestampUtc!.Value.ToUnixTimeMilliseconds()
                        : null),
                !observed.CommittedOffset.HasValue,
                observed.CommittedOffset,
                observed.LowWatermark,
                observed.HighWatermark,
                resolved,
                delta,
                movement),
            null);
    }

    private static ConsumerMutationPlanningFailure? ValidateObservedRange(
        ConsumerMutationPartitionObservation observed,
        int ordinal)
    {
        if (observed.Partition < 0 ||
            observed.CommittedOffset is < 0 ||
            observed.LowWatermark < 0 ||
            observed.HighWatermark < observed.LowWatermark)
        {
            return new(
                ConsumerMutationPlanningFailureCode.ObservationFailed,
                "Kafka returned an invalid partition offset range.",
                ordinal);
        }

        return null;
    }

    private static IReadOnlyDictionary<(string Topic, int Partition),
        ConsumerMutationPartitionObservation>? IndexObservedPartitions(
        IReadOnlyList<ConsumerMutationPartitionObservation> partitions,
        out ConsumerMutationPlanningFailure? failure)
    {
        failure = null;
        var result = new Dictionary<
            (string Topic, int Partition),
            ConsumerMutationPartitionObservation>();

        foreach (var partition in partitions)
        {
            if (partition is null ||
                string.IsNullOrWhiteSpace(partition.Topic) ||
                partition.Partition < 0 ||
                !result.TryAdd(
                    (partition.Topic, partition.Partition),
                    partition))
            {
                failure = new(
                    ConsumerMutationPlanningFailureCode.ObservationFailed,
                    "Kafka returned malformed or duplicate consumer partition observations.");
                return null;
            }
        }

        return new ReadOnlyDictionary<
            (string Topic, int Partition),
            ConsumerMutationPartitionObservation>(result);
    }

    private static MutationIntentDescriptor BuildIntent(
        MutationOperationKind kind,
        string clusterId,
        string groupId,
        string canonicalIntent,
        IReadOnlyList<(string Topic, int Partition)> partitions,
        IReadOnlyList<MutationPrecondition> preconditions)
    {
        var resources = new List<string>
        {
            ConsumerMutationCanonicalization.GroupResourceKey(
                clusterId,
                groupId),
        };
        resources.AddRange(
            partitions.Select(target =>
                ConsumerMutationCanonicalization.PartitionResourceKey(
                    clusterId,
                    target.Topic,
                    target.Partition)));

        var action = MutationAuthorization.ExpectedAction(kind);
        var authorizationTargets = new List<MutationAuthorizationTarget>
        {
            new(
                action,
                clusterId,
                ConsumerMutationCanonicalization.GroupAuthorizationResource(
                    groupId)),
        };
        authorizationTargets.AddRange(
            partitions
                .Select(target => target.Topic)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(topic => topic, StringComparer.Ordinal)
                .Select(topic => new MutationAuthorizationTarget(
                    action,
                    clusterId,
                    ConsumerMutationCanonicalization.TopicAuthorizationResource(
                        topic))));

        return new MutationIntentDescriptor(
            kind,
            clusterId,
            canonicalIntent,
            Array.AsReadOnly(
                resources
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(value => value, StringComparer.Ordinal)
                    .ToArray()),
            Array.AsReadOnly(preconditions.ToArray()),
            AuthorizationTargets: Array.AsReadOnly(authorizationTargets.ToArray()));
    }

    private static bool TryNormalizeIdentity(
        string clusterId,
        string groupId,
        out string? normalizedClusterId,
        out string? normalizedGroupId,
        out ConsumerMutationPlanningFailure? failure)
    {
        normalizedClusterId = null;
        normalizedGroupId = null;
        failure = null;
        try
        {
            normalizedClusterId =
                ConsumerMutationCanonicalization.RequireIdentifier(
                    clusterId,
                    "Cluster ID",
                    256);
            normalizedGroupId =
                ConsumerMutationCanonicalization.RequireIdentifier(
                    groupId,
                    "Consumer group ID",
                    255);
            return true;
        }
        catch (ArgumentException exception)
        {
            failure = Invalid(exception.Message);
            return false;
        }
    }

    private static ConsumerMutationPlanningFailure Invalid(string message) =>
        new(ConsumerMutationPlanningFailureCode.InvalidInput, message);

    private static ConsumerMutationPlanningFailure MapObservationFailure(
        KafkaFailure? failure) =>
        failure?.Category switch
        {
            KafkaFailureCategory.Unauthorized =>
                new(
                    ConsumerMutationPlanningFailureCode.ProviderUnauthorized,
                    "Kafka denied access to observe the consumer mutation target."),
            KafkaFailureCategory.NotSupported =>
                new(
                    ConsumerMutationPlanningFailureCode.ProviderUnsupported,
                    "Kafka does not support the required consumer mutation observation."),
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure =>
                new(
                    ConsumerMutationPlanningFailureCode.ProviderUnavailable,
                    "Kafka consumer state is currently unavailable for safe planning."),
            _ =>
                new(
                    ConsumerMutationPlanningFailureCode.ObservationFailed,
                    "Kafka consumer state could not be safely observed."),
        };

    private sealed class TimestampResolutionException : Exception;
    private sealed class MissingCommittedOffsetException : Exception;
}
