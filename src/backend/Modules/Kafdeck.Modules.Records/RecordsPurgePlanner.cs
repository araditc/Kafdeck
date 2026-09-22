using System.Collections.ObjectModel;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed class RecordsPurgePlanner
{
    private readonly IRecordsPurgeObservationPort _observations;
    private readonly RecordsPurgePolicy _policy;
    private readonly TimeProvider _timeProvider;

    public RecordsPurgePlanner(
        IRecordsPurgeObservationPort observations,
        RecordsPurgePolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ??
            throw new ArgumentNullException(nameof(observations));
        _policy = policy ?? RecordsPurgePolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<RecordsPurgePlanningResult> PlanAsync(
        RecordsPurgeRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        try
        {
            clusterId = RecordsPurgeCanonicalization.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);
        }
        catch (ArgumentException exception)
        {
            return Failed(
                RecordsPurgePlanningFailureCode.InvalidInput,
                exception.Message);
        }

        var normalizedTargets =
            NormalizeTargets(request.Targets, out var normalizeFailure);
        if (normalizeFailure is not null)
            return RecordsPurgePlanningResult.Failed(normalizeFailure);

        var observed = await _observations.ObserveAsync(
                clusterId,
                normalizedTargets!
                    .Select(target => new RecordsPurgeObservationTarget(
                        target.TopicName,
                        target.Partition,
                        target.Selector.Kind == RecordsPurgeSelectorKind.Timestamp
                            ? target.Selector.TimestampUtc!.Value
                                .ToUniversalTime()
                            : null))
                    .ToArray(),
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow()
                        .Add(_policy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess || observed.Value is null)
            return RecordsPurgePlanningResult.Failed(
                MapObservationFailure(observed.Failure));

        var byPartition = IndexObservations(
            observed.Value,
            out var observationFailure);
        if (observationFailure is not null)
            return RecordsPurgePlanningResult.Failed(observationFailure);

        var canonicalTargets =
            new List<RecordsPurgeCanonicalTarget>(
                normalizedTargets!.Count);
        var preconditions =
            new List<MutationPrecondition>(
                normalizedTargets.Count);
        long totalPurgeDistance = 0;

        for (var ordinal = 0; ordinal < normalizedTargets.Count; ordinal++)
        {
            var target = normalizedTargets[ordinal];
            if (!byPartition!.TryGetValue(
                    (target.TopicName, target.Partition),
                    out var observation))
            {
                return Failed(
                    RecordsPurgePlanningFailureCode.TargetNotFound,
                    "A records purge target could not be observed.",
                    ordinal);
            }

            var rangeFailure = ValidateObservation(
                observation,
                ordinal);
            if (rangeFailure is not null)
                return RecordsPurgePlanningResult.Failed(rangeFailure);

            long beforeOffset;
            if (target.Selector.Kind == RecordsPurgeSelectorKind.Absolute)
            {
                beforeOffset = target.Selector.BeforeOffset!.Value;
            }
            else
            {
                if (!observation.TimestampResolvedOffset.HasValue)
                {
                    return Failed(
                        RecordsPurgePlanningFailureCode.TimestampUnresolved,
                        "Kafka could not resolve the requested purge timestamp to an explicit offset.",
                        ordinal);
                }

                beforeOffset =
                    observation.TimestampResolvedOffset.Value;
            }

            if (beforeOffset < observation.LowWatermark ||
                beforeOffset > observation.HighWatermark)
            {
                return Failed(
                    RecordsPurgePlanningFailureCode.OffsetOutOfRange,
                    "Resolved records purge offset is outside the observed partition range.",
                    ordinal);
            }

            long purgeDistance;
            try
            {
                purgeDistance = checked(
                    beforeOffset - observation.LowWatermark);
                totalPurgeDistance = checked(
                    totalPurgeDistance + purgeDistance);
            }
            catch (OverflowException)
            {
                return Failed(
                    RecordsPurgePlanningFailureCode.OffsetOutOfRange,
                    "Records purge distance exceeds the supported range.",
                    ordinal);
            }

            canonicalTargets.Add(
                new RecordsPurgeCanonicalTarget(
                    ordinal,
                    target.TopicName,
                    target.Partition,
                    new RecordsPurgeCanonicalSelector(
                        target.Selector.Kind,
                        target.Selector.Kind ==
                            RecordsPurgeSelectorKind.Absolute
                            ? target.Selector.BeforeOffset
                            : null,
                        target.Selector.Kind ==
                            RecordsPurgeSelectorKind.Timestamp
                            ? target.Selector.TimestampUtc!.Value
                                .ToUniversalTime()
                                .ToUnixTimeMilliseconds()
                            : null),
                    observation.LowWatermark,
                    observation.HighWatermark,
                    beforeOffset,
                    purgeDistance));

            preconditions.Add(
                new MutationPrecondition(
                    $"records.purge/{ordinal:D4}",
                    RecordsPurgeCanonicalization
                        .FingerprintPartition(observation)));
        }

        var canonical = new RecordsPurgeCanonicalIntent(
            clusterId,
            Irreversible: true,
            UndoSupported: false,
            RecordsPurgeCanonicalization.WarningCode,
            totalPurgeDistance,
            Array.AsReadOnly(canonicalTargets.ToArray()));

        var canonicalJson =
            RecordsPurgeCanonicalization.Serialize(canonical);
        if (canonicalJson.Length >
            MutationLimits.MaxCanonicalIntentCharacters)
        {
            return Failed(
                RecordsPurgePlanningFailureCode.LimitExceeded,
                "Records purge preview exceeds the canonical intent ceiling.");
        }

        var resources = canonicalTargets
            .Select(target =>
                RecordsPurgeCanonicalization.ResourceKey(
                    clusterId,
                    target.TopicName,
                    target.Partition))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();

        var authorizationTargets = canonicalTargets
            .Select(target => target.TopicName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(topic => topic, StringComparer.Ordinal)
            .Select(topic => new MutationAuthorizationTarget(
                AuthorizationAction.RecordsPurge,
                clusterId,
                topic))
            .ToArray();

        var intent = new MutationIntentDescriptor(
            MutationOperationKind.RecordsPurge,
            clusterId,
            canonicalJson,
            Array.AsReadOnly(resources),
            Array.AsReadOnly(preconditions.ToArray()),
            AuthorizationTargets:
                Array.AsReadOnly(authorizationTargets));

        var risk = MutationRiskClassifier.Classify(
            new MutationRiskInput(
                MutationOperationKind.RecordsPurge,
                canonicalTargets.Count));

        return RecordsPurgePlanningResult.Success(
            new RecordsPurgePlan(
                canonical,
                intent,
                risk));
    }

    private IReadOnlyList<RecordsPurgeTargetInput>? NormalizeTargets(
        IReadOnlyList<RecordsPurgeTargetInput>? targets,
        out RecordsPurgePlanningFailure? failure)
    {
        failure = null;
        if (targets is null ||
            targets.Count < 1 ||
            targets.Count > _policy.MaxTargets)
        {
            failure = new(
                RecordsPurgePlanningFailureCode.LimitExceeded,
                $"Records purge requires between 1 and {_policy.MaxTargets} targets.");
            return null;
        }

        var normalized =
            new Dictionary<
                (string Topic, int Partition),
                RecordsPurgeTargetInput>();

        foreach (var input in targets)
        {
            if (input is null || input.Selector is null)
            {
                failure = new(
                    RecordsPurgePlanningFailureCode.InvalidInput,
                    "Records purge contains a null target.");
                return null;
            }

            string topic;
            try
            {
                topic = RecordsPurgeCanonicalization.RequireTopicName(
                    input.TopicName);
            }
            catch (ArgumentException exception)
            {
                failure = new(
                    RecordsPurgePlanningFailureCode.InvalidInput,
                    exception.Message);
                return null;
            }

            if (input.Partition < 0)
            {
                failure = new(
                    RecordsPurgePlanningFailureCode.InvalidInput,
                    "Records purge partition must be non-negative.");
                return null;
            }

            var selector = NormalizeSelector(
                input.Selector,
                out failure);
            if (failure is not null)
                return null;

            var item = new RecordsPurgeTargetInput(
                topic,
                input.Partition,
                selector!);
            var key = (topic, input.Partition);

            if (normalized.TryGetValue(key, out var existing))
            {
                if (existing.Selector != item.Selector)
                {
                    failure = new(
                        RecordsPurgePlanningFailureCode.InvalidInput,
                        "A records purge partition cannot have conflicting selectors.");
                    return null;
                }

                continue;
            }

            normalized.Add(key, item);
        }

        return Array.AsReadOnly(
            normalized.Values
                .OrderBy(item => item.TopicName, StringComparer.Ordinal)
                .ThenBy(item => item.Partition)
                .ToArray());
    }

    private static RecordsPurgeSelector? NormalizeSelector(
        RecordsPurgeSelector selector,
        out RecordsPurgePlanningFailure? failure)
    {
        failure = null;
        if (!Enum.IsDefined(selector.Kind))
        {
            failure = new(
                RecordsPurgePlanningFailureCode.InvalidInput,
                "Records purge selector is invalid.");
            return null;
        }

        switch (selector.Kind)
        {
            case RecordsPurgeSelectorKind.Absolute:
                if (selector.BeforeOffset is null or < 0 ||
                    selector.TimestampUtc is not null)
                {
                    failure = new(
                        RecordsPurgePlanningFailureCode.InvalidInput,
                        "Absolute purge selector requires one non-negative beforeOffset.");
                    return null;
                }
                break;

            case RecordsPurgeSelectorKind.Timestamp:
                if (selector.BeforeOffset is not null ||
                    selector.TimestampUtc is null)
                {
                    failure = new(
                        RecordsPurgePlanningFailureCode.InvalidInput,
                        "Timestamp purge selector requires exactly one timestamp.");
                    return null;
                }

                selector = selector with
                {
                    TimestampUtc =
                        selector.TimestampUtc.Value.ToUniversalTime(),
                };
                break;

            default:
                failure = new(
                    RecordsPurgePlanningFailureCode.InvalidInput,
                    "Records purge selector is invalid.");
                return null;
        }

        return selector;
    }

    private static IReadOnlyDictionary<
        (string Topic, int Partition),
        RecordsPurgePartitionObservation>? IndexObservations(
        IReadOnlyList<RecordsPurgePartitionObservation> observations,
        out RecordsPurgePlanningFailure? failure)
    {
        failure = null;
        var result =
            new Dictionary<
                (string Topic, int Partition),
                RecordsPurgePartitionObservation>();

        foreach (var observation in observations)
        {
            if (observation is null ||
                string.IsNullOrWhiteSpace(observation.Topic) ||
                observation.Partition < 0 ||
                !result.TryAdd(
                    (observation.Topic, observation.Partition),
                    observation))
            {
                failure = new(
                    RecordsPurgePlanningFailureCode.ObservationFailed,
                    "Kafka returned malformed or duplicate purge observations.");
                return null;
            }
        }

        return new ReadOnlyDictionary<
            (string Topic, int Partition),
            RecordsPurgePartitionObservation>(result);
    }

    private static RecordsPurgePlanningFailure? ValidateObservation(
        RecordsPurgePartitionObservation observation,
        int ordinal)
    {
        if (observation.Partition < 0 ||
            observation.LowWatermark < 0 ||
            observation.HighWatermark <
                observation.LowWatermark ||
            observation.TimestampResolvedOffset is < 0)
        {
            return new(
                RecordsPurgePlanningFailureCode.ObservationFailed,
                "Kafka returned an invalid purge partition observation.",
                ordinal);
        }

        return null;
    }

    private static RecordsPurgePlanningResult Failed(
        RecordsPurgePlanningFailureCode code,
        string message,
        int? ordinal = null) =>
        RecordsPurgePlanningResult.Failed(
            new RecordsPurgePlanningFailure(
                code,
                message,
                ordinal));

    private static RecordsPurgePlanningFailure MapObservationFailure(
        KafkaFailure? failure) =>
        failure?.Category switch
        {
            KafkaFailureCategory.Unauthorized =>
                new(
                    RecordsPurgePlanningFailureCode.ProviderUnauthorized,
                    "Kafka denied access to observe the records purge target."),
            KafkaFailureCategory.NotSupported =>
                new(
                    RecordsPurgePlanningFailureCode.ProviderUnsupported,
                    "Kafka does not support the required records purge observation."),
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure =>
                new(
                    RecordsPurgePlanningFailureCode.ProviderUnavailable,
                    "Kafka purge target state is currently unavailable."),
            _ =>
                new(
                    RecordsPurgePlanningFailureCode.ObservationFailed,
                    "Kafka purge target state could not be safely observed."),
        };
}
