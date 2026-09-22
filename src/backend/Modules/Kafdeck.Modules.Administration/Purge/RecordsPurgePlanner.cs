using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Purge;

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
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
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
            return RecordsPurgePlanningResult.Failed(Invalid(exception.Message));
        }

        var targets = NormalizeTargets(request.Targets, out var normalizationFailure);
        if (normalizationFailure is not null)
            return RecordsPurgePlanningResult.Failed(normalizationFailure);

        var observationTargets = targets!
            .Select(target => new RecordsPurgeObservationTarget(
                target.TopicName,
                target.Partition,
                target.Selector.Kind == RecordsPurgeSelectorKind.Timestamp
                    ? target.Selector.TimestampUtc!.Value.ToUniversalTime()
                    : null))
            .ToArray();

        KafkaResult<RecordsPurgeObservation> observation;
        try
        {
            observation = await _observations.ObserveAsync(
                    clusterId,
                    observationTargets,
                    new KafkaOperationContext(
                        _timeProvider.GetUtcNow().Add(_policy.ObservationTimeout)),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return RecordsPurgePlanningResult.Failed(new(
                RecordsPurgePlanningFailureCode.ProviderUnavailable,
                "Kafka purge planning observation was cancelled or timed out."));
        }
        catch
        {
            return RecordsPurgePlanningResult.Failed(new(
                RecordsPurgePlanningFailureCode.ObservationFailed,
                "Kafka purge planning observation failed."));
        }

        if (!observation.IsSuccess || observation.Value is null)
            return RecordsPurgePlanningResult.Failed(MapObservationFailure(observation.Failure));

        var indexed = IndexObservation(
            targets,
            observation.Value,
            out var observationFailure);
        if (observationFailure is not null)
            return RecordsPurgePlanningResult.Failed(observationFailure);

        var canonicalTargets = new List<RecordsPurgeCanonicalTarget>(targets.Count);
        var preconditions = new List<MutationPrecondition>(targets.Count);

        for (var ordinal = 0; ordinal < targets.Count; ordinal++)
        {
            var target = targets[ordinal];
            var observed = indexed![(target.TopicName, target.Partition)];

            if (observed.LowWatermark < 0 ||
                observed.HighWatermark < observed.LowWatermark)
            {
                return RecordsPurgePlanningResult.Failed(new(
                    RecordsPurgePlanningFailureCode.ObservationFailed,
                    "Kafka returned an invalid purge target watermark range.",
                    ordinal));
            }

            var resolved = ResolveBeforeOffset(target, observed, ordinal);
            if (resolved.Failure is not null)
                return RecordsPurgePlanningResult.Failed(resolved.Failure);

            canonicalTargets.Add(new RecordsPurgeCanonicalTarget(
                ordinal,
                target.TopicName,
                target.Partition,
                new RecordsPurgeCanonicalSelector(
                    target.Selector.Kind,
                    target.Selector.BeforeOffset,
                    target.Selector.TimestampUtc?.ToUnixTimeMilliseconds()),
                observed.LowWatermark,
                observed.HighWatermark,
                resolved.BeforeOffset));

            preconditions.Add(new MutationPrecondition(
                $"records.purge.partition/{ordinal:D4}",
                RecordsPurgeCanonicalization.PartitionPreconditionFingerprint(observed)));
        }

        var canonical = new RecordsPurgeCanonicalIntent(
            clusterId,
            Irreversible: true,
            Array.AsReadOnly(canonicalTargets.ToArray()));

        var resourceKeys = canonicalTargets
            .Select(target => RecordsPurgeCanonicalization.PartitionResourceKey(
                clusterId,
                target.TopicName,
                target.Partition))
            .ToArray();

        var authorizationTargets = canonicalTargets
            .Select(target => target.TopicName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(topic => topic, StringComparer.Ordinal)
            .Select(topic => new MutationAuthorizationTarget(
                AuthorizationAction.RecordsPurge,
                clusterId,
                RecordsPurgeCanonicalization.TopicAuthorizationResource(topic)))
            .ToArray();

        var intent = new MutationIntentDescriptor(
            MutationOperationKind.RecordsPurge,
            clusterId,
            RecordsPurgeCanonicalization.Serialize(canonical),
            Array.AsReadOnly(resourceKeys),
            Array.AsReadOnly(preconditions.ToArray()),
            MaterialDigests: null,
            AuthorizationTargets: Array.AsReadOnly(authorizationTargets),
            RiskContext: null);

        var risk = MutationRiskClassifier.Classify(new MutationRiskInput(
            MutationOperationKind.RecordsPurge,
            canonicalTargets.Count));

        return RecordsPurgePlanningResult.Success(new RecordsPurgePlan(
            canonical,
            intent,
            risk));
    }

    private IReadOnlyList<RecordsPurgeTargetInput>? NormalizeTargets(
        IReadOnlyList<RecordsPurgeTargetInput>? targets,
        out RecordsPurgePlanningFailure? failure)
    {
        failure = null;
        if (targets is null || targets.Count < 1 || targets.Count > _policy.MaxTargets)
        {
            failure = new(
                RecordsPurgePlanningFailureCode.LimitExceeded,
                $"Records purge requires between 1 and {_policy.MaxTargets} explicit partition targets.");
            return null;
        }

        var byPartition = new Dictionary<
            (string Topic, int Partition),
            RecordsPurgeTargetInput>();

        foreach (var input in targets)
        {
            if (input is null || input.Selector is null)
            {
                failure = Invalid("Records purge contains a null target.");
                return null;
            }

            string topic;
            try
            {
                topic = RecordsPurgeCanonicalization.RequireTopicName(input.TopicName);
            }
            catch (ArgumentException exception)
            {
                failure = Invalid(exception.Message);
                return null;
            }

            if (input.Partition < 0)
            {
                failure = Invalid("Records purge partition must be non-negative.");
                return null;
            }

            var selector = NormalizeSelector(input.Selector, out failure);
            if (failure is not null)
                return null;

            var normalized = new RecordsPurgeTargetInput(
                topic,
                input.Partition,
                selector!);
            var key = (topic, input.Partition);
            if (byPartition.TryGetValue(key, out var existing))
            {
                if (existing.Selector != normalized.Selector)
                {
                    failure = Invalid(
                        "A purge partition cannot have conflicting selectors.");
                    return null;
                }

                continue;
            }

            byPartition.Add(key, normalized);
        }

        return Array.AsReadOnly(byPartition.Values
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
            failure = Invalid("Records purge selector kind is invalid.");
            return null;
        }

        if (selector.Kind == RecordsPurgeSelectorKind.Absolute)
        {
            if (selector.BeforeOffset is null or < 0 || selector.TimestampUtc is not null)
            {
                failure = Invalid(
                    "Absolute records purge requires one non-negative beforeOffset and no timestamp.");
                return null;
            }

            return new RecordsPurgeSelector(
                RecordsPurgeSelectorKind.Absolute,
                selector.BeforeOffset,
                null);
        }

        if (selector.BeforeOffset is not null || selector.TimestampUtc is null)
        {
            failure = Invalid(
                "Timestamp records purge requires one timestamp and no absolute beforeOffset.");
            return null;
        }

        return new RecordsPurgeSelector(
            RecordsPurgeSelectorKind.Timestamp,
            null,
            selector.TimestampUtc.Value.ToUniversalTime());
    }

    private static Dictionary<(string Topic, int Partition), RecordsPurgePartitionObservation>?
        IndexObservation(
            IReadOnlyList<RecordsPurgeTargetInput> targets,
            RecordsPurgeObservation observation,
            out RecordsPurgePlanningFailure? failure)
    {
        failure = null;
        if (observation.Partitions is null)
        {
            failure = new(
                RecordsPurgePlanningFailureCode.ObservationFailed,
                "Kafka returned no purge target observations.");
            return null;
        }

        var expected = targets
            .Select(target => (target.TopicName, target.Partition))
            .ToHashSet();
        var result = new Dictionary<
            (string Topic, int Partition),
            RecordsPurgePartitionObservation>();

        foreach (var item in observation.Partitions)
        {
            if (item is null ||
                !expected.Contains((item.TopicName, item.Partition)) ||
                !result.TryAdd((item.TopicName, item.Partition), item))
            {
                failure = new(
                    RecordsPurgePlanningFailureCode.ObservationFailed,
                    "Kafka returned an unexpected or duplicate purge target observation.");
                return null;
            }
        }

        if (result.Count != expected.Count)
        {
            failure = new(
                RecordsPurgePlanningFailureCode.TargetNotFound,
                "One or more purge targets could not be observed.");
            return null;
        }

        return result;
    }

    private static (long BeforeOffset, RecordsPurgePlanningFailure? Failure)
        ResolveBeforeOffset(
            RecordsPurgeTargetInput target,
            RecordsPurgePartitionObservation observed,
            int ordinal)
    {
        long resolved;
        if (target.Selector.Kind == RecordsPurgeSelectorKind.Absolute)
        {
            resolved = target.Selector.BeforeOffset!.Value;
        }
        else
        {
            if (observed.TimestampResolvedOffset is null or < 0)
            {
                return (0, new RecordsPurgePlanningFailure(
                    RecordsPurgePlanningFailureCode.TimestampUnresolved,
                    "The purge timestamp could not be resolved to a concrete Kafka offset.",
                    ordinal));
            }

            resolved = observed.TimestampResolvedOffset.Value;
        }

        if (resolved < observed.LowWatermark || resolved > observed.HighWatermark)
        {
            return (0, new RecordsPurgePlanningFailure(
                RecordsPurgePlanningFailureCode.OffsetOutOfRange,
                "The requested purge offset is outside the observed partition watermark range; no clamping is permitted.",
                ordinal));
        }

        return (resolved, null);
    }

    private static RecordsPurgePlanningFailure MapObservationFailure(
        KafkaFailure? failure)
    {
        if (failure is null)
        {
            return new(
                RecordsPurgePlanningFailureCode.ObservationFailed,
                "Kafka purge planning observation failed.");
        }

        return failure.Category switch
        {
            KafkaFailureCategory.Unauthorized or
            KafkaFailureCategory.AuthenticationFailed => new(
                RecordsPurgePlanningFailureCode.ProviderUnauthorized,
                "Kafka did not authorize purge planning observation."),
            KafkaFailureCategory.NotSupported => new(
                RecordsPurgePlanningFailureCode.ProviderUnsupported,
                "Kafka does not support the required purge planning observation."),
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.Cancelled => new(
                RecordsPurgePlanningFailureCode.ProviderUnavailable,
                "Kafka purge planning observation is temporarily unavailable."),
            _ => new(
                RecordsPurgePlanningFailureCode.ObservationFailed,
                "Kafka purge planning observation failed."),
        };
    }

    private static RecordsPurgePlanningFailure Invalid(string message) =>
        new(RecordsPurgePlanningFailureCode.InvalidInput, message);
}
