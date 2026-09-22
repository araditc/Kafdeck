using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Purge;

public sealed class RecordsPurgePreconditionValidator
{
    private readonly IRecordsPurgeObservationPort _observations;
    private readonly RecordsPurgePolicy _policy;
    private readonly TimeProvider _timeProvider;

    public RecordsPurgePreconditionValidator(
        IRecordsPurgeObservationPort observations,
        RecordsPurgePolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _policy = policy ?? RecordsPurgePolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (operation.OperationKind != MutationOperationKind.RecordsPurge)
        {
            return new(
                MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                "records_purge_precondition_operation_not_supported");
        }

        RecordsPurgeCanonicalIntent canonical;
        try
        {
            canonical = RecordsPurgeCanonicalization
                .Deserialize<RecordsPurgeCanonicalIntent>(operation.CanonicalIntent);
        }
        catch
        {
            return Stale("records_purge_intent_invalid");
        }

        if (!TryValidateCanonical(canonical, out var orderedTargets))
            return Stale("records_purge_intent_invalid");

        if (!BindingsMatch(operation, canonical, orderedTargets))
            return Stale("records_purge_binding_changed");

        if (!TryGetUniquePreconditions(operation.Preconditions, out var preconditions) ||
            preconditions!.Count != orderedTargets.Length)
        {
            return Stale("records_purge_precondition_shape_changed");
        }

        var observationTargets = orderedTargets
            .Select(target => new RecordsPurgeObservationTarget(
                target.TopicName,
                target.Partition))
            .ToArray();

        Kafdeck.Core.Kafka.KafkaResult<RecordsPurgeObservation> observed;
        try
        {
            observed = await _observations.ObserveAsync(
                    canonical.ClusterId,
                    observationTargets,
                    new Kafdeck.Core.Kafka.KafkaOperationContext(
                        _timeProvider.GetUtcNow().Add(_policy.ObservationTimeout)),
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unsupported("records_purge_precondition_observation_unavailable");
        }
        catch
        {
            return Unsupported("records_purge_precondition_observation_unavailable");
        }

        if (!observed.IsSuccess || observed.Value is null)
            return Unsupported("records_purge_precondition_observation_unavailable");

        var expected = orderedTargets
            .Select(target => (target.TopicName, target.Partition))
            .ToHashSet();
        var actual = new Dictionary<
            (string Topic, int Partition),
            RecordsPurgePartitionObservation>();

        foreach (var partition in observed.Value.Partitions)
        {
            if (partition is null ||
                !expected.Contains((partition.TopicName, partition.Partition)) ||
                !actual.TryAdd((partition.TopicName, partition.Partition), partition) ||
                partition.LowWatermark < 0 ||
                partition.HighWatermark < partition.LowWatermark)
            {
                return Stale("records_purge_precondition_observation_invalid");
            }
        }

        if (actual.Count != expected.Count)
            return Stale("records_purge_precondition_partition_missing");

        foreach (var target in orderedTargets)
        {
            var partition = actual[(target.TopicName, target.Partition)];
            var key = $"records.purge.partition/{target.Ordinal:D4}";
            if (!preconditions.TryGetValue(key, out var expectedFingerprint) ||
                !string.Equals(
                    expectedFingerprint,
                    RecordsPurgeCanonicalization.PartitionPreconditionFingerprint(partition),
                    StringComparison.Ordinal))
            {
                return Stale("records_purge_precondition_watermark_changed");
            }

            if (target.ResolvedBeforeOffset < partition.LowWatermark ||
                target.ResolvedBeforeOffset > partition.HighWatermark)
            {
                return Stale("records_purge_precondition_target_out_of_range");
            }
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private static bool TryValidateCanonical(
        RecordsPurgeCanonicalIntent canonical,
        out RecordsPurgeCanonicalTarget[] orderedTargets)
    {
        orderedTargets = Array.Empty<RecordsPurgeCanonicalTarget>();
        if (!canonical.Irreversible ||
            canonical.Targets is null ||
            canonical.Targets.Count is < 1 or > RecordsPurgePolicy.HardMaxTargets)
        {
            return false;
        }

        try
        {
            _ = RecordsPurgeCanonicalization.RequireIdentifier(
                canonical.ClusterId,
                "Cluster ID",
                256);

            orderedTargets = canonical.Targets
                .OrderBy(target => target.Ordinal)
                .ToArray();
            var seen = new HashSet<(string Topic, int Partition)>();

            for (var ordinal = 0; ordinal < orderedTargets.Length; ordinal++)
            {
                var target = orderedTargets[ordinal];
                if (target is null || target.Selector is null ||
                    target.Ordinal != ordinal ||
                    target.Partition < 0 ||
                    target.LowWatermark < 0 ||
                    target.HighWatermark < target.LowWatermark ||
                    target.ResolvedBeforeOffset < target.LowWatermark ||
                    target.ResolvedBeforeOffset > target.HighWatermark ||
                    !Enum.IsDefined(target.Selector.Kind))
                {
                    return false;
                }

                var topic = RecordsPurgeCanonicalization.RequireTopicName(target.TopicName);
                if (!string.Equals(topic, target.TopicName, StringComparison.Ordinal) ||
                    !seen.Add((topic, target.Partition)))
                {
                    return false;
                }

                if (target.Selector.Kind == RecordsPurgeSelectorKind.Absolute)
                {
                    if (target.Selector.RequestedBeforeOffset is null or < 0 ||
                        target.Selector.TimestampUnixMilliseconds is not null ||
                        target.ResolvedBeforeOffset != target.Selector.RequestedBeforeOffset.Value)
                    {
                        return false;
                    }
                }
                else
                {
                    if (target.Selector.RequestedBeforeOffset is not null ||
                        !target.Selector.TimestampUnixMilliseconds.HasValue)
                    {
                        return false;
                    }

                    _ = DateTimeOffset.FromUnixTimeMilliseconds(
                        target.Selector.TimestampUnixMilliseconds.Value);
                }
            }

            return true;
        }
        catch (Exception exception) when (
            exception is ArgumentException or ArgumentOutOfRangeException)
        {
            return false;
        }
    }

    private static bool BindingsMatch(
        MutationOperationSnapshot operation,
        RecordsPurgeCanonicalIntent canonical,
        IReadOnlyList<RecordsPurgeCanonicalTarget> targets)
    {
        if (!string.Equals(
                operation.ClusterId,
                canonical.ClusterId,
                StringComparison.Ordinal))
        {
            return false;
        }

        var expectedResources = targets
            .Select(target => RecordsPurgeCanonicalization.PartitionResourceKey(
                canonical.ClusterId,
                target.TopicName,
                target.Partition))
            .ToHashSet(StringComparer.Ordinal);
        if (operation.ResourceKeys.Count != expectedResources.Count ||
            !operation.ResourceKeys.ToHashSet(StringComparer.Ordinal)
                .SetEquals(expectedResources))
        {
            return false;
        }

        var expectedAuthorization = targets
            .Select(target => target.TopicName)
            .Distinct(StringComparer.Ordinal)
            .Select(topic => (
                AuthorizationAction.RecordsPurge,
                canonical.ClusterId,
                RecordsPurgeCanonicalization.TopicAuthorizationResource(topic)))
            .ToHashSet();

        var actualAuthorization = operation.AuthorizationTargets
            .Select(target => (target.Action, target.ClusterId, target.ResourceName))
            .ToHashSet();

        return actualAuthorization.Count == expectedAuthorization.Count &&
               actualAuthorization.SetEquals(expectedAuthorization);
    }

    private static bool TryGetUniquePreconditions(
        IReadOnlyList<MutationPrecondition> values,
        out Dictionary<string, string>? preconditions)
    {
        preconditions = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var value in values)
        {
            if (value is null ||
                string.IsNullOrWhiteSpace(value.Key) ||
                string.IsNullOrWhiteSpace(value.Fingerprint) ||
                !preconditions.TryAdd(value.Key, value.Fingerprint))
            {
                preconditions = null;
                return false;
            }
        }

        return true;
    }

    private static MutationPreDispatchGuardResult Stale(string code) =>
        new(MutationPreDispatchGuardOutcome.StalePreview, code);

    private static MutationPreDispatchGuardResult Unsupported(string code) =>
        new(MutationPreDispatchGuardOutcome.CapabilityUnsupported, code);
}
