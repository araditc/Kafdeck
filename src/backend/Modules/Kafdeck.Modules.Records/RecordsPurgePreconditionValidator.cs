using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

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
        _observations = observations ??
            throw new ArgumentNullException(nameof(observations));
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
            canonical =
                RecordsPurgeCanonicalization.Deserialize(
                    operation.CanonicalIntent);
        }
        catch
        {
            return Stale("records_purge_precondition_intent_invalid");
        }

        if (!RecordsPurgeCanonicalValidator.TryBuildProviderTargets(
                canonical,
                out var providerTargets))
        {
            return Stale("records_purge_precondition_intent_invalid");
        }

        if (!string.Equals(
                operation.ClusterId,
                canonical.ClusterId,
                StringComparison.Ordinal) ||
            operation.Risk.RiskClass != MutationRiskClass.Critical ||
            operation.Risk.ConfirmationMode !=
                MutationConfirmationMode.TypedTarget ||
            !operation.Risk.RequiresIndependentApproval)
        {
            return Stale("records_purge_precondition_binding_changed");
        }

        var expectedResources = providerTargets
            .Select(target =>
                RecordsPurgeCanonicalization.ResourceKey(
                    canonical.ClusterId,
                    target.TopicName,
                    target.Partition))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();

        if (!operation.ResourceKeys
                .OrderBy(value => value, StringComparer.Ordinal)
                .SequenceEqual(
                    expectedResources,
                    StringComparer.Ordinal))
        {
            return Stale("records_purge_precondition_binding_changed");
        }

        var expectedAuthorization = providerTargets
            .Select(target => target.TopicName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(topic => topic, StringComparer.Ordinal)
            .Select(topic => new MutationAuthorizationTarget(
                AuthorizationAction.RecordsPurge,
                canonical.ClusterId,
                topic))
            .ToArray();

        if (!operation.AuthorizationTargets
                .OrderBy(target => target.ResourceName, StringComparer.Ordinal)
                .SequenceEqual(
                    expectedAuthorization
                        .OrderBy(
                            target => target.ResourceName,
                            StringComparer.Ordinal)))
        {
            return Stale("records_purge_precondition_binding_changed");
        }

        var preconditions =
            new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var item in operation.Preconditions)
        {
            if (item is null ||
                !preconditions.TryAdd(
                    item.Key,
                    item.Fingerprint))
            {
                return Stale(
                    "records_purge_precondition_shape_changed");
            }
        }

        if (preconditions.Count != canonical.Targets.Count)
        {
            return Stale(
                "records_purge_precondition_shape_changed");
        }

        var observed = await _observations.ObserveAsync(
                canonical.ClusterId,
                providerTargets
                    .Select(target => new RecordsPurgeObservationTarget(
                        target.TopicName,
                        target.Partition))
                    .ToArray(),
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow()
                        .Add(_policy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess || observed.Value is null)
        {
            return new(
                MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                "records_purge_precondition_observation_unavailable");
        }

        var byPartition =
            new Dictionary<
                (string Topic, int Partition),
                RecordsPurgePartitionObservation>();

        foreach (var observation in observed.Value)
        {
            if (observation is null ||
                !byPartition.TryAdd(
                    (observation.Topic, observation.Partition),
                    observation))
            {
                return Stale(
                    "records_purge_precondition_observation_invalid");
            }
        }

        if (byPartition.Count != canonical.Targets.Count)
        {
            return Stale(
                "records_purge_precondition_partition_missing");
        }

        foreach (var target in canonical.Targets
                     .OrderBy(item => item.Ordinal))
        {
            if (!byPartition.TryGetValue(
                    (target.TopicName, target.Partition),
                    out var observation) ||
                observation.LowWatermark < 0 ||
                observation.HighWatermark <
                    observation.LowWatermark ||
                observation.HighWatermark < target.BeforeOffset)
            {
                return Stale(
                    "records_purge_precondition_partition_changed");
            }

            var key = $"records.purge/{target.Ordinal:D4}";
            if (!preconditions.TryGetValue(
                    key,
                    out var expectedFingerprint) ||
                !string.Equals(
                    expectedFingerprint,
                    RecordsPurgeCanonicalization
                        .FingerprintPartition(observation),
                    StringComparison.Ordinal))
            {
                return Stale(
                    "records_purge_precondition_partition_changed");
            }
        }

        return MutationPreDispatchGuardResult.Allowed;
    }

    private static MutationPreDispatchGuardResult Stale(
        string code) =>
        new(
            MutationPreDispatchGuardOutcome.StalePreview,
            code);
}
