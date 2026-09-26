using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed record ClusterTransferPlannerPolicy(TimeSpan ObservationTimeout)
{
    public static ClusterTransferPlannerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10));
}

public sealed class ClusterTransferPlanner
{
    private readonly IKafkaAdministrationPort _kafka;
    private readonly IRecordMaskingPolicyProvider _masking;
    private readonly ClusterTransferPlannerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public ClusterTransferPlanner(
        IKafkaAdministrationPort kafka,
        IRecordMaskingPolicyProvider masking,
        ClusterTransferPlannerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _masking = masking ?? throw new ArgumentNullException(nameof(masking));
        _policy = policy ?? ClusterTransferPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
        if (_policy.ObservationTimeout < TimeSpan.FromSeconds(1) ||
            _policy.ObservationTimeout > TimeSpan.FromSeconds(30))
            throw new ArgumentOutOfRangeException(nameof(policy));
    }

    public async Task<ClusterTransferPlanResult> PlanAsync(
        ClusterTransferPlanningRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string sourceCluster;
        string destinationCluster;
        string sourceVersion;
        string destinationVersion;
        ClusterTransferMappingRequest[] mappings;
        try
        {
            sourceCluster = ClusterTransferPolicy.RequireIdentifier(
                request.SourceClusterId,
                "Source cluster",
                256);
            destinationCluster = ClusterTransferPolicy.RequireIdentifier(
                request.DestinationClusterId,
                "Destination cluster",
                256);
            sourceVersion = ClusterTransferPolicy.RequireIdentifier(
                request.SourceProfileVersion,
                "Source profile version",
                256);
            destinationVersion = ClusterTransferPolicy.RequireIdentifier(
                request.DestinationProfileVersion,
                "Destination profile version",
                256);
            if (string.Equals(sourceCluster, destinationCluster, StringComparison.Ordinal))
                return ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.SamePhysicalCluster,
                    "Transfer source and destination cluster profiles must be distinct.");
            if (request.Mappings is null ||
                request.Mappings.Count is < 1 or > ClusterTransferPolicy.MaxMappings)
                return ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.InvalidInput,
                    "Transfer mapping count is outside the admitted finite bound.");

            mappings = request.Mappings.Select(ClusterTransferPolicy.Normalize).ToArray();
            if (mappings
                .GroupBy(item => (item.SourceTopic, item.SourcePartition))
                .Any(group => group.Count() != 1))
                return ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.InvalidInput,
                    "Each source topic/partition may appear only once in one finite transfer plan.");
        }
        catch (ArgumentException)
        {
            return ClusterTransferPlanResult.Failed(
                ClusterTransferPlanningFailureCode.InvalidInput,
                "Cluster transfer planning input is invalid.");
        }

        ClusterTransferDataPolicy dataPolicy;
        try
        {
            dataPolicy = ClusterTransferPolicy.RequireBytePreservingPolicy(
                _masking.Current);
        }
        catch (MutationStateException)
        {
            return ClusterTransferPlanResult.Failed(
                ClusterTransferPlanningFailureCode.MaskingPolicyUnsupported,
                "Current masking policy is incompatible with byte-preserving transfer.");
        }

        var observation = new KafkaOperationContext(
            _timeProvider.GetUtcNow().Add(_policy.ObservationTimeout));
        var sourceMetadata = await _kafka.GetClusterMetadataAsync(
                sourceCluster,
                observation,
                cancellationToken)
            .ConfigureAwait(false);
        if (!sourceMetadata.IsSuccess || sourceMetadata.Value is null)
            return FromObservation(sourceMetadata.Failure, "source cluster");

        var destinationMetadata = await _kafka.GetClusterMetadataAsync(
                destinationCluster,
                observation,
                cancellationToken)
            .ConfigureAwait(false);
        if (!destinationMetadata.IsSuccess || destinationMetadata.Value is null)
            return FromObservation(destinationMetadata.Failure, "destination cluster");

        var sourcePhysical = sourceMetadata.Value.KafkaClusterId;
        var destinationPhysical = destinationMetadata.Value.KafkaClusterId;
        if (string.IsNullOrWhiteSpace(sourcePhysical) ||
            string.IsNullOrWhiteSpace(destinationPhysical))
            return ClusterTransferPlanResult.Failed(
                ClusterTransferPlanningFailureCode.IdentityUnavailable,
                "Kafka physical cluster identity is required for governed transfer.");
        if (string.Equals(sourcePhysical, destinationPhysical, StringComparison.Ordinal))
            return ClusterTransferPlanResult.Failed(
                ClusterTransferPlanningFailureCode.SamePhysicalCluster,
                "Source and destination resolve to the same physical Kafka cluster.");

        var sourceTopics = new Dictionary<string, TopicMetadata>(StringComparer.Ordinal);
        var destinationTopics = new Dictionary<string, TopicMetadata>(StringComparer.Ordinal);
        foreach (var topicName in mappings.Select(item => item.SourceTopic).Distinct(StringComparer.Ordinal))
        {
            var result = await _kafka.GetTopicMetadataAsync(
                    sourceCluster,
                    topicName,
                    observation,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!result.IsSuccess || result.Value is null)
                return FromObservation(result.Failure, "source topic");
            if (result.Value.IsInternal)
                return ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.InternalTopicUnsupported,
                    "Internal Kafka topics are not admitted as finite transfer source topics.");
            sourceTopics.Add(topicName, result.Value);
        }

        foreach (var topicName in mappings.Select(item => item.DestinationTopic).Distinct(StringComparer.Ordinal))
        {
            var result = await _kafka.GetTopicMetadataAsync(
                    destinationCluster,
                    topicName,
                    observation,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!result.IsSuccess || result.Value is null)
                return FromObservation(result.Failure, "destination topic");
            if (result.Value.IsInternal)
                return ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.InternalTopicUnsupported,
                    "Internal Kafka topics are not admitted as finite transfer destination topics.");
            destinationTopics.Add(topicName, result.Value);
        }

        var frozen = new List<ClusterTransferMapping>(mappings.Length);
        long requestedRecords = 0;
        foreach (var mapping in mappings)
        {
            var sourceTopic = sourceTopics[mapping.SourceTopic];
            var destinationTopic = destinationTopics[mapping.DestinationTopic];
            if (!sourceTopic.Partitions.Any(item => item.PartitionId == mapping.SourcePartition) ||
                !destinationTopic.Partitions.Any(item => item.PartitionId == mapping.DestinationPartition))
                return ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.PartitionUnavailable,
                    "One or more exact transfer partitions do not exist.");

            requestedRecords = checked(
                requestedRecords +
                (mapping.EndExclusive - mapping.StartInclusive));
            if (requestedRecords > request.Budget.MaxTotalRecords)
                return ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.InvalidInput,
                    "Frozen transfer ranges exceed the approved total record ceiling.");

            frozen.Add(new ClusterTransferMapping(
                mapping.SourceTopic,
                mapping.SourcePartition,
                mapping.DestinationTopic,
                mapping.DestinationPartition,
                mapping.StartInclusive,
                mapping.EndExclusive,
                ClusterTransferPolicy.FingerprintTopic(sourceTopic),
                ClusterTransferPolicy.FingerprintTopic(destinationTopic)));
        }

        var source = new ClusterTransferEndpoint(
            sourceCluster,
            sourceVersion,
            sourcePhysical);
        var destination = new ClusterTransferEndpoint(
            destinationCluster,
            destinationVersion,
            destinationPhysical);
        var immutableMappings = Array.AsReadOnly(frozen.ToArray());
        var fingerprint = ClusterTransferPolicy.PlanFingerprint(
            source,
            destination,
            immutableMappings,
            request.Budget,
            dataPolicy);
        var plan = new ClusterTransferPlan(
            source,
            destination,
            immutableMappings,
            request.Budget,
            dataPolicy,
            fingerprint);

        try
        {
            var intent = ClusterTransferPolicy.BuildIntent(plan);
            return ClusterTransferPlanResult.Success(
                plan,
                intent,
                ClusterTransferPolicy.ClassifyRisk(plan));
        }
        catch (Exception exception) when (
            exception is ArgumentException or MutationStateException)
        {
            return ClusterTransferPlanResult.Failed(
                ClusterTransferPlanningFailureCode.InvalidInput,
                "Transfer plan could not be bound to the admitted authorization/conflict contract.");
        }
    }

    private static ClusterTransferPlanResult FromObservation(
        KafkaFailure? failure,
        string target) =>
        failure?.Category switch
        {
            KafkaFailureCategory.Unauthorized =>
                ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.ProviderUnauthorized,
                    $"Kafka denied {target} observation."),
            KafkaFailureCategory.NotSupported =>
                ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.ProviderUnsupported,
                    $"Kafka does not support required {target} observation."),
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure =>
                ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.ProviderUnavailable,
                    $"Kafka {target} observation is currently unavailable."),
            _ =>
                ClusterTransferPlanResult.Failed(
                    ClusterTransferPlanningFailureCode.ObservationFailed,
                    $"Kafka {target} observation failed."),
        };
}
