using System.Collections.ObjectModel;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed record RecordProductionPlannerPolicy(
    TimeSpan ObservationTimeout,
    string PolicyVersion)
{
    public static RecordProductionPlannerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10), "v0.5-record-production-p1");
}

public sealed class RecordProductionPlanner
{
    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    private readonly IKafkaAdministrationPort _kafka;
    private readonly IMutationMaterialDigestService _digest;
    private readonly IRecordProductionSchemaValidator? _schemaValidator;
    private readonly RecordProductionPolicy _policy;
    private readonly RecordProductionPlannerPolicy _plannerPolicy;
    private readonly TimeProvider _timeProvider;

    public RecordProductionPlanner(
        IKafkaAdministrationPort kafka,
        IMutationMaterialDigestService digest,
        IRecordProductionSchemaValidator? schemaValidator = null,
        RecordProductionPolicy? policy = null,
        RecordProductionPlannerPolicy? plannerPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _digest = digest ?? throw new ArgumentNullException(nameof(digest));
        _schemaValidator = schemaValidator;
        _policy = policy ?? new RecordProductionPolicy();
        _plannerPolicy = plannerPolicy ?? RecordProductionPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (_plannerPolicy.ObservationTimeout is < TimeSpan.FromSeconds(1) or > TimeSpan.FromSeconds(30))
            throw new ArgumentOutOfRangeException(nameof(plannerPolicy));
        RecordProductionValidation.RequireIdentifier(
            _plannerPolicy.PolicyVersion,
            "Record production policy version",
            256);
    }

    public async Task<RecordProductionPlanningResult> PlanAsync(
        RecordProductionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        string clusterId;
        string topicName;
        try
        {
            clusterId = RecordProductionValidation.RequireIdentifier(
                request.ClusterId,
                "Cluster ID",
                256);
            topicName = RecordProductionValidation.RequireIdentifier(
                request.TopicName,
                "Topic name",
                249);
        }
        catch (ArgumentException exception)
        {
            return Failed(RecordProductionPlanningFailureCode.InvalidInput, exception.Message);
        }

        if (request.Records is null ||
            request.Records.Count < 1 ||
            request.Records.Count > _policy.MaxRecords)
        {
            return Failed(
                RecordProductionPlanningFailureCode.LimitExceeded,
                $"Record production requires between 1 and {_policy.MaxRecords} records.");
        }

        var observation = new KafkaOperationContext(
            _timeProvider.GetUtcNow().Add(_plannerPolicy.ObservationTimeout));
        var topic = await _kafka.GetTopicMetadataAsync(
                clusterId,
                topicName,
                observation,
                cancellationToken)
            .ConfigureAwait(false);

        if (!topic.IsSuccess || topic.Value is null)
            return Failed(MapObservationFailure(topic.Failure));

        if (topic.Value.IsInternal)
            return Failed(
                RecordProductionPlanningFailureCode.InternalTopicUnsupported,
                "Internal Kafka topics are not valid record-production targets.");

        RecordProductionCanonicalSchema? canonicalSchema = null;
        if (request.SchemaValidation is not null)
        {
            if (_schemaValidator is null)
                return Failed(
                    RecordProductionPlanningFailureCode.SchemaValidationUnavailable,
                    "Schema validation was requested but no admitted validator is configured.");

            try
            {
                canonicalSchema = new RecordProductionCanonicalSchema(
                    RecordProductionValidation.RequireIdentifier(
                        request.SchemaValidation.ValidatorId,
                        "Schema validator ID",
                        128),
                    RecordProductionValidation.RequireIdentifier(
                        request.SchemaValidation.SchemaIdentity,
                        "Schema identity",
                        512));
            }
            catch (ArgumentException exception)
            {
                return Failed(
                    RecordProductionPlanningFailureCode.InvalidInput,
                    exception.Message);
            }
        }

        RecordProductionCanonicalTemplate? canonicalTemplate = null;
        if (request.Template is not null)
        {
            try
            {
                if (request.Template.Version <= 0)
                    throw new ArgumentOutOfRangeException(nameof(request.Template.Version));

                canonicalTemplate = new RecordProductionCanonicalTemplate(
                    RecordProductionValidation.RequireIdentifier(
                        request.Template.TemplateId,
                        "Template ID",
                        256),
                    request.Template.Version);
            }
            catch (ArgumentException exception)
            {
                return Failed(
                    RecordProductionPlanningFailureCode.TemplateInvalid,
                    exception.Message);
            }
        }

        var material = new Dictionary<string, byte[]>(request.Records.Count, StringComparer.Ordinal);
        var canonicalRecords = new List<RecordProductionCanonicalRecord>(request.Records.Count);
        var digests = new List<MutationMaterialDigest>(request.Records.Count);
        long totalBytes = 0;

        try
        {
            for (var ordinal = 0; ordinal < request.Records.Count; ordinal++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var input = request.Records[ordinal]
                    ?? throw new ArgumentException("Record production contains a null record.");

                if (input.Key is { } key && key.Length > _policy.MaxKeyBytes)
                    return FailAndDispose(material, RecordProductionPlanningFailureCode.LimitExceeded,
                        "Record key exceeds the configured byte ceiling.", ordinal);

                if (input.Value.Length > _policy.MaxValueBytes)
                    return FailAndDispose(material, RecordProductionPlanningFailureCode.LimitExceeded,
                        "Record value exceeds the configured byte ceiling.", ordinal);

                if (input.Headers is null || input.Headers.Count > _policy.MaxHeadersPerRecord)
                    return FailAndDispose(material, RecordProductionPlanningFailureCode.LimitExceeded,
                        "Record headers exceed the configured count ceiling.", ordinal);

                var headers = new List<KeyValuePair<string, ReadOnlyMemory<byte>>>(input.Headers.Count);
                var canonicalHeaders = new List<RecordProductionCanonicalHeader>(input.Headers.Count);
                foreach (var pair in input.Headers.OrderBy(pair => pair.Key, StringComparer.Ordinal))
                {
                    string name;
                    try
                    {
                        name = RecordProductionValidation.RequireIdentifier(
                            pair.Key,
                            "Kafka header name",
                            RecordProductionPolicy.HardMaxHeaderNameCharacters);
                    }
                    catch (ArgumentException exception)
                    {
                        return FailAndDispose(
                            material,
                            RecordProductionPlanningFailureCode.InvalidInput,
                            exception.Message,
                            ordinal);
                    }

                    headers.Add(new KeyValuePair<string, ReadOnlyMemory<byte>>(name, pair.Value));
                    canonicalHeaders.Add(new RecordProductionCanonicalHeader(name, pair.Value.Length));
                    totalBytes = checked(totalBytes + Encoding.UTF8.GetByteCount(name) + pair.Value.Length);
                }

                totalBytes = checked(
                    totalBytes +
                    (input.Key?.Length ?? 0) +
                    input.Value.Length);

                if (totalBytes > _policy.MaxTotalBytes)
                    return FailAndDispose(material, RecordProductionPlanningFailureCode.LimitExceeded,
                        "Record production exceeds the configured total byte ceiling.", ordinal);

                string? validationCode = null;
                string? schemaFingerprint = null;
                if (request.SchemaValidation is not null)
                {
                    var validation = await _schemaValidator!.ValidateAsync(
                            new RecordProductionSchemaValidationContext(
                                clusterId,
                                topicName,
                                ordinal,
                                input.Value,
                                request.SchemaValidation),
                            cancellationToken)
                        .ConfigureAwait(false);

                    try
                    {
                        validationCode = RecordProductionValidation.RequireSafeCode(
                            validation.Code,
                            "Schema validation code");
                        schemaFingerprint = RecordProductionValidation.RequireSha256OrNull(
                            validation.SchemaFingerprint,
                            "Schema fingerprint");
                    }
                    catch (ArgumentException)
                    {
                        return FailAndDispose(
                            material,
                            RecordProductionPlanningFailureCode.SchemaValidationUnavailable,
                            "Schema validator returned unsafe or malformed evidence.",
                            ordinal);
                    }

                    if (validation.State == RecordProductionSchemaValidationState.Unavailable)
                        return FailAndDispose(
                            material,
                            RecordProductionPlanningFailureCode.SchemaValidationUnavailable,
                            "Schema validation is currently unavailable.",
                            ordinal);
                    if (validation.State != RecordProductionSchemaValidationState.Valid)
                        return FailAndDispose(
                            material,
                            RecordProductionPlanningFailureCode.SchemaValidationFailed,
                            "Record payload did not satisfy the requested schema validation.",
                            ordinal);
                }

                var materialName = $"record/{ordinal:D4}";
                var envelope = RecordProductionMaterialCodec.Encode(
                    input.Key,
                    input.Value,
                    headers);
                material.Add(materialName, envelope);
                digests.Add(new MutationMaterialDigest(
                    materialName,
                    _digest.ComputeDigest(envelope)));

                canonicalRecords.Add(
                    new RecordProductionCanonicalRecord(
                        ordinal,
                        materialName,
                        input.Key?.Length,
                        input.Value.Length,
                        Array.AsReadOnly(canonicalHeaders.ToArray()),
                        validationCode,
                        schemaFingerprint));
            }

            var canonical = new RecordProductionCanonicalIntent(
                clusterId,
                topicName,
                Array.AsReadOnly(canonicalRecords.ToArray()),
                totalBytes,
                canonicalSchema,
                canonicalTemplate);

            var canonicalJson = JsonSerializer.Serialize(canonical, CanonicalJson);
            if (canonicalJson.Length > MutationLimits.MaxCanonicalIntentCharacters)
                return FailAndDispose(
                    material,
                    RecordProductionPlanningFailureCode.LimitExceeded,
                    "Record production safe preview exceeds the canonical intent ceiling.");

            var intent = new MutationIntentDescriptor(
                MutationOperationKind.RecordProduce,
                clusterId,
                canonicalJson,
                new[] { ResourceKey(clusterId, topicName) },
                new[]
                {
                    new MutationPrecondition(
                        "record.topic",
                        FingerprintTopic(topic.Value)),
                },
                Array.AsReadOnly(digests.ToArray()),
                new[]
                {
                    new MutationAuthorizationTarget(
                        AuthorizationAction.RecordProduce,
                        clusterId,
                        topicName),
                });

            var risk = _policy.ClassifyRisk(request.Records.Count, totalBytes);
            if (risk.RequiresIndependentApproval)
                return FailAndDispose(
                    material,
                    RecordProductionPlanningFailureCode.PolicyApprovalUnsupported,
                    "Record production with ephemeral payload material cannot enter a delayed independent-approval workflow.");

            return RecordProductionPlanningResult.Success(
                new RecordProductionPlan(canonical, intent, risk),
                new RecordProductionExecutionMaterial(material));
        }
        catch
        {
            ZeroAndClear(material);
            throw;
        }
    }

    internal static string ResourceKey(string clusterId, string topicName) =>
        $"cluster/{clusterId}/topic/{topicName}";

    internal static string FingerprintTopic(TopicMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        var builder = new StringBuilder(512);
        Append(builder, "topic", metadata.Name);
        Append(builder, "internal", metadata.IsInternal ? "1" : "0");
        foreach (var partition in metadata.Partitions.OrderBy(item => item.PartitionId))
        {
            Append(builder, "partition", partition.PartitionId.ToString(System.Globalization.CultureInfo.InvariantCulture));
            Append(builder, "leader", partition.LeaderBrokerId?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "none");
            Append(builder, "replicas", string.Join(",", partition.ReplicaBrokerIds.OrderBy(value => value)));
            Append(builder, "isr", string.Join(",", partition.InSyncReplicaBrokerIds.OrderBy(value => value)));
        }

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    internal static RecordProductionCanonicalIntent DeserializeCanonical(string json) =>
        JsonSerializer.Deserialize<RecordProductionCanonicalIntent>(json, CanonicalJson)
        ?? throw new MutationStateException("Record production canonical intent could not be deserialized.");

    private static RecordProductionPlanningResult Failed(
        RecordProductionPlanningFailureCode code,
        string message,
        int? ordinal = null) =>
        RecordProductionPlanningResult.Failed(
            new RecordProductionPlanningFailure(code, message, ordinal));

    private static RecordProductionPlanningResult FailAndDispose(
        Dictionary<string, byte[]> material,
        RecordProductionPlanningFailureCode code,
        string message,
        int? ordinal = null)
    {
        ZeroAndClear(material);
        return Failed(code, message, ordinal);
    }

    private static void ZeroAndClear(Dictionary<string, byte[]> material)
    {
        foreach (var value in material.Values)
            CryptographicOperations.ZeroMemory(value);
        material.Clear();
    }

    private static RecordProductionPlanningFailure MapObservationFailure(KafkaFailure? failure) =>
        failure?.Category switch
        {
            KafkaFailureCategory.Unauthorized =>
                new(RecordProductionPlanningFailureCode.ProviderUnauthorized,
                    "Kafka denied access to observe the record-production target."),
            KafkaFailureCategory.NotSupported =>
                new(RecordProductionPlanningFailureCode.ProviderUnsupported,
                    "Kafka does not support the required target observation."),
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure =>
                new(RecordProductionPlanningFailureCode.ProviderUnavailable,
                    "Kafka target state is currently unavailable for safe production planning."),
            KafkaFailureCategory.ProtocolError when failure.Code is
                "kafka_unknowntopicorpart" or "kafka_local_unknowntopic" or "kafka_resourcenotfound" =>
                new(RecordProductionPlanningFailureCode.TopicNotFound,
                    "The record-production topic was not found."),
            _ =>
                new(RecordProductionPlanningFailureCode.ObservationFailed,
                    "Kafka target state could not be safely observed."),
        };

    private static void Append(StringBuilder builder, string key, string value) =>
        builder.Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}

public sealed class RecordProductionPreconditionValidator
{
    private readonly IKafkaAdministrationPort _kafka;
    private readonly RecordProductionPlannerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public RecordProductionPreconditionValidator(
        IKafkaAdministrationPort kafka,
        RecordProductionPlannerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _policy = policy ?? RecordProductionPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationPreDispatchGuardResult> ValidateAsync(
        MutationOperationSnapshot operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (operation.OperationKind != MutationOperationKind.RecordProduce)
            return new(MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                "record_production_precondition_operation_not_supported");

        RecordProductionCanonicalIntent canonical;
        try
        {
            canonical = RecordProductionPlanner.DeserializeCanonical(operation.CanonicalIntent);
        }
        catch
        {
            return new(MutationPreDispatchGuardOutcome.StalePreview,
                "record_production_precondition_intent_invalid");
        }

        var expectedResource = RecordProductionPlanner.ResourceKey(canonical.ClusterId, canonical.TopicName);
        if (!string.Equals(operation.ClusterId, canonical.ClusterId, StringComparison.Ordinal) ||
            operation.ResourceKeys.Count != 1 ||
            !string.Equals(operation.ResourceKeys[0], expectedResource, StringComparison.Ordinal) ||
            operation.AuthorizationTargets.Count != 1 ||
            operation.AuthorizationTargets[0].Action != AuthorizationAction.RecordProduce ||
            !string.Equals(operation.AuthorizationTargets[0].ResourceName, canonical.TopicName, StringComparison.Ordinal))
        {
            return new(MutationPreDispatchGuardOutcome.StalePreview,
                "record_production_precondition_binding_changed");
        }

        var matches = operation.Preconditions
            .Where(item => string.Equals(item.Key, "record.topic", StringComparison.Ordinal))
            .Take(2)
            .ToArray();
        if (matches.Length != 1)
            return new(MutationPreDispatchGuardOutcome.StalePreview,
                "record_production_precondition_missing");

        var observed = await _kafka.GetTopicMetadataAsync(
                canonical.ClusterId,
                canonical.TopicName,
                new KafkaOperationContext(_timeProvider.GetUtcNow().Add(_policy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);

        if (!observed.IsSuccess || observed.Value is null)
            return new(MutationPreDispatchGuardOutcome.CapabilityUnsupported,
                "record_production_precondition_observation_unavailable");

        if (observed.Value.IsInternal)
            return new(MutationPreDispatchGuardOutcome.StalePreview,
                "record_production_precondition_internal_topic");

        return string.Equals(
                matches[0].Fingerprint,
                RecordProductionPlanner.FingerprintTopic(observed.Value),
                StringComparison.Ordinal)
            ? MutationPreDispatchGuardResult.Allowed
            : new(MutationPreDispatchGuardOutcome.StalePreview,
                "record_production_precondition_changed");
    }
}
