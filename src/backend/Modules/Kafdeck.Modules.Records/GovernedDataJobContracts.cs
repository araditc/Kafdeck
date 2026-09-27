using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public enum GovernedDataJobKind
{
    Replay = 1,
    Forward = 2,
    Reprocess = 3,
    DlqForward = 4,
}

public enum GovernedDataTransformKind
{
    BytePreserving = 1,
    MaskedStructuredProjection = 2,
}

public sealed record GovernedDataJobEndpoint(
    string ClusterId,
    string ProfileVersion,
    string KafkaClusterId);

public sealed record GovernedDataJobRange(
    string SourceTopic,
    int SourcePartition,
    string DestinationTopic,
    int DestinationPartition,
    long StartInclusive,
    long EndExclusive,
    string SourceTopicFingerprint,
    string DestinationTopicFingerprint);

public sealed record GovernedDataTransform(
    GovernedDataTransformKind Kind,
    string? SerdeFormat = null,
    IReadOnlyList<string>? ProjectedFields = null);

public sealed record GovernedDataJobPlan(
    GovernedDataJobKind Kind,
    GovernedDataJobEndpoint Source,
    GovernedDataJobEndpoint Destination,
    IReadOnlyList<GovernedDataJobRange> Ranges,
    ClusterTransferBudget Budget,
    ClusterTransferDataPolicy DataPolicy,
    GovernedDataTransform Transform,
    string PlanFingerprint);

public static class GovernedDataJobPolicy
{
    public const int MaxRanges = ClusterTransferPolicy.MaxMappings;
    public const int MaxProjectedFields = 64;
    public const int MaxProjectedFieldCharacters = 256;

    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        WriteIndented = false,
    };

    public static GovernedDataJobPlan FromTransfer(
        GovernedDataJobKind kind,
        ClusterTransferPlan transfer,
        GovernedDataTransform? transform = null)
    {
        ArgumentNullException.ThrowIfNull(transfer);
        if (!Enum.IsDefined(kind))
            throw new ArgumentOutOfRangeException(nameof(kind));

        var effectiveTransform = transform ??
            new GovernedDataTransform(
                GovernedDataTransformKind.BytePreserving);

        ValidateTransform(effectiveTransform);

        var source = new GovernedDataJobEndpoint(
            transfer.Source.ClusterId,
            transfer.Source.ProfileVersion,
            transfer.Source.KafkaClusterId);
        var destination = new GovernedDataJobEndpoint(
            transfer.Destination.ClusterId,
            transfer.Destination.ProfileVersion,
            transfer.Destination.KafkaClusterId);
        var ranges = transfer.Mappings
            .Select(mapping => new GovernedDataJobRange(
                mapping.SourceTopic,
                mapping.SourcePartition,
                mapping.DestinationTopic,
                mapping.DestinationPartition,
                mapping.StartInclusive,
                mapping.EndExclusive,
                mapping.SourceTopicFingerprint,
                mapping.DestinationTopicFingerprint))
            .ToArray();

        var fingerprint = PlanFingerprint(
            kind,
            source,
            destination,
            ranges,
            transfer.Budget,
            transfer.DataPolicy,
            effectiveTransform);

        return new GovernedDataJobPlan(
            kind,
            source,
            destination,
            ranges,
            transfer.Budget,
            transfer.DataPolicy,
            effectiveTransform,
            fingerprint);
    }

    public static MutationIntentDescriptor BuildIntent(
        GovernedDataJobPlan plan)
    {
        ValidatePlan(plan);

        var transfer = ToTransferPlan(plan);
        var baseIntent = ClusterTransferPolicy.BuildIntent(transfer);

        var dataJobResource =
            $"data-job/{plan.PlanFingerprint}";

        var authorization = (baseIntent.AuthorizationTargets ??
            throw new MutationStateException(
                "Cluster-transfer intent is missing authorization targets."))
            .Concat(
            [
                new MutationAuthorizationTarget(
                    AuthorizationAction.DataJobPlan,
                    plan.Source.ClusterId,
                    dataJobResource),
                new MutationAuthorizationTarget(
                    AuthorizationAction.DataJobExecute,
                    plan.Source.ClusterId,
                    dataJobResource),
            ])
            .Distinct()
            .OrderBy(item => item.Action)
            .ThenBy(item => item.ClusterId, StringComparer.Ordinal)
            .ThenBy(item => item.ResourceName, StringComparer.Ordinal)
            .ToArray();

        var resources = baseIntent.ResourceKeys
            .Concat(new[] { dataJobResource })
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();

        var preconditions = (baseIntent.Preconditions ??
            throw new MutationStateException(
                "Cluster-transfer intent is missing preconditions."))
            .Where(item => !string.Equals(
                item.Key,
                "transfer.plan",
                StringComparison.Ordinal))
            .Concat(
            [
                new MutationPrecondition(
                    "data-job.plan",
                    plan.PlanFingerprint),
                new MutationPrecondition(
                    "data-job.transform",
                    TransformFingerprint(plan.Transform)),
            ])
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .ToArray();

        var canonical = JsonSerializer.Serialize(
            plan,
            CanonicalJson);

        return new MutationIntentDescriptor(
            MutationOperationKind.DataJob,
            plan.Source.ClusterId,
            canonical,
            resources,
            preconditions,
            AuthorizationTargets: authorization);
    }

    public static MutationRiskDecision ClassifyRisk(
        GovernedDataJobPlan plan)
    {
        ValidatePlan(plan);

        var transferRisk =
            ClusterTransferPolicy.ClassifyRisk(
                ToTransferPlan(plan));

        if (plan.Transform.Kind ==
            GovernedDataTransformKind.BytePreserving)
        {
            return transferRisk;
        }

        var reasons = transferRisk.Reasons
            .Concat(new[] { "data_job_reprocess" })
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();

        return new MutationRiskDecision(
            (int)transferRisk.RiskClass < (int)MutationRiskClass.High
                ? MutationRiskClass.High
                : transferRisk.RiskClass,
            Array.AsReadOnly(reasons),
            MutationConfirmationMode.TypedTarget,
            transferRisk.RequiresIndependentApproval);
    }

    public static string PlanFingerprint(
        GovernedDataJobKind kind,
        GovernedDataJobEndpoint source,
        GovernedDataJobEndpoint destination,
        IReadOnlyList<GovernedDataJobRange> ranges,
        ClusterTransferBudget budget,
        ClusterTransferDataPolicy policy,
        GovernedDataTransform transform)
    {
        ValidateTransform(transform);

        var canonical = JsonSerializer.Serialize(
            new
            {
                kind,
                source,
                destination,
                ranges,
                budget = new
                {
                    budget.MaxBatchRecords,
                    budget.MaxBatchBytes,
                    budget.MaxTotalRecords,
                    budget.MaxTotalBytes,
                    maxDurationTicks =
                        budget.MaxDuration.Ticks,
                    budget.MaxRecordsPerSecond,
                    budget.MaxBytesPerSecond,
                },
                policy,
                transform = CanonicalTransform(transform),
            },
            CanonicalJson);

        return Sha256(canonical);
    }

    public static void ValidatePlan(
        GovernedDataJobPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        if (!Enum.IsDefined(plan.Kind))
            throw new MutationStateException(
                "Data-job kind is invalid.");

        if (plan.Ranges.Count is < 1 or > MaxRanges)
            throw new MutationStateException(
                "Data-job range count is outside the admitted bound.");

        _ = ClusterTransferPolicy.RequireIdentifier(
            plan.Source.ClusterId,
            "Source cluster",
            256);
        _ = ClusterTransferPolicy.RequireIdentifier(
            plan.Destination.ClusterId,
            "Destination cluster",
            256);
        _ = ClusterTransferPolicy.RequireIdentifier(
            plan.Source.ProfileVersion,
            "Source profile version",
            256);
        _ = ClusterTransferPolicy.RequireIdentifier(
            plan.Destination.ProfileVersion,
            "Destination profile version",
            256);
        _ = ClusterTransferPolicy.RequireIdentifier(
            plan.Source.KafkaClusterId,
            "Source physical Kafka cluster",
            256);
        _ = ClusterTransferPolicy.RequireIdentifier(
            plan.Destination.KafkaClusterId,
            "Destination physical Kafka cluster",
            256);

        if (string.Equals(
                plan.Source.KafkaClusterId,
                plan.Destination.KafkaClusterId,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Data-job source and destination physical Kafka clusters must be distinct.");
        }

        foreach (var range in plan.Ranges)
        {
            _ = ClusterTransferPolicy.Normalize(
                new ClusterTransferMappingRequest(
                    range.SourceTopic,
                    range.SourcePartition,
                    range.DestinationTopic,
                    range.DestinationPartition,
                    range.StartInclusive,
                    range.EndExclusive));

            RequireSha256(
                range.SourceTopicFingerprint,
                "Source topic fingerprint");
            RequireSha256(
                range.DestinationTopicFingerprint,
                "Destination topic fingerprint");
        }

        ValidateTransform(plan.Transform);

        var expected = PlanFingerprint(
            plan.Kind,
            plan.Source,
            plan.Destination,
            plan.Ranges,
            plan.Budget,
            plan.DataPolicy,
            plan.Transform);

        if (!string.Equals(
                expected,
                plan.PlanFingerprint,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Data-job plan fingerprint does not match canonical content.");
        }
    }

    public static GovernedDataJobPlan DeserializePlan(
        string canonicalIntent)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            canonicalIntent);

        try
        {
            var plan =
                JsonSerializer.Deserialize<
                    GovernedDataJobPlan>(
                    canonicalIntent,
                    CanonicalJson) ??
                throw new MutationStateException(
                    "Data-job canonical intent is invalid.");

            ValidatePlan(plan);
            return plan;
        }
        catch (JsonException exception)
        {
            throw new MutationStateException(
                $"Data-job canonical intent is invalid: {exception.GetType().Name}.");
        }
    }

    public static ClusterTransferPlan ToTransferPlan(
        GovernedDataJobPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        var source = new ClusterTransferEndpoint(
            plan.Source.ClusterId,
            plan.Source.ProfileVersion,
            plan.Source.KafkaClusterId);
        var destination = new ClusterTransferEndpoint(
            plan.Destination.ClusterId,
            plan.Destination.ProfileVersion,
            plan.Destination.KafkaClusterId);
        var mappings = plan.Ranges
            .Select(range => new ClusterTransferMapping(
                range.SourceTopic,
                range.SourcePartition,
                range.DestinationTopic,
                range.DestinationPartition,
                range.StartInclusive,
                range.EndExclusive,
                range.SourceTopicFingerprint,
                range.DestinationTopicFingerprint))
            .ToArray();

        return new ClusterTransferPlan(
            source,
            destination,
            mappings,
            plan.Budget,
            plan.DataPolicy,
            ClusterTransferPolicy.PlanFingerprint(
                source,
                destination,
                mappings,
                plan.Budget,
                plan.DataPolicy));
    }

    private static void ValidateTransform(
        GovernedDataTransform transform)
    {
        ArgumentNullException.ThrowIfNull(transform);
        if (!Enum.IsDefined(transform.Kind))
            throw new MutationStateException(
                "Data-job transform kind is invalid.");

        switch (transform.Kind)
        {
            case GovernedDataTransformKind.BytePreserving:
                if (transform.SerdeFormat is not null ||
                    transform.ProjectedFields is { Count: > 0 })
                {
                    throw new MutationStateException(
                        "Byte-preserving data jobs must not configure a structured transform.");
                }
                break;

            case GovernedDataTransformKind.MaskedStructuredProjection:
                var format =
                    ClusterTransferPolicy.RequireIdentifier(
                        transform.SerdeFormat ??
                        throw new MutationStateException(
                            "Structured projection requires a SerDe format."),
                        "SerDe format",
                        64);

                if (!string.Equals(
                        format,
                        "json",
                        StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(
                        format,
                        "cbor",
                        StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(
                        format,
                        "xml",
                        StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(
                        format,
                        "messagepack",
                        StringComparison.OrdinalIgnoreCase))
                {
                    throw new MutationStateException(
                        "Structured projection SerDe format is unsupported.");
                }

                var fields =
                    transform.ProjectedFields ??
                    throw new MutationStateException(
                        "Structured projection requires explicit projected fields.");

                if (fields.Count is < 1 or > MaxProjectedFields)
                {
                    throw new MutationStateException(
                        "Structured projection field count is outside the admitted bound.");
                }

                if (fields
                    .Select(field =>
                        ClusterTransferPolicy.RequireIdentifier(
                            field,
                            "Projected field",
                            MaxProjectedFieldCharacters))
                    .Distinct(StringComparer.Ordinal)
                    .Count() != fields.Count)
                {
                    throw new MutationStateException(
                        "Structured projection fields must be unique.");
                }
                break;

            default:
                throw new MutationStateException(
                    "Data-job transform kind is unsupported.");
        }
    }

    private static object CanonicalTransform(
        GovernedDataTransform transform) =>
        new
        {
            transform.Kind,
            serdeFormat =
                transform.SerdeFormat?.ToLowerInvariant(),
            projectedFields =
                transform.ProjectedFields?
                    .OrderBy(
                        value => value,
                        StringComparer.Ordinal)
                    .ToArray(),
        };

    private static string TransformFingerprint(
        GovernedDataTransform transform)
    {
        var canonical = JsonSerializer.Serialize(
            CanonicalTransform(transform),
            CanonicalJson);
        return Sha256(canonical);
    }

    private static void RequireSha256(
        string value,
        string field)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length != 64 ||
            value.Any(character =>
                !char.IsAsciiHexDigit(character)))
        {
            throw new MutationStateException(
                $"{field} must be a SHA-256 hex digest.");
        }
    }

    private static string Sha256(
        string value) =>
        Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();
}
