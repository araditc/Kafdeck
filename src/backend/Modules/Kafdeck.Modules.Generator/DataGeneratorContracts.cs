using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Generator;

public enum DataGeneratorSourceKind
{
    Schema = 1,
    BuiltInTemplate = 2,
}

public enum DataGeneratorBuiltInTemplate
{
    BasicJsonV1 = 1,
}

public sealed record DataGeneratorSchemaSource(
    string Subject,
    int Version,
    int SchemaId,
    RecordSchemaFormat Format,
    string SchemaFingerprint);

public sealed record DataGeneratorTemplateSource(
    DataGeneratorBuiltInTemplate Template,
    int Version,
    string TemplateFingerprint);

public sealed record DataGeneratorSource(
    DataGeneratorSourceKind Kind,
    DataGeneratorSchemaSource? Schema = null,
    DataGeneratorTemplateSource? Template = null);

public sealed record DataGeneratorDestination(
    string ClusterId,
    string ProfileVersion,
    string KafkaClusterId,
    string TopicName,
    int Partition,
    string TopicFingerprint);

public sealed record DataGeneratorBudget
{
    public const int DefaultMaxBatchRecords = 100;
    public const int HardMaxBatchRecords = 1_000;

    public const long DefaultMaxBatchBytes = 1L * 1024 * 1024;
    public const long HardMaxBatchBytes = 10L * 1024 * 1024;

    public const int DefaultMaxTotalRecords = 10_000;
    public const int HardMaxTotalRecords = 100_000;

    public const long DefaultMaxTotalBytes = 10L * 1024 * 1024;
    public const long HardMaxTotalBytes = 100L * 1024 * 1024;

    public static readonly TimeSpan DefaultMaxDuration =
        TimeSpan.FromMinutes(10);
    public static readonly TimeSpan HardMaxDuration =
        TimeSpan.FromHours(1);

    public const int DefaultMaxRecordsPerSecond = 100;
    public const int HardMaxRecordsPerSecond = 1_000;

    public const long DefaultMaxBytesPerSecond = 1L * 1024 * 1024;
    public const long HardMaxBytesPerSecond = 10L * 1024 * 1024;

    public DataGeneratorBudget(
        int maxBatchRecords = DefaultMaxBatchRecords,
        long maxBatchBytes = DefaultMaxBatchBytes,
        int maxTotalRecords = DefaultMaxTotalRecords,
        long maxTotalBytes = DefaultMaxTotalBytes,
        TimeSpan? maxDuration = null,
        int maxRecordsPerSecond = DefaultMaxRecordsPerSecond,
        long maxBytesPerSecond = DefaultMaxBytesPerSecond)
    {
        if (maxBatchRecords is < 1 or > HardMaxBatchRecords)
            throw new ArgumentOutOfRangeException(nameof(maxBatchRecords));
        if (maxBatchBytes is < 1 or > HardMaxBatchBytes)
            throw new ArgumentOutOfRangeException(nameof(maxBatchBytes));
        if (maxTotalRecords is < 1 or > HardMaxTotalRecords)
            throw new ArgumentOutOfRangeException(nameof(maxTotalRecords));
        if (maxTotalBytes is < 1 or > HardMaxTotalBytes)
            throw new ArgumentOutOfRangeException(nameof(maxTotalBytes));
        if (maxRecordsPerSecond is < 1 or > HardMaxRecordsPerSecond)
            throw new ArgumentOutOfRangeException(nameof(maxRecordsPerSecond));
        if (maxBytesPerSecond is < 1 or > HardMaxBytesPerSecond)
            throw new ArgumentOutOfRangeException(nameof(maxBytesPerSecond));

        var duration = maxDuration ?? DefaultMaxDuration;
        if (duration <= TimeSpan.Zero ||
            duration > HardMaxDuration)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDuration));
        }

        if (maxBatchRecords > maxTotalRecords)
            throw new ArgumentOutOfRangeException(
                nameof(maxBatchRecords),
                "Generator batch-record ceiling cannot exceed the total-record ceiling.");

        if (maxBatchBytes > maxTotalBytes)
            throw new ArgumentOutOfRangeException(
                nameof(maxBatchBytes),
                "Generator batch-byte ceiling cannot exceed the total-byte ceiling.");

        MaxBatchRecords = maxBatchRecords;
        MaxBatchBytes = maxBatchBytes;
        MaxTotalRecords = maxTotalRecords;
        MaxTotalBytes = maxTotalBytes;
        MaxDuration = duration;
        MaxRecordsPerSecond = maxRecordsPerSecond;
        MaxBytesPerSecond = maxBytesPerSecond;
    }

    public int MaxBatchRecords { get; }
    public long MaxBatchBytes { get; }
    public int MaxTotalRecords { get; }
    public long MaxTotalBytes { get; }
    public TimeSpan MaxDuration { get; }
    public int MaxRecordsPerSecond { get; }
    public long MaxBytesPerSecond { get; }
}

public sealed record DataGeneratorPlan(
    DataGeneratorDestination Destination,
    DataGeneratorSource Source,
    int RecordCount,
    int Seed,
    DataGeneratorBudget Budget,
    string PlanFingerprint);

public sealed record DataGeneratorDeploymentPolicy(
    IReadOnlySet<string> EnabledClusterIds)
{
    public static DataGeneratorDeploymentPolicy DenyAll { get; } =
        new(new HashSet<string>(StringComparer.Ordinal));

    public bool IsEnabled(string clusterId) =>
        EnabledClusterIds.Contains(clusterId);
}

public static class DataGeneratorPolicy
{
    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        WriteIndented = false,
    };

    public static DataGeneratorPlan CreatePlan(
        DataGeneratorDestination destination,
        DataGeneratorSource source,
        int recordCount,
        int seed,
        DataGeneratorBudget budget)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(budget);

        ValidateDestination(destination);
        ValidateSource(source);

        if (recordCount is < 1 or >
            DataGeneratorBudget.HardMaxTotalRecords)
        {
            throw new MutationStateException(
                "Generator record count is outside the admitted finite bound.");
        }

        if (recordCount > budget.MaxTotalRecords)
        {
            throw new MutationStateException(
                "Generator record count exceeds the approved total-record budget.");
        }

        var fingerprint = PlanFingerprint(
            destination,
            source,
            recordCount,
            seed,
            budget);

        return new DataGeneratorPlan(
            destination,
            source,
            recordCount,
            seed,
            budget,
            fingerprint);
    }

    public static void ValidatePlan(DataGeneratorPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        ValidateDestination(plan.Destination);
        ValidateSource(plan.Source);

        if (plan.RecordCount is < 1 or >
            DataGeneratorBudget.HardMaxTotalRecords ||
            plan.RecordCount > plan.Budget.MaxTotalRecords)
        {
            throw new MutationStateException(
                "Generator record count is outside the immutable budget.");
        }

        var expected = PlanFingerprint(
            plan.Destination,
            plan.Source,
            plan.RecordCount,
            plan.Seed,
            plan.Budget);

        if (!string.Equals(
                expected,
                plan.PlanFingerprint,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Generator plan fingerprint does not match canonical content.");
        }
    }

    public static MutationIntentDescriptor BuildIntent(
        DataGeneratorPlan plan)
    {
        ValidatePlan(plan);

        var destination =
            plan.Destination;
        var physicalResource =
            FleetConflictKeyCodec.TopicPartition(
                destination.KafkaClusterId,
                destination.TopicName,
                destination.Partition);
        var generatorResource =
            $"data-generator/{plan.PlanFingerprint}";

        var authorization =
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.DataGeneratorPlan,
                    destination.ClusterId,
                    generatorResource),
                new MutationAuthorizationTarget(
                    AuthorizationAction.DataGeneratorExecute,
                    destination.ClusterId,
                    generatorResource),
                new MutationAuthorizationTarget(
                    AuthorizationAction.ClusterRead,
                    destination.ClusterId,
                    destination.ClusterId),
                new MutationAuthorizationTarget(
                    AuthorizationAction.TopicRead,
                    destination.ClusterId,
                    destination.TopicName),
                new MutationAuthorizationTarget(
                    AuthorizationAction.RecordProduce,
                    destination.ClusterId,
                    destination.TopicName),
            };

        var preconditions =
            new[]
            {
                new MutationPrecondition(
                    "generator.plan",
                    plan.PlanFingerprint),
                new MutationPrecondition(
                    "generator.source",
                    SourceFingerprint(plan.Source)),
                new MutationPrecondition(
                    "destination.kafka-cluster-id",
                    Sha256(destination.KafkaClusterId)),
                new MutationPrecondition(
                    "destination.topic",
                    destination.TopicFingerprint),
            };

        return new MutationIntentDescriptor(
            MutationOperationKind.DataGenerator,
            destination.ClusterId,
            SerializePlan(plan),
            new[]
            {
                physicalResource,
                generatorResource,
            },
            preconditions,
            AuthorizationTargets: authorization);
    }

    public static MutationRiskDecision ClassifyRisk(
        DataGeneratorPlan plan)
    {
        ValidatePlan(plan);

        var critical =
            plan.RecordCount >
                DataGeneratorBudget.DefaultMaxTotalRecords ||
            plan.Budget.MaxTotalBytes >
                DataGeneratorBudget.DefaultMaxTotalBytes ||
            plan.Budget.MaxRecordsPerSecond >
                DataGeneratorBudget.DefaultMaxRecordsPerSecond ||
            plan.Budget.MaxBytesPerSecond >
                DataGeneratorBudget.DefaultMaxBytesPerSecond ||
            plan.Budget.MaxDuration >
                DataGeneratorBudget.DefaultMaxDuration;

        return critical
            ? new MutationRiskDecision(
                MutationRiskClass.Critical,
                Array.AsReadOnly(
                    new[]
                    {
                        "data_generator",
                        "data_generator_blast_radius",
                    }),
                MutationConfirmationMode.TypedTarget,
                RequiresIndependentApproval: true)
            : new MutationRiskDecision(
                MutationRiskClass.High,
                Array.AsReadOnly(
                    new[] { "data_generator" }),
                MutationConfirmationMode.TypedTarget,
                RequiresIndependentApproval: false);
    }

    public static string FingerprintSchema(
        RecordSchemaDocument schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var canonical = JsonSerializer.Serialize(
            new
            {
                schema.Id,
                schema.Format,
                schema.SchemaText,
                references = schema.References
                    .OrderBy(
                        item => item.Name,
                        StringComparer.Ordinal)
                    .ThenBy(
                        item => item.Subject,
                        StringComparer.Ordinal)
                    .ThenBy(item => item.Version)
                    .ToArray(),
            },
            CanonicalJson);

        return Sha256(canonical);
    }

    public static string BuiltInTemplateFingerprint(
        DataGeneratorBuiltInTemplate template,
        int version)
    {
        if (!Enum.IsDefined(template) ||
            version != 1)
        {
            throw new MutationStateException(
                "Generator built-in template identity is unsupported.");
        }

        return Sha256(
            $"kafdeck:data-generator-template:{(int)template}:{version}");
    }

    public static string SourceFingerprint(
        DataGeneratorSource source)
    {
        ValidateSource(source);

        return source.Kind switch
        {
            DataGeneratorSourceKind.Schema =>
                source.Schema!.SchemaFingerprint,

            DataGeneratorSourceKind.BuiltInTemplate =>
                source.Template!.TemplateFingerprint,

            _ => throw new MutationStateException(
                "Generator source kind is unsupported."),
        };
    }

    public static string PlanFingerprint(
        DataGeneratorDestination destination,
        DataGeneratorSource source,
        int recordCount,
        int seed,
        DataGeneratorBudget budget)
    {
        ValidateDestination(destination);
        ValidateSource(source);
        ArgumentNullException.ThrowIfNull(budget);

        var canonical = JsonSerializer.Serialize(
            new
            {
                destination,
                source,
                recordCount,
                seed,
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
            },
            CanonicalJson);

        return Sha256(canonical);
    }

    public static DataGeneratorPlan DeserializePlan(
        string canonicalIntent)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            canonicalIntent);

        try
        {
            var plan =
                JsonSerializer.Deserialize<DataGeneratorPlan>(
                    canonicalIntent,
                    CanonicalJson) ??
                throw new MutationStateException(
                    "Generator canonical intent is invalid.");

            ValidatePlan(plan);
            return plan;
        }
        catch (Exception exception)
            when (exception is
                JsonException or
                ArgumentException or
                OverflowException)
        {
            throw new MutationStateException(
                $"Generator canonical intent is invalid: {exception.GetType().Name}.");
        }
    }

    private static string SerializePlan(
        DataGeneratorPlan plan) =>
        JsonSerializer.Serialize(
            plan,
            CanonicalJson);

    private static void ValidateDestination(
        DataGeneratorDestination destination)
    {
        _ = RequireIdentifier(
            destination.ClusterId,
            "Generator destination cluster",
            256);
        _ = RequireIdentifier(
            destination.ProfileVersion,
            "Generator destination profile version",
            256);
        _ = RequireIdentifier(
            destination.KafkaClusterId,
            "Generator destination Kafka cluster",
            256);
        _ = RequireIdentifier(
            destination.TopicName,
            "Generator destination topic",
            249);

        if (destination.Partition < 0)
        {
            throw new MutationStateException(
                "Generator destination partition must be non-negative.");
        }

        RequireSha256(
            destination.TopicFingerprint,
            "Generator destination topic fingerprint");
    }

    private static void ValidateSource(
        DataGeneratorSource source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (!Enum.IsDefined(source.Kind))
        {
            throw new MutationStateException(
                "Generator source kind is invalid.");
        }

        switch (source.Kind)
        {
            case DataGeneratorSourceKind.Schema:
                if (source.Schema is null ||
                    source.Template is not null)
                {
                    throw new MutationStateException(
                        "Schema generator source shape is invalid.");
                }

                _ = RequireIdentifier(
                    source.Schema.Subject,
                    "Generator schema subject",
                    512);

                if (source.Schema.Version <= 0 ||
                    source.Schema.SchemaId <= 0 ||
                    !Enum.IsDefined(source.Schema.Format))
                {
                    throw new MutationStateException(
                        "Generator schema identity is invalid.");
                }

                RequireSha256(
                    source.Schema.SchemaFingerprint,
                    "Generator schema fingerprint");
                break;

            case DataGeneratorSourceKind.BuiltInTemplate:
                if (source.Template is null ||
                    source.Schema is not null ||
                    !Enum.IsDefined(source.Template.Template) ||
                    source.Template.Version != 1)
                {
                    throw new MutationStateException(
                        "Built-in generator template identity is invalid.");
                }

                var expected =
                    BuiltInTemplateFingerprint(
                        source.Template.Template,
                        source.Template.Version);
                if (!string.Equals(
                        expected,
                        source.Template.TemplateFingerprint,
                        StringComparison.Ordinal))
                {
                    throw new MutationStateException(
                        "Built-in generator template fingerprint is invalid.");
                }
                break;

            default:
                throw new MutationStateException(
                    "Generator source kind is unsupported.");
        }
    }

    private static string RequireIdentifier(
        string value,
        string field,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new MutationStateException(
                $"{field} is invalid or exceeds the admitted bound.");
        }

        return value;
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
                $"{field} must be one SHA-256 hex digest.");
        }
    }

    private static string Sha256(
        string value) =>
        Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();
}
