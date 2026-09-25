using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

public enum KafkaQuotaEntityDimension
{
    User = 1,
    ClientId = 2,
    Ip = 3,
}

public enum KafkaQuotaMetric
{
    ProducerByteRate = 1,
    ConsumerByteRate = 2,
    RequestPercentage = 3,
    ControllerMutationRate = 4,
}

public sealed record KafkaQuotaEntityComponent(
    KafkaQuotaEntityDimension Dimension,
    string? Value);

public sealed record KafkaQuotaEntity(
    IReadOnlyList<KafkaQuotaEntityComponent> Components);

public sealed record KafkaQuotaChange(
    KafkaQuotaMetric Metric,
    double? Value);

public sealed record QuotaMutationPlan(
    string ClusterId,
    KafkaQuotaEntity Entity,
    IReadOnlyList<KafkaQuotaChange> Changes,
    string ObservedFingerprint);

public enum DynamicConfigTargetKind
{
    ClusterDefault = 1,
    Broker = 2,
}

public enum DynamicConfigUpdateMode
{
    ClusterWide = 1,
    PerBroker = 2,
    Both = 3,
}

public enum DynamicConfigValueKind
{
    Int32 = 1,
    Int64 = 2,
    Double = 3,
    Boolean = 4,
    String = 5,
}

public enum DynamicConfigSafetyClass
{
    Ordinary = 1,
    Critical = 2,
}

public enum DynamicConfigMutationMode
{
    Set = 1,
    Reset = 2,
}

public sealed record DynamicConfigTarget(
    string ClusterId,
    DynamicConfigTargetKind Kind,
    string? BrokerId = null);

public sealed record DynamicConfigDefinition(
    string Key,
    DynamicConfigUpdateMode UpdateMode,
    DynamicConfigValueKind ValueKind,
    DynamicConfigSafetyClass SafetyClass,
    string ProviderEvidenceId,
    IReadOnlyList<string> SupportedBrokerVersions);

public sealed record DynamicConfigChange(
    string Key,
    DynamicConfigMutationMode Mode,
    string? Value);

public sealed record DynamicConfigPlannedChange(
    string Key,
    DynamicConfigMutationMode Mode,
    string? Value,
    DynamicConfigSafetyClass SafetyClass,
    string ProviderEvidenceId,
    IReadOnlyList<string> SupportedBrokerVersions);

public sealed record DynamicConfigMutationPlan(
    DynamicConfigTarget Target,
    IReadOnlyList<DynamicConfigPlannedChange> Changes,
    string ObservedFingerprint);

public sealed class DynamicConfigRegistry
{
    private static readonly string[] ForbiddenPrefixes =
    {
        "ssl.",
        "sasl.",
        "listener.",
        "security.",
        "password.encoder.",
    };

    private static readonly HashSet<string> ForbiddenExact = new(
        new[]
        {
            "advertised.listeners",
            "authorizer.class.name",
            "broker.rack",
            "controller.listener.names",
            "controller.quorum.voters",
            "follower.replication.throttled.rate",
            "inter.broker.listener.name",
            "leader.replication.throttled.rate",
            "log.dirs",
            "metadata.log.dir",
            "node.id",
            "principal.builder.class",
            "process.roles",
            "super.users",
        },
        StringComparer.Ordinal);

    private readonly IReadOnlyDictionary<string, DynamicConfigDefinition> _definitions;

    public DynamicConfigRegistry(IEnumerable<DynamicConfigDefinition> definitions)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        var normalized = new Dictionary<string, DynamicConfigDefinition>(StringComparer.Ordinal);
        foreach (var definition in definitions)
        {
            var item = NormalizeDefinition(definition);
            if (!normalized.TryAdd(item.Key, item))
            {
                throw new ArgumentException(
                    $"Dynamic configuration key '{item.Key}' is defined more than once.",
                    nameof(definitions));
            }
        }

        _definitions = normalized;
    }

    public static DynamicConfigRegistry Empty { get; } =
        new(Array.Empty<DynamicConfigDefinition>());

    public bool TryGet(string key, out DynamicConfigDefinition definition)
    {
        var normalized = NormalizeKey(key);
        return _definitions.TryGetValue(normalized, out definition!);
    }

    public DynamicConfigDefinition Require(string key)
    {
        var normalized = NormalizeKey(key);
        if (!_definitions.TryGetValue(normalized, out var definition))
        {
            throw new MutationStateException(
                $"Dynamic configuration key '{normalized}' is not present in the server-owned provider registry.");
        }

        return definition;
    }

    public static bool IsForbiddenEscapeKey(string key)
    {
        var normalized = NormalizeKey(key);
        return ForbiddenExact.Contains(normalized) ||
               ForbiddenPrefixes.Any(prefix =>
                   normalized.StartsWith(prefix, StringComparison.Ordinal));
    }

    private static DynamicConfigDefinition NormalizeDefinition(
        DynamicConfigDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        if (!Enum.IsDefined(definition.UpdateMode) ||
            !Enum.IsDefined(definition.ValueKind) ||
            !Enum.IsDefined(definition.SafetyClass))
        {
            throw new ArgumentOutOfRangeException(
                nameof(definition),
                "Dynamic configuration definition contains unsupported enum values.");
        }

        var key = NormalizeKey(definition.Key);
        if (IsForbiddenEscapeKey(key))
        {
            throw new ArgumentException(
                $"Dynamic configuration key '{key}' is reserved for a stronger dedicated control path.",
                nameof(definition));
        }

        var evidence = RequireBoundedExact(
            definition.ProviderEvidenceId,
            "Provider evidence ID",
            256);
        ArgumentNullException.ThrowIfNull(definition.SupportedBrokerVersions);
        var versions = definition.SupportedBrokerVersions
            .Select(version => RequireBoundedExact(
                version,
                "Supported broker version",
                64))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(version => version, StringComparer.Ordinal)
            .ToArray();
        if (versions.Length == 0 || versions.Length > 32)
        {
            throw new ArgumentOutOfRangeException(
                nameof(definition),
                "Dynamic configuration definitions require between 1 and 32 exact tested broker versions.");
        }

        return definition with
        {
            Key = key,
            ProviderEvidenceId = evidence,
            SupportedBrokerVersions = Array.AsReadOnly(versions),
        };
    }

    internal static string NormalizeKey(string key)
    {
        var normalized = RequireBoundedExact(key, "Dynamic configuration key", 128);
        if (normalized.Any(character =>
                !(char.IsAsciiLetterOrDigit(character) ||
                  character is '.' or '_' or '-')) ||
            !string.Equals(normalized, normalized.ToLowerInvariant(), StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Dynamic configuration keys must use canonical lowercase Kafka key syntax.",
                nameof(key));
        }

        return normalized;
    }

    internal static string RequireBoundedExact(
        string value,
        string fieldName,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new ArgumentException(
                $"{fieldName} must be exact, at most {maxLength} characters, and contain no control characters.",
                fieldName);
        }

        return value;
    }
}

public static class QuotaMutationPolicy
{
    private const int MaxEntityValueCharacters = 256;
    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        WriteIndented = false,
    };

    public static QuotaMutationPlan CreatePlan(
        string clusterId,
        KafkaQuotaEntity entity,
        IReadOnlyList<KafkaQuotaChange> changes,
        string observedFingerprint)
    {
        var cluster = RequireCluster(clusterId);
        var normalizedEntity = NormalizeEntity(entity);
        var normalizedChanges = NormalizeChanges(changes);
        return new QuotaMutationPlan(
            cluster,
            normalizedEntity,
            normalizedChanges,
            RequireSha256(observedFingerprint, "Quota observation fingerprint"));
    }

    public static MutationIntentDescriptor BuildIntent(QuotaMutationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var normalized = CreatePlan(
            plan.ClusterId,
            plan.Entity,
            plan.Changes,
            plan.ObservedFingerprint);
        var resource = FleetConflictKeyCodec.Encode(
            new FleetConflictTarget(
                FleetConflictTargetKind.QuotaEntity,
                normalized.ClusterId,
                EntityIdentityHash(normalized.Entity)));

        var requirements = FleetMutationAuthorization.NormalizeRequirements(
            MutationOperationKind.QuotaAlter,
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.QuotaRead,
                    normalized.ClusterId,
                    resource),
                new MutationAuthorizationTarget(
                    AuthorizationAction.QuotaAlter,
                    normalized.ClusterId,
                    resource),
            });

        return new MutationIntentDescriptor(
            MutationOperationKind.QuotaAlter,
            normalized.ClusterId,
            JsonSerializer.Serialize(normalized, CanonicalJson),
            new[] { resource },
            new[]
            {
                new MutationPrecondition(
                    "quota.entity",
                    normalized.ObservedFingerprint),
            },
            AuthorizationTargets: requirements);
    }

    public static MutationRiskDecision ClassifyRisk(QuotaMutationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        _ = CreatePlan(
            plan.ClusterId,
            plan.Entity,
            plan.Changes,
            plan.ObservedFingerprint);
        return MutationRiskClassifier.Classify(
            new MutationRiskInput(MutationOperationKind.QuotaAlter));
    }

    public static KafkaQuotaEntity NormalizeEntity(KafkaQuotaEntity entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(entity.Components);
        if (entity.Components.Count == 0 || entity.Components.Count > 3)
        {
            throw new ArgumentOutOfRangeException(
                nameof(entity),
                "Quota entity must contain between one and three dimensions.");
        }

        var components = entity.Components
            .Select(component =>
            {
                ArgumentNullException.ThrowIfNull(component);
                if (!Enum.IsDefined(component.Dimension))
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(entity),
                        "Quota entity contains an unsupported dimension.");
                }

                var value = component.Value is null
                    ? null
                    : DynamicConfigRegistry.RequireBoundedExact(
                        component.Value,
                        "Quota entity value",
                        MaxEntityValueCharacters);
                return component with { Value = value };
            })
            .OrderBy(component => component.Dimension)
            .ToArray();

        if (components.Select(component => component.Dimension).Distinct().Count() !=
            components.Length)
        {
            throw new ArgumentException(
                "Quota entity dimensions must be unique.",
                nameof(entity));
        }

        return new KafkaQuotaEntity(Array.AsReadOnly(components));
    }

    public static string EntityIdentityHash(KafkaQuotaEntity entity)
    {
        var normalized = NormalizeEntity(entity);
        var builder = new StringBuilder(512);
        foreach (var component in normalized.Components)
        {
            Append(
                builder,
                "dimension",
                ((int)component.Dimension).ToString(CultureInfo.InvariantCulture));
            Append(
                builder,
                "value-kind",
                component.Value is null ? "default" : "named");
            if (component.Value is not null)
            {
                Append(builder, "value", component.Value);
            }
        }

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    private static IReadOnlyList<KafkaQuotaChange> NormalizeChanges(
        IReadOnlyList<KafkaQuotaChange> changes)
    {
        ArgumentNullException.ThrowIfNull(changes);
        if (changes.Count == 0 || changes.Count > 4)
        {
            throw new ArgumentOutOfRangeException(
                nameof(changes),
                "Quota mutation must contain between one and four typed metrics.");
        }

        var normalized = changes
            .Select(change =>
            {
                ArgumentNullException.ThrowIfNull(change);
                if (!Enum.IsDefined(change.Metric))
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(changes),
                        "Quota mutation contains an unsupported metric.");
                }

                if (change.Value is { } value &&
                    (!double.IsFinite(value) || value < 0))
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(changes),
                        "Quota values must be finite and non-negative; null is the explicit reset operation.");
                }

                return change;
            })
            .OrderBy(change => change.Metric)
            .ToArray();

        if (normalized.Select(change => change.Metric).Distinct().Count() !=
            normalized.Length)
        {
            throw new ArgumentException(
                "Quota mutation metrics must be unique.",
                nameof(changes));
        }

        return Array.AsReadOnly(normalized);
    }

    private static string RequireCluster(string clusterId) =>
        DynamicConfigRegistry.RequireBoundedExact(
            clusterId,
            "Quota physical cluster ID",
            256);

    private static string RequireSha256(string value, string fieldName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length != 64 || value.Any(character => !char.IsAsciiHexDigit(character)))
        {
            throw new ArgumentException(
                $"{fieldName} must be one full SHA-256 fingerprint.",
                fieldName);
        }

        return value.ToLowerInvariant();
    }

    private static void Append(StringBuilder builder, string key, string value) =>
        builder
            .Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}

public static class DynamicConfigMutationPolicy
{
    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        WriteIndented = false,
    };

    public static DynamicConfigMutationPlan CreatePlan(
        DynamicConfigTarget target,
        IReadOnlyList<DynamicConfigChange> changes,
        string observedFingerprint,
        DynamicConfigRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        var normalizedTarget = NormalizeTarget(target);
        ArgumentNullException.ThrowIfNull(changes);
        if (changes.Count == 0 || changes.Count > 32)
        {
            throw new ArgumentOutOfRangeException(
                nameof(changes),
                "Dynamic configuration mutation must contain between one and 32 keys.");
        }

        var planned = changes
            .Select(change => NormalizeChange(normalizedTarget, change, registry))
            .OrderBy(change => change.Key, StringComparer.Ordinal)
            .ToArray();
        if (planned.Select(change => change.Key).Distinct(StringComparer.Ordinal).Count() !=
            planned.Length)
        {
            throw new ArgumentException(
                "Dynamic configuration mutation keys must be unique.",
                nameof(changes));
        }

        return new DynamicConfigMutationPlan(
            normalizedTarget,
            Array.AsReadOnly(planned),
            RequireSha256(
                observedFingerprint,
                "Dynamic configuration observation fingerprint"));
    }

    public static MutationIntentDescriptor BuildIntent(
        DynamicConfigMutationPlan plan,
        DynamicConfigRegistry registry)
    {
        var normalized = RevalidatePlan(plan, registry);
        var resources = normalized.Changes
            .Select(change => ResourceKey(normalized.Target, change.Key))
            .ToArray();
        var requirements = resources
            .SelectMany(resource => new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.ClusterConfigRead,
                    normalized.Target.ClusterId,
                    resource),
                new MutationAuthorizationTarget(
                    AuthorizationAction.ClusterConfigAlter,
                    normalized.Target.ClusterId,
                    resource),
            })
            .ToArray();

        return new MutationIntentDescriptor(
            MutationOperationKind.ClusterConfigAlter,
            normalized.Target.ClusterId,
            JsonSerializer.Serialize(normalized, CanonicalJson),
            resources,
            new[]
            {
                new MutationPrecondition(
                    "cluster.config",
                    normalized.ObservedFingerprint),
            },
            AuthorizationTargets:
                FleetMutationAuthorization.NormalizeRequirements(
                    MutationOperationKind.ClusterConfigAlter,
                    requirements));
    }

    public static MutationRiskDecision ClassifyRisk(
        DynamicConfigMutationPlan plan,
        DynamicConfigRegistry registry)
    {
        var normalized = RevalidatePlan(plan, registry);
        var proposed = normalized.Changes.Any(change =>
                change.SafetyClass == DynamicConfigSafetyClass.Critical)
            ? new MutationRiskDecision(
                MutationRiskClass.Critical,
                new[] { "dynamic_config_sensitive_key" },
                MutationConfirmationMode.TypedTarget,
                true)
            : new MutationRiskDecision(
                MutationRiskClass.High,
                new[] { "dynamic_config_allowlisted_key" },
                MutationConfirmationMode.TypedTarget,
                false);

        return MutationRiskClassifier.EnforceBuiltInFloor(
            new MutationRiskInput(
                MutationOperationKind.ClusterConfigAlter,
                normalized.Changes.Count),
            proposed);
    }

    public static DynamicConfigTarget NormalizeTarget(DynamicConfigTarget target)
    {
        ArgumentNullException.ThrowIfNull(target);
        if (!Enum.IsDefined(target.Kind))
        {
            throw new ArgumentOutOfRangeException(
                nameof(target),
                "Dynamic configuration target kind is unsupported.");
        }

        var clusterId = DynamicConfigRegistry.RequireBoundedExact(
            target.ClusterId,
            "Dynamic configuration physical cluster ID",
            256);
        return target.Kind switch
        {
            DynamicConfigTargetKind.ClusterDefault when target.BrokerId is null =>
                target with { ClusterId = clusterId },
            DynamicConfigTargetKind.ClusterDefault =>
                throw new ArgumentException(
                    "Cluster-default configuration must not carry a broker ID.",
                    nameof(target)),
            DynamicConfigTargetKind.Broker when !string.IsNullOrWhiteSpace(target.BrokerId) =>
                target with
                {
                    ClusterId = clusterId,
                    BrokerId = DynamicConfigRegistry.RequireBoundedExact(
                        target.BrokerId!,
                        "Dynamic configuration broker ID",
                        128),
                },
            _ => throw new ArgumentException(
                "Broker configuration requires one exact broker ID.",
                nameof(target)),
        };
    }

    private static DynamicConfigMutationPlan RevalidatePlan(
        DynamicConfigMutationPlan plan,
        DynamicConfigRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var requested = plan.Changes
            .Select(change => new DynamicConfigChange(
                change.Key,
                change.Mode,
                change.Value))
            .ToArray();
        var normalized = CreatePlan(
            plan.Target,
            requested,
            plan.ObservedFingerprint,
            registry);

        for (var index = 0; index < normalized.Changes.Count; index++)
        {
            var actual = normalized.Changes[index];
            var supplied = plan.Changes
                .OrderBy(change => change.Key, StringComparer.Ordinal)
                .ElementAt(index);
            if (actual.SafetyClass != supplied.SafetyClass ||
                !string.Equals(
                    actual.ProviderEvidenceId,
                    supplied.ProviderEvidenceId,
                    StringComparison.Ordinal) ||
                !actual.SupportedBrokerVersions.SequenceEqual(
                    supplied.SupportedBrokerVersions,
                    StringComparer.Ordinal))
            {
                throw new MutationStateException(
                    "Dynamic configuration plan no longer matches the current server-owned registry evidence.");
            }
        }

        return normalized;
    }

    private static DynamicConfigPlannedChange NormalizeChange(
        DynamicConfigTarget target,
        DynamicConfigChange change,
        DynamicConfigRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(change);
        if (!Enum.IsDefined(change.Mode))
        {
            throw new ArgumentOutOfRangeException(
                nameof(change),
                "Dynamic configuration mutation mode is unsupported.");
        }

        var definition = registry.Require(change.Key);
        var targetAllowed = target.Kind switch
        {
            DynamicConfigTargetKind.ClusterDefault =>
                definition.UpdateMode is
                    DynamicConfigUpdateMode.ClusterWide or
                    DynamicConfigUpdateMode.Both,
            DynamicConfigTargetKind.Broker =>
                definition.UpdateMode is
                    DynamicConfigUpdateMode.PerBroker or
                    DynamicConfigUpdateMode.Both,
            _ => false,
        };
        if (!targetAllowed)
        {
            throw new MutationStateException(
                $"Dynamic configuration key '{definition.Key}' is not admitted for target kind '{target.Kind}'.");
        }

        var value = change.Mode switch
        {
            DynamicConfigMutationMode.Reset when change.Value is null => null,
            DynamicConfigMutationMode.Reset =>
                throw new ArgumentException(
                    "Dynamic configuration reset must not carry a value.",
                    nameof(change)),
            DynamicConfigMutationMode.Set when change.Value is not null =>
                ValidateValue(definition, change.Value),
            DynamicConfigMutationMode.Set =>
                throw new ArgumentException(
                    "Dynamic configuration set requires one explicit value.",
                    nameof(change)),
            _ => throw new ArgumentOutOfRangeException(nameof(change)),
        };

        return new DynamicConfigPlannedChange(
            definition.Key,
            change.Mode,
            value,
            definition.SafetyClass,
            definition.ProviderEvidenceId,
            definition.SupportedBrokerVersions);
    }

    private static string ValidateValue(
        DynamicConfigDefinition definition,
        string value)
    {
        var normalized = DynamicConfigRegistry.RequireBoundedExact(
            value,
            $"Dynamic configuration value for '{definition.Key}'",
            1024);

        var valid = definition.ValueKind switch
        {
            DynamicConfigValueKind.Int32 =>
                int.TryParse(
                    normalized,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out _),
            DynamicConfigValueKind.Int64 =>
                long.TryParse(
                    normalized,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out _),
            DynamicConfigValueKind.Double =>
                double.TryParse(
                    normalized,
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture,
                    out var parsedDouble) &&
                double.IsFinite(parsedDouble),
            DynamicConfigValueKind.Boolean =>
                bool.TryParse(normalized, out _),
            DynamicConfigValueKind.String => true,
            _ => false,
        };

        if (!valid)
        {
            throw new ArgumentException(
                $"Dynamic configuration value for '{definition.Key}' does not match its server-owned typed value contract.",
                nameof(value));
        }

        return normalized;
    }

    private static string ResourceKey(
        DynamicConfigTarget target,
        string key)
    {
        var brokerIdentity = target.Kind == DynamicConfigTargetKind.ClusterDefault
            ? "__cluster_default__"
            : target.BrokerId!;

        return FleetConflictKeyCodec.BrokerConfiguration(
            target.ClusterId,
            brokerIdentity,
            key);
    }

    private static string RequireSha256(string value, string fieldName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length != 64 || value.Any(character => !char.IsAsciiHexDigit(character)))
        {
            throw new ArgumentException(
                $"{fieldName} must be one full SHA-256 fingerprint.",
                fieldName);
        }

        return value.ToLowerInvariant();
    }
}
