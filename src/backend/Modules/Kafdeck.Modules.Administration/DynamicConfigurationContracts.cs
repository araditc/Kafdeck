using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

public enum DynamicConfigurationScope
{
    ClusterDefault = 1,
    BrokerOverride = 2,
}

public enum KafkaCompatibilityLine
{
    Kafka392 = 1,
    Kafka412 = 2,
    Kafka421 = 3,
    Kafka431 = 4,
}

public sealed record DynamicConfigurationTarget(
    string ClusterId,
    DynamicConfigurationScope Scope,
    int? BrokerId,
    string Key);

public sealed record DynamicConfigurationSynonym(
    string Source,
    string? Value);

public sealed record DynamicConfigurationObservation(
    DynamicConfigurationTarget Target,
    string? EffectiveValue,
    string EffectiveSource,
    bool IsSensitive,
    bool IsReadOnly,
    IReadOnlyList<DynamicConfigurationSynonym> Synonyms);

public sealed record DynamicConfigurationMutation(
    DynamicConfigurationTarget Target,
    string? Value);

public sealed record DynamicConfigurationKeyPolicy(
    string Key,
    bool AvailabilitySensitive,
    bool DurabilitySensitive,
    long MinValue,
    long MaxValue)
{
    public bool IsCritical =>
        AvailabilitySensitive || DurabilitySensitive;

    public string NormalizeValue(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > 128 ||
            value.Any(char.IsControl) ||
            !long.TryParse(
                value,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed < MinValue ||
            parsed > MaxValue)
        {
            throw new DynamicConfigurationPolicyException(
                "Dynamic configuration value is outside the server-owned typed range.");
        }

        return parsed.ToString(CultureInfo.InvariantCulture);
    }
}

/// <summary>
/// Closed server-owned W44 registry. Unknown keys are unavailable rather than
/// being passed through to Kafka. Reassignment throttles, security material,
/// cordon/maintenance settings and static broker settings therefore cannot be
/// smuggled through the ordinary dynamic-config family.
/// </summary>
public sealed class DynamicConfigurationRegistry
{
    private static readonly IReadOnlyDictionary<string, DynamicConfigurationKeyPolicy>
        Common = new Dictionary<string, DynamicConfigurationKeyPolicy>(
            StringComparer.Ordinal)
        {
            ["log.cleaner.threads"] = new(
                "log.cleaner.threads",
                AvailabilitySensitive: false,
                DurabilitySensitive: false,
                MinValue: 1,
                MaxValue: 128),
            ["log.cleaner.backoff.ms"] = new(
                "log.cleaner.backoff.ms",
                AvailabilitySensitive: false,
                DurabilitySensitive: false,
                MinValue: 0,
                MaxValue: 3_600_000),
            ["message.max.bytes"] = new(
                "message.max.bytes",
                AvailabilitySensitive: true,
                DurabilitySensitive: false,
                MinValue: 1_024,
                MaxValue: 104_857_600),
            ["max.connections"] = new(
                "max.connections",
                AvailabilitySensitive: true,
                DurabilitySensitive: false,
                MinValue: 1,
                MaxValue: int.MaxValue),
        };

    private static readonly IReadOnlyDictionary<KafkaCompatibilityLine, IReadOnlySet<string>>
        KeysByVersion =
            Enum.GetValues<KafkaCompatibilityLine>()
                .ToDictionary(
                    version => version,
                    _ => (IReadOnlySet<string>)Common.Keys.ToHashSet(StringComparer.Ordinal));

    public DynamicConfigurationKeyPolicy Require(
        KafkaCompatibilityLine compatibility,
        string key)
    {
        if (!Enum.IsDefined(compatibility))
        {
            throw new DynamicConfigurationPolicyException(
                "Kafka compatibility line is not admitted by W44.");
        }

        var normalizedKey = DynamicConfigurationPolicy.NormalizeKey(key);
        if (!KeysByVersion.TryGetValue(compatibility, out var keys) ||
            !keys.Contains(normalizedKey) ||
            !Common.TryGetValue(normalizedKey, out var policy))
        {
            throw new DynamicConfigurationPolicyException(
                "Configuration key is not in the W44 per-version dynamic allowlist.");
        }

        return policy;
    }

    public bool IsAdmitted(
        KafkaCompatibilityLine compatibility,
        string key)
    {
        try
        {
            _ = Require(compatibility, key);
            return true;
        }
        catch (DynamicConfigurationPolicyException)
        {
            return false;
        }
    }
}

public sealed class DynamicConfigurationPolicyException : ArgumentException
{
    public DynamicConfigurationPolicyException(string message)
        : base(message)
    {
    }
}

public sealed record DynamicConfigurationPlan(
    KafkaCompatibilityLine Compatibility,
    DynamicConfigurationMutation Mutation,
    string ObservedFingerprint,
    string? InheritedValue,
    string? InheritedSource);

public static class DynamicConfigurationPolicy
{
    private const string DynamicBrokerSource = "DynamicBrokerConfig";
    private const string DynamicDefaultSource = "DynamicDefaultBrokerConfig";

    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        WriteIndented = false,
    };

    public static DynamicConfigurationTarget NormalizeTarget(
        DynamicConfigurationTarget target)
    {
        ArgumentNullException.ThrowIfNull(target);
        var clusterId = RequireExact(target.ClusterId, "Cluster ID", 256);
        var key = NormalizeKey(target.Key);

        if (!Enum.IsDefined(target.Scope))
        {
            throw new DynamicConfigurationPolicyException(
                "Dynamic configuration scope is unsupported.");
        }

        return target.Scope switch
        {
            DynamicConfigurationScope.ClusterDefault
                when target.BrokerId is null =>
                target with { ClusterId = clusterId, Key = key },

            DynamicConfigurationScope.BrokerOverride
                when target.BrokerId is >= 0 =>
                target with { ClusterId = clusterId, Key = key },

            _ => throw new DynamicConfigurationPolicyException(
                "Cluster-default targets must not bind a broker ID and broker overrides must bind one non-negative broker ID."),
        };
    }

    public static string NormalizeKey(string key) =>
        RequireExact(key, "Configuration key", 256);

    public static DynamicConfigurationObservation NormalizeObservation(
        DynamicConfigurationObservation observation)
    {
        ArgumentNullException.ThrowIfNull(observation);
        var target = NormalizeTarget(observation.Target);
        var source = RequireExact(
            observation.EffectiveSource,
            "Configuration source",
            128);

        if (observation.EffectiveValue is { Length: > 4096 } ||
            observation.EffectiveValue?.Any(char.IsControl) == true)
        {
            throw new DynamicConfigurationPolicyException(
                "Observed configuration value exceeds the safe W44 preview bound.");
        }

        ArgumentNullException.ThrowIfNull(observation.Synonyms);
        if (observation.Synonyms.Count > 32)
        {
            throw new DynamicConfigurationPolicyException(
                "Observed configuration synonyms exceed the W44 bound.");
        }

        var synonyms = observation.Synonyms
            .Select(item =>
            {
                ArgumentNullException.ThrowIfNull(item);
                var synonymSource = RequireExact(
                    item.Source,
                    "Configuration synonym source",
                    128);
                if (item.Value is { Length: > 4096 } ||
                    item.Value?.Any(char.IsControl) == true)
                {
                    throw new DynamicConfigurationPolicyException(
                        "Observed configuration synonym exceeds the safe W44 bound.");
                }

                return item with { Source = synonymSource };
            })
            .ToArray();

        return observation with
        {
            Target = target,
            EffectiveSource = source,
            Synonyms = Array.AsReadOnly(synonyms),
        };
    }

    public static string Fingerprint(
        DynamicConfigurationObservation observation)
    {
        var normalized = NormalizeObservation(observation);
        var builder = new StringBuilder();
        Append(builder, "cluster", normalized.Target.ClusterId);
        Append(builder, "scope", ((int)normalized.Target.Scope).ToString(CultureInfo.InvariantCulture));
        Append(builder, "broker", normalized.Target.BrokerId?.ToString(CultureInfo.InvariantCulture) ?? string.Empty);
        Append(builder, "key", normalized.Target.Key);
        Append(builder, "value", normalized.EffectiveValue ?? "<null>");
        Append(builder, "source", normalized.EffectiveSource);
        Append(builder, "sensitive", normalized.IsSensitive ? "1" : "0");
        Append(builder, "read-only", normalized.IsReadOnly ? "1" : "0");

        for (var index = 0; index < normalized.Synonyms.Count; index++)
        {
            Append(builder, $"synonym[{index}].source", normalized.Synonyms[index].Source);
            Append(builder, $"synonym[{index}].value", normalized.Synonyms[index].Value ?? "<null>");
        }

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    public static DynamicConfigurationSynonym RequireInheritedValue(
        DynamicConfigurationObservation observation)
    {
        var normalized = NormalizeObservation(observation);
        var requiredCurrentSource = normalized.Target.Scope switch
        {
            DynamicConfigurationScope.ClusterDefault => DynamicDefaultSource,
            DynamicConfigurationScope.BrokerOverride => DynamicBrokerSource,
            _ => throw new DynamicConfigurationPolicyException(
                "Dynamic configuration scope is unsupported."),
        };

        if (!string.Equals(
                normalized.EffectiveSource,
                requiredCurrentSource,
                StringComparison.Ordinal))
        {
            throw new DynamicConfigurationPolicyException(
                "Reset requires an observed dynamic override at the exact requested scope.");
        }

        var inherited = normalized.Synonyms
            .FirstOrDefault(item =>
                !string.Equals(
                    item.Source,
                    requiredCurrentSource,
                    StringComparison.Ordinal) &&
                item.Value is not null);

        return inherited ??
               throw new DynamicConfigurationPolicyException(
                   "Reset is unavailable because the inherited value/source is not safely observable.");
    }

    public static string ExpectedDynamicSource(
        DynamicConfigurationScope scope) =>
        scope switch
        {
            DynamicConfigurationScope.ClusterDefault => DynamicDefaultSource,
            DynamicConfigurationScope.BrokerOverride => DynamicBrokerSource,
            _ => throw new DynamicConfigurationPolicyException(
                "Dynamic configuration scope is unsupported."),
        };

    public static MutationIntentDescriptor BuildIntent(
        DynamicConfigurationPlan plan,
        DynamicConfigurationKeyPolicy keyPolicy)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(keyPolicy);

        var target = NormalizeTarget(plan.Mutation.Target);
        if (!string.Equals(target.Key, keyPolicy.Key, StringComparison.Ordinal))
        {
            throw new DynamicConfigurationPolicyException(
                "Dynamic configuration key policy does not match the exact target.");
        }

        var resource = ConflictResource(target);
        var canonical = JsonSerializer.Serialize(
            plan with
            {
                Mutation = plan.Mutation with { Target = target },
            },
            CanonicalJson);

        return new MutationIntentDescriptor(
            MutationOperationKind.ClusterConfigAlter,
            target.ClusterId,
            canonical,
            new[] { resource },
            new[]
            {
                new MutationPrecondition(
                    "cluster.config",
                    plan.ObservedFingerprint),
            },
            AuthorizationTargets: new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.ClusterConfigRead,
                    target.ClusterId,
                    resource),
                new MutationAuthorizationTarget(
                    AuthorizationAction.ClusterConfigAlter,
                    target.ClusterId,
                    resource),
            });
    }

    public static MutationRiskDecision ClassifyRisk(
        DynamicConfigurationKeyPolicy keyPolicy)
    {
        ArgumentNullException.ThrowIfNull(keyPolicy);
        var input = new MutationRiskInput(
            MutationOperationKind.ClusterConfigAlter,
            TargetCount: 1);
        var floor = MutationRiskClassifier.Classify(input);
        if (!keyPolicy.IsCritical)
        {
            return floor;
        }

        return MutationRiskClassifier.EnforceBuiltInFloor(
            input,
            new MutationRiskDecision(
                MutationRiskClass.Critical,
                Array.AsReadOnly(new[]
                {
                    "dynamic_config_sensitive_operational_key",
                }),
                MutationConfirmationMode.TypedTarget,
                RequiresIndependentApproval: true));
    }

    public static DynamicConfigurationPlan DeserializePlan(
        string canonicalIntent)
    {
        try
        {
            var plan = JsonSerializer.Deserialize<DynamicConfigurationPlan>(
                           canonicalIntent,
                           CanonicalJson) ??
                       throw new DynamicConfigurationPolicyException(
                           "Dynamic configuration canonical intent is invalid.");
            if (!Enum.IsDefined(plan.Compatibility))
            {
                throw new DynamicConfigurationPolicyException(
                    "Dynamic configuration compatibility line is invalid.");
            }

            return plan with
            {
                Mutation = plan.Mutation with
                {
                    Target = NormalizeTarget(plan.Mutation.Target),
                },
            };
        }
        catch (JsonException)
        {
            throw new DynamicConfigurationPolicyException(
                "Dynamic configuration canonical intent is invalid.");
        }
    }

    public static string ConflictResource(
        DynamicConfigurationTarget target)
    {
        var normalized = NormalizeTarget(target);
        var brokerIdentity = normalized.Scope == DynamicConfigurationScope.ClusterDefault
            ? "default"
            : $"broker:{normalized.BrokerId!.Value.ToString(CultureInfo.InvariantCulture)}";

        return FleetConflictKeyCodec.BrokerConfiguration(
            normalized.ClusterId,
            brokerIdentity,
            normalized.Key);
    }

    private static string RequireExact(
        string value,
        string field,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (!string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new DynamicConfigurationPolicyException(
                $"{field} is invalid or exceeds the W44 bound.");
        }

        return value;
    }

    private static void Append(
        StringBuilder builder,
        string key,
        string value) =>
        builder
            .Append(key)
            .Append('=')
            .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(value)))
            .Append('\n');
}
