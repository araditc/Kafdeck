using System.Collections.ObjectModel;
using System.Globalization;

namespace Kafdeck.Modules.Administration;

public static class MutationProviderEvidence
{
    private enum EvidenceValueKind
    {
        NonNegativeInt64 = 1,
        PositiveInt32 = 2,
        Boolean = 3,
        VerificationState = 4,
        Sha256 = 5,
    }

    private static readonly IReadOnlyDictionary<string, EvidenceValueKind> AllowedKeys =
        new Dictionary<string, EvidenceValueKind>(StringComparer.Ordinal)
        {
            ["partition"] = EvidenceValueKind.NonNegativeInt64,
            ["offset"] = EvidenceValueKind.NonNegativeInt64,
            ["before.offset"] = EvidenceValueKind.NonNegativeInt64,
            ["low.watermark"] = EvidenceValueKind.NonNegativeInt64,
            ["high.watermark"] = EvidenceValueKind.NonNegativeInt64,
            ["task.id"] = EvidenceValueKind.NonNegativeInt64,
            ["partition.count"] = EvidenceValueKind.PositiveInt32,
            ["replication.factor"] = EvidenceValueKind.PositiveInt32,
            ["record.count"] = EvidenceValueKind.PositiveInt32,
            ["acknowledged.count"] = EvidenceValueKind.NonNegativeInt64,
            ["failure.ordinal"] = EvidenceValueKind.NonNegativeInt64,
            ["schema.version"] = EvidenceValueKind.PositiveInt32,
            ["provider.accepted"] = EvidenceValueKind.Boolean,
            ["resource.exists"] = EvidenceValueKind.Boolean,
            ["compatibility.valid"] = EvidenceValueKind.Boolean,
            ["verification.state"] = EvidenceValueKind.VerificationState,
            ["resource.fingerprint"] = EvidenceValueKind.Sha256,
            ["configuration.fingerprint"] = EvidenceValueKind.Sha256,
        };

    private static readonly HashSet<string> VerificationStates =
        new(
        [
            "observed",
            "inconclusive",
            "not-observable",
        ],
        StringComparer.Ordinal);

    public static IReadOnlyDictionary<string, string> Normalize(
        IReadOnlyDictionary<string, string>? evidence)
    {
        if (evidence is null || evidence.Count == 0)
        {
            return Empty();
        }

        if (evidence.Count > MutationLimits.MaxProviderEvidenceEntries)
        {
            throw new MutationStateException(
                $"Provider evidence must not exceed {MutationLimits.MaxProviderEvidenceEntries} entries.");
        }

        var normalized = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var totalCharacters = 0;

        foreach (var pair in evidence)
        {
            var key = RequireAllowedKey(pair.Key);
            var value = NormalizeValue(key, pair.Value, AllowedKeys[key]);

            totalCharacters = checked(totalCharacters + key.Length + value.Length);
            if (totalCharacters > MutationLimits.MaxProviderEvidenceTotalCharacters)
            {
                throw new MutationStateException(
                    $"Provider evidence must not exceed {MutationLimits.MaxProviderEvidenceTotalCharacters} total characters.");
            }

            if (!normalized.TryAdd(key, value))
            {
                throw new MutationStateException(
                    $"Provider evidence key '{key}' is duplicated.");
            }
        }

        return new ReadOnlyDictionary<string, string>(
            normalized.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal));
    }

    private static IReadOnlyDictionary<string, string> Empty() =>
        new ReadOnlyDictionary<string, string>(
            new Dictionary<string, string>(StringComparer.Ordinal));

    private static string RequireAllowedKey(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();

        if (normalized.Length > MutationLimits.MaxProviderEvidenceKeyCharacters ||
            normalized.Any(char.IsControl) ||
            !AllowedKeys.ContainsKey(normalized))
        {
            throw new MutationStateException(
                $"Provider evidence key '{normalized}' is not an admitted typed projection.");
        }

        return normalized;
    }

    private static string NormalizeValue(
        string key,
        string value,
        EvidenceValueKind kind)
    {
        ArgumentNullException.ThrowIfNull(value);
        var normalized = value.Trim();

        if (normalized.Length > MutationLimits.MaxProviderEvidenceValueCharacters ||
            normalized.Any(char.IsControl))
        {
            throw new MutationStateException(
                $"Provider evidence value for '{key}' exceeds the bounded safe projection contract.");
        }

        return kind switch
        {
            EvidenceValueKind.NonNegativeInt64 =>
                NormalizeNonNegativeInt64(key, normalized),
            EvidenceValueKind.PositiveInt32 =>
                NormalizePositiveInt32(key, normalized),
            EvidenceValueKind.Boolean =>
                NormalizeBoolean(key, normalized),
            EvidenceValueKind.VerificationState =>
                NormalizeVerificationState(key, normalized),
            EvidenceValueKind.Sha256 =>
                NormalizeSha256(key, normalized),
            _ => throw new MutationStateException(
                $"Provider evidence key '{key}' has an unsupported value contract."),
        };
    }

    private static string NormalizeNonNegativeInt64(string key, string value)
    {
        if (!long.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed < 0)
        {
            throw InvalidValue(key);
        }

        return parsed.ToString(CultureInfo.InvariantCulture);
    }

    private static string NormalizePositiveInt32(string key, string value)
    {
        if (!int.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed <= 0)
        {
            throw InvalidValue(key);
        }

        return parsed.ToString(CultureInfo.InvariantCulture);
    }

    private static string NormalizeBoolean(string key, string value)
    {
        if (!bool.TryParse(value, out var parsed))
        {
            throw InvalidValue(key);
        }

        return parsed ? "true" : "false";
    }

    private static string NormalizeVerificationState(string key, string value)
    {
        var normalized = value.ToLowerInvariant();
        if (!VerificationStates.Contains(normalized))
        {
            throw InvalidValue(key);
        }

        return normalized;
    }

    private static string NormalizeSha256(string key, string value)
    {
        var normalized = value.ToLowerInvariant();
        if (normalized.Length != 64 ||
            normalized.Any(character =>
                !char.IsAsciiHexDigit(character)))
        {
            throw InvalidValue(key);
        }

        return normalized;
    }

    private static MutationStateException InvalidValue(string key) =>
        new($"Provider evidence value for '{key}' does not match its admitted typed projection.");
}
