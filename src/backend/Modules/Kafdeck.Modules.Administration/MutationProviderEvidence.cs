using System.Collections.ObjectModel;

namespace Kafdeck.Modules.Administration;

public static class MutationProviderEvidence
{
    private static readonly string[] ForbiddenKeyTokens =
    [
        "secret",
        "password",
        "passwd",
        "token",
        "cookie",
        "credential",
        "authorization",
        "clientsecret",
        "privatekey",
        "saslpassword",
    ];

    public static IReadOnlyDictionary<string, string> Normalize(
        IReadOnlyDictionary<string, string>? evidence)
    {
        if (evidence is null || evidence.Count == 0)
        {
            return new ReadOnlyDictionary<string, string>(
                new Dictionary<string, string>(StringComparer.Ordinal));
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
            var key = RequireKey(pair.Key);
            var value = RequireValue(pair.Value, key);

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

    private static string RequireKey(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();

        if (normalized.Length > MutationLimits.MaxProviderEvidenceKeyCharacters ||
            normalized.Any(char.IsControl) ||
            normalized.Any(character =>
                !(char.IsLetterOrDigit(character) ||
                  character is '.' or '_' or '-' or ':')))
        {
            throw new MutationStateException(
                "Provider evidence keys must be bounded canonical identifiers.");
        }

        var compact = normalized
            .Replace(".", string.Empty, StringComparison.Ordinal)
            .Replace("_", string.Empty, StringComparison.Ordinal)
            .Replace("-", string.Empty, StringComparison.Ordinal)
            .Replace(":", string.Empty, StringComparison.Ordinal);

        if (ForbiddenKeyTokens.Any(token =>
                compact.Contains(token, StringComparison.OrdinalIgnoreCase)))
        {
            throw new MutationStateException(
                $"Provider evidence key '{normalized}' is not permitted because it is secret-like.");
        }

        return normalized;
    }

    private static string RequireValue(string value, string key)
    {
        ArgumentNullException.ThrowIfNull(value);
        var normalized = value.Trim();

        if (normalized.Length > MutationLimits.MaxProviderEvidenceValueCharacters ||
            normalized.Any(char.IsControl))
        {
            throw new MutationStateException(
                $"Provider evidence value for '{key}' exceeds the bounded safe projection contract.");
        }

        return normalized;
    }
}
