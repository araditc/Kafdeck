using System.Security.Cryptography;
using System.Text;

namespace Kafdeck.Modules.Administration;

public static class MutationPreviewHasher
{
    public static string ComputeHash(
        MutationIntentDescriptor intent,
        MutationRiskDecision risk,
        string policyVersion,
        DateTimeOffset expiresAtUtc)
    {
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentNullException.ThrowIfNull(risk);
        ArgumentException.ThrowIfNullOrWhiteSpace(policyVersion);

        var builder = new StringBuilder(1024);
        Append(builder, "kind", ((int)intent.Kind).ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(builder, "cluster", intent.ClusterId);
        Append(builder, "intent", MutationIdempotency.HashCanonicalIntent(intent.CanonicalIntent));

        foreach (var resource in NormalizeResources(intent.ResourceKeys))
        {
            Append(builder, "resource", resource);
        }

        foreach (var precondition in NormalizePreconditions(intent.Preconditions))
        {
            Append(builder, "precondition-key", precondition.Key);
            Append(builder, "precondition-fingerprint", precondition.Fingerprint);
        }

        foreach (var digest in NormalizeDigests(intent.MaterialDigests))
        {
            Append(builder, "material-name", digest.Name);
            Append(builder, "material-digest", digest.Digest);
        }

        Append(builder, "risk", ((int)risk.RiskClass).ToString(System.Globalization.CultureInfo.InvariantCulture));
        foreach (var reason in risk.Reasons
                     .Select(value => RequireBounded(value, "Risk reason", 512))
                     .Distinct(StringComparer.Ordinal)
                     .OrderBy(value => value, StringComparer.Ordinal))
        {
            Append(builder, "risk-reason", reason);
        }

        Append(builder, "confirmation", ((int)risk.ConfirmationMode).ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(builder, "independent-approval", risk.RequiresIndependentApproval ? "1" : "0");
        Append(builder, "policy", policyVersion);
        Append(builder, "expires", expiresAtUtc.ToUniversalTime().ToString("O", System.Globalization.CultureInfo.InvariantCulture));

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    internal static IReadOnlyList<string> NormalizeResources(IReadOnlyList<string> resources)
    {
        ArgumentNullException.ThrowIfNull(resources);
        if (resources.Count == 0)
        {
            throw new ArgumentException("Mutation must target at least one concrete resource.", nameof(resources));
        }

        if (resources.Count > MutationLimits.MaxResourceKeys)
        {
            throw new ArgumentOutOfRangeException(
                nameof(resources),
                $"Mutation must not target more than {MutationLimits.MaxResourceKeys} resource keys.");
        }

        var normalized = resources
            .Select(resource => RequireBounded(resource, "Resource key", 1024))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(resource => resource, StringComparer.Ordinal)
            .ToArray();

        return Array.AsReadOnly(normalized);
    }

    internal static IReadOnlyList<MutationPrecondition> NormalizePreconditions(
        IReadOnlyList<MutationPrecondition>? preconditions)
    {
        var items = preconditions ?? Array.Empty<MutationPrecondition>();
        if (items.Count > MutationLimits.MaxPreconditions)
        {
            throw new ArgumentOutOfRangeException(
                nameof(preconditions),
                $"Mutation must not contain more than {MutationLimits.MaxPreconditions} preconditions.");
        }

        var normalized = items
            .Select(item => new MutationPrecondition(
                RequireBounded(item.Key, "Precondition key", 512),
                RequireBounded(item.Fingerprint, "Precondition fingerprint", 1024)))
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .ThenBy(item => item.Fingerprint, StringComparer.Ordinal)
            .ToArray();

        return Array.AsReadOnly(normalized);
    }

    internal static IReadOnlyList<MutationMaterialDigest> NormalizeDigests(
        IReadOnlyList<MutationMaterialDigest>? digests)
    {
        var items = digests ?? Array.Empty<MutationMaterialDigest>();
        if (items.Count > MutationLimits.MaxMaterialDigests)
        {
            throw new ArgumentOutOfRangeException(
                nameof(digests),
                $"Mutation must not contain more than {MutationLimits.MaxMaterialDigests} material digests.");
        }

        var normalized = items
            .Select(item => new MutationMaterialDigest(
                RequireBounded(item.Name, "Material digest name", 256),
                RequireBounded(item.Digest, "Material digest", 512)))
            .OrderBy(item => item.Name, StringComparer.Ordinal)
            .ThenBy(item => item.Digest, StringComparer.Ordinal)
            .ToArray();

        var duplicateName = normalized
            .GroupBy(item => item.Name, StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicateName is not null)
        {
            throw new ArgumentException(
                $"Material digest name '{duplicateName.Key}' is duplicated.",
                nameof(digests));
        }

        return Array.AsReadOnly(normalized);
    }

    private static void Append(StringBuilder builder, string key, string value)
    {
        var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(value));
        builder.Append(key).Append('=').Append(encoded).Append('\n');
    }

    private static string RequireBounded(string value, string field, int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (normalized.Length > maxLength || normalized.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(field, $"{field} must be at most {maxLength} characters and contain no control characters.");
        }

        return normalized;
    }
}
