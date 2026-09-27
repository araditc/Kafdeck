using System.Security.Cryptography;
using System.Text;

namespace Kafdeck.Infrastructure.Configuration;

public static class KafkaConnectProfileIdentityFingerprint
{
    public static string Compute(
        ClusterProfile cluster,
        string connectProfileId)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectProfileId);

        var profile = KafkaConnectProfileSet
            .Effective(cluster)
            .SingleOrDefault(item =>
                string.Equals(
                    item.Id,
                    connectProfileId,
                    StringComparison.Ordinal))
            ?? throw new KafdeckConfigurationException(
                $"Kafka Connect profile '{connectProfileId}' is not configured for cluster '{cluster.Id}'.");

        if (!Uri.TryCreate(
                profile.Url,
                UriKind.Absolute,
                out var uri))
        {
            throw new KafdeckConfigurationException(
                "Kafka Connect profile URL is invalid.");
        }

        var canonicalEndpoint = string.Join(
            "|",
            uri.Scheme.ToLowerInvariant(),
            uri.IdnHost.ToLowerInvariant(),
            uri.Port.ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            uri.AbsolutePath.TrimEnd('/'));

        var canonical = string.Join(
            "\n",
            cluster.Id,
            profile.Id,
            canonicalEndpoint,
            ((int)profile.MutationProviderProfile).ToString(
                System.Globalization.CultureInfo.InvariantCulture),
            SecretIdentity(profile.Username),
            SecretIdentity(profile.Password));

        return Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(canonical)))
            .ToLowerInvariant();
    }

    private static string SecretIdentity(
        SecretReference? reference)
    {
        if (reference is null)
        {
            return "none";
        }

        var locatorHash = Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(
                        reference.Locator)))
            .ToLowerInvariant();

        return $"{(int)reference.Kind}:{locatorHash}";
    }
}
