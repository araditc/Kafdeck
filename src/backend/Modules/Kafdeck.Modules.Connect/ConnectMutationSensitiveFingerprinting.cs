using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

internal static class ConnectMutationSensitiveFingerprinting
{
    private const string Domain = "Kafdeck.Connect.ConfigurationValue.v2";

    public static IReadOnlyList<ConnectConfigurationCanonicalItem> ProjectRequested(
        IReadOnlyDictionary<string, string> configuration,
        IMutationMaterialDigestService digest)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(digest);

        var projected = ConnectMutationCanonicalization.ProjectConfiguration(configuration);
        var secured = projected
            .Select(item => RequiresKeyedFingerprint(item.Key)
                ? item with
                {
                    ValueSha256 = KeyedFingerprintFromRawSha256(
                        item.Key,
                        ConnectMutationCanonicalization.Sha256(
                            Encoding.UTF8.GetBytes(configuration[item.Key])),
                        digest),
                }
                : item)
            .ToArray();

        return Array.AsReadOnly(secured);
    }

    public static ConnectMutationObservation ProtectObservation(
        ConnectMutationObservation observation,
        IMutationMaterialDigestService digest)
    {
        ArgumentNullException.ThrowIfNull(observation);
        ArgumentNullException.ThrowIfNull(digest);

        if (!observation.Exists || observation.Configuration.Count == 0)
        {
            return observation;
        }

        var secured = observation.Configuration
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .Select(item => RequiresKeyedFingerprint(item.Key)
                ? item with
                {
                    ValueSha256 = KeyedFingerprintFromRawSha256(
                        item.Key,
                        item.ValueSha256,
                        digest),
                }
                : item)
            .ToArray();

        var fingerprint = ConnectMutationCanonicalization.ConfigurationFingerprint(
            secured.Select(item => new ConnectConfigurationCanonicalItem(
                item.Key,
                item.ValueSha256,
                item.SafeValue)));

        return observation with
        {
            Configuration = Array.AsReadOnly(secured),
            ConfigurationFingerprint = fingerprint,
        };
    }

    private static bool RequiresKeyedFingerprint(string key) =>
        !ConnectSafeConfigurationPolicy.IsExplicitlySafeConfigKey(key) ||
        ConnectSafeConfigurationPolicy.IsSecretKey(key);

    private static string KeyedFingerprintFromRawSha256(
        string key,
        string rawSha256,
        IMutationMaterialDigestService digest)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(rawSha256);

        var bytes = Encoding.UTF8.GetBytes(
            $"{Domain}\n{key}\n{rawSha256}");
        try
        {
            return digest.ComputeDigest(bytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(bytes);
        }
    }
}
