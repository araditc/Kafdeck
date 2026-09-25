using System.Globalization;
using System.Text;

namespace Kafdeck.Modules.Administration;

public enum KafkaScramMechanism
{
    ScramSha256 = 1,
    ScramSha512 = 2,
}

/// <summary>
/// Safe provider-observable SCRAM metadata. Credential material, salts and
/// verifiers are deliberately absent from this contract.
/// </summary>
public sealed record KafkaScramCredentialMetadata(
    string User,
    KafkaScramMechanism Mechanism,
    int Iterations);

public static class ScramCredentialPolicy
{
    public const int MaxUserCharacters = 256;
    public const int MaxCredentialsPerUser = 2;

    public static string NormalizeUser(string user)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(user);
        if (user.Length > MaxUserCharacters ||
            user.Any(char.IsControl) ||
            !string.Equals(user, user.Trim(), StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "SCRAM user must be an exact bounded identifier without surrounding whitespace or control characters.",
                nameof(user));
        }

        return user;
    }

    public static KafkaScramCredentialMetadata NormalizeMetadata(
        KafkaScramCredentialMetadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        if (!Enum.IsDefined(metadata.Mechanism))
        {
            throw new ArgumentOutOfRangeException(
                nameof(metadata),
                "SCRAM metadata contains an unsupported mechanism.");
        }

        if (metadata.Iterations <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(metadata),
                "SCRAM metadata must contain a positive provider-observed iteration count.");
        }

        return metadata with { User = NormalizeUser(metadata.User) };
    }

    public static IReadOnlyList<KafkaScramCredentialMetadata> NormalizeMetadataSet(
        string user,
        IEnumerable<KafkaScramCredentialMetadata> metadata)
    {
        var normalizedUser = NormalizeUser(user);
        ArgumentNullException.ThrowIfNull(metadata);

        var values = metadata
            .Select(NormalizeMetadata)
            .ToArray();

        if (values.Length > MaxCredentialsPerUser)
        {
            throw new ArgumentOutOfRangeException(
                nameof(metadata),
                $"SCRAM metadata exceeds the admitted {MaxCredentialsPerUser}-mechanism bound per user.");
        }

        if (values.Any(value =>
                !string.Equals(value.User, normalizedUser, StringComparison.Ordinal)))
        {
            throw new ArgumentException(
                "SCRAM metadata must belong to the exact requested user.",
                nameof(metadata));
        }

        var duplicateMechanism = values
            .GroupBy(value => value.Mechanism)
            .Any(group => group.Count() != 1);
        if (duplicateMechanism)
        {
            throw new ArgumentException(
                "SCRAM metadata contains duplicate mechanism entries.",
                nameof(metadata));
        }

        return Array.AsReadOnly(
            values
                .OrderBy(value => value.Mechanism)
                .ToArray());
    }

    public static string ConflictIdentity(
        string physicalClusterId,
        string user,
        KafkaScramMechanism mechanism)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(physicalClusterId);
        if (physicalClusterId.Length > 256 ||
            physicalClusterId.Any(char.IsControl) ||
            !string.Equals(
                physicalClusterId,
                physicalClusterId.Trim(),
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "SCRAM physical cluster identity is invalid.",
                nameof(physicalClusterId));
        }

        var normalizedUser = NormalizeUser(user);
        if (!Enum.IsDefined(mechanism))
        {
            throw new ArgumentOutOfRangeException(nameof(mechanism));
        }

        // Length-prefix the user instead of delimiter-concatenating an
        // untrusted Kafka identity. This value is safe metadata, not a secret.
        var userBytes = Encoding.UTF8.GetBytes(normalizedUser);
        var encodedUser = Convert.ToBase64String(userBytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

        return string.Join(
            '|',
            "scram1",
            physicalClusterId,
            ((int)mechanism).ToString(CultureInfo.InvariantCulture),
            $"{userBytes.Length.ToString(CultureInfo.InvariantCulture)}:{encodedUser}");
    }
}
