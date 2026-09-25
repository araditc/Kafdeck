using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;

namespace Kafdeck.Modules.Administration;

public sealed record ScramCredentialBindingDescriptor(
    string ClusterId,
    string User,
    KafkaScramMechanism Mechanism,
    int Iterations);

/// <summary>
/// Ephemeral credential bytes supplied by the original requester. The material
/// is never part of canonical intent, audit, safe evidence or durable state.
/// </summary>
public sealed class ScramCredentialExecutionMaterial : IDisposable
{
    public const int HardMaxPasswordBytes = 4 * 1024;

    private readonly byte[] _password;
    private bool _disposed;

    public ScramCredentialExecutionMaterial(ReadOnlyMemory<byte> password)
    {
        if (password.Length is < 1 or > HardMaxPasswordBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(password),
                $"SCRAM password material must contain between 1 and {HardMaxPasswordBytes} bytes.");
        }

        _password = password.ToArray();
    }

    [JsonIgnore]
    public ReadOnlyMemory<byte> Password
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _password;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        CryptographicOperations.ZeroMemory(_password);
        _disposed = true;
    }
}

/// <summary>
/// Builds the durable, keyed binding for write-only SCRAM material. The HMAC
/// input is domain-separated and binds exact physical cluster, exact user,
/// mechanism and iteration metadata before password bytes. The resulting
/// digest is internal coordinator state and must never be projected as a
/// requester-visible credential verifier.
/// </summary>
public static class ScramCredentialMaterialBinding
{
    public static string Compute(
        IMutationMaterialDigestService digestService,
        ScramCredentialBindingDescriptor descriptor,
        ReadOnlySpan<byte> password)
    {
        ArgumentNullException.ThrowIfNull(digestService);
        var envelope = ScramCredentialMaterialCodec.Encode(
            descriptor,
            password);
        try
        {
            return digestService.ComputeDigest(envelope);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(envelope);
        }
    }

    public static bool Matches(
        IMutationMaterialDigestService digestService,
        ScramCredentialBindingDescriptor descriptor,
        ReadOnlySpan<byte> password,
        string expectedBinding)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedBinding);
        if (expectedBinding.Length != 64 ||
            expectedBinding.Any(character => !char.IsAsciiHexDigit(character)))
        {
            throw new ArgumentException(
                "SCRAM material binding must be a SHA-256 HMAC hex value.",
                nameof(expectedBinding));
        }

        var actual = Compute(digestService, descriptor, password);
        var expectedBytes = Encoding.ASCII.GetBytes(expectedBinding.ToLowerInvariant());
        var actualBytes = Encoding.ASCII.GetBytes(actual);
        try
        {
            return CryptographicOperations.FixedTimeEquals(
                actualBytes,
                expectedBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(actualBytes);
            CryptographicOperations.ZeroMemory(expectedBytes);
        }
    }

    public static ScramCredentialBindingDescriptor Normalize(
        ScramCredentialBindingDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentException.ThrowIfNullOrWhiteSpace(descriptor.ClusterId);

        if (descriptor.ClusterId.Length > 256 ||
            descriptor.ClusterId.Any(char.IsControl) ||
            !string.Equals(
                descriptor.ClusterId,
                descriptor.ClusterId.Trim(),
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "SCRAM physical cluster identity is invalid.",
                nameof(descriptor));
        }

        if (!Enum.IsDefined(descriptor.Mechanism))
        {
            throw new ArgumentOutOfRangeException(
                nameof(descriptor),
                "SCRAM mechanism is unsupported.");
        }

        if (descriptor.Iterations <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(descriptor),
                "SCRAM iteration count must be positive.");
        }

        return descriptor with
        {
            User = ScramCredentialPolicy.NormalizeUser(descriptor.User),
        };
    }

}
