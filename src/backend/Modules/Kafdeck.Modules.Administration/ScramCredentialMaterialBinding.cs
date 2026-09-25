using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;

namespace Kafdeck.Modules.Administration;

public sealed record ScramCredentialBindingDescriptor(
    string ClusterId,
    string User,
    KafkaScramMechanism Mechanism,
    int Iterations);

public sealed record ScramCredentialMaterialBindingContext(
    Guid OperationId,
    string RequesterPrincipalId,
    string PolicyVersion,
    string DigestKeyId,
    ScramCredentialBindingDescriptor Credential);

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
/// Builds the durable keyed binding for write-only SCRAM material. The input
/// envelope binds operation identity, requester principal, policy/digest-key
/// identity and exact credential target before password bytes.
/// </summary>
public static class ScramCredentialMaterialBinding
{
    public static string Compute(
        IMutationMaterialDigestService digestService,
        ScramCredentialMaterialBindingContext context,
        ReadOnlySpan<byte> password)
    {
        ArgumentNullException.ThrowIfNull(digestService);
        var envelope = ScramCredentialMaterialCodec.Encode(
            context,
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
        ScramCredentialMaterialBindingContext context,
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

        var actual = Compute(digestService, context, password);
        var expectedBytes = Encoding.ASCII.GetBytes(
            expectedBinding.ToLowerInvariant());
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
        var clusterId = RequireExactBounded(
            descriptor.ClusterId,
            "SCRAM physical cluster identity",
            256);

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
            ClusterId = clusterId,
            User = ScramCredentialPolicy.NormalizeUser(descriptor.User),
        };
    }

    public static ScramCredentialMaterialBindingContext Normalize(
        ScramCredentialMaterialBindingContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.OperationId == Guid.Empty)
        {
            throw new ArgumentException(
                "SCRAM material binding requires a non-empty operation ID.",
                nameof(context));
        }

        return context with
        {
            RequesterPrincipalId = RequireExactBounded(
                context.RequesterPrincipalId,
                "SCRAM requester principal",
                4096),
            PolicyVersion = RequireExactBounded(
                context.PolicyVersion,
                "SCRAM policy version",
                256),
            DigestKeyId = RequireExactBounded(
                context.DigestKeyId,
                "SCRAM digest-key ID",
                256),
            Credential = Normalize(context.Credential),
        };
    }

    private static string RequireExactBounded(
        string value,
        string field,
        int maxLength)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length > maxLength ||
            value.Any(char.IsControl) ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"{field} is invalid or exceeds the admitted bound.",
                field);
        }

        return value;
    }
}
