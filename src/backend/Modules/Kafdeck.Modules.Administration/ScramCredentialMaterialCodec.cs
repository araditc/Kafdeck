using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Strict in-memory envelope for one SCRAM upsert finalization. The envelope is
/// request-scoped execution material: it may be HMAC-bound by the existing
/// mutation kernel, but it must never be persisted, logged, audited or exposed
/// as safe provider evidence.
/// </summary>
public static class ScramCredentialMaterialCodec
{
    private static readonly byte[] Domain =
        Encoding.UTF8.GetBytes("kafdeck:v0.6:scram-execution:v1");

    private const int HardMaxEnvelopeBytes =
        ScramCredentialExecutionMaterial.HardMaxPasswordBytes +
        (256 * 4) +
        (ScramCredentialPolicy.MaxUserCharacters * 4) +
        1024;

    public static byte[] Encode(
        ScramCredentialBindingDescriptor descriptor,
        ReadOnlySpan<byte> password)
    {
        var normalized = ScramCredentialMaterialBinding.Normalize(descriptor);
        ValidatePassword(password);

        var cluster = Encoding.UTF8.GetBytes(normalized.ClusterId);
        var user = Encoding.UTF8.GetBytes(normalized.User);
        var envelope = new byte[checked(
            sizeof(int) + Domain.Length +
            sizeof(int) + cluster.Length +
            sizeof(int) + user.Length +
            sizeof(int) +
            sizeof(int) +
            sizeof(int) + password.Length)];

        try
        {
            var position = 0;
            WriteBytes(envelope, ref position, Domain);
            WriteBytes(envelope, ref position, cluster);
            WriteBytes(envelope, ref position, user);
            WriteInt32(envelope, ref position, (int)normalized.Mechanism);
            WriteInt32(envelope, ref position, normalized.Iterations);
            WriteBytes(envelope, ref position, password);

            if (position != envelope.Length)
            {
                throw new MutationStateException(
                    "SCRAM execution material envelope length is inconsistent.");
            }

            return envelope;
        }
        catch
        {
            CryptographicOperations.ZeroMemory(envelope);
            throw;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(cluster);
            CryptographicOperations.ZeroMemory(user);
        }
    }

    public static ScramDecodedCredentialMaterial Decode(
        ReadOnlyMemory<byte> envelope)
    {
        if (envelope.Length == 0 ||
            envelope.Length > HardMaxEnvelopeBytes)
        {
            throw Invalid();
        }

        var span = envelope.Span;
        var position = 0;

        var domain = ReadBytes(span, ref position, Domain.Length);
        if (!domain.SequenceEqual(Domain))
        {
            throw Invalid();
        }

        var clusterBytes = ReadBytes(span, ref position, 256 * 4);
        var userBytes = ReadBytes(
            span,
            ref position,
            ScramCredentialPolicy.MaxUserCharacters * 4);
        var mechanismValue = ReadInt32(span, ref position);
        var iterations = ReadInt32(span, ref position);
        var password = ReadBytes(
                span,
                ref position,
                ScramCredentialExecutionMaterial.HardMaxPasswordBytes)
            .ToArray();

        try
        {
            if (position != span.Length)
            {
                throw Invalid();
            }

            var descriptor = ScramCredentialMaterialBinding.Normalize(
                new ScramCredentialBindingDescriptor(
                    DecodeUtf8(clusterBytes),
                    DecodeUtf8(userBytes),
                    (KafkaScramMechanism)mechanismValue,
                    iterations));

            ValidatePassword(password);
            return new ScramDecodedCredentialMaterial(descriptor, password);
        }
        catch
        {
            CryptographicOperations.ZeroMemory(password);
            throw;
        }
    }

    private static string DecodeUtf8(ReadOnlySpan<byte> value)
    {
        try
        {
            return new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false,
                    throwOnInvalidBytes: true)
                .GetString(value);
        }
        catch (DecoderFallbackException)
        {
            throw Invalid();
        }
    }

    private static void ValidatePassword(ReadOnlySpan<byte> password)
    {
        if (password.Length is < 1 or >
            ScramCredentialExecutionMaterial.HardMaxPasswordBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(password),
                $"SCRAM password material must contain between 1 and {ScramCredentialExecutionMaterial.HardMaxPasswordBytes} bytes.");
        }
    }

    private static void WriteBytes(
        Span<byte> destination,
        ref int position,
        ReadOnlySpan<byte> value)
    {
        WriteInt32(destination, ref position, value.Length);
        value.CopyTo(destination[position..]);
        position += value.Length;
    }

    private static void WriteInt32(
        Span<byte> destination,
        ref int position,
        int value)
    {
        if (destination.Length - position < sizeof(int))
        {
            throw Invalid();
        }

        BinaryPrimitives.WriteInt32BigEndian(
            destination.Slice(position, sizeof(int)),
            value);
        position += sizeof(int);
    }

    private static ReadOnlySpan<byte> ReadBytes(
        ReadOnlySpan<byte> source,
        ref int position,
        int maxLength)
    {
        var length = ReadInt32(source, ref position);
        if (length < 0 ||
            length > maxLength ||
            source.Length - position < length)
        {
            throw Invalid();
        }

        var result = source.Slice(position, length);
        position += length;
        return result;
    }

    private static int ReadInt32(
        ReadOnlySpan<byte> source,
        ref int position)
    {
        if (position < 0 || source.Length - position < sizeof(int))
        {
            throw Invalid();
        }

        var value = BinaryPrimitives.ReadInt32BigEndian(
            source.Slice(position, sizeof(int)));
        position += sizeof(int);
        return value;
    }

    private static MutationStateException Invalid() =>
        new("SCRAM execution material does not match the admitted bounded envelope.");
}

public sealed class ScramDecodedCredentialMaterial : IDisposable
{
    private readonly byte[] _password;
    private bool _disposed;

    internal ScramDecodedCredentialMaterial(
        ScramCredentialBindingDescriptor descriptor,
        byte[] password)
    {
        Descriptor = descriptor ??
                     throw new ArgumentNullException(nameof(descriptor));
        _password = password ??
                    throw new ArgumentNullException(nameof(password));
    }

    public ScramCredentialBindingDescriptor Descriptor { get; }

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
