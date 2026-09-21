using System.Buffers.Binary;
using System.Text;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

internal static class RecordProductionMaterialCodec
{
    private const int Version = 1;

    public static byte[] Encode(
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte> value,
        IReadOnlyList<KeyValuePair<string, ReadOnlyMemory<byte>>> headers)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        writer.Write(Version);
        if (key.HasValue)
        {
            writer.Write(key.Value.Length);
            writer.Write(key.Value.Span);
        }
        else
        {
            writer.Write(-1);
        }

        writer.Write(value.Length);
        writer.Write(value.Span);
        writer.Write(headers.Count);

        foreach (var header in headers)
        {
            var nameBytes = Encoding.UTF8.GetBytes(header.Key);
            writer.Write(nameBytes.Length);
            writer.Write(nameBytes);
            writer.Write(header.Value.Length);
            writer.Write(header.Value.Span);
        }

        writer.Flush();
        return stream.ToArray();
    }

    public static RecordProduceMutation Decode(
        string clusterId,
        string topicName,
        ReadOnlyMemory<byte> envelope,
        RecordProductionCanonicalRecord expected)
    {
        ArgumentNullException.ThrowIfNull(expected);
        var position = 0;

        try
        {
            if (ReadInt32(envelope.Span, ref position) != Version)
                throw Invalid();

            var keyLength = ReadInt32(envelope.Span, ref position);
            if (keyLength < -1 ||
                (keyLength == -1) != !expected.KeyBytes.HasValue ||
                (keyLength >= 0 && expected.KeyBytes != keyLength) ||
                keyLength > RecordProductionPolicy.HardMaxKeyBytes)
            {
                throw Invalid();
            }

            ReadOnlyMemory<byte>? key = keyLength < 0
                ? null
                : ReadSlice(envelope, ref position, keyLength);

            var valueLength = ReadInt32(envelope.Span, ref position);
            if (valueLength != expected.ValueBytes ||
                valueLength < 0 ||
                valueLength > RecordProductionPolicy.HardMaxValueBytes)
            {
                throw Invalid();
            }

            var value = ReadSlice(envelope, ref position, valueLength);

            var headerCount = ReadInt32(envelope.Span, ref position);
            if (headerCount != expected.Headers.Count ||
                headerCount < 0 ||
                headerCount > RecordProductionPolicy.HardMaxHeadersPerRecord)
            {
                throw Invalid();
            }

            var headers = new Dictionary<string, ReadOnlyMemory<byte>>(headerCount, StringComparer.Ordinal);
            for (var index = 0; index < headerCount; index++)
            {
                var nameLength = ReadInt32(envelope.Span, ref position);
                if (nameLength is < 1 or > 1024)
                    throw Invalid();

                var nameBytes = ReadSlice(envelope, ref position, nameLength);
                var name = Encoding.UTF8.GetString(nameBytes.Span);
                var expectedHeader = expected.Headers[index];
                if (!string.Equals(name, expectedHeader.Name, StringComparison.Ordinal))
                    throw Invalid();

                var headerValueLength = ReadInt32(envelope.Span, ref position);
                if (headerValueLength != expectedHeader.ValueBytes || headerValueLength < 0)
                    throw Invalid();

                if (!headers.TryAdd(
                        name,
                        ReadSlice(envelope, ref position, headerValueLength)))
                {
                    throw Invalid();
                }
            }

            if (position != envelope.Length)
                throw Invalid();

            return new RecordProduceMutation(clusterId, topicName, key, value, headers);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw Invalid();
        }
        catch (DecoderFallbackException)
        {
            throw Invalid();
        }
    }

    private static int ReadInt32(ReadOnlySpan<byte> value, ref int position)
    {
        if (position < 0 || value.Length - position < sizeof(int))
            throw Invalid();

        var result = BinaryPrimitives.ReadInt32LittleEndian(
            value.Slice(position, sizeof(int)));
        position += sizeof(int);
        return result;
    }

    private static ReadOnlyMemory<byte> ReadSlice(
        ReadOnlyMemory<byte> value,
        ref int position,
        int length)
    {
        if (length < 0 || position < 0 || value.Length - position < length)
            throw Invalid();

        var result = value.Slice(position, length);
        position += length;
        return result;
    }

    private static MutationStateException Invalid() =>
        new("Record production execution material does not match the admitted preview shape.");
}
