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
        using var stream = new MemoryStream(envelope.ToArray(), writable: false);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: false);

        try
        {
            if (reader.ReadInt32() != Version)
                throw Invalid();

            var keyLength = reader.ReadInt32();
            if (keyLength < -1 ||
                (keyLength == -1) != !expected.KeyBytes.HasValue ||
                (keyLength >= 0 && expected.KeyBytes != keyLength) ||
                keyLength > RecordProductionPolicy.HardMaxKeyBytes)
            {
                throw Invalid();
            }

            ReadOnlyMemory<byte>? key = keyLength < 0
                ? null
                : new ReadOnlyMemory<byte>(ReadExact(reader, keyLength));

            var valueLength = reader.ReadInt32();
            if (valueLength != expected.ValueBytes ||
                valueLength < 0 ||
                valueLength > RecordProductionPolicy.HardMaxValueBytes)
            {
                throw Invalid();
            }

            var value = new ReadOnlyMemory<byte>(ReadExact(reader, valueLength));

            var headerCount = reader.ReadInt32();
            if (headerCount != expected.Headers.Count ||
                headerCount < 0 ||
                headerCount > RecordProductionPolicy.HardMaxHeadersPerRecord)
            {
                throw Invalid();
            }

            var headers = new Dictionary<string, ReadOnlyMemory<byte>>(headerCount, StringComparer.Ordinal);
            for (var index = 0; index < headerCount; index++)
            {
                var nameLength = reader.ReadInt32();
                if (nameLength is < 1 or > 1024)
                    throw Invalid();

                var name = Encoding.UTF8.GetString(ReadExact(reader, nameLength));
                var expectedHeader = expected.Headers[index];
                if (!string.Equals(name, expectedHeader.Name, StringComparison.Ordinal))
                    throw Invalid();

                var headerValueLength = reader.ReadInt32();
                if (headerValueLength != expectedHeader.ValueBytes || headerValueLength < 0)
                    throw Invalid();

                if (!headers.TryAdd(
                        name,
                        new ReadOnlyMemory<byte>(ReadExact(reader, headerValueLength))))
                {
                    throw Invalid();
                }
            }

            if (stream.Position != stream.Length)
                throw Invalid();

            return new RecordProduceMutation(clusterId, topicName, key, value, headers);
        }
        catch (EndOfStreamException)
        {
            throw Invalid();
        }
        catch (DecoderFallbackException)
        {
            throw Invalid();
        }
    }

    private static byte[] ReadExact(BinaryReader reader, int count)
    {
        if (count < 0) throw Invalid();
        var value = reader.ReadBytes(count);
        if (value.Length != count) throw new EndOfStreamException();
        return value;
    }

    private static MutationStateException Invalid() =>
        new("Record production execution material does not match the admitted preview shape.");
}
