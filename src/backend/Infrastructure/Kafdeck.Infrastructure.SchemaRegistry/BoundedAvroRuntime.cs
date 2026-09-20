using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using Avro;
using Avro.Generic;
using Avro.IO;
using Kafdeck.Core.Records;

namespace Kafdeck.Infrastructure.SchemaRegistry;

internal sealed class BoundedAvroDecoder : Decoder
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private const int MaxCollectionBlockElements = 20_000;
    private const int MaxScalarBytes = (int)RecordOperationBudget.HardMaxRawBytes;

    private readonly Stream _stream;

    public BoundedAvroDecoder(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));

        if (!stream.CanRead)
        {
            throw new ArgumentException("Avro stream must be readable.", nameof(stream));
        }
    }

    public void ReadNull()
    {
    }

    public bool ReadBoolean()
    {
        var value = ReadByteChecked();
        return value switch
        {
            0 => false,
            1 => true,
            _ => throw new InvalidDataException("Invalid Avro boolean."),
        };
    }

    public int ReadInt()
    {
        var value = ReadLong();
        if (value is < int.MinValue or > int.MaxValue)
        {
            throw new InvalidDataException("Avro int is outside Int32 range.");
        }

        return (int)value;
    }

    public long ReadLong()
    {
        ulong raw = 0;
        var shift = 0;

        for (var index = 0; index < 10; index++)
        {
            var current = ReadByteChecked();

            if (index == 9 && (current & 0xFE) != 0)
            {
                throw new InvalidDataException("Avro varint exceeds 64 bits.");
            }

            raw |= (ulong)(current & 0x7F) << shift;

            if ((current & 0x80) == 0)
            {
                return (long)(raw >> 1) ^ -((long)raw & 1);
            }

            shift += 7;
        }

        throw new InvalidDataException("Avro varint is unterminated.");
    }

    public float ReadFloat()
    {
        Span<byte> bytes = stackalloc byte[sizeof(float)];
        ReadExact(bytes);
        var bits = BinaryPrimitives.ReadInt32LittleEndian(bytes);
        return BitConverter.Int32BitsToSingle(bits);
    }

    public double ReadDouble()
    {
        Span<byte> bytes = stackalloc byte[sizeof(double)];
        ReadExact(bytes);
        var bits = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        return BitConverter.Int64BitsToDouble(bits);
    }

    public byte[] ReadBytes()
    {
        var length = ReadBoundedLength("Avro bytes");
        var result = GC.AllocateUninitializedArray<byte>(length);
        ReadExact(result);
        return result;
    }

    public string ReadString()
    {
        var bytes = ReadBytes();

        try
        {
            return StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException("Avro string contains invalid UTF-8.", exception);
        }
    }

    public int ReadEnum() => ReadInt();

    public long ReadArrayStart() => ReadCollectionBlockCount("Avro array");

    public long ReadArrayNext() => ReadCollectionBlockCount("Avro array");

    public long ReadMapStart() => ReadCollectionBlockCount("Avro map");

    public long ReadMapNext() => ReadCollectionBlockCount("Avro map");

    public int ReadUnionIndex()
    {
        var index = ReadLong();
        if (index is < 0 or > int.MaxValue)
        {
            throw new InvalidDataException("Avro union index is invalid.");
        }

        return (int)index;
    }

    public void ReadFixed(byte[] buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ReadFixed(buffer, 0, buffer.Length);
    }

    public void ReadFixed(byte[] buffer, int start, int length)
    {
        ArgumentNullException.ThrowIfNull(buffer);

        if (start < 0 ||
            length < 0 ||
            start > buffer.Length - length ||
            length > MaxScalarBytes)
        {
            throw new InvalidDataException("Avro fixed value exceeds the configured bound.");
        }

        ReadExact(buffer.AsSpan(start, length));
    }

    public void SkipNull()
    {
    }

    public void SkipBoolean() => _ = ReadBoolean();

    public void SkipInt() => _ = ReadInt();

    public void SkipLong() => _ = ReadLong();

    public void SkipFloat() => SkipExact(sizeof(float));

    public void SkipDouble() => SkipExact(sizeof(double));

    public void SkipBytes() => SkipExact(ReadBoundedLength("Avro bytes"));

    public void SkipString() => SkipBytes();

    public void SkipEnum() => _ = ReadEnum();

    public void SkipUnionIndex() => _ = ReadUnionIndex();

    public void SkipFixed(int len)
    {
        if (len < 0 || len > MaxScalarBytes)
        {
            throw new InvalidDataException("Avro fixed value exceeds the configured bound.");
        }

        SkipExact(len);
    }

    private int ReadBoundedLength(string kind)
    {
        var length = ReadLong();

        if (length < 0 ||
            length > MaxScalarBytes ||
            length > RemainingBytes())
        {
            throw new InvalidDataException($"{kind} length exceeds the configured bound.");
        }

        return checked((int)length);
    }

    private long ReadCollectionBlockCount(string kind)
    {
        var count = ReadLong();
        if (count == 0)
        {
            return 0;
        }

        if (count < 0)
        {
            if (count == long.MinValue)
            {
                throw new InvalidDataException($"{kind} block count is invalid.");
            }

            count = -count;
            var declaredBlockBytes = ReadLong();

            if (declaredBlockBytes < 0 ||
                declaredBlockBytes > MaxScalarBytes ||
                declaredBlockBytes > RemainingBytes())
            {
                throw new InvalidDataException($"{kind} block size exceeds the configured bound.");
            }
        }

        if (count > MaxCollectionBlockElements)
        {
            throw new InvalidDataException($"{kind} block element count exceeds the configured bound.");
        }

        return count;
    }

    private int ReadByteChecked()
    {
        var value = _stream.ReadByte();
        if (value < 0)
        {
            throw new EndOfStreamException("Unexpected end of Avro payload.");
        }

        return value;
    }

    private long RemainingBytes()
    {
        if (!_stream.CanSeek)
        {
            return MaxScalarBytes;
        }

        return _stream.Length - _stream.Position;
    }

    private void ReadExact(Span<byte> destination)
    {
        var read = 0;

        while (read < destination.Length)
        {
            var count = _stream.Read(destination[read..]);
            if (count <= 0)
            {
                throw new EndOfStreamException("Unexpected end of Avro payload.");
            }

            read += count;
        }
    }

    private void SkipExact(int length)
    {
        Span<byte> scratch = stackalloc byte[1024];
        var remaining = length;

        while (remaining > 0)
        {
            var chunk = Math.Min(remaining, scratch.Length);
            ReadExact(scratch[..chunk]);
            remaining -= chunk;
        }
    }
}

internal sealed class BoundedGenericDatumReader : GenericDatumReader<object>
{
    private readonly AvroDeserializationBudget _budget;

    public BoundedGenericDatumReader(
        Schema writerSchema,
        Schema readerSchema,
        AvroDeserializationBudget budget)
        : base(writerSchema, readerSchema)
    {
        _budget = budget ?? throw new ArgumentNullException(nameof(budget));
    }

    protected override ArrayAccess GetArrayAccess(ArraySchema readerSchema) =>
        new BoundedArrayAccess(_budget);

    protected override MapAccess GetMapAccess(MapSchema readerSchema) =>
        new BoundedMapAccess(_budget);

    protected override RecordAccess GetRecordAccess(RecordSchema readerSchema) =>
        new BoundedRecordAccess(readerSchema, _budget);

    protected override EnumAccess GetEnumAccess(EnumSchema readerSchema) =>
        new BoundedEnumAccess(readerSchema, _budget);

    protected override FixedAccess GetFixedAccess(FixedSchema readerSchema) =>
        new BoundedFixedAccess(readerSchema, _budget);

    private sealed class BoundedArrayAccess : ArrayAccess
    {
        private readonly AvroDeserializationBudget _budget;

        public BoundedArrayAccess(AvroDeserializationBudget budget)
        {
            _budget = budget;
        }

        public object Create(object reuse) =>
            reuse is object[] existing ? existing : Array.Empty<object>();

        public void EnsureSize(ref object array, int targetSize)
        {
            _budget.ValidateCollectionSize(targetSize);

            if (((object[])array).Length < targetSize)
            {
                SizeTo(ref array, targetSize);
            }
        }

        public void Resize(ref object array, int targetSize)
        {
            _budget.ValidateCollectionSize(targetSize);
            SizeTo(ref array, targetSize);
        }

        public void AddElements(
            object arrayObj,
            int elements,
            int index,
            ReadItem itemReader,
            Decoder decoder,
            bool reuse)
        {
            _budget.Reserve(elements);

            var array = (object[])arrayObj;
            if (index < 0 || elements < 0 || index > array.Length - elements)
            {
                throw new InvalidDataException("Avro array block is outside the bounded allocation.");
            }

            for (var current = index; current < index + elements; current++)
            {
                array[current] = reuse
                    ? itemReader(array[current], decoder)
                    : itemReader(null!, decoder);
            }
        }

        private static void SizeTo(ref object array, int targetSize)
        {
            var values = (object[])array;
            Array.Resize(ref values, targetSize);
            array = values;
        }
    }

    private sealed class BoundedMapAccess : MapAccess
    {
        private readonly AvroDeserializationBudget _budget;

        public BoundedMapAccess(AvroDeserializationBudget budget)
        {
            _budget = budget;
        }

        public object Create(object reuse)
        {
            if (reuse is IDictionary<string, object> map)
            {
                map.Clear();
                return map;
            }

            return new Dictionary<string, object>(StringComparer.Ordinal);
        }

        public void AddElements(
            object mapObj,
            int elements,
            ReadItem itemReader,
            Decoder decoder,
            bool reuse)
        {
            _budget.Reserve(elements);

            var map = (IDictionary<string, object>)mapObj;
            if (elements < 0 ||
                map.Count + elements > AvroDeserializationBudget.MaxNodes)
            {
                throw new InvalidDataException("Avro map exceeds the configured bound.");
            }

            for (var index = 0; index < elements; index++)
            {
                var key = decoder.ReadString();
                map[key] = itemReader(null!, decoder);
            }
        }
    }

    private sealed class BoundedRecordAccess : RecordAccess
    {
        private readonly RecordSchema _schema;
        private readonly AvroDeserializationBudget _budget;

        public BoundedRecordAccess(
            RecordSchema schema,
            AvroDeserializationBudget budget)
        {
            _schema = schema;
            _budget = budget;
        }

        public object CreateRecord(object reuse)
        {
            _budget.Reserve(1);

            if (reuse is GenericRecord record &&
                record.Schema.Equals(_schema))
            {
                return record;
            }

            return new GenericRecord(_schema);
        }

        public object GetField(object record, string fieldName, int fieldPos) =>
            ((GenericRecord)record).TryGetValue(fieldPos, out var result)
                ? result
                : null!;

        public void AddField(
            object record,
            string fieldName,
            int fieldPos,
            object fieldValue)
        {
            _budget.Reserve(1);
            ((GenericRecord)record).Add(fieldName, fieldValue);
        }
    }

    private sealed class BoundedEnumAccess : EnumAccess
    {
        private readonly EnumSchema _schema;
        private readonly AvroDeserializationBudget _budget;

        public BoundedEnumAccess(
            EnumSchema schema,
            AvroDeserializationBudget budget)
        {
            _schema = schema;
            _budget = budget;
        }

        public object CreateEnum(object reuse, int ordinal)
        {
            _budget.Reserve(1);

            if (reuse is GenericEnum value &&
                value.Schema.Equals(_schema))
            {
                value.Value = _schema[ordinal];
                return value;
            }

            return new GenericEnum(_schema, _schema[ordinal]);
        }
    }

    private sealed class BoundedFixedAccess : FixedAccess
    {
        private readonly FixedSchema _schema;
        private readonly AvroDeserializationBudget _budget;

        public BoundedFixedAccess(
            FixedSchema schema,
            AvroDeserializationBudget budget)
        {
            _schema = schema;
            _budget = budget;
        }

        public object CreateFixed(object reuse)
        {
            _budget.ValidateFixedSize(_schema.Size);
            _budget.Reserve(1);

            return reuse is GenericFixed fixedValue &&
                   fixedValue.Schema.Equals(_schema)
                ? fixedValue
                : new GenericFixed(_schema);
        }

        public byte[] GetFixedBuffer(object value) =>
            ((GenericFixed)value).Value;
    }
}

internal sealed class AvroDeserializationBudget
{
    public const int MaxNodes = 20_000;

    private int _reservedNodes;

    public void Reserve(int count)
    {
        if (count < 0 ||
            _reservedNodes > MaxNodes - count)
        {
            throw new InvalidDataException("Avro structured value exceeds the configured node bound.");
        }

        _reservedNodes += count;
    }

    public void ValidateCollectionSize(int size)
    {
        if (size < 0 || size > MaxNodes)
        {
            throw new InvalidDataException("Avro collection exceeds the configured bound.");
        }
    }

    public void ValidateFixedSize(int size)
    {
        if (size < 0 || size > RecordOperationBudget.HardMaxRawBytes)
        {
            throw new InvalidDataException("Avro fixed value exceeds the configured bound.");
        }
    }
}

internal sealed class BoundedJsonBufferWriter : IBufferWriter<byte>
{
    private readonly ArrayBufferWriter<byte> _inner = new();
    private readonly int _maxBytes;

    public BoundedJsonBufferWriter(int maxBytes)
    {
        if (maxBytes < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBytes));
        }

        _maxBytes = maxBytes;
    }

    public ReadOnlyMemory<byte> WrittenMemory => _inner.WrittenMemory;

    public void Advance(int count)
    {
        if (count < 0 || _inner.WrittenCount > _maxBytes - count)
        {
            throw new InvalidDataException("Structured projection exceeds the configured byte bound.");
        }

        _inner.Advance(count);
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        var remaining = _maxBytes - _inner.WrittenCount;
        if (remaining <= 0)
        {
            throw new InvalidDataException("Structured projection exceeds the configured byte bound.");
        }

        var requested = sizeHint <= 0 ? Math.Min(256, remaining) : sizeHint;
        if (requested > remaining)
        {
            throw new InvalidDataException("Structured projection exceeds the configured byte bound.");
        }

        return _inner.GetMemory(requested)[..remaining];
    }

    public Span<byte> GetSpan(int sizeHint = 0) =>
        GetMemory(sizeHint).Span;
}

internal static class BoundedStructuredProjection
{
    public static JsonElement SerializeToElement(object? value)
    {
        var writerBuffer = new BoundedJsonBufferWriter(
            checked((int)RecordOperationBudget.HardMaxProjectedBytes));

        using (var writer = new Utf8JsonWriter(writerBuffer))
        {
            if (value is null)
            {
                writer.WriteNullValue();
            }
            else
            {
                JsonSerializer.Serialize(writer, value, value.GetType());
            }

            writer.Flush();
        }

        using var document = JsonDocument.Parse(writerBuffer.WrittenMemory);
        return document.RootElement.Clone();
    }
}
