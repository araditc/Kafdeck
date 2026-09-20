using System.Buffers.Binary;
using System.Text;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;

namespace Kafdeck.Infrastructure.SchemaRegistry;

internal sealed class DecodeExecutionGuard
{
    private readonly KafkaOperationContext _operation;
    private readonly CancellationToken _cancellationToken;
    private readonly TimeProvider _timeProvider;
    private int _checks;

    public DecodeExecutionGuard(
        KafkaOperationContext operation,
        CancellationToken cancellationToken,
        TimeProvider? timeProvider = null)
    {
        _operation = operation;
        _cancellationToken = cancellationToken;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public void Check()
    {
        _cancellationToken.ThrowIfCancellationRequested();

        if (_operation.IsExpired(_timeProvider.GetUtcNow()))
        {
            throw new OperationCanceledException(
                "Record decoding exceeded its deadline.",
                CancellationToken.None);
        }
    }

    public void CheckPeriodically()
    {
        if ((Interlocked.Increment(ref _checks) & 0x0F) == 0)
        {
            Check();
        }
    }
}

internal sealed class BoundedProtobufReader
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private const int MaxLengthDelimitedBytes = 16 * 1024 * 1024;

    private readonly ReadOnlyMemory<byte> _data;
    private readonly DecodeExecutionGuard _guard;
    private int _position;

    public BoundedProtobufReader(
        ReadOnlyMemory<byte> data,
        DecodeExecutionGuard guard)
    {
        _data = data;
        _guard = guard ?? throw new ArgumentNullException(nameof(guard));
    }

    public bool IsAtEnd => _position >= _data.Length;

    public uint ReadTag()
    {
        _guard.CheckPeriodically();

        if (IsAtEnd)
        {
            return 0;
        }

        var value = ReadVarint64();
        if (value > uint.MaxValue)
        {
            throw new InvalidDataException("Protobuf tag exceeds UInt32 range.");
        }

        return (uint)value;
    }

    public double ReadDouble() =>
        BitConverter.Int64BitsToDouble(unchecked((long)ReadFixed64()));

    public float ReadFloat() =>
        BitConverter.Int32BitsToSingle(unchecked((int)ReadFixed32()));

    public long ReadInt64() => unchecked((long)ReadVarint64());

    public ulong ReadUInt64() => ReadVarint64();

    public int ReadInt32() => unchecked((int)ReadVarint64());

    public ulong ReadFixed64()
    {
        var span = ReadFixedSpan(sizeof(ulong));
        return BinaryPrimitives.ReadUInt64LittleEndian(span);
    }

    public uint ReadFixed32()
    {
        var span = ReadFixedSpan(sizeof(uint));
        return BinaryPrimitives.ReadUInt32LittleEndian(span);
    }

    public bool ReadBool() =>
        ReadVarint64() != 0;

    public string ReadString()
    {
        var bytes = ReadLengthDelimitedSlice();

        try
        {
            return StrictUtf8.GetString(bytes.Span);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException(
                "Protobuf string contains invalid UTF-8.",
                exception);
        }
    }

    public byte[] ReadBytes() => ReadLengthDelimitedSlice().ToArray();

    public uint ReadUInt32()
    {
        var value = ReadVarint64();
        if (value > uint.MaxValue)
        {
            throw new InvalidDataException("Protobuf UInt32 exceeds range.");
        }

        return (uint)value;
    }

    public int ReadEnum() => unchecked((int)ReadVarint64());

    public int ReadSFixed32() => unchecked((int)ReadFixed32());

    public long ReadSFixed64() => unchecked((long)ReadFixed64());

    public int ReadSInt32()
    {
        var raw = ReadUInt32();
        return unchecked((int)((raw >> 1) ^ (uint)-(int)(raw & 1)));
    }

    public long ReadSInt64()
    {
        var raw = ReadVarint64();
        return unchecked((long)((raw >> 1) ^ (ulong)-(long)(raw & 1)));
    }

    public ReadOnlyMemory<byte> ReadSubMessage() =>
        ReadLengthDelimitedSlice();

    public void SkipField(int wireType)
    {
        _guard.CheckPeriodically();

        switch (wireType)
        {
            case 0:
                _ = ReadVarint64();
                return;
            case 1:
                Skip(sizeof(ulong));
                return;
            case 2:
                _ = ReadLengthDelimitedSlice();
                return;
            case 5:
                Skip(sizeof(uint));
                return;
            default:
                throw new NotSupportedException(
                    "Protobuf groups and unsupported wire types are not accepted.");
        }
    }

    private ulong ReadVarint64()
    {
        _guard.CheckPeriodically();

        ulong value = 0;
        var shift = 0;

        for (var index = 0; index < 10; index++)
        {
            if (_position >= _data.Length)
            {
                throw new EndOfStreamException(
                    "Unexpected end of Protobuf varint.");
            }

            var current = _data.Span[_position++];

            if (index == 9 && (current & 0xFE) != 0)
            {
                throw new InvalidDataException(
                    "Protobuf varint exceeds 64 bits.");
            }

            value |= (ulong)(current & 0x7F) << shift;

            if ((current & 0x80) == 0)
            {
                return value;
            }

            shift += 7;
        }

        throw new InvalidDataException(
            "Protobuf varint is unterminated.");
    }

    private ReadOnlyMemory<byte> ReadLengthDelimitedSlice()
    {
        var lengthValue = ReadVarint64();

        if (lengthValue > MaxLengthDelimitedBytes ||
            lengthValue > int.MaxValue)
        {
            throw new InvalidDataException(
                "Protobuf length-delimited value exceeds the configured bound.");
        }

        var length = (int)lengthValue;

        if (length > _data.Length - _position)
        {
            throw new EndOfStreamException(
                "Unexpected end of Protobuf length-delimited value.");
        }

        var slice = _data.Slice(_position, length);
        _position += length;
        return slice;
    }

    private ReadOnlySpan<byte> ReadFixedSpan(int length)
    {
        _guard.CheckPeriodically();

        if (length > _data.Length - _position)
        {
            throw new EndOfStreamException(
                "Unexpected end of Protobuf fixed-width value.");
        }

        var span = _data.Span.Slice(_position, length);
        _position += length;
        return span;
    }

    private void Skip(int length)
    {
        _guard.CheckPeriodically();

        if (length < 0 || length > _data.Length - _position)
        {
            throw new EndOfStreamException(
                "Unexpected end of Protobuf field.");
        }

        _position += length;
    }
}
