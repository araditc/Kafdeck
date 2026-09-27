using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Xml;
using Kafdeck.Core.Records;

namespace Kafdeck.Infrastructure.SerDe;

public sealed class ControlledSerdeService : IControlledSerdePort
{
    private static readonly IReadOnlyList<ControlledSerdeCapability> Capabilities =
    [
        new(
            ControlledSerdeFormat.Cbor,
            DecodeSupported: true,
            EncodeSupported: true,
            [
                "Definite-length primitives, arrays and string-key maps only.",
                "CBOR tags, indefinite-length items and half-precision floats are unsupported.",
                "Byte strings project as an object containing only '$binary'.",
            ]),
        new(
            ControlledSerdeFormat.Xml,
            DecodeSupported: true,
            EncodeSupported: true,
            [
                "DTD, external entity, external schema and external resource resolution are prohibited.",
                "XML projects to the deterministic name/attributes/content model.",
                "XML namespaces are unsupported so decode/encode identity cannot be misrepresented.",
                "Comments and processing instructions are ignored.",
            ]),
        new(
            ControlledSerdeFormat.MessagePack,
            DecodeSupported: true,
            EncodeSupported: true,
            [
                "Primitives, arrays and string-key maps only.",
                "Extension types are unsupported.",
                "Binary values project as an object containing only '$binary'.",
            ]),
    ];

    public IReadOnlyList<ControlledSerdeCapability> GetCapabilities() =>
        Capabilities;

    public Task<ControlledSerdeResult<ControlledSerdeDecodedValue>> DecodeAsync(
        ControlledSerdeDecodeRequest request,
        ControlledSerdeLimits limits,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(limits);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromResult(Failed<ControlledSerdeDecodedValue>(
                ControlledSerdeFailureCategory.Cancelled,
                "serde_operation_cancelled",
                "SerDe operation was cancelled."));
        }

        if (DateTimeOffset.UtcNow >= deadlineUtc)
        {
            return Task.FromResult(Failed<ControlledSerdeDecodedValue>(
                ControlledSerdeFailureCategory.Timeout,
                "serde_deadline_exceeded",
                "SerDe operation exceeded its deadline."));
        }

        if (request.Payload.Length is < 1 ||
            request.Payload.Length > limits.MaxInputBytes)
        {
            return Task.FromResult(Failed<ControlledSerdeDecodedValue>(
                ControlledSerdeFailureCategory.BoundExceeded,
                "serde_input_bound_exceeded",
                "SerDe input exceeded the configured bound."));
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var budget = new StructureBudget(limits, deadlineUtc, cancellationToken);
            var structured = request.Format switch
            {
                ControlledSerdeFormat.Cbor =>
                    DecodeCbor(request.Payload.Span, budget),
                ControlledSerdeFormat.Xml =>
                    DecodeXml(request.Payload, budget),
                ControlledSerdeFormat.MessagePack =>
                    DecodeMessagePack(request.Payload.Span, budget),
                _ => throw new UnsupportedSerdeException(
                    "serde_format_unsupported",
                    "SerDe format is unsupported."),
            };

            using var projected = new BoundedMemoryStream(
                limits.MaxOutputBytes);
            JsonSerializer.Serialize(
                projected,
                structured,
                structured?.GetType() ?? typeof(object));
            var projectedBytes = projected.ToArray();
            using var projectedDocument =
                JsonDocument.Parse(projectedBytes);

            return Task.FromResult(
                ControlledSerdeResult<ControlledSerdeDecodedValue>.Success(
                    new ControlledSerdeDecodedValue(
                        request.Format,
                        projectedDocument.RootElement.Clone())));
        }
        catch (OperationCanceledException)
        {
            return Task.FromResult(Failed<ControlledSerdeDecodedValue>(
                cancellationToken.IsCancellationRequested
                    ? ControlledSerdeFailureCategory.Cancelled
                    : ControlledSerdeFailureCategory.Timeout,
                cancellationToken.IsCancellationRequested
                    ? "serde_operation_cancelled"
                    : "serde_deadline_exceeded",
                cancellationToken.IsCancellationRequested
                    ? "SerDe operation was cancelled."
                    : "SerDe operation exceeded its deadline."));
        }
        catch (SerdeBoundException exception)
        {
            return Task.FromResult(Failed<ControlledSerdeDecodedValue>(
                ControlledSerdeFailureCategory.BoundExceeded,
                exception.Code,
                exception.SafeMessage));
        }
        catch (UnsupportedSerdeException exception)
        {
            return Task.FromResult(Failed<ControlledSerdeDecodedValue>(
                ControlledSerdeFailureCategory.Unsupported,
                exception.Code,
                exception.SafeMessage));
        }
        catch (Exception exception) when (
            exception is InvalidDataException or
            JsonException or
            XmlException or
            FormatException or
            OverflowException or
            ArgumentException)
        {
            return Task.FromResult(Failed<ControlledSerdeDecodedValue>(
                ControlledSerdeFailureCategory.MalformedInput,
                "serde_malformed_input",
                "SerDe input could not be decoded safely."));
        }
    }

    public Task<ControlledSerdeResult<ControlledSerdeEncodedValue>> EncodeAsync(
        ControlledSerdeEncodeRequest request,
        ControlledSerdeLimits limits,
        DateTimeOffset deadlineUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(limits);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromResult(Failed<ControlledSerdeEncodedValue>(
                ControlledSerdeFailureCategory.Cancelled,
                "serde_operation_cancelled",
                "SerDe operation was cancelled."));
        }

        if (DateTimeOffset.UtcNow >= deadlineUtc)
        {
            return Task.FromResult(Failed<ControlledSerdeEncodedValue>(
                ControlledSerdeFailureCategory.Timeout,
                "serde_deadline_exceeded",
                "SerDe operation exceeded its deadline."));
        }

        try
        {
            var budget = new StructureBudget(limits, deadlineUtc, cancellationToken);

            var bytes = request.Format switch
            {
                ControlledSerdeFormat.Cbor =>
                    EncodeCbor(request.StructuredValue, budget),
                ControlledSerdeFormat.Xml =>
                    EncodeXml(request.StructuredValue, budget),
                ControlledSerdeFormat.MessagePack =>
                    EncodeMessagePack(request.StructuredValue, budget),
                _ => throw new UnsupportedSerdeException(
                    "serde_format_unsupported",
                    "SerDe format is unsupported."),
            };

            if (bytes.Length > limits.MaxOutputBytes)
            {
                throw new SerdeBoundException(
                    "serde_output_bound_exceeded",
                    "SerDe output exceeded the configured bound.");
            }

            return Task.FromResult(
                ControlledSerdeResult<ControlledSerdeEncodedValue>.Success(
                    new ControlledSerdeEncodedValue(
                        request.Format,
                        bytes)));
        }
        catch (OperationCanceledException)
        {
            return Task.FromResult(Failed<ControlledSerdeEncodedValue>(
                cancellationToken.IsCancellationRequested
                    ? ControlledSerdeFailureCategory.Cancelled
                    : ControlledSerdeFailureCategory.Timeout,
                cancellationToken.IsCancellationRequested
                    ? "serde_operation_cancelled"
                    : "serde_deadline_exceeded",
                cancellationToken.IsCancellationRequested
                    ? "SerDe operation was cancelled."
                    : "SerDe operation exceeded its deadline."));
        }
        catch (SerdeBoundException exception)
        {
            return Task.FromResult(Failed<ControlledSerdeEncodedValue>(
                ControlledSerdeFailureCategory.BoundExceeded,
                exception.Code,
                exception.SafeMessage));
        }
        catch (UnsupportedSerdeException exception)
        {
            return Task.FromResult(Failed<ControlledSerdeEncodedValue>(
                ControlledSerdeFailureCategory.Unsupported,
                exception.Code,
                exception.SafeMessage));
        }
        catch (Exception exception) when (
            exception is InvalidDataException or
            JsonException or
            XmlException or
            FormatException or
            OverflowException or
            ArgumentException)
        {
            return Task.FromResult(Failed<ControlledSerdeEncodedValue>(
                ControlledSerdeFailureCategory.InvalidRequest,
                "serde_invalid_structured_value",
                "Structured value could not be encoded safely."));
        }
    }

    private static object? DecodeCbor(
        ReadOnlySpan<byte> payload,
        StructureBudget budget)
    {
        var reader = new CborCursor(payload, budget);
        var value = reader.ReadValue(depth: 0);
        reader.EnsureComplete();
        return value;
    }

    private static byte[] EncodeCbor(
        JsonElement value,
        StructureBudget budget)
    {
        using var stream = new BoundedMemoryStream(
            budget.Limits.MaxOutputBytes);
        WriteCbor(stream, value, budget, depth: 0);
        return stream.ToArray();
    }

    private static void WriteCbor(
        Stream stream,
        JsonElement value,
        StructureBudget budget,
        int depth)
    {
        budget.EnterNode(depth);

        switch (value.ValueKind)
        {
            case JsonValueKind.Null:
                stream.WriteByte(0xF6);
                return;

            case JsonValueKind.True:
                stream.WriteByte(0xF5);
                return;

            case JsonValueKind.False:
                stream.WriteByte(0xF4);
                return;

            case JsonValueKind.String:
            {
                var text = budget.BoundString(value.GetString() ?? string.Empty);
                var bytes = Encoding.UTF8.GetBytes(text);
                WriteCborUnsigned(stream, 3, checked((ulong)bytes.Length));
                stream.Write(bytes);
                return;
            }

            case JsonValueKind.Number:
                if (value.TryGetInt64(out var signed))
                {
                    if (signed >= 0)
                    {
                        WriteCborUnsigned(stream, 0, checked((ulong)signed));
                    }
                    else
                    {
                        WriteCborUnsigned(
                            stream,
                            1,
                            checked((ulong)(-1L - signed)));
                    }

                    return;
                }

                if (value.TryGetUInt64(out var unsigned))
                {
                    WriteCborUnsigned(stream, 0, unsigned);
                    return;
                }

                stream.WriteByte(0xFB);
                Span<byte> doubleBytes = stackalloc byte[8];
                BinaryPrimitives.WriteInt64BigEndian(
                    doubleBytes,
                    BitConverter.DoubleToInt64Bits(value.GetDouble()));
                stream.Write(doubleBytes);
                return;

            case JsonValueKind.Array:
            {
                var length = value.GetArrayLength();
                budget.BoundCollection(length);
                WriteCborUnsigned(stream, 4, checked((ulong)length));
                foreach (var item in value.EnumerateArray())
                {
                    WriteCbor(stream, item, budget, depth + 1);
                }

                return;
            }

            case JsonValueKind.Object:
                if (TryGetBinaryProjection(value, budget, out var binary))
                {
                    WriteCborUnsigned(stream, 2, checked((ulong)binary.Length));
                    stream.Write(binary);
                    return;
                }

                var properties = value
                    .EnumerateObject()
                    .OrderBy(property => property.Name, StringComparer.Ordinal)
                    .ToArray();
                budget.BoundCollection(properties.Length);
                WriteCborUnsigned(stream, 5, checked((ulong)properties.Length));

                foreach (var property in properties)
                {
                    var key = budget.BoundString(property.Name);
                    var keyBytes = Encoding.UTF8.GetBytes(key);
                    WriteCborUnsigned(stream, 3, checked((ulong)keyBytes.Length));
                    stream.Write(keyBytes);
                    WriteCbor(stream, property.Value, budget, depth + 1);
                }

                return;

            default:
                throw new InvalidDataException(
                    "JSON value kind is unsupported for CBOR encoding.");
        }
    }

    private static void WriteCborUnsigned(
        Stream stream,
        int major,
        ulong value)
    {
        var prefix = major << 5;
        if (value < 24)
        {
            stream.WriteByte((byte)(prefix | (int)value));
            return;
        }

        if (value <= byte.MaxValue)
        {
            stream.WriteByte((byte)(prefix | 24));
            stream.WriteByte((byte)value);
            return;
        }

        if (value <= ushort.MaxValue)
        {
            stream.WriteByte((byte)(prefix | 25));
            Span<byte> buffer = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16BigEndian(buffer, (ushort)value);
            stream.Write(buffer);
            return;
        }

        if (value <= uint.MaxValue)
        {
            stream.WriteByte((byte)(prefix | 26));
            Span<byte> buffer = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(buffer, (uint)value);
            stream.Write(buffer);
            return;
        }

        stream.WriteByte((byte)(prefix | 27));
        Span<byte> large = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(large, value);
        stream.Write(large);
    }

    private static object DecodeXml(
        ReadOnlyMemory<byte> payload,
        StructureBudget budget)
    {
        using var stream = new MemoryStream(payload.ToArray(), writable: false);
        var settings = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            IgnoreComments = true,
            IgnoreProcessingInstructions = true,
            IgnoreWhitespace = false,
            CloseInput = false,
            MaxCharactersFromEntities = 0,
            MaxCharactersInDocument = budget.Limits.MaxInputBytes,
        };

        using var reader = XmlReader.Create(stream, settings);
        while (reader.Read() && reader.NodeType != XmlNodeType.Element)
        {
            budget.Check();
        }

        if (reader.NodeType != XmlNodeType.Element)
        {
            throw new XmlException("XML document does not contain a root element.");
        }

        var root = ReadXmlElement(reader, budget, depth: 0);

        while (reader.Read())
        {
            budget.Check();
            if (reader.NodeType is XmlNodeType.Element or
                XmlNodeType.Text or
                XmlNodeType.CDATA)
            {
                throw new XmlException(
                    "XML document contains content after the root element.");
            }
        }

        return root;
    }

    private static Dictionary<string, object?> ReadXmlElement(
        XmlReader reader,
        StructureBudget budget,
        int depth)
    {
        budget.EnterNode(depth);

        if (!string.IsNullOrEmpty(reader.NamespaceURI) ||
            !string.IsNullOrEmpty(reader.Prefix))
        {
            throw new UnsupportedSerdeException(
                "serde_xml_namespace_unsupported",
                "XML namespaces are unsupported by the controlled projection.");
        }

        var name = budget.BoundString(reader.Name);
        var attributes = new SortedDictionary<string, string>(
            StringComparer.Ordinal);

        if (reader.HasAttributes)
        {
            while (reader.MoveToNextAttribute())
            {
                if (!string.IsNullOrEmpty(reader.NamespaceURI) ||
                    !string.IsNullOrEmpty(reader.Prefix) ||
                    string.Equals(reader.Name, "xmlns", StringComparison.Ordinal))
                {
                    throw new UnsupportedSerdeException(
                        "serde_xml_namespace_unsupported",
                        "XML namespaces are unsupported by the controlled projection.");
                }

                budget.BoundCollection(attributes.Count + 1);
                attributes.Add(
                    budget.BoundString(reader.Name),
                    budget.BoundString(reader.Value));
            }

            reader.MoveToElement();
        }

        var content = new List<object?>();
        if (reader.IsEmptyElement)
        {
            return XmlElementProjection(name, attributes, content);
        }

        while (reader.Read())
        {
            budget.Check();
            switch (reader.NodeType)
            {
                case XmlNodeType.Element:
                    budget.BoundCollection(content.Count + 1);
                    content.Add(
                        ReadXmlElement(reader, budget, depth + 1));
                    break;

                case XmlNodeType.Text:
                case XmlNodeType.CDATA:
                case XmlNodeType.SignificantWhitespace:
                case XmlNodeType.Whitespace:
                    budget.BoundCollection(content.Count + 1);
                    content.Add(
                        budget.BoundString(reader.Value));
                    break;

                case XmlNodeType.EndElement:
                    return XmlElementProjection(
                        name,
                        attributes,
                        content);

                case XmlNodeType.Comment:
                case XmlNodeType.ProcessingInstruction:
                    break;

                default:
                    throw new UnsupportedSerdeException(
                        "serde_xml_node_unsupported",
                        "XML node type is unsupported by the controlled projection.");
            }
        }

        throw new XmlException("XML element is not closed.");
    }

    private static Dictionary<string, object?> XmlElementProjection(
        string name,
        IReadOnlyDictionary<string, string> attributes,
        IReadOnlyList<object?> content) =>
        new(StringComparer.Ordinal)
        {
            ["name"] = name,
            ["attributes"] = attributes,
            ["content"] = content,
        };

    private static byte[] EncodeXml(
        JsonElement value,
        StructureBudget budget)
    {
        using var stream = new BoundedMemoryStream(
            budget.Limits.MaxOutputBytes);
        var settings = new XmlWriterSettings
        {
            Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            OmitXmlDeclaration = true,
            Indent = false,
            NewLineHandling = NewLineHandling.None,
            CloseOutput = false,
        };

        using (var writer = XmlWriter.Create(stream, settings))
        {
            WriteXmlElement(writer, value, budget, depth: 0);
            writer.Flush();
        }

        return stream.ToArray();
    }

    private static void WriteXmlElement(
        XmlWriter writer,
        JsonElement element,
        StructureBudget budget,
        int depth)
    {
        budget.EnterNode(depth);

        if (element.ValueKind != JsonValueKind.Object ||
            !element.TryGetProperty("name", out var nameElement) ||
            nameElement.ValueKind != JsonValueKind.String)
        {
            throw new InvalidDataException(
                "XML projection requires an object with a string name.");
        }

        var name = budget.BoundString(nameElement.GetString() ?? string.Empty);
        XmlConvert.VerifyName(name);
        writer.WriteStartElement(name);

        if (element.TryGetProperty("attributes", out var attributes))
        {
            if (attributes.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidDataException(
                    "XML attributes projection must be an object.");
            }

            var properties = attributes
                .EnumerateObject()
                .OrderBy(property => property.Name, StringComparer.Ordinal)
                .ToArray();
            budget.BoundCollection(properties.Length);

            foreach (var property in properties)
            {
                if (property.Value.ValueKind != JsonValueKind.String)
                {
                    throw new InvalidDataException(
                        "XML attribute values must be strings.");
                }

                XmlConvert.VerifyName(property.Name);
                writer.WriteAttributeString(
                    property.Name,
                    budget.BoundString(
                        property.Value.GetString() ?? string.Empty));
            }
        }

        if (element.TryGetProperty("content", out var content))
        {
            if (content.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidDataException(
                    "XML content projection must be an array.");
            }

            budget.BoundCollection(content.GetArrayLength());
            foreach (var item in content.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.String)
                {
                    writer.WriteString(
                        budget.BoundString(item.GetString() ?? string.Empty));
                }
                else if (item.ValueKind == JsonValueKind.Object)
                {
                    WriteXmlElement(writer, item, budget, depth + 1);
                }
                else
                {
                    throw new InvalidDataException(
                        "XML content items must be strings or element objects.");
                }
            }
        }

        foreach (var property in element.EnumerateObject())
        {
            if (property.Name is not ("name" or "attributes" or "content"))
            {
                throw new InvalidDataException(
                    "XML projection contains an unsupported property.");
            }
        }

        writer.WriteEndElement();
    }

    private static object? DecodeMessagePack(
        ReadOnlySpan<byte> payload,
        StructureBudget budget)
    {
        var reader = new MessagePackCursor(payload, budget);
        var value = reader.ReadValue(depth: 0);
        reader.EnsureComplete();
        return value;
    }

    private static byte[] EncodeMessagePack(
        JsonElement value,
        StructureBudget budget)
    {
        using var stream = new BoundedMemoryStream(
            budget.Limits.MaxOutputBytes);
        WriteMessagePack(stream, value, budget, depth: 0);
        return stream.ToArray();
    }

    private static void WriteMessagePack(
        Stream stream,
        JsonElement value,
        StructureBudget budget,
        int depth)
    {
        budget.EnterNode(depth);

        switch (value.ValueKind)
        {
            case JsonValueKind.Null:
                stream.WriteByte(0xC0);
                return;

            case JsonValueKind.True:
                stream.WriteByte(0xC3);
                return;

            case JsonValueKind.False:
                stream.WriteByte(0xC2);
                return;

            case JsonValueKind.String:
                WriteMessagePackString(
                    stream,
                    budget.BoundString(value.GetString() ?? string.Empty));
                return;

            case JsonValueKind.Number:
                if (value.TryGetInt64(out var signed))
                {
                    WriteMessagePackInteger(stream, signed);
                    return;
                }

                if (value.TryGetUInt64(out var unsigned))
                {
                    WriteMessagePackUnsigned(stream, unsigned);
                    return;
                }

                stream.WriteByte(0xCB);
                Span<byte> doubleBytes = stackalloc byte[8];
                BinaryPrimitives.WriteInt64BigEndian(
                    doubleBytes,
                    BitConverter.DoubleToInt64Bits(value.GetDouble()));
                stream.Write(doubleBytes);
                return;

            case JsonValueKind.Array:
            {
                var length = value.GetArrayLength();
                budget.BoundCollection(length);
                WriteMessagePackArrayHeader(stream, length);
                foreach (var item in value.EnumerateArray())
                {
                    WriteMessagePack(stream, item, budget, depth + 1);
                }

                return;
            }

            case JsonValueKind.Object:
                if (TryGetBinaryProjection(value, budget, out var binary))
                {
                    WriteMessagePackBinary(stream, binary);
                    return;
                }

                var properties = value
                    .EnumerateObject()
                    .OrderBy(property => property.Name, StringComparer.Ordinal)
                    .ToArray();
                budget.BoundCollection(properties.Length);
                WriteMessagePackMapHeader(stream, properties.Length);

                foreach (var property in properties)
                {
                    WriteMessagePackString(
                        stream,
                        budget.BoundString(property.Name));
                    WriteMessagePack(
                        stream,
                        property.Value,
                        budget,
                        depth + 1);
                }

                return;

            default:
                throw new InvalidDataException(
                    "JSON value kind is unsupported for MessagePack encoding.");
        }
    }

    private static void WriteMessagePackString(
        Stream stream,
        string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        if (bytes.Length <= 31)
        {
            stream.WriteByte((byte)(0xA0 | bytes.Length));
        }
        else if (bytes.Length <= byte.MaxValue)
        {
            stream.WriteByte(0xD9);
            stream.WriteByte((byte)bytes.Length);
        }
        else if (bytes.Length <= ushort.MaxValue)
        {
            stream.WriteByte(0xDA);
            Span<byte> size = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16BigEndian(size, (ushort)bytes.Length);
            stream.Write(size);
        }
        else
        {
            stream.WriteByte(0xDB);
            Span<byte> size = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(size, checked((uint)bytes.Length));
            stream.Write(size);
        }

        stream.Write(bytes);
    }

    private static void WriteMessagePackBinary(
        Stream stream,
        byte[] bytes)
    {
        if (bytes.Length <= byte.MaxValue)
        {
            stream.WriteByte(0xC4);
            stream.WriteByte((byte)bytes.Length);
        }
        else if (bytes.Length <= ushort.MaxValue)
        {
            stream.WriteByte(0xC5);
            Span<byte> size = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16BigEndian(size, (ushort)bytes.Length);
            stream.Write(size);
        }
        else
        {
            stream.WriteByte(0xC6);
            Span<byte> size = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(size, checked((uint)bytes.Length));
            stream.Write(size);
        }

        stream.Write(bytes);
    }

    private static void WriteMessagePackArrayHeader(
        Stream stream,
        int length)
    {
        if (length <= 15)
        {
            stream.WriteByte((byte)(0x90 | length));
        }
        else if (length <= ushort.MaxValue)
        {
            stream.WriteByte(0xDC);
            Span<byte> size = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16BigEndian(size, (ushort)length);
            stream.Write(size);
        }
        else
        {
            stream.WriteByte(0xDD);
            Span<byte> size = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(size, checked((uint)length));
            stream.Write(size);
        }
    }

    private static void WriteMessagePackMapHeader(
        Stream stream,
        int length)
    {
        if (length <= 15)
        {
            stream.WriteByte((byte)(0x80 | length));
        }
        else if (length <= ushort.MaxValue)
        {
            stream.WriteByte(0xDE);
            Span<byte> size = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16BigEndian(size, (ushort)length);
            stream.Write(size);
        }
        else
        {
            stream.WriteByte(0xDF);
            Span<byte> size = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(size, checked((uint)length));
            stream.Write(size);
        }
    }

    private static void WriteMessagePackInteger(
        Stream stream,
        long value)
    {
        if (value >= 0)
        {
            WriteMessagePackUnsigned(stream, checked((ulong)value));
            return;
        }

        if (value >= -32)
        {
            stream.WriteByte(unchecked((byte)(sbyte)value));
        }
        else if (value >= sbyte.MinValue)
        {
            stream.WriteByte(0xD0);
            stream.WriteByte(unchecked((byte)(sbyte)value));
        }
        else if (value >= short.MinValue)
        {
            stream.WriteByte(0xD1);
            Span<byte> buffer = stackalloc byte[2];
            BinaryPrimitives.WriteInt16BigEndian(buffer, (short)value);
            stream.Write(buffer);
        }
        else if (value >= int.MinValue)
        {
            stream.WriteByte(0xD2);
            Span<byte> buffer = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(buffer, (int)value);
            stream.Write(buffer);
        }
        else
        {
            stream.WriteByte(0xD3);
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(buffer, value);
            stream.Write(buffer);
        }
    }

    private static void WriteMessagePackUnsigned(
        Stream stream,
        ulong value)
    {
        if (value <= 0x7F)
        {
            stream.WriteByte((byte)value);
        }
        else if (value <= byte.MaxValue)
        {
            stream.WriteByte(0xCC);
            stream.WriteByte((byte)value);
        }
        else if (value <= ushort.MaxValue)
        {
            stream.WriteByte(0xCD);
            Span<byte> buffer = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16BigEndian(buffer, (ushort)value);
            stream.Write(buffer);
        }
        else if (value <= uint.MaxValue)
        {
            stream.WriteByte(0xCE);
            Span<byte> buffer = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(buffer, (uint)value);
            stream.Write(buffer);
        }
        else
        {
            stream.WriteByte(0xCF);
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
            stream.Write(buffer);
        }
    }

    private static bool TryGetBinaryProjection(
        JsonElement value,
        StructureBudget budget,
        out byte[] bytes)
    {
        bytes = [];
        if (value.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        var properties = value.EnumerateObject().ToArray();
        if (properties.Length != 1 ||
            !string.Equals(
                properties[0].Name,
                "$binary",
                StringComparison.Ordinal) ||
            properties[0].Value.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        var encoded = budget.BoundString(
            properties[0].Value.GetString() ?? string.Empty);
        bytes = Convert.FromBase64String(encoded);
        budget.BoundBinary(bytes.Length);
        return true;
    }

    private static ControlledSerdeResult<T> Failed<T>(
        ControlledSerdeFailureCategory category,
        string code,
        string message) =>
        ControlledSerdeResult<T>.Failed(
            new ControlledSerdeFailure(
                category,
                code,
                message));

    private sealed class CborCursor
    {
        private readonly ReadOnlyMemory<byte> _memory;
        private readonly StructureBudget _budget;
        private int _position;

        public CborCursor(
            ReadOnlySpan<byte> payload,
            StructureBudget budget)
        {
            _memory = payload.ToArray();
            _budget = budget;
        }

        public object? ReadValue(int depth)
        {
            _budget.EnterNode(depth);
            var initial = ReadByte();
            var major = initial >> 5;
            var additional = initial & 0x1F;

            if (additional == 31)
            {
                throw new UnsupportedSerdeException(
                    "serde_cbor_indefinite_unsupported",
                    "Indefinite-length CBOR is unsupported.");
            }

            switch (major)
            {
                case 0:
                    return ReadLength(additional);

                case 1:
                {
                    var encoded = ReadLength(additional);
                    if (encoded > long.MaxValue)
                    {
                        throw new UnsupportedSerdeException(
                            "serde_cbor_integer_unsupported",
                            "CBOR negative integer exceeds the supported range.");
                    }

                    return -1L - checked((long)encoded);
                }

                case 2:
                {
                    var length = CheckedLength(ReadLength(additional));
                    _budget.BoundBinary(length);
                    var bytes = ReadBytes(length).ToArray();
                    return new Dictionary<string, object?>(
                        StringComparer.Ordinal)
                    {
                        ["$binary"] = Convert.ToBase64String(bytes),
                    };
                }

                case 3:
                {
                    var length = CheckedLength(ReadLength(additional));
                    var bytes = ReadBytes(length);
                    var text = new UTF8Encoding(
                        encoderShouldEmitUTF8Identifier: false,
                        throwOnInvalidBytes: true)
                        .GetString(bytes);
                    return _budget.BoundString(text);
                }

                case 4:
                {
                    var count = CheckedLength(ReadLength(additional));
                    _budget.BoundCollection(count);
                    var result = new List<object?>(count);
                    for (var index = 0; index < count; index++)
                    {
                        result.Add(ReadValue(depth + 1));
                    }

                    return result;
                }

                case 5:
                {
                    var count = CheckedLength(ReadLength(additional));
                    _budget.BoundCollection(count);
                    var result = new Dictionary<string, object?>(
                        StringComparer.Ordinal);
                    for (var index = 0; index < count; index++)
                    {
                        var key = ReadValue(depth + 1) as string
                            ?? throw new UnsupportedSerdeException(
                                "serde_cbor_map_key_unsupported",
                                "CBOR map keys must be text strings.");
                        if (!result.TryAdd(
                                _budget.BoundString(key),
                                ReadValue(depth + 1)))
                        {
                            throw new InvalidDataException(
                                "CBOR map contains duplicate keys.");
                        }
                    }

                    return result;
                }

                case 6:
                    throw new UnsupportedSerdeException(
                        "serde_cbor_tag_unsupported",
                        "CBOR tags are unsupported.");

                case 7:
                    return ReadCborSimple(additional);

                default:
                    throw new InvalidDataException(
                        "CBOR major type is invalid.");
            }
        }

        public void EnsureComplete()
        {
            if (_position != _memory.Length)
            {
                throw new InvalidDataException(
                    "Trailing CBOR bytes remain.");
            }
        }

        private object? ReadCborSimple(int additional) =>
            additional switch
            {
                20 => false,
                21 => true,
                22 => null,
                26 => BitConverter.Int32BitsToSingle(
                    BinaryPrimitives.ReadInt32BigEndian(
                        ReadBytes(4))),
                27 => BitConverter.Int64BitsToDouble(
                    BinaryPrimitives.ReadInt64BigEndian(
                        ReadBytes(8))),
                25 => throw new UnsupportedSerdeException(
                    "serde_cbor_float16_unsupported",
                    "CBOR half-precision floats are unsupported."),
                _ => throw new UnsupportedSerdeException(
                    "serde_cbor_simple_unsupported",
                    "CBOR simple value is unsupported."),
            };

        private ulong ReadLength(int additional) =>
            additional switch
            {
                < 24 => (ulong)additional,
                24 => ReadByte(),
                25 => BinaryPrimitives.ReadUInt16BigEndian(
                    ReadBytes(2)),
                26 => BinaryPrimitives.ReadUInt32BigEndian(
                    ReadBytes(4)),
                27 => BinaryPrimitives.ReadUInt64BigEndian(
                    ReadBytes(8)),
                _ => throw new InvalidDataException(
                    "CBOR additional information is invalid."),
            };

        private byte ReadByte()
        {
            _budget.Check();
            if (_position >= _memory.Length)
            {
                throw new InvalidDataException(
                    "CBOR input is truncated.");
            }

            return _memory.Span[_position++];
        }

        private ReadOnlySpan<byte> ReadBytes(int count)
        {
            _budget.Check();
            if (count < 0 ||
                _position > _memory.Length - count)
            {
                throw new InvalidDataException(
                    "CBOR input is truncated.");
            }

            var slice = _memory.Span.Slice(_position, count);
            _position += count;
            return slice;
        }

        private static int CheckedLength(ulong value) =>
            value > int.MaxValue
                ? throw new SerdeBoundException(
                    "serde_collection_bound_exceeded",
                    "Structured collection exceeded the configured bound.")
                : (int)value;
    }

    private sealed class MessagePackCursor
    {
        private readonly ReadOnlyMemory<byte> _memory;
        private readonly StructureBudget _budget;
        private int _position;

        public MessagePackCursor(
            ReadOnlySpan<byte> payload,
            StructureBudget budget)
        {
            _memory = payload.ToArray();
            _budget = budget;
        }

        public object? ReadValue(int depth)
        {
            _budget.EnterNode(depth);
            var code = ReadByte();

            if (code <= 0x7F)
            {
                return (long)code;
            }

            if (code >= 0xE0)
            {
                return (long)unchecked((sbyte)code);
            }

            if ((code & 0xE0) == 0xA0)
            {
                return ReadString(code & 0x1F);
            }

            if ((code & 0xF0) == 0x90)
            {
                return ReadArray(code & 0x0F, depth);
            }

            if ((code & 0xF0) == 0x80)
            {
                return ReadMap(code & 0x0F, depth);
            }

            return code switch
            {
                0xC0 => null,
                0xC2 => false,
                0xC3 => true,
                0xC4 => BinaryProjection(ReadBinary(ReadByte())),
                0xC5 => BinaryProjection(
                    ReadBinary(
                        BinaryPrimitives.ReadUInt16BigEndian(
                            ReadBytes(2)))),
                0xC6 => BinaryProjection(
                    ReadBinary(
                        CheckedLength(
                            BinaryPrimitives.ReadUInt32BigEndian(
                                ReadBytes(4))))),
                0xCA => BitConverter.Int32BitsToSingle(
                    BinaryPrimitives.ReadInt32BigEndian(
                        ReadBytes(4))),
                0xCB => BitConverter.Int64BitsToDouble(
                    BinaryPrimitives.ReadInt64BigEndian(
                        ReadBytes(8))),
                0xCC => (ulong)ReadByte(),
                0xCD => (ulong)BinaryPrimitives.ReadUInt16BigEndian(
                    ReadBytes(2)),
                0xCE => (ulong)BinaryPrimitives.ReadUInt32BigEndian(
                    ReadBytes(4)),
                0xCF => BinaryPrimitives.ReadUInt64BigEndian(
                    ReadBytes(8)),
                0xD0 => (long)unchecked((sbyte)ReadByte()),
                0xD1 => (long)BinaryPrimitives.ReadInt16BigEndian(
                    ReadBytes(2)),
                0xD2 => (long)BinaryPrimitives.ReadInt32BigEndian(
                    ReadBytes(4)),
                0xD3 => BinaryPrimitives.ReadInt64BigEndian(
                    ReadBytes(8)),
                0xD9 => ReadString(ReadByte()),
                0xDA => ReadString(
                    BinaryPrimitives.ReadUInt16BigEndian(
                        ReadBytes(2))),
                0xDB => ReadString(
                    CheckedLength(
                        BinaryPrimitives.ReadUInt32BigEndian(
                            ReadBytes(4)))),
                0xDC => ReadArray(
                    BinaryPrimitives.ReadUInt16BigEndian(
                        ReadBytes(2)),
                    depth),
                0xDD => ReadArray(
                    CheckedLength(
                        BinaryPrimitives.ReadUInt32BigEndian(
                            ReadBytes(4))),
                    depth),
                0xDE => ReadMap(
                    BinaryPrimitives.ReadUInt16BigEndian(
                        ReadBytes(2)),
                    depth),
                0xDF => ReadMap(
                    CheckedLength(
                        BinaryPrimitives.ReadUInt32BigEndian(
                            ReadBytes(4))),
                    depth),
                0xC1 => throw new InvalidDataException(
                    "MessagePack never-used code is invalid."),
                >= 0xC7 and <= 0xC9 or
                >= 0xD4 and <= 0xD8 =>
                    throw new UnsupportedSerdeException(
                        "serde_messagepack_extension_unsupported",
                        "MessagePack extension types are unsupported."),
                _ => throw new UnsupportedSerdeException(
                    "serde_messagepack_type_unsupported",
                    "MessagePack type is unsupported."),
            };
        }

        public void EnsureComplete()
        {
            if (_position != _memory.Length)
            {
                throw new InvalidDataException(
                    "Trailing MessagePack bytes remain.");
            }
        }

        private List<object?> ReadArray(
            int count,
            int depth)
        {
            _budget.BoundCollection(count);
            var result = new List<object?>(count);
            for (var index = 0; index < count; index++)
            {
                result.Add(ReadValue(depth + 1));
            }

            return result;
        }

        private Dictionary<string, object?> ReadMap(
            int count,
            int depth)
        {
            _budget.BoundCollection(count);
            var result = new Dictionary<string, object?>(
                StringComparer.Ordinal);
            for (var index = 0; index < count; index++)
            {
                var key = ReadValue(depth + 1) as string
                    ?? throw new UnsupportedSerdeException(
                        "serde_messagepack_map_key_unsupported",
                        "MessagePack map keys must be strings.");
                if (!result.TryAdd(
                        _budget.BoundString(key),
                        ReadValue(depth + 1)))
                {
                    throw new InvalidDataException(
                        "MessagePack map contains duplicate keys.");
                }
            }

            return result;
        }

        private string ReadString(int length)
        {
            var bytes = ReadBytes(length);
            var text = new UTF8Encoding(
                encoderShouldEmitUTF8Identifier: false,
                throwOnInvalidBytes: true)
                .GetString(bytes);
            return _budget.BoundString(text);
        }

        private byte[] ReadBinary(int length)
        {
            _budget.BoundBinary(length);
            return ReadBytes(length).ToArray();
        }

        private static Dictionary<string, object?> BinaryProjection(
            byte[] bytes) =>
            new(StringComparer.Ordinal)
            {
                ["$binary"] = Convert.ToBase64String(bytes),
            };

        private byte ReadByte()
        {
            _budget.Check();
            if (_position >= _memory.Length)
            {
                throw new InvalidDataException(
                    "MessagePack input is truncated.");
            }

            return _memory.Span[_position++];
        }

        private ReadOnlySpan<byte> ReadBytes(int count)
        {
            _budget.Check();
            if (count < 0 ||
                _position > _memory.Length - count)
            {
                throw new InvalidDataException(
                    "MessagePack input is truncated.");
            }

            var slice = _memory.Span.Slice(_position, count);
            _position += count;
            return slice;
        }

        private static int CheckedLength(uint value) =>
            value > int.MaxValue
                ? throw new SerdeBoundException(
                    "serde_collection_bound_exceeded",
                    "Structured collection exceeded the configured bound.")
                : (int)value;
    }

    private sealed class BoundedMemoryStream : MemoryStream
    {
        private readonly int _maxBytes;

        public BoundedMemoryStream(int maxBytes)
            : base(Math.Min(maxBytes, 64 * 1024))
        {
            if (maxBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxBytes));
            }

            _maxBytes = maxBytes;
        }

        public override void Write(
            byte[] buffer,
            int offset,
            int count)
        {
            EnsureWrite(count);
            base.Write(buffer, offset, count);
        }

        public override void Write(
            ReadOnlySpan<byte> buffer)
        {
            EnsureWrite(buffer.Length);
            base.Write(buffer);
        }

        public override void WriteByte(byte value)
        {
            EnsureWrite(1);
            base.WriteByte(value);
        }

        private void EnsureWrite(int count)
        {
            if (count < 0 ||
                Position > _maxBytes - (long)count)
            {
                throw new SerdeBoundException(
                    "serde_output_bound_exceeded",
                    "SerDe output exceeded the configured bound.");
            }
        }
    }

    private sealed class StructureBudget
    {
        private int _nodes;

        public StructureBudget(
            ControlledSerdeLimits limits,
            DateTimeOffset deadlineUtc,
            CancellationToken cancellationToken)
        {
            Limits = limits;
            DeadlineUtc = deadlineUtc;
            CancellationToken = cancellationToken;
        }

        public ControlledSerdeLimits Limits { get; }
        private DateTimeOffset DeadlineUtc { get; }
        private CancellationToken CancellationToken { get; }

        public void Check()
        {
            CancellationToken.ThrowIfCancellationRequested();
            if (DateTimeOffset.UtcNow >= DeadlineUtc)
            {
                throw new OperationCanceledException(
                    "SerDe deadline exceeded.");
            }
        }

        public void EnterNode(int depth)
        {
            Check();
            if (depth > Limits.MaxDepth ||
                ++_nodes > Limits.MaxNodes)
            {
                throw new SerdeBoundException(
                    "serde_structure_bound_exceeded",
                    "Structured value exceeded the configured depth or node bound.");
            }
        }

        public void BoundCollection(int count)
        {
            Check();
            if (count < 0 ||
                count > Limits.MaxCollectionItems)
            {
                throw new SerdeBoundException(
                    "serde_collection_bound_exceeded",
                    "Structured collection exceeded the configured bound.");
            }
        }

        public string BoundString(string value)
        {
            Check();
            if (value.Length > Limits.MaxStringCharacters)
            {
                throw new SerdeBoundException(
                    "serde_string_bound_exceeded",
                    "Structured string exceeded the configured bound.");
            }

            return value;
        }

        public void BoundBinary(int count)
        {
            Check();
            if (count < 0 ||
                count > Limits.MaxBinaryBytes)
            {
                throw new SerdeBoundException(
                    "serde_binary_bound_exceeded",
                    "Structured binary value exceeded the configured bound.");
            }
        }

        public void ValidateJson(
            JsonElement value,
            int depth)
        {
            EnterNode(depth);
            switch (value.ValueKind)
            {
                case JsonValueKind.Object:
                {
                    var count = 0;
                    foreach (var property in value.EnumerateObject())
                    {
                        BoundString(property.Name);
                        BoundCollection(++count);
                        ValidateJson(property.Value, depth + 1);
                    }

                    break;
                }

                case JsonValueKind.Array:
                    BoundCollection(value.GetArrayLength());
                    foreach (var item in value.EnumerateArray())
                    {
                        ValidateJson(item, depth + 1);
                    }

                    break;

                case JsonValueKind.String:
                    BoundString(value.GetString() ?? string.Empty);
                    break;

                case JsonValueKind.Number:
                case JsonValueKind.True:
                case JsonValueKind.False:
                case JsonValueKind.Null:
                    break;

                default:
                    throw new InvalidDataException(
                        "JSON value kind is unsupported.");
            }
        }
    }

    private sealed class SerdeBoundException : Exception
    {
        public SerdeBoundException(
            string code,
            string safeMessage)
        {
            Code = code;
            SafeMessage = safeMessage;
        }

        public string Code { get; }
        public string SafeMessage { get; }
    }

    private sealed class UnsupportedSerdeException : Exception
    {
        public UnsupportedSerdeException(
            string code,
            string safeMessage)
        {
            Code = code;
            SafeMessage = safeMessage;
        }

        public string Code { get; }
        public string SafeMessage { get; }
    }
}
