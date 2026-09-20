using System.Collections;
using System.Globalization;
using System.Text.Json;
using Avro;
using Avro.Generic;
using Avro.IO;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;

namespace Kafdeck.Infrastructure.SchemaRegistry;

public sealed class ConfluentRecordDecoder : IRecordDecodePort
{
    private const byte MagicByte = 0;
    private const int HeaderSize = 5;
    private const int MaxStructuredDepth = 64;
    private const int MaxStructuredNodes = 20_000;
    private const int MaxSchemaReferences = 64;
    private const int MaxMessageIndexDepth = 32;

    private readonly IRecordSchemaReadPort _schemas;
    private readonly TimeProvider _timeProvider;

    public ConfluentRecordDecoder(
        IRecordSchemaReadPort schemas,
        TimeProvider? timeProvider = null)
    {
        _schemas = schemas ?? throw new ArgumentNullException(nameof(schemas));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<RecordSchemaResult<RecordDecodedValue>> DecodeAsync(
        RecordDecodeRequest request,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed(
                RecordSchemaFailureCategory.Cancelled,
                "record_decode_cancelled",
                "Record decoding was cancelled.",
                false);
        }

        if (operation.IsExpired(_timeProvider.GetUtcNow()))
        {
            return Failed(
                RecordSchemaFailureCategory.Timeout,
                "record_decode_timeout",
                "Record decoding exceeded its deadline.",
                true);
        }

        if (request.Payload.Length < HeaderSize ||
            request.Payload.Length > RecordOperationBudget.HardMaxRawBytes)
        {
            return DecodeFailure("invalid_confluent_payload");
        }

        var span = request.Payload.Span;
        if (span[0] != MagicByte)
        {
            return DecodeFailure("unsupported_confluent_magic_byte");
        }

        var schemaId =
            (span[1] << 24) |
            (span[2] << 16) |
            (span[3] << 8) |
            span[4];

        if (schemaId <= 0)
        {
            return DecodeFailure("invalid_confluent_schema_id");
        }

        var schemaResult = await _schemas.GetSchemaByIdAsync(
                request.ClusterId,
                schemaId,
                operation,
                cancellationToken)
            .ConfigureAwait(false);

        if (!schemaResult.IsSuccess)
        {
            return RecordSchemaResult<RecordDecodedValue>.Failed(schemaResult.Failure!);
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            var document = schemaResult.Value!;
            var framedBody = request.Payload[HeaderSize..];

            var structured = document.Format switch
            {
                RecordSchemaFormat.JsonSchema => DecodeJsonSchema(document, framedBody),
                RecordSchemaFormat.Avro => DecodeAvro(document, framedBody),
                RecordSchemaFormat.Protobuf => await DecodeProtobufAsync(
                        request.ClusterId,
                        document,
                        framedBody,
                        operation,
                        cancellationToken)
                    .ConfigureAwait(false),
                _ => throw new NotSupportedException("Unsupported schema format."),
            };

            return RecordSchemaResult<RecordDecodedValue>.Success(
                new RecordDecodedValue(schemaId, document.Format, structured));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return Failed(
                RecordSchemaFailureCategory.Cancelled,
                "record_decode_cancelled",
                "Record decoding was cancelled.",
                false);
        }
        catch (OperationCanceledException)
        {
            return Failed(
                RecordSchemaFailureCategory.Timeout,
                "record_decode_timeout",
                "Record decoding exceeded its deadline.",
                true);
        }
        catch (NotSupportedException)
        {
            return Failed(
                RecordSchemaFailureCategory.UnsupportedFormat,
                "record_schema_format_unsupported",
                "The record schema format is not supported.",
                false);
        }
        catch (SchemaDependencyException exception)
        {
            return RecordSchemaResult<RecordDecodedValue>.Failed(
                exception.Failure);
        }
        catch (Exception exception) when (
            exception is AvroException or
            InvalidProtocolBufferException or
            InvalidDataException or
            JsonException or
            FormatException or
            IOException or
            InvalidOperationException or
            ArgumentException or
            OverflowException or
            IndexOutOfRangeException)
        {
            return DecodeFailure("record_decode_failed");
        }
    }

    private static JsonElement DecodeJsonSchema(
        RecordSchemaDocument document,
        ReadOnlyMemory<byte> payload)
    {
        using var schema = JsonDocument.Parse(
            document.SchemaText,
            new JsonDocumentOptions
            {
                MaxDepth = MaxStructuredDepth,
                CommentHandling = JsonCommentHandling.Disallow,
                AllowTrailingCommas = false,
            });

        using var value = JsonDocument.Parse(
            payload,
            new JsonDocumentOptions
            {
                MaxDepth = MaxStructuredDepth,
                CommentHandling = JsonCommentHandling.Disallow,
                AllowTrailingCommas = false,
            });

        EnsureJsonNodeBound(value.RootElement);
        return value.RootElement.Clone();
    }

    private static JsonElement DecodeAvro(
        RecordSchemaDocument document,
        ReadOnlyMemory<byte> payload)
    {
        var schema = Schema.Parse(document.SchemaText);
        var deserializationBudget = new AvroDeserializationBudget();
        var reader = new BoundedGenericDatumReader(
            schema,
            schema,
            deserializationBudget);

        using var stream = new MemoryStream(payload.ToArray(), writable: false);
        var decoder = new BoundedAvroDecoder(stream);
        var value = reader.Read(default!, decoder);

        if (stream.Position != stream.Length)
        {
            throw new InvalidDataException("Trailing Avro bytes remain.");
        }

        var structureBudget = new StructureBudget();
        var normalized = NormalizeAvro(value, structureBudget, 0);

        return BoundedStructuredProjection.SerializeToElement(normalized);
    }

    private async Task<JsonElement> DecodeProtobufAsync(
        string clusterId,
        RecordSchemaDocument rootDocument,
        ReadOnlyMemory<byte> framedBody,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        var cursor = 0;
        var indexes = ReadMessageIndexes(framedBody.Span, ref cursor);
        if (cursor > framedBody.Length)
        {
            throw new InvalidDataException("Invalid message index framing.");
        }

        var files = await LoadProtobufFilesAsync(
                clusterId,
                rootDocument,
                operation,
                cancellationToken)
            .ConfigureAwait(false);

        var rootFile = files.Root;
        var message = SelectMessage(rootFile, indexes);
        var registry = ProtobufTypeRegistry.Create(files.All);

        var messageBytes = framedBody[cursor..].ToArray();
        using var input = new CodedInputStream(messageBytes);

        var budget = new StructureBudget();
        var decoded = DecodeProtobufMessage(input, message, rootFile.Package, registry, budget, 0);

        if (!input.IsAtEnd)
        {
            throw new InvalidDataException("Trailing Protobuf bytes remain.");
        }

        return BoundedStructuredProjection.SerializeToElement(decoded);
    }

    private async Task<ProtobufFileSet> LoadProtobufFilesAsync(
        string clusterId,
        RecordSchemaDocument root,
        KafkaOperationContext operation,
        CancellationToken cancellationToken)
    {
        if (root.Format != RecordSchemaFormat.Protobuf)
        {
            throw new InvalidOperationException("Root schema is not Protobuf.");
        }

        var rootFile = ParseDescriptor(root.SchemaText);
        var files = new List<FileDescriptorProto> { rootFile };
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Queue<RecordSchemaReference>(root.References);

        while (pending.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (files.Count > MaxSchemaReferences)
            {
                throw new InvalidOperationException("Protobuf schema reference limit exceeded.");
            }

            var reference = pending.Dequeue();
            var identity = $"{reference.Subject}@{reference.Version}";
            if (!visited.Add(identity))
            {
                continue;
            }

            var result = await _schemas.GetSchemaBySubjectVersionAsync(
                    clusterId,
                    reference.Subject,
                    reference.Version,
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);

            if (!result.IsSuccess)
            {
                throw new SchemaDependencyException(
                    result.Failure
                    ?? new RecordSchemaFailure(
                        RecordSchemaFailureCategory.InvalidResponse,
                        "schema_dependency_failed",
                        "Referenced schema could not be resolved safely.",
                        false));
            }

            var document = result.Value!;
            if (document.Format != RecordSchemaFormat.Protobuf)
            {
                throw new InvalidOperationException("Referenced schema is not Protobuf.");
            }

            var descriptor = ParseDescriptor(document.SchemaText);
            if (string.IsNullOrWhiteSpace(descriptor.Name))
            {
                descriptor.Name = reference.Name;
            }

            files.Add(descriptor);

            foreach (var nestedReference in document.References)
            {
                pending.Enqueue(nestedReference);
            }
        }

        return new ProtobufFileSet(rootFile, files);
    }

    private static FileDescriptorProto ParseDescriptor(string base64)
    {
        var bytes = Convert.FromBase64String(base64);
        if (bytes.Length == 0 || bytes.Length > 4 * 1024 * 1024)
        {
            throw new InvalidDataException("Invalid descriptor size.");
        }

        return FileDescriptorProto.Parser.ParseFrom(bytes);
    }

    private static IReadOnlyList<int> ReadMessageIndexes(
        ReadOnlySpan<byte> payload,
        ref int cursor)
    {
        var countOrZero = ReadZigZagInt(payload, ref cursor);
        if (countOrZero == 0)
        {
            return [0];
        }

        if (countOrZero < 1 || countOrZero > MaxMessageIndexDepth)
        {
            throw new InvalidDataException("Invalid Protobuf message index depth.");
        }

        var indexes = new int[countOrZero];
        for (var index = 0; index < indexes.Length; index++)
        {
            var value = ReadZigZagInt(payload, ref cursor);
            if (value < 0)
            {
                throw new InvalidDataException("Invalid Protobuf message index.");
            }

            indexes[index] = value;
        }

        return indexes;
    }

    private static int ReadZigZagInt(ReadOnlySpan<byte> payload, ref int cursor)
    {
        uint value = 0;
        var shift = 0;

        while (true)
        {
            if (cursor >= payload.Length || shift >= 35)
            {
                throw new InvalidDataException("Invalid Protobuf message index varint.");
            }

            var current = payload[cursor++];
            value |= (uint)(current & 0x7F) << shift;

            if ((current & 0x80) == 0)
            {
                break;
            }

            shift += 7;
        }

        return unchecked((int)((value >> 1) ^ (uint)-(int)(value & 1)));
    }

    private static DescriptorProto SelectMessage(
        FileDescriptorProto file,
        IReadOnlyList<int> indexes)
    {
        if (indexes.Count == 0 ||
            indexes[0] < 0 ||
            indexes[0] >= file.MessageType.Count)
        {
            throw new InvalidDataException("Protobuf message index is outside the schema.");
        }

        var current = file.MessageType[indexes[0]];

        for (var depth = 1; depth < indexes.Count; depth++)
        {
            var index = indexes[depth];
            if (index < 0 || index >= current.NestedType.Count)
            {
                throw new InvalidDataException("Nested Protobuf message index is outside the schema.");
            }

            current = current.NestedType[index];
        }

        return current;
    }

    private static Dictionary<string, object?> DecodeProtobufMessage(
        CodedInputStream input,
        DescriptorProto descriptor,
        string package,
        ProtobufTypeRegistry registry,
        StructureBudget budget,
        int depth)
    {
        budget.EnterNode(depth);

        var fields = descriptor.Field.ToDictionary(field => field.Number);
        var output = new Dictionary<string, object?>(StringComparer.Ordinal);

        while (!input.IsAtEnd)
        {
            var tag = input.ReadTag();
            if (tag == 0)
            {
                break;
            }

            var number = (int)(tag >> 3);
            var wireType = (int)(tag & 0x07);

            if (!fields.TryGetValue(number, out var field))
            {
                input.SkipLastField();
                continue;
            }

            if (field.Label == FieldDescriptorProto.Types.Label.Repeated &&
                wireType == 2 &&
                IsPackable(field.Type))
            {
                var packed = input.ReadBytes();
                using var packedInput = new CodedInputStream(packed.ToByteArray());
                while (!packedInput.IsAtEnd)
                {
                    AddFieldValue(
                        output,
                        field,
                        ReadScalar(packedInput, field, package, registry, budget, depth + 1),
                        package,
                        registry,
                        budget,
                        depth + 1);
                }

                continue;
            }

            ValidateWireType(field, wireType);

            var value = field.Type == FieldDescriptorProto.Types.Type.Message
                ? DecodeNestedMessage(input, field, package, registry, budget, depth + 1)
                : ReadScalar(input, field, package, registry, budget, depth + 1);

            AddFieldValue(output, field, value, package, registry, budget, depth + 1);
        }

        return output;
    }

    private static object? DecodeNestedMessage(
        CodedInputStream input,
        FieldDescriptorProto field,
        string package,
        ProtobufTypeRegistry registry,
        StructureBudget budget,
        int depth)
    {
        var bytes = input.ReadBytes().ToByteArray();
        var target = registry.ResolveMessage(field.TypeName, package);

        using var nested = new CodedInputStream(bytes);
        var result = DecodeProtobufMessage(
            nested,
            target.Descriptor,
            target.Package,
            registry,
            budget,
            depth);

        if (!nested.IsAtEnd)
        {
            throw new InvalidDataException("Nested Protobuf message contains trailing bytes.");
        }

        return result;
    }

    private static object? ReadScalar(
        CodedInputStream input,
        FieldDescriptorProto field,
        string package,
        ProtobufTypeRegistry registry,
        StructureBudget budget,
        int depth)
    {
        budget.EnterNode(depth);

        return field.Type switch
        {
            FieldDescriptorProto.Types.Type.Double => input.ReadDouble(),
            FieldDescriptorProto.Types.Type.Float => input.ReadFloat(),
            FieldDescriptorProto.Types.Type.Int64 => input.ReadInt64(),
            FieldDescriptorProto.Types.Type.Uint64 => input.ReadUInt64(),
            FieldDescriptorProto.Types.Type.Int32 => input.ReadInt32(),
            FieldDescriptorProto.Types.Type.Fixed64 => input.ReadFixed64(),
            FieldDescriptorProto.Types.Type.Fixed32 => input.ReadFixed32(),
            FieldDescriptorProto.Types.Type.Bool => input.ReadBool(),
            FieldDescriptorProto.Types.Type.String => budget.BoundString(input.ReadString()),
            FieldDescriptorProto.Types.Type.Bytes => budget.BoundBytes(input.ReadBytes().ToByteArray()),
            FieldDescriptorProto.Types.Type.Uint32 => input.ReadUInt32(),
            FieldDescriptorProto.Types.Type.Enum => ResolveEnumName(
                input.ReadEnum(),
                field,
                package,
                registry),
            FieldDescriptorProto.Types.Type.Sfixed32 => input.ReadSFixed32(),
            FieldDescriptorProto.Types.Type.Sfixed64 => input.ReadSFixed64(),
            FieldDescriptorProto.Types.Type.Sint32 => input.ReadSInt32(),
            FieldDescriptorProto.Types.Type.Sint64 => input.ReadSInt64(),
            FieldDescriptorProto.Types.Type.Group => throw new NotSupportedException("Protobuf groups are not supported."),
            FieldDescriptorProto.Types.Type.Message => throw new InvalidOperationException("Nested messages require length-delimited decoding."),
            _ => throw new NotSupportedException("Unsupported Protobuf field type."),
        };
    }

    private static object ResolveEnumName(
        int number,
        FieldDescriptorProto field,
        string package,
        ProtobufTypeRegistry registry)
    {
        var enumeration = registry.ResolveEnum(field.TypeName, package);
        var match = enumeration.Value.FirstOrDefault(value => value.Number == number);
        return match is null ? number : match.Name;
    }

    private static void AddFieldValue(
        Dictionary<string, object?> output,
        FieldDescriptorProto field,
        object? value,
        string package,
        ProtobufTypeRegistry registry,
        StructureBudget budget,
        int depth)
    {
        var name = string.IsNullOrWhiteSpace(field.JsonName)
            ? field.Name
            : field.JsonName;

        if (field.Label != FieldDescriptorProto.Types.Label.Repeated)
        {
            output[name] = value;
            return;
        }

        if (field.Type == FieldDescriptorProto.Types.Type.Message)
        {
            var target = registry.ResolveMessage(field.TypeName, package);
            if (target.Descriptor.Options?.MapEntry == true &&
                value is Dictionary<string, object?> entry &&
                entry.TryGetValue("key", out var mapKey) &&
                entry.TryGetValue("value", out var mapValue))
            {
                if (!output.TryGetValue(name, out var existing) ||
                    existing is not Dictionary<string, object?> map)
                {
                    map = new Dictionary<string, object?>(StringComparer.Ordinal);
                    output[name] = map;
                }

                map[MapKey(mapKey)] = mapValue;
                budget.EnterNode(depth);
                return;
            }
        }

        if (!output.TryGetValue(name, out var repeated) ||
            repeated is not List<object?> values)
        {
            values = [];
            output[name] = values;
        }

        values.Add(value);
        budget.EnterNode(depth);
    }

    private static string MapKey(object? value) => value switch
    {
        null => string.Empty,
        bool boolean => boolean ? "true" : "false",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty,
    };

    private static void ValidateWireType(FieldDescriptorProto field, int actual)
    {
        var expected = field.Type switch
        {
            FieldDescriptorProto.Types.Type.Double or
            FieldDescriptorProto.Types.Type.Fixed64 or
            FieldDescriptorProto.Types.Type.Sfixed64 => 1,

            FieldDescriptorProto.Types.Type.Float or
            FieldDescriptorProto.Types.Type.Fixed32 or
            FieldDescriptorProto.Types.Type.Sfixed32 => 5,

            FieldDescriptorProto.Types.Type.String or
            FieldDescriptorProto.Types.Type.Bytes or
            FieldDescriptorProto.Types.Type.Message => 2,

            FieldDescriptorProto.Types.Type.Group => 3,

            _ => 0,
        };

        if (actual != expected)
        {
            throw new InvalidDataException("Protobuf wire type does not match schema.");
        }
    }

    private static bool IsPackable(FieldDescriptorProto.Types.Type type) =>
        type is not (
            FieldDescriptorProto.Types.Type.String or
            FieldDescriptorProto.Types.Type.Bytes or
            FieldDescriptorProto.Types.Type.Message or
            FieldDescriptorProto.Types.Type.Group);

    private static object? NormalizeAvro(
        object? value,
        StructureBudget budget,
        int depth)
    {
        budget.EnterNode(depth);

        return value switch
        {
            null => null,
            GenericRecord record => NormalizeAvroRecord(record, budget, depth + 1),
            GenericEnum enumeration => budget.BoundString(enumeration.Value),
            GenericFixed fixedValue => Convert.ToBase64String(budget.BoundBytes(fixedValue.Value)),
            byte[] bytes => Convert.ToBase64String(budget.BoundBytes(bytes)),
            string text => budget.BoundString(text),
            IDictionary<string, object> map => map.ToDictionary(
                pair => pair.Key,
                pair => NormalizeAvro(pair.Value, budget, depth + 1),
                StringComparer.Ordinal),
            IDictionary dictionary => NormalizeDictionary(dictionary, budget, depth + 1),
            IEnumerable enumerable when value is not string && value is not byte[] =>
                NormalizeEnumerable(enumerable, budget, depth + 1),
            DateTime dateTime => dateTime,
            DateTimeOffset dateTimeOffset => dateTimeOffset,
            decimal decimalValue => decimalValue,
            bool or int or long or float or double => value,
            _ => value.ToString(),
        };
    }

    private static Dictionary<string, object?> NormalizeAvroRecord(
        GenericRecord record,
        StructureBudget budget,
        int depth)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var field in record.Schema.Fields)
        {
            result[field.Name] = NormalizeAvro(record.GetValue(field.Pos), budget, depth);
        }

        return result;
    }

    private static Dictionary<string, object?> NormalizeDictionary(
        IDictionary dictionary,
        StructureBudget budget,
        int depth)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (DictionaryEntry entry in dictionary)
        {
            result[entry.Key?.ToString() ?? string.Empty] =
                NormalizeAvro(entry.Value, budget, depth);
        }

        return result;
    }

    private static List<object?> NormalizeEnumerable(
        IEnumerable values,
        StructureBudget budget,
        int depth)
    {
        var result = new List<object?>();
        foreach (var value in values)
        {
            result.Add(NormalizeAvro(value, budget, depth));
        }

        return result;
    }

    private static void EnsureJsonNodeBound(JsonElement root)
    {
        var stack = new Stack<(JsonElement Element, int Depth)>();
        stack.Push((root, 0));
        var nodes = 0;

        while (stack.Count > 0)
        {
            var (element, depth) = stack.Pop();
            if (++nodes > MaxStructuredNodes || depth > MaxStructuredDepth)
            {
                throw new JsonException("Structured value exceeded the configured bound.");
            }

            if (element.ValueKind == JsonValueKind.String &&
                (element.GetString()?.Length ?? 0) > RecordOperationBudget.HardMaxProjectedBytes)
            {
                throw new JsonException("Structured string exceeded the configured bound.");
            }

            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in element.EnumerateObject())
                {
                    stack.Push((property.Value, depth + 1));
                }
            }
            else if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                {
                    stack.Push((item, depth + 1));
                }
            }
        }
    }

    private static RecordSchemaResult<RecordDecodedValue> DecodeFailure(string code) =>
        Failed(
            RecordSchemaFailureCategory.DecodeFailed,
            code,
            "Record payload could not be decoded safely.",
            false);

    private static RecordSchemaResult<RecordDecodedValue> Failed(
        RecordSchemaFailureCategory category,
        string code,
        string message,
        bool retryable) =>
        RecordSchemaResult<RecordDecodedValue>.Failed(
            new RecordSchemaFailure(category, code, message, retryable));

    private sealed class StructureBudget
    {
        private const int MaxStringChars = 4 * 1024 * 1024;
        private const int MaxBytes = 16 * 1024 * 1024;
        private int _nodes;

        public void EnterNode(int depth)
        {
            if (depth > MaxStructuredDepth || ++_nodes > MaxStructuredNodes)
            {
                throw new InvalidOperationException("Structured decode limit exceeded.");
            }
        }

        public string BoundString(string value)
        {
            if (value.Length > MaxStringChars)
            {
                throw new InvalidOperationException("Decoded string limit exceeded.");
            }

            return value;
        }

        public byte[] BoundBytes(byte[] value)
        {
            if (value.Length > MaxBytes)
            {
                throw new InvalidOperationException("Decoded byte limit exceeded.");
            }

            return value;
        }
    }

    private sealed record ProtobufFileSet(
        FileDescriptorProto Root,
        IReadOnlyList<FileDescriptorProto> All);

    private sealed class ProtobufTypeRegistry
    {
        private readonly Dictionary<string, MessageTypeInfo> _messages;
        private readonly Dictionary<string, EnumDescriptorProto> _enums;

        private ProtobufTypeRegistry(
            Dictionary<string, MessageTypeInfo> messages,
            Dictionary<string, EnumDescriptorProto> enums)
        {
            _messages = messages;
            _enums = enums;
        }

        public static ProtobufTypeRegistry Create(IReadOnlyList<FileDescriptorProto> files)
        {
            var messages = new Dictionary<string, MessageTypeInfo>(StringComparer.Ordinal);
            var enums = new Dictionary<string, EnumDescriptorProto>(StringComparer.Ordinal);

            foreach (var file in files.Concat(KnownWellKnownFiles()).GroupBy(file => file.Name, StringComparer.Ordinal).Select(group => group.First()))
            {
                foreach (var message in file.MessageType)
                {
                    IndexMessage(file.Package, null, message, messages, enums);
                }

                foreach (var enumeration in file.EnumType)
                {
                    enums[FullName(file.Package, null, enumeration.Name)] = enumeration;
                }
            }

            return new ProtobufTypeRegistry(messages, enums);
        }

        private static IEnumerable<FileDescriptorProto> KnownWellKnownFiles()
        {
            yield return Any.Descriptor.File.ToProto();
            yield return Api.Descriptor.File.ToProto();
            yield return Duration.Descriptor.File.ToProto();
            yield return Empty.Descriptor.File.ToProto();
            yield return FieldMask.Descriptor.File.ToProto();
            yield return SourceContext.Descriptor.File.ToProto();
            yield return Struct.Descriptor.File.ToProto();
            yield return Timestamp.Descriptor.File.ToProto();
            yield return Google.Protobuf.WellKnownTypes.Type.Descriptor.File.ToProto();
            yield return DoubleValue.Descriptor.File.ToProto();
            yield return DescriptorProto.Descriptor.File.ToProto();
        }

        public MessageTypeInfo ResolveMessage(string typeName, string package)
        {
            var key = NormalizeTypeName(typeName, package);
            return _messages.TryGetValue(key, out var value)
                ? value
                : throw new InvalidOperationException("Protobuf message type could not be resolved.");
        }

        public EnumDescriptorProto ResolveEnum(string typeName, string package)
        {
            var key = NormalizeTypeName(typeName, package);
            return _enums.TryGetValue(key, out var value)
                ? value
                : throw new InvalidOperationException("Protobuf enum type could not be resolved.");
        }

        private static void IndexMessage(
            string package,
            string? parent,
            DescriptorProto message,
            IDictionary<string, MessageTypeInfo> messages,
            IDictionary<string, EnumDescriptorProto> enums)
        {
            var fullName = FullName(package, parent, message.Name);
            messages[fullName] = new MessageTypeInfo(message, package);

            var nestedParent = string.IsNullOrEmpty(parent)
                ? message.Name
                : $"{parent}.{message.Name}";

            foreach (var nested in message.NestedType)
            {
                IndexMessage(package, nestedParent, nested, messages, enums);
            }

            foreach (var enumeration in message.EnumType)
            {
                enums[FullName(package, nestedParent, enumeration.Name)] = enumeration;
            }
        }

        private static string NormalizeTypeName(string typeName, string package)
        {
            if (string.IsNullOrWhiteSpace(typeName))
            {
                throw new InvalidOperationException("Protobuf type name is missing.");
            }

            if (typeName[0] == '.')
            {
                return typeName[1..];
            }

            return string.IsNullOrWhiteSpace(package)
                ? typeName
                : $"{package}.{typeName}";
        }

        private static string FullName(string package, string? parent, string name)
        {
            var local = string.IsNullOrEmpty(parent) ? name : $"{parent}.{name}";
            return string.IsNullOrEmpty(package) ? local : $"{package}.{local}";
        }
    }

    private sealed record MessageTypeInfo(
        DescriptorProto Descriptor,
        string Package);

    private sealed class SchemaDependencyException : Exception
    {
        public SchemaDependencyException(RecordSchemaFailure failure)
            : base(failure.SafeMessage)
        {
            Failure = failure ?? throw new ArgumentNullException(nameof(failure));
        }

        public RecordSchemaFailure Failure { get; }
    }
}
