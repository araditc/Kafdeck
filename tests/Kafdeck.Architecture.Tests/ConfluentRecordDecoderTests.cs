using System.Text;
using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Infrastructure.SchemaRegistry;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ConfluentRecordDecoderTests
{
    [Fact]
    public async Task Json_schema_payload_decodes_to_bounded_structured_value()
    {
        const int schemaId = 11;
        var schemas = new StubSchemaPort(
            new RecordSchemaDocument(
                schemaId,
                RecordSchemaFormat.JsonSchema,
                """{"type":"object","properties":{"name":{"type":"string"},"count":{"type":"integer"}}}""",
                []));

        var body = Encoding.UTF8.GetBytes("""{"name":"alpha","count":7}""");
        var decoder = new ConfluentRecordDecoder(schemas);

        var result = await decoder.DecodeAsync(
            Request(schemaId, body),
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(RecordSchemaFormat.JsonSchema, result.Value!.Format);
        Assert.Equal("alpha", result.Value.StructuredValue.GetProperty("name").GetString());
        Assert.Equal(7, result.Value.StructuredValue.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Avro_record_decodes_with_generic_runtime()
    {
        const int schemaId = 12;
        const string schema = """
            {
              "type":"record",
              "name":"Order",
              "fields":[
                {"name":"name","type":"string"},
                {"name":"count","type":"int"}
              ]
            }
            """;

        var schemas = new StubSchemaPort(
            new RecordSchemaDocument(schemaId, RecordSchemaFormat.Avro, schema, []));

        // Avro binary: string length 5 => zigzag long 10, then UTF-8 bytes;
        // int value 7 => zigzag int 14.
        var body = new byte[]
        {
            0x0A,
            (byte)'a', (byte)'l', (byte)'p', (byte)'h', (byte)'a',
            0x0E,
        };

        var decoder = new ConfluentRecordDecoder(schemas);

        var result = await decoder.DecodeAsync(
            Request(schemaId, body),
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(RecordSchemaFormat.Avro, result.Value!.Format);
        Assert.Equal("alpha", result.Value.StructuredValue.GetProperty("name").GetString());
        Assert.Equal(7, result.Value.StructuredValue.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Protobuf_record_uses_serialized_descriptor_and_message_indexes()
    {
        const int schemaId = 13;

        var message = new DescriptorProto { Name = "Order" };
        message.Field.Add(new FieldDescriptorProto
        {
            Name = "name",
            JsonName = "name",
            Number = 1,
            Label = FieldDescriptorProto.Types.Label.Optional,
            Type = FieldDescriptorProto.Types.Type.String,
        });
        message.Field.Add(new FieldDescriptorProto
        {
            Name = "count",
            JsonName = "count",
            Number = 2,
            Label = FieldDescriptorProto.Types.Label.Optional,
            Type = FieldDescriptorProto.Types.Type.Int32,
        });

        var file = new FileDescriptorProto
        {
            Name = "order.proto",
            Package = "test",
            Syntax = "proto3",
        };
        file.MessageType.Add(message);

        var schemas = new StubSchemaPort(
            new RecordSchemaDocument(
                schemaId,
                RecordSchemaFormat.Protobuf,
                Convert.ToBase64String(file.ToByteArray()),
                []));

        // Confluent Protobuf body: optimized [0] message-index byte,
        // followed by standard protobuf: field 1 string "alpha", field 2 int32 7.
        var body = new byte[]
        {
            0x00,
            0x0A, 0x05,
            (byte)'a', (byte)'l', (byte)'p', (byte)'h', (byte)'a',
            0x10, 0x07,
        };

        var decoder = new ConfluentRecordDecoder(schemas);

        var result = await decoder.DecodeAsync(
            Request(schemaId, body),
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(RecordSchemaFormat.Protobuf, result.Value!.Format);
        Assert.Equal("alpha", result.Value.StructuredValue.GetProperty("name").GetString());
        Assert.Equal(7, result.Value.StructuredValue.GetProperty("count").GetInt32());
    }


    [Fact]
    public async Task Protobuf_well_known_timestamp_resolves_without_registry_reference()
    {
        const int schemaId = 14;

        var rootMessage = new DescriptorProto { Name = "Root" };
        rootMessage.Field.Add(new FieldDescriptorProto
        {
            Name = "created_at",
            JsonName = "createdAt",
            Number = 1,
            Label = FieldDescriptorProto.Types.Label.Optional,
            Type = FieldDescriptorProto.Types.Type.Message,
            TypeName = ".google.protobuf.Timestamp",
        });

        var file = new FileDescriptorProto
        {
            Name = "root.proto",
            Package = "test",
            Syntax = "proto3",
        };
        file.Dependency.Add("google/protobuf/timestamp.proto");
        file.MessageType.Add(rootMessage);

        var schemas = new StubSchemaPort(
            new RecordSchemaDocument(
                schemaId,
                RecordSchemaFormat.Protobuf,
                Convert.ToBase64String(file.ToByteArray()),
                []));

        // indexes [0], Root.created_at => Timestamp { seconds = 1, nanos = 2 }.
        var body = new byte[]
        {
            0x00,
            0x0A, 0x04,
            0x08, 0x01,
            0x10, 0x02,
        };

        var decoder = new ConfluentRecordDecoder(schemas);
        var result = await decoder.DecodeAsync(Request(schemaId, body), Operation(), CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        var timestamp = result.Value!.StructuredValue.GetProperty("createdAt");
        Assert.Equal(1L, timestamp.GetProperty("seconds").GetInt64());
        Assert.Equal(2, timestamp.GetProperty("nanos").GetInt32());
    }

    [Fact]
    public async Task Protobuf_map_entry_projects_to_json_object()
    {
        const int schemaId = 15;

        var entry = new DescriptorProto
        {
            Name = "LabelsEntry",
            Options = new MessageOptions { MapEntry = true },
        };
        entry.Field.Add(new FieldDescriptorProto
        {
            Name = "key",
            JsonName = "key",
            Number = 1,
            Label = FieldDescriptorProto.Types.Label.Optional,
            Type = FieldDescriptorProto.Types.Type.String,
        });
        entry.Field.Add(new FieldDescriptorProto
        {
            Name = "value",
            JsonName = "value",
            Number = 2,
            Label = FieldDescriptorProto.Types.Label.Optional,
            Type = FieldDescriptorProto.Types.Type.Int32,
        });

        var root = new DescriptorProto { Name = "Root" };
        root.NestedType.Add(entry);
        root.Field.Add(new FieldDescriptorProto
        {
            Name = "labels",
            JsonName = "labels",
            Number = 1,
            Label = FieldDescriptorProto.Types.Label.Repeated,
            Type = FieldDescriptorProto.Types.Type.Message,
            TypeName = ".test.Root.LabelsEntry",
        });

        var file = new FileDescriptorProto
        {
            Name = "root.proto",
            Package = "test",
            Syntax = "proto3",
        };
        file.MessageType.Add(root);

        var schemas = new StubSchemaPort(
            new RecordSchemaDocument(
                schemaId,
                RecordSchemaFormat.Protobuf,
                Convert.ToBase64String(file.ToByteArray()),
                []));

        // indexes [0], Root.labels map entry { key = "a", value = 7 }.
        var body = new byte[]
        {
            0x00,
            0x0A, 0x05,
            0x0A, 0x01, (byte)'a',
            0x10, 0x07,
        };

        var decoder = new ConfluentRecordDecoder(schemas);
        var result = await decoder.DecodeAsync(Request(schemaId, body), Operation(), CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(7, result.Value!.StructuredValue.GetProperty("labels").GetProperty("a").GetInt32());
    }

    [Fact]
    public async Task Invalid_magic_byte_fails_without_schema_lookup()
    {
        var schemas = new StubSchemaPort(
            new RecordSchemaDocument(
                1,
                RecordSchemaFormat.JsonSchema,
                """{"type":"object"}""",
                []));

        var payload = new byte[] { 1, 0, 0, 0, 1, (byte)'{' , (byte)'}' };
        var decoder = new ConfluentRecordDecoder(schemas);

        var result = await decoder.DecodeAsync(
            new RecordDecodeRequest("cluster-a", "orders", 0, 0, false, payload),
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal("unsupported_confluent_magic_byte", result.Failure!.Code);
        Assert.Equal(0, schemas.IdLookupCount);
    }


    [Fact]
    public async Task Truncated_confluent_frame_fails_before_registry_lookup()
    {
        var schemas = new StubSchemaPort(
            new RecordSchemaDocument(
                1,
                RecordSchemaFormat.JsonSchema,
                """{"type":"object"}""",
                []));

        var decoder = new ConfluentRecordDecoder(schemas);

        var result = await decoder.DecodeAsync(
            new RecordDecodeRequest(
                "cluster-a",
                "orders",
                0,
                0,
                false,
                new byte[] { 0, 0, 0, 1 }),
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal("invalid_confluent_payload", result.Failure!.Code);
        Assert.Equal(0, schemas.IdLookupCount);
    }

    [Fact]
    public async Task Excessive_protobuf_message_index_depth_fails_closed()
    {
        const int schemaId = 17;
        var file = new FileDescriptorProto
        {
            Name = "root.proto",
            Package = "test",
            Syntax = "proto3",
        };
        file.MessageType.Add(new DescriptorProto { Name = "Root" });

        var schemas = new StubSchemaPort(
            new RecordSchemaDocument(
                schemaId,
                RecordSchemaFormat.Protobuf,
                Convert.ToBase64String(file.ToByteArray()),
                []));

        // Zig-zag encoded positive 33 => 66, above MaxMessageIndexDepth.
        var decoder = new ConfluentRecordDecoder(schemas);
        var result = await decoder.DecodeAsync(
            Request(schemaId, new byte[] { 66 }),
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(RecordSchemaFailureCategory.DecodeFailed, result.Failure!.Category);
        Assert.Equal("record_decode_failed", result.Failure.Code);
    }

    [Fact]
    public async Task Schema_registry_failure_is_propagated_without_payload_detail()
    {
        var schemas = new FailingSchemaPort();
        var decoder = new ConfluentRecordDecoder(schemas);

        var result = await decoder.DecodeAsync(
            Request(23, Encoding.UTF8.GetBytes("""{"secret":"must-not-leak"}""")),
            Operation(),
            CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(RecordSchemaFailureCategory.Unavailable, result.Failure!.Category);
        Assert.DoesNotContain("must-not-leak", result.Failure.SafeMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Protobuf_references_are_loaded_by_subject_version()
    {
        const int rootId = 21;
        var childMessage = new DescriptorProto { Name = "Child" };
        childMessage.Field.Add(new FieldDescriptorProto
        {
            Name = "value",
            JsonName = "value",
            Number = 1,
            Label = FieldDescriptorProto.Types.Label.Optional,
            Type = FieldDescriptorProto.Types.Type.String,
        });

        var childFile = new FileDescriptorProto
        {
            Name = "child.proto",
            Package = "shared",
            Syntax = "proto3",
        };
        childFile.MessageType.Add(childMessage);

        var rootMessage = new DescriptorProto { Name = "Root" };
        rootMessage.Field.Add(new FieldDescriptorProto
        {
            Name = "child",
            JsonName = "child",
            Number = 1,
            Label = FieldDescriptorProto.Types.Label.Optional,
            Type = FieldDescriptorProto.Types.Type.Message,
            TypeName = ".shared.Child",
        });

        var rootFile = new FileDescriptorProto
        {
            Name = "root.proto",
            Package = "test",
            Syntax = "proto3",
        };
        rootFile.Dependency.Add("child.proto");
        rootFile.MessageType.Add(rootMessage);

        var root = new RecordSchemaDocument(
            rootId,
            RecordSchemaFormat.Protobuf,
            Convert.ToBase64String(rootFile.ToByteArray()),
            [new RecordSchemaReference("child.proto", "shared-child", 1)]);

        var child = new RecordSchemaDocument(
            22,
            RecordSchemaFormat.Protobuf,
            Convert.ToBase64String(childFile.ToByteArray()),
            []);

        var schemas = new StubSchemaPort(root);
        schemas.AddReference("shared-child", 1, child);

        // Message indexes [0]. Root.child = embedded message { value = "ok" }.
        var body = new byte[]
        {
            0x00,
            0x0A, 0x04,
            0x0A, 0x02, (byte)'o', (byte)'k',
        };

        var decoder = new ConfluentRecordDecoder(schemas);

        var result = await decoder.DecodeAsync(
            Request(rootId, body),
            Operation(),
            CancellationToken.None);

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        Assert.Equal(
            "ok",
            result.Value!.StructuredValue
                .GetProperty("child")
                .GetProperty("value")
                .GetString());
        Assert.Equal(1, schemas.SubjectLookupCount);
    }

    private static RecordDecodeRequest Request(int schemaId, byte[] body)
    {
        var framed = new byte[5 + body.Length];
        framed[0] = 0;
        framed[1] = (byte)(schemaId >> 24);
        framed[2] = (byte)(schemaId >> 16);
        framed[3] = (byte)(schemaId >> 8);
        framed[4] = (byte)schemaId;
        body.CopyTo(framed, 5);

        return new RecordDecodeRequest(
            "cluster-a",
            "orders",
            0,
            0,
            false,
            framed);
    }

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10));


    private sealed class FailingSchemaPort : IRecordSchemaReadPort
    {
        public Task<RecordSchemaResult<RecordSchemaDocument>> GetSchemaByIdAsync(
            string clusterId,
            int schemaId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(
                RecordSchemaResult<RecordSchemaDocument>.Failed(
                    new RecordSchemaFailure(
                        RecordSchemaFailureCategory.Unavailable,
                        "schema_registry_unavailable",
                        "Schema Registry is temporarily unavailable.",
                        true)));

        public Task<RecordSchemaResult<RecordSchemaDocument>> GetSchemaBySubjectVersionAsync(
            string clusterId,
            string subject,
            int version,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }

    private sealed class StubSchemaPort : IRecordSchemaReadPort
    {
        private readonly RecordSchemaDocument _root;
        private readonly Dictionary<(string Subject, int Version), RecordSchemaDocument> _references = new();

        public StubSchemaPort(RecordSchemaDocument root)
        {
            _root = root;
        }

        public int IdLookupCount { get; private set; }

        public int SubjectLookupCount { get; private set; }

        public void AddReference(string subject, int version, RecordSchemaDocument document) =>
            _references[(subject, version)] = document;

        public Task<RecordSchemaResult<RecordSchemaDocument>> GetSchemaByIdAsync(
            string clusterId,
            int schemaId,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            IdLookupCount++;
            return Task.FromResult(
                schemaId == _root.Id
                    ? RecordSchemaResult<RecordSchemaDocument>.Success(_root)
                    : RecordSchemaResult<RecordSchemaDocument>.Failed(
                        new RecordSchemaFailure(
                            RecordSchemaFailureCategory.SchemaNotFound,
                            "schema_not_found",
                            "Schema not found.",
                            false)));
        }

        public Task<RecordSchemaResult<RecordSchemaDocument>> GetSchemaBySubjectVersionAsync(
            string clusterId,
            string subject,
            int version,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            SubjectLookupCount++;

            return Task.FromResult(
                _references.TryGetValue((subject, version), out var document)
                    ? RecordSchemaResult<RecordSchemaDocument>.Success(document)
                    : RecordSchemaResult<RecordSchemaDocument>.Failed(
                        new RecordSchemaFailure(
                            RecordSchemaFailureCategory.SchemaNotFound,
                            "schema_not_found",
                            "Schema not found.",
                            false)));
        }
    }
}
