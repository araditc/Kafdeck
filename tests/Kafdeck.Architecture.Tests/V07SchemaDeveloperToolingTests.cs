using System.Text.Json;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Modules.Schemas;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07SchemaDeveloperToolingTests
{
    [Fact]
    public async Task Reference_graph_is_bounded_deterministic_and_cycle_aware()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("orders", 2)] = Detail(
                    "orders",
                    2,
                    20,
                    RecordSchemaFormat.Avro,
                    """{"type":"record","name":"Orders","fields":[]}""",
                    new RecordSchemaReference("common.avsc", "common", 1)),
                [("common", 1)] = Detail(
                    "common",
                    1,
                    10,
                    RecordSchemaFormat.Protobuf,
                    """message Common {}""",
                    new RecordSchemaReference("orders.avsc", "orders", 2)),
            });

        var service = new SchemaDeveloperService(catalog);

        var first = await service.BuildReferenceGraphAsync(
            "cluster-a",
            "orders",
            2);
        var second = await service.BuildReferenceGraphAsync(
            "cluster-a",
            "orders",
            2);

        Assert.True(first.IsSuccess);
        Assert.True(second.IsSuccess);
        Assert.NotNull(first.Value);
        Assert.NotNull(second.Value);
        Assert.True(first.Value!.HasCycle);
        Assert.Equal(
            new[] { "orders:2:0", "common:1:1" },
            first.Value.Nodes
                .Select(node =>
                    $"{node.Subject}:{node.Version}:{node.Depth}")
                .ToArray());
        Assert.Equal(
            first.Value.Nodes,
            second.Value.Nodes);
        Assert.Equal(
            first.Value.Edges,
            second.Value.Edges);
        Assert.Equal(
            2,
            first.Value.Edges.Count);
        Assert.True(first.Value.TotalSchemaBytes > 0);
    }

    [Fact]
    public async Task Reference_graph_fails_closed_before_reading_an_unauthorized_referenced_subject()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("orders", 1)] = Detail(
                    "orders",
                    1,
                    1,
                    RecordSchemaFormat.Avro,
                    """{"type":"record","name":"Orders","fields":[]}""",
                    new RecordSchemaReference("common.avsc", "common", 1)),
                [("common", 1)] = Detail(
                    "common",
                    1,
                    2,
                    RecordSchemaFormat.Avro,
                    """{"type":"record","name":"Common","fields":[]}"""),
            });
        var service = new SchemaDeveloperService(catalog);

        var result = await service.BuildReferenceGraphAsync(
            "cluster-a",
            "orders",
            1,
            subjectAuthorization:
                candidate => !string.Equals(
                    candidate,
                    "common",
                    StringComparison.Ordinal));

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.Unauthorized,
            result.Failure!.Category);
        Assert.Equal(
            "schema_reference_authorization_denied",
            result.Failure.Code);
        Assert.DoesNotContain(
            ("common", 1),
            catalog.Reads);
    }

    [Fact]
    public async Task Reference_graph_fails_closed_when_depth_bound_is_exceeded()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("a", 1)] = Detail(
                    "a",
                    1,
                    1,
                    RecordSchemaFormat.JsonSchema,
                    """{"type":"object"}""",
                    new RecordSchemaReference("b", "b", 1)),
                [("b", 1)] = Detail(
                    "b",
                    1,
                    2,
                    RecordSchemaFormat.JsonSchema,
                    """{"type":"object"}""",
                    new RecordSchemaReference("c", "c", 1)),
                [("c", 1)] = Detail(
                    "c",
                    1,
                    3,
                    RecordSchemaFormat.JsonSchema,
                    """{"type":"object"}"""),
            });

        var service = new SchemaDeveloperService(
            catalog,
            new SchemaDeveloperPolicy(
                TimeSpan.FromSeconds(1),
                MaxReferenceNodes: 8,
                MaxReferenceEdges: 16,
                MaxReferenceDepth: 1,
                MaxSchemaBytes: 16 * 1024,
                MaxGeneratedExamples: 2,
                MaxGeneratedBytes: 16 * 1024));

        var result = await service.BuildReferenceGraphAsync(
            "cluster-a",
            "a",
            1);

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.ResponseTooLarge,
            result.Failure!.Category);
        Assert.Equal(
            "schema_reference_depth_exceeded",
            result.Failure.Code);
    }

    [Fact]
    public async Task Reference_graph_rejects_edge_fanout_above_hard_bound()
    {
        var references = Enumerable
            .Range(0, 257)
            .Select(index =>
                new RecordSchemaReference(
                    $"reference-{index}",
                    "common",
                    1))
            .ToArray();

        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("orders", 1)] = Detail(
                    "orders",
                    1,
                    1,
                    RecordSchemaFormat.Avro,
                    """{"type":"record","name":"Orders","fields":[]}""",
                    references),
                [("common", 1)] = Detail(
                    "common",
                    1,
                    2,
                    RecordSchemaFormat.Avro,
                    """{"type":"record","name":"Common","fields":[]}"""),
            });

        var result = await new SchemaDeveloperService(catalog)
            .BuildReferenceGraphAsync(
                "cluster-a",
                "orders",
                1);

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.ResponseTooLarge,
            result.Failure!.Category);
        Assert.Equal(
            "schema_reference_edges_exceeded",
            result.Failure.Code);
        Assert.Equal(
            new[] { ("orders", 1) },
            catalog.Reads);
    }

    [Fact]
    public async Task Reference_graph_rejects_malformed_reference_without_followup_read()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("orders", 1)] = Detail(
                    "orders",
                    1,
                    1,
                    RecordSchemaFormat.Avro,
                    """{"type":"record","name":"Orders","fields":[]}""",
                    new RecordSchemaReference(
                        "invalid",
                        "",
                        0)),
            });

        var result = await new SchemaDeveloperService(catalog)
            .BuildReferenceGraphAsync(
                "cluster-a",
                "orders",
                1);

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.InvalidRequest,
            result.Failure!.Category);
        Assert.Equal(
            "invalid_schema_reference",
            result.Failure.Code);
        Assert.Equal(
            new[] { ("orders", 1) },
            catalog.Reads);
    }

    [Fact]
    public async Task Reference_graph_rejects_aggregate_schema_bytes_above_bound()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("orders", 1)] = Detail(
                    "orders",
                    1,
                    1,
                    RecordSchemaFormat.JsonSchema,
                    """{"type":"object","properties":{"value":{"type":"string"}}}"""),
            });
        var service = new SchemaDeveloperService(
            catalog,
            new SchemaDeveloperPolicy(
                TimeSpan.FromSeconds(1),
                MaxReferenceNodes: 8,
                MaxReferenceEdges: 16,
                MaxReferenceDepth: 2,
                MaxSchemaBytes: 8,
                MaxGeneratedExamples: 2,
                MaxGeneratedBytes: 16 * 1024));

        var result = await service.BuildReferenceGraphAsync(
            "cluster-a",
            "orders",
            1);

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.ResponseTooLarge,
            result.Failure!.Category);
        Assert.Equal(
            "schema_reference_bytes_exceeded",
            result.Failure.Code);
    }

    [Fact]
    public async Task Compatibility_explanation_preserves_inherited_scope()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>(),
            new SchemaCompatibilityObservation(
                "orders",
                SchemaCompatibilityMode.FullTransitive,
                IsInherited: true));
        var service = new SchemaDeveloperService(catalog);

        var result = await service.ExplainCompatibilityAsync(
            "cluster-a",
            "orders");

        Assert.True(result.IsSuccess);
        Assert.NotNull(result.Value);
        Assert.Equal("global-inherited", result.Value!.Scope);
        Assert.True(result.Value.IsInherited);
        Assert.Equal(
            SchemaCompatibilityMode.FullTransitive,
            result.Value.Mode);
        Assert.NotEmpty(result.Value.Rules);
    }

    [Fact]
    public async Task Mock_generation_is_deterministic_across_supported_formats_and_redacts_sensitive_names()
    {
        var fixtures =
            new[]
            {
                Detail(
                    "avro",
                    1,
                    1,
                    RecordSchemaFormat.Avro,
                    """
                    {
                      "type": "record",
                      "name": "User",
                      "fields": [
                        { "name": "username", "type": "string" },
                        { "name": "password", "type": "string", "default": "never-copy-this" }
                      ]
                    }
                    """),
                Detail(
                    "json",
                    1,
                    2,
                    RecordSchemaFormat.JsonSchema,
                    """
                    {
                      "type": "object",
                      "properties": {
                        "username": { "type": "string" },
                        "apiKey": { "type": "string", "default": "never-copy-this" }
                      }
                    }
                    """),
                Detail(
                    "proto",
                    1,
                    3,
                    RecordSchemaFormat.Protobuf,
                    """
                    syntax = "proto3";
                    message User {
                      string username = 1;
                      string access_token = 2;
                    }
                    """),
            };

        foreach (var fixture in fixtures)
        {
            var catalog = new FakeSchemaCatalog(
                new Dictionary<(string Subject, int Version), SchemaVersionDetail>
                {
                    [(fixture.Subject, fixture.Version)] = fixture,
                });
            var service = new SchemaDeveloperService(catalog);

            var first = await service.GenerateMockAsync(
                "cluster-a",
                fixture.Subject,
                fixture.Version,
                count: 2,
                seed: 42);
            var second = await service.GenerateMockAsync(
                "cluster-a",
                fixture.Subject,
                fixture.Version,
                count: 2,
                seed: 42);

            Assert.True(first.IsSuccess);
            Assert.True(second.IsSuccess);
            Assert.NotNull(first.Value);
            Assert.NotNull(second.Value);
            Assert.Equal(
                first.Value!.Examples,
                second.Value!.Examples);
            Assert.Equal(2, first.Value.Examples.Count);
            Assert.All(
                first.Value.Examples,
                example =>
                {
                    Assert.DoesNotContain(
                        "never-copy-this",
                        example.Json,
                        StringComparison.Ordinal);

                    using var json = JsonDocument.Parse(example.Json);
                    var root = json.RootElement;
                    Assert.Equal(
                        JsonValueKind.Object,
                        root.ValueKind);

                    var sensitiveProperty =
                        fixture.Schema.Format switch
                        {
                            RecordSchemaFormat.Avro => "password",
                            RecordSchemaFormat.JsonSchema => "apiKey",
                            RecordSchemaFormat.Protobuf => "access_token",
                            _ => throw new InvalidOperationException(),
                        };

                    Assert.Equal(
                        "[REDACTED]",
                        root.GetProperty(sensitiveProperty).GetString());
                });
        }
    }

    [Fact]
    public async Task Mock_generation_rejects_external_references_until_typed_resolution_exists()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("orders", 1)] = Detail(
                    "orders",
                    1,
                    1,
                    RecordSchemaFormat.Avro,
                    """{"type":"record","name":"Orders","fields":[]}""",
                    new RecordSchemaReference(
                        "common.avsc",
                        "common",
                        1)),
            });
        var service = new SchemaDeveloperService(catalog);

        var result = await service.GenerateMockAsync(
            "cluster-a",
            "orders",
            1,
            count: 1,
            seed: 0);

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.Unsupported,
            result.Failure!.Category);
        Assert.Equal(
            "schema_mock_references_unsupported",
            result.Failure.Code);
    }

    [Fact]
    public async Task Mock_generation_rejects_output_above_byte_bound()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("orders", 1)] = Detail(
                    "orders",
                    1,
                    1,
                    RecordSchemaFormat.JsonSchema,
                    """
                    {
                      "type": "object",
                      "properties": {
                        "customerName": { "type": "string" }
                      }
                    }
                    """),
            });
        var service = new SchemaDeveloperService(
            catalog,
            new SchemaDeveloperPolicy(
                TimeSpan.FromSeconds(1),
                MaxReferenceNodes: 8,
                MaxReferenceEdges: 16,
                MaxReferenceDepth: 2,
                MaxSchemaBytes: 16 * 1024,
                MaxGeneratedExamples: 2,
                MaxGeneratedBytes: 8));

        var result = await service.GenerateMockAsync(
            "cluster-a",
            "orders",
            1,
            count: 1,
            seed: 0);

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.ResponseTooLarge,
            result.Failure!.Category);
        Assert.Equal(
            "schema_mock_output_bytes_exceeded",
            result.Failure.Code);
    }

    [Fact]
    public async Task Mock_generation_rejects_count_above_hard_bound()
    {
        var catalog = new FakeSchemaCatalog(
            new Dictionary<(string Subject, int Version), SchemaVersionDetail>
            {
                [("orders", 1)] = Detail(
                    "orders",
                    1,
                    1,
                    RecordSchemaFormat.JsonSchema,
                    """{"type":"object"}"""),
            });
        var service = new SchemaDeveloperService(catalog);

        var result = await service.GenerateMockAsync(
            "cluster-a",
            "orders",
            1,
            count: 11,
            seed: 0);

        Assert.False(result.IsSuccess);
        Assert.NotNull(result.Failure);
        Assert.Equal(
            ReadViewFailureCategory.InvalidRequest,
            result.Failure!.Category);
        Assert.Equal(
            "invalid_schema_mock_request",
            result.Failure.Code);
    }

    private static SchemaVersionDetail Detail(
        string subject,
        int version,
        int id,
        RecordSchemaFormat format,
        string schema,
        params RecordSchemaReference[] references) =>
        new(
            subject,
            version,
            new RecordSchemaDocument(
                id,
                format,
                schema,
                references));

    private sealed class FakeSchemaCatalog : ISchemaCatalogReadPort
    {
        private readonly IReadOnlyDictionary<
            (string Subject, int Version),
            SchemaVersionDetail> _versions;
        private readonly SchemaCompatibilityObservation _compatibility;
        private readonly List<(string Subject, int Version)> _reads = [];

        public IReadOnlyList<(string Subject, int Version)> Reads => _reads;

        public FakeSchemaCatalog(
            IReadOnlyDictionary<
                (string Subject, int Version),
                SchemaVersionDetail> versions,
            SchemaCompatibilityObservation? compatibility = null)
        {
            _versions = versions;
            _compatibility = compatibility ??
                new SchemaCompatibilityObservation(
                    "default",
                    SchemaCompatibilityMode.Backward,
                    IsInherited: false);
        }

        public Task<ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>>
            ListSubjectsAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>
                    .Success(
                        _versions.Keys
                            .Select(key => key.Subject)
                            .Distinct(StringComparer.Ordinal)
                            .OrderBy(value => value, StringComparer.Ordinal)
                            .Select(value =>
                                new SchemaSubjectSummary(value))
                            .ToArray()));

        public Task<ReadViewResult<IReadOnlyList<SchemaVersionSummary>>>
            ListVersionsAsync(
                string clusterId,
                string subject,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<IReadOnlyList<SchemaVersionSummary>>
                    .Success(
                        _versions.Values
                            .Where(value =>
                                string.Equals(
                                    value.Subject,
                                    subject,
                                    StringComparison.Ordinal))
                            .OrderBy(value => value.Version)
                            .Select(value =>
                                new SchemaVersionSummary(
                                    value.Subject,
                                    value.Version,
                                    value.Schema.Id,
                                    value.Schema.Format,
                                    value.Schema.References))
                            .ToArray()));

        public Task<ReadViewResult<SchemaVersionDetail>> GetVersionAsync(
            string clusterId,
            string subject,
            int version,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
        {
            _reads.Add((subject, version));

            if (_versions.TryGetValue(
                    (subject, version),
                    out var value))
            {
                return Task.FromResult(
                    ReadViewResult<SchemaVersionDetail>.Success(value));
            }

            return Task.FromResult(
                ReadViewResult<SchemaVersionDetail>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Unavailable,
                        "schema_not_found",
                        "Schema version is unavailable.",
                        false)));
        }

        public Task<ReadViewResult<SchemaCompatibilityObservation>>
            GetCompatibilityAsync(
                string clusterId,
                string subject,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<SchemaCompatibilityObservation>
                    .Success(
                        _compatibility with { Subject = subject }));

        public Task<ReadViewResult<SchemaGlobalCompatibilityObservation>>
            GetGlobalCompatibilityAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<SchemaGlobalCompatibilityObservation>
                    .Success(
                        new SchemaGlobalCompatibilityObservation(
                            _compatibility.Mode)));
    }
}
