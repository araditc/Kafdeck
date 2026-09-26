using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;

namespace Kafdeck.Modules.Schemas;

public sealed record SchemaDeveloperPolicy(
    TimeSpan OperationTimeout,
    int MaxReferenceNodes,
    int MaxReferenceDepth,
    long MaxSchemaBytes,
    int MaxGeneratedExamples,
    long MaxGeneratedBytes)
{
    public static SchemaDeveloperPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(10),
            MaxReferenceNodes: 64,
            MaxReferenceDepth: 12,
            MaxSchemaBytes: 2 * 1024 * 1024,
            MaxGeneratedExamples: 10,
            MaxGeneratedBytes: 256 * 1024);
}

public sealed record SchemaReferenceNode(
    string Subject,
    int Version,
    int SchemaId,
    RecordSchemaFormat Format,
    int Depth);

public sealed record SchemaReferenceEdge(
    string Name,
    string FromSubject,
    int FromVersion,
    string ToSubject,
    int ToVersion);

public sealed record SchemaReferenceGraph(
    string RootSubject,
    int RootVersion,
    IReadOnlyList<SchemaReferenceNode> Nodes,
    IReadOnlyList<SchemaReferenceEdge> Edges,
    bool HasCycle,
    long TotalSchemaBytes);

public sealed record SchemaCompatibilityExplanation(
    string Subject,
    SchemaCompatibilityMode Mode,
    bool IsInherited,
    string Scope,
    string Summary,
    IReadOnlyList<string> Rules);

public sealed record SchemaMockExample(
    int Index,
    string Json);

public sealed record SchemaMockResult(
    string Subject,
    int Version,
    RecordSchemaFormat Format,
    int Seed,
    IReadOnlyList<SchemaMockExample> Examples,
    long TotalBytes);

public sealed class SchemaDeveloperService
{
    private static readonly Regex ProtobufFieldPattern = new(
        @"(?m)^\s*(?:(optional|required|repeated)\s+)?(?<type>[A-Za-z_][A-Za-z0-9_.<>]*)\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*\d+\s*(?:\[[^\]]*\])?\s*;",
        RegexOptions.CultureInvariant | RegexOptions.Compiled,
        TimeSpan.FromMilliseconds(100));

    private static readonly Regex SensitiveNamePattern = new(
        @"(?:password|passwd|secret|token|credential|api[_-]?key|private[_-]?key|authorization)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled,
        TimeSpan.FromMilliseconds(100));

    private readonly ISchemaCatalogReadPort _schemas;
    private readonly SchemaDeveloperPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public SchemaDeveloperService(
        ISchemaCatalogReadPort schemas,
        SchemaDeveloperPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _schemas = schemas ?? throw new ArgumentNullException(nameof(schemas));
        _policy = policy ?? SchemaDeveloperPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (_policy.MaxReferenceNodes <= 0 ||
            _policy.MaxReferenceDepth <= 0 ||
            _policy.MaxSchemaBytes <= 0 ||
            _policy.MaxGeneratedExamples <= 0 ||
            _policy.MaxGeneratedBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(policy));
        }
    }

    public async Task<ReadViewResult<SchemaReferenceGraph>> BuildReferenceGraphAsync(
        string clusterId,
        string subject,
        int version,
        CancellationToken cancellationToken = default,
        Func<string, bool>? subjectAuthorization = null)
    {
        var normalizedSubject = NormalizeSubject(subject);
        if (normalizedSubject is null || version <= 0)
        {
            return Invalid<SchemaReferenceGraph>("invalid_schema_reference_graph_request");
        }

        var operation = Operation();
        var queue = new Queue<(string Subject, int Version, int Depth)>();
        var discovered = new HashSet<string>(StringComparer.Ordinal);
        var nodes = new List<SchemaReferenceNode>();
        var edges = new List<SchemaReferenceEdge>();
        long totalSchemaBytes = 0;

        queue.Enqueue((normalizedSubject, version, 0));
        discovered.Add(Key(normalizedSubject, version));

        while (queue.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var current = queue.Dequeue();

            if (current.Depth > _policy.MaxReferenceDepth)
            {
                return Bound<SchemaReferenceGraph>(
                    "schema_reference_depth_exceeded",
                    "Schema reference graph exceeded the configured depth bound.");
            }

            if (subjectAuthorization is not null &&
                !subjectAuthorization(current.Subject))
            {
                return Failed<SchemaReferenceGraph>(
                    ReadViewFailureCategory.Unauthorized,
                    "schema_reference_authorization_denied",
                    "Schema reference traversal reached a subject that the current operator is not authorized to read.",
                    false);
            }

            var result = await _schemas
                .GetVersionAsync(
                    clusterId,
                    current.Subject,
                    current.Version,
                    operation,
                    cancellationToken)
                .ConfigureAwait(false);

            if (!result.IsSuccess || result.Value is null)
            {
                return ReadViewResult<SchemaReferenceGraph>.Failed(result.Failure!);
            }

            var detail = result.Value;
            totalSchemaBytes += Encoding.UTF8.GetByteCount(detail.Schema.SchemaText);
            if (totalSchemaBytes > _policy.MaxSchemaBytes)
            {
                return Bound<SchemaReferenceGraph>(
                    "schema_reference_bytes_exceeded",
                    "Schema reference graph exceeded the configured byte bound.");
            }

            nodes.Add(
                new SchemaReferenceNode(
                    current.Subject,
                    current.Version,
                    detail.Schema.Id,
                    detail.Schema.Format,
                    current.Depth));

            if (nodes.Count > _policy.MaxReferenceNodes)
            {
                return Bound<SchemaReferenceGraph>(
                    "schema_reference_nodes_exceeded",
                    "Schema reference graph exceeded the configured node bound.");
            }

            foreach (var reference in detail.Schema.References)
            {
                var targetSubject = NormalizeSubject(reference.Subject);
                if (targetSubject is null || reference.Version <= 0)
                {
                    return Invalid<SchemaReferenceGraph>(
                        "invalid_schema_reference");
                }

                edges.Add(
                    new SchemaReferenceEdge(
                        reference.Name,
                        current.Subject,
                        current.Version,
                        targetSubject,
                        reference.Version));

                var targetKey = Key(targetSubject, reference.Version);
                if (discovered.Add(targetKey))
                {
                    if (discovered.Count > _policy.MaxReferenceNodes)
                    {
                        return Bound<SchemaReferenceGraph>(
                            "schema_reference_nodes_exceeded",
                            "Schema reference graph exceeded the configured node bound.");
                    }

                    queue.Enqueue(
                        (targetSubject, reference.Version, current.Depth + 1));
                }
            }
        }

        return ReadViewResult<SchemaReferenceGraph>.Success(
            new SchemaReferenceGraph(
                normalizedSubject,
                version,
                nodes
                    .OrderBy(item => item.Depth)
                    .ThenBy(item => item.Subject, StringComparer.Ordinal)
                    .ThenBy(item => item.Version)
                    .ToArray(),
                edges
                    .OrderBy(item => item.FromSubject, StringComparer.Ordinal)
                    .ThenBy(item => item.FromVersion)
                    .ThenBy(item => item.Name, StringComparer.Ordinal)
                    .ThenBy(item => item.ToSubject, StringComparer.Ordinal)
                    .ThenBy(item => item.ToVersion)
                    .ToArray(),
                HasCycle(edges),
                totalSchemaBytes));
    }

    public async Task<ReadViewResult<SchemaCompatibilityExplanation>>
        ExplainCompatibilityAsync(
            string clusterId,
            string subject,
            CancellationToken cancellationToken = default)
    {
        var normalizedSubject = NormalizeSubject(subject);
        if (normalizedSubject is null)
        {
            return Invalid<SchemaCompatibilityExplanation>(
                "invalid_schema_subject");
        }

        var result = await _schemas
            .GetCompatibilityAsync(
                clusterId,
                normalizedSubject,
                Operation(),
                cancellationToken)
            .ConfigureAwait(false);

        if (!result.IsSuccess || result.Value is null)
        {
            return ReadViewResult<SchemaCompatibilityExplanation>.Failed(
                result.Failure!);
        }

        var observation = result.Value;
        var (summary, rules) = DescribeCompatibility(observation.Mode);

        return ReadViewResult<SchemaCompatibilityExplanation>.Success(
            new SchemaCompatibilityExplanation(
                normalizedSubject,
                observation.Mode,
                observation.IsInherited,
                observation.IsInherited ? "global-inherited" : "subject",
                summary,
                rules));
    }

    public async Task<ReadViewResult<SchemaMockResult>> GenerateMockAsync(
        string clusterId,
        string subject,
        int version,
        int count,
        int? seed,
        CancellationToken cancellationToken = default)
    {
        var normalizedSubject = NormalizeSubject(subject);
        if (normalizedSubject is null ||
            version <= 0 ||
            count <= 0 ||
            count > _policy.MaxGeneratedExamples)
        {
            return Invalid<SchemaMockResult>(
                "invalid_schema_mock_request");
        }

        var result = await _schemas
            .GetVersionAsync(
                clusterId,
                normalizedSubject,
                version,
                Operation(),
                cancellationToken)
            .ConfigureAwait(false);

        if (!result.IsSuccess || result.Value is null)
        {
            return ReadViewResult<SchemaMockResult>.Failed(result.Failure!);
        }

        var detail = result.Value;
        var schemaBytes =
            Encoding.UTF8.GetByteCount(detail.Schema.SchemaText);
        if (schemaBytes > _policy.MaxSchemaBytes)
        {
            return Bound<SchemaMockResult>(
                "schema_mock_schema_bytes_exceeded",
                "Schema source exceeded the configured mock-generation byte bound.");
        }

        var resolvedSeed = seed ?? 0;
        var random = new Random(resolvedSeed);
        var examples = new List<SchemaMockExample>(count);
        long totalBytes = 0;

        try
        {
            for (var index = 0; index < count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var payload = detail.Schema.Format switch
                {
                    RecordSchemaFormat.Avro =>
                        GenerateAvro(detail.Schema.SchemaText, random, index),
                    RecordSchemaFormat.JsonSchema =>
                        GenerateJsonSchema(detail.Schema.SchemaText, random, index),
                    RecordSchemaFormat.Protobuf =>
                        GenerateProtobuf(detail.Schema.SchemaText, random, index),
                    _ => throw new NotSupportedException(
                        "Schema format is unsupported."),
                };

                var json = JsonSerializer.Serialize(payload);
                totalBytes += Encoding.UTF8.GetByteCount(json);
                if (totalBytes > _policy.MaxGeneratedBytes)
                {
                    return Bound<SchemaMockResult>(
                        "schema_mock_output_bytes_exceeded",
                        "Generated schema examples exceeded the configured byte bound.");
                }

                examples.Add(new SchemaMockExample(index, json));
            }
        }
        catch (JsonException)
        {
            return Failed<SchemaMockResult>(
                ReadViewFailureCategory.InvalidResponse,
                "schema_mock_invalid_schema",
                "Schema source could not be safely interpreted for mock generation.",
                false);
        }
        catch (RegexMatchTimeoutException)
        {
            return Failed<SchemaMockResult>(
                ReadViewFailureCategory.InvalidResponse,
                "schema_mock_parse_timeout",
                "Schema source exceeded the bounded parser budget.",
                false);
        }
        catch (NotSupportedException)
        {
            return Failed<SchemaMockResult>(
                ReadViewFailureCategory.Unsupported,
                "schema_mock_format_unsupported",
                "Schema format is not supported by bounded mock generation.",
                false);
        }

        return ReadViewResult<SchemaMockResult>.Success(
            new SchemaMockResult(
                normalizedSubject,
                version,
                detail.Schema.Format,
                resolvedSeed,
                examples,
                totalBytes));
    }

    private static object? GenerateAvro(
        string schemaText,
        Random random,
        int exampleIndex)
    {
        using var document = JsonDocument.Parse(
            schemaText,
            new JsonDocumentOptions
            {
                MaxDepth = 64,
                CommentHandling = JsonCommentHandling.Disallow,
                AllowTrailingCommas = false,
            });

        return AvroValue(
            document.RootElement,
            fieldName: null,
            random,
            exampleIndex,
            depth: 0);
    }

    private static object? AvroValue(
        JsonElement schema,
        string? fieldName,
        Random random,
        int exampleIndex,
        int depth)
    {
        if (depth > 16)
        {
            throw new JsonException("Avro schema nesting exceeded the bounded generator depth.");
        }

        if (schema.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in schema.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.String &&
                    string.Equals(
                        item.GetString(),
                        "null",
                        StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                return AvroValue(
                    item,
                    fieldName,
                    random,
                    exampleIndex,
                    depth + 1);
            }

            return null;
        }

        if (schema.ValueKind == JsonValueKind.String)
        {
            return PrimitiveValue(
                schema.GetString(),
                fieldName,
                random,
                exampleIndex);
        }

        if (schema.ValueKind != JsonValueKind.Object ||
            !schema.TryGetProperty("type", out var type))
        {
            throw new JsonException("Avro schema type is missing.");
        }

        if (type.ValueKind is JsonValueKind.Array or JsonValueKind.Object)
        {
            return AvroValue(
                type,
                fieldName,
                random,
                exampleIndex,
                depth + 1);
        }

        var typeName = type.GetString();
        switch (typeName?.ToLowerInvariant())
        {
            case "record":
            {
                if (!schema.TryGetProperty("fields", out var fields) ||
                    fields.ValueKind != JsonValueKind.Array)
                {
                    throw new JsonException("Avro record fields are invalid.");
                }

                var result = new Dictionary<string, object?>(StringComparer.Ordinal);
                var fieldCount = 0;
                foreach (var field in fields.EnumerateArray())
                {
                    if (++fieldCount > 128 ||
                        field.ValueKind != JsonValueKind.Object ||
                        !field.TryGetProperty("name", out var nameElement) ||
                        nameElement.ValueKind != JsonValueKind.String ||
                        !field.TryGetProperty("type", out var fieldType))
                    {
                        throw new JsonException("Avro field is invalid.");
                    }

                    var name = nameElement.GetString();
                    if (string.IsNullOrWhiteSpace(name))
                    {
                        throw new JsonException("Avro field name is invalid.");
                    }

                    result[name] = AvroValue(
                        fieldType,
                        name,
                        random,
                        exampleIndex,
                        depth + 1);
                }

                return result;
            }
            case "array":
                return schema.TryGetProperty("items", out var items)
                    ? new[]
                    {
                        AvroValue(
                            items,
                            fieldName,
                            random,
                            exampleIndex,
                            depth + 1),
                    }
                    : throw new JsonException("Avro array items are invalid.");
            case "map":
                return schema.TryGetProperty("values", out var values)
                    ? new Dictionary<string, object?>
                    {
                        ["key"] = AvroValue(
                            values,
                            fieldName,
                            random,
                            exampleIndex,
                            depth + 1),
                    }
                    : throw new JsonException("Avro map values are invalid.");
            case "enum":
                return "EXAMPLE";
            case "fixed":
                return string.Empty;
            default:
                return PrimitiveValue(
                    typeName,
                    fieldName,
                    random,
                    exampleIndex);
        }
    }

    private static object? GenerateJsonSchema(
        string schemaText,
        Random random,
        int exampleIndex)
    {
        using var document = JsonDocument.Parse(
            schemaText,
            new JsonDocumentOptions
            {
                MaxDepth = 64,
                CommentHandling = JsonCommentHandling.Disallow,
                AllowTrailingCommas = false,
            });

        return JsonSchemaValue(
            document.RootElement,
            fieldName: null,
            random,
            exampleIndex,
            depth: 0);
    }

    private static object? JsonSchemaValue(
        JsonElement schema,
        string? fieldName,
        Random random,
        int exampleIndex,
        int depth)
    {
        if (depth > 16)
        {
            throw new JsonException("JSON Schema nesting exceeded the bounded generator depth.");
        }

        if (schema.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("JSON Schema node is invalid.");
        }

        var typeName = ReadJsonSchemaType(schema);
        switch (typeName)
        {
            case "object":
            {
                var result = new Dictionary<string, object?>(StringComparer.Ordinal);
                if (!schema.TryGetProperty("properties", out var properties))
                {
                    return result;
                }

                if (properties.ValueKind != JsonValueKind.Object)
                {
                    throw new JsonException("JSON Schema properties are invalid.");
                }

                var propertyCount = 0;
                foreach (var property in properties.EnumerateObject())
                {
                    if (++propertyCount > 128)
                    {
                        throw new JsonException("JSON Schema property bound exceeded.");
                    }

                    result[property.Name] = JsonSchemaValue(
                        property.Value,
                        property.Name,
                        random,
                        exampleIndex,
                        depth + 1);
                }

                return result;
            }
            case "array":
                return schema.TryGetProperty("items", out var items) &&
                       items.ValueKind == JsonValueKind.Object
                    ? new[]
                    {
                        JsonSchemaValue(
                            items,
                            fieldName,
                            random,
                            exampleIndex,
                            depth + 1),
                    }
                    : Array.Empty<object?>();
            case "integer":
                return SensitiveName(fieldName)
                    ? 0
                    : random.Next(1, 10_000);
            case "number":
                return SensitiveName(fieldName)
                    ? 0d
                    : Math.Round(random.NextDouble() * 100d, 3);
            case "boolean":
                return false;
            case "null":
                return null;
            case "string":
            default:
                return SensitiveName(fieldName)
                    ? "[REDACTED]"
                    : $"example-{exampleIndex + 1}-{random.Next(1000, 9999)}";
        }
    }

    private static string ReadJsonSchemaType(JsonElement schema)
    {
        if (!schema.TryGetProperty("type", out var type))
        {
            return schema.TryGetProperty("properties", out _)
                ? "object"
                : "string";
        }

        if (type.ValueKind == JsonValueKind.String)
        {
            return type.GetString()?.ToLowerInvariant() ?? "string";
        }

        if (type.ValueKind == JsonValueKind.Array)
        {
            foreach (var candidate in type.EnumerateArray())
            {
                if (candidate.ValueKind != JsonValueKind.String)
                {
                    continue;
                }

                var value = candidate.GetString()?.ToLowerInvariant();
                if (value is not null && value != "null")
                {
                    return value;
                }
            }

            return "null";
        }

        throw new JsonException("JSON Schema type is invalid.");
    }

    private static object GenerateProtobuf(
        string schemaText,
        Random random,
        int exampleIndex)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        var count = 0;

        foreach (Match match in ProtobufFieldPattern.Matches(schemaText))
        {
            if (++count > 128)
            {
                throw new JsonException("Protobuf field bound exceeded.");
            }

            var name = match.Groups["name"].Value;
            var type = match.Groups["type"].Value;
            var repeated = string.Equals(
                match.Groups[1].Value,
                "repeated",
                StringComparison.Ordinal);

            var value = PrimitiveValue(
                type,
                name,
                random,
                exampleIndex);

            result[name] = repeated
                ? new[] { value }
                : value;
        }

        if (result.Count == 0)
        {
            throw new JsonException("No bounded Protobuf fields were recognized.");
        }

        return result;
    }

    private static object? PrimitiveValue(
        string? typeName,
        string? fieldName,
        Random random,
        int exampleIndex)
    {
        var normalized = typeName?.Trim().ToLowerInvariant();
        var sensitive = SensitiveName(fieldName);

        return normalized switch
        {
            "null" => null,
            "bool" or "boolean" => false,
            "int" or "int32" or "int64" or "uint32" or "uint64" or
                "sint32" or "sint64" or "fixed32" or "fixed64" or
                "sfixed32" or "sfixed64" =>
                    sensitive ? 0 : random.Next(1, 10_000),
            "float" or "double" or "number" =>
                sensitive ? 0d : Math.Round(random.NextDouble() * 100d, 3),
            "bytes" => string.Empty,
            "string" =>
                sensitive
                    ? "[REDACTED]"
                    : $"example-{exampleIndex + 1}-{random.Next(1000, 9999)}",
            _ =>
                sensitive
                    ? "[REDACTED]"
                    : $"example-{exampleIndex + 1}",
        };
    }

    private static bool SensitiveName(string? name) =>
        !string.IsNullOrWhiteSpace(name) &&
        SensitiveNamePattern.IsMatch(name);

    private static bool HasCycle(
        IReadOnlyList<SchemaReferenceEdge> edges)
    {
        var adjacency = edges
            .GroupBy(
                edge => Key(edge.FromSubject, edge.FromVersion),
                StringComparer.Ordinal)
            .ToDictionary(
                group => group.Key,
                group => group
                    .Select(edge => Key(edge.ToSubject, edge.ToVersion))
                    .Distinct(StringComparer.Ordinal)
                    .ToArray(),
                StringComparer.Ordinal);

        var visiting = new HashSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.Ordinal);

        bool Visit(string node)
        {
            if (visiting.Contains(node))
            {
                return true;
            }

            if (!visited.Add(node))
            {
                return false;
            }

            visiting.Add(node);
            if (adjacency.TryGetValue(node, out var targets))
            {
                foreach (var target in targets)
                {
                    if (Visit(target))
                    {
                        return true;
                    }
                }
            }

            visiting.Remove(node);
            return false;
        }

        return adjacency.Keys.Any(Visit);
    }

    private static (string Summary, IReadOnlyList<string> Rules)
        DescribeCompatibility(SchemaCompatibilityMode mode) =>
        mode switch
        {
            SchemaCompatibilityMode.None => (
                "No compatibility constraint is enforced by the observed policy.",
                new[]
                {
                    "New schemas may break existing readers or writers.",
                    "Use explicit review before changing contracts relied on by deployed clients.",
                }),
            SchemaCompatibilityMode.Backward => (
                "New schema versions are checked so new readers can consume data written with the previous schema.",
                new[]
                {
                    "Validation is against the immediately previous schema version.",
                    "This does not prove compatibility with every historical version.",
                }),
            SchemaCompatibilityMode.BackwardTransitive => (
                "New schema versions are checked for backward compatibility with all retained historical versions.",
                new[]
                {
                    "New readers should remain able to consume data written by historical schemas.",
                    "The provider remains authoritative for the actual compatibility decision.",
                }),
            SchemaCompatibilityMode.Forward => (
                "New schema versions are checked so existing readers can consume data written with the new schema.",
                new[]
                {
                    "Validation is against the immediately previous schema version.",
                    "This does not prove compatibility with every historical version.",
                }),
            SchemaCompatibilityMode.ForwardTransitive => (
                "New schema versions are checked for forward compatibility with all retained historical versions.",
                new[]
                {
                    "Historical readers should remain able to consume data written by the new schema.",
                    "The provider remains authoritative for the actual compatibility decision.",
                }),
            SchemaCompatibilityMode.Full => (
                "The observed policy combines backward and forward checks against the previous schema version.",
                new[]
                {
                    "Both reader and writer compatibility are checked against the immediately previous version.",
                    "This does not imply transitive compatibility across all historical versions.",
                }),
            SchemaCompatibilityMode.FullTransitive => (
                "The observed policy combines backward and forward checks across retained historical versions.",
                new[]
                {
                    "Reader and writer compatibility are checked transitively.",
                    "The provider remains authoritative for the actual compatibility decision.",
                }),
            _ => (
                "The provider did not expose a compatibility mode that Kafdeck can explain safely.",
                new[]
                {
                    "Treat compatibility guarantees as unknown.",
                    "Do not infer permissive or restrictive behavior from an unknown provider value.",
                }),
        };

    private ReadViewOperationContext Operation() =>
        new(
            _timeProvider.GetUtcNow().Add(_policy.OperationTimeout),
            _policy.MaxReferenceNodes,
            _policy.MaxSchemaBytes);

    private static string? NormalizeSubject(string? subject)
    {
        if (string.IsNullOrWhiteSpace(subject))
        {
            return null;
        }

        var normalized = subject.Trim();
        return normalized.Length <= 512 &&
               !normalized.Any(char.IsControl)
            ? normalized
            : null;
    }

    private static string Key(string subject, int version) =>
        $"{subject}\u001f{version}";

    private static ReadViewResult<T> Invalid<T>(string code) =>
        Failed<T>(
            ReadViewFailureCategory.InvalidRequest,
            code,
            "Schema developer-tooling request is invalid.",
            false);

    private static ReadViewResult<T> Bound<T>(
        string code,
        string message) =>
        Failed<T>(
            ReadViewFailureCategory.ResponseTooLarge,
            code,
            message,
            false);

    private static ReadViewResult<T> Failed<T>(
        ReadViewFailureCategory category,
        string code,
        string message,
        bool retryable) =>
        ReadViewResult<T>.Failed(
            new ReadViewFailure(
                category,
                code,
                message,
                retryable));
}
