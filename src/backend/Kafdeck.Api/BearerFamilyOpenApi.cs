using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Kafdeck.Api;

internal static class BearerFamilyOpenApi
{
    public static string Extend(string baseline)
    {
        var document = JsonNode.Parse(baseline)!;
        var paths = document["paths"]!.AsObject();
        var schemas = document["components"]!["schemas"]!.AsObject();
        paths["/api/v1/automation/capabilities/mutations"] = new JsonObject { ["get"] = new JsonObject
        {
            ["operationId"] = "v08-bearer-mutation-capabilities", ["summary"] = "Read the complete supported-UI mutation capability inventory",
            ["description"] = "Requires current SystemRead. Lists every operation kind and preserves inherited Blocked/Unsupported reasons. Compiled named contracts do not imply provider capability or authorization, and this read grants no effect authority.",
            ["responses"] = new JsonObject { ["200"] = new JsonObject { ["description"] = "Typed bounded capability inventory.",
                ["content"] = new JsonObject { ["application/json"] = new JsonObject { ["schema"] = Schema(typeof(BearerMutationCapabilityInventory), schemas) } } },
                ["401"] = Response("Unauthenticated"), ["403"] = Response("Denied") },
        } };
        foreach (var contract in BearerFamilyRoutes.Contracts)
        {
            var parameters = new JsonArray();
            foreach (var part in contract.Path.Split('/').Where(p => p.StartsWith('{') && p.EndsWith('}')))
                parameters.Add(new JsonObject { ["name"] = part[1..^1], ["in"] = "path", ["required"] = true,
                    ["schema"] = new JsonObject { ["type"] = "string", ["minLength"] = 1, ["maxLength"] = 512 } });
            if (!contract.Execution && !contract.Read) parameters.Add(new JsonObject { ["name"] = "Idempotency-Key", ["in"] = "header", ["required"] = true,
                ["schema"] = new JsonObject { ["type"] = "string", ["minLength"] = 1, ["maxLength"] = 256 } });
            var operation = new JsonObject
            {
                ["operationId"] = contract.Name, ["summary"] = contract.Execution ? "Execute exact stored family intent with bound material" : "Preview exact typed " + contract.Kind,
                ["description"] = "Default-off exact family registration AND current backend action/resource permissions. Maximum 65536-byte closed duplicate-free request. Server-owned immutable plan, actor, risk, current policy and provider preconditions. CRITICAL requires a distinct registered human. Material execution revalidates hashes and never persists raw secrets. No automatic retry of unknown effects. Provider acceptance is tracked separately.",
                ["parameters"] = parameters,
                ["responses"] = new JsonObject
                {
                    ["200"] = Response("Status"), ["201"] = Response("Status"), ["400"] = Response("Invalid"),
                    ["401"] = Response("Unauthenticated"), ["403"] = Response("Denied"), ["404"] = Response("NotFound"),
                    ["409"] = Response("Conflict"), ["413"] = Response("TooLarge"), ["501"] = Response("Unsupported"), ["503"] = Response("Unavailable"),
                },
            };
            if (contract.RequestType is { } type)
            {
                operation["requestBody"] = new JsonObject { ["required"] = true, ["content"] = new JsonObject
                    { ["application/json"] = new JsonObject { ["schema"] = Schema(type, schemas) } } };
            }
            if (contract.ResponseType is { } responseType)
                operation["responses"]!["200"] = new JsonObject { ["description"] = "Typed runtime progress. Activation does not prove completion of background effects.",
                    ["content"] = new JsonObject { ["application/json"] = new JsonObject { ["schema"] = Schema(responseType, schemas) } } };
            if (contract.OriginalName is "v07-data-job-reconcile" or "v07-data-generator-reconcile")
                operation["description"] = "Currently fails closed with 501: bare provenNonApplication text cannot authorize a retry. Durable evidence-bound independent reconciliation review remains required.";
            if (paths[contract.Path] is not JsonObject path) { path = new JsonObject(); paths[contract.Path] = path; }
            path[contract.Method] = operation;
        }
        var kinds = new JsonArray();
        foreach (var kind in Enum.GetValues<Kafdeck.Modules.Administration.MutationOperationKind>().Where(BearerFamilyRoutes.IsKind))
            kinds.Add(JsonNamingPolicy.CamelCase.ConvertName(kind.ToString()));
        schemas["MutationStatus"]!["properties"]!["operationKind"]!["enum"] = kinds;
        schemas["MutationStatus"]!["properties"]!["reconciliationReview"] = new JsonObject { ["anyOf"] = new JsonArray(
            Schema(typeof(Kafdeck.Modules.Administration.JobReconciliationPlan), schemas), new JsonObject { ["type"] = "null" }) };
        BearerReadOpenApi.Extend(paths, schemas);
        document["info"]!["title"] = "Kafdeck v0.8 governed bearer mutation contracts";
        document["info"]!["description"] = "Closed default-off topic, record, consumer, schema and Connect family routes using existing governed planners and durable execution. Exact registered subject/client/action AND direct backend RBAC; separate human-only approval activation and current MutationApprove plus every underlying action scope. Cookie/deployment collisions denied. Immutable actor/policy/current-state binding and unknown outcomes retained. This contract is not complete provider acceptance or full G2 closure; missing runtime families are not admitted.";
        return document.ToJsonString();
    }

    private static JsonObject Response(string name) => new() { ["$ref"] = "#/components/responses/" + name };
    private static string SchemaName(Type type) => type.IsGenericType ? type.Name.Split('`')[0] + "Of" + string.Join("And", type.GetGenericArguments().Select(SchemaName)) : type.Name;
    private static JsonObject Reference(Type type) => new() { ["$ref"] = "#/components/schemas/Family" + SchemaName(type) };
    internal static JsonObject Schema(Type type, JsonObject schemas)
    {
        if (Nullable.GetUnderlyingType(type) is { } nullable)
            return new() { ["anyOf"] = new JsonArray(Schema(nullable, schemas), new JsonObject { ["type"] = "null" }) };
        if (type == typeof(JsonElement) || type == typeof(object)) return new JsonObject();
        if (type == typeof(ReadOnlyMemory<byte>) || type == typeof(Memory<byte>)) return new() { ["type"] = "string", ["contentEncoding"] = "base64" };
        // Whole request/query bounds are stated on their named contracts.
        // A DTO type alone does not establish a response string or row limit.
        if (type == typeof(string)) return new() { ["type"] = "string" };
        if (type == typeof(byte[])) return new() { ["type"] = "string", ["contentEncoding"] = "base64" };
        if (type == typeof(bool)) return new() { ["type"] = "boolean" };
        if (type == typeof(TimeSpan)) return new() { ["type"] = "string", ["pattern"] = @"^-?(?:[0-9]+\.)?[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,7})?$" };
        if (type == typeof(DateTimeOffset) || type == typeof(DateTime)) return new() { ["type"] = "string", ["format"] = "date-time" };
        if (type == typeof(Guid)) return new() { ["type"] = "string", ["format"] = "uuid" };
        if (type.IsEnum) return new() { ["anyOf"] = new JsonArray(new JsonObject { ["type"] = "string", ["enum"] = new JsonArray(Enum.GetNames(type).Select(n => JsonValue.Create(JsonNamingPolicy.CamelCase.ConvertName(n))).ToArray()) },
            new JsonObject { ["type"] = "integer", ["enum"] = new JsonArray(Enum.GetValues(type).Cast<object>().Select(n => JsonValue.Create(Convert.ToInt32(n))).ToArray()) }) };
        if (type.IsPrimitive || type == typeof(decimal)) return new() { ["type"] = type == typeof(float) || type == typeof(double) || type == typeof(decimal) ? "number" : "integer" };
        var dictionary = type.GetInterfaces().Append(type).FirstOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>));
        if (dictionary is not null) return new() { ["type"] = "object", ["additionalProperties"] = Schema(dictionary.GetGenericArguments()[1], schemas) };
        var sequence = type.GetInterfaces().Append(type).FirstOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        if (sequence is not null) return new() { ["type"] = "array", ["items"] = Schema(sequence.GetGenericArguments()[0], schemas) };
        var key = "Family" + SchemaName(type);
        if (!schemas.ContainsKey(key))
        {
            var properties = new JsonObject();
            var required = new JsonArray();
            schemas[key] = new JsonObject { ["type"] = "object", ["additionalProperties"] = false, ["properties"] = properties, ["required"] = required };
            var constructor = type.GetConstructors().OrderByDescending(c => c.GetParameters().Length).FirstOrDefault();
            var nullability = new NullabilityInfoContext();
            foreach (var property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (property.GetIndexParameters().Length != 0) continue;
                var name = property.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? JsonNamingPolicy.CamelCase.ConvertName(property.Name);
                var propertySchema = Schema(property.PropertyType, schemas);
                properties[name] = !property.PropertyType.IsValueType && nullability.Create(property).ReadState == NullabilityState.Nullable
                    ? new JsonObject { ["anyOf"] = new JsonArray(propertySchema, new JsonObject { ["type"] = "null" }) }
                    : propertySchema;
                if (constructor?.GetParameters().Any(p => string.Equals(p.Name, property.Name, StringComparison.OrdinalIgnoreCase) && !p.HasDefaultValue) == true)
                    required.Add(name);
            }
        }
        return Reference(type);
    }
}
