using System.Reflection;
using System.Text.Json.Nodes;
using Kafdeck.Contracts;

namespace Kafdeck.Api;

internal static class BearerReadOpenApi
{
    private static readonly string[] Assemblies = ["Kafdeck.Api", "Kafdeck.Core", "Kafdeck.Modules.Clusters", "Kafdeck.Modules.Topics",
        "Kafdeck.Modules.Consumers", "Kafdeck.Modules.Schemas", "Kafdeck.Modules.Records", "Kafdeck.Modules.Administration", "Kafdeck.Infrastructure.Persistence"];
    private static readonly Lazy<Type[]> Types = new(() => Assemblies.Select(Assembly.Load).SelectMany(a => a.GetTypes()).ToArray());
    internal static Type TypeNamed(string name)
    {
        if (name.EndsWith("[]", StringComparison.Ordinal)) return TypeNamed(name[..^2]).MakeArrayType();
        return Types.Value.Single(t => t.Name == name);
    }

    internal static Type? ResponseType(NamedReadContract contract)
    {
        if (contract.Name == "v03-records-export") return TypeNamed("ExportRow").MakeArrayType();
        if (contract.Name == "v03-records-tail") return typeof(string);
        var name = contract.Response ?? contract.Name switch
        {
            "v01-clusters-list" or "v01-clusters-detail" => "ClusterData",
            "v01-clusters-capabilities" => null,
            "v01-brokers-list" => "BrokerProjection[]", "v01-brokers-detail" => "BrokerProjection",
            "v01-topics-list" => "TopicPageData", "v01-topics-detail" => "TopicDetailData", "v01-topics-partitions" => "PartitionProjection[]",
            "v04-consumer-groups-list" => "ConsumerGroupSummary[]", "v04-consumer-groups-detail" => "ConsumerGroupDetail",
            "v04-consumer-groups-lag" => "ConsumerLagProjection", "v04-consumer-groups-diagnostics" => "ConsumerDiagnosticsProjection",
            "v04-schema-subjects-list" => "SchemaSubjectSummary[]", "v04-schema-versions-list" => "SchemaVersionSummary[]",
            "v04-schema-version-detail" => "SchemaVersionDetail", "v04-schema-compatibility" => "SchemaCompatibilityObservation", "v04-schema-diff" => "SchemaDiffResult",
            "v07-schema-registry-capabilities" => "SchemaRegistryCapabilitiesData",
            "v08-notification-governance-destinations" => "NotificationManagedDestinationList",
            "v08-notification-governance-audit" => "NotificationManagedAuditList",
            "v08-notification-management-capability" => "NotificationManagementCapabilityData",
            "v08-notification-product-event-status" => "NotificationProductEventSourceStatus",
            "v08-bearer-read-capabilities" => nameof(NamedReadInventoryData),
            _ => null,
        };
        if (name is null) return null;
        var type = TypeNamed(name);
        if (contract.Name.StartsWith("v01-", StringComparison.Ordinal)) return typeof(ApiEnvelope<>).MakeGenericType(type);
        if (contract.Name.StartsWith("v04-", StringComparison.Ordinal) || contract.Name.StartsWith("v07-connect-", StringComparison.Ordinal) ||
            contract.Name is "v07-ksql-query" or "v07-streams-applications" or "v07-streams-topology" or "v07-streams-state-stores" or
                "v07-lineage" or "v07-schema-reference-graph" or "v07-schema-compatibility-explanation" or "v07-schema-mock")
            return typeof(ReadViewApiEnvelope<>).MakeGenericType(type);
        return type;
    }

    internal static void Extend(JsonObject paths, JsonObject schemas)
    {
        foreach (var contract in KafdeckReadContracts.All)
        {
            var parameters = new JsonArray();
            foreach (var segment in contract.Path.Split('/').Where(s => s.StartsWith('{')))
                parameters.Add(new JsonObject { ["name"] = segment[1..^1], ["in"] = "path", ["required"] = true,
                    ["schema"] = new JsonObject { ["type"] = "string", ["minLength"] = 1, ["maxLength"] = 512 } });
            foreach (var key in contract.QueryKeys.Split(',', StringSplitOptions.RemoveEmptyEntries))
                parameters.Add(new JsonObject { ["name"] = key, ["in"] = "query", ["required"] = false,
                    ["schema"] = new JsonObject { ["type"] = "string", ["maxLength"] = 4096 } });
            JsonObject responseSchema = ResponseType(contract) is { } response ? BearerFamilyOpenApi.Schema(response, schemas) :
                contract.Name == "v01-clusters-capabilities" ? BearerFamilyOpenApi.Schema(typeof(ApiEnvelope<IReadOnlyDictionary<string, string>>), schemas) :
                new JsonObject { ["type"] = "object", ["description"] = "Existing bounded metadata result; see the versioned UI contract.", ["additionalProperties"] = true };
            if (contract.Name == "v01-clusters-list") responseSchema = new JsonObject { ["type"] = "object", ["additionalProperties"] = false,
                ["properties"] = new JsonObject { ["data"] = new JsonObject { ["type"] = "array", ["items"] = responseSchema } } };
            var media = contract.Name == "v03-records-tail" ? "text/event-stream" : "application/json";
            var content = new JsonObject { [media] = new JsonObject { ["schema"] = responseSchema } };
            if (contract.Name == "v03-records-tail")
                content[media]!["x-event-schema"] = BearerFamilyOpenApi.Schema(TypeNamed("RecordTailSafeFrame"), schemas);
            if (contract.Name == "v03-records-export")
            {
                content["text/csv"] = new JsonObject { ["schema"] = new JsonObject { ["type"] = "string" } };
                content["application/x-ndjson"] = new JsonObject { ["schema"] = new JsonObject { ["type"] = "string" } };
            }
            var operation = new JsonObject { ["operationId"] = contract.Name, ["summary"] = "Read " + contract.Name,
                ["description"] = "Explicit current registration plus current exact backend " + contract.Action + ". Scoped collections remain filtered; sensitive records remain bounded and masked. POST read tools have a closed 65536-byte body and no effect authority. CLI: views " + contract.Name + ".",
                ["parameters"] = parameters, ["responses"] = new JsonObject
                {
                    ["200"] = new JsonObject { ["description"] = "Typed bounded read result.", ["content"] = content },
                    ["400"] = new JsonObject { ["description"] = "Invalid closed query or body" },
                    ["401"] = new JsonObject { ["description"] = "Missing or rejected bearer registration" },
                    ["403"] = new JsonObject { ["description"] = "Current resource scope denied" },
                    ["501"] = new JsonObject { ["description"] = "Inherited backend/provider capability unsupported" },
                } };
            if (BearerReadBoundary.RequestType(contract) is { } request)
                operation["requestBody"] = new JsonObject { ["required"] = true, ["content"] = new JsonObject
                    { ["application/json"] = new JsonObject { ["schema"] = BearerFamilyOpenApi.Schema(request, schemas) } } };
            if (paths[contract.Path] is not JsonObject path) { path = new JsonObject(); paths[contract.Path] = path; }
            path[contract.Method] = operation;
        }
    }
}

public sealed record NamedReadInventoryData(int Version, bool AuthorityFromCapability, IReadOnlyList<NamedReadContract> Contracts);
