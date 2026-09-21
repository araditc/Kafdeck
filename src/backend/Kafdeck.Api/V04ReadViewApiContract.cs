namespace Kafdeck.Api;

public static class V04ReadViewApiContract
{
    public static readonly IReadOnlyList<ApiRouteDefinition> ProductRoutes =
    [
        new("GET", "/api/v1/clusters/{clusterId}/consumer-groups", "v04-consumer-groups-list"),
        new("GET", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}", "v04-consumer-groups-detail"),
        new("GET", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/lag", "v04-consumer-groups-lag"),
        new("GET", "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/diagnostics", "v04-consumer-groups-diagnostics"),
        new("GET", "/api/v1/clusters/{clusterId}/schemas/subjects", "v04-schema-subjects-list"),
        new("GET", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/versions", "v04-schema-versions-list"),
        new("GET", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/versions/{version:int}", "v04-schema-version-detail"),
        new("GET", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/compatibility", "v04-schema-compatibility"),
        new("GET", "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/diff", "v04-schema-diff"),
        new("GET", "/api/v1/clusters/{clusterId}/connect", "v04-connect-info"),
        new("GET", "/api/v1/clusters/{clusterId}/connect/connectors", "v04-connect-connectors-list"),
        new("GET", "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}", "v04-connect-connectors-detail"),
        new("GET", "/api/v1/clusters/{clusterId}/ksql", "v04-ksql-info"),
        new("GET", "/api/v1/clusters/{clusterId}/ksql/metadata", "v04-ksql-metadata"),
        new("GET", "/api/v1/clusters/{clusterId}/catalog/topics/{topicName}", "v04-topic-catalog-detail"),
    ];

    public const string OpenApiJson = """
{
  "openapi": "3.1.0",
  "info": {
    "title": "Kafdeck v0.4 Read Views API",
    "version": "0.4.0",
    "description": "Read-only consumer, schema, Kafka Connect, ksqlDB and topic-catalog views. No consumer offset, schema, connector, ksqlDB, Kafka topic/configuration, or record mutation is exposed. Record payload permissions remain separate."
  },
  "paths": {
    "/api/v1/clusters/{clusterId}/consumer-groups": { "get": { "operationId": "v04-consumer-groups-list", "responses": { "200": { "description": "Authorized bounded consumer groups" }, "403": { "description": "consumer.read denied" } } } },
    "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}": { "get": { "operationId": "v04-consumer-groups-detail", "responses": { "200": { "description": "Consumer group members and assignments" }, "403": { "description": "consumer.read denied" } } } },
    "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/lag": { "get": { "operationId": "v04-consumer-groups-lag", "responses": { "200": { "description": "Committed/end offsets and bounded lag projection" } } } },
    "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/diagnostics": { "get": { "operationId": "v04-consumer-groups-diagnostics", "responses": { "200": { "description": "Evidence-based diagnostics; unavailable evidence is explicit" } } } },
    "/api/v1/clusters/{clusterId}/schemas/subjects": { "get": { "operationId": "v04-schema-subjects-list", "responses": { "200": { "description": "Authorized bounded Schema Registry subjects" } } } },
    "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/versions": { "get": { "operationId": "v04-schema-versions-list", "responses": { "200": { "description": "Schema versions and references" } } } },
    "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/versions/{version}": { "get": { "operationId": "v04-schema-version-detail", "responses": { "200": { "description": "Read-only schema version detail" } } } },
    "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/compatibility": { "get": { "operationId": "v04-schema-compatibility", "responses": { "200": { "description": "Read-only compatibility configuration observation" } } } },
    "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/diff": { "get": { "operationId": "v04-schema-diff", "parameters": [ { "name": "leftVersion", "in": "query", "required": true, "schema": { "type": "integer", "minimum": 1 } }, { "name": "rightVersion", "in": "query", "required": true, "schema": { "type": "integer", "minimum": 1 } } ], "responses": { "200": { "description": "Bounded local schema diff" } } } },
    "/api/v1/clusters/{clusterId}/connect": { "get": { "operationId": "v04-connect-info", "responses": { "200": { "description": "Configured Kafka Connect worker identity" } } } },
    "/api/v1/clusters/{clusterId}/connect/connectors": { "get": { "operationId": "v04-connect-connectors-list", "responses": { "200": { "description": "Authorized bounded connector names" } } } },
    "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}": { "get": { "operationId": "v04-connect-connectors-detail", "responses": { "200": { "description": "Connector/task status and server-redacted safe configuration" } } } },
    "/api/v1/clusters/{clusterId}/ksql": { "get": { "operationId": "v04-ksql-info", "responses": { "200": { "description": "GET-only ksqlDB server info/health" } } } },
    "/api/v1/clusters/{clusterId}/ksql/metadata": { "get": { "operationId": "v04-ksql-metadata", "responses": { "501": { "description": "Unsupported when metadata would require statement execution" } } } },
    "/api/v1/clusters/{clusterId}/catalog/topics/{topicName}": { "get": { "operationId": "v04-topic-catalog-detail", "responses": { "200": { "description": "Configuration-owned descriptive topic catalog metadata" }, "404": { "description": "Catalog entry not configured" } } } }
  },
  "components": {
    "securitySchemes": {
      "oidcSession": { "type": "apiKey", "in": "cookie", "name": "Kafdeck.Session" },
      "deploymentToken": { "type": "apiKey", "in": "header", "name": "X-Kafdeck-Access-Token" }
    }
  },
  "security": [ { "oidcSession": [] }, { "deploymentToken": [] } ]
}
""";
}
