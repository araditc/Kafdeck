namespace Kafdeck.Api;

public static class KafdeckV05OpenApi
{
    public const string DocumentPath = "/api/v1/openapi/v0.5.json";

    public static WebApplication MapKafdeckV05OpenApi(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet(DocumentPath, () => Results.Text(
                Document,
                "application/vnd.oai.openapi+json;version=3.1.0"))
            .WithName("v05-openapi");

        return app;
    }

    // Intentionally static: this contract is reviewed together with the admitted W39 routes.
    // It does not discover or expose provider-specific endpoints at runtime.
    public const string Document = """
{
  "openapi": "3.1.0",
  "info": {
    "title": "Kafdeck v0.5 Safe Administration API",
    "version": "0.5.0",
    "description": "Governed mutation API. State-changing requests require an authenticated OIDC operator session and the X-Kafdeck-CSRF antiforgery header. Preview admission also requires Idempotency-Key. No generic provider command surface is exposed."
  },
  "servers": [{ "url": "/" }],
  "tags": [
    { "name": "Mutation lifecycle" },
    { "name": "Topic administration" },
    { "name": "Record production" },
    { "name": "Consumer administration" },
    { "name": "Schema Registry" },
    { "name": "Kafka Connect" },
    { "name": "Controlled purge" }
  ],
  "paths": {
    "/api/v1/auth/csrf": {
      "get": {
        "summary": "Issue an antiforgery request token for the current cookie session",
        "responses": { "200": { "description": "Antiforgery token", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/CsrfToken" } } } } }
      }
    },
    "/api/v1/mutations/approvals": {
      "get": {
        "tags": ["Mutation lifecycle"],
        "summary": "List currently authorized independent approval candidates",
        "parameters": [{ "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 100, "default": 50 } }],
        "responses": {
          "200": { "description": "Bounded approval inbox", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MutationApprovalInbox" } } } },
          "401": { "$ref": "#/components/responses/Problem" }
        }
      }
    },
    "/api/v1/mutations/{operationId}": {
      "get": {
        "tags": ["Mutation lifecycle"],
        "summary": "Read safe mutation status",
        "parameters": [{ "$ref": "#/components/parameters/OperationId" }],
        "responses": {
          "200": { "$ref": "#/components/responses/MutationStatus" },
          "401": { "$ref": "#/components/responses/Problem" },
          "403": { "$ref": "#/components/responses/Problem" },
          "404": { "$ref": "#/components/responses/Problem" }
        }
      }
    },
    "/api/v1/mutations/{operationId}/confirm": {
      "post": {
        "tags": ["Mutation lifecycle"],
        "summary": "Confirm the admitted preview",
        "parameters": [{ "$ref": "#/components/parameters/OperationId" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MutationConfirmRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } }
      }
    },
    "/api/v1/mutations/{operationId}/approve": { "post": { "$ref": "#/components/pathItems/ReviewAction" } },
    "/api/v1/mutations/{operationId}/reject": { "post": { "$ref": "#/components/pathItems/ReviewAction" } },
    "/api/v1/mutations/{operationId}/cancel": {
      "post": {
        "tags": ["Mutation lifecycle"],
        "summary": "Cancel before dispatch",
        "parameters": [{ "$ref": "#/components/parameters/OperationId" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MutationReviewRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/MutationStatus" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } }
      }
    },
    "/api/v1/mutations/{operationId}/execute": { "post": { "$ref": "#/components/pathItems/ExecuteWithoutMaterial" } },

    "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/create/preview": { "post": { "$ref": "#/components/pathItems/TopicCreatePreview" } },
    "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/alter/preview": { "post": { "$ref": "#/components/pathItems/TopicAlterPreview" } },
    "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/increase-partitions/preview": { "post": { "$ref": "#/components/pathItems/TopicIncreasePartitionsPreview" } },
    "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/delete/preview": { "post": { "$ref": "#/components/pathItems/TopicDeletePreview" } },

    "/api/v1/clusters/{clusterId}/topics/{topicName}/mutations/produce/preview": {
      "post": {
        "tags": ["Record production"],
        "summary": "Preview bounded record production",
        "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/TopicName" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RecordProductionPreviewRequest" } } } },
        "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } }
      }
    },
    "/api/v1/mutations/{operationId}/record-production/execute": {
      "post": {
        "tags": ["Record production"],
        "summary": "Execute record production with re-submitted ephemeral material",
        "description": "Raw key/value/header bytes are request-scoped and must digest-match the admitted preview. They are not durable mutation state.",
        "parameters": [{ "$ref": "#/components/parameters/OperationId" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RecordProductionExecuteRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } }
      }
    },

    "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/mutations/offsets/preview": { "post": { "$ref": "#/components/pathItems/ConsumerOffsetPreview" } },
    "/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/mutations/delete/preview": { "post": { "$ref": "#/components/pathItems/ConsumerDeletePreview" } },

    "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/register/preview": { "post": { "$ref": "#/components/pathItems/SchemaRegisterPreview" } },
    "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/compatibility/preview": { "post": { "$ref": "#/components/pathItems/SchemaCompatibilityPreview" } },
    "/api/v1/clusters/{clusterId}/schemas/mutations/compatibility/preview": { "post": { "$ref": "#/components/pathItems/SchemaGlobalCompatibilityPreview" } },
    "/api/v1/clusters/{clusterId}/schemas/subjects/{subject}/mutations/delete/preview": { "post": { "$ref": "#/components/pathItems/SchemaDeletePreview" } },
    "/api/v1/mutations/{operationId}/schema-create/execute": {
      "post": {
        "tags": ["Schema Registry"],
        "summary": "Execute schema registration with re-submitted ephemeral schema source",
        "parameters": [{ "$ref": "#/components/parameters/OperationId" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SchemaCreateExecuteRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } }
      }
    },

    "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/create/preview": { "post": { "$ref": "#/components/pathItems/ConnectConfigurationPreview" } },
    "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/update/preview": { "post": { "$ref": "#/components/pathItems/ConnectConfigurationPreview" } },
    "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/control/preview": { "post": { "$ref": "#/components/pathItems/ConnectControlPreview" } },
    "/api/v1/clusters/{clusterId}/connect/connectors/{connectorName}/mutations/delete/preview": { "post": { "$ref": "#/components/pathItems/ConnectDeletePreview" } },
    "/api/v1/mutations/{operationId}/connect-configuration/execute": {
      "post": {
        "tags": ["Kafka Connect"],
        "summary": "Execute connector create/update with re-submitted ephemeral configuration",
        "parameters": [{ "$ref": "#/components/parameters/OperationId" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ConnectConfigurationRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } }
      }
    },
    "/api/v1/mutations/{operationId}/connect/execute": { "post": { "$ref": "#/components/pathItems/ExecuteWithoutMaterial" } },

    "/api/v1/clusters/{clusterId}/records/purge/preview": {
      "post": {
        "tags": ["Controlled purge"],
        "summary": "Preview irreversible controlled DeleteRecords targets",
        "description": "CRITICAL destructive operation. Targets are materialized to explicit partition/offset values and there is no undo claim.",
        "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RecordsPurgePreviewRequest" } } } },
        "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } }
      }
    }
  },
  "components": {
    "parameters": {
      "OperationId": { "name": "operationId", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } },
      "ClusterId": { "name": "clusterId", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 256 } },
      "TopicName": { "name": "topicName", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1 } },
      "GroupId": { "name": "groupId", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1 } },
      "Subject": { "name": "subject", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1 } },
      "ConnectorName": { "name": "connectorName", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1 } },
      "IdempotencyKey": { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 256 } },
      "CsrfHeader": { "name": "X-Kafdeck-CSRF", "in": "header", "required": true, "schema": { "type": "string", "minLength": 1 }, "description": "Token issued by /api/v1/auth/csrf for the current same-origin cookie session." }
    },
    "responses": {
      "MutationStatus": { "description": "Safe mutation operation projection", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MutationStatus" } } } },
      "Problem": { "description": "RFC 9457-style Problem Details with stable Kafdeck URN type", "content": { "application/problem+json": { "schema": { "$ref": "#/components/schemas/ProblemDetails" } } } }
    },
    "pathItems": {
      "ReviewAction": {
        "tags": ["Mutation lifecycle"],
        "parameters": [{ "$ref": "#/components/parameters/OperationId" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MutationReviewRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/MutationStatus" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } }
      },
      "ExecuteWithoutMaterial": {
        "tags": ["Mutation lifecycle"],
        "summary": "Execute a ready admitted mutation that requires no re-submitted execution material",
        "parameters": [{ "$ref": "#/components/parameters/OperationId" }, { "$ref": "#/components/parameters/CsrfHeader" }],
        "responses": { "200": { "$ref": "#/components/responses/MutationStatus" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" }, "501": { "$ref": "#/components/responses/Problem" } }
      },
      "TopicCreatePreview": { "tags": ["Topic administration"], "summary": "Preview topic creation", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/TopicName" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TopicCreateRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } } },
      "TopicAlterPreview": { "tags": ["Topic administration"], "summary": "Preview allowlisted topic configuration alteration", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/TopicName" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TopicAlterRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" } } },
      "TopicIncreasePartitionsPreview": { "tags": ["Topic administration"], "summary": "Preview partition increase", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/TopicName" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TopicIncreasePartitionsRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" } } },
      "TopicDeletePreview": { "tags": ["Topic administration"], "summary": "Preview topic deletion", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/TopicName" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "403": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } } },
      "ConsumerOffsetPreview": { "tags": ["Consumer administration"], "summary": "Preview finite consumer offset targets", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/GroupId" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ConsumerOffsetRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } } },
      "ConsumerDeletePreview": { "tags": ["Consumer administration"], "summary": "Preview consumer group or committed-offset deletion", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/GroupId" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ConsumerDeleteRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" }, "409": { "$ref": "#/components/responses/Problem" } } },
      "SchemaRegisterPreview": { "tags": ["Schema Registry"], "summary": "Preview schema registration", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/Subject" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SchemaRegistrationRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" } } },
      "SchemaCompatibilityPreview": { "tags": ["Schema Registry"], "summary": "Preview subject compatibility mutation", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/Subject" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SchemaCompatibilityRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" } } },
      "SchemaGlobalCompatibilityPreview": { "tags": ["Schema Registry"], "summary": "Preview global compatibility mutation", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SchemaCompatibilityRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" } } },
      "SchemaDeletePreview": { "tags": ["Schema Registry"], "summary": "Preview explicit schema subject/version deletion", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/Subject" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SchemaDeleteRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "409": { "$ref": "#/components/responses/Problem" } } },
      "ConnectConfigurationPreview": { "tags": ["Kafka Connect"], "summary": "Preview connector create/update with bounded configuration", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/ConnectorName" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ConnectConfigurationRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" } } },
      "ConnectControlPreview": { "tags": ["Kafka Connect"], "summary": "Preview pause/resume/restart connector or task", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/ConnectorName" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ConnectControlRequest" } } } }, "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "400": { "$ref": "#/components/responses/Problem" } } },
      "ConnectDeletePreview": { "tags": ["Kafka Connect"], "summary": "Preview connector deletion", "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/ConnectorName" }, { "$ref": "#/components/parameters/IdempotencyKey" }, { "$ref": "#/components/parameters/CsrfHeader" }], "responses": { "201": { "$ref": "#/components/responses/MutationStatus" }, "200": { "$ref": "#/components/responses/MutationStatus" }, "409": { "$ref": "#/components/responses/Problem" } } }
    },
    "schemas": {
      "ProblemDetails": { "type": "object", "properties": { "type": { "type": "string" }, "title": { "type": "string" }, "status": { "type": "integer" }, "detail": { "type": "string" }, "code": { "type": "string" } }, "additionalProperties": true },
      "CsrfToken": { "type": "object", "required": ["requestToken", "headerName"], "properties": { "requestToken": { "type": "string" }, "headerName": { "type": "string", "const": "X-Kafdeck-CSRF" } } },
      "MutationStatus": { "type": "object", "required": ["operationId", "clusterId", "operationKind", "riskClass", "state", "previewHash", "previewExpiresAtUtc", "version"], "properties": { "operationId": { "type": "string", "format": "uuid" }, "clusterId": { "type": "string" }, "operationKind": { "type": "string" }, "riskClass": { "type": "string", "enum": ["low", "moderate", "high", "critical"] }, "confirmationMode": { "type": "string", "enum": ["explicit", "typedTarget"] }, "requiresIndependentApproval": { "type": "boolean" }, "state": { "type": "string" }, "previewHash": { "type": "string" }, "previewExpiresAtUtc": { "type": "string", "format": "date-time" }, "confirmationChallenge": { "type": ["string", "null"] }, "resultCode": { "type": ["string", "null"] }, "safeProviderEvidence": { "type": "object", "additionalProperties": { "type": "string" } }, "version": { "type": "integer", "format": "int64" }, "createdAtUtc": { "type": "string", "format": "date-time" }, "updatedAtUtc": { "type": "string", "format": "date-time" } }, "additionalProperties": false },
      "MutationApprovalInbox": { "type": "object", "required": ["items"], "properties": { "items": { "type": "array", "maxItems": 100, "items": { "$ref": "#/components/schemas/MutationStatus" } } } },
      "MutationConfirmRequest": { "type": "object", "required": ["previewHash"], "properties": { "previewHash": { "type": "string" }, "typedTargetChallenge": { "type": ["string", "null"] } } },
      "MutationReviewRequest": { "type": "object", "required": ["previewHash"], "properties": { "previewHash": { "type": "string" } } },
      "TopicCreateRequest": { "type": "object", "required": ["partitionCount", "replicationFactor"], "properties": { "partitionCount": { "type": "integer", "minimum": 1 }, "replicationFactor": { "type": "integer", "minimum": 1 }, "configurations": { "type": "object", "additionalProperties": { "type": "string" } } } },
      "TopicAlterRequest": { "type": "object", "required": ["changes"], "properties": { "changes": { "type": "object", "additionalProperties": { "type": ["string", "null"] } } } },
      "TopicIncreasePartitionsRequest": { "type": "object", "required": ["newPartitionCount"], "properties": { "newPartitionCount": { "type": "integer", "minimum": 1 } } },
      "RecordHeader": { "type": "object", "required": ["name", "value"], "properties": { "name": { "type": "string" }, "value": { "type": "string", "contentEncoding": "base64" } } },
      "RecordProductionRecord": { "type": "object", "required": ["value"], "properties": { "key": { "type": ["string", "null"], "contentEncoding": "base64" }, "value": { "type": "string", "contentEncoding": "base64" }, "headers": { "type": "array", "items": { "$ref": "#/components/schemas/RecordHeader" } } } },
      "RecordProductionPreviewRequest": { "type": "object", "required": ["records"], "properties": { "records": { "type": "array", "minItems": 1, "items": { "$ref": "#/components/schemas/RecordProductionRecord" } }, "schemaValidation": { "type": ["object", "null"] } } },
      "RecordProductionExecuteRequest": { "type": "object", "required": ["records"], "properties": { "records": { "type": "array", "minItems": 1, "items": { "$ref": "#/components/schemas/RecordProductionRecord" } } } },
      "ConsumerOffsetRequest": { "type": "object", "required": ["targets"], "properties": { "targets": { "type": "array", "minItems": 1, "items": { "type": "object", "additionalProperties": true } } } },
      "ConsumerDeleteRequest": { "type": "object", "required": ["mode"], "properties": { "mode": { "type": "string" }, "targets": { "type": ["array", "null"], "items": { "type": "object", "additionalProperties": true } } } },
      "SchemaRegistrationRequest": { "type": "object", "required": ["format", "schema"], "properties": { "format": { "type": "string" }, "schema": { "type": "string" }, "references": { "type": "array", "items": { "type": "object", "additionalProperties": true } } } },
      "SchemaCompatibilityRequest": { "type": "object", "required": ["requestedMode"], "properties": { "requestedMode": { "type": "string" } } },
      "SchemaDeleteRequest": { "type": "object", "required": ["permanent"], "properties": { "version": { "type": ["integer", "null"], "minimum": 1 }, "permanent": { "type": "boolean" } } },
      "SchemaCreateExecuteRequest": { "type": "object", "required": ["schema"], "properties": { "schema": { "type": "string" } } },
      "ConnectConfigurationRequest": { "type": "object", "required": ["configuration"], "properties": { "configuration": { "type": "object", "additionalProperties": { "type": "string" } } } },
      "ConnectControlRequest": { "type": "object", "required": ["action"], "properties": { "action": { "type": "string", "enum": ["pause", "resume", "restart"] }, "taskId": { "type": ["integer", "null"], "minimum": 0 } } },
      "RecordsPurgePreviewRequest": { "type": "object", "required": ["targets"], "properties": { "targets": { "type": "array", "minItems": 1, "items": { "type": "object", "additionalProperties": true } } } }
    }
  }
}
""";
}
