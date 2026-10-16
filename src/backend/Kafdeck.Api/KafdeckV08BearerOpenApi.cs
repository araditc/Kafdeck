namespace Kafdeck.Api;

public static class KafdeckV08BearerOpenApi
{
    public const string DocumentPath = "/api/v1/openapi/v0.8-bearer.json";
    public static WebApplication MapKafdeckV08BearerOpenApi(this WebApplication app)
    {
        app.MapGet(DocumentPath, () => Results.Text(Document, "application/vnd.oai.openapi+json;version=3.1.0"))
            .WithName("v08-bearer-openapi").RequireKafdeckAuthorization(Kafdeck.Core.Security.AuthorizationAction.SystemRead);
        return app;
    }

    public const string Document = """
{
  "openapi": "3.1.0",
  "info": {
    "title": "Kafdeck v0.8 governed bearer topic integration",
    "version": "0.8.0-candidate",
    "description": "Default-off topic create/alter/increase/delete contract, not full W67 acceptance or publication. Requires enabled mutation engine, NonBrowserRead.Enabled, GovernedMutationsEnabled, active exact principal/client registration with the matching TopicCreate/TopicAlter/TopicDelete grant AND current backend action/resource RBAC. Partition increase uses TopicAlter and is always at least HIGH; durability-sensitive config changes may raise risk. TopicDelete is always CRITICAL with typed confirmation and a distinct eligible human. Approval/rejection additionally require GovernedApprovalsEnabled, a current human ApprovalActions grant, MutationApprove and underlying TopicDelete resource scopes. Cookie/deployment credential collisions are rejected. Existing browser mutation routes retain antiforgery. Every plan is server-stored and bound to original principal, immutable preview and current policy fingerprint; dispatch rechecks rights, approver eligibility and provider state. Cancellation stops only a permitted pre-dispatch state and never clears unknown effects. Other mutation families remain unavailable. Never replay ExecutionUnknown."
  },
  "servers": [{ "url": "/" }],
  "security": [{ "bearerAuth": [] }],
  "paths": {
    "/api/v1/automation/clusters/{clusterId}/topics/{topicName}/create/preview": {
      "post": {
        "operationId": "v08-bearer-topic-create-preview",
        "summary": "Plan and durably admit a typed TopicCreate intent",
        "description": "Maximum request body 65536 bytes. No effect is dispatched. A fresh Idempotency-Key identifies intent; reusing a key for another intent conflicts. Provider capability and current resource facts are observed by the existing backend planner.",
        "parameters": [
          { "name": "clusterId", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 256 } },
          { "name": "topicName", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 249, "pattern": "^[a-zA-Z0-9._-]+$", "not": { "enum": [".", ".."] } } },
          { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 256 } }
        ],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TopicCreateRequest" } } } },
        "responses": {
          "201": { "description": "Durable preview created; Location identifies its bearer status route.", "headers": { "Location": { "schema": { "type": "string" } } }, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MutationStatus" } } } },
          "200": { "$ref": "#/components/responses/Status" },
          "400": { "$ref": "#/components/responses/Invalid" }, "401": { "$ref": "#/components/responses/Unauthenticated" },
          "403": { "$ref": "#/components/responses/Denied" }, "409": { "$ref": "#/components/responses/Conflict" },
          "413": { "$ref": "#/components/responses/TooLarge" }, "501": { "$ref": "#/components/responses/Unsupported" },
          "503": { "$ref": "#/components/responses/Unavailable" }, "default": { "$ref": "#/components/responses/Failure" }
        }
      }
    },
    "/api/v1/automation/clusters/{clusterId}/topics/{topicName}/alter/preview": {
      "post": {
        "operationId": "v08-bearer-topic-alter-preview", "summary": "Plan a closed topic configuration patch or reset",
        "description": "Exact TopicAlter registration and current backend resource scopes required. Null resets an admitted key. Current configuration/metadata fingerprints and server-owned risk bind the immutable plan; no provider effect is dispatched.",
        "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/TopicName" }, { "$ref": "#/components/parameters/IdempotencyKey" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TopicAlterRequest" } } } },
        "responses": { "201": { "$ref": "#/components/responses/Status" }, "200": { "$ref": "#/components/responses/Status" }, "400": { "$ref": "#/components/responses/Invalid" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "409": { "$ref": "#/components/responses/Conflict" }, "413": { "$ref": "#/components/responses/TooLarge" }, "default": { "$ref": "#/components/responses/Failure" } }
      }
    },
    "/api/v1/automation/clusters/{clusterId}/topics/{topicName}/increase-partitions/preview": {
      "post": {
        "operationId": "v08-bearer-topic-increase-partitions-preview", "summary": "Plan an explicit higher partition target",
        "description": "Exact TopicAlter registration/resource permission. Backend rejects a target that is not above observed count, binds metadata, enforces HIGH risk and exact typed target confirmation. No reduction or arbitrary provider command.",
        "parameters": [{ "$ref": "#/components/parameters/ClusterId" }, { "$ref": "#/components/parameters/TopicName" }, { "$ref": "#/components/parameters/IdempotencyKey" }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TopicIncreasePartitionsRequest" } } } },
        "responses": { "201": { "$ref": "#/components/responses/Status" }, "200": { "$ref": "#/components/responses/Status" }, "400": { "$ref": "#/components/responses/Invalid" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "409": { "$ref": "#/components/responses/Conflict" }, "413": { "$ref": "#/components/responses/TooLarge" }, "default": { "$ref": "#/components/responses/Failure" } }
      }
    },
    "/api/v1/automation/clusters/{clusterId}/topics/{topicName}/delete/preview": {
      "post": {
        "operationId": "v08-bearer-topic-delete-preview", "summary": "Plan CRITICAL deletion of an exact existing topic",
        "description": "No request body. No effect is dispatched. Server-owned Critical risk, typed target confirmation and a distinct human approval cannot be waived.",
        "parameters": [
          { "name": "clusterId", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 256 } },
          { "name": "topicName", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 249 } },
          { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 256 } }
        ],
        "responses": { "201": { "$ref": "#/components/responses/Status" }, "200": { "$ref": "#/components/responses/Status" }, "400": { "$ref": "#/components/responses/Invalid" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "409": { "$ref": "#/components/responses/Conflict" }, "default": { "$ref": "#/components/responses/Failure" } }
      }
    },
    "/api/v1/automation/mutations/{operationId}/approval": {
      "parameters": [{ "$ref": "#/components/parameters/OperationId" }],
      "get": {
        "operationId": "v08-bearer-mutation-approval", "summary": "Inspect a CRITICAL topic deletion as a distinct eligible registered human",
        "description": "Current human registration, exact approval grant, direct MutationApprove and TopicDelete scopes, current policy and canonical human distinction are required. A service cannot inspect through this approval surface.",
        "responses": { "200": { "$ref": "#/components/responses/Status" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "404": { "$ref": "#/components/responses/NotFound" } }
      }
    },
    "/api/v1/automation/mutations/{operationId}/approve": {
      "parameters": [{ "$ref": "#/components/parameters/OperationId" }],
      "post": {
        "operationId": "v08-bearer-mutation-approve", "summary": "Approve the exact preview as a distinct eligible human",
        "description": "No actor override. The same issuer/subject cannot approve by changing client or browser transport. Machines cannot approve. Approval is durable and eligibility is rechecked before dispatch.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ApprovalRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/Status" }, "400": { "$ref": "#/components/responses/Invalid" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" }, "413": { "$ref": "#/components/responses/TooLarge" } }
      }
    },
    "/api/v1/automation/mutations/{operationId}/cancel": {
      "parameters": [{ "$ref": "#/components/parameters/OperationId" }],
      "post": {
        "operationId": "v08-bearer-mutation-cancel", "summary": "Stop the original actor's permitted pre-dispatch topic plan",
        "description": "Exact original actor, current topic registration/action scopes, policy and preview hash required. Permitted only for Previewed/AwaitingConfirmation/AwaitingApproval/Ready. Current approval is not required to stop; no effect is authorized. Executing, unknown and terminal states cannot be cancelled.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ApprovalRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/Status" }, "400": { "$ref": "#/components/responses/Invalid" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" }, "413": { "$ref": "#/components/responses/TooLarge" } }
      }
    },
    "/api/v1/automation/mutations/{operationId}/reject": {
      "parameters": [{ "$ref": "#/components/parameters/OperationId" }],
      "post": {
        "operationId": "v08-bearer-mutation-reject", "summary": "Reject an awaiting CRITICAL topic deletion as a distinct eligible human",
        "description": "Same live registered human, direct approval/resource authority and exact preview hash as approve. Rejection records the actual human and durable evidence; it is not a provider effect or uncertainty disposition.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ApprovalRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/Status" }, "400": { "$ref": "#/components/responses/Invalid" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" }, "413": { "$ref": "#/components/responses/TooLarge" } }
      }
    },
    "/api/v1/automation/mutations/{operationId}/status": {
      "parameters": [{ "$ref": "#/components/parameters/OperationId" }],
      "get": {
        "operationId": "v08-bearer-mutation-status", "summary": "Read the original principal's governed topic operation status",
        "responses": { "200": { "$ref": "#/components/responses/Status" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "404": { "$ref": "#/components/responses/NotFound" } }
      }
    },
    "/api/v1/automation/mutations/{operationId}/confirm": {
      "parameters": [{ "$ref": "#/components/parameters/OperationId" }],
      "post": {
        "operationId": "v08-bearer-mutation-confirm", "summary": "Confirm the exact immutable preview as its original principal",
        "description": "A CRITICAL result remains AwaitingApproval; confirmation is not independent approval.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ConfirmRequest" } } } },
        "responses": { "200": { "$ref": "#/components/responses/Status" }, "400": { "$ref": "#/components/responses/Invalid" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" }, "413": { "$ref": "#/components/responses/TooLarge" } }
      }
    },
    "/api/v1/automation/mutations/{operationId}/execute": {
      "parameters": [{ "$ref": "#/components/parameters/OperationId" }],
      "post": {
        "operationId": "v08-bearer-mutation-execute", "summary": "Dispatch a Ready admitted topic plan through the existing guarded durable engine",
        "description": "No caller body or generic provider command is accepted. Current principal, permissions, policy, expiry, immutable preview and provider preconditions are rechecked. HTTP 200 carries the actual typed outcome and never converts ExecutionUnknown to successful completion. Repeat execution cannot authorize a blind resend.",
        "responses": { "200": { "$ref": "#/components/responses/Status" }, "401": { "$ref": "#/components/responses/Unauthenticated" }, "403": { "$ref": "#/components/responses/Denied" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" }, "default": { "$ref": "#/components/responses/Failure" } }
      }
    }
  },
  "components": {
    "securitySchemes": { "bearerAuth": { "type": "http", "scheme": "bearer", "bearerFormat": "JWT", "description": "Registered service or independently registered CLI human. Access-token audience is the deployment's exact Kafdeck API audience; ID tokens and client assertions are rejected." } },
    "parameters": {
      "OperationId": { "name": "operationId", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } },
      "ClusterId": { "name": "clusterId", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 256 } },
      "TopicName": { "name": "topicName", "in": "path", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 249, "pattern": "^[a-zA-Z0-9._-]+$", "not": { "enum": [".", ".."] } } },
      "IdempotencyKey": { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 256 } }
    },
    "schemas": {
      "TopicCreateRequest": { "type": "object", "additionalProperties": false, "required": ["partitionCount", "replicationFactor"], "properties": {
        "partitionCount": { "type": "integer", "minimum": 1, "maximum": 100000 },
        "replicationFactor": { "type": "integer", "minimum": 1, "maximum": 32767 },
        "configurations": { "type": ["object", "null"], "additionalProperties": false, "maxProperties": 9, "description": "Optional existing planner-allowlisted topic configuration values; numeric/cleanup semantics are validated by that planner.", "properties": {
          "cleanup.policy": { "type": "string" }, "retention.ms": { "type": "string" }, "retention.bytes": { "type": "string" },
          "segment.ms": { "type": "string" }, "segment.bytes": { "type": "string" }, "min.insync.replicas": { "type": "string" },
          "max.message.bytes": { "type": "string" }, "delete.retention.ms": { "type": "string" }, "min.cleanable.dirty.ratio": { "type": "string" }
        } }
      } },
      "TopicAlterRequest": { "type": "object", "additionalProperties": false, "required": ["changes"], "properties": {
        "changes": { "type": "object", "additionalProperties": false, "minProperties": 1, "maxProperties": 9, "description": "Existing backend planner validates exact key/value semantics; null requests safe reset to provider default.", "properties": {
          "cleanup.policy": { "type": ["string", "null"] }, "retention.ms": { "type": ["string", "null"] }, "retention.bytes": { "type": ["string", "null"] },
          "segment.ms": { "type": ["string", "null"] }, "segment.bytes": { "type": ["string", "null"] }, "min.insync.replicas": { "type": ["string", "null"] },
          "max.message.bytes": { "type": ["string", "null"] }, "delete.retention.ms": { "type": ["string", "null"] }, "min.cleanable.dirty.ratio": { "type": ["string", "null"] }
        } }
      } },
      "TopicIncreasePartitionsRequest": { "type": "object", "additionalProperties": false, "required": ["newPartitionCount"], "properties": {
        "newPartitionCount": { "type": "integer", "minimum": 1, "maximum": 100000, "description": "Must also exceed the current observed count." }
      } },
      "ConfirmRequest": { "type": "object", "additionalProperties": false, "required": ["previewHash"], "properties": {
        "previewHash": { "type": "string", "minLength": 64, "maxLength": 64, "pattern": "^[a-fA-F0-9]{64}$" },
        "typedTargetChallenge": { "type": ["string", "null"], "maxLength": 512 }
      } },
      "ApprovalRequest": { "type": "object", "additionalProperties": false, "required": ["previewHash"], "properties": {
        "previewHash": { "type": "string", "minLength": 64, "maxLength": 64, "pattern": "^[a-fA-F0-9]{64}$" }
      } },
      "MutationStatus": { "type": "object", "required": ["operationId", "clusterId", "operationKind", "riskClass", "confirmationMode", "requiresIndependentApproval", "requiresExecutionMaterial", "state", "previewHash", "previewExpiresAtUtc", "safeProviderEvidence", "version", "createdAtUtc", "updatedAtUtc"], "properties": {
        "operationId": { "type": "string", "format": "uuid" }, "clusterId": { "type": "string" }, "operationKind": { "enum": ["topicCreate", "topicAlter", "topicIncreasePartitions", "topicDelete"] },
        "riskClass": { "enum": ["low", "moderate", "high", "critical"] }, "confirmationMode": { "enum": ["explicit", "typedTarget"] },
        "requiresIndependentApproval": { "type": "boolean" }, "requiresExecutionMaterial": { "type": "boolean" },
        "state": { "enum": ["previewed", "awaitingConfirmation", "awaitingApproval", "ready", "executing", "appliedVerified", "appliedUnverified", "partiallyApplied", "executionUnknown", "rejected", "expired", "cancelled", "stalePreview", "failedBeforeDispatch", "failedDefinitive"], "description": "Exact durable state. AwaitingApproval and ExecutionUnknown are never relabeled applied; no client retries an unknown effect automatically." },
        "previewHash": { "type": "string", "pattern": "^[a-fA-F0-9]{64}$" }, "previewExpiresAtUtc": { "type": "string", "format": "date-time" },
        "confirmationChallenge": { "type": ["string", "null"] }, "resultCode": { "type": ["string", "null"] },
        "safeProviderEvidence": { "type": "object", "additionalProperties": { "type": "string" } }, "version": { "type": "integer", "minimum": 0 },
        "createdAtUtc": { "type": "string", "format": "date-time" }, "updatedAtUtc": { "type": "string", "format": "date-time" }
      } },
      "Problem": { "type": "object", "properties": { "type": { "type": "string" }, "title": { "type": "string" }, "status": { "type": "integer" }, "detail": { "type": "string" } } }
    },
    "responses": {
      "Status": { "description": "Typed durable operation state, not an unconditional successful effect claim.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/MutationStatus" } } } },
      "Invalid": { "description": "Typed request or preview confirmation invalid.", "content": { "application/problem+json": { "schema": { "$ref": "#/components/schemas/Problem" } } } },
      "Unauthenticated": { "description": "Bearer trust, token, registration, activation or conflicting-credential admission rejected.", "content": { "application/problem+json": { "schema": { "$ref": "#/components/schemas/Problem" } } } },
      "Denied": { "description": "Current exact action/resource authority, original owner or policy fingerprint denied.", "content": { "application/problem+json": { "schema": { "$ref": "#/components/schemas/Problem" } } } },
      "NotFound": { "description": "Operation not found.", "content": { "application/problem+json": { "schema": { "$ref": "#/components/schemas/Problem" } } } },
      "Conflict": { "description": "Idempotency conflict, stale preview, not Ready/awaiting approval, expiry or concurrent state transition; inspect status and never blindly execute again.", "content": { "application/problem+json": { "schema": { "$ref": "#/components/schemas/Problem" } } } },
      "TooLarge": { "description": "Request exceeded the finite 65536-byte body budget." },
      "Unsupported": { "description": "Provider capability unavailable for this admitted family." },
      "Unavailable": { "description": "Provider observation unavailable; no effect was claimed." },
      "Failure": { "description": "Safe backend failure; typed status and durable evidence determine any external effect.", "content": { "application/problem+json": { "schema": { "$ref": "#/components/schemas/Problem" } } } }
    }
  }
}
""";
}
