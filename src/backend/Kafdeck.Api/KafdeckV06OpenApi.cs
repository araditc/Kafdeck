namespace Kafdeck.Api;

public static class KafdeckV06OpenApi
{
    public const string DocumentPath = "/api/v1/openapi/v0.6.json";

    public static WebApplication MapKafdeckV06OpenApi(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet(
                DocumentPath,
                () => Results.Text(
                    Document,
                    "application/vnd.oai.openapi+json;version=3.1.0"))
            .WithName("v06-openapi");

        return app;
    }

    public const string Document = """
{
  "openapi": "3.1.0",
  "info": {
    "title": "Kafdeck v0.6 Fleet Operations API",
    "version": "0.6.0",
    "description": "Capability-driven v0.6 fleet extension. Unsupported and governance-blocked operations are reported explicitly and are never exposed through generic execution tunnels. Existing v0.1-v0.5 routes retain their published contracts."
  },
  "servers": [{ "url": "/" }],
  "paths": {
    "/api/v1/fleet/capabilities": {
      "get": {
        "operationId": "v06-fleet-capabilities",
        "summary": "Read v0.6 fleet capability dispositions",
        "responses": {
          "200": {
            "description": "Bounded fleet capability catalog",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/FleetCapabilityResponse" }
              }
            }
          },
          "401": {
            "description": "Operator authentication required in OIDC mode"
          }
        }
      }
    }
  },
  "components": {
    "schemas": {
      "FleetCapabilityResponse": {
        "type": "object",
        "required": ["version", "capabilities"],
        "properties": {
          "version": { "type": "string", "const": "v0.6" },
          "capabilities": {
            "type": "array",
            "items": { "$ref": "#/components/schemas/FleetCapabilityStatus" }
          }
        }
      },
      "FleetCapabilityStatus": {
        "type": "object",
        "required": ["id", "displayName", "state", "reason", "workstream", "evidence"],
        "properties": {
          "id": { "type": "string", "maxLength": 96 },
          "displayName": { "type": "string" },
          "state": {
            "type": "string",
            "enum": ["supported", "unsupported", "blocked", "unconfigured", "unknown"]
          },
          "reason": { "type": "string", "maxLength": 1024 },
          "workstream": { "type": "string", "maxLength": 16 },
          "evidence": {
            "type": "array",
            "maxItems": 8,
            "items": { "type": "string", "maxLength": 512 }
          }
        }
      }
    }
  }
}
""";
}
