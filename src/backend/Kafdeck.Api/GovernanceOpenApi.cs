using System.Text.Json.Nodes;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

public static class GovernanceOpenApi
{
    public static WebApplication MapGovernanceOpenApi(this WebApplication app)
    {
        app.MapGet("/api/v1/openapi/v0.9-governance.json", () =>
        {
            var schemas = new JsonObject(); var paths = new JsonObject();
            foreach (var endpoint in ((IEndpointRouteBuilder)app).DataSources.SelectMany(s => s.Endpoints).OfType<RouteEndpoint>())
            {
                var contract = endpoint.Metadata.GetMetadata<GovernanceRouteContract>();
                if (contract is null || endpoint.RoutePattern.RawText is not { } raw || !raw.StartsWith("/api/v1/governance/", StringComparison.Ordinal)) continue;
                var methods = endpoint.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods ?? [];
                var route = raw;
                foreach (var parameter in endpoint.RoutePattern.Parameters)
                    route = route.Replace("{" + parameter.Name + (parameter.ParameterPolicies.Count > 0 ? ":" + string.Join(':', parameter.ParameterPolicies.Select(p => p.Content)) : "") + "}", "{" + parameter.Name + "}", StringComparison.Ordinal);
                var path = paths[route] as JsonObject ?? new JsonObject();
                if (paths[route] is null) paths[route] = path;
                foreach (var method in methods)
                {
                    var parameters = new JsonArray();
                    foreach (var parameter in endpoint.RoutePattern.Parameters)
                        parameters.Add(new JsonObject { ["name"] = parameter.Name, ["in"] = "path", ["required"] = true, ["schema"] = new JsonObject { ["type"] = "string" } });
                    foreach (var query in contract.QueryKeys.Split(',', StringSplitOptions.RemoveEmptyEntries))
                        parameters.Add(new JsonObject { ["name"] = query, ["in"] = "query", ["required"] = contract.RequiredQueryKeys.Split(',',StringSplitOptions.RemoveEmptyEntries).Contains(query,StringComparer.Ordinal),
                            ["schema"] = query == "pageSize" ? new JsonObject { ["type"] = "integer", ["minimum"] = 1, ["maximum"] = 200, ["default"] = 50 } : new JsonObject { ["type"] = "string", ["maxLength"] = 4096 } });
                    if (!contract.Read && raw.EndsWith("/preview", StringComparison.Ordinal)) parameters.Add(new JsonObject { ["name"] = "Idempotency-Key", ["in"] = "header", ["required"] = true,
                        ["schema"] = new JsonObject { ["type"] = "string", ["maxLength"] = 256 } });
                    var responses = new JsonObject { ["200"] = new JsonObject { ["description"] = "Bounded scoped result",
                        ["content"] = new JsonObject { ["application/json"] = new JsonObject { ["schema"] = contract.ResponseType is { } response
                            ? BearerFamilyOpenApi.Schema(response, schemas) : new JsonObject { ["type"] = "object" } } } } };
                    foreach (var code in new[] { "400", "401", "403", "404", "409", "413", "429", "503" }) responses[code] = new JsonObject { ["description"] = "Closed safe problem response; current authority and state remain required." };
                    var operation = new JsonObject { ["operationId"] = endpoint.Metadata.GetMetadata<IEndpointNameMetadata>()?.EndpointName,
                        ["x-authorization-action"] = contract.Action.ToString(), ["x-governed-mutation"] = !contract.Read,
                        ["x-max-body-bytes"] = contract.MaxBodyBytes, ["parameters"] = parameters, ["responses"] = responses,
                        ["security"] = new JsonArray(new JsonObject { ["Bearer"] = new JsonArray() }) };
                    if (contract.RequestType is { } request) operation["requestBody"] = new JsonObject { ["required"] = true,
                        ["content"] = new JsonObject { ["application/json"] = new JsonObject { ["schema"] = BearerFamilyOpenApi.Schema(request, schemas) } } };
                    path[method.ToLowerInvariant()] = operation;
                }
            }
            return Results.Json(new JsonObject { ["openapi"] = "3.1.0", ["info"] = new JsonObject { ["title"] = "Kafdeck governance contract", ["version"] = "0.9" },
                ["paths"] = paths, ["components"] = new JsonObject { ["schemas"] = schemas,
                    ["securitySchemes"] = new JsonObject { ["Bearer"] = new JsonObject { ["type"] = "http", ["scheme"] = "bearer", ["bearerFormat"] = "JWT" } } } });
        }).WithName("v09-governance-openapi").WithMetadata(new GovernanceRouteContract(AuthorizationAction.SystemRead, true, QueryKeys: ""))
            .RequireKafdeckAuthorization(AuthorizationAction.SystemRead);
        return app;
    }
}
