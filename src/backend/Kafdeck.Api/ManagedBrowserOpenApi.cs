using System.Text.Json.Nodes;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

// Inventory metadata only: the original handlers, authorization and CSRF remain authoritative.
internal sealed record ManagedBrowserRouteContract(string OperationId, Type? RequestType);

internal static class ManagedBrowserOpenApi
{
    internal const string DocumentPath = "/api/v1/openapi/managed-browser.json";
    internal static string Catalog { get; } = ReadCatalog();

    internal static RouteHandlerBuilder WithManagedBrowserContract(this RouteHandlerBuilder route,
        string operationId, Type? requestType = null) =>
        route.WithName(operationId).WithMetadata(new ManagedBrowserRouteContract(operationId, requestType));

    internal static void MapManagedBrowserOpenApi(this WebApplication app) =>
        app.MapGet(DocumentPath, (HttpContext context) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            return Results.Text(ForApplication(app).ToJsonString(), "application/vnd.oai.openapi+json;version=3.1.0");
        }).WithName("managed-browser-openapi").RequireKafdeckAuthorization(AuthorizationAction.SystemRead);

    internal static JsonObject ForApplication(WebApplication app)
    {
        var document = JsonNode.Parse(Catalog)!.AsObject();
        var catalog = document["paths"]!.AsObject();
        var enabled = new JsonObject();
        foreach (var endpoint in ((IEndpointRouteBuilder)app).DataSources.SelectMany(s => s.Endpoints).OfType<RouteEndpoint>())
        {
            if (endpoint.Metadata.GetMetadata<ManagedBrowserRouteContract>() is not { } contract) continue;
            var path = Normalize(endpoint);
            foreach (var method in endpoint.Metadata.GetMetadata<HttpMethodMetadata>()!.HttpMethods)
            {
                var verb = method.ToLowerInvariant();
                var operation = catalog[path]?[verb] ?? throw new InvalidOperationException("Managed browser route absent from the contract.");
                if (operation["operationId"]?.GetValue<string>() != contract.OperationId)
                    throw new InvalidOperationException("Managed browser operation identity differs from its contract.");
                if (enabled[path] is not JsonObject) enabled[path] = new JsonObject();
                enabled[path]![verb] = operation.DeepClone();
            }
        }
        document["paths"] = enabled;
        return document;
    }

    internal static string Normalize(RouteEndpoint endpoint)
    {
        var path = endpoint.RoutePattern.RawText!;
        foreach (var parameter in endpoint.RoutePattern.Parameters)
            if (parameter.ParameterPolicies.Count > 0)
                path = path.Replace("{" + parameter.Name + ":" + string.Join(':', parameter.ParameterPolicies.Select(p => p.Content)) + "}",
                    "{" + parameter.Name + "}", StringComparison.Ordinal);
        return path;
    }

    private static string ReadCatalog()
    {
        using var stream = typeof(ManagedBrowserOpenApi).Assembly.GetManifestResourceStream("Kafdeck.ManagedBrowser.OpenApi")
            ?? throw new InvalidOperationException("Managed browser contract resource is missing.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
