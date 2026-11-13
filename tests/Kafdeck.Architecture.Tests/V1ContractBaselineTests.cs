using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Mcp;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Metadata only: no listener, provider connection, worker, handler or credential resolution.
public sealed class V1ContractBaselineTests
{
    private static readonly string Root = FindRoot();
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = true };

    [Fact]
    public async Task Mapped_routes_and_generated_documents_have_an_explicit_frozen_selection()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Production", ContentRootPath = Root });
        builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:1");
        // RequestDelegateFactory needs the same service-vs-body distinction as production.
        // These factories must never run: this fixture only builds route metadata.
        var api = typeof(KafdeckV08BearerOpenApi).Assembly;
        var assemblies = LoadAssemblies(api);
        var production = string.Join('\n', Directory.GetFiles(Path.Combine(Root, "src/backend"), "*.cs", SearchOption.AllDirectories)
            .Where(p => !p.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar)).Select(File.ReadAllText));
        var registered = Regex.Matches(production, @"\b(?:Try)?Add(?:Singleton|Scoped|Transient)\s*<\s*([\w.]+)|\b(?:Try)?Add(?:Singleton|Scoped|Transient)\s*\(\s*(?:\w+\s*=>\s*)?new\s+([\w.]+)")
            .Select(m => (m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value).Split('.').Last()).ToHashSet(StringComparer.Ordinal);
        // Program registers this preconstructed instance by inferred generic type.
        registered.Add(nameof(KafdeckAuthorizationService));
        registered.Add(nameof(KafdeckOptions));
        registered.UnionWith([nameof(GovernanceRuntimeOptions), "AttributionRuntimeOptions", "NotificationGovernanceOptions", "NotificationRecoveryOptions",
            "SecretResolver", "CompiledRecordMaskingPolicy", "ReloadingAuthorizationPolicy"]);
        foreach (var type in assemblies.SelectMany(a => a.GetTypes()).Where(t => !t.ContainsGenericParameters && t != typeof(RegisteredKafkaRuntime) && registered.Contains(t.Name)))
            builder.Services.AddSingleton(type, _ => throw new InvalidOperationException("Contract metadata attempted to resolve " + type.FullName));
        await using var app = builder.Build();
        var options = new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:1", null, AccessMode.Oidc, null), [],
            DataQuality: new(true, true, DataQualityPersistenceProvider.Sqlite, DataQualityExecutionMode.Standalone, "unused.db", null),
            Notifications: new(true, NotificationPersistenceProvider.Sqlite, NotificationExecutionMode.Standalone, "unused.db", null, ManagementEnabled: true));
        var store = new ManagedIdentityStore(Path.Combine(Path.GetTempPath(), "kafdeck-contract-nonexistent-" + Guid.NewGuid().ToString("N")), "http://127.0.0.1:1/realms/inventory");
        var runtime = new RegisteredHttpServiceRuntime(store, new HttpContextAccessor(), new EphemeralDataProtectionProvider(), builder.Configuration);
        var program = File.ReadAllText(Path.Combine(Root, "src/backend/Kafdeck.Api/Program.cs"));
        var names = Regex.Matches(program, @"\bapp\.(Map\w+)\(").Select(m => m.Groups[1].Value)
            .Where(n => n is not ("MapGet" or "MapPost" or "MapFallbackToFile")).Distinct().Order(StringComparer.Ordinal).ToArray();
        foreach (var name in names)
        {
            var candidates = api.GetTypes().SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
                .Where(m => m.Name == name && m.GetParameters().FirstOrDefault()?.ParameterType.IsAssignableFrom(typeof(WebApplication)) == true).ToArray();
            Assert.True(candidates.Length == 1, "Unresolved mapper: " + name);
            var method = candidates[0];
            var args = method.GetParameters().Select(p => p.ParameterType.IsInstanceOfType(app) ? (object)app :
                p.ParameterType == typeof(KafdeckOptions) ? options : p.ParameterType == typeof(ManagedIdentityStore) ? store :
                p.ParameterType == typeof(RegisteredHttpServiceRuntime) ? runtime : p.HasDefaultValue ? p.DefaultValue :
                p.ParameterType == typeof(bool) ? true : throw new InvalidOperationException("Inventory argument unclassified: " + name + "/" + p.Name)).ToArray();
            // Feature switches are enabled; recursive browser/bearer switches retain their defaults.
            foreach (var p in method.GetParameters().Where(p => p.ParameterType == typeof(bool) && p.Name != "bearer")) args[p.Position] = true;
            method.Invoke(null, args);
        }
        var endpoints = ((IEndpointRouteBuilder)app).DataSources.SelectMany(s => s.Endpoints).OfType<RouteEndpoint>().ToArray();
        var schemas = new JsonObject();
        var routes = endpoints.SelectMany(e => (e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods ?? [])
            .Select(verb => new { method = verb, path = Normalize(e), name = e.Metadata.GetMetadata<IEndpointNameMetadata>()?.EndpointName,
                request = e.Metadata.GetMetadata<IAcceptsMetadata>()?.RequestType is { } body ? BearerFamilyOpenApi.Schema(body, schemas) : null,
                governance = e.Metadata.GetMetadata<GovernanceRouteContract>() is { } g ? new { action = g.Action.ToString(), g.Read, g.QueryKeys, g.RequiredQueryKeys, g.MaxBodyBytes,
                    response = g.ResponseType is { } response ? BearerFamilyOpenApi.Schema(response, schemas) : null } : null,
                declarative = e.Metadata.GetMetadata<DeclarativeRouteContract>() is { } d ? d.Action.ToString() : null,
                antiforgery = e.Metadata.GetMetadata<IAntiforgeryMetadata>()?.RequiresValidation ?? false,
                managed = e.Metadata.GetMetadata<ManagedBrowserRouteContract>()?.OperationId }))
            .OrderBy(r => r.path, StringComparer.Ordinal).ThenBy(r => r.method, StringComparer.Ordinal).ToArray();
        Assert.Equal(routes.Length, routes.Select(r => (r.method, r.path)).Distinct().Count());
        var documents = new SortedDictionary<string, JsonNode>(StringComparer.Ordinal);
        foreach (var type in api.GetTypes().Where(t => t.Name.Contains("OpenApi", StringComparison.Ordinal)))
        {
            var document = type.GetField("Document", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)?.GetValue(null) as string;
            if (document is not null) documents.Add(type.Name, JsonNode.Parse(document)!);
        }
        foreach (var path in Directory.GetFiles(Path.Combine(Root, "docs/api"), "openapi-*.json").Order(StringComparer.Ordinal))
            if (!path.EndsWith("openapi-managed-browser.json", StringComparison.Ordinal)) documents.Add(Path.GetFileName(path), JsonNode.Parse(File.ReadAllText(path))!);
        var identitySet = routes.Select(r => r.method + " " + r.path).ToHashSet(StringComparer.Ordinal);
        // Versioned historical catalogs may intentionally document a subset. Every selected
        // operation must nevertheless exist in the maximal compiled mapping.
        foreach (var document in documents.Values)
        foreach (var path in document["paths"]!.AsObject())
        foreach (var operation in path.Value!.AsObject().Where(p => Verbs.Contains(p.Key)))
            Assert.Contains(operation.Key.ToUpperInvariant() + " " + path.Key, identitySet);
        var stable = routes.Where(r => r.managed is null && !r.path.StartsWith("/api/v1/auth/", StringComparison.Ordinal) &&
            (r.governance is not null || r.declarative is not null || documents.Values.Any(document =>
                document["paths"]?[r.path]?[r.method.ToLowerInvariant()] is not null)))
            .Select(r => r.method + " " + r.path).ToArray();
        Baseline("api", JsonSerializer.SerializeToNode(new { mappingMethods = names, routes, schemas,
            documents = documents.Select(p => new { producer = p.Key,
                operations = p.Value["paths"]!.AsObject().SelectMany(path => path.Value!.AsObject().Where(o => Verbs.Contains(o.Key)).Select(o => o.Key.ToUpperInvariant() + " " + path.Key)).Order(StringComparer.Ordinal).ToArray(),
                schemaNames = p.Value["components"]?["schemas"]?.AsObject().Select(s => s.Key).Order(StringComparer.Ordinal).ToArray() ?? [],
                sha256 = Convert.ToHexStringLower(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(p.Value.ToJsonString()))) }),
            selection = new { stable, experimental = routes.Select(r => r.method + " " + r.path).Except(stable).ToArray(),
                programInline = new[] { "GET /healthz: Stable operator readiness fields status/product", "GET /api/v1/auth/mode: Experimental", "GET /api/v1/auth/session non-OIDC variant: Experimental" },
                @internal = new[] { "SPA fallback/static assets/DOM", "private persistence and implementation types" } },
            // Authorization filters and conditional modes execute in existing admission tests.
            // Freeze their authoritative producers too, without pretending metadata proves runtime authorization.
            producers = Sources("src/backend/Kafdeck.Api", ".cs").Concat(Sources("src/shared", ".cs")).ToDictionary(p => p.Key, p => p.Value) }, Json)!);
    }

    [Fact]
    public void Mcp_protocol_schemas_and_effect_boundary_match_the_admitted_inventory()
    {
        Assert.Equal(McpTools.All.Count, McpTools.All.Select(t => t.Name).Distinct(StringComparer.Ordinal).Count());
        Assert.All(McpTools.All, t => Assert.False(t.InputSchema["additionalProperties"]!.GetValue<bool>()));
        Baseline("mcp", JsonSerializer.SerializeToNode(new { protocol = McpServer.ProtocolVersion,
            tools = McpTools.All.OrderBy(t => t.Name, StringComparer.Ordinal).Select(t => new { t.Name, t.Effect, t.InputSchema }),
            producers = Sources("src/mcp/Kafdeck.Mcp", ".cs") }, Json)!);
    }

    [Fact]
    public void Cli_read_contracts_exits_and_contract_producers_are_frozen()
    {
        var exits = typeof(Kafdeck.Cli.CliApplication).GetFields(BindingFlags.Static | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(int)).ToDictionary(f => f.Name, f => f.GetRawConstantValue());
        Baseline("cli", JsonSerializer.SerializeToNode(new { exits, usage = Kafdeck.Cli.CliParser.Usage, reads = Kafdeck.Cli.Contracts.KafdeckReadContracts.All,
            // Conservative review tripwire: changes to parser/output/error handling need explicit compatibility review.
            producers = Sources("src/cli/Kafdeck.Cli", ".cs") }, Json)!);
    }

    [Fact]
    public void Operator_configuration_and_independently_versioned_provider_selection_are_frozen()
    {
        var producers = Sources("src/backend/Infrastructure/Kafdeck.Infrastructure.Configuration", ".cs")
            .Concat(Sources("src/backend/Kafdeck.Api", ".cs").Where(p => p.Key.EndsWith("Program.cs", StringComparison.Ordinal) || p.Key.Contains("Options", StringComparison.Ordinal)
                || Regex.IsMatch(File.ReadAllText(Path.Combine(Root, p.Key)), @"GetSection\(|GetValue[<(]|GetEnvironmentVariable\(|(?:configuration|Configuration|config)\[")))
            .Concat(Sources("deploy/helm/kafdeck", ".yaml", ".json", ".tpl"))
            .Concat(Sources("providers/terraform/internal/provider", ".go")).ToDictionary(p => p.Key, p => p.Value);
        var keys = producers.Keys.Where(p => p.EndsWith(".cs", StringComparison.Ordinal)).SelectMany(p =>
            Regex.Matches(File.ReadAllText(Path.Combine(Root, p)), "\"(Kafdeck:[^\"\\r\\n]+|KAFDECK_[A-Z0-9_]+)\"")
                .Select(m => m.Groups[1].Value)).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        var sectionAccesses = producers.Keys.Where(p => p.EndsWith(".cs", StringComparison.Ordinal)).Select(p => new { file = p,
            // Relative keys retain their receiver and source, rather than inventing a flattened qualified path.
            accesses = Regex.Matches(File.ReadAllText(Path.Combine(Root, p)), "(\\w+)\\s*(?:\\[\"([^\"]+)\"\\]|\\.GetSection\\(\"([^\"]+)\"\\))")
                .Select(m => new { receiver = m.Groups[1].Value, key = m.Groups[2].Success ? m.Groups[2].Value : m.Groups[3].Value })
                .Distinct().OrderBy(a => a.receiver, StringComparer.Ordinal).ThenBy(a => a.key, StringComparer.Ordinal).ToArray() });
        var models = typeof(KafdeckOptions).Assembly.GetTypes().Where(t => t.IsPublic && (t.IsEnum || t.Name.EndsWith("Options", StringComparison.Ordinal) || t.Name.EndsWith("Profile", StringComparison.Ordinal)))
            .OrderBy(t => t.FullName, StringComparer.Ordinal).Select(t => new { name = t.FullName, values = t.IsEnum ? Enum.GetNames(t) : [],
                properties = t.GetProperties(BindingFlags.Public | BindingFlags.Instance).OrderBy(p => p.Name, StringComparer.Ordinal).Select(p => new { p.Name, type = p.PropertyType.ToString() }).ToArray() });
        Baseline("operator", JsonSerializer.SerializeToNode(new { keys, sectionAccesses, models, producers }, Json)!);
    }

    private static readonly string[] Verbs = ["get", "post", "put", "patch", "delete", "head", "options"];
    private static string Normalize(RouteEndpoint e) => Regex.Replace(e.RoutePattern.RawText!, @"\{([^}:]+):[^}]+\}", "{$1}");
    private static Dictionary<string, string> Sources(string directory, params string[] extensions) => Directory.GetFiles(Path.Combine(Root, directory), "*", SearchOption.AllDirectories)
        .Where(p => extensions.Contains(Path.GetExtension(p)) && !p.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar)
            && !p.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar) && !p.EndsWith("_test.go", StringComparison.Ordinal))
        .Order(StringComparer.Ordinal).ToDictionary(p => Path.GetRelativePath(Root, p).Replace('\\', '/'), p => Convert.ToHexStringLower(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(File.ReadAllText(p).Replace("\r\n", "\n")))));
    private static void Baseline(string name, JsonNode current)
    {
        var path = Path.Combine(Root, "docs/api/contracts/v1", name + ".json");
        if (Environment.GetEnvironmentVariable("KAFDECK_CONTRACT_EXPORT_DIRECTORY") is { } output)
        {
            // Explicit export is preparation, never a passing compatibility assertion.
            Directory.CreateDirectory(output); File.WriteAllText(Path.Combine(output, name + ".json"), current.ToJsonString(Json) + "\n");
        }
        Assert.True(File.Exists(path), "Missing admitted baseline: " + path);
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(File.ReadAllText(path)), current), "Compatibility drift requires an explicit contract review: " + name);
    }
    private static Assembly[] LoadAssemblies(Assembly root)
    {
        var result = new Dictionary<string, Assembly>(); var pending = new Queue<Assembly>(); pending.Enqueue(root);
        while (pending.TryDequeue(out var assembly))
        {
            if (!result.TryAdd(assembly.FullName!, assembly)) continue;
            foreach (var reference in assembly.GetReferencedAssemblies().Where(n => n.Name!.StartsWith("Kafdeck.", StringComparison.Ordinal))) pending.Enqueue(Assembly.Load(reference));
        }
        return result.Values.ToArray();
    }
    private static string FindRoot()
    {
        for (var p = new DirectoryInfo(AppContext.BaseDirectory); p is not null; p = p.Parent)
            if (File.Exists(Path.Combine(p.FullName, "Kafdeck.slnx"))) return p.FullName;
        throw new InvalidOperationException("Repository root missing.");
    }
}
