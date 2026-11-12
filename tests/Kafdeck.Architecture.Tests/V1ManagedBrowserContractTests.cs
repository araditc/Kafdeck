using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Confluent.Kafka;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Kafdeck.Modules.Clusters;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedBrowserContractTests
{
    [Theory]
    [InlineData(true, true, 20)]
    [InlineData(true, false, 17)]
    [InlineData(false, true, 3)]
    public async Task Deployed_inventory_matches_the_catalog_names_DTOs_CSRF_and_declared_limits(bool managed, bool workspace, int count)
    {
        await using var f = await ContractFixture.Start(managed, workspace);
        using var catalog = JsonDocument.Parse(ManagedBrowserOpenApi.Catalog);
        using var deployed = await f.Client.GetFromJsonAsync<JsonDocument>(ManagedBrowserOpenApi.DocumentPath);
        var endpoints = ((IEndpointRouteBuilder)f.App).DataSources.SelectMany(s => s.Endpoints).OfType<RouteEndpoint>()
            .Where(e => e.RoutePattern.RawText == "/api/v1/setup" ||
                e.RoutePattern.RawText?.StartsWith("/api/v1/identity/users", StringComparison.Ordinal) == true ||
                e.RoutePattern.RawText?.StartsWith("/api/v1/connections/", StringComparison.Ordinal) == true).ToArray();
        Assert.All(endpoints, e => Assert.NotNull(e.Metadata.GetMetadata<ManagedBrowserRouteContract>()));
        Assert.Equal(count, endpoints.Length);
        Assert.Equal(count, deployed!.RootElement.GetProperty("paths").EnumerateObject().Sum(p => p.Value.EnumerateObject().Count()));
        Assert.Equal("experimental", deployed.RootElement.GetProperty("x-stability").GetString());
        Assert.Equal(endpoints.Length, endpoints.Select(e => e.Metadata.GetMetadata<IEndpointNameMetadata>()!.EndpointName).Distinct().Count());
        foreach (var endpoint in endpoints)
        {
            var metadata = endpoint.Metadata.GetMetadata<ManagedBrowserRouteContract>()!;
            var verb = Assert.Single(endpoint.Metadata.GetMetadata<HttpMethodMetadata>()!.HttpMethods).ToLowerInvariant();
            var path = ManagedBrowserOpenApi.Normalize(endpoint);
            var operation = deployed.RootElement.GetProperty("paths").GetProperty(path).GetProperty(verb);
            Assert.Equal(metadata.OperationId, operation.GetProperty("operationId").GetString());
            Assert.Equal(metadata.OperationId, endpoint.Metadata.GetMetadata<IEndpointNameMetadata>()!.EndpointName);
            Assert.Equal(verb != "get", operation.GetProperty("x-csrf-required").GetBoolean());
            Assert.Equal(verb != "get", endpoint.Metadata.GetMetadata<IAntiforgeryMetadata>()?.RequiresValidation == true);
            Assert.DoesNotContain("Bearer", operation.GetProperty("security").GetRawText(), StringComparison.Ordinal);
            Assert.Null(Kafdeck.Contracts.KafdeckReadContracts.Find(metadata.OperationId));
            if (metadata.RequestType is { } type)
            {
                var schema = catalog.RootElement.GetProperty("components").GetProperty("schemas")
                    .GetProperty(operation.GetProperty("x-runtime-request-schema").GetString()!);
                Assert.Equal(type.GetProperties().Select(p => JsonNamingPolicy.CamelCase.ConvertName(p.Name)).Order(StringComparer.Ordinal),
                    schema.GetProperty("properties").EnumerateObject().Select(p => p.Name).Order(StringComparer.Ordinal));
                Assert.True(schema.GetProperty("additionalProperties").GetBoolean()); // Current runtime ignores unknown JSON fields.
            }
            if (verb != "get")
                Assert.Equal<long?>(operation.GetProperty("x-declared-body-limit-bytes").GetInt64(),
                    endpoint.Metadata.GetMetadata<IRequestSizeLimitMetadata>()!.MaxRequestBodySize);
            Assert.Equal(endpoint.RoutePattern.Parameters.Select(p => p.Name).Order(StringComparer.Ordinal),
                operation.TryGetProperty("parameters", out var parameters)
                    ? parameters.EnumerateArray().Where(p => p.GetProperty("in").GetString() == "path").Select(p => p.GetProperty("name").GetString()!).Order(StringComparer.Ordinal)
                    : []);
        }
    }

    [Fact]
    public void Catalog_has_resolvable_refs_unique_operations_and_no_secret_output_fields()
    {
        using var d = JsonDocument.Parse(ManagedBrowserOpenApi.Catalog);
        var root = d.RootElement; var names = new HashSet<string>(StringComparer.Ordinal);
        void Visit(JsonElement node)
        {
            if (node.ValueKind == JsonValueKind.Array) { foreach (var item in node.EnumerateArray()) Visit(item); }
            if (node.ValueKind != JsonValueKind.Object) return;
            if (node.TryGetProperty("$ref", out var reference)) Assert.NotEqual(JsonValueKind.Undefined, Resolve(root, reference.GetString()!).ValueKind);
            foreach (var item in node.EnumerateObject()) Visit(item.Value);
        }
        Visit(root);
        foreach (var path in root.GetProperty("paths").EnumerateObject())
        foreach (var method in path.Value.EnumerateObject())
        {
            Assert.True(names.Add(method.Value.GetProperty("operationId").GetString()!));
            Assert.Equal("experimental", method.Value.GetProperty("x-stability").GetString());
        }
        Assert.Equal(20, names.Count);
        foreach (var schema in new[] { "ManagedUser", "PendingProvisioning", "WorkspaceTarget", "PublicSettings" })
            Assert.DoesNotContain(root.GetProperty("components").GetProperty("schemas").GetProperty(schema).GetProperty("properties").EnumerateObject(),
                p => p.Name.Contains("password", StringComparison.OrdinalIgnoreCase) || p.Name.Contains("secret", StringComparison.OrdinalIgnoreCase) || p.Name.Contains("proof", StringComparison.OrdinalIgnoreCase) || p.Name.Contains("protected", StringComparison.OrdinalIgnoreCase));
        foreach (var (schema, field) in new[] { ("SetupInput", "code"), ("SetupInput", "password"), ("NewUserInput", "password"), ("ConnectionDraft", "password"), ("ConnectionDraft", "clientKey"), ("SettingsInput", "secret") })
            Assert.True(root.GetProperty("components").GetProperty("schemas").GetProperty(schema).GetProperty("properties").GetProperty(field).GetProperty("writeOnly").GetBoolean());
    }

    [Theory]
    [InlineData("available")]
    [InlineData("recovery")]
    [InlineData("complete")]
    public async Task Setup_state_response_conforms_without_exposing_the_host_code(string state)
    {
        await using var f = await ContractFixture.Start();
        if (state != "complete") f.Identity.Store.Write(new(ManagedProvisioningFixture.Issuer, null, state == "recovery", null, default, []));
        using var response = await f.Client.GetAsync("/api/v1/setup");
        var json = await CheckResponse(response, "/api/v1/setup", "get");
        Assert.Equal(state, json.GetProperty("state").GetString());
        Assert.Equal(state == "available", json.GetProperty("csrf").ValueKind == JsonValueKind.String);
        Assert.DoesNotContain("code", json.EnumerateObject().Select(p => p.Name));
    }

    [Fact]
    public async Task Users_and_connection_observations_match_their_actual_wire_shapes()
    {
        await using var f = await ContractFixture.Start();
        using var users = await f.Client.GetAsync("/api/v1/identity/users");
        await CheckResponse(users, "/api/v1/identity/users", "get");
        using var test = await f.Client.PostAsJsonAsync("/api/v1/connections/test", new { bootstrapServers = new[] { "127.0.0.1:19092" }, protocol = "PLAINTEXT", name = "Contract probe", ignoredFutureProperty = true });
        var observed = await CheckResponse(test, "/api/v1/connections/test", "post");
        Assert.True(observed.GetProperty("requiresRegistration").GetBoolean());
        Assert.Equal(JsonValueKind.Null, observed.GetProperty("savedTarget").ValueKind);
        using var registration = await f.Client.PostAsJsonAsync("/api/v1/connections/targets", new { bootstrapServers = new[] { "127.0.0.1:19092" }, protocol = "PLAINTEXT", name = "Contract probe" });
        Assert.Equal(HttpStatusCode.OK, registration.StatusCode); // Existing wire contract, not 201.
        await CheckResponse(registration, "/api/v1/connections/targets", "post");
        using var targets = await f.Client.GetAsync("/api/v1/connections/targets");
        await CheckResponse(targets, "/api/v1/connections/targets", "get");
    }

    [Fact]
    public async Task User_create_and_access_update_return_the_documented_public_user_shape()
    {
        await using var f = await ContractFixture.Start();
        using var created = await f.Client.PostAsJsonAsync("/api/v1/identity/users", f.Identity.Input);
        Assert.Equal(HttpStatusCode.Created, created.StatusCode);
        var user = await CheckResponse(created, "/api/v1/identity/users", "post");
        var id = user.GetProperty("id").GetGuid();
        using var updated = await f.Client.PutAsJsonAsync($"/api/v1/identity/users/{id:D}", new { role = "manager", clusterIds = Array.Empty<string>(), enabled = true });
        Assert.Equal(HttpStatusCode.OK, updated.StatusCode);
        var changed = await CheckResponse(updated, "/api/v1/identity/users/{id}", "put");
        Assert.Equal("manager", changed.GetProperty("role").GetString());
        Assert.Equal(id, changed.GetProperty("id").GetGuid());
        Assert.DoesNotContain("password", changed.EnumerateObject().Select(p => p.Name));
    }

    [Fact]
    public async Task Pending_problem_operation_identifier_and_recovered_user_have_the_documented_wire_shape()
    {
        await using var f = await ContractFixture.Start(); f.Identity.Provider.LoseReply = true;
        using var created = await f.Client.PostAsJsonAsync("/api/v1/identity/users", f.Identity.Input);
        Assert.Equal(HttpStatusCode.Conflict, created.StatusCode);
        var pending = await CheckResponse(created, "/api/v1/identity/users", "post");
        Assert.Equal("urn:kafdeck:problem:identity-provisioning-pending", pending.GetProperty("type").GetString());
        var operation = pending.GetProperty("operationId").GetGuid();
        Assert.NotEqual(Guid.Empty, operation);
        using var recovered = await f.Client.PostAsync($"/api/v1/identity/users/provisioning/{operation:D}/recover", null);
        Assert.Equal(HttpStatusCode.Created, recovered.StatusCode);
        await CheckResponse(recovered, "/api/v1/identity/users/provisioning/{operationId}/recover", "post");
        Assert.Equal(1, f.Identity.Provider.CreateCount);
    }

    [Theory]
    [InlineData("schema-registry")]
    [InlineData("kafka-connect")]
    [InlineData("ksqldb")]
    public async Task Unconfigured_service_reads_are_redacted_and_CSRF_rejections_match_the_contract(string slug)
    {
        await using var f = await ContractFixture.Start(); f.AddTarget();
        var path = "/api/v1/connections/connection-contract/" + slug;
        using var response = await f.Client.GetAsync(path);
        var settings = await CheckResponse(response, "/api/v1/connections/{clusterId}/" + slug, "get");
        Assert.Equal(JsonValueKind.Null, settings.GetProperty("revision").ValueKind);
        Assert.Equal("none", settings.GetProperty("authentication").GetString());
        f.Client.DefaultRequestHeaders.Remove(KafdeckAntiforgeryExtensions.HeaderName);
        using var denied = await f.Client.PutAsJsonAsync(path, new { url = "http://127.0.0.1:1/", authentication = "none" });
        Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode);
        var problem = await CheckResponse(denied, "/api/v1/connections/{clusterId}/" + slug, "put");
        Assert.EndsWith(":antiforgery-validation-failed", problem.GetProperty("type").GetString());
    }

    [Fact]
    public async Task Real_HTTP_settings_probe_save_retention_and_stale_revision_follow_the_documented_CAS()
    {
        await using var f = await ContractFixture.Start(); f.AddTarget();
        var serviceBuilder = WebApplication.CreateBuilder(); serviceBuilder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var service = serviceBuilder.Build(); var headers = new List<string>();
        service.MapGet("/subjects", (HttpContext c) => { headers.Add(c.Request.Headers.Authorization.ToString()); return Results.Ok(new[] { "contract.subject" }); });
        await service.StartAsync();
        try
        {
            const string secret = "contract-only-private-value";
            var url = Assert.Single(service.Urls) + "/";
            const string path = "/api/v1/connections/connection-contract/schema-registry";
            const string template = "/api/v1/connections/{clusterId}/schema-registry";
            using var test = await f.Client.PostAsJsonAsync(path + "/test", new { url, authentication = "basic", username = "contract", secret });
            await CheckResponse(test, template + "/test", "post");
            Assert.Null(Assert.Single(f.Identity.Store.Read().Connections!).SchemaRegistry);
            using var saved = await f.Client.PutAsJsonAsync(path, new { url, authentication = "basic", username = "contract", secret });
            var first = await CheckResponse(saved, template, "put"); var revision = first.GetProperty("revision").GetGuid();
            Assert.DoesNotContain(secret, first.GetRawText(), StringComparison.Ordinal);
            using var retained = await f.Client.PutAsJsonAsync(path, new { url, authentication = "basic", revision });
            var second = await CheckResponse(retained, template, "put");
            Assert.NotEqual(revision, second.GetProperty("revision").GetGuid());
            Assert.Equal(3, headers.Count); Assert.All(headers, h => Assert.Equal(headers[0], h)); Assert.StartsWith("Basic ", headers[0]);
            using var stale = await f.Client.PutAsJsonAsync(path, new { url, authentication = "none", revision });
            Assert.Equal(HttpStatusCode.Conflict, stale.StatusCode); await CheckResponse(stale, template, "put");
            Assert.Equal(3, headers.Count);
        }
        finally { await service.StopAsync(); }
    }

    [Fact]
    public async Task Malformed_DTO_and_non_owner_requests_do_not_gain_managed_authority()
    {
        await using var f = await ContractFixture.Start();
        using var malformed = await f.Client.PostAsync("/api/v1/identity/users", new StringContent("{", System.Text.Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, malformed.StatusCode);
        f.Actor = new ClaimsPrincipal(new ClaimsIdentity());
        using var denied = await f.Client.GetAsync("/api/v1/identity/users");
        Assert.Equal(HttpStatusCode.Forbidden, denied.StatusCode); await CheckResponse(denied, "/api/v1/identity/users", "get");
        Assert.Equal(0, f.Identity.Provider.CreateCount);
        Assert.Single(f.Identity.Store.Read().Users);
        using var discovery = await f.Client.GetAsync(ManagedBrowserOpenApi.DocumentPath);
        Assert.True(discovery.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden);
    }

    private static async Task<JsonElement> CheckResponse(HttpResponseMessage response, string path, string method)
    {
        using var catalog = JsonDocument.Parse(ManagedBrowserOpenApi.Catalog);
        var operation = catalog.RootElement.GetProperty("paths").GetProperty(path).GetProperty(method);
        var schema = operation.GetProperty("responses").GetProperty(((int)response.StatusCode).ToString(System.Globalization.CultureInfo.InvariantCulture))
            .GetProperty("content").EnumerateObject().Single().Value.GetProperty("schema");
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        CheckShape(catalog.RootElement, schema, body.RootElement);
        return body.RootElement.Clone();
    }

    private static JsonElement Resolve(JsonElement root, string reference)
    {
        Assert.StartsWith("#/", reference);
        foreach (var part in reference[2..].Split('/')) root = root.GetProperty(part);
        return root;
    }

    // Focused wire-shape assertions, not a replacement for a complete JSON Schema validator.
    private static void CheckShape(JsonElement root, JsonElement schema, JsonElement value)
    {
        if (schema.TryGetProperty("$ref", out var reference)) { CheckShape(root, Resolve(root, reference.GetString()!), value); return; }
        if (schema.TryGetProperty("anyOf", out var alternatives))
        {
            var candidate = alternatives.EnumerateArray().First(s =>
                (value.ValueKind == JsonValueKind.Null) == (s.TryGetProperty("type", out var t) && t.ValueKind == JsonValueKind.String && t.GetString() == "null"));
            CheckShape(root, candidate, value); return;
        }
        if (schema.TryGetProperty("type", out var types))
        {
            var actual = value.ValueKind switch { JsonValueKind.Object => "object", JsonValueKind.Array => "array", JsonValueKind.String => "string", JsonValueKind.Number => "integer", JsonValueKind.True or JsonValueKind.False => "boolean", _ => "null" };
            Assert.Contains(actual, types.ValueKind == JsonValueKind.Array ? types.EnumerateArray().Select(t => t.GetString()) : [types.GetString()]);
        }
        if (schema.TryGetProperty("enum", out var allowed)) Assert.Contains(allowed.EnumerateArray(), a => a.GetRawText() == value.GetRawText());
        if (value.ValueKind == JsonValueKind.Object)
        {
            if (schema.TryGetProperty("required", out var required)) foreach (var key in required.EnumerateArray()) Assert.True(value.TryGetProperty(key.GetString()!, out _), "Required response field missing: " + key.GetString());
            if (schema.TryGetProperty("properties", out var properties)) foreach (var item in value.EnumerateObject()) if (properties.TryGetProperty(item.Name, out var child)) CheckShape(root, child, item.Value);
        }
        if (value.ValueKind == JsonValueKind.Array && schema.TryGetProperty("items", out var items)) foreach (var item in value.EnumerateArray()) CheckShape(root, items, item);
    }

    private sealed class ContractFixture : IAsyncDisposable
    {
        internal readonly ManagedProvisioningFixture Identity = new();
        internal WebApplication App = null!;
        internal HttpClient Client = null!;
        internal ClaimsPrincipal Actor = null!;
        internal void AddTarget() { var s = Identity.Store.Read(); Identity.Store.Write(s with { Connections = [new("connection-contract", "Contract", ["127.0.0.1:19092"], "PLAINTEXT", null)] }); }
        internal static async Task<ContractFixture> Start(bool managed = true, bool workspace = true)
        {
            var f = new ContractFixture();
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Production" }); builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Configuration["Kafdeck:ConnectionWorkspace:RegistrationNetworks:0"] = "127.0.0.0/8";
            var options = new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0", null, AccessMode.Oidc, null), []);
            var policy = AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition([], [], []));
            builder.Services.AddSingleton(new KafdeckAuthorizationService(options, new AuthorizationPolicyEvaluator(policy)) { ManagedIdentity = f.Identity.Store });
            builder.Services.AddSingleton<ISecurityAuditSink>(f.Identity.Audit); builder.Services.AddSingleton(f.Identity.Client);
            builder.Services.AddSingleton(new GovernanceRuntimeOptions()); builder.Services.AddSingleton<ClusterExplorerService>(_ => null!);
            builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
            f.Actor = OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", f.Identity.OwnerId.ToString("D"))], "oidc")), ManagedProvisioningFixture.Issuer, null, DateTimeOffset.UtcNow);
            f.App = builder.Build(); f.App.UseRouting(); f.App.Use((context, next) => { context.User = f.Actor; return next(context); }); f.App.UseAntiforgery(); f.App.MapKafdeckAntiforgeryEndpoint();
            if (managed)
            {
                f.App.MapManagedIdentity(f.Identity.Store, options);
                var runtime = new RegisteredHttpServiceRuntime(f.Identity.Store, new HttpContextAccessor(), new EphemeralDataProtectionProvider(), builder.Configuration);
                f.App.MapRegisteredHttpServices(f.Identity.Store, runtime);
            }
            if (workspace) f.App.MapKafkaConnectionWorkspace(options, f.Identity.Store, (_, _) => Task.FromResult(new Metadata([new Confluent.Kafka.BrokerMetadata(1, "127.0.0.1", 19092)], [], 1, "127.0.0.1:19092")));
            f.App.MapManagedBrowserOpenApi(); await f.App.StartAsync();
            f.Client = new(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new Uri(Assert.Single(f.App.Urls)) };
            using var csrf = await f.Client.GetFromJsonAsync<JsonDocument>("/api/v1/auth/csrf");
            f.Client.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName, csrf!.RootElement.GetProperty("requestToken").GetString());
            return f;
        }
        public async ValueTask DisposeAsync() { Client.Dispose(); await App.StopAsync(); await App.DisposeAsync(); Identity.Dispose(); }
    }
}
