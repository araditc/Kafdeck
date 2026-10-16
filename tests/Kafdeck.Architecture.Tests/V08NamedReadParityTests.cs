using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafdeck.Api;
using Kafdeck.Cli;
using Kafdeck.Contracts;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Records;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08NamedReadParityTests
{
    [Fact]
    public void Inventory_is_closed_and_every_named_path_is_documented_without_effect_authority()
    {
        Assert.Equal(67, KafdeckReadContracts.All.Count);
        Assert.Equal(67, KafdeckReadContracts.All.Select(c => c.Name).Distinct().Count());
        Assert.Equal(5, KafdeckReadContracts.All.Count(c => c.Method == "post"));
        Assert.All(KafdeckReadContracts.All, c =>
        {
            Assert.Contains(c.Method, new[] { "get", "post" });
            Assert.True(Enum.TryParse<AuthorizationAction>(c.Action, out _));
            Assert.DoesNotContain("/execute", c.Path); Assert.DoesNotContain("/mutations/", c.Path);
            Assert.Equal(c.Method == "post", BearerReadBoundary.RequestType(c) is not null);
        });
        Assert.Null(KafdeckReadContracts.Find("v05-mutation-execute"));
        Assert.Null(KafdeckReadContracts.Find("v05-auth-csrf"));
        using var document = JsonDocument.Parse(KafdeckV08BearerOpenApi.Document);
        foreach (var contract in KafdeckReadContracts.All)
            Assert.Equal(contract.Name, document.RootElement.GetProperty("paths").GetProperty(contract.Path)
                .GetProperty(contract.Method).GetProperty("operationId").GetString());
    }

    [Theory]
    [InlineData("v03-records-browse", "cluster", "orders", "0")]
    [InlineData("v07-connect-profile-connectors-detail", "cluster", "profile", "connector")]
    public void Cli_view_name_cannot_select_an_arbitrary_path_or_authority(string name, string first, string second, string third)
    {
        var contract = KafdeckReadContracts.Find(name)!;
        var invocation = CliReadCommands.TryParse(["views", name, first, second, third]);
        Assert.NotNull(invocation); Assert.True(invocation.Read); Assert.False(invocation.Preview);
        Assert.Equal(contract.Method, invocation.Method); Assert.Equal(name, invocation.ReadContract);
        Assert.Throws<CliUsageException>(() => CliReadCommands.TryParse(["views", "https://other.invalid/admin"]));
        Assert.Throws<CliUsageException>(() => CliReadCommands.TryParse(["views", name, first, "..", third]));
    }

    [Fact]
    public void Closed_query_rejects_unknown_repeated_and_control_fields_and_encodes_values()
    {
        var contract = KafdeckReadContracts.Find("v03-records-browse")!;
        Assert.True(BearerReadBoundary.ValidQuery(new QueryCollection(new Dictionary<string, StringValues> { ["maxRecords"] = "10" }), contract));
        Assert.False(BearerReadBoundary.ValidQuery(new QueryCollection(new Dictionary<string, StringValues> { ["force"] = "true" }), contract));
        Assert.False(BearerReadBoundary.ValidQuery(new QueryCollection(new Dictionary<string, StringValues> { ["offset"] = new StringValues(new[] { "0", "1" }) }), contract));
        using var query = JsonDocument.Parse("{\"keyEquals\":\"a&offset=999\",\"maxRecords\":10}");
        Assert.True(CliReadCommands.Validate(query.RootElement, contract.Name));
        var invocation = CliReadCommands.TryParse(["views", contract.Name, "cluster", "topic", "0"])!;
        Assert.Contains("keyEquals=a%26offset%3D999", CliReadCommands.BuildRoute(invocation, query), StringComparison.Ordinal);
        using var unknown = JsonDocument.Parse("{\"providerUrl\":\"https://other.invalid\"}");
        Assert.False(CliReadCommands.Validate(unknown.RootElement, contract.Name));
    }

    [Theory]
    [InlineData("{\"format\":\"cbor\",\"structuredValue\":{\"a\":1}}", true)]
    [InlineData("{\"format\":\"cbor\",\"structuredValue\":{\"a\":1,\"a\":2}}", false)]
    [InlineData("{\"format\":\"cbor\",\"structuredValue\":{},\"requesterPrincipalId\":\"override\"}", false)]
    public void Read_post_tools_preserve_arbitrary_structured_data_but_reject_duplicate_or_authority_fields(string json, bool expected)
    {
        using var document = JsonDocument.Parse(json);
        Assert.Equal(expected, BearerFamilyRoutes.IsClosed(document.RootElement, typeof(ControlledSerdeEncodeApiRequest)));
        Assert.Equal(expected, CliReadCommands.Validate(document.RootElement, "v07-serde-encode"));
    }

    [Fact]
    public void Record_read_registration_alone_does_not_grant_backend_scope_and_revocation_is_immediate()
    {
        var values = new Dictionary<string, string?>
        {
            ["Enabled"] = "true", ["Issuer"] = "https://sso.test/realms/read", ["ApiAudience"] = "api",
            ["Registrations:0:Id"] = "reader", ["Registrations:0:OwnerId"] = "team", ["Registrations:0:Subject"] = "subject",
            ["Registrations:0:ClientId"] = "client", ["Registrations:0:Enabled"] = "true",
            ["Registrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(5).ToString("O"), ["Registrations:0:ReadActions:0"] = "RecordRead",
        };
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values.ToDictionary(v => "Kafdeck:NonBrowserRead:" + v.Key, v => v.Value)).Build();
        var registry = new NonBrowserReadRegistry(configuration);
        var identity = new NonBrowserReadIdentity("reader", new("https://sso.test/realms/read", "subject", "client", "api", DateTimeOffset.UtcNow.AddMinutes(1)));
        var denied = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new([], [], [])));
        Assert.True(registry.IsRegisteredReadAllowed(identity, AuthorizationAction.RecordRead));
        Assert.False(registry.IsAllowed(identity, new(AuthorizationAction.RecordRead, "cluster", "orders"), denied));
        var granted = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
            [new("read", [new(AuthorizationAction.RecordRead, ["cluster"], ["orders"])])],
            [new("https://sso.test/realms/read", "subject", ["read"])], [])));
        Assert.True(registry.IsAllowed(identity, new(AuthorizationAction.RecordRead, "cluster", "orders"), granted));
        Assert.False(registry.IsAllowed(identity, new(AuthorizationAction.RecordRead, "cluster", "other"), granted));
        Assert.False(registry.IsAllowed(identity, new(AuthorizationAction.RecordExport, "cluster", "orders"), granted));
        configuration["Kafdeck:NonBrowserRead:Registrations:0:Enabled"] = "false";
        Assert.False(registry.IsAllowed(identity, new(AuthorizationAction.RecordRead, "cluster", "orders"), granted));
    }

    [Fact]
    public async Task Export_rechecks_live_authority_without_fabricating_a_browser_identity()
    {
        RecordSafeProjection Row(long offset, string value) => new(0, offset, null, null, false, RecordPayloadProjectionKind.Raw,
            Encoding.UTF8.GetBytes(value), null, [], "mask", 1, []);
        var page = new RecordSafePage("cluster", "topic", 0, [Row(1, "first"), Row(2, "second")], 0, 3,
            null, null, RecordBudgetOutcome.Complete, RecordFilterBudgetOutcome.Complete, [], "mask", 1);
        var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new([], [], [])));
        var service = new RecordExportService(); await using var denied = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => service.ExportAsync(page, new(RecordExportFormat.Ndjson), evaluator,
            null, denied, liveAuthorization: () => false));
        Assert.Equal(0, denied.Length);
        var checks = 0; await using var partial = new MemoryStream();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => service.ExportAsync(page, new(RecordExportFormat.Ndjson), evaluator,
            null, partial, liveAuthorization: () => ++checks <= 2));
        Assert.Equal(3, checks); Assert.NotEmpty(partial.ToArray());
        Assert.DoesNotContain(Convert.ToBase64String(Encoding.UTF8.GetBytes("second")), Encoding.UTF8.GetString(partial.ToArray()), StringComparison.Ordinal);
    }

    [Fact]
    public void Tail_requires_an_explicit_completed_frame_for_success()
    {
        Assert.Equal(CliApplication.RemoteError, CliReadCommands.Outcome("data: {\"kind\":\"records\"}\n\n", "v03-records-tail"));
        Assert.Equal(CliApplication.RemoteError, CliReadCommands.Outcome("data: {\"kind\":\"error\"}\n\n", "v03-records-tail"));
        Assert.Equal(CliApplication.Success, CliReadCommands.Outcome("data: {\"kind\":\"completed\"}\n\n", "v03-records-tail"));
    }

    [Fact]
    public void Openapi_response_types_preserve_nullable_redaction_and_actual_duration_without_invented_caps()
    {
        var schemas = new JsonObject();
        var text = BearerFamilyOpenApi.Schema(typeof(string), schemas);
        Assert.False(text.ContainsKey("maxLength"));
        var rows = BearerFamilyOpenApi.Schema(typeof(IReadOnlyList<ConfigurationEntryData>), schemas);
        Assert.False(rows.ContainsKey("maxItems"));
        var value = schemas["FamilyConfigurationEntryData"]!["properties"]!["value"]!;
        Assert.Contains(value["anyOf"]!.AsArray(), alternative => alternative!["type"]?.GetValue<string>() == "null");
        var duration = BearerFamilyOpenApi.Schema(typeof(TimeSpan), schemas);
        Assert.Equal("string", duration["type"]!.GetValue<string>());
        Assert.Matches(duration["pattern"]!.GetValue<string>(), JsonSerializer.SerializeToElement(TimeSpan.FromDays(2)).GetString()!);
        using var document = JsonDocument.Parse(KafdeckV08BearerOpenApi.Document);
        var paths = document.RootElement.GetProperty("paths");
        var rootPath = "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions/{partition}/records";
        Assert.Equal("array", paths.GetProperty(rootPath + "/export").GetProperty("get").GetProperty("responses")
            .GetProperty("200").GetProperty("content").GetProperty("application/json").GetProperty("schema").GetProperty("type").GetString());
        Assert.Equal("string", paths.GetProperty(rootPath + "/tail").GetProperty("get").GetProperty("responses")
            .GetProperty("200").GetProperty("content").GetProperty("text/event-stream").GetProperty("schema").GetProperty("type").GetString());
    }
}
