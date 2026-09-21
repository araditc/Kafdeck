using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Records;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class RecordApiContractTests
{
    [Fact]
    public void V03_record_contract_exposes_get_only_browse_tail_and_export_routes()
    {
        Assert.Equal(3, V03RecordApiContract.ProductRoutes.Count);
        Assert.All(V03RecordApiContract.ProductRoutes, route => Assert.Equal("GET", route.Method));
        Assert.Contains(V03RecordApiContract.ProductRoutes, route => route.Name == "v03-records-browse");
        Assert.Contains(V03RecordApiContract.ProductRoutes, route => route.Name == "v03-records-tail");
        Assert.Contains(V03RecordApiContract.ProductRoutes, route => route.Name == "v03-records-export");
        Assert.DoesNotContain(V03RecordApiContract.ProductRoutes, route =>
            route.Method is "POST" or "PUT" or "PATCH" or "DELETE");
    }

    [Fact]
    public void Checked_in_v03_OpenApi_is_reproducible_read_only_and_declares_supported_authentication()
    {
        var root = FindRepositoryRoot();
        var checkedIn = File.ReadAllText(Path.Combine(root, "docs", "api", "openapi-v0.3.json"));
        Assert.Equal(NormalizeText(V03RecordApiContract.OpenApiJson), NormalizeText(checkedIn));

        using var document = JsonDocument.Parse(checkedIn);
        var paths = document.RootElement.GetProperty("paths").EnumerateObject().ToArray();
        Assert.Equal(3, paths.Length);
        Assert.All(paths, path =>
        {
            var operations = path.Value.EnumerateObject().Select(property => property.Name).ToArray();
            Assert.Contains("get", operations);
            Assert.DoesNotContain(operations, operation => operation is "post" or "put" or "patch" or "delete");

            var parameters = path.Value
                .GetProperty("get")
                .GetProperty("parameters")
                .EnumerateArray()
                .Select(parameter => parameter.GetProperty("name").GetString())
                .ToArray();
            Assert.Contains("decode", parameters);
        });

        var schemes = document.RootElement
            .GetProperty("components")
            .GetProperty("securitySchemes");
        var oidc = schemes.GetProperty("oidcSession");
        Assert.Equal("apiKey", oidc.GetProperty("type").GetString());
        Assert.Equal("cookie", oidc.GetProperty("in").GetString());
        Assert.Equal("Kafdeck.Session", oidc.GetProperty("name").GetString());
        var deployment = schemes.GetProperty("deploymentToken");
        Assert.Equal("apiKey", deployment.GetProperty("type").GetString());
        Assert.Equal("header", deployment.GetProperty("in").GetString());
        Assert.Equal("X-Kafdeck-Access-Token", deployment.GetProperty("name").GetString());

        var security = document.RootElement.GetProperty("security").EnumerateArray().ToArray();
        Assert.Contains(security, requirement => requirement.TryGetProperty("oidcSession", out _));
        Assert.Contains(security, requirement => requirement.TryGetProperty("deploymentToken", out _));
    }


    [Fact]
    public void Record_endpoint_preserves_HTTP_JSON_contract_for_SSE_and_structured_decode()
    {
        var root = FindRepositoryRoot();
        var source = File.ReadAllText(Path.Combine(
            root,
            "src",
            "backend",
            "Kafdeck.Api",
            "KafdeckRecordEndpoints.cs"));

        Assert.Contains("jsonOptions.Value.SerializerOptions", source, StringComparison.Ordinal);
        Assert.Contains("query.RequireDecodedValue", source, StringComparison.Ordinal);
        Assert.Contains("ParseBoolean(query[\"decode\"], \"decode\")", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Masking_configuration_preserves_exact_header_identity_and_compiles_at_startup_boundary()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Records:Masking:PolicyId"] = "banking-pii",
            ["Kafdeck:Records:Masking:Version"] = "2",
            ["Kafdeck:Records:Masking:StructuredRules:0:Path"] = "/customer/card",
            ["Kafdeck:Records:Masking:HeaderRules:0:Name"] = "authorization ",
        };
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();

        var options = KafdeckConfigurationLoader.Load(configuration);
        var compiled = RecordMaskingPolicyCompiler.Compile(options.Records!.MaskingPolicy);

        Assert.Equal("banking-pii", compiled.PolicyId);
        Assert.Equal(2, compiled.Version);
        Assert.Equal("/customer/card", Assert.Single(compiled.StructuredRules).JsonPointer);
        Assert.Equal("authorization ", Assert.Single(compiled.HeaderRules).HeaderName);
    }

    [Fact]
    public async Task Masking_required_decode_failure_keeps_record_for_fail_closed_projection()
    {
        var raw = new KafkaRawRecord(
            7,
            DateTimeOffset.UtcNow,
            null,
            Encoding.UTF8.GetBytes("{\"secret\":\"clear\"}"),
            []);
        var reader = new StubReader(raw);
        var decoder = new FailingDecoder();
        var service = new RecordFilterService(reader, decoder);
        var request = new RecordReadRequest(
            "cluster-a",
            "payments",
            0,
            RecordAnchor.AtOffset(7),
            RecordReadDirection.Forward,
            RecordOperationBudget.Default);
        var plan = RecordFilterCompiler.Compile(new RecordFilterRequest());
        var operation = new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(5));

        var filtered = await service.FilterPageAsync(
            request,
            plan,
            operation,
            requireDecodedValue: true,
            CancellationToken.None);

        Assert.True(filtered.IsSuccess);
        var item = Assert.Single(filtered.Value!.Records);
        Assert.Null(item.DecodedValue);

        var policy = RecordMaskingPolicyCompiler.Compile(
            new RecordMaskingPolicyDefinition(
                "mandatory",
                1,
                [new RecordStructuredMaskRule("/secret")]));
        var safe = new RecordMaskingService().Apply(0, item, policy);
        Assert.Equal(RecordPayloadProjectionKind.FullyRedacted, safe.ValueKind);
        Assert.Null(safe.RawValue);
        Assert.DoesNotContain("clear", JsonSerializer.Serialize(safe), StringComparison.Ordinal);
    }

    private static string NormalizeText(string value) =>
        value.Replace("\r\n", "\n", StringComparison.Ordinal).Trim();

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Unable to locate Kafdeck repository root from test output directory.");
    }

    private sealed class StubReader(KafkaRawRecord record) : IKafkaRecordReadPort
    {
        public Task<KafkaResult<RecordReadBatch>> ReadPageAsync(
            RecordReadRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken)
        {
            var now = DateTimeOffset.UtcNow;
            var observation = new ObservationMetadata(now, now.AddSeconds(1), now.AddSeconds(5), ObservationSource.Live);
            var batch = new RecordReadBatch(
                [record],
                0,
                10,
                record.Offset,
                record.Offset,
                RecordAnchor.AtOffset(record.Offset + 1),
                null,
                RecordBudgetOutcome.Complete);
            return Task.FromResult(KafkaResult<RecordReadBatch>.Success(batch, observation));
        }
    }

    private sealed class FailingDecoder : IRecordDecodePort
    {
        public Task<RecordSchemaResult<RecordDecodedValue>> DecodeAsync(
            RecordDecodeRequest request,
            KafkaOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(RecordSchemaResult<RecordDecodedValue>.Failed(
                new RecordSchemaFailure(
                    RecordSchemaFailureCategory.DecodeFailed,
                    "decode_failed",
                    "Record decode failed.",
                    false)));
    }
}
