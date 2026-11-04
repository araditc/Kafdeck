using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Api;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http.Metadata;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G5DeclarativeOpenApiTests
{
    [Fact]
    public async Task Document_matches_every_actual_named_declarative_endpoint_and_http_method()
    {
        var builder=WebApplication.CreateBuilder();
        builder.Services.AddSingleton<IW68RuntimeStore>(_=>null!);
        builder.Services.AddSingleton<Kafdeck.Core.Notifications.INotificationRoutingStore>(_=>null!);
        builder.Services.AddSingleton<DeclarativeRuntimeAuthorizer>(_=>null!);
        builder.Services.AddSingleton<KafdeckAuthorizationService>(_=>null!);
        builder.Services.AddSingleton<DeclarativeEndpointService>(_=>null!);
        await using var app=builder.Build();app.MapKafdeckDeclarative(true);
        var endpoints=((IEndpointRouteBuilder)app).DataSources.SelectMany(x=>x.Endpoints).OfType<RouteEndpoint>().ToArray();
        using var document=JsonDocument.Parse(KafdeckV08BearerOpenApi.Document);
        var paths=document.RootElement.GetProperty("paths");
        Assert.Equal(15,endpoints.Length);
        foreach(var endpoint in endpoints)
        {
            var path=Regex.Replace(endpoint.RoutePattern.RawText!,@":[^}]+","");
            var named=endpoint.Metadata.GetMetadata<IEndpointNameMetadata>();Assert.NotNull(named);
            var method=Assert.Single(endpoint.Metadata.GetMetadata<HttpMethodMetadata>()!.HttpMethods).ToLowerInvariant();
            Assert.True(paths.TryGetProperty(path,out var entry),"Deployed endpoint absent from OpenAPI: "+path);
            var operation=entry.GetProperty(method);Assert.Equal(named.EndpointName,operation.GetProperty("operationId").GetString());
            Assert.Equal("bearerAuth",Assert.Single(operation.GetProperty("security").EnumerateArray()).EnumerateObject().Single().Name);
            Assert.NotEmpty(operation.GetProperty("x-kafdeck-actions").EnumerateArray());
        }
    }

    [Theory]
    [InlineData("DeclarativeManifestRequest",typeof(DeclarativeManifestRequest))]
    [InlineData("DeclarativeApplyRequest",typeof(DeclarativeApplyRequest))]
    [InlineData("DeclarativeSourceAdvanceRequest",typeof(DeclarativeSourceAdvanceRequest))]
    public void Closed_request_fields_match_actual_runtime_DTOs(string name,Type dto)
    {
        using var document=JsonDocument.Parse(KafdeckV08BearerOpenApi.Document);
        var schema=document.RootElement.GetProperty("components").GetProperty("schemas").GetProperty(name);
        Assert.False(schema.GetProperty("additionalProperties").GetBoolean());
        var actual=dto.GetProperties().Select(x=>JsonNamingPolicy.CamelCase.ConvertName(x.Name)).Order(StringComparer.Ordinal);
        Assert.Equal(actual,schema.GetProperty("properties").EnumerateObject().Select(x=>x.Name).Order(StringComparer.Ordinal));
        Assert.Equal(actual,schema.GetProperty("required").EnumerateArray().Select(x=>x.GetString()!).Order(StringComparer.Ordinal));
        Assert.DoesNotContain(schema.GetProperty("properties").EnumerateObject(),x=>x.Name.Contains("principal",StringComparison.OrdinalIgnoreCase)||x.Name.Contains("provider",StringComparison.OrdinalIgnoreCase)||x.Name.Contains("approver",StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Source_CAS_and_inverse_preview_have_exact_closed_bounds_and_no_approval_shortcut()
    {
        using var document=JsonDocument.Parse(KafdeckV08BearerOpenApi.Document);var root=document.RootElement;
        var schemas=root.GetProperty("components").GetProperty("schemas");
        var manifest=schemas.GetProperty("DeclarativeManifestRequest").GetProperty("properties");
        Assert.Equal(16384,manifest.GetProperty("manifest").GetProperty("maxLength").GetInt32());
        Assert.Equal("^[a-f0-9]{64}$",manifest.GetProperty("sourceRevision").GetProperty("pattern").GetString());
        Assert.Equal(0,manifest.GetProperty("expectedOwnershipRevision").GetProperty("minimum").GetInt32());
        Assert.Equal(new[]{"json","yaml"},manifest.GetProperty("format").GetProperty("enum").EnumerateArray().Select(x=>x.GetString()));
        var paths=root.GetProperty("paths");var preview=paths.GetProperty("/api/v1/declarative/plans/{sourcePlanId}/compensation-preview").GetProperty("post");
        Assert.Equal("#/components/schemas/DeclarativeManifestRequest",preview.GetProperty("requestBody").GetProperty("content").GetProperty("application/json").GetProperty("schema").GetProperty("$ref").GetString());
        Assert.Contains(preview.GetProperty("parameters").EnumerateArray(),x=>x.GetProperty("name").GetString()=="Idempotency-Key"&&x.GetProperty("required").GetBoolean());
        Assert.Contains("distinct human approval",preview.GetProperty("description").GetString(),StringComparison.Ordinal);
        Assert.True(schemas.GetProperty("DeclarativeStoredPlan").GetProperty("properties").TryGetProperty("compensatesPlanId",out _));
        Assert.DoesNotContain(paths.EnumerateObject(),x=>x.Name.Contains("declarative",StringComparison.Ordinal)&& (x.Name.EndsWith("/destroy",StringComparison.Ordinal)||x.Name.EndsWith("/delete",StringComparison.Ordinal)));
    }
}
