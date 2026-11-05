using System.Text.Json;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W74W76CliTests
{
    private const string Id="11111111-1111-4111-8111-111111111111",Request="11111111111141118111111111111111";
    [Theory]
    [InlineData("measurements","list")][InlineData("attribution","windows")][InlineData("dashboards","show")]
    public void Measured_reads_bind_registered_family_exact_interval_resource_and_signed_cursor(string family,string command)
    {
        var parsed=CliParser.Parse(["--service-profile","service.json",family,command,"alpha","2026-11-05T11:00:00Z","2026-11-05T12:00:00Z","orders","--cursor","signed+/=","--limit","200"]);
        Assert.Equal($"/api/v1/governance/{family}?clusterId=alpha&pageSize=200&cursor=signed%2B%2F%3D&fromUtc=2026-11-05T11%3A00%3A00Z&toUtc=2026-11-05T12%3A00%3A00Z&resourceId=orders",parsed.Family!.Route);
        Assert.True(parsed.Family.Read);Assert.Equal("get",parsed.Family.Method);
        Assert.Throws<CliUsageException>(()=>CliWorkflowCommands.TryParse([family,command,"../hidden","2026-11-05T11:00:00Z","2026-11-05T12:00:00Z"]));
        Assert.Throws<CliUsageException>(()=>CliWorkflowCommands.TryParse([family,command,"alpha","2026-11-05T11:00:00Z","2027-11-05T12:00:00Z"]));
    }
    [Fact]
    public void Request_and_rate_card_routes_are_compiled_and_recovery_is_bodyless()
    {
        Assert.Equal("/api/v1/governance/requests/"+Request+"?clusterId=alpha",CliWorkflowCommands.TryParse(["requests","get","alpha",Request])!.Route);
        Assert.Equal("/api/v1/governance/requests/plans/"+Id,CliWorkflowCommands.TryParse(["requests","plan",Id])!.Route);
        foreach(var family in new[]{"requests","rate-cards"})
        {
            var recovery=CliWorkflowCommands.TryParse([family,"recover",Id])!;Assert.Equal(CliFamilyPayload.Empty,recovery.Payload);Assert.Equal("post",recovery.Method);Assert.False(recovery.Preview);
            Assert.Throws<CliUsageException>(()=>CliWorkflowCommands.TryParse([family,"recover",Id],"cursor"));
        }
    }
    [Fact]
    public void Closed_business_decision_selects_only_the_compiled_decision_route_and_requires_human_identity()
    {
        using var approved=JsonDocument.Parse("{\"clusterId\":\"alpha\",\"requestId\":\""+Request+"\",\"expectedVersion\":2,\"command\":\"Approve\",\"reason\":\"Exact scoped review\"}");
        var command=CliWorkflowCommands.TryParse(["requests","change-preview","alpha"])!;
        Assert.True(CliWorkflowCommands.Validate(approved.RootElement,command));Assert.True(CliWorkflowCommands.RequiresHumanDecision(approved.RootElement,command.Payload));
        Assert.Equal("/api/v1/governance/requests/decisions/preview",CliWorkflowCommands.BindCompiledDecisionRoute(command,approved.RootElement).Route);
        using var injected=JsonDocument.Parse("{\"clusterId\":\"alpha\",\"requestId\":\""+Request+"\",\"expectedVersion\":2,\"command\":\"Approve\",\"reason\":\"Reviewed\",\"requesterPrincipalId\":\"administrator\"}");
        Assert.False(CliWorkflowCommands.Validate(injected.RootElement,command));
    }
    [Theory]
    [InlineData("{\"clusterId\":\"alpha\",\"expectedVersion\":0,\"command\":\"Draft\",\"operationId\":\"11111111-1111-4111-8111-111111111111\",\"reason\":\"Draft\",\"expiresAtUtc\":\"2026-11-05T12:00:00Z\"}",true)]
    [InlineData("{\"clusterId\":\"alpha\",\"clusterId\":\"hidden\",\"expectedVersion\":0,\"command\":\"Draft\",\"reason\":\"Draft\"}",false)]
    [InlineData("{\"clusterId\":\"alpha\",\"expectedVersion\":-1,\"command\":\"Draft\",\"reason\":\"Draft\"}",false)]
    public void Request_files_reject_duplicate_aliases_missing_material_and_invalid_CAS(string json,bool valid)
    {using var value=JsonDocument.Parse(json);Assert.Equal(valid,CliWorkflowCommands.Validate(value.RootElement,CliFamilyPayload.WorkflowRequestChange));}
    [Fact]
    public void Rate_documents_use_exact_decimal_unit_owner_weights_and_closed_schema()
    {
        var document=new{clusterId="alpha",document=new{metricKind="trafficBytes",unit="bytes",currency="USD",amount="0.0000000000000000000000000001",effectiveFromUtc="2026-11-05T11:00:00Z",effectiveToUtc="2026-11-05T12:00:00Z",weights=new[]{new{teamId=Request,applicationId=(string?)null,weight="0.5"}}}};
        using var valid=JsonDocument.Parse(JsonSerializer.Serialize(document));Assert.True(CliWorkflowCommands.Validate(valid.RootElement,CliFamilyPayload.WorkflowRateCardDocument));
        using var utcOffset=JsonDocument.Parse(valid.RootElement.GetRawText().Replace("00Z","00+00:00",StringComparison.Ordinal));
        Assert.True(CliWorkflowCommands.Validate(utcOffset.RootElement,CliFamilyPayload.WorkflowRateCardDocument));
        using var loose=JsonDocument.Parse(valid.RootElement.GetRawText().Replace("2026-11-05T11:00:00Z","November 5 2026 11:00:00Z",StringComparison.Ordinal));
        Assert.False(CliWorkflowCommands.Validate(loose.RootElement,CliFamilyPayload.WorkflowRateCardDocument));
        foreach(var invalid in new[]{valid.RootElement.GetRawText().Replace("\"bytes\"","\"records\"",StringComparison.Ordinal),valid.RootElement.GetRawText().Replace("\"0.5\"","\"1.1\"",StringComparison.Ordinal),valid.RootElement.GetRawText().Replace("\"USD\"","\"usd\"",StringComparison.Ordinal)})
        {using var value=JsonDocument.Parse(invalid);Assert.False(CliWorkflowCommands.Validate(value.RootElement,CliFamilyPayload.WorkflowRateCardDocument));}
    }
    [Fact]
    public void Typed_rate_document_roundtrip_uses_the_actual_camel_case_wire_contract_without_case_aliases()
    {
        var rate=new Kafdeck.Core.Attribution.RateCardDocument("trafficBytes","bytes","USD","0.00001",
            new DateTimeOffset(2026,11,5,11,0,0,TimeSpan.Zero),new DateTimeOffset(2026,11,5,12,0,0,TimeSpan.Zero),[]);
        var body=new{clusterId="alpha",document=rate};
        using var wire=JsonDocument.Parse(JsonSerializer.Serialize(body,Kafdeck.Core.Attribution.AttributionContract.Json));
        Assert.True(CliWorkflowCommands.Validate(wire.RootElement,CliFamilyPayload.WorkflowRateCardDocument));
        using var nonWire=JsonDocument.Parse(JsonSerializer.Serialize(body));
        Assert.False(CliWorkflowCommands.Validate(nonWire.RootElement,CliFamilyPayload.WorkflowRateCardDocument));
    }
}
