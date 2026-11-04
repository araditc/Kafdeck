using Kafdeck.Cli;
using Kafdeck.Mcp;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G5ClientBoundaryTests
{
    [Fact]
    public void Compensation_clients_admit_only_exact_named_routes_and_immutable_parent()
    {
        var id=Guid.NewGuid().ToString("D");
        var prefix=new[]{"--service-profile","registered.json"};
        Assert.Equal("/api/v1/declarative/compensation/catalog",CliRouteBuilder.Build(CliParser.Parse([..prefix,"declarative","compensation-catalog"])));
        Assert.Equal("/api/v1/declarative/plans/"+id+"/compensation",CliRouteBuilder.Build(CliParser.Parse([..prefix,"declarative","compensation",id])));
        var preview=CliParser.Parse([..prefix,"declarative","compensation-preview",id,"--manifest-file","inverse.json","--source","reviewed-source",
            "--source-revision",new string('a',64),"--ownership-revision","2","--idempotency-key","fresh-inverse"]);
        Assert.Equal("/api/v1/declarative/plans/"+id+"/compensation-preview",CliRouteBuilder.Build(preview));
        Assert.Throws<CliUsageException>(()=>CliParser.Parse([..prefix,"declarative","compensation","../other"]));
        Assert.Throws<CliUsageException>(()=>CliParser.Parse([..prefix,"declarative","compensation-catalog","--idempotency-key","mutate"]));
        Assert.False(Assert.Single(McpTools.All,x=>x.Name=="compensation_catalog").Effect);
        Assert.False(Assert.Single(McpTools.All,x=>x.Name=="compensation_inspect").Effect);
        Assert.True(Assert.Single(McpTools.All,x=>x.Name=="compensation_preview").Effect);
    }
}
