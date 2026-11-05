using System.Text.Json;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09GovernanceCliTests
{
    private const string Id="11111111-1111-4111-8111-111111111111";
    private const string RecordId="11111111111141118111111111111111";
    [Theory]
    [InlineData("teams")]
    [InlineData("applications")]
    [InlineData("topics")]
    public void Catalog_lists_are_compiled_scoped_and_paginated(string family)
    {
        var parsed=CliParser.Parse(["--oidc-profile","human.json","catalog",family,"alpha","--limit","200","--cursor","opaque+/="]);
        Assert.Equal($"/api/v1/governance/catalog/{family}?clusterId=alpha&pageSize=200&cursor=opaque%2B%2F%3D",CliRouteBuilder.Build(parsed));
        Assert.True(parsed.Family!.Read);Assert.Equal("get",parsed.Family.Method);
    }
    [Fact]
    public void Catalog_exact_get_history_and_plan_have_no_arbitrary_route_selector()
    {
        var get=CliGovernanceCommands.TryParse(["catalog","get","alpha","teams",RecordId]);
        Assert.Equal($"/api/v1/governance/catalog/teams/{RecordId}?clusterId=alpha",get!.Route);
        var history=CliGovernanceCommands.TryParse(["catalog","history","alpha","topics",RecordId],"cursor",10);
        Assert.Equal($"/api/v1/governance/catalog/topics/{RecordId}/history?clusterId=alpha&pageSize=10&cursor=cursor",history!.Route);
        Assert.Throws<CliUsageException>(()=>CliGovernanceCommands.TryParse(["catalog","get","alpha","../policies",Id]));
        Assert.Throws<CliUsageException>(()=>CliGovernanceCommands.TryParse(["catalog","plan","../execute"]));
        Assert.Throws<CliUsageException>(()=>CliGovernanceCommands.TryParse(["catalog","get","alpha","teams",Id],"cursor"));
    }
    [Fact]
    public void Governance_requires_registered_identity_and_previews_require_stable_key_and_file()
    {
        string[] args=["--service-profile","service.json","catalog","change-preview","alpha","teams","--request-file","request.json","--idempotency-key","key"];
        var parsed=CliParser.Parse(args);
        Assert.Equal(CliFamilyPayload.GovernanceCatalogChange,parsed.Family!.Payload);Assert.Equal("alpha",parsed.Family.GovernanceScope);
        Assert.Throws<CliUsageException>(()=>CliParser.Parse(args.Skip(2).ToArray()));
        Assert.Throws<CliUsageException>(()=>CliParser.Parse(args.Take(args.Length-2).ToArray()));
        Assert.Throws<CliUsageException>(()=>CliParser.Parse(["--service-profile","service.json","catalog","teams","alpha","--search","x"]));
    }
    [Theory]
    [InlineData("active","/active?clusterId=alpha")]
    [InlineData("state","/active?clusterId=alpha")]
    [InlineData("versions","/versions?clusterId=alpha&pageSize=50")]
    public void Policy_read_aliases_preserve_typed_route_and_do_not_execute(string command,string suffix)
    {
        var parsed=CliParser.Parse(["--service-profile","service.json","policies",command,"alpha"]);
        Assert.Equal("/api/v1/governance/policies"+suffix,parsed.Family!.Route);Assert.True(parsed.Family.Read);Assert.False(parsed.Family.Preview);
    }
    [Theory]
    [InlineData("validate",CliFamilyPayload.GovernancePolicyDocument)]
    [InlineData("explain",CliFamilyPayload.GovernancePolicyFact)]
    [InlineData("dry-run",CliFamilyPayload.GovernancePolicyDryRun)]
    public void Policy_observations_use_closed_post_bodies_without_mutation_idempotency(string command,CliFamilyPayload payload)
    {
        var parsed=CliParser.Parse(["--service-profile","service.json","policies",command,"--request-file","fixture.json"]);
        Assert.Equal(payload,parsed.Family!.Payload);Assert.True(parsed.Family.Read);Assert.False(parsed.Family.Preview);
    }
    [Theory]
    [InlineData("{\"clusterId\":\"alpha\",\"changes\":[{\"kind\":\"Team\",\"id\":null,\"expectedRevision\":0,\"name\":\"ops\"}]}",true)]
    [InlineData("{\"clusterId\":\"alpha\",\"clusterId\":\"other\",\"changes\":[]}",false)]
    [InlineData("{\"clusterId\":\"alpha\",\"changes\":[{\"kind\":\"Team\",\"expectedRevision\":0,\"name\":\"ops\",\"actor\":\"admin\"}]}",false)]
    [InlineData("{\"clusterId\":\"alpha\",\"changes\":[{\"kind\":\"Team\",\"expectedRevision\":0,\"name\":\"a\",\"name\":\"b\"}]}",false)]
    [InlineData("{\"clusterId\":\"alpha\",\"changes\":[{\"kind\":\"Team\",\"expectedRevision\":-1,\"name\":\"ops\"}]}",false)]
    [InlineData("{\"clusterId\":\"other\",\"changes\":[{\"kind\":\"Team\",\"expectedRevision\":0,\"name\":\"ops\"}]}",false)]
    public void Catalog_schema_rejects_duplicate_unknown_invalid_and_cross_scope_before_signin(string json,bool expected)
    {
        var invocation=CliGovernanceCommands.TryParse(["catalog","change-preview","alpha","teams"])!;
        using var value=JsonDocument.Parse(json);Assert.Equal(expected,CliGovernanceCommands.Validate(value.RootElement,invocation));
    }
    [Fact]
    public void Policy_closed_schema_cannot_supply_approval_or_metadata_authority()
    {
        var invocation=CliGovernanceCommands.TryParse(["policies","explain","alpha"])!;
        using var good=JsonDocument.Parse("{\"clusterId\":\"alpha\",\"resourceKind\":\"topic\",\"resourceId\":\"orders\",\"action\":\"TopicAlter\",\"risk\":\"High\"}");
        Assert.True(CliGovernanceCommands.Validate(good.RootElement,invocation));
        using var injected=JsonDocument.Parse("{\"clusterId\":\"alpha\",\"resourceKind\":\"topic\",\"resourceId\":\"orders\",\"action\":\"TopicAlter\",\"risk\":\"High\",\"requestApproved\":true}");
        Assert.False(CliGovernanceCommands.Validate(injected.RootElement,invocation));
        using var duplicate=JsonDocument.Parse("{\"document\":\"{}\",\"document\":\"{}\"}");
        Assert.False(CliGovernanceCommands.Validate(duplicate.RootElement,CliFamilyPayload.GovernancePolicyDocument));
    }
    [Theory]
    [InlineData("{\"clusterId\":\"alpha\",\"document\":\"{}\"}",true)]
    [InlineData("{\"document\":\"{}\"}",false)]
    [InlineData("{\"clusterId\":\"other\",\"document\":\"{}\"}",false)]
    [InlineData("{\"clusterId\":\"alpha\",\"document\":\"{}\",\"idempotencyKey\":\"not-a-mutation\"}",false)]
    public void Validation_requires_explicit_matching_scope_without_global_read_or_mutation_fields(string json,bool expected)
    {
        var invocation=CliGovernanceCommands.TryParse(["policies","validate","alpha"])!;
        using var document=JsonDocument.Parse(json);
        Assert.Equal(expected,CliGovernanceCommands.Validate(document.RootElement,invocation));
    }
    [Fact]
    public void Catalog_character_bounds_preserve_valid_Persian_labels_while_body_bytes_stay_bounded()
    {
        using var accepted=JsonDocument.Parse(JsonSerializer.Serialize(new{clusterId="alpha",changes=new[]{new{kind="Team",expectedRevision=0,name=new string('ک',128),description=new string('ک',2048),tags=new[]{new string('ک',64)}}}}));
        Assert.True(CliGovernanceCommands.Validate(accepted.RootElement,CliFamilyPayload.GovernanceCatalogChange));
        using var rejected=JsonDocument.Parse(JsonSerializer.Serialize(new{clusterId="alpha",changes=new[]{new{kind="Topic",expectedRevision=0,name="orders",owner=new string('ک',129)}}}));
        Assert.False(CliGovernanceCommands.Validate(rejected.RootElement,CliFamilyPayload.GovernanceCatalogChange));
    }
    [Fact]
    public async Task Only_bounded_imports_admit_large_files_and_buffers_are_not_persisted_elsewhere()
    {
        var file=Path.GetTempFileName();
        try
        {
            var changes=Enumerable.Range(0,400).Select(i=>new{kind="Team",expectedRevision=0,name="team"+i,description=new string('x',1800)});
            await File.WriteAllTextAsync(file,JsonSerializer.Serialize(new{clusterId="alpha",changes}));
            Assert.InRange(new FileInfo(file).Length,256*1024+1,1024*1024);
            var import=CliGovernanceCommands.TryParse(["catalog","import-preview","alpha"])! with{RequestFile=file};
            using var payload=await CliFamilyCommands.ReadPayloadAsync(import,CancellationToken.None);
            Assert.Equal(400,payload!.RootElement.GetProperty("changes").GetArrayLength());
            var single=CliGovernanceCommands.TryParse(["catalog","change-preview","alpha","teams"])! with{RequestFile=file};
            await Assert.ThrowsAsync<CliUsageException>(()=>CliFamilyCommands.ReadPayloadAsync(single,CancellationToken.None));
            Assert.Equal(65536,CliGovernanceCommands.RequestByteLimit(new("route",true,CliFamilyPayload.RecordsPreview)));
        }
        finally{File.Delete(file);}
    }
    [Theory]
    [InlineData("awaitingApproval",CliApplication.PendingApproval)]
    [InlineData("executionUnknown",CliApplication.UnknownEffect)]
    [InlineData("stalePreview",CliApplication.Conflict)]
    [InlineData("appliedUnverified",CliApplication.PartialOrUnverified)]
    public void Frozen_catalog_envelopes_preserve_existing_mutation_exit_semantics(string state,int expected)
    {
        var invocation=CliGovernanceCommands.TryParse(["catalog","plan",Id])!;
        Assert.Equal(expected,CliGovernanceCommands.Outcome(JsonSerializer.Serialize(new{operation=new{state}}),invocation));
    }
    [Theory]
    [InlineData("failedBeforeDispatch","GovernanceDenied",CliApplication.AuthenticationOrAuthorizationError)]
    [InlineData("failedBeforeDispatch","RequestApprovalRequired",CliApplication.AuthenticationOrAuthorizationError)]
    [InlineData("failedBeforeDispatch","GovernanceUnavailable",CliApplication.RemoteError)]
    [InlineData("failedBeforeDispatch","EvidenceUnavailable",CliApplication.RemoteError)]
    [InlineData("executionUnknown","GovernanceDenied",CliApplication.UnknownEffect)]
    [InlineData("failedDefinitive","GovernanceDenied",CliApplication.RemoteError)]
    public void Only_explicit_pre_dispatch_policy_denials_use_authorization_exit(string state,string resultCode,int expected)
    {
        var response=JsonSerializer.Serialize(new{state,resultCode});
        Assert.Equal(expected,CliApplication.MutationOutcomeCategory(response));
        Assert.Equal(expected,CliGovernanceCommands.Outcome(JsonSerializer.Serialize(new{operation=new{state,resultCode}}),CliGovernanceCommands.TryParse(["catalog","plan",Id])!));
    }
}
