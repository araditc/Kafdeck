using System.Text.Json;
using Kafdeck.Cli;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;
using Fixture=Kafdeck.Architecture.Tests.V08G3DeclarativeCliLiveTests.Fixture;
using Artifact=Kafdeck.Architecture.Tests.V08G3DeclarativeCliLiveTests.Artifact;
using Preview=Kafdeck.Architecture.Tests.V08G3DeclarativeCliLiveTests.Preview;

namespace Kafdeck.Architecture.Tests;

public sealed partial class V08G4ExternalIntegrationLiveTests
{
    [G3DeclarativeLiveTheory][InlineData(false)][InlineData(true)]
    public async Task Actual_Terraform_and_MCP_require_current_distinct_human_business_request_in_addition_to_critical_approval(bool postgres)
    {
        RequirePostgres(postgres);using var deadline=new CancellationTokenSource(TimeSpan.FromMinutes(18));var token=deadline.Token;
        var evidence=Evidence();await using var f=await Fixture.CreateAsync(evidence,postgres,token);f.EnableService();
        var resources=new List<Artifact>();foreach(var name in new[]{"w74-tf-no-request","w74-mcp-submitted","w74-tf-approved","w74-mcp-approved","w74-mcp-cancelled","w74-mcp-expired","w74-mcp-unknown"})resources.Add(await f.CommitAsync(name,"Active",token));
        ConfigureRequestClients(f);
        var settings=f.Lab.Settings;settings["Kafdeck:Governance:RequestsEnabled"]="true";settings["Kafdeck:Governance:DeploymentGeneration"]=f.Generation.ToString("D");
        settings["Logging:LogLevel:Kafdeck.Infrastructure.Security.LoggingMutationAuditSink"]="Information";
        var connections=postgres?(IMutationDbConnectionFactory)new PostgreSqlMutationDbConnectionFactory(await File.ReadAllTextAsync(settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"][5..],token)):
            new SqliteMutationDbConnectionFactory(settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"]);
        var generation=new AdoGovernanceGenerationGate(connections,f.Generation);await generation.InitializeAsync(token);
        var policy=new AdoGovernancePolicyStore(connections,generationGate:generation);await policy.InitializeAsync(token);
        var compiled=GovernancePolicyCompiler.Compile(JsonSerializer.Serialize(new{schemaVersion=1,rules=new[]{new{id="require-current-business-request",priority=1,
            scope=new{clusterId="$deployment",resourceKind="notification",resourceIds=resources.Select(a=>a.Resource).ToArray(),action="NotificationManage"},conditions=Array.Empty<object>(),outcome="RequireApprovedRequest"}}}));
        var before=await policy.GetStateAsync("$deployment",token);var binding=before.Digest is null?Array.Empty<MutationPrecondition>():new[]{new MutationPrecondition("v09.governance:$deployment",before.Digest)};
        var version=await policy.StageAsync("$deployment",compiled,"trusted-policy-port-fixture",token);
        await policy.CommitVersionAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"$deployment",version.VersionId,binding),version.VersionId,version.ContentHash,()=>true,token);
        await policy.ActivateAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"$deployment","active",binding),version.VersionId,version.ContentHash,before.Revision,true,()=>true,token);
        await f.Lab.StartAsync(token);
        // Install the real provider before any 110-second reviewed W68 plan exists.
        // Human business/critical approvals must not spend that lease on tool installation.
        var tfMissingClient=await PrepareW74Terraform(f,evidence,token);var tfApprovedClient=await PrepareW74Terraform(f,evidence,token);
        async Task<JsonElement> Success(params string[] args)
        {var exit=await f.Lab.RunAsync(token,args);var phase=string.Join('.',args.Take(2).Select(value=>value.Length<=40&&value.All(c=>char.IsAsciiLetterLower(c)||c=='-')?value:"argument"));
            Assert.True(exit==CliApplication.Success,"W74 real automation request command failed; phase="+phase+"; exit="+exit+"; "+f.Lab.SafeOutcome);using var document=JsonDocument.Parse(f.Lab.Output);return document.RootElement.Clone();}
        async Task<JsonElement> Change(string command,string? id,long expected,Preview target,DateTimeOffset? expires=null)
        {
            if(command=="Draft"&&expires is null)expires=(await f.Operations.GetAsync(target.Id,token))!.PreviewExpiresAtUtc;
            var expiresAtUtc=expires?.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ",System.Globalization.CultureInfo.InvariantCulture);
            var path=await f.Lab.RequestFileAsync(new{clusterId="g3-deployment",requestId=id,expectedVersion=expected,command,
                operationId=command=="Draft"?target.Id:(Guid?)null,reason="bounded external-client request",expiresAtUtc},token);
            return await Success("requests","change-preview","g3-deployment","--request-file",path,"--idempotency-key",Guid.NewGuid().ToString("N"));
        }
        async Task Commit(JsonElement operation)
        {var id=operation.GetProperty("operationId").GetGuid().ToString("D");await Success("operations","confirm",id,operation.GetProperty("previewHash").GetString()!);await Success("operations","execute",id);}
        async Task<string> Business(Preview target,bool approve,DateTimeOffset? expires=null)
        {
            var draft=await Change("Draft",null,0,target,expires);await Commit(draft);
            var material=await Success("requests","plan",draft.GetProperty("operationId").GetGuid().ToString("D"));var id=material.GetProperty("plan").GetProperty("requestId").GetString()!;
            await Commit(await Change("Submit",id,1,target));
            if(approve){f.Lab.Approver=true;try{await Commit(await Change("Approve",id,2,target));}finally{f.Lab.Approver=false;}}
            return id;
        }
        async Task<Preview> Critical(Artifact resource)
        {var preview=await f.PlanAsync(resource,0,token);await f.ApproveAsync(preview,token);return preview;}
        async Task AssertDenied(Artifact resource,Preview preview,string code)
        {
            var saved=await f.Operations.GetAsync(preview.Id,token);Assert.NotNull(saved);Assert.Equal(MutationOperationState.FailedBeforeDispatch,saved.State);Assert.Equal(code,saved.ResultCode);
            Assert.Null(await f.Routing.GetSubscriptionAsync(resource.Resource,token));Assert.Null(await f.Store.GetCommitReceiptAsync(preview.Id,token));
        }
        var tfMissing=await Critical(resources[0]);Assert.NotEqual(0,await tfMissingClient.Apply(f.Lab.Api,resources[0],tfMissing,token));await AssertDenied(resources[0],tfMissing,"RequestApprovalRequired");
        var mcpSubmitted=await Critical(resources[1]);var submittedRequest=await Business(mcpSubmitted,false);
        var submittedBefore=await Success("requests","get","g3-deployment",submittedRequest);
        Assert.Equal(2,submittedBefore.GetProperty("version").GetInt64());Assert.Equal("Submitted",submittedBefore.GetProperty("businessState").GetString());
        var serviceDecisionLogins=f.Lab.CompletedLogins;
        // Bypass CLI's human-only prevalidation deliberately: a genuine service token
        // even has the RequestDecide role and mutation registration in this fixture.
        // The actual HTTP handler must still reject machine business decisions.
        var serviceInvocation=CliParser.Parse(["--url",f.Lab.Api.AbsoluteUri,"--service-profile",f.Lab.ServiceProfileFile,"system","info"]);
        using(var service=await KafdeckCliClient.CreateAsync(serviceInvocation,token))
        using(var deniedDecision=await service.PostAsync("/api/v1/governance/requests/decisions/preview",new{clusterId="g3-deployment",requestId=submittedRequest,
            expectedVersion=2,command="Approve",reason="genuine service cannot supply a human decision"},Guid.NewGuid().ToString("N"),token))
        {
            Assert.Equal(System.Net.HttpStatusCode.Forbidden,deniedDecision.StatusCode);
            using var body=JsonDocument.Parse(await CliResponseBodyReader.ReadAsync(deniedDecision.Content,token));Assert.Equal("ScopeDenied",body.RootElement.GetProperty("code").GetString());
        }
        Assert.Equal(serviceDecisionLogins,f.Lab.CompletedLogins);
        var submittedAfter=await Success("requests","get","g3-deployment",submittedRequest);
        Assert.Equal(2,submittedAfter.GetProperty("version").GetInt64());Assert.Equal("Submitted",submittedAfter.GetProperty("businessState").GetString());
        await using(var mcp=await McpProcess.StartAsync(f,true,token))_=await mcp.ToolAsync("declarative_apply",new{planId=mcpSubmitted.Id,planFingerprint=mcpSubmitted.Hash},token);
        await AssertDenied(resources[1],mcpSubmitted,"RequestApprovalRequired");
        var tfApproved=await Critical(resources[2]);await Business(tfApproved,true);
        var reviewedTfPlan=await f.Store.GetPlanAsync(tfApproved.Id,token);Assert.NotNull(reviewedTfPlan);
        Assert.True(reviewedTfPlan.ExpiresAtUtc>await f.Store.ReadDatabaseUtcNowAsync(token),"Positive Terraform fixture exhausted its reviewed W68 plan before apply.");
        var beforeTf=f.Lab.CompletedLogins;var tfExit=await tfApprovedClient.Apply(f.Lab.Api,resources[2],tfApproved,token);
        var tfSaved=await f.Operations.GetAsync(tfApproved.Id,token);Assert.NotNull(tfSaved);
        Assert.True(tfExit==0,"W74 approved Terraform failed; exit="+tfExit+"; state="+tfSaved.State+"; code="+SafeW74Code(tfSaved.ResultCode)+"; "+tfApprovedClient.SafeDiagnostic);
        Assert.Equal(beforeTf,f.Lab.CompletedLogins);
        Assert.Equal(1,(await f.Routing.GetSubscriptionAsync(resources[2].Resource,token))!.Revision);
        var mcpApproved=await Critical(resources[3]);await Business(mcpApproved,true);
        var beforeMcp=f.Lab.CompletedLogins;
        await using(var mcp=await McpProcess.StartAsync(f,true,token))Assert.False(IsError(await mcp.ToolAsync("declarative_apply",new{planId=mcpApproved.Id,planFingerprint=mcpApproved.Hash},token)));
        Assert.Equal(beforeMcp,f.Lab.CompletedLogins);Assert.Equal(1,(await f.Routing.GetSubscriptionAsync(resources[3].Resource,token))!.Revision);
        var cancel=await Critical(resources[4]);var cancelId=await Business(cancel,true);await Commit(await Change("Cancel",cancelId,3,cancel));
        await using(var mcp=await McpProcess.StartAsync(f,true,token))_=await mcp.ToolAsync("declarative_apply",new{planId=cancel.Id,planFingerprint=cancel.Hash},token);
        await AssertDenied(resources[4],cancel,"RequestApprovalRequired");
        var expired=await Critical(resources[5]);var expires=DateTimeOffset.UtcNow.AddSeconds(90);var expiredRequest=await Business(expired,true,expires);
        var beforeExpiry=await Success("requests","get","g3-deployment",expiredRequest);
        Assert.Equal("Approved",beforeExpiry.GetProperty("businessState").GetString());
        Assert.True(expires>DateTimeOffset.UtcNow,"Automation request fixture exhausted its finite setup budget before the intended expiry phase.");
        var delay=expires-DateTimeOffset.UtcNow+TimeSpan.FromMilliseconds(150);if(delay>TimeSpan.Zero)await Task.Delay(delay,token);
        var expiredData=await Success("requests","get","g3-deployment",expiredRequest);Assert.Equal("Expired",expiredData.GetProperty("businessState").GetString());
        await using(var mcp=await McpProcess.StartAsync(f,true,token))_=await mcp.ToolAsync("declarative_apply",new{planId=expired.Id,planFingerprint=expired.Hash},token);
        var expiredOperation=await f.Operations.GetAsync(expired.Id,token);Assert.NotNull(expiredOperation);
        if(expiredOperation.State==MutationOperationState.StalePreview)
        {
            var expiredPlan=await f.Store.GetPlanAsync(expired.Id,token);Assert.NotNull(expiredPlan);
            Assert.True(expiredPlan.ExpiresAtUtc<=await f.Store.ReadDatabaseUtcNowAsync(token));
            await f.Lab.StopAsync(token);
            var privateAudit=await File.ReadAllTextAsync(Path.Combine(Path.GetDirectoryName(f.Lab.ConfigurationFile)!,"api-private-console.log"),token);
            Assert.True(privateAudit.Split('\n').Any(line=>line.Contains("OperationId="+expired.Id.ToString("D"),StringComparison.Ordinal)&&
                line.Contains("State=StalePreview",StringComparison.Ordinal)&&line.Contains("OutcomeCode=declarative_plan_or_authority_stale",StringComparison.Ordinal)),
                "Expired reviewed plan did not record the exact closed declarative stale audit outcome.");
            await f.Lab.StartAsync(token);
            Assert.Null(await f.Routing.GetSubscriptionAsync(resources[5].Resource,token));Assert.Null(await f.Store.GetCommitReceiptAsync(expired.Id,token));
        }
        else await AssertDenied(resources[5],expired,"RequestApprovalRequired");
        var unknown=await Critical(resources[6]);var unknownId=await Business(unknown,true);await InjectDurableUnknownAsync(f,unknown,token);
        var cancelUnknown=await Change("Cancel",unknownId,3,unknown);var cid=cancelUnknown.GetProperty("operationId").GetGuid().ToString("D");
        await Success("operations","confirm",cid,cancelUnknown.GetProperty("previewHash").GetString()!);Assert.NotEqual(CliApplication.Success,await f.Lab.RunAsync(token,"operations","execute",cid));
        await using(var mcp=await McpProcess.StartAsync(f,true,token))Assert.True(IsError(await mcp.ToolAsync("declarative_apply",new{planId=unknown.Id,planFingerprint=unknown.Hash},token)));
        Assert.Equal(MutationOperationState.ExecutionUnknown,(await f.Operations.GetAsync(unknown.Id,token))!.State);Assert.Null(await f.Routing.GetSubscriptionAsync(resources[6].Resource,token));
        await File.WriteAllBytesAsync(Path.Combine(evidence,(postgres?"postgres":"sqlite")+"-w74-automation-request-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(new{
            provider=postgres?"PostgreSQL":"SQLite",status="Passed",genuineKeycloakPrivateKeyJwt=true,actualTerraformPlugin=true,actualInstalledMcpStdio=true,
            realDistinctHumanBusinessApproval=true,independentCriticalKernelApprovalAlsoRequired=true,missingRequestDenied=true,submittedRequestDenied=true,
            approvedTerraformEffect=true,approvedMcpEffect=true,cancelledRequestDenied=true,expiredRequestDenied=true,noDeniedProductWrites=true,
            explicitDurableUnknownFaultFixture=true,lateCancelCannotClearUnknown=true,unknownNeverRedispatched=true,trustedPolicyPortSeeding=true,
            serviceEffectsNoBrowserCallbacks=true,serviceDecisionHttpDenied=true,serviceDecisionRoleAndRegistrationGranted=true,
            serviceDecisionRequestVersionUnchanged=true,businessExpiryIndependentlyVerified=true,
            expiryClosedByReviewedPlanDeadline=expiredOperation.State==MutationOperationState.StalePreview,
            ownershipAuditCount=await f.CountAsync("kafdeck_declarative_ownership_audit",token)}),token);
        Assert.Equal(2,await f.CountAsync("kafdeck_declarative_commit_receipts",token));Assert.Equal(2,await f.CountAsync("kafdeck_declarative_ownership_audit",token));
    }
    private static void ConfigureRequestClients(Fixture f)
    {
        for(var person=0;person<2;person++)
        {
            var registration=$"Kafdeck:NonBrowserRead:HumanRegistrations:{person}:";f.Lab.Settings[registration+"ReadActions:80"]="RequestRead";
            f.Lab.Settings[registration+"MutationActions:80"]="RequestSubmit";f.Lab.Settings[registration+"MutationActions:81"]="RequestDecide";
            foreach(var (action,index)in new[]{"RequestRead","RequestSubmit","RequestDecide"}.Select((value,index)=>(value,index)))f.Lab.Settings[$"Kafdeck:Authorization:Roles:{person}:Permissions:{80+index}:Action"]=action;
        }
        f.Lab.Settings["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:80"]="RequestRead";f.Lab.Settings["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:80"]="RequestSubmit";
        f.Lab.Settings["Kafdeck:NonBrowserRead:Registrations:0:MutationActions:81"]="RequestDecide";
        f.Lab.Settings["Kafdeck:Authorization:Roles:2:Permissions:80:Action"]="RequestRead";f.Lab.Settings["Kafdeck:Authorization:Roles:2:Permissions:81:Action"]="RequestSubmit";
        f.Lab.Settings["Kafdeck:Authorization:Roles:2:Permissions:82:Action"]="RequestDecide";
    }
    private sealed class W74PreparedTerraform(string directory,TerraformProcess runner)
    {
        internal string SafeDiagnostic {get;private set;}="http=none; lifecycle=none";
        internal async Task<int> Apply(Uri endpoint,Artifact artifact,Preview preview,CancellationToken token)
        {
            await WriteConfiguration(directory,endpoint,new Dictionary<string,Dictionary<string,object>>{{"managed",Resource(artifact,preview)}},token);
            var exit=await runner.RunAsync(token,"apply","-input=false","-auto-approve","-no-color");
            var log=Directory.EnumerateFiles(directory,"private-apply-*.log").Single();var text=await File.ReadAllTextAsync(log,token);
            var http=System.Text.RegularExpressions.Regex.Match(text,@"HTTP ([1-5][0-9]{2})",System.Text.RegularExpressions.RegexOptions.CultureInvariant,TimeSpan.FromSeconds(1));
            var known=new[]{"expired","RequestApprovalRequired","GovernancePolicyChanged","GovernanceUnavailable","ExecutionUnknown","StalePreview"}.Where(code=>text.Contains(code,StringComparison.OrdinalIgnoreCase));
            SafeDiagnostic="http="+(http.Success?http.Groups[1].Value:"none")+"; lifecycle="+string.Join(',',known);return exit;
        }
    }
    private static string SafeW74Code(string? value)=>value is{Length:>0 and <=96}&&value.All(c=>char.IsAsciiLetterOrDigit(c)||c is '_' or '-')?value:"none";
    private static async Task<W74PreparedTerraform> PrepareW74Terraform(Fixture f,string evidence,CancellationToken token)
    {
        var root=Path.Combine(evidence,"w74-terraform-private-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);
        var provider=RequiredFile("KAFDECK_G4_PROVIDER_BIN");var platform=OperatingSystem.IsWindows()?"windows_amd64":OperatingSystem.IsMacOS()?"darwin_arm64":"linux_amd64";
        var mirror=Path.Combine(root,"mirror","registry.terraform.io","araditc","kafdeck","0.8.0",platform);Directory.CreateDirectory(mirror);File.Copy(provider,Path.Combine(mirror,Path.GetFileName(provider)));
        var configuration=Path.Combine(root,"terraform.rc");await File.WriteAllTextAsync(configuration,"provider_installation { filesystem_mirror { path = "+JsonSerializer.Serialize(Path.Combine(root,"mirror").Replace('\\','/'))+" include = [\"registry.terraform.io/araditc/kafdeck\"] } }",token);
        await File.WriteAllBytesAsync(Path.Combine(root,"main.tf.json"),JsonSerializer.SerializeToUtf8Bytes(new{
            terraform=new{required_providers=new{kafdeck=new{source="araditc/kafdeck",version="0.8.0"}}},provider=new{kafdeck=new{endpoint=f.Lab.Api.AbsoluteUri.TrimEnd('/')}}}),token);
        var runner=new TerraformProcess(RequiredFile("KAFDECK_G4_TERRAFORM_BIN"),root,configuration,f.Lab.ServiceProfileFile);Assert.Equal(0,await runner.RunAsync(token,"init","-input=false","-no-color"));
        return new(root,runner);
    }
}
