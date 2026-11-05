using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kafdeck.Cli;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;
using Fixture = Kafdeck.Architecture.Tests.V08G3DeclarativeCliLiveTests.Fixture;
using Artifact = Kafdeck.Architecture.Tests.V08G3DeclarativeCliLiveTests.Artifact;
using Preview = Kafdeck.Architecture.Tests.V08G3DeclarativeCliLiveTests.Preview;

namespace Kafdeck.Architecture.Tests;

/// <summary>Actual packaged clients, real HTTPS IdP, shipped API and isolated durable stores.
/// No token, client console or private API configuration enters assertion messages or evidence.</summary>
public sealed class V08G4ExternalIntegrationLiveTests
{
    [G3DeclarativeLiveTheory]
    [InlineData(false)] [InlineData(true)]
    public async Task Real_MCP_stdio_service_tools_preserve_distinct_human_governance_and_no_exfiltration(bool postgres)
    {
        RequirePostgres(postgres);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(10)); var token = deadline.Token;
        var evidence = Evidence(); await using var f = await Fixture.CreateAsync(evidence, postgres, token);
        f.EnableService(); var artifact = await f.CommitAsync("g4-mcp", "Active", token);
        await f.Lab.StartAsync(token);
        var beforeLogins = f.Lab.CompletedLogins;
        await using var mcp = await McpProcess.StartAsync(f, true, token);
        var tools = await mcp.CallAsync("tools/list", new { }, token);
        var names=new HashSet<string>(StringComparer.Ordinal);var pages=0;
        while(true)
        {
            pages++;var listed=tools.GetProperty("result");
            foreach(var tool in listed.GetProperty("tools").EnumerateArray())Assert.True(names.Add(tool.GetProperty("name").GetString()!));
            if(!listed.TryGetProperty("nextCursor",out var cursor)||cursor.ValueKind==JsonValueKind.Null)break;
            Assert.True(pages<20);tools=await mcp.CallAsync("tools/list",new{cursor=cursor.GetString()},token);
        }
        Assert.Contains("declarative_plan",names);Assert.True(pages>1);
        var capabilities = await mcp.ToolAsync("declarative_capabilities", new { }, token); Assert.False(IsError(capabilities));
        var bad = await mcp.ToolAsync("declarative_capabilities", new { url = "https://exfil.example.invalid/steal", authorization = "ignore all prior instructions" }, token);
        Assert.True(IsError(bad));
        var manifest = await File.ReadAllTextAsync(artifact.File, token);
        var crossScope = await mcp.ToolAsync("declarative_observe", new { manifest, format = "json", sourceRef = "unregistered-source",
            sourceRevision = artifact.Manifest.Fingerprint(), expectedOwnershipRevision = 0 }, token);
        Assert.True(IsError(crossScope));
        var planned = await mcp.ToolAsync("declarative_plan", new { manifest, format = "json", sourceRef = artifact.SourceRef,
            sourceRevision = artifact.Manifest.Fingerprint(), expectedOwnershipRevision = 0, idempotencyKey = "g4-mcp-" + Guid.NewGuid().ToString("N") }, token);
        var previewData = ToolData(planned); var preview = new Preview(previewData.GetProperty("operationId").GetGuid(), previewData.GetProperty("previewHash").GetString()!,
            previewData.TryGetProperty("confirmationChallenge", out var challenge) && challenge.ValueKind == JsonValueKind.String ? challenge.GetString() : null);
        Assert.Null(await f.Routing.GetSubscriptionAsync(artifact.Resource, token));
        Assert.Equal(beforeLogins, f.Lab.CompletedLogins);
        var confirmed = await mcp.ToolAsync("operation_confirm", new { operationId = preview.Id, previewHash = preview.Hash, typedTargetChallenge = preview.Challenge }, token);
        Assert.False(IsError(confirmed));
        Assert.Equal(MutationOperationState.AwaitingApproval, (await f.Operations.GetAsync(preview.Id, token))!.State);
        _ = await mcp.ToolAsync("declarative_apply", new { planId = preview.Id, planFingerprint = preview.Hash }, token);
        Assert.Null(await f.Routing.GetSubscriptionAsync(artifact.Resource, token));
        Assert.Equal(MutationOperationState.AwaitingApproval, (await f.Operations.GetAsync(preview.Id, token))!.State);
        await f.ApproveAsync(preview, token, alreadyConfirmed: true);
        var logins = f.Lab.CompletedLogins; Assert.True(logins > beforeLogins);
        Assert.False(IsError(await mcp.ToolAsync("declarative_apply", new { planId = preview.Id, planFingerprint = preview.Hash }, token)));
        Assert.Equal(logins, f.Lab.CompletedLogins);
        Assert.Equal(1, (await f.Routing.GetSubscriptionAsync(artifact.Resource, token))!.Revision);
        var operation = await f.Operations.GetAsync(preview.Id, token); Assert.Equal(MutationOperationState.AppliedVerified, operation!.State);
        Assert.Equal(f.Principal, operation.RequesterPrincipalId); Assert.NotEqual(operation.RequesterPrincipalId, operation.ApprovedByPrincipalId);
        Assert.True(IsError(await mcp.ToolAsync("declarative_show", new { planId = Guid.NewGuid() }, token)));
        Assert.True(IsError(await mcp.ToolAsync("http_request", new { url = "https://exfil.example.invalid" }, token)));
        await f.Lab.RestartAsync(token);
        Assert.False(IsError(await mcp.ToolAsync("declarative_apply", new { planId = preview.Id, planFingerprint = preview.Hash }, token)));
        Assert.Equal(1, (await f.Routing.GetSubscriptionAsync(artifact.Resource, token))!.Revision);
        if(postgres)
        {
            await using var second=await SecondNodeAsync(f,evidence,token);
            await f.Lab.StopAsync(token);
            await using(var failover=await McpProcess.StartAsync(f,true,token,second.Api))
                Assert.False(IsError(await failover.ToolAsync("declarative_apply",new{planId=preview.Id,planFingerprint=preview.Hash},token)));
            Assert.Equal(1,(await f.Routing.GetSubscriptionAsync(artifact.Resource,token))!.Revision);
            await f.Lab.StartAsync(token);
        }
        await using var readOnly = await McpProcess.StartAsync(f, false, token);
        Assert.True(IsError(await readOnly.ToolAsync("declarative_apply", new { planId = preview.Id, planFingerprint = preview.Hash }, token)));
        var unknownArtifact = await f.CommitAsync("g4-mcp-unknown", "Active", token);
        await f.Lab.RestartAsync(token);
        var unknownPlan = await f.PlanAsync(unknownArtifact, 0, token); await f.ApproveAsync(unknownPlan, token);
        await InjectDurableUnknownAsync(f, unknownPlan, token);
        Assert.True(IsError(await mcp.ToolAsync("declarative_apply", new { planId = unknownPlan.Id, planFingerprint = unknownPlan.Hash }, token)));
        Assert.True(IsError(await mcp.ToolAsync("declarative_recover", new { planId = unknownPlan.Id, planFingerprint = unknownPlan.Hash }, token)));
        Assert.Equal(MutationOperationState.ExecutionUnknown, (await f.Operations.GetAsync(unknownPlan.Id, token))!.State);
        Assert.Null(await f.Routing.GetSubscriptionAsync(unknownArtifact.Resource, token));
        await WriteEvidence(evidence, postgres, "mcp", new { actualStdio = true, actualKeycloakService = true, noServiceBrowserCallbacks = true,
            distinctHumanApproval = true, pendingWithoutProductWrites = true, appliedExactlyOnceAcrossApiRestart = true,
            unknownToolAndAuthorityInjectionDenied = true, crossScopeDenied = true, durableUnknownFaultInjectionNeverRedispatched = true,
            absentReceiptCannotRecover = true, effectsOffByDefault = true, secondApiReceiptReplayWithFirstStopped = postgres,
            sqliteOrPostgres = postgres ? "PostgreSQL" : "SQLite" }, token);
    }

    [G3DeclarativeLiveTheory]
    [InlineData(false)] [InlineData(true)]
    public async Task Actual_Terraform_plugin_plan_apply_refresh_and_denied_destroy_use_reviewed_backend_plan(bool postgres)
    {
        RequirePostgres(postgres);
        var terraform = RequiredFile("KAFDECK_G4_TERRAFORM_BIN"); var provider = RequiredFile("KAFDECK_G4_PROVIDER_BIN");
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(12)); var token = deadline.Token;
        var evidence = Evidence(); await using var f = await Fixture.CreateAsync(evidence, postgres, token);
        f.EnableService(); var artifact = await f.CommitAsync("g4-terraform", "Active", token);
        f.Lab.Settings[f.RegistrationPrefix(artifact.Resource)+"AllowRevisionAdvance"]="true";
        var imported=await f.CommitAsync("g4-terraform-import", "Active", token);
        var unknown=await f.CommitAsync("g4-terraform-unknown", "Active", token);
        await f.Routing.CreateSubscriptionAsync(imported.Manifest.Definition,NotificationSubscriptionState.Active,DateTimeOffset.UtcNow,token);
        await f.Lab.StartAsync(token); var preview = await f.PlanAsync(artifact, 0, token);
        var root = Path.Combine(evidence, "g4-terraform-private-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(root);
        var mirror = Path.Combine(root, "mirror", "registry.terraform.io", "araditc", "kafdeck", "0.8.0", OperatingSystem.IsWindows() ? "windows_amd64" : OperatingSystem.IsMacOS() ? "darwin_arm64" : "linux_amd64");
        Directory.CreateDirectory(mirror); File.Copy(provider, Path.Combine(mirror, Path.GetFileName(provider)));
        var cli = Path.Combine(root, "terraform.rc");
        await File.WriteAllTextAsync(cli, "provider_installation { filesystem_mirror { path = " + JsonSerializer.Serialize(Path.Combine(root, "mirror").Replace('\\','/')) + " include = [\"registry.terraform.io/araditc/kafdeck\"] } }", token);
        var fields = Resource(artifact, preview);
        var resources=new Dictionary<string,Dictionary<string,object>>{{"managed",fields}};
        await WriteConfiguration(root,f.Lab.Api,resources,token);
        var runner = new TerraformProcess(terraform, root, cli, f.Lab.ServiceProfileFile);
        Assert.Equal(0, await runner.RunAsync(token, "init", "-input=false", "-no-color"));
        Assert.Equal(0, await runner.RunAsync(token, "validate", "-no-color"));
        Assert.Equal(0, await runner.RunAsync(token, "plan", "-input=false", "-no-color", "-out=governed.tfplan"));
        Assert.NotEqual(0, await runner.RunAsync(token, "apply", "-input=false", "-no-color", "governed.tfplan"));
        Assert.Null(await f.Routing.GetSubscriptionAsync(artifact.Resource, token));
        await f.ApproveAsync(preview, token);
        Assert.Equal(0, await runner.RunAsync(token, "apply", "-input=false", "-auto-approve", "-no-color"));
        Assert.Equal(1, (await f.Routing.GetSubscriptionAsync(artifact.Resource, token))!.Revision);
        await f.Lab.RestartAsync(token);
        Assert.Equal(0, await runner.RunAsync(token, "plan", "-input=false", "-no-color", "-detailed-exitcode"));
        if(postgres)
        {
            await using var second=await SecondNodeAsync(f,evidence,token);await f.Lab.StopAsync(token);
            await WriteConfiguration(root,second.Api,resources,token);
            Assert.Equal(0,await runner.RunAsync(token,"apply","-input=false","-auto-approve","-no-color"));
            Assert.Equal(1,(await f.Routing.GetSubscriptionAsync(artifact.Resource,token))!.Revision);
            await f.Lab.StartAsync(token);await WriteConfiguration(root,f.Lab.Api,resources,token);
        }
        Assert.NotEqual(0, await runner.RunAsync(token, "destroy", "-input=false", "-auto-approve", "-no-color"));
        Assert.Equal(1, (await f.Routing.GetSubscriptionAsync(artifact.Resource, token))!.Revision);
        Assert.Equal(MutationOperationState.AppliedVerified, (await f.Operations.GetAsync(preview.Id, token))!.State);
        var adoption=await f.PlanAsync(imported,0,token,"import-preview");await f.ApproveAsync(adoption,token);
        Assert.Equal(CliApplication.Success,await f.ApplyAsync(adoption,token));
        resources["imported"]=Resource(imported,adoption);await WriteConfiguration(root,f.Lab.Api,resources,token);
        Assert.Equal(0,await runner.RunAsync(token,"import","-input=false","-no-color","kafdeck_notification_subscription.imported",JsonSerializer.Serialize(resources["imported"])));
        Assert.Equal(0,await runner.RunAsync(token,"plan","-input=false","-no-color","-detailed-exitcode"));
        var stateFile=Path.Combine(root,"terraform.tfstate");var oldState=JsonNode.Parse(await File.ReadAllTextAsync(stateFile,token))!;
        foreach(var resource in oldState["resources"]!.AsArray().Where(item=>item!["mode"]!.GetValue<string>()=="managed"))foreach(var instance in resource!["instances"]!.AsArray())instance!["schema_version"]=0;
        await File.WriteAllTextAsync(stateFile,oldState.ToJsonString(),token);
        Assert.Equal(0,await runner.RunAsync(token,"apply","-refresh-only","-input=false","-auto-approve","-no-color"));
        var migrated=JsonNode.Parse(await File.ReadAllTextAsync(stateFile,token))!;
        foreach(var resource in migrated["resources"]!.AsArray().Where(item=>item!["mode"]!.GetValue<string>()=="managed"))foreach(var instance in resource!["instances"]!.AsArray())Assert.Equal(1,instance!["schema_version"]!.GetValue<int>());
        var source=await f.SourceAsync(artifact,token);var update=await f.CommitAsync(artifact.Resource,"Paused",token);
        Assert.Equal(CliApplication.Success,await f.AdvanceAsync(update,source,token));
        var updatePlan=await f.PlanAsync(update,1,token);await f.ApproveAsync(updatePlan,token);
        resources["managed"]=Resource(update,updatePlan);await WriteConfiguration(root,f.Lab.Api,resources,token);
        // Only an explicitly reviewed new source/plan can cross the stale old-state source pin.
        // The provider and backend still revalidate exact desired/current state before effect.
        Assert.Equal(0,await runner.RunAsync(token,"apply","-refresh=false","-input=false","-auto-approve","-no-color"));
        Assert.Equal(2,(await f.Routing.GetSubscriptionAsync(artifact.Resource,token))!.Revision);
        Assert.Equal(NotificationSubscriptionState.Paused,(await f.Routing.GetSubscriptionAsync(artifact.Resource,token))!.State);
        Assert.Equal(0,await runner.RunAsync(token,"plan","-input=false","-no-color","-detailed-exitcode"));
        var unknownPlan=await f.PlanAsync(unknown,0,token);await f.ApproveAsync(unknownPlan,token);await InjectDurableUnknownAsync(f,unknownPlan,token);
        resources["unknown"]=Resource(unknown,unknownPlan);await WriteConfiguration(root,f.Lab.Api,resources,token);
        Assert.NotEqual(0,await runner.RunAsync(token,"apply","-input=false","-auto-approve","-no-color"));
        Assert.Equal(MutationOperationState.ExecutionUnknown,(await f.Operations.GetAsync(unknownPlan.Id,token))!.State);
        Assert.Null(await f.Routing.GetSubscriptionAsync(unknown.Resource,token));
        resources.Remove("unknown");resources["denied"]=Resource(artifact,preview);resources["denied"]["owner_id"]="foreign-owner";
        await WriteConfiguration(root,f.Lab.Api,resources,token);Assert.NotEqual(0,await runner.RunAsync(token,"apply","-input=false","-auto-approve","-no-color"));
        Assert.Equal(2,(await f.Routing.GetSubscriptionAsync(artifact.Resource,token))!.Revision);
        resources.Remove("denied");await WriteConfiguration(root,f.Lab.Api,resources,token);
        // A real concurrent storage writer creates physical drift; refresh must not
        // erase management state, infer success, or silently repair the product.
        Assert.NotNull(await f.Routing.ReplaceSubscriptionAsync(update.Manifest.Definition,NotificationSubscriptionState.Active,2,DateTimeOffset.UtcNow,token));
        Assert.NotEqual(0,await runner.RunAsync(token,"plan","-input=false","-no-color","-detailed-exitcode"));
        Assert.Equal(3,(await f.Routing.GetSubscriptionAsync(artifact.Resource,token))!.Revision);
        await WriteEvidence(evidence, postgres, "terraform", new { actualTerraformBinary = true, actualProviderPlugin = true, filesystemMirrorOnly = true,
            realKeycloakPrivateKeyJwt = true, immutableBackendPlan = true, pendingRetainsNoProductWrites = true, independentlyHumanApproved = true,
            appliedExactlyOnce = true, restartRefreshNoChanges = true, unsupportedDestroyDenied = true, exactAdoptedImport = true,
            actualTerraformStateVersionZeroUpgrade = true, durableUnknownFaultInjectionNeverRedispatched = true, foreignOwnerDenied = true,
            reviewedSourceAdvanceAndTypedUpdate = true, actualTypedDataSourceRead = true, concurrentWriterDriftNotSilentlyRepaired = true,
            secondApiRefreshWithFirstStopped = postgres }, token);
    }

    [G3DeclarativeLiveTheory]
    [InlineData(false)] [InlineData(true)]
    public async Task Actual_Terraform_and_MCP_cannot_bypass_active_global_notification_policy(bool postgres)
    {
        RequirePostgres(postgres);
        using var deadline=new CancellationTokenSource(TimeSpan.FromMinutes(12));var token=deadline.Token;
        var evidence=Evidence();await using var f=await Fixture.CreateAsync(evidence,postgres,token);f.EnableService();
        f.Lab.Settings["Kafdeck:Governance:DeploymentGeneration"]=f.Generation.ToString("D");
        var tfControl=await f.CommitAsync("w75-tf-control","Active",token);var mcpControl=await f.CommitAsync("w75-mcp-control","Active",token);
        var tfBlocked=await f.CommitAsync("w75-tf-denied","Active",token);var mcpBlocked=await f.CommitAsync("w75-mcp-denied","Active",token);
        var oldBlocked=await f.CommitAsync("w75-old-denied","Active",token);
        var settings=f.Lab.Settings;
        var connections=postgres? (IMutationDbConnectionFactory)new PostgreSqlMutationDbConnectionFactory(
            await File.ReadAllTextAsync(settings["Kafdeck:Administration:Mutations:Persistence:ConnectionString"][5..],token)):
            new SqliteMutationDbConnectionFactory(settings["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"]);
        var generation=new AdoGovernanceGenerationGate(connections,f.Generation);await generation.InitializeAsync(token);
        var policy=new AdoGovernancePolicyStore(connections,generationGate:generation);await policy.InitializeAsync(token);
        await f.Lab.StartAsync(token);await using var mcp=await McpProcess.StartAsync(f,true,token);
        async Task<int> TerraformApply(Artifact artifact,Preview preview)
        {
            var root=Path.Combine(evidence,"w75-terraform-private-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);
            var provider=RequiredFile("KAFDECK_G4_PROVIDER_BIN");
            var mirror=Path.Combine(root,"mirror","registry.terraform.io","araditc","kafdeck","0.8.0",OperatingSystem.IsWindows()?"windows_amd64":OperatingSystem.IsMacOS()?"darwin_arm64":"linux_amd64");
            Directory.CreateDirectory(mirror);File.Copy(provider,Path.Combine(mirror,Path.GetFileName(provider)));
            var cli=Path.Combine(root,"terraform.rc");
            await File.WriteAllTextAsync(cli,"provider_installation { filesystem_mirror { path = "+JsonSerializer.Serialize(Path.Combine(root,"mirror").Replace('\\','/'))+" include = [\"registry.terraform.io/araditc/kafdeck\"] } }",token);
            await WriteConfiguration(root,f.Lab.Api,new(){{"managed",Resource(artifact,preview)}},token);
            var runner=new TerraformProcess(RequiredFile("KAFDECK_G4_TERRAFORM_BIN"),root,cli,f.Lab.ServiceProfileFile);
            Assert.Equal(0,await runner.RunAsync(token,"init","-input=false","-no-color"));
            return await runner.RunAsync(token,"apply","-input=false","-auto-approve","-no-color");
        }
        var tfControlPlan=await f.PlanAsync(tfControl,0,token);await f.ApproveAsync(tfControlPlan,token);
        Assert.Equal(0,await TerraformApply(tfControl,tfControlPlan));
        var mcpControlPlan=await f.PlanAsync(mcpControl,0,token);await f.ApproveAsync(mcpControlPlan,token);
        Assert.False(IsError(await mcp.ToolAsync("declarative_apply",new{planId=mcpControlPlan.Id,planFingerprint=mcpControlPlan.Hash},token)));
        Assert.Equal(1,(await f.Routing.GetSubscriptionAsync(tfControl.Resource,token))!.Revision);
        Assert.Equal(1,(await f.Routing.GetSubscriptionAsync(mcpControl.Resource,token))!.Revision);
        var oldPlan=await f.PlanAsync(oldBlocked,0,token);await f.ApproveAsync(oldPlan,token);
        var oldOperation=await f.Operations.GetAsync(oldPlan.Id,token);Assert.NotNull(oldOperation);
        var serverTarget=Assert.Single(oldOperation.AuthorizationTargets,t=>t.Action==AuthorizationAction.NotificationManage&&t.ResourceName==oldBlocked.Resource);
        Assert.Null(DeclarativeMutationAuthorization.Request(serverTarget).ClusterId);
        var ownershipBefore=await f.CountAsync("kafdeck_declarative_ownership_audit",token);
        var commitsBefore=await f.CountAsync("kafdeck_declarative_commit_receipts",token);
        var runtimeBefore=await f.CountAsync("kafdeck_declarative_runtime_audit",token);
        // Trusted fixture policy seeding is not claimed as product activation/approval evidence.
        // The actual client plans still require genuine service identity and separate human approval.
        var compiled=GovernancePolicyCompiler.Compile(JsonSerializer.Serialize(new{schemaVersion=1,rules=new[]{new{id="deny-reviewed-global-controls",priority=1,
            scope=new{clusterId="$deployment",resourceKind="notification",resourceIds=new[]{tfBlocked.Resource,mcpBlocked.Resource,oldBlocked.Resource},action=serverTarget.Action.ToString()},
            conditions=Array.Empty<object>(),outcome="Deny"}}}));
        Assert.False(GovernancePolicyCompiler.Evaluate(compiled,new("$deployment","notification",serverTarget.ResourceName,serverTarget.Action.ToString(),"critical"),null).Allowed);
        var before=await policy.GetStateAsync("$deployment",token);
        var binding=before.Digest is null?Array.Empty<MutationPrecondition>():new[]{new MutationPrecondition("v09.governance:$deployment",before.Digest)};
        var version=await policy.StageAsync("$deployment",compiled,"trusted-acceptance-policy-owner",token);
        await policy.CommitVersionAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"$deployment",version.VersionId,binding),version.VersionId,version.ContentHash,()=>true,token);
        await policy.ActivateAsync(V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"$deployment","active",binding),version.VersionId,version.ContentHash,before.Revision,true,()=>true,token);
        Assert.True((await policy.GetStateAsync("$deployment",token)).Enabled);
        _=await mcp.ToolAsync("declarative_apply",new{planId=oldPlan.Id,planFingerprint=oldPlan.Hash},token);
        var oldAfter=await f.Operations.GetAsync(oldPlan.Id,token);Assert.NotNull(oldAfter);
        var tfPlan=await f.PlanAsync(tfBlocked,0,token);await f.ApproveAsync(tfPlan,token);
        var mcpPlan=await f.PlanAsync(mcpBlocked,0,token);await f.ApproveAsync(mcpPlan,token);
        var logins=f.Lab.CompletedLogins;Assert.True(logins>0);
        var tfExit=await TerraformApply(tfBlocked,tfPlan);
        var mcpReply=await mcp.ToolAsync("declarative_apply",new{planId=mcpPlan.Id,planFingerprint=mcpPlan.Hash},token);
        var replay=await mcp.ToolAsync("declarative_apply",new{planId=mcpPlan.Id,planFingerprint=mcpPlan.Hash},token);
        var tfAfter=await f.Operations.GetAsync(tfPlan.Id,token);var mcpAfter=await f.Operations.GetAsync(mcpPlan.Id,token);
        Assert.NotNull(tfAfter);Assert.NotNull(mcpAfter);
        var tfDenied=tfExit!=0&&tfAfter.State==MutationOperationState.FailedBeforeDispatch&&tfAfter.ResultCode=="GovernanceDenied";
        var mcpDenied=mcpAfter.State==MutationOperationState.FailedBeforeDispatch&&mcpAfter.ResultCode=="GovernanceDenied"&&IsError(replay);
        var staleFenced=oldAfter.State==MutationOperationState.StalePreview;
        var noEffects=await f.Routing.GetSubscriptionAsync(tfBlocked.Resource,token) is null&&await f.Routing.GetSubscriptionAsync(mcpBlocked.Resource,token) is null&&await f.Routing.GetSubscriptionAsync(oldBlocked.Resource,token) is null;
        var auditsUnchanged=ownershipBefore==await f.CountAsync("kafdeck_declarative_ownership_audit",token)&&commitsBefore==await f.CountAsync("kafdeck_declarative_commit_receipts",token)&&runtimeBefore==await f.CountAsync("kafdeck_declarative_runtime_audit",token);
        await File.WriteAllBytesAsync(Path.Combine(evidence,(postgres?"postgres":"sqlite")+"-w75-automation-policy-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(new{
            status=tfDenied&&mcpDenied&&staleFenced&&noEffects&&auditsUnchanged?"Passed":"Failed",provider=postgres?"PostgreSQL":"SQLite",
            genuineKeycloakService=true,actualTerraformPlugin=true,actualPackagedMcpStdio=true,positiveControlsApplied=true,trustedPolicyFixtureSeeding=true,
            sourceTargetDerivedFromActualW68Plan=true,canonicalGlobalScope="$deployment",policyContentHash=compiled.ContentHash,
            distinctHumanClientPlanApproval=true,tfDenied,mcpDenied,staleFenced,noEffects,auditsUnchanged,
            firstMcpTransportError=IsError(mcpReply),terminalBackendCode=mcpAfter.ResultCode,serviceEffectsHaveNoBrowserCallbacks=logins==f.Lab.CompletedLogins}),token);
        Assert.True(staleFenced,"Global policy activation failed to stale a pre-existing approved automation plan.");
        Assert.True(tfDenied,"Actual Terraform bypassed the active global notification policy.");Assert.True(mcpDenied,"Actual MCP bypassed the active global notification policy.");
        Assert.True(noEffects&&auditsUnchanged,"Denied automation changed product state or its durable audit.");
    }

    private static Dictionary<string,object> Resource(Artifact artifact, Preview plan) => new()
    {
        ["deployment_id"]="g3-deployment",["owner_id"]="team-ops",["source_ref"]=artifact.SourceRef,["source_revision"]=artifact.Manifest.Fingerprint(),
        ["resource_id"]=artifact.Resource,["destination_ref"]="ops-notifier",["subscription_state"]=artifact.Manifest.State.ToString(),["event_classes"]=new[]{"Operational"},
        ["event_types"]=new[]{"consumer.lag.alert"},["apply_plan_id"]=plan.Id.ToString("D"),["apply_plan_fingerprint"]=plan.Hash
    };
    private static Task WriteConfiguration(string directory,Uri endpoint,Dictionary<string,Dictionary<string,object>> resources,CancellationToken token)
    {
        var data=new Dictionary<string,object>(resources["managed"]){["depends_on"]=new[]{"kafdeck_notification_subscription.managed"}};
        return File.WriteAllBytesAsync(Path.Combine(directory,"main.tf.json"),JsonSerializer.SerializeToUtf8Bytes(new{
            terraform=new{required_providers=new{kafdeck=new{source="araditc/kafdeck",version="0.8.0"}}},provider=new{kafdeck=new{endpoint=endpoint.AbsoluteUri.TrimEnd('/')}},
            resource=new{kafdeck_notification_subscription=resources},
            data=new{kafdeck_notification_subscription=new Dictionary<string,Dictionary<string,object>>{{"observed",data}}}}),token);
    }
    private static async Task<G2HumanCliLaboratory> SecondNodeAsync(Fixture f,string evidence,CancellationToken token)
    {
        var second=new G2HumanCliLaboratory(Path.Combine(evidence,"g4-second-api-private-"+Guid.NewGuid().ToString("N")));
        foreach(var item in f.Lab.Settings)second.Settings[item.Key]=item.Value;
        second.Settings["Kafdeck:Deployment:ListenUrls:0"]=second.Api.AbsoluteUri;
        try{await second.StartAsync(token);return second;}catch{await second.DisposeAsync();throw;}
    }
    private static bool IsError(JsonElement wire) => wire.TryGetProperty("error", out _) || wire.GetProperty("result").TryGetProperty("isError", out var error) && error.GetBoolean();
    private static JsonElement ToolData(JsonElement wire)
    {
        Assert.False(IsError(wire)); var result = wire.GetProperty("result");
        if (result.TryGetProperty("structuredContent", out var structured)) return structured.GetProperty("data");
        var text = result.GetProperty("content").EnumerateArray().Single(item => item.GetProperty("type").GetString() == "text").GetProperty("text").GetString()!;
        using var document = JsonDocument.Parse(text); return document.RootElement.Clone();
    }
    private static void RequirePostgres(bool postgres) { if(postgres && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES"))) throw new InvalidOperationException("Enabled G4 acceptance requires real PostgreSQL."); }
    private static string Evidence() { var path=Environment.GetEnvironmentVariable("KAFDECK_G4_EVIDENCE_DIRECTORY") ?? Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetTempPath(); Directory.CreateDirectory(path); return path; }
    private static string RequiredFile(string name) { var path=Environment.GetEnvironmentVariable(name); Assert.True(path is not null && File.Exists(path), "Enabled G4 acceptance requires the actual packaged integration binary: "+name); return path!; }
    private static Task WriteEvidence(string directory,bool postgres,string family,object proof,CancellationToken token) => File.WriteAllBytesAsync(Path.Combine(directory,(postgres?"postgres":"sqlite")+"-g4-"+family+"-evidence.json"),JsonSerializer.SerializeToUtf8Bytes(proof),token);

    // Explicit durable fault injection at the actual repository seam models a dispatched
    // operation with no trusted receipt. This is not claimed as a real process-crash test.
    private static async Task InjectDurableUnknownAsync(Fixture f,Preview preview,CancellationToken token)
    {
        var snapshot=await f.Operations.GetAsync(preview.Id,token);Assert.NotNull(snapshot);
        var operation=MutationOperation.Restore(snapshot);var version=operation.Snapshot.Version;var now=DateTimeOffset.UtcNow;
        operation.ClaimExecution(now,now.AddMinutes(2));Assert.Equal(MutationSaveOutcome.Saved,(await f.Operations.TrySaveAsync(operation.Snapshot,version,token)).Outcome);
        Assert.Equal(MutationResourceClaimOutcome.Acquired,(await f.Operations.TryAcquireResourceClaimsAsync(preview.Id,operation.Snapshot.ExecutionClaimGeneration,operation.Snapshot.ResourceKeys,operation.Snapshot.ExecutionClaimExpiresAtUtc!.Value,token)).Outcome);
        version=operation.Snapshot.Version;operation.MarkDispatchStarted(now);Assert.Equal(MutationSaveOutcome.Saved,(await f.Operations.TrySaveAsync(operation.Snapshot,version,token)).Outcome);
        version=operation.Snapshot.Version;operation.Complete(MutationExecutionResultKind.ExecutionUnknown,"g4_acceptance_durable_fault_injection",now);
        Assert.Equal(MutationSaveOutcome.Saved,(await f.Operations.TrySaveAsync(operation.Snapshot,version,token)).Outcome);
    }

    private sealed class TerraformProcess(string executable,string directory,string configuration,string profileFile)
    {
        internal async Task<int> RunAsync(CancellationToken token,params string[] args)
        {
            // Fresh real private_key_jwt exchange per command; no stale access-token cache.
            using var session=await CliServiceOidcAuthenticator.AcquireAsync(await CliServiceOidcProfile.LoadAsync(profileFile,token),token);
            var credential=session.AccessToken;
            var start=new ProcessStartInfo(executable){WorkingDirectory=directory,UseShellExecute=false,CreateNoWindow=true,RedirectStandardOutput=true,RedirectStandardError=true};
            foreach(var arg in args)start.ArgumentList.Add(arg);
            start.Environment["TF_CLI_CONFIG_FILE"]=configuration;start.Environment["TF_IN_AUTOMATION"]="1";start.Environment["CHECKPOINT_DISABLE"]="1";start.Environment["KAFDECK_TOKEN"]=credential;
            using var process=Process.Start(start)??throw new InvalidOperationException("Actual Terraform did not start.");
            var stdout=process.StandardOutput.ReadToEndAsync(token);var stderr=process.StandardError.ReadToEndAsync(token);
            try{await process.WaitForExitAsync(token);}catch{if(!process.HasExited)process.Kill(true);throw;}
            var text=await stdout+await stderr;
            Assert.False(text.Contains(credential,StringComparison.Ordinal),"Terraform output disclosed an access credential.");
            var stateFile=Path.Combine(directory,"terraform.tfstate");
            if(File.Exists(stateFile))
                Assert.False((await File.ReadAllTextAsync(stateFile,token)).Contains(credential,StringComparison.Ordinal),"Terraform state disclosed an access credential.");
            await File.WriteAllTextAsync(Path.Combine(directory,"private-"+args[0]+"-"+Guid.NewGuid().ToString("N")+".log"),text,token);
            Console.WriteLine("G4_TERRAFORM phase="+args[0]+"; exit="+process.ExitCode);return process.ExitCode;
        }
    }

    private sealed class McpProcess : IAsyncDisposable
    {
        private readonly Process _process;private readonly Task<string> _stderr;private int _id;
        private McpProcess(Process process){_process=process;_stderr=process.StandardError.ReadToEndAsync();}
        internal static async Task<McpProcess> StartAsync(Fixture f,bool effects,CancellationToken token,Uri? endpoint=null)
        {
            var root=new DirectoryInfo(AppContext.BaseDirectory);while(!File.Exists(Path.Combine(root.FullName,"Kafdeck.slnx")))root=root.Parent!;
            var configuration=typeof(V08G4ExternalIntegrationLiveTests).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration??"Debug";
            var dll=Environment.GetEnvironmentVariable("KAFDECK_G4_MCP_DLL")??Path.Combine(root.FullName,"src/mcp/Kafdeck.Mcp/bin",configuration,"net10.0/kafdeck-mcp.dll");
            Assert.True(File.Exists(dll),"Actual packaged MCP executable is required.");
            var start=new ProcessStartInfo("dotnet"){UseShellExecute=false,CreateNoWindow=true,RedirectStandardInput=true,RedirectStandardOutput=true,RedirectStandardError=true};
            foreach(var arg in new[]{dll,"--base-url",(endpoint??f.Lab.Api).AbsoluteUri,"--service-profile",f.Lab.ServiceProfileFile})start.ArgumentList.Add(arg);
            if(effects)start.ArgumentList.Add("--enable-effects");
            var client=new McpProcess(Process.Start(start)??throw new InvalidOperationException("Actual MCP process did not start."));
            try{var response=await client.CallAsync("initialize",new{protocolVersion="2025-06-18",capabilities=new{},clientInfo=new{name="kafdeck-g4-acceptance",version="0.8.0"}},token);Assert.False(response.TryGetProperty("error",out _));
                await client._process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(new{jsonrpc="2.0",method="notifications/initialized"}).AsMemory(),token);await client._process.StandardInput.FlushAsync(token);return client;}
            catch{await client.DisposeAsync();throw;}
        }
        internal Task<JsonElement> ToolAsync(string name,object arguments,CancellationToken token)=>CallAsync("tools/call",new{name,arguments},token);
        internal async Task<JsonElement> CallAsync(string method,object parameters,CancellationToken token)
        {
            var id=++_id;await _process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(new{jsonrpc="2.0",id,method,@params=parameters}).AsMemory(),token);await _process.StandardInput.FlushAsync(token);
            while(true){var line=await _process.StandardOutput.ReadLineAsync(token);Assert.NotNull(line);Assert.True(line.Length<=256*1024,"MCP exceeded output budget.");using var json=JsonDocument.Parse(line);
                if(json.RootElement.TryGetProperty("id",out var reply)&&reply.ValueKind==JsonValueKind.Number&&reply.GetInt32()==id)return json.RootElement.Clone();}
        }
        public async ValueTask DisposeAsync(){_process.StandardInput.Close();using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(10));try{await _process.WaitForExitAsync(deadline.Token);}catch(OperationCanceledException){if(!_process.HasExited)_process.Kill(true);await _process.WaitForExitAsync();}_=await _stderr;_process.Dispose();}
    }
}
