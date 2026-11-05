using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W75PolicyMissingMetadataTests
{
    [Fact]
    public async Task SQLite_HTTP_explain_dry_run_and_transaction_gate_agree_on_unset_catalog_facts()
    {
        var path=Path.Combine(Path.GetTempPath(),"w75-missing-facts-"+Guid.NewGuid().ToString("N")+".db");
        try{await Scenario(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }

    [G2PostgreSqlFact]
    public async Task PostgreSQL_HTTP_explain_dry_run_and_transaction_gate_agree_on_unset_catalog_facts()
    {
        await using var database=await G2CliPostgreSqlDatabase.CreateAsync();
        await Scenario(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString));
    }

    private static async Task Scenario(IMutationDbConnectionFactory factory)
    {
        using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(60));var token=deadline.Token;
        var catalog=new AdoCatalogStore(factory,"missing-fact-fixture");await catalog.InitializeAsync(token);
        var topics=new ControlledTopicIdentity();
        var change=CatalogContract.Validate(new(CatalogEntityKind.Topic,null,0,"Orders",TopicName:"orders"));
        var items=new[]{new CatalogPlannedChange(change,Guid.NewGuid().ToString("N"),topics.Identity.Fingerprint)};
        var now=DateTimeOffset.UtcNow;
        var plan=new CatalogPlan(Guid.NewGuid(),"alpha","oidc:https://idp.example|operator",new string('a',64),catalog.DeploymentGeneration,
            CatalogContract.MaterialHash("alpha",items),items,now,now.AddMinutes(5));
        await catalog.StageAsync(plan,token);Assert.NotNull(await catalog.CommitAsync(plan,()=>true,token));
        var row=await catalog.FindTopicAsync("alpha","orders",token);Assert.NotNull(row);
        var projected=GovernancePolicyMetadata.FromCatalog(row);Assert.NotNull(projected);
        foreach(var field in new[]{"classification","domain","teamId","applicationId"})Assert.False(projected.ContainsKey(field));
        Assert.Equal("",GovernancePolicyMetadata.FromCatalog(row with{Classification=""})!["classification"]);

        var store=new AdoGovernancePolicyStore(factory,catalog);await store.InitializeAsync(token);
        var roles=AuthorizationPolicyCompiler.Compile(new([
            new("operator",new[]{AuthorizationAction.GovernancePolicyRead,AuthorizationAction.CatalogRead,AuthorizationAction.TopicDelete}
                .Select(action=>new AuthorizationPermissionDefinition(action,["alpha"],["*"])).ToArray())],
            [new("https://idp.example","operator",["operator"])],[]));
        var auth=new KafdeckAuthorizationService(new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[]),new AuthorizationPolicyEvaluator(roles));
        var principal=OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub","operator")],"oidc")),"https://idp.example",null,DateTimeOffset.UtcNow);
        var builder=WebApplication.CreateBuilder(new WebApplicationOptions{EnvironmentName="Production"});builder.WebHost.UseKestrel().UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(auth);builder.Services.AddSingleton<IGovernancePolicyStore>(store);builder.Services.AddSingleton<ICatalogStore>(catalog);builder.Services.AddSingleton<ICatalogTopicIdentityPort>(topics);
        await using var app=builder.Build();app.UseRouting();app.Use((context,next)=>{context.User=principal;return next(context);});app.MapKafdeckPolicyEvaluation();await app.StartAsync(token);
        using var client=new HttpClient(new SocketsHttpHandler{UseProxy=false}){BaseAddress=new Uri(Assert.Single(app.Urls)),Timeout=TimeSpan.FromSeconds(10)};

        foreach(var field in new[]{"classification","domain","teamId","applicationId"})
        {
            var before=await store.GetStateAsync("alpha",token);
            var binding=before.Digest is null?Array.Empty<MutationPrecondition>():new[]{new MutationPrecondition("v09.governance:alpha",before.Digest)};
            var document=JsonSerializer.Serialize(new{schemaVersion=1,rules=new[]{new{id="deny-restricted",priority=1,
                scope=new{clusterId="alpha",resourceKind="topic",resourceIds=new[]{"orders"},action="TopicDelete"},
                conditions=new[]{new{field,@operator="equals",value="restricted"}},outcome="Deny"}}});
            var version=await store.StageAsync("alpha",GovernancePolicyCompiler.Compile(document),"owner",token);
            var commit=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyVersion,AuthorizationAction.GovernancePolicyManage,"alpha",version.VersionId,binding);
            await store.CommitVersionAsync(commit,version.VersionId,version.ContentHash,()=>true,token);
            var activation=V09W75RecoverySeed.Operation(MutationOperationKind.GovernancePolicyActivation,AuthorizationAction.GovernancePolicyActivate,"alpha","active",binding);
            Assert.Equal(MutationExecutionResultKind.AppliedVerified,(await store.ActivateAsync(activation,version.VersionId,version.ContentHash,before.Revision,true,()=>true,token)).ResultKind);

            var fixture=new PolicyExplainRequest("alpha","topic","orders","TopicDelete","high");
            using(var response=await client.PostAsJsonAsync("/api/v1/governance/policies/explain",fixture,token))
            {
                Assert.Equal(HttpStatusCode.OK,response.StatusCode);using var body=JsonDocument.Parse(await response.Content.ReadAsStringAsync(token));
                Assert.False(body.RootElement.GetProperty("allowed").GetBoolean());Assert.Equal("GovernanceEvidenceUnavailable",body.RootElement.GetProperty("code").GetString());
            }
            using(var response=await client.PostAsJsonAsync("/api/v1/governance/policies/dry-run",new PolicyDryRunRequest([fixture]),token))
            {
                Assert.Equal(HttpStatusCode.OK,response.StatusCode);using var body=JsonDocument.Parse(await response.Content.ReadAsStringAsync(token));
                var result=body.RootElement.GetProperty("items")[0];Assert.False(result.GetProperty("allowed").GetBoolean());Assert.Equal("GovernanceEvidenceUnavailable",result.GetProperty("code").GetString());
            }
            var state=await store.GetStateAsync("alpha",token);
            var operation=V09W75RecoverySeed.Operation(MutationOperationKind.TopicDelete,AuthorizationAction.TopicDelete,"alpha","orders",[new("v09.governance:alpha",state.Digest!)]);
            store.CaptureTopicIdentity(operation.OperationId,"alpha","orders",topics.Identity.Fingerprint);
            Assert.Equal("GovernanceEvidenceUnavailable",(await Assert.ThrowsAsync<GovernanceEffectRejectedException>(()=>store.ValidateOperationAsync(operation,token))).Code);
        }
        await app.StopAsync(token);
    }

    // Controlled provider observation for this HTTP/SQL regression; genuine Kafka identity is covered by the live acceptance lane.
    private sealed class ControlledTopicIdentity:ICatalogTopicIdentityPort
    {
        internal CatalogTopicIdentity Identity {get;}=new("physical-cluster","physical-topic");
        public Task<CatalogTopicIdentity?> ObserveAsync(string clusterId,string topicName,CancellationToken token)=>Task.FromResult<CatalogTopicIdentity?>(clusterId=="alpha"&&topicName=="orders"?Identity:null);
    }
}
