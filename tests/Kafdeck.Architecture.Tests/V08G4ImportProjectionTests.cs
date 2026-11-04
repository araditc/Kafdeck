using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Production-handler/storage contract proof; real OIDC is tested separately.
public sealed class V08G4ImportProjectionTests
{
    [Theory]
    [InlineData("valid",200)] [InlineData("owner",409)] [InlineData("source",409)]
    [InlineData("controller",409)] [InlineData("principal",409)] [InlineData("revision",409)]
    [InlineData("grant",403)] [InlineData("generation",409)]
    public Task SQLite_import_checks_current_binding_without_writes(string scenario,int status) => Run(false,scenario,status);

    [W68G3PostgresTheory]
    [InlineData("valid",200)] [InlineData("owner",409)] [InlineData("source",409)]
    [InlineData("controller",409)] [InlineData("principal",409)] [InlineData("revision",409)]
    [InlineData("grant",403)] [InlineData("generation",409)]
    public Task PostgreSQL_import_checks_current_binding_without_writes(string scenario,int status) => Run(true,scenario,status);

    private static async Task Run(bool postgres,string scenario,int status)
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(60));var token=timeout.Token;
        await using var db=await W68G3PersistenceFixture.CreateAsync(postgres,token);
        var user=OidcIdentityNormalizer.Normalize(new(new ClaimsIdentity([new Claim("sub","terraform-reader")],"oidc")),
            "https://issuer.example",null,DateTimeOffset.UtcNow);
        Assert.True(MutationActor.TryCreate(user,out var actor));
        var manifest=JsonSerializer.Serialize(new {apiVersion="kafdeck.io/v0.8",kind="NotificationSubscription",
            metadata=new {deploymentRef=W68G3PersistenceFixture.Deployment,name="subscription-a",ownerRef=W68G3PersistenceFixture.Owner},
            spec=new {destinationRef="destination-b",state="Paused",eventClasses=new[]{"Operational"},eventTypes=new[]{"fixture.changed"}}});
        var hash=DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(manifest),"json").Fingerprint();
        var source=await db.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source,W68G3PersistenceFixture.Deployment,W68G3PersistenceFixture.Source,token);
        Assert.NotNull(await db.Store.UpsertRegistrationAsync(source! with {PinnedRevision=hash},source!.Revision,W68G3PersistenceFixture.Yes,token));
        var (plan,_)=await db.PlanAsync(W68RuntimePlanCommand.Apply,sourceRevision:hash,token:token);
        Assert.NotNull(await db.Store.ApplySubscriptionAsync(plan.PlanId,plan.PlanFingerprint,W68G3PersistenceFixture.Principal,W68G3PersistenceFixture.Yes,token));
        const string readerController="g4-import-reader";
        var controller=new W68RuntimeRegistration(W68RuntimeRegistrationKind.Controller,W68G3PersistenceFixture.Deployment,readerController,Guid.NewGuid(),0,W68G3PersistenceFixture.Owner,scenario=="principal"?"oidc:other":actor!.PrincipalId,"","","",true);
        Assert.NotNull(await db.Store.UpsertRegistrationAsync(controller,0,W68G3PersistenceFixture.Yes,token));
        if(scenario is "owner" or "source" or "controller")
        {
            var kind=scenario=="owner"?W68RuntimeRegistrationKind.Owner:scenario=="source"?W68RuntimeRegistrationKind.Source:W68RuntimeRegistrationKind.Controller;
            var id=kind==W68RuntimeRegistrationKind.Owner?W68G3PersistenceFixture.Owner:kind==W68RuntimeRegistrationKind.Source?W68G3PersistenceFixture.Source:readerController;
            var row=await db.Store.GetRegistrationAsync(kind,W68G3PersistenceFixture.Deployment,id,token);
            var changed=row! with {Enabled=false};
            Assert.NotNull(await db.Store.UpsertRegistrationAsync(changed,row!.Revision,W68G3PersistenceFixture.Yes,token));
        }
        if(scenario=="generation") await W68RuntimeSchema.AdmitGenerationAsync(db.Connections,W68G3PersistenceFixture.Deployment,db.Generation,Guid.NewGuid(),W68G3PersistenceFixture.R1,token);
        var anchor=Path.Combine(Path.GetTempPath(),"g4-import-"+Guid.NewGuid().ToString("N")+".json");
        await File.WriteAllTextAsync(anchor,JsonSerializer.Serialize(new {version=1,deploymentId=W68G3PersistenceFixture.Deployment,generation=db.Generation,state="active"}),token);
        try
        {
            var values=new Dictionary<string,string?> { ["Kafdeck:Declarative:Enabled"]="true",["Kafdeck:Declarative:DeploymentId"]=W68G3PersistenceFixture.Deployment,
                ["Kafdeck:Declarative:DeploymentGeneration"]=db.Generation.ToString(),["Kafdeck:Declarative:RecoveryAnchorFile"]=anchor,
                ["Kafdeck:Declarative:RecoveryAnchorSha256"]=Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(anchor,token))).ToLowerInvariant() };
            var prefix="Kafdeck:Declarative:Registrations:0:";
            foreach(var pair in new Dictionary<string,string> { ["Id"]=readerController,["OwnerId"]=W68G3PersistenceFixture.Owner,
                ["SourceRef"]=W68G3PersistenceFixture.Source,["SourceRevision"]=hash,["PrincipalId"]=actor!.PrincipalId,["Enabled"]="true",
                ["ExpiresAtUtc"]=DateTimeOffset.UtcNow.AddHours(1).ToString("O"),["ResourceIds:0"]="subscription-a" }) values[prefix+pair.Key]=pair.Value;
            var config=new ConfigurationBuilder().AddInMemoryCollection(values).Build();
            var grants=new List<AuthorizationPermissionDefinition> {new(AuthorizationAction.DeclarativeRead,[W68G3PersistenceFixture.Deployment],["subscription-a"]),
                new(AuthorizationAction.DeclarativePlan,[W68G3PersistenceFixture.Deployment],["subscription-a"]),new(AuthorizationAction.NotificationRead,null,["subscription-a","destination-b"])};
            if(scenario!="grant")grants.Add(new(AuthorizationAction.DeclarativeAdopt,[W68G3PersistenceFixture.Deployment],["subscription-a"]));
            var evaluator=new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new([new("reader",grants)],
                [new("https://issuer.example","terraform-reader",["reader"])],[])));
            var options=new KafdeckOptions(new("http://127.0.0.1:8080",null,AccessMode.Oidc,null),[]);
            var policy=new KafdeckAuthorizationService(options,evaluator);var authority=new DeclarativeRuntimeAuthorizer(config,policy);
            using var services=new ServiceCollection().AddSingleton<IConfiguration>(config).BuildServiceProvider();
            var context=new DefaultHttpContext {User=user,RequestServices=services};
            var service=new DeclarativeEndpointService(db.Store,db.Routing,authority,null!,null!,null!,policy,null!,null!,null!);
            var key=new W68RuntimeResourceKey(W68G3PersistenceFixture.Deployment,"Deployment",W68G3PersistenceFixture.Deployment,"NotificationSubscription","subscription-a");
            var before=await db.Store.GetOwnershipAsync(key,token);var subscription=await db.Routing.GetSubscriptionAsync("subscription-a",token);
            var result=await service.HandleAsync("import-check",new(manifest,"json",W68G3PersistenceFixture.Source,hash,scenario=="revision"?99:1),context,token);
            Assert.Equal(status,((IStatusCodeHttpResult)result).StatusCode);
            Assert.Equal(before,await db.Store.GetOwnershipAsync(key,token));Assert.Equal(JsonSerializer.Serialize(subscription),JsonSerializer.Serialize(await db.Routing.GetSubscriptionAsync("subscription-a",token)));
            if(status==200)
            {
                var json=JsonSerializer.SerializeToElement(((IValueHttpResult)result).Value);
                Assert.Equal(before!.GenerationId,json.GetProperty("ownershipGeneration").GetGuid());
                Assert.Equal(db.Generation,json.GetProperty("deploymentGeneration").GetGuid());
                Assert.Equal("destination-b",json.GetProperty("current").GetProperty("destinationRef").GetString());
                Assert.True(json.GetProperty("sourceInSync").GetBoolean());
            }
        }
        finally{File.Delete(anchor);}
    }
}
