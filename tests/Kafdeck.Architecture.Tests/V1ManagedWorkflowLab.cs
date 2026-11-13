using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Confluent.Kafka;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Clusters;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Caching.Memory;

namespace Kafdeck.Architecture.Tests;

// Real loopback routing/CSRF/file state with an explicit controlled Keycloak transport.
internal sealed class V1ManagedWorkflowLab : IAsyncDisposable
{
    internal readonly ManagedProvisioningFixture Identity = new();
    internal readonly WorkflowKeycloakHandler Provider = new();
    internal readonly IHttpContextAccessor Requests = new HttpContextAccessor();
    internal readonly IConfiguration Configuration = new ConfigurationBuilder().AddInMemoryCollection(
        new Dictionary<string,string?> { ["Kafdeck:ConnectionWorkspace:RegistrationNetworks:0"] = "127.0.0.0/8" }).Build();
    internal ManagedIdentityStore Store => Identity.Store;
    internal WebApplication App = null!;
    internal HttpClient Http = null!;
    internal ClaimsPrincipal Actor = null!;
    internal int MetadataProbes;
    internal RegisteredKafkaRuntime Runtime = null!;
    private ManagedKeycloakUsers _providerClient = null!;

    internal static ClaimsPrincipal Browser(Guid id, DateTimeOffset? authenticated = null) =>
        OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub",id.ToString("D"))],"oidc")),
            ManagedProvisioningFixture.Issuer,null,authenticated ?? DateTimeOffset.UtcNow);

    internal Guid AddUser(string role = "manager", string[]? clusters = null, bool enabled = true)
    {
        var state=Store.Read(); var id=Guid.NewGuid();
        Store.Write(state with { Users=[..state.Users,new(id,"ordinary@example.test",role,clusters ?? ["connection-a"],enabled,default)] });
        return id;
    }
    internal void ChangeUser(Guid id, Func<ManagedUser,ManagedUser> change)
    { var state=Store.Read(); Store.Write(state with { Users=state.Users.Select(u=>u.Id==id?change(u):u).ToArray() }); }
    internal void AddTargets(string protocol = "SASL_PLAINTEXT")
    {
        var state=Store.Read();Store.Write(state with { Connections=[
            new("connection-a","A",["127.0.0.1:19092"],protocol,protocol=="PLAINTEXT"?null:"PLAIN"),
            new("connection-b","B",["127.0.0.1:19093"],protocol,protocol=="PLAINTEXT"?null:"PLAIN")] });
    }
    internal static async Task<V1ManagedWorkflowLab> Start(bool setup = false, MemoryCache? credentials = null)
    {
        var f=new V1ManagedWorkflowLab();
        if(setup)f.Store.Write(new(ManagedProvisioningFixture.Issuer,null,false,null,default,[]));
        f.Actor=setup?new ClaimsPrincipal(new ClaimsIdentity()):Browser(f.Identity.OwnerId);
        var builder=WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName="Production" });
        builder.WebHost.UseUrls("http://127.0.0.1:0");builder.Configuration.AddConfiguration(f.Configuration);
        var options=new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[]);
        var policy=AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition([],[],[]));
        builder.Services.AddSingleton(new KafdeckAuthorizationService(options,new AuthorizationPolicyEvaluator(policy)) { ManagedIdentity=f.Store });
        f._providerClient=new(new HttpClient(f.Provider),ManagedProvisioningFixture.Issuer,"provisioner","fixture-only-secret");
        builder.Services.AddSingleton(f._providerClient);builder.Services.AddSingleton<ISecurityAuditSink>(f.Identity.Audit);
        builder.Services.AddSingleton(new GovernanceRuntimeOptions());builder.Services.AddSingleton<ClusterExplorerService>(_=>null!);
        f.Runtime=new(f.Requests,f.Store,f.Configuration,credentials);builder.Services.AddSingleton(f.Runtime);builder.Services.AddSingleton<IHttpContextAccessor>(f.Requests);
        builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
        f.App=builder.Build();f.App.UseRouting();f.App.Use((context,next)=>{context.User=f.Actor;return next(context);});f.App.UseAntiforgery();f.App.MapKafdeckAntiforgeryEndpoint();
        f.App.MapManagedIdentity(f.Store,options);
        f.App.MapKafkaConnectionWorkspace(options,f.Store,(_,_)=>{f.MetadataProbes++;return Task.FromResult(new Metadata([new Confluent.Kafka.BrokerMetadata(1,"127.0.0.1",19092)],[],1,"127.0.0.1:19092"));});
        f.App.MapRegisteredHttpServices(f.Store,new(f.Store,f.Requests,new EphemeralDataProtectionProvider(),f.Configuration));
        await f.App.StartAsync();f.Http=new(new SocketsHttpHandler { UseProxy=false }){BaseAddress=new Uri(f.App.Urls.Single())};
        await f.Csrf();return f;
    }
    internal async Task Csrf()
    {
        using var tokens=await Http.GetFromJsonAsync<JsonDocument>("/api/v1/auth/csrf");
        Http.DefaultRequestHeaders.Remove(KafdeckAntiforgeryExtensions.HeaderName);
        Http.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName,tokens!.RootElement.GetProperty("requestToken").GetString());
    }
    internal Task<HttpResponseMessage> Setup(string code)=>Http.PostAsJsonAsync("/api/v1/setup",new {code,email="First.Owner@example.test",password="Temporary-password-123!"});
    internal static async Task<string?> Problem(HttpResponseMessage response)
    { using var body=JsonDocument.Parse(await response.Content.ReadAsStringAsync());return body.RootElement.GetProperty("type").GetString(); }
    public async ValueTask DisposeAsync()
    { Http.Dispose();await App.StopAsync();await App.DisposeAsync();_providerClient.Dispose();Runtime.Dispose();Identity.Dispose(); }
}

internal sealed class WorkflowKeycloakHandler : HttpMessageHandler
{
    internal int Creates,EnableChanges,Logouts;
    internal bool ExistingForeignUser,LoseCommittedReply,FailUserChanges;
    internal HttpStatusCode CreateStatus=HttpStatusCode.Created;
    internal Func<Task>? CreateGate;
    internal readonly List<Guid> Accounts=[];
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken cancellation)
    {
        var path=request.RequestUri!.AbsolutePath;
        if(path.EndsWith("/token",StringComparison.Ordinal))return Json(new {access_token="fixture-token"});
        if(request.Method==HttpMethod.Get)return Json(ExistingForeignUser?new[]{new {username="foreign@example.test"}}:Array.Empty<object>());
        if(request.Method==HttpMethod.Put)
        { EnableChanges++;return new(FailUserChanges?HttpStatusCode.ServiceUnavailable:HttpStatusCode.NoContent); }
        if(path.EndsWith("/logout",StringComparison.Ordinal)){Logouts++;return new(HttpStatusCode.NoContent);}
        Creates++;if(CreateGate is {} gate)await gate();
        if(CreateStatus!=HttpStatusCode.Created)return new(CreateStatus);
        var id=Guid.NewGuid();Accounts.Add(id);
        if(LoseCommittedReply)throw new HttpRequestException("Fixture lost an already committed first-owner reply.");
        var response=new HttpResponseMessage(HttpStatusCode.Created);response.Headers.Location=new Uri(request.RequestUri!.AbsoluteUri+"/"+id);return response;
    }
    private static HttpResponseMessage Json(object body)=>new(HttpStatusCode.OK){Content=JsonContent.Create(body)};
}
