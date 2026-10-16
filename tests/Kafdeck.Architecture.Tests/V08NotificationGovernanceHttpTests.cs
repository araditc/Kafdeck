using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08NotificationGovernanceHttpTests
{
    [Theory]
    [InlineData(AuthorizationAction.NotificationManage,"ops",true,200,1)]
    [InlineData(AuthorizationAction.NotificationManage,null,true,200,2)]
    [InlineData(AuthorizationAction.NotificationRead,null,true,403,0)]
    [InlineData(AuthorizationAction.NotificationManage,null,false,401,0)]
    public async Task Actual_metadata_GET_filters_destinations_before_any_scoped_store_access(AuthorizationAction action,
        string? resource,bool authenticated,int expectedStatus,int expectedReads)
    {
        var store=new ReadSpy();var builder=Builder(action,resource,authenticated,out _);
        builder.Services.AddSingleton<INotificationGovernanceStore>(store);
        await using var app=builder.Build();Install(app,Principal(authenticated));
        await app.StartAsync();
        using var client=new HttpClient(new SocketsHttpHandler {UseProxy=false}) {BaseAddress=new Uri(Assert.Single(app.Urls))};
        using var response=await client.GetAsync("/api/v1/notifications/management/destinations");
        Assert.Equal(expectedStatus,(int)response.StatusCode);Assert.Equal(expectedReads,store.Reads);
        if(resource=="ops") { var body=await response.Content.ReadAsStringAsync();Assert.Contains("ops",body);Assert.DoesNotContain("hidden",body); }
        await app.StopAsync();
    }
    [Fact]
    public async Task Actual_HTTP_management_requires_CSRF_scoped_current_grant_CAS_and_preserves_atomic_safe_audit()
    {
        var directory=Path.Combine(Path.GetTempPath(),"governance-http-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        try
        {
            var store=new AdoNotificationGovernanceStore(new SqliteNotificationDeliveryDbConnectionFactory(Path.Combine(directory,"db")));
            await store.InitializeAsync(default);
            var builder=Builder(AuthorizationAction.NotificationManage,"ops",true,out var audit);
            builder.Services.AddSingleton<INotificationGovernanceStore>(store);
            await using var app=builder.Build();Install(app,Principal(true));await app.StartAsync();
            using var client=new HttpClient(new SocketsHttpHandler {UseProxy=false,UseCookies=true,CookieContainer=new CookieContainer()})
                {BaseAddress=new Uri(Assert.Single(app.Urls))};
            const string route="/api/v1/notifications/management/destinations/ops";
            using(var denied=await client.PutAsJsonAsync(route,new NotificationManagedDestinationChangeRequest("v1",0,true,true)))
                Assert.Equal(HttpStatusCode.Forbidden,denied.StatusCode);
            Assert.Null(await store.GetAsync("ops",default));
            using(var token=JsonDocument.Parse(await client.GetStringAsync("/api/v1/auth/csrf")))
                client.DefaultRequestHeaders.Add(token.RootElement.GetProperty("headerName").GetString()!,token.RootElement.GetProperty("requestToken").GetString());
            using(var hidden=await client.PutAsJsonAsync("/api/v1/notifications/management/destinations/hidden",new NotificationManagedDestinationChangeRequest("v1",0,true,true)))
                Assert.Equal(HttpStatusCode.Forbidden,hidden.StatusCode);
            Assert.Null(await store.GetAsync("hidden",default));
            using(var invalid=await client.PutAsJsonAsync(route,new {candidateId="v1",expectedRevision=0,profileEnabled=true,credentialEnabled=true,secretReference="env:PATH"}))
                Assert.Equal(HttpStatusCode.BadRequest,invalid.StatusCode);
            using(var created=await client.PutAsJsonAsync(route,new NotificationManagedDestinationChangeRequest("v1",0,true,true)))
                Assert.Equal(HttpStatusCode.OK,created.StatusCode);
            using(var stale=await client.PutAsJsonAsync(route,new NotificationManagedDestinationChangeRequest("v1",0,true,false)))
                Assert.Equal(HttpStatusCode.Conflict,stale.StatusCode);
            using(var revoked=await client.PutAsJsonAsync(route,new NotificationManagedDestinationChangeRequest("v1",1,true,false)))
                Assert.Equal(HttpStatusCode.OK,revoked.StatusCode);
            var state=await store.GetAsync("ops",default);Assert.NotNull(state);Assert.Equal(2,state.Revision);Assert.False(state.CredentialEnabled);
            var rows=await store.ReadAuditAsync("ops",0,50,default);Assert.Equal(2,rows.Count);
            Assert.All(rows,row=>Assert.Equal("oidc:https://idp.example|operator",row.Actor));
            Assert.Equal(2,audit.Events.Count(e=>e.EventType==SecurityAuditEventType.NotificationManagementChanged));
            var output=await client.GetStringAsync(route+"/audit");
            Assert.Contains("actorFingerprint",output);Assert.DoesNotContain("oidc:",output);Assert.DoesNotContain("env:",output);
            Assert.DoesNotContain("https://hooks.example.org",output);
            using(var hiddenAudit=await client.GetAsync("/api/v1/notifications/management/destinations/hidden/audit"))
                Assert.Equal(HttpStatusCode.Forbidden,hiddenAudit.StatusCode);
            await app.StopAsync();
        }
        finally { SqliteConnection.ClearAllPools();Directory.Delete(directory,true); }
    }
    private static WebApplicationBuilder Builder(AuthorizationAction action,string? resource,bool authenticated,out AuditSink audit)
    {
        var policy=new AuthorizationPolicyDefinition([new("manager",[new(action,ResourcePatterns:resource is null?[]:[resource])])],
            [new("https://idp.example","operator",["manager"])],[]);
        var builder=WebApplication.CreateBuilder();builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(new KafdeckAuthorizationService(new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[]),
            new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(policy))));
        builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
        audit=new AuditSink();builder.Services.AddSingleton<ISecurityAuditSink>(audit);
        var source=new NotificationDestinationProfile("source",NotificationProviderKind.Webhook,"Source",[NotificationEventClass.Operational],new Uri("https://hooks.example.org/notify"));
        builder.Services.AddSingleton(new NotificationGovernanceOptions([new("v1","ops",source,null),new("v1","hidden",source,null)]));
        return builder;
    }
    private static ClaimsPrincipal Principal(bool authenticated)=>authenticated?OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity(
        [new Claim("sub","operator")],"oidc")),"https://idp.example",null,DateTimeOffset.UtcNow):new ClaimsPrincipal();
    private static void Install(WebApplication app,ClaimsPrincipal principal)
    { app.UseRouting();app.Use((context,next)=>{context.User=principal;return next(context);});app.UseAntiforgery();app.MapKafdeckAntiforgeryEndpoint();app.MapKafdeckNotificationGovernance(); }
    private sealed class AuditSink:ISecurityAuditSink
    { internal ConcurrentBag<SecurityAuditEvent> Events {get;}=[];public ValueTask WriteAsync(SecurityAuditEvent value,CancellationToken token=default){Events.Add(value);return ValueTask.CompletedTask;} }
    private sealed class ReadSpy:INotificationGovernanceStore
    {
        internal int Reads;
        public Task InitializeAsync(CancellationToken token)=>Task.CompletedTask;
        public Task<NotificationGovernanceState?> GetAsync(string id,CancellationToken token){Reads++;return Task.FromResult<NotificationGovernanceState?>(null);}
        public Task<NotificationGovernanceState?> ChangeAsync(NotificationGovernanceChange value,CancellationToken token)=>throw new InvalidOperationException();
        public Task<IReadOnlyList<NotificationGovernanceAudit>> ReadAuditAsync(string id,long after,int take,CancellationToken token)=>throw new InvalidOperationException();
    }
}
