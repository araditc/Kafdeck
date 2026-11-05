using System.Net;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Attribution;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

// Actual Kestrel/route/model binding with a labeled controlled identity fixture; genuine authentication is tested separately.
public sealed class V09W76AttributionHttpTests
{
    [Fact]
    public async Task SQLite_actual_HTTP_topic_visibility_precedes_measurement_paging_continuation_and_totals()
    {
        var path=Path.Combine(Path.GetTempPath(),"w76-http-topic-visibility-"+Guid.NewGuid().ToString("N")+".db");
        try{await TopicVisibility(new SqliteMutationDbConnectionFactory(path));}
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }
    [G2PostgreSqlFact]
    public async Task PostgreSQL_actual_HTTP_topic_visibility_precedes_measurement_paging_continuation_and_totals()
    {
        await using var database=await G2CliPostgreSqlDatabase.CreateAsync();
        await TopicVisibility(new PostgreSqlMutationDbConnectionFactory(database.ConnectionString));
    }
    private static async Task TopicVisibility(IMutationDbConnectionFactory factory)
    {
        using var timeout=new CancellationTokenSource(TimeSpan.FromMinutes(1));var token=timeout.Token;
        var rates=new AdoAttributionStore(factory);await rates.InitializeAsync(token);
        var catalog=new AdoCatalogStore(factory,"http-topic-visibility-fixture");await catalog.InitializeAsync(token);
        var operations=new AdoMutationOperationRepository(factory);await operations.InitializeAsync(token);
        var start=DateTimeOffset.UtcNow.AddMinutes(-3);var end=start.AddMinutes(1);
        MeasurementWindow Window(char id,string resource,string quantity)=>new(new string(id,64),"visibility-fixture","alpha","controlled-store-fixture",
            "trafficBytes","bytes",resource,start,end,end.AddSeconds(1),quantity,"Observed",[],new(1,new string('a',32),null,new string('b',64),"Observed"),new string(id,64));
        await rates.AppendAsync([Window('1',"hidden-front","999991"),Window('2',"visible-one","3"),Window('3',"hidden-middle","999993"),Window('4',"visible-two","7"),Window('5',"hidden-tail","999995")],token);
        // Trusted fixture seeding exercises real repositories/routes, not activation authorization.
        var card=await rates.StageRateAsync("alpha",new("trafficBytes","bytes","USD","1",start.AddMinutes(-1),end.AddMinutes(1),[]),"trusted-visibility-fixture",token);
        await rates.CommitRateAsync(V09W76AttributionTests.Operation(MutationOperationKind.RateCardVersion,card),card.VersionId,card.ContentHash,0,false,()=>true,token);
        await rates.CommitRateAsync(V09W76AttributionTests.Operation(MutationOperationKind.RateCardActivation,card,0,true),card.VersionId,card.ContentHash,0,true,()=>true,token);
        AuthorizationPermissionDefinition[] Common()=>[new(AuthorizationAction.AttributionRead,["alpha"],["*"]),new(AuthorizationAction.CatalogRead,["alpha"],["*"])];
        var compiled=AuthorizationPolicyCompiler.Compile(new([
            new("without-topic",Common()),new("visible-topic",Common().Append(new(AuthorizationAction.TopicRead,["alpha"],["visible-one","visible-two"])).ToArray())],
            [new("https://idp.example","without-topic",["without-topic"]),new("https://idp.example","visible-topic",["visible-topic"])],[]));
        var options=new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[],Administration:new(new MutationOptions(true,null,null,TimeSpan.FromMinutes(5),2)));
        var auth=new KafdeckAuthorizationService(options,new AuthorizationPolicyEvaluator(compiled));
        ClaimsPrincipal Principal(string subject)=>OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub",subject)],"oidc")),"https://idp.example",null,DateTimeOffset.UtcNow);
        var hiddenPrincipal=Principal("without-topic");var visiblePrincipal=Principal("visible-topic");
        var authorization=new MutationRequestAuthorizationService(auth);var admission=new MutationAdmissionService(operations,authorization,options);
        var builder=WebApplication.CreateBuilder(new WebApplicationOptions{EnvironmentName="Production"});builder.WebHost.UseKestrel().UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(auth);builder.Services.AddSingleton<IAttributionStore>(rates);builder.Services.AddSingleton(rates);builder.Services.AddSingleton<ICatalogStore>(catalog);
        builder.Services.AddSingleton<IMutationOperationRepository>(operations);builder.Services.AddSingleton(admission);builder.Services.AddSingleton<IMutationAuditSink>(new NoAudit());
        builder.Services.AddSingleton<IDataProtectionProvider>(new EphemeralDataProtectionProvider());builder.Services.AddSingleton(authorization);builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
        await using var app=builder.Build();app.UseRouting();app.Use((context,next)=>{context.User=context.Request.Headers["X-Visibility-Fixture"]=="visible"?visiblePrincipal:hiddenPrincipal;return next(context);});
        app.UseAntiforgery();app.MapKafdeckAntiforgeryEndpoint();app.MapKafdeckAttribution();await app.StartAsync(token);
        using var client=new HttpClient(new SocketsHttpHandler{UseProxy=false}){BaseAddress=new Uri(Assert.Single(app.Urls)),Timeout=TimeSpan.FromSeconds(10)};
        async Task<JsonElement> Read(string family,int size,string? cursor=null,string? resource=null)
        {
            var query="clusterId=alpha&fromUtc="+Uri.EscapeDataString(start.ToString("O",System.Globalization.CultureInfo.InvariantCulture))+"&toUtc="+Uri.EscapeDataString(end.ToString("O",System.Globalization.CultureInfo.InvariantCulture))+"&pageSize="+size;
            if(cursor is not null)query+="&cursor="+Uri.EscapeDataString(cursor);if(resource is not null)query+="&resourceId="+Uri.EscapeDataString(resource);
            using var response=await client.GetAsync("/api/v1/governance/"+family+"?"+query,token);Assert.Equal(HttpStatusCode.OK,response.StatusCode);
            var body=await response.Content.ReadAsStringAsync(token);
            foreach(var secret in new[]{"hidden-front","hidden-middle","hidden-tail","999991","999993","999995"})Assert.DoesNotContain(secret,body,StringComparison.Ordinal);
            using var parsed=JsonDocument.Parse(body);return parsed.RootElement.Clone();
        }
        static string Resource(string family,JsonElement item)=>(family=="measurements"?item:item.GetProperty("measurement")).GetProperty("resourceId").GetString()!;
        foreach(var family in new[]{"measurements","attribution","dashboards"})
        {
            // AttributionRead plus all catalog-owner grants cannot disclose even topic names or a hidden-row sentinel.
            var denied=await Read(family,1);Assert.Empty(denied.GetProperty("items").EnumerateArray());Assert.Equal(JsonValueKind.Null,denied.GetProperty("nextCursor").ValueKind);Assert.Equal("Missing",denied.GetProperty("coverage").GetString());
            if(family!="measurements"){var total=denied.GetProperty("totals");Assert.Equal(JsonValueKind.Null,total.GetProperty("roundedAmount").ValueKind);Assert.Equal(JsonValueKind.Null,total.GetProperty("currency").ValueKind);Assert.False(total.GetProperty("complete").GetBoolean());}
            var filtered=await Read(family,1,resource:"hidden-front");Assert.Empty(filtered.GetProperty("items").EnumerateArray());Assert.Equal(JsonValueKind.Null,filtered.GetProperty("nextCursor").ValueKind);
            client.DefaultRequestHeaders.Add("X-Visibility-Fixture","visible");
            var first=await Read(family,1);Assert.Equal("visible-one",Resource(family,Assert.Single(first.GetProperty("items").EnumerateArray())));var cursor=first.GetProperty("nextCursor").GetString();Assert.False(string.IsNullOrEmpty(cursor));
            var second=await Read(family,1,cursor);Assert.Equal("visible-two",Resource(family,Assert.Single(second.GetProperty("items").EnumerateArray())));Assert.Equal(JsonValueKind.Null,second.GetProperty("nextCursor").ValueKind);
            var combined=await Read(family,200);Assert.Equal(2,combined.GetProperty("items").GetArrayLength());Assert.Equal(JsonValueKind.Null,combined.GetProperty("nextCursor").ValueKind);
            if(family!="measurements")
            {
                Assert.Equal("3",first.GetProperty("totals").GetProperty("roundedAmount").GetString());Assert.Equal("7",second.GetProperty("totals").GetProperty("roundedAmount").GetString());
                Assert.Equal("10",combined.GetProperty("totals").GetProperty("roundedAmount").GetString());
                var exact=await Read(family,200,resource:"visible-one");Assert.Equal("3",exact.GetProperty("totals").GetProperty("roundedAmount").GetString());Assert.True(exact.GetProperty("totals").GetProperty("complete").GetBoolean());
            }
            client.DefaultRequestHeaders.Remove("X-Visibility-Fixture");
        }
        await app.StopAsync(token);
    }
    [Fact]
    public async Task Null_documents_and_nested_values_return_HTTP400_without_staging_rate_versions_or_disclosing_exceptions()
    {
        var path=Path.Combine(Path.GetTempPath(),"w76-http-"+Guid.NewGuid().ToString("N")+".db");
        try
        {
            var factory=new SqliteMutationDbConnectionFactory(path);var rates=new AdoAttributionStore(factory);await rates.InitializeAsync();
            var catalog=new AdoCatalogStore(factory,"http-fixture");await catalog.InitializeAsync(CancellationToken.None);
            var operations=new AdoMutationOperationRepository(factory);await operations.InitializeAsync();
            var policy=AuthorizationPolicyCompiler.Compile(new([new("fixture",new[]{AuthorizationAction.RateCardManage,AuthorizationAction.AttributionRead,AuthorizationAction.CatalogRead}.Select(a=>new AuthorizationPermissionDefinition(a,["alpha"],["*"])).ToArray())],[new("https://idp.example","operator",["fixture"])],[]));
            var options=new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[],Administration:new(new MutationOptions(true,null,null,TimeSpan.FromMinutes(5),2)));
            var auth=new KafdeckAuthorizationService(options,new AuthorizationPolicyEvaluator(policy));
            var principal=OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub","operator")],"oidc")),"https://idp.example",null,DateTimeOffset.UtcNow);
            var admission=new MutationAdmissionService(operations,new MutationRequestAuthorizationService(auth),options);
            var builder=WebApplication.CreateBuilder(new WebApplicationOptions{EnvironmentName="Production"});builder.WebHost.UseKestrel().UseUrls("http://127.0.0.1:0");
            builder.Services.AddSingleton(auth);builder.Services.AddSingleton<IAttributionStore>(rates);builder.Services.AddSingleton(rates);builder.Services.AddSingleton<ICatalogStore>(catalog);
            builder.Services.AddSingleton<IMutationOperationRepository>(operations);builder.Services.AddSingleton(admission);builder.Services.AddSingleton<IMutationAuditSink>(new NoAudit());builder.Services.AddSingleton<IDataProtectionProvider>(new EphemeralDataProtectionProvider());
            builder.Services.AddSingleton(new MutationRequestAuthorizationService(auth));
            builder.Services.AddKafdeckAntiforgery("http://127.0.0.1:0");
            await using var app=builder.Build();app.UseRouting();app.Use((h,next)=>{h.User=principal;return next(h);});app.UseAntiforgery();app.MapKafdeckAntiforgeryEndpoint();app.MapKafdeckAttribution();await app.StartAsync();
            using var client=new HttpClient(new SocketsHttpHandler{UseProxy=false}){BaseAddress=new Uri(Assert.Single(app.Urls)),Timeout=TimeSpan.FromSeconds(10)};
            using(var csrf=JsonDocument.Parse(await client.GetStringAsync("/api/v1/auth/csrf")))client.DefaultRequestHeaders.Add(KafdeckAntiforgeryExtensions.HeaderName,csrf.RootElement.GetProperty("requestToken").GetString());
            var now=DateTimeOffset.UtcNow;var valid=new{metricKind="trafficBytes",unit="bytes",currency="USD",amount="0.00001",effectiveFromUtc=now,effectiveToUtc=now.AddDays(1),weights=Array.Empty<object>()};
            var cases=new object?[]{null,
                new{metricKind="trafficBytes",unit="bytes",currency=(string?)null,amount="1",effectiveFromUtc=now,effectiveToUtc=now.AddDays(1),weights=Array.Empty<object>()},
                new{metricKind="trafficBytes",unit="bytes",currency="USD",amount=(string?)null,effectiveFromUtc=now,effectiveToUtc=now.AddDays(1),weights=Array.Empty<object>()},
                new{metricKind="trafficBytes",unit="bytes",currency="USD",amount="1",effectiveFromUtc=now,effectiveToUtc=now.AddDays(1),weights=new object?[]{null}},
                new{metricKind="trafficBytes",unit="bytes",currency="USD",amount="1",effectiveFromUtc=now,effectiveToUtc=now.AddDays(1),weights=(object[]?)null}};
            for(var index=0;index<cases.Length;index++)
            {
                var document=cases[index];
                using var response=await client.PostAsync("/api/v1/governance/rate-cards/versions/preview",new StringContent(JsonSerializer.Serialize(new{clusterId="alpha",document}),Encoding.UTF8,"application/json"));
                Assert.True(response.StatusCode==HttpStatusCode.BadRequest,"Malformed field case "+index+" returned HTTP "+(int)response.StatusCode);var body=await response.Content.ReadAsStringAsync();Assert.DoesNotContain("NullReferenceException",body,StringComparison.Ordinal);Assert.DoesNotContain("StackTrace",body,StringComparison.Ordinal);
            }
            _=valid;
            await using var db=await factory.OpenAsync();await using var count=db.CreateCommand();count.CommandText="SELECT COUNT(*) FROM attribution_rate_versions";Assert.Equal(0,Convert.ToInt32(await count.ExecuteScalarAsync()));
            await app.StopAsync();
        }
        finally{SqliteConnection.ClearAllPools();File.Delete(path);File.Delete(path+"-wal");File.Delete(path+"-shm");}
    }
    private sealed class NoAudit:IMutationAuditSink{public ValueTask WriteAsync(MutationAuditEvent entry,CancellationToken cancellationToken=default)=>ValueTask.CompletedTask;}
}
