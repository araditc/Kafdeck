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
