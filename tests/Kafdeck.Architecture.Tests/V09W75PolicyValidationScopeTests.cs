using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V09W75PolicyValidationScopeTests
{
    [Fact]
    public async Task Actual_validation_HTTP_accepts_scoped_reader_without_global_grant_and_rejects_scope_widening()
    {
        var policy=AuthorizationPolicyCompiler.Compile(new([
            new("reader",[new(AuthorizationAction.GovernancePolicyRead,["alpha"],["*"])])],
            [new("https://idp.example","operator",["reader"])],[]));
        var evaluator=new AuthorizationPolicyEvaluator(policy);
        var service=new KafdeckAuthorizationService(new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0",null,AccessMode.Oidc,null),[]),evaluator);
        var principal=OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub","operator")],"oidc")),"https://idp.example",null,DateTimeOffset.UtcNow);
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden,service.AuthorizeCollection(principal,AuthorizationAction.GovernancePolicyRead,null));
        var builder=WebApplication.CreateBuilder(new WebApplicationOptions{EnvironmentName="Production"});
        builder.WebHost.UseKestrel().UseUrls("http://127.0.0.1:0");builder.Services.AddSingleton(service);
        await using var app=builder.Build();app.UseRouting();app.Use((context,next)=>{context.User=principal;return next(context);});
        app.MapKafdeckPolicyValidation();await app.StartAsync();
        using var client=new HttpClient(new SocketsHttpHandler{UseProxy=false}){BaseAddress=new Uri(Assert.Single(app.Urls)),Timeout=TimeSpan.FromSeconds(10)};
        const string empty="{\"schemaVersion\":1,\"rules\":[]}";
        using(var scoped=await client.PostAsJsonAsync("/api/v1/governance/policies/validate",new PolicyDocumentRequest("alpha",empty)))
        {
            Assert.Equal(HttpStatusCode.OK,scoped.StatusCode);
            using var body=JsonDocument.Parse(await scoped.Content.ReadAsStringAsync());Assert.True(body.RootElement.GetProperty("valid").GetBoolean());
            Assert.Equal(64,body.RootElement.GetProperty("contentHash").GetString()!.Length);
        }
        // Authorization is evaluated before compiling caller material: hidden-scope input cannot probe anything.
        using(var hidden=await client.PostAsJsonAsync("/api/v1/governance/policies/validate",new PolicyDocumentRequest("hidden","not-json")))
            Assert.Equal(HttpStatusCode.Forbidden,hidden.StatusCode);
        const string cross="""{"schemaVersion":1,"rules":[{"id":"deny","priority":1,"scope":{"clusterId":"hidden","resourceKind":"topic","resourceIds":["orders"],"action":"TopicDelete"},"conditions":[],"outcome":"Deny"}]}""";
        using(var widening=await client.PostAsJsonAsync("/api/v1/governance/policies/validate",new PolicyDocumentRequest("alpha",cross)))
            Assert.Equal(HttpStatusCode.BadRequest,widening.StatusCode);
        using(var sameScope=await client.PostAsJsonAsync("/api/v1/governance/policies/validate",new PolicyDocumentRequest("alpha",cross.Replace("hidden","alpha",StringComparison.Ordinal))))
            Assert.Equal(HttpStatusCode.OK,sameScope.StatusCode);
        using(var missing=await client.PostAsJsonAsync("/api/v1/governance/policies/validate",new{document=empty}))
            Assert.Equal(HttpStatusCode.BadRequest,missing.StatusCode);
        await app.StopAsync();
    }
}
