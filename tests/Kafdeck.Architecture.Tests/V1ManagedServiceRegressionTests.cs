using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using Kafdeck.Api;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedServiceRegressionTests
{
    [Theory]
    [InlineData("schema-registry")]
    [InlineData("kafka-connect")]
    [InlineData("ksqldb")]
    public async Task Non_owner_cannot_read_test_or_save_service_settings_or_reach_the_provider(string slug)
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();var id=f.AddUser();f.Actor=V1ManagedWorkflowLab.Browser(id);await f.Csrf();
        var builder=WebApplication.CreateBuilder();builder.WebHost.UseUrls("http://127.0.0.1:0");await using var provider=builder.Build();var calls=0;
        provider.MapGet("/{**path}",()=>{Interlocked.Increment(ref calls);return Results.Ok(new{});});await provider.StartAsync();
        try
        {
            var path="/api/v1/connections/connection-a/"+slug;var input=new {url=provider.Urls.Single(),authentication="none"};
            using var read=await f.Http.GetAsync(path);using var test=await f.Http.PostAsJsonAsync(path+"/test",input);using var save=await f.Http.PutAsJsonAsync(path,input);
            Assert.Equal(HttpStatusCode.Forbidden,read.StatusCode);Assert.Equal(HttpStatusCode.Forbidden,test.StatusCode);Assert.Equal(HttpStatusCode.Forbidden,save.StatusCode);
            Assert.Equal(0,calls);var target=f.Store.Read().Connections!.Single(t=>t.Id=="connection-a");Assert.Null(target.SchemaRegistry);Assert.Null(target.Connect);Assert.Null(target.KsqlDb);
            Assert.Empty(f.Identity.Audit.Events);
        }
        finally{await provider.StopAsync();}
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Protected_credentials_survive_new_provider_instances_only_with_the_same_keyring_target_and_service(int kind)
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();
        var directory=new DirectoryInfo(Path.Combine(f.Identity.DirectoryPath,"keyring"));
        var keys=DataProtectionProvider.Create(directory,b=>b.SetApplicationName("g04-fixture"));
        var runtime=new RegisteredHttpServiceRuntime(f.Store,f.Requests,keys,f.Configuration);var service=(RegisteredHttpService)kind;
        var saved=runtime.Prepare("connection-a",service,new("http://127.0.0.1:19093/","basic","purpose-user","purpose-secret",null),null);Assert.NotNull(saved);
        var state=f.Store.Read();f.Store.Write(state with {Connections=state.Connections!.Select(t=>t.Id=="connection-a"?service switch{
            RegisteredHttpService.SchemaRegistry=>t with {SchemaRegistry=saved},RegisteredHttpService.KafkaConnect=>t with {Connect=saved},_=>t with {KsqlDb=saved}}:t).ToArray()});
        Assert.DoesNotContain("purpose-secret",File.ReadAllText(f.Identity.StatePath),StringComparison.Ordinal);
        var reopenedKeys=DataProtectionProvider.Create(directory,b=>b.SetApplicationName("g04-fixture"));
        var reopened=new RegisteredHttpServiceRuntime(new ManagedIdentityStore(f.Identity.StatePath,ManagedProvisioningFixture.Issuer),f.Requests,reopenedKeys,f.Configuration);
        using var client=reopened.Open("connection-a",service,saved);Assert.Equal("Basic",client.DefaultRequestHeaders.Authorization!.Scheme);
        Assert.Throws<CryptographicException>(()=>reopened.Open("connection-b",service,saved));
        Assert.Throws<CryptographicException>(()=>reopened.Open("connection-a",(RegisteredHttpService)((kind+1)%3),saved));
        var missing=new RegisteredHttpServiceRuntime(f.Store,f.Requests,new EphemeralDataProtectionProvider(),f.Configuration);
        Assert.Throws<CryptographicException>(()=>missing.Open("connection-a",service,saved));
    }

    [Fact]
    public async Task Unreadable_saved_service_credential_fails_closed_without_reusing_or_clearing_it()
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();
        var runtime=new RegisteredHttpServiceRuntime(f.Store,f.Requests,new EphemeralDataProtectionProvider(),f.Configuration);
        var saved=runtime.Prepare("connection-a",RegisteredHttpService.SchemaRegistry,new("http://127.0.0.1:19093/","bearer",null,"scoped-secret",null),null)!;
        var state=f.Store.Read();f.Store.Write(state with {Connections=state.Connections!.Select(t=>t.Id=="connection-a"?t with {SchemaRegistry=saved}:t).ToArray()});
        var lostKeys=new RegisteredHttpServiceRuntime(f.Store,f.Requests,new EphemeralDataProtectionProvider(),f.Configuration);
        f.Requests.HttpContext=new DefaultHttpContext{User=f.Actor};Assert.Null(lostKeys.OpenRegistry("connection-a"));
        Assert.Equal(saved,f.Store.Read().Connections!.Single(t=>t.Id=="connection-a").SchemaRegistry);
    }
}
