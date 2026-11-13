using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Internal;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1CredentialCapacityTests
{
    [Fact]
    public async Task Default_capacity_admits_exactly_128_leases_and_rejects_the_next_without_eviction()
    {
        await using var f = await V1ManagedWorkflowLab.Start(); f.AddTargets();
        var contexts = Enumerable.Range(0,129).Select(_ => new DefaultHttpContext { User = V1ManagedWorkflowLab.Browser(f.Identity.OwnerId) }).ToArray();
        var admitted = contexts.Select(c => f.Runtime.Remember(c,"connection-a",Config())).ToArray();
        Assert.Equal(128, admitted.Count(x=>x)); Assert.False(admitted[^1]);
        foreach (var context in contexts.Take(128))
        { f.Requests.HttpContext=context; Assert.NotNull(f.Runtime.GetProducerConfig("connection-a")); }
    }

    [Fact]
    public async Task Concurrent_sessions_cannot_report_more_admitted_leases_than_capacity()
    {
        await using var f = await V1ManagedWorkflowLab.Start(credentials:new MemoryCache(new MemoryCacheOptions {SizeLimit=2})); f.AddTargets();
        var contexts=Enumerable.Range(0,8).Select(_=>new DefaultHttpContext {User=V1ManagedWorkflowLab.Browser(f.Identity.OwnerId)}).ToArray();
        var admitted=await Task.WhenAll(contexts.Select(c=>Task.Run(()=>f.Runtime.Remember(c,"connection-a",Config()))));
        Assert.Equal(2,admitted.Count(x=>x));
        for(var i=0;i<contexts.Length;i++)
        { f.Requests.HttpContext=contexts[i];Assert.Equal(admitted[i],f.Runtime.GetProducerConfig("connection-a") is not null); }
    }

    [Fact]
    public async Task Retesting_a_full_cache_rotates_its_existing_lease_and_expiration_frees_capacity()
    {
        var clock=new LeaseClock();
        await using var f=await V1ManagedWorkflowLab.Start(credentials:new MemoryCache(new MemoryCacheOptions {SizeLimit=1,Clock=clock}));f.AddTargets();
        var first=new DefaultHttpContext {User=f.Actor}; f.Requests.HttpContext=first;
        Assert.True(f.Runtime.Remember(first,"connection-a",Config()));var before=f.Runtime.SnapshotScope("connection-a");
        Assert.True(f.Runtime.Remember(first,"connection-a",Config()));Assert.NotEqual(before,f.Runtime.SnapshotScope("connection-a"));
        clock.UtcNow+=TimeSpan.FromMinutes(30);Assert.Null(f.Runtime.GetProducerConfig("connection-a"));
        var next=new DefaultHttpContext {User=V1ManagedWorkflowLab.Browser(f.Identity.OwnerId)};
        Assert.True(f.Runtime.Remember(next,"connection-a",Config()));
    }

    [Fact]
    public async Task Full_credential_cache_cannot_report_a_usable_registered_connection()
    {
        await using var f = await V1ManagedWorkflowLab.Start(credentials: new MemoryCache(new MemoryCacheOptions { SizeLimit = 2 }));
        f.AddTargets();
        for (var i = 0; i < 2; i++)
        {
            var actor = V1ManagedWorkflowLab.Browser(f.Identity.OwnerId);
            f.Runtime.Remember(new DefaultHttpContext { User = actor }, "connection-a", Config());
        }
        using var response = await f.Http.PostAsJsonAsync("/api/v1/connections/test", new
        {
            clusterId = "connection-a", bootstrapServers = new[] { "127.0.0.1:19092" },
            protocol = "SASL_PLAINTEXT", mechanism = "PLAIN", username = "capacity-user", password = "capacity-test-secret!"
        });
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.EndsWith(":connection-credential-capacity", body.RootElement.GetProperty("type").GetString());
        Assert.Equal("connection-a", body.RootElement.GetProperty("clusterId").GetString());
        f.Requests.HttpContext = new DefaultHttpContext { User = f.Actor };
        Assert.Null(f.Runtime.GetProducerConfig("connection-a"));
    }

    [Fact]
    public async Task Registration_capacity_failure_honestly_retains_the_saved_definition_without_a_secret()
    {
        await using var f=await V1ManagedWorkflowLab.Start(credentials:new MemoryCache(new MemoryCacheOptions {SizeLimit=1}));f.AddTargets();
        Assert.True(f.Runtime.Remember(new DefaultHttpContext {User=V1ManagedWorkflowLab.Browser(f.Identity.OwnerId)},"connection-a",Config()));
        using var response=await f.Http.PostAsJsonAsync("/api/v1/connections/targets",new
        {name="Capacity saved",bootstrapServers=new[]{"127.0.0.1:19094"},protocol="SASL_PLAINTEXT",mechanism="PLAIN",username="capacity-user",password="capacity-test-secret!"});
        Assert.Equal(HttpStatusCode.Conflict,response.StatusCode);
        using var body=JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.True(body.RootElement.GetProperty("targetSaved").GetBoolean());var id=body.RootElement.GetProperty("clusterId").GetString();
        Assert.Contains(f.Store.Read().Connections!,target=>target.Id==id&&target.Name=="Capacity saved");
        Assert.DoesNotContain("capacity-test-secret!",File.ReadAllText(f.Identity.StatePath),StringComparison.Ordinal);
        Assert.Single(f.Identity.Audit.Events);
    }

    private static AdminClientConfig Config() => new()
    {
        BootstrapServers = "127.0.0.1:19092", SecurityProtocol = SecurityProtocol.SaslPlaintext,
        SaslMechanism = SaslMechanism.Plain, SaslUsername = "capacity-user", SaslPassword = "capacity-test-secret!"
    };
    private sealed class LeaseClock : ISystemClock { public DateTimeOffset UtcNow {get;set;}=DateTimeOffset.UtcNow; }
}
