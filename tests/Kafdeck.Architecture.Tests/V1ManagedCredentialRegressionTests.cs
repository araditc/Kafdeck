using System.Net.Http.Json;
using System.Text.Json;
using Confluent.Kafka;
using Kafdeck.Api;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Internal;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedCredentialRegressionTests
{
    [Fact]
    public async Task Test_register_and_open_use_the_same_session_lease_without_persisting_Kafka_secrets()
    {
        await using var f=await V1ManagedWorkflowLab.Start();
        var draft=new {bootstrapServers=new[]{"127.0.0.1:19092"},protocol="SASL_PLAINTEXT",mechanism="PLAIN",username="lease-user",password="lease-secret-123!",name="Lease target"};
        using var tested=await f.Http.PostAsJsonAsync("/api/v1/connections/test",draft);Assert.True(tested.IsSuccessStatusCode);Assert.Null(f.Store.Read().Connections);
        using var saved=await f.Http.PostAsJsonAsync("/api/v1/connections/targets",draft);Assert.True(saved.IsSuccessStatusCode);
        using var body=JsonDocument.Parse(await saved.Content.ReadAsStringAsync());var id=body.RootElement.GetProperty("clusterId").GetString()!;
        Assert.Equal(id,Assert.Single(f.Runtime.IncludeRegistered([])).Id);
        f.Requests.HttpContext=new DefaultHttpContext{User=f.Actor};
        var producer=f.Runtime.GetProducerConfig(id);Assert.NotNull(producer);Assert.Equal("lease-secret-123!",producer.SaslPassword);
        var consumer=f.Runtime.GetConsumerConfig(id);Assert.NotNull(consumer);Assert.False(consumer.EnableAutoCommit);Assert.False(consumer.EnableAutoOffsetStore);Assert.False(consumer.AllowAutoCreateTopics);
        var disk=File.ReadAllText(f.Identity.StatePath);Assert.DoesNotContain("lease-secret-123!",disk,StringComparison.Ordinal);Assert.DoesNotContain("lease-user",disk,StringComparison.Ordinal);
        Assert.Equal(2,f.MetadataProbes);
    }

    [Fact]
    public async Task Secured_lease_expires_at_thirty_minutes_without_a_wall_clock_wait()
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();
        var clock=new LeaseClock();using var cache=new MemoryCache(new MemoryCacheOptions{SizeLimit=128,Clock=clock});
        using var runtime=new RegisteredKafkaRuntime(f.Requests,f.Store,f.Configuration,cache);
        var context=new DefaultHttpContext{User=f.Actor};f.Requests.HttpContext=context;runtime.Remember(context,"connection-a",Config());
        clock.UtcNow=clock.UtcNow.AddMinutes(29);Assert.NotNull(runtime.GetProducerConfig("connection-a"));
        clock.UtcNow=clock.UtcNow.AddMinutes(1);Assert.Equal("locked",runtime.SnapshotScope("connection-a"));Assert.Null(runtime.GetConsumerConfig("connection-a"));Assert.Null(runtime.GetProducerConfig("connection-a"));
    }

    [Fact]
    public async Task Retest_rotates_snapshot_scope_and_a_new_session_cannot_use_the_old_lease()
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();var original=new DefaultHttpContext{User=f.Actor};f.Requests.HttpContext=original;
        f.Runtime.Remember(original,"connection-a",Config());var before=f.Runtime.SnapshotScope("connection-a");Assert.NotEqual("locked",before);
        f.Runtime.Remember(original,"connection-a",Config("replacement-secret"));Assert.NotEqual(before,f.Runtime.SnapshotScope("connection-a"));Assert.Equal("replacement-secret",f.Runtime.GetProducerConfig("connection-a")!.SaslPassword);
        f.Requests.HttpContext=new DefaultHttpContext{User=V1ManagedWorkflowLab.Browser(f.Identity.OwnerId)};
        Assert.Equal("locked",f.Runtime.SnapshotScope("connection-a"));Assert.Null(f.Runtime.GetProducerConfig("connection-a"));
        f.Requests.HttpContext=original;Assert.NotNull(f.Runtime.GetProducerConfig("connection-a"));Assert.Null(f.Runtime.GetProducerConfig("connection-b"));
    }

    [Fact]
    public async Task Restart_retains_secured_target_definition_but_not_its_in_memory_secret()
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();var c=new DefaultHttpContext{User=f.Actor};f.Requests.HttpContext=c;f.Runtime.Remember(c,"connection-a",Config());
        using var restarted=new RegisteredKafkaRuntime(f.Requests,new ManagedIdentityStore(f.Identity.StatePath,ManagedProvisioningFixture.Issuer),f.Configuration);
        Assert.Equal(2,restarted.IncludeRegistered([]).Count);Assert.Equal("locked",restarted.SnapshotScope("connection-a"));Assert.Null(restarted.GetProducerConfig("connection-a"));
        Assert.NotNull(f.Runtime.GetProducerConfig("connection-a"));
    }

    [Fact]
    public async Task A_retained_lease_never_overrides_current_cluster_or_session_revocation()
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets();var id=f.AddUser();var c=new DefaultHttpContext{User=V1ManagedWorkflowLab.Browser(id)};f.Requests.HttpContext=c;f.Runtime.Remember(c,"connection-a",Config());
        Assert.NotNull(f.Runtime.GetProducerConfig("connection-a"));
        f.ChangeUser(id,u=>u with {ClusterIds=[]});Assert.Null(f.Runtime.GetProducerConfig("connection-a"));
        f.ChangeUser(id,u=>u with {ClusterIds=["connection-a"],SessionsValidFrom=DateTimeOffset.UtcNow.AddMinutes(1)});Assert.Null(f.Runtime.GetConsumerConfig("connection-a"));
    }

    [Fact]
    public async Task Plaintext_target_reopens_without_a_secret_but_still_requires_current_authority_and_CIDR()
    {
        await using var f=await V1ManagedWorkflowLab.Start();f.AddTargets("PLAINTEXT");var id=f.AddUser();f.Requests.HttpContext=new DefaultHttpContext{User=V1ManagedWorkflowLab.Browser(id)};
        Assert.NotNull(f.Runtime.GetConsumerConfig("connection-a"));Assert.Null(f.Runtime.GetConsumerConfig("connection-b"));
        var state=f.Store.Read();f.Store.Write(state with {Connections=state.Connections!.Select(t=>t.Id=="connection-a"?t with {BootstrapServers=["192.0.2.9:9092"]}:t).ToArray()});
        Assert.Null(f.Runtime.GetProducerConfig("connection-a"));
    }

    private static AdminClientConfig Config(string secret="lease-secret-123!")=>new(){BootstrapServers="127.0.0.1:19092",SecurityProtocol=SecurityProtocol.SaslPlaintext,SaslMechanism=SaslMechanism.Plain,SaslUsername="lease-user",SaslPassword=secret};
    private sealed class LeaseClock : ISystemClock {public DateTimeOffset UtcNow{get;set;}=DateTimeOffset.UtcNow;}
}
