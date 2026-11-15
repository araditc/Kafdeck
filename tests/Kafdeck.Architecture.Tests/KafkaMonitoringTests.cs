using System.Security.Cryptography;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Kafdeck.Api;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaMonitoringTests : IDisposable
{
    private readonly string _file = Path.Combine(Path.GetTempPath(), $"kafdeck-monitor-{Guid.NewGuid():N}.db");
    private readonly TestClock _clock = new();
    private readonly BrokerSource _source = new("a","broker-1",RandomNumberGenerator.GetBytes(32));
    private readonly MonitoringStore _store;
    private const string Metric = "kafka.network:RequestMetrics/LocalTimeMs/99thPercentile";

    public KafkaMonitoringTests() => _store = Open();
    private MonitoringStore Open() => new(new(_file, [_source]),new ConfigurationBuilder().Build(),NullLogger<MonitoringStore>.Instance,_clock);
    private static JsonElement Body(IResult result) => JsonSerializer.SerializeToElement(((IValueHttpResult)result).Value,
        new JsonSerializerOptions(JsonSerializerDefaults.Web));
    private static int? Status(IResult result) => ((IStatusCodeHttpResult)result).StatusCode;
    private static JmxPoint Point(double value) => new(Metric,new() {["request"]="Produce"},value);
    private IResult Ingest(params JmxPoint[] samples) => _store.Ingest(_source,new(false,Guid.NewGuid(),0,true,samples),default);
    private JsonElement Rows(string kind, string? cluster=null) => Body(_store.Read(cluster ?? "a",kind,null,null,60,null,null,default));
    private sealed class TestClock : TimeProvider
    {
        private DateTimeOffset _time = DateTimeOffset.FromUnixTimeSeconds(1_800_000_000);
        public override DateTimeOffset GetUtcNow() => _time;
        public void Advance(int seconds) => _time=_time.AddSeconds(seconds);
    }

    [Fact]
    public void Custom_threshold_survives_restart_tracks_hold_and_recovery_and_records_evidence()
    {
        var rule=new KafkaMetricSources.RuleChange("produce-latency",Metric,20,false,60,"warning",false);
        Assert.Equal(200,Status(_store.ChangeRule("a",rule)));
        Assert.Equal("produce-latency",Body(Open().Read("a","rules",null,null,60,null,null,default)).GetProperty("rows")[0].GetProperty("name").GetString());
        Assert.Equal(204,Status(Ingest(Point(25))));
        Assert.Equal("pending",Rows("alerts").GetProperty("rows")[0].GetProperty("state").GetString());
        _clock.Advance(60);
        Assert.Equal(204,Status(Ingest(Point(30))));
        Assert.Equal("firing",Rows("alerts").GetProperty("rows")[0].GetProperty("state").GetString());
        _clock.Advance(20);
        Assert.Equal(204,Status(Ingest(Point(20))));
        Assert.Empty(Rows("alerts").GetProperty("rows").EnumerateArray());
        var incident=Rows("incidents").GetProperty("rows")[0];
        Assert.Equal("recovered",incident.GetProperty("reason").GetString());
        Assert.Equal(30,incident.GetProperty("value").GetDouble());
        Assert.Equal("Produce",incident.GetProperty("labels").GetProperty("request").GetString());
        Assert.Equal(200,Status(_store.ChangeRule("a",rule with {Remove=true})));
        Assert.Empty(Rows("rules").GetProperty("rows").EnumerateArray());
        Assert.Equal(400,Status(_store.ChangeRule("a",rule with {Name="KafkaTargetDown"})));
    }

    [Fact]
    public void Agent_heartbeat_fires_and_recovers_without_a_browser()
    {
        _clock.Advance(119); _store.Maintain();
        Assert.Empty(Rows("alerts").GetProperty("rows").EnumerateArray());
        _clock.Advance(1); _store.Maintain();
        Assert.Equal("KafkaTargetDown",Rows("alerts").GetProperty("rows")[0].GetProperty("name").GetString());
        Assert.Equal(204,Status(Ingest(Point(1))));
        Assert.Equal("recovered",Rows("incidents").GetProperty("rows")[0].GetProperty("reason").GetString());
        Assert.Equal("unconfigured",Rows("alerts","b").GetProperty("state").GetString());
        _clock.Advance(86_500); _store.Maintain();
        Assert.DoesNotContain(Rows("incidents").GetProperty("rows").EnumerateArray(),r=>r.GetProperty("reason").GetString()=="recovered");
    }

    [Fact]
    public void Multipart_order_bounds_cluster_filters_and_sample_transaction_are_enforced()
    {
        var id=Guid.NewGuid();
        Assert.Equal(204,Status(_store.Ingest(_source,new(false,id,0,false,[Point(1)]),default)));
        Assert.True(Rows("metrics").GetProperty("warning").GetBoolean());
        Assert.Equal("partial",Rows("sources").GetProperty("rows")[0].GetProperty("health").GetString());
        Assert.Equal(409,Status(_store.Ingest(_source,new(false,id,2,true,[]),default)));
        Assert.Equal(400,Status(_store.Ingest(_source,new(false,id,1,true,[Point(4),Point(8)]),default)));
        Assert.Equal(204,Status(_store.Ingest(_source,new(false,id,1,true,[new("test:depth",[],9)]),default)));
        Assert.False(Rows("metrics").GetProperty("warning").GetBoolean());
        Assert.Equal("current",Rows("sources").GetProperty("rows")[0].GetProperty("health").GetString());
        Assert.Equal(409,Status(_store.Ingest(_source,new(false,id,1,true,[]),default)));
        Assert.Equal(429,Status(Ingest(Point(2))));
        var match=Body(_store.Read("a","series",Metric,null,60,"broker-1","{\"request\":\"Produce\"}",default));
        Assert.Equal(1,match.GetProperty("rows")[0].GetProperty("points")[0].GetProperty("value").GetDouble());
        Assert.Equal(400,Status(_store.Read("a","series",Metric,null,60,null,"{\"instance\":\"foreign\"}",default)));
    }

    [Fact]
    public async Task Managed_browser_enrollment_rules_and_cluster_read_permissions_use_normal_http_routes()
    {
        await using var f=await V1ManagedWorkflowLab.Start(monitoring:_store);
        f.AddTargets("PLAINTEXT");
        const string path="/api/v1/clusters/connection-a/monitoring/";
        using var enrolled=await f.Http.PostAsJsonAsync(path+"agents",new {instance="broker-live",action="create"});
        Assert.Equal(HttpStatusCode.OK,enrolled.StatusCode);
        var data=await enrolled.Content.ReadFromJsonAsync<JsonElement>();
        var rule=new KafkaMetricSources.RuleChange("latency",Metric,12,false,0,"warning",false);
        using var saved=await f.Http.PostAsJsonAsync(path+"rules",rule);
        Assert.Equal(HttpStatusCode.OK,saved.StatusCode);
        var packet=new JmxBatch(false,Guid.NewGuid(),0,true,[Point(25)]);
        using var post=new HttpRequestMessage(HttpMethod.Post,"/monitoring/ingest"){Content=JsonContent.Create(packet)};
        post.Headers.Add("X-Kafdeck-Agent-Token",data.GetProperty("token").GetString());
        using var accepted=await f.Http.SendAsync(post);
        Assert.Equal(HttpStatusCode.NoContent,accepted.StatusCode);
        var alerts=await f.Http.GetFromJsonAsync<JsonElement>(path+"alerts");
        Assert.Equal("latency",alerts.GetProperty("rows")[0].GetProperty("name").GetString());
        f.Http.DefaultRequestHeaders.Remove(KafdeckAntiforgeryExtensions.HeaderName);
        using var missing=await f.Http.PostAsJsonAsync(path+"rules",rule);
        Assert.False(missing.IsSuccessStatusCode);
        f.Actor=V1ManagedWorkflowLab.Browser(f.AddUser("operator")); await f.Csrf();
        using var visible=await f.Http.GetAsync(path+"metrics");
        Assert.Equal(HttpStatusCode.OK,visible.StatusCode);
        using var denied=await f.Http.PostAsJsonAsync(path+"rules",rule);
        Assert.Equal(HttpStatusCode.Forbidden,denied.StatusCode);
        using var other=await f.Http.GetAsync("/api/v1/clusters/connection-b/monitoring/alerts");
        Assert.Equal(HttpStatusCode.Forbidden,other.StatusCode);
        f.Actor=V1ManagedWorkflowLab.Browser(f.AddUser("viewer"));
        using var viewer=await f.Http.GetAsync(path+"series?metric=x");
        Assert.Equal(HttpStatusCode.Forbidden,viewer.StatusCode);
    }

    [Fact]
    public void Broker_cpu_gc_and_request_queue_rules_fire_after_their_hold_and_recover_at_threshold()
    {
        const string cpu = "kafdeck.jvm/cpu_percent";
        const string gc = "kafdeck.jvm/gc_time_percent";
        const string queue = "kafka.network:RequestMetrics/RequestQueueTimeMs/99thPercentile";
        var labels = new SortedDictionary<string, string> { ["request"] = "Produce" };
        var count = "kafka.network:RequestMetrics/RequestQueueTimeMs/Count";
        Ingest(new JmxPoint(count,labels,0));
        _clock.Advance(30);
        for (int seconds = 0; seconds <= 300; seconds += 30)
        {
            Assert.Equal(204, Status(Ingest(new(cpu, [], 86), new(gc, [], 11), new(queue, labels, 101),new(count,labels,seconds+1))));
            var alerts = Rows("alerts").GetProperty("rows").EnumerateArray().ToDictionary(
                row => row.GetProperty("name").GetString()!, row => row.GetProperty("state").GetString());
            Assert.Equal(seconds < 300 ? "pending" : "firing", alerts["KafkaBrokerCpuHigh"]);
            Assert.Equal(seconds < 120 ? "pending" : "firing", alerts["KafkaBrokerGcHigh"]);
            Assert.Equal(seconds < 120 ? "pending" : "firing", alerts["KafkaRequestQueueSlow"]);
            _clock.Advance(30);
        }
        Assert.Equal(204, Status(Ingest(new(cpu, [], 85), new(gc, [], 10), new(queue, labels, 100),new(count,labels,332))));
        Assert.Empty(Rows("alerts").GetProperty("rows").EnumerateArray());
        var incidents = Rows("incidents").GetProperty("rows").EnumerateArray().ToArray();
        Assert.Equal(3, incidents.Length);
        Assert.All(incidents, row => Assert.Equal("recovered", row.GetProperty("reason").GetString()));
    }

    [Fact]
    public void Request_latency_requires_new_requests_and_excludes_long_poll_total_time()
    {
        const string total="kafka.network:RequestMetrics/TotalTimeMs/";
        const string queue="kafka.network:RequestMetrics/RequestQueueTimeMs/";
        JmxPoint[] Packet(int count) => new[]{"Produce","FetchConsumer","FetchFollower","JoinGroup"}.SelectMany(request=>new[]{
            new JmxPoint(total+"Count",new(){["request"]=request},count),
            new JmxPoint(total+"99thPercentile",new(){["request"]=request},900),
            new JmxPoint(queue+"Count",new(){["request"]=request},count),
            new JmxPoint(queue+"99thPercentile",new(){["request"]=request},3)
        }).ToArray();
        Assert.Equal(204,Status(Ingest(Packet(0))));
        Assert.Empty(Rows("alerts").GetProperty("rows").EnumerateArray());
        for(int tick=1;tick<=11;tick++)
        {
            _clock.Advance(30);
            Assert.Equal(204,Status(Ingest(Packet(tick))));
        }
        var alert=Assert.Single(Rows("alerts").GetProperty("rows").EnumerateArray());
        Assert.Equal("KafkaRequestSlow",alert.GetProperty("name").GetString());
        Assert.Equal("Produce",alert.GetProperty("labels").GetProperty("request").GetString());
        Assert.Equal("firing",alert.GetProperty("state").GetString());
        _clock.Advance(30);
        Ingest(Packet(11)); // An unchanged reservoir still reports 900 ms.
        Assert.Empty(Rows("alerts").GetProperty("rows").EnumerateArray());
        Assert.Equal("noTraffic",Rows("incidents").GetProperty("rows")[0].GetProperty("reason").GetString());
        _clock.Advance(30);
        Ingest(Packet(0)); // Broker counter reset is a new baseline.
        Assert.Empty(Rows("alerts").GetProperty("rows").EnumerateArray());
    }

    [Fact]
    public void Multipart_latency_waits_for_completed_cycle_and_uses_counter_from_another_part()
    {
        const string metric="kafka.network:RequestMetrics/RequestQueueTimeMs/";
        var labels=new SortedDictionary<string,string>{["request"]="Produce"};
        Ingest(new JmxPoint(metric+"Count",labels,0));
        _clock.Advance(30);
        var id=Guid.NewGuid();
        Assert.Equal(204,Status(_store.Ingest(_source,new(false,id,0,false,[new(metric+"99thPercentile",labels,150)]),default)));
        Assert.Empty(Rows("alerts").GetProperty("rows").EnumerateArray());
        _clock.Advance(1);
        Assert.Equal(204,Status(_store.Ingest(_source,new(false,id,1,true,[new(metric+"Count",labels,1)]),default)));
        Assert.Equal("KafkaRequestQueueSlow",Assert.Single(Rows("alerts").GetProperty("rows").EnumerateArray()).GetProperty("name").GetString());
    }

    [Fact]
    public void Series_health_is_scoped_to_the_returned_measurement_and_broker_charts_exclude_topic_dimensions()
    {
        const string metric="kafka.server:BrokerTopicMetrics/BytesInPerSec/OneMinuteRate";
        var batch=new JmxBatch(true,Guid.NewGuid(),0,true,[
            new(metric,[],5),new(metric,new(){["topic"]="orders"},9)]);
        _store.Ingest(_source,batch,default);
        Assert.True(Rows("sources").GetProperty("warning").GetBoolean());
        JsonElement Read() => Body(_store.Read("a","series",metric,null,15,null,null,default,aggregate:true));
        var result=Read();
        Assert.False(result.GetProperty("warning").GetBoolean());
        var row=Assert.Single(result.GetProperty("rows").EnumerateArray());
        Assert.Equal("current",row.GetProperty("health").GetString());
        Assert.Equal(5,row.GetProperty("points")[0].GetProperty("value").GetDouble());
        _clock.Advance(120);
        Assert.True(Read().GetProperty("warning").GetBoolean());
        Assert.Equal("stale",Read().GetProperty("rows")[0].GetProperty("health").GetString());
    }

    [Fact]
    public void Selected_topic_metrics_are_persistent_scoped_and_browsable_per_partition()
    {
        Assert.Equal(400, Status(_store.SetTopic("a","alpha,beta",true)));
        Assert.Equal(200, Status(_store.SetTopic("a","orders",true)));
        Assert.Equal(["orders"], Open().SelectedTopics("a")!);
        Assert.Null(_store.SelectedTopics("b"));
        var key = "kafka.log:Log/Size/Value";
        var batch = new JmxBatch(false, Guid.NewGuid(), 0, true, [
            new(key, new(){["topic"]="orders", ["partition"]="0"}, 23),
            new(key, new(){["topic"]="hidden", ["partition"]="0"}, 90),
            new("kafka.server:BrokerTopicMetrics/BytesInPerSec/Count",new(){["topic"]="orders"},31)]);
        Assert.Equal(204,Status(Ingest(Point(4))));
        Assert.Equal(204,Status(_store.Ingest(_source,batch,default,true)));
        Assert.Equal(429,Status(_store.Ingest(_source,batch,default,true)));
        string[] Catalogue(string topic,int? partition,string? after=null) =>
            Body(_store.Read("a","metrics",null,"",60,null,null,default,after,topic,partition))
                .GetProperty("names").EnumerateArray().Select(n=>n.GetString()!).ToArray();
        Assert.Equal([key],Catalogue("orders",0));
        Assert.Empty(Catalogue("hidden",0));
        Assert.Empty(Catalogue("orders",0,key));
        Assert.Equal(["kafka.server:BrokerTopicMetrics/BytesInPerSec/Count"],Catalogue("orders",null));
        Assert.Equal(200, Status(_store.SetTopic("a","orders",false)));
        Assert.Empty(Open().SelectedTopics("a")!);
        _clock.Advance(30);
        Assert.Equal(204,Status(_store.Ingest(_source,batch,default,true)));
        var history=Body(_store.Read("a","series",key,null,15,null,"{\"topic\":\"orders\",\"partition\":\"0\"}",default));
        Assert.Single(history.GetProperty("rows")[0].GetProperty("points").EnumerateArray());
    }

    [Fact]
    public void Sampling_interval_is_bounded_persistent_and_scoped_to_one_cluster()
    {
        Assert.Null(_store.SamplingOverride("a"));
        foreach(var value in new[]{10,30,60})
        {
            Assert.Equal(200,Status(_store.SetSampling("a",value)));
            Assert.Equal(value,Open().SamplingOverride("a"));
        }
        Assert.Equal(400,Status(_store.SetSampling("a",61)));
        Assert.Equal(400,Status(_store.SetSampling("a",9)));
        Assert.Null(_store.SamplingOverride("b"));
    }

    [Fact]
    public void Kafka_metadata_projection_matches_leaders_replication_and_suppresses_incomplete_totals()
    {
        var ok=new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.NoError);
        var noLeader=new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.LeaderNotAvailable);
        var brokers=new List<Confluent.Kafka.BrokerMetadata>{new(1,"first",9092),new(2,"second",9092)};
        var topic=new Confluent.Kafka.TopicMetadata("topic",[new(0,1,[1,2],[1],ok),new(1,-1,[2],[],noLeader)],ok);
        var batch=KafkaMetadataMonitor.Capture(new(brokers,[topic],1,"first"),21.5);
        double Value(string name)=>Assert.Single(batch.Samples,p=>p.Metric=="kafka.metadata/"+name).Value;
        Assert.False(batch.Partial);
        Assert.Equal(2,Value("brokers")); Assert.Equal(1,Value("offline_partitions")); Assert.Equal(2,Value("under_replicated_partitions"));
        Assert.Equal(21.5,Value("response_ms")); Assert.Equal(1,Assert.Single(batch.Samples,p=>p.Metric.EndsWith("partition_leaders") && p.Labels["broker_id"]=="1").Value);
        var failed=new Confluent.Kafka.TopicMetadata("unavailable",[],new(Confluent.Kafka.ErrorCode.TopicAuthorizationFailed));
        var partial=KafkaMetadataMonitor.Capture(new(brokers,[topic,failed],1,"first"),50);
        Assert.True(partial.Partial);Assert.DoesNotContain(partial.Samples,p=>p.Metric.Contains("partitions"));
    }

    [Fact]
    public void Client_statistics_keep_numeric_metrics_with_broker_topic_partition_and_request_dimensions()
    {
        const long now=1_800_000_000;
        using var doc=JsonDocument.Parse("""{"name":"writer","type":"producer","time":1800000000,"replyq":0,"brokers":{"k1/1":{"rtt":{"avg":2500,"cnt":3},"req":{"Produce":5}}},"topics":{"orders":{"partitions":{"1":{"consumer_lag":4}}}}}""");
        var batch=Assert.IsType<JmxBatch>(KafkaClientStatistics.Read(doc.RootElement,now));
        Assert.False(batch.Partial);
        var latency=Assert.Single(batch.Samples,p=>p.Metric=="librdkafka/brokers/rtt/avg");
        Assert.Equal(2500,latency.Value);
        Assert.Equal("k1/1",latency.Labels["broker"]);
        Assert.Contains(batch.Samples,p=>p.Metric=="librdkafka/brokers/req" && p.Labels["request"]=="Produce");
        Assert.Contains(batch.Samples,p=>p.Metric=="librdkafka/topics/partitions/consumer_lag" && p.Labels["topic"]=="orders" && p.Labels["partition"]=="1");
        Assert.Null(KafkaClientStatistics.Read(doc.RootElement,now+121));
        Assert.Equal(204,Status(_store.Ingest(_source,batch,default)));
    }

    [Fact]
    public void Rotating_and_revoking_a_dashboard_credential_invalidates_previously_admitted_batches()
    {
        var token=Body(_store.Enroll("b","secondary","create")).GetProperty("token").GetString()!;
        var old=_store.Sources().Single(s=>s.ClusterId=="b");
        Assert.Equal(SHA256.HashData(Encoding.UTF8.GetBytes(token)),old.Credential);
        Assert.Equal(409,Status(_store.Enroll("b","secondary","create")));
        Assert.Equal(200,Status(_store.Enroll("b","secondary","rotate")));
        Assert.Equal(401,Status(_store.Ingest(old,new(false,Guid.NewGuid(),0,true,[Point(2)]),default)));
        var rotated=_store.Sources().Single(s=>s.ClusterId=="b");
        Assert.Equal(204,Status(_store.Ingest(rotated,new(false,Guid.NewGuid(),0,true,[Point(3)]),default)));
        Assert.Equal(200,Status(_store.Enroll("b","secondary","revoke")));
        Assert.Equal(401,Status(_store.Ingest(rotated,new(false,Guid.NewGuid(),0,true,[]),default)));
        Assert.Equal("unconfigured",Rows("metrics","b").GetProperty("state").GetString());
    }

    public void Dispose()
    {
        using var db=new SqliteConnection(new SqliteConnectionStringBuilder {DataSource=_file,DefaultTimeout=3}.ToString());
        SqliteConnection.ClearPool(db);
        foreach(var suffix in new[]{"","-wal","-shm"}) File.Delete(_file+suffix);
    }
}
