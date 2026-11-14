using Confluent.Kafka;
using System.Diagnostics;
using Microsoft.Data.Sqlite;

namespace Kafdeck.Api;

internal sealed class KafkaMetadataMonitor(MonitoringSettings settings, RegisteredKafkaRuntime runtime,
    MonitoringStore store, ILogger<KafkaMetadataMonitor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var previous=new Dictionary<string,long>(StringComparer.Ordinal);
        using var timer=new PeriodicTimer(TimeSpan.FromSeconds(1));
        do
        {
            foreach(var cluster in settings.MetadataClusters)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    var now=Stopwatch.GetTimestamp();
                    if(previous.TryGetValue(cluster,out var last) && Stopwatch.GetElapsedTime(last,now).TotalSeconds<(store.SamplingOverride(cluster)??30)) continue;
                    previous[cluster]=now;
                    var config=runtime.ReadOnlyMetadataConfig(cluster);
                    if(config is null) continue;
                    using var client=new AdminClientBuilder(config).SetLogHandler((_,_)=>{}).Build();
                    var watch=Stopwatch.StartNew();
                    var metadata=client.GetMetadata(TimeSpan.FromSeconds(5));
                    var batch=Capture(metadata,watch.Elapsed.TotalMilliseconds);
                    store.Ingest(new(cluster,MonitoringSettings.MetadataInstance,[]),batch,ct);
                }
                catch(Exception ex) when (ex is KafkaException or SqliteException or IOException)
                {
                    logger.LogWarning("Kafka metadata monitoring could not sample {Cluster}: {ErrorType}",cluster,ex.GetType().Name);
                }
            }
        } while(await timer.WaitForNextTickAsync(ct));
    }

    internal static JmxBatch Capture(Metadata metadata,double elapsedMs)
    {
        var knownTopics=metadata.Topics.Where(t=>!t.Error.IsError).ToArray();
        var partitions=knownTopics.SelectMany(t=>t.Partitions).ToArray();
        var knownPartitions=partitions.Where(p=>!p.Error.IsError || p.Error.Code==ErrorCode.LeaderNotAvailable).ToArray();
        bool partial=knownTopics.Length!=metadata.Topics.Count || knownPartitions.Length!=partitions.Length;
        var points=new List<JmxPoint>{
            new("kafka.metadata/brokers",[],metadata.Brokers.Count),
            new("kafka.metadata/response_ms",[],elapsedMs)
        };
        if(!partial)
        {
            points.Add(new("kafka.metadata/topics",[],knownTopics.Length));
            points.Add(new("kafka.metadata/partitions",[],partitions.Length));
            points.Add(new("kafka.metadata/offline_partitions",[],partitions.Count(p=>p.Leader<0 || p.Error.Code==ErrorCode.LeaderNotAvailable)));
            points.Add(new("kafka.metadata/under_replicated_partitions",[],partitions.Count(p=>p.InSyncReplicas.Length<p.Replicas.Length)));
            foreach(var broker in metadata.Brokers)
                points.Add(new("kafka.metadata/partition_leaders",new(){["broker_id"]=broker.BrokerId.ToString(System.Globalization.CultureInfo.InvariantCulture)},partitions.Count(p=>p.Leader==broker.BrokerId)));
        }
        return new(partial,Guid.NewGuid(),0,true,points.ToArray());
    }
}
