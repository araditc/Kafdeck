export interface MonitoringChart {title:string;metric:string;unit:string;factor?:number}
const traffic=(name:string,title:string,unit:string,factor=1):MonitoringChart=>({
  title,metric:'kafka.server:BrokerTopicMetrics/'+name+'/OneMinuteRate',unit,factor
});
export const overviewCharts:MonitoringChart[]=[
  {title:'Broker CPU',metric:'kafdeck.jvm/cpu_percent',unit:'%'},
  {title:'GC time',metric:'kafdeck.jvm/gc_time_percent',unit:'%'},
  {title:'Produce request queue p99',metric:'kafka.network:RequestMetrics/RequestQueueTimeMs/99thPercentile',unit:'ms'},
  {title:'Metadata response time',metric:'kafka.metadata/response_ms',unit:'ms'},
  {title:'Offline partitions',metric:'kafka.metadata/offline_partitions',unit:'partitions'},
  {title:'Under-replicated partitions',metric:'kafka.metadata/under_replicated_partitions',unit:'partitions'},
];
export const brokerCharts:Record<string,MonitoringChart[]>={
  Traffic:[
    traffic('BytesInPerSec','Incoming data','MiB/s',1/1048576),
    traffic('BytesOutPerSec','Outgoing data','MiB/s',1/1048576),
    traffic('MessagesInPerSec','Incoming records','records/s'),
    traffic('ReplicationBytesInPerSec','Replication ingress','MiB/s',1/1048576),
    traffic('ReplicationBytesOutPerSec','Replication egress','MiB/s',1/1048576),
  ],
  'Request latency':[
    {title:'Request queue p99',metric:'kafka.network:RequestMetrics/RequestQueueTimeMs/99thPercentile',unit:'ms'},
    {title:'Local processing p99',metric:'kafka.network:RequestMetrics/LocalTimeMs/99thPercentile',unit:'ms'},
    {title:'Replication wait p99',metric:'kafka.network:RequestMetrics/RemoteTimeMs/99thPercentile',unit:'ms'},
    {title:'Response send p99',metric:'kafka.network:RequestMetrics/ResponseSendTimeMs/99thPercentile',unit:'ms'},
    {title:'Total request time p99',metric:'kafka.network:RequestMetrics/TotalTimeMs/99thPercentile',unit:'ms'},
    {title:'Network processor idle time',metric:'kafka.network:SocketServer/NetworkProcessorAvgIdlePercent/Value',unit:'%',factor:100},
  ],
  'JVM memory':[
    ...overviewCharts.slice(0,2),
    ...['used','committed','max'].map(name=>({title:{used:'Heap used',committed:'Heap committed',max:'Heap limit'}[name]!,
      metric:'java.lang:Memory/HeapMemoryUsage/'+name,unit:'MiB',factor:1/1048576})),
    {title:'Non-heap used',metric:'java.lang:Memory/NonHeapMemoryUsage/used',unit:'MiB',factor:1/1048576},
  ],
  'Replication and disk':[
    {title:'Under-replicated partitions',metric:'kafka.server:ReplicaManager/UnderReplicatedPartitions/Value',unit:'partitions'},
    {title:'ISR shrink rate',metric:'kafka.server:ReplicaManager/IsrShrinksPerSec/OneMinuteRate',unit:'events/s'},
    {title:'ISR expansion rate',metric:'kafka.server:ReplicaManager/IsrExpandsPerSec/OneMinuteRate',unit:'events/s'},
    {title:'Offline replicas',metric:'kafka.server:ReplicaManager/OfflineReplicaCount/Value',unit:'replicas'},
    {title:'Partition leaders',metric:'kafka.server:ReplicaManager/LeaderCount/Value',unit:'partitions'},
    {title:'Disk flush p99',metric:'kafka.log:LogFlushStats/LogFlushRateAndTimeMs/99thPercentile',unit:'ms'},
  ],
};
export const topicCharts=brokerCharts.Traffic!.slice(0,3);
export const partitionCharts:MonitoringChart[]=[
  {title:'Replica log size',metric:'kafka.log:Log/Size/Value',unit:'MiB',factor:1/1048576},
  {title:'Log end offset',metric:'kafka.log:Log/LogEndOffset/Value',unit:'offsets'},
  {title:'Log start offset',metric:'kafka.log:Log/LogStartOffset/Value',unit:'offsets'},
  {title:'Log segments',metric:'kafka.log:Log/NumLogSegments/Value',unit:'segments'},
  {title:'In-sync replicas',metric:'kafka.cluster:Partition/InSyncReplicasCount/Value',unit:'replicas'},
  {title:'Under-replicated replica',metric:'kafka.cluster:Partition/UnderReplicated/Value',unit:'0/1'},
];
