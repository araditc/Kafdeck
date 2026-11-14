# Kafka monitoring in Kafdeck

Select a cluster and open **Monitoring**. The dashboard, numeric samples, rule engine and alert timeline run in Kafdeck. A Kafka connection alone provides group offsets, lag and partition metadata. Each broker or controller also exposes measurements inside its own JVM. The Kafdeck agent reads those locally and sends them directly to your Kafdeck instance.

## Enable broker measurements

1. Sign in as the managed deployment owner. Open **Monitoring → Manage monitoring sources** and download the agent. Register a distinct instance name for each Kafka broker or standalone controller, such as `broker-2`.
2. Save the displayed key to a file readable by the Kafka service account. Use separate keys for each source. Registration and rotation display the key once. The server stores its SHA-256 digest.
3. Copy the JAR to the Kafka host. Put the following in a file such as `/etc/kafdeck/agent.properties`. Use the HTTPS address reachable from *the broker*. The Kafka JVM must trust that server's certificate.

   ```properties
   endpoint=https://kafdeck.example.org/monitoring/ingest
   tokenFile=/etc/kafdeck/agent-token
   intervalSeconds=30
   ```

4. Append the following JVM option to the broker's existing `KAFKA_OPTS` and perform a rolling restart following the cluster's maintenance procedure. The agent requires Java 17 or later. It runs on a daemon thread in the existing Kafka process.

   ```text
   -javaagent:/opt/kafdeck/kafdeck-kafka-agent.jar=/etc/kafdeck/agent.properties
   ```

5. Open **Metric explorer**, search for `UnderReplicatedPartitions`, select its `Value` attribute and the desired broker. The first points should arrive after a successful capture. On a missing or partial capture, check the source status, Kafka process log, endpoint, key and certificate trust. Rotate the key in the dashboard and update the token file to replace credentials without restarting Kafka. **Revoke** removes that source and its monitoring history.

The [agent source and packaged JAR](../../tools/kafka-agent/) ship together. To rebuild with Python and a JDK, run `python tools/kafka-agent/build.py --output tools/kafka-agent/kafdeck-kafka-agent.jar`.

## What each source measures

| Question | Where to look in Kafdeck | Interpretation |
| --- | --- | --- |
| Is a consumer falling behind? | Select a group under **Consumer activity**. Compare total lag, worst partitions and committed versus end-offset growth. | Growth concentrated on one partition calls for checking its assignment, key distribution and consumer. Rising lag across partitions calls for comparing ingress and processing capacity. Offset growth includes offset gaps and commits. To establish successful message processing, compare application receipts and completions. |
| Why are Produce requests slow? | Under **Investigate broker latency**, compare request queue, local processing, remote wait and response send p99 on the same broker and time range. | A growing request queue with low handler idle fraction suggests contention on the broker. For local time, compare disk flush and JVM pauses. For remote wait, check ISR shrink and followers. For response send, compare network idle and client RTT. These percentiles come from different distributions and cannot be added. |
| Is data availability affected? | Inspect controller offline partitions, broker under-replicated partitions and the topic's leader distribution. | An offline leader affects serving that partition. Under-replication signals reduced redundancy. Inspect the affected topic and follower before changing settings. |
| Is the client waiting? | Submit client statistics and compare broker-labelled RTT, queued requests, delivery counters, rebalances and fetch queues at matching times. | A single broker's long RTT directs the investigation to that broker and its path. Slow end-to-end delivery with ordinary broker latency warrants checking producer buffering, client load and application queues. |
| Is the application itself slow? | Open a workload measurement file to compare queue wait, worker activity, publish acknowledgement, commit time, CPU or GC in the same interval. | Use measured pipeline stages from the actual service. Its worker count, group ID and useful thresholds depend on the deployment. |

Under **Reporting sources** each registered instance has its own last-received time and capture status. An instance waiting for its first sample has never reported to this receiver.

The agent discovers numeric attributes and first-level numeric composite fields under `kafka.*`, `java.lang` and `java.nio`. Kafka version, process role, active traffic, configured metric recording level and installed components determine which metrics exist. Dimensions such as topic, partition and request type stay attached. The metric name follows `domain:type[/name]/attribute[/field]`. Use exact labels such as `{"request":"Produce"}` to narrow a series. Java Kafka clients can load the same agent under distinct source names. Host disk queue depth and OS retransmits require measurements from the host. [Apache Kafka monitoring reference](https://kafka.apache.org/43/operations/monitoring/)

## Connect a librdkafka application

Register one source per client instance in **Manage monitoring sources**, assign it its own key and set the client's `statistics.interval.ms` to 30000. Have the application's statistics callback enqueue the latest JSON snapshot for an asynchronous HTTPS POST to `/monitoring/client-stats`. Set `Content-Type: application/json` and pass the key in `X-Kafdeck-Agent-Token`. Keep the queue bounded so a slow monitoring endpoint cannot block Kafka's poll or delivery thread. HTTP 204 acknowledges a snapshot. Each key binds the measurements to the registered cluster and instance.

The explorer preserves numeric librdkafka field names and native units. For example, `librdkafka/brokers/rtt/p99` is measured in **microseconds**, with a broker label. Consult the paired `cnt` before interpreting an empty latency window. A distinct source per client keeps cumulative counters separate across processes. Snapshots older than 120 seconds or more than 30 seconds in the future are rejected. Points use server receipt time. [librdkafka field definitions](https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md)

For an existing incident capture, **Workload and broker measurements** opens UTF-8 JSON Lines locally. The accepted formats and units are listed next to the file chooser. Native client-window imports convert microseconds to milliseconds and omit empty windows. A workload can also supply individual observations, for example:

```json
{"time":"2026-11-14T08:15:00Z","source":"checkout-worker-2","metric":"queueWaitP95","value":41.2}
```

Imported files stay in the tab. Server-side rules evaluate live source measurements.

## Set and investigate alerts

The built-in rules monitor missing source heartbeats (120 seconds), controller offline partitions (above zero immediately), under-replicated partitions (above zero for two minutes), handler idle fraction (below 0.15 for five minutes) and request p99 (above 250 ms for five minutes). These are starting thresholds. Use the baseline and latency objective for the selected workload.

Select any collected metric, then open **Custom alert rules** as deployment owner. Choose a name, comparison, threshold in the metric's native units, hold time and severity. The rule applies independently to each instance and label set in that cluster. For a signal that should fire on the first observation, use zero seconds. A change to a rule resets its hold time.

**Cluster alerts** shows the affected instance, label set, value, threshold and first observation. **Inspect measurement** opens the corresponding series. **Alert history** records when a firing incident started and when the value recovered, the rule changed or evidence went stale. Stale data does not establish recovery. The server evaluates rules while the browser is closed. Optional browser notifications require permission and an open Monitoring page. Browsers or operating systems may suspend background tabs.

## Storage, retention and operating bounds

In managed deployments the server creates `kafdeck-monitoring.sqlite` beside the managed identity state. Give the Kafdeck service account a private writable directory for the database and its WAL. Single-host SQLite stores the last 24 hours of broker/client history with cleanup every five minutes. Allocate disk according to series cardinality and sampling interval. For example, 10,000 series sampled every 30 seconds produce approximately 28.8 million points per day. The agent starts each interval after its previous capture and transmission completes. Multiple Kafdeck replicas require a deliberate single receiving instance for this feature.

For a deployment without managed enrollment, configure an absolute database path and pre-register each source in server configuration. Tokens are file or environment references:

```json
{
  "Kafdeck": {
    "Monitoring": {
      "DatabasePath": "/var/lib/kafdeck/monitor/metrics.sqlite",
      "Sources": [
        {"ClusterId":"office","Instance":"broker-1","Token":"file:/etc/kafdeck/keys/broker-1"}
      ]
    }
  }
}
```

The broker interval supports 10–60 seconds. Captures use packets of up to 8,192 observations and at most 64 packets. The client endpoint accepts up to 8,192 numeric observations per snapshot. The catalogue returns up to 500 names per search and 12 series per range query. Partial results are flagged. Refine the search by metric, instance and labels. Charts show actual observations with UTC timestamps, using the last observation per display bucket. The existing consumer-history provider and its sampling permissions determine availability of historical group lag. Current group reads can work independently of that provider.
