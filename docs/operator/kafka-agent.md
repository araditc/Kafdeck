# Kafka monitoring agent (Java 21)

The Kafdeck agent runs **inside each Kafka broker JVM** and reads local platform/Kafka JMX MBeans. It sends bounded batches over HTTPS directly to Kafdeck. No remote JMX port, Prometheus or exporter process is required. It does not collect Kafka message payloads or log credentials.

Download `kafdeck-kafka-agent-java21.zip` and its checksum from the [1.0.0 release](https://github.com/araditc/Kafdeck/releases/tag/v1.0.0). The full Windows offline ZIP also contains the JAR under `payload/agent`. Source, build script and focused tests are in [`tools/kafka-agent`](https://github.com/araditc/Kafdeck/tree/v1.0.0/tools/kafka-agent). The source license is the repository [LICENSE](https://github.com/araditc/Kafdeck/blob/v1.0.0/LICENSE).

## JVM compatibility and build

The JAR uses Java 11 bytecode (`javac --release 11`); the same JAR was tested on Java 21. Keep the Kafka distribution's supported JVM: installing this agent does not upgrade the broker JVM. Java 11 has also been exercised on the three deployment brokers. The repository CI defines a broader compatibility matrix; consult its run results before claiming support for another runtime.

To build from source, install Python 3 and a JDK 11 or later (JDK 21 recommended):

```sh
python tools/kafka-agent/build.py --jdk /path/to/jdk-21 --output artifacts/kafdeck-kafka-agent.jar
```

Windows example:

```powershell
python tools/kafka-agent/build.py --jdk C:\Java\jdk-21 --output artifacts/kafdeck-kafka-agent.jar
```

Focused developer checks, when changing the agent:

```sh
python tools/kafka-agent/tests/smoke.py
python tools/kafka-agent/tests/diagnostics.py
```

The standalone ZIP includes the same source layout, so these commands work from its root. The source remains available through GitHub as well as the download.

## 1. Create one source per broker

Sign in to Kafdeck, select the cluster and open **Monitoring â†’ Sources**. Create a source for each broker and save its generated token privately. A three-broker cluster needs three source records and three different tokens. These are ingestion tokens, not Kafka passwords, Keycloak credentials or deployment login tokens.

Place the token, as a single line, in `/opt/kafdeck-monitor/agent-token` inside the broker environment. Do not put the token in a JVM command line, Docker environment, image layer or committed configuration. Give the Kafka runtime user read access and restrict token/config files, for example mode `0600`. JAR and public CA files need no write permission for Kafka. For a container, determine its actual UID/GID first (`docker exec kafka-1 id`) and set host file ownership accordingly.

## 2. Configure the agent

Create `/opt/kafdeck-monitor/agent.properties`:

```properties
endpoint=https://YOUR-KAFDECK-SERVER-IP:8080/monitoring/ingest
tokenFile=/opt/kafdeck-monitor/agent-token
intervalSeconds=30
includeTopicPartitions=false
# Required for the private CA created by the Windows offline installer:
caFile=/opt/kafdeck-monitor/ca.pem
```

For that installer, securely copy the public `C:\Kafdeck\public\server-public.pem` to `ca.pem`. For a publicly trusted certificate, omit `caFile` to use the JVM trust store. The configured hostname/IP must match the certificate. Do not disable TLS verification. The endpoint must be reachable from each broker; `127.0.0.1` refers to that broker's own environment. The agent does not follow redirects. Only the exact `/monitoring/ingest` path is accepted; do not append credentials, a query or fragment.

The initial interval is **10â€“60 seconds**, default 30. Dashboard controls can send updated cadence (such as 10, 30 or 60 seconds) and topic selection back on successful ingestion responses. `includeTopicPartitions=false` avoids initial unbounded topic/partition enumeration; explicitly enable/select the wanted topics in the dashboard. Current selection is bounded to 32 topics. Availability varies with JVM, Kafka version, broker/controller role, workload and replica placement.

On Windows broker hosts use forward slashes in Java Properties paths, for example `tokenFile=C:/kafdeck-agent/agent-token` and `caFile=C:/kafdeck-agent/ca.pem`.

## 3. Add to Kafka startup

Copy the JAR to `/opt/kafdeck-monitor/kafdeck-kafka-agent.jar`. Append this argument to the broker's **existing** JVM options:

```text
-javaagent:/opt/kafdeck-monitor/kafdeck-kafka-agent.jar=/opt/kafdeck-monitor/agent.properties
```

For Windows Kafka:

```text
-javaagent:C:/kafdeck-agent/kafdeck-kafka-agent.jar=C:/kafdeck-agent/agent.properties
```

Use the launcher's `KAFKA_OPTS` or service environment; retain existing SSL, SASL, JAAS and other agent options. For split KRaft broker/controller processes, install on each JVM whose metrics you need. A JVM agent normally takes effect at process startup.

### Docker Compose

For each broker create a separate private host directory containing the JAR, properties, token and public CA. Example for one broker:

```yaml
services:
  kafka-1:
    volumes:
      - /srv/kafdeck-agent/kafka-1:/opt/kafdeck-monitor:ro
    environment:
      KAFKA_OPTS: "${KAFKA1_EXISTING_OPTS:-} -javaagent:/opt/kafdeck-monitor/kafdeck-kafka-agent.jar=/opt/kafdeck-monitor/agent.properties"
```

Set `KAFKA1_EXISTING_OPTS` to that broker's previous options; leave it empty only if previously empty. Integrate this with the existing Compose definition rather than replacing other mounts/environment. Give kafka-2 and kafka-3 their own directories and source tokens. Confirm files are readable by the container user before restarting.

Apply JVM/mount changes in a maintenance window, one broker at a time, following your Kafka restart procedure. Check ISR, replication and cluster health between brokers. A rolling restart does not guarantee uninterrupted availability for replication-factor-1 topics. The public bundle has no live-attach migration helper.

## 4. Verify charts and alerts

1. Check Kafka logs for `Kafdeck monitoring configuration invalid` or repeated `Kafdeck monitoring capture failed`. Configuration is read on agent startup; fix paths/TLS/options and restart according to the maintenance procedure when needed.
2. After an ingestion cycle, the source should become **Current**. Check its last-seen time. Rates and GC deltas need at least two samples.
3. In Monitoring inspect CPU, heap, GC and request queue charts; select real metric names from the explorer. Missing MBeans cannot be invented by the dashboard.
4. In topic/partition monitoring enable a real topic, select a partition and confirm recent timestamps and matching replica/broker dimensions. Allow another cycle for a newly selected topic.
5. Review alert rules, thresholds and observation status. Idle workloads can legitimately show zero rates. Missing/stale/unsupported observations must not be interpreted as healthy zeroes.

An accepted ingest batch returns HTTP **204**. **401** indicates an invalid/revoked token, **403** can indicate an HTTPS boundary failure, and **429** can indicate cadence limits or duplicate agents. The agent logs exception classes, not response bodies or secrets; inspect source status and Kafdeck server logs for details. Check network access to the dashboard port from within the container, public CA trust, certificate SAN and clock before changing credentials.

Unsupported JVM memory usage thresholds (for example certain G1 pools) appear as unavailable details without making the entire source Partial. Actual getter failures, disappeared beans or collection limits still require investigation. No committed consumer offset is different from zero lag; consumer activity also depends on Kafka permissions and real group offsets.

## Rotation, upgrades and bounds

Rotate a source key in the dashboard and atomically replace only its broker's token file. The token is reread each cycle; this does not require a Kafka restart. Revoke invalidates that source and removes its associated data; use it deliberately. To upgrade the JAR or properties, back up configuration and restart brokers through the maintenance procedure. To remove the agent, remove its JVM argument and restart; revoke the unused source afterward.

Collection is bounded to 64 packets per cycle, 8,192 values per packet and less than 2 MB per packet. JMX metrics are read from the running JVM; not every Kafka/JVM combination exposes the same set. Keep tokens, private keys and installation secrets out of support bundles and Git.

See [Windows offline installation](windows-offline-install.md), [monitoring behavior](kafka-monitoring.md), and the [Persian RTL agent guide](kafka-agent-java21.fa.md).
