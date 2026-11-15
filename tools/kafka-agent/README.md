# Kafdeck Kafka monitoring agent

Java source: [`src/org/kafdeck/monitoring/KafkaMonitoringAgent.java`](src/org/kafdeck/monitoring/KafkaMonitoringAgent.java).

Read the [complete agent installation, security, Docker/Windows, verification and build guide](../../docs/operator/kafka-agent.md). Download the JAR/source bundle from the [monitoring preview](https://github.com/araditc/Kafdeck/releases/tag/monitoring-preview-20261115).

The packaged agent targets Java 11 bytecode and is tested on Java 21. It runs within the Kafka JVM, reads local JMX metrics and sends bounded batches to Kafdeck over HTTPS. Create one dashboard source/token per broker. Do not commit tokens or private keys. See [agent.properties.example](agent.properties.example) for a template, not a working credential.

Build from the repository root using Python 3 and JDK 21:

```sh
python tools/kafka-agent/build.py --jdk /path/to/jdk-21 --output artifacts/kafdeck-kafka-agent.jar
```

This code is covered by the repository's [Apache 2.0 license](../../LICENSE).
