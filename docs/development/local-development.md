# Local Development Baseline

## Prerequisites

- Git
- Docker with Docker Compose v2
- .NET 10 SDK when backend source is introduced
- Node.js 24 LTS when frontend source is introduced

## Kafka development broker

Start the pinned Apache Kafka 4.3.1 KRaft development broker:

```bash
./scripts/dev/kafka-up.sh
```

Bootstrap server:

```text
localhost:9092
```

Stop and remove development state:

```bash
./scripts/dev/kafka-down.sh
```

This broker is for development/testing only. It is not a production deployment reference architecture.

## Reproducibility

Developers must not depend on locally installed Kafka tooling for required CI behavior. Repository scripts and containers define the repeatable baseline.
