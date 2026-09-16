# ADR-0003: Kafka Compatibility and ZooKeeper Policy

- Status: Accepted
- Date: 2026-09-16
- Gate: 0

## Context

Modern Apache Kafka is KRaft-based, while older deployments may still exist. Direct ZooKeeper integration would add legacy coupling and bypass Kafka's supported client/admin contracts.

## Decision

- Kafdeck is designed for the KRaft-era Kafka architecture.
- Kafdeck will not directly connect to ZooKeeper.
- Tier 1 support tracks Apache Kafka release lines that are currently supported and explicitly validated in the Kafdeck CI matrix.
- Kafka 3.9 latest patch is an initial Tier 2 compatibility target where feasible.
- Older Kafka support requires an explicit compatibility decision.

## Consequences

Compatibility claims must be backed by real Kafka integration tests. A ZooKeeper-mode cluster may be usable only insofar as normal broker/client APIs supported by Kafdeck remain compatible; ZooKeeper management is out of scope.
