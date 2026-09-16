# Kafdeck Consolidated Capability Map

Status: **PROPOSED — Gate 2**  
Review date: **2026-09-16**

This document consolidates the requested feature inventory into a product-safe target model. It preserves the intended capabilities while explicitly marking features that require reframing, sequencing or exclusion.

Legend:
- **TARGET** — accepted as a long-term Kafdeck capability.
- **REFRAME** — product goal is valid but the implementation/claim must change.
- **DEFER** — valid theme, deliberately scheduled later.
- **NOT CORE** — not implemented as described because it creates an unacceptable architecture/security boundary; a safer replacement is specified.

## 1. Connectivity & Multi-Cluster

- **TARGET:** multiple simultaneously configured clusters and fast switching.
- **TARGET:** Command Palette (`Ctrl/Cmd+K`) for cluster/resource navigation; scheduled after core navigation stabilizes.
- **REFRAME:** provider support is a tested capability profile, not a blanket compatibility claim. Target profiles include Apache Kafka, Confluent Platform/Cloud, AWS MSK, Redpanda, Aiven and Google Managed Kafka.
- **REFRAME:** Strimzi is treated as an Apache Kafka Kubernetes/operator environment, not a distinct Kafka protocol.
- **REFRAME:** Azure Event Hubs receives its own limited Kafka-compatible provider profile because it does not expose every Apache Kafka administration capability.
- **TARGET:** KRaft-era Kafka is primary.
- **REFRAME:** ZooKeeper-mode clusters may be supported through normal Kafka broker/client APIs when tested; Kafdeck never connects directly to ZooKeeper.
- **TARGET:** PLAINTEXT, TLS/SSL, mTLS, SASL_PLAIN and SASL_SCRAM.
- **DEFER:** SASL/OAUTHBEARER, GSSAPI/Kerberos and cloud-native authentication adapters such as AWS MSK IAM.
- **TARGET:** Schema Registry, Kafka Connect and ksqlDB integrations behind independent adapters.
- **TARGET:** air-gapped/on-premise operation with no cloud dependency.
- **REFRAME:** Kafdeck does not require an external database for stateless features, but it also does not use the managed Kafka cluster as its mandatory application-state database. SQLite is the standalone durable provider; PostgreSQL is optional for production-scale durable state.
- **TARGET:** YAML/environment/Helm configuration as code.
- **DEFER:** official Terraform provider until stable write APIs exist.

## 2. Cluster & Broker Management

- **TARGET:** cluster overview: brokers, controller, topics, partitions and evidence-based health.
- **TARGET:** broker partition assignment and rack-awareness views.
- **REFRAME:** disk/CPU/network/throughput depend on JMX/Prometheus/OpenTelemetry/provider metrics and are shown only when that metric source is configured/available.
- **TARGET:** dynamic broker configuration only for settings Kafka/provider reports as dynamically alterable.
- **TARGET:** under-replicated/offline partitions and ISR anomalies.
- **REFRAME:** health scoring must be explainable and multidimensional; opaque ML/anomaly scores are later enhancements, not authoritative health truth.
- **TARGET:** preferred leader election.
- **REFRAME:** there is no fabricated generic broker "read-only mode". Maintenance uses actual primitives such as Kafka 4.x broker/log-directory cordoning, reassignment and controlled shutdown/decommission workflows when supported.

## 3. Topic & Partition Management

- **TARGET:** topic CRUD and bulk operations through the governed operation pipeline.
- **TARGET:** dynamic topic configs such as retention, cleanup policy, segment and compression where valid.
- **TARGET:** partition increase.
- **REFRAME:** replication-factor change is implemented through partition reassignment, not treated as an unrelated primitive.
- **TARGET:** visual reassignment planner with preview, progress and throttling.
- **REFRAME:** transfer-size/network impact is an estimate only when log-size/metric evidence exists; unknown evidence is surfaced as unknown.
- **TARGET:** DeleteRecords-based truncation/purge.
- **REFRAME:** timestamp purge first resolves timestamp to explicit per-partition offsets, previews them and classifies the irreversible delete as HIGH/CRITICAL.
- **TARGET:** partition skew and topic health insights.
- **TARGET:** topic documentation/catalog pages backed by Git/application metadata.

## 4. Message Browser, Search & Producer

- **TARGET:** JSON, Avro, Protobuf, JSON Schema, text and raw binary/hex.
- **DEFER/TARGET:** CBOR, XML and MessagePack through controlled SerDe extension points.
- **TARGET:** filters by key, offset, timestamp, partition, header, text and regex.
- **TARGET:** deterministic CEL and jq-style expression language.
- **NOT CORE:** arbitrary server-side JavaScript filtering. Replacement: CEL/jq-style DSL; any future user code requires a separately sandboxed execution RFC.
- **REFRAME:** no fixed "millions of messages per second" promise. Search is bounded by time/bytes/rate and benchmarked per release.
- **TARGET:** offset/timestamp time travel and bounded live tail.
- **REFRAME:** native SQL is not used to reimplement a stream-processing platform. Kafdeck offers ksqlDB integration plus bounded query/filter tooling; a future indexed query engine requires a dedicated architecture RFC.
- **TARGET:** tree/pretty views and header inspection.
- **TARGET:** single/batch/template production with schema validation after safe mutations are enabled.
- **TARGET:** replay/reprocess from DLQ, forward and cross-cluster transfer as governed jobs with rate limits and audit.
- **TARGET:** JSON/CSV/NDJSON exports with authorization and masking.

## 5. Consumer Groups & Lag

- **TARGET:** state, members, host/client metadata and assignments.
- **TARGET:** per-partition and aggregate lag.
- **TARGET:** real-time comparison with production/consumption rates when metric evidence exists.
- **TARGET:** reset/shift offset by earliest/latest/timestamp/absolute/relative values with preview.
- **TARGET:** delete consumer group/offset where supported.
- **TARGET:** lagging/slow consumer diagnostics.
- **REFRAME:** rebalance history requires historical observation/persistence and therefore arrives with observability history, not as a stateless guarantee.

## 6. Schema Registry

- **TARGET:** browse/create/update/delete subjects/schemas.
- **TARGET:** Avro, Protobuf and JSON Schema.
- **TARGET:** version history, references and diff viewer.
- **TARGET:** compatibility check before publish.
- **TARGET:** mock payload generation with deterministic bounds.
- **TARGET:** Confluent-compatible baseline plus tested Karapace/Apicurio adapters/capabilities.

## 7. Kafka Connect

- **TARGET:** multiple Connect clusters.
- **TARGET:** connector CRUD with schema/config-aware forms and documentation.
- **TARGET:** pause/resume/restart at connector/task level and stack-trace diagnostics.
- **REFRAME:** self-healing is an opt-in bounded remediation policy with max attempts, exponential backoff, circuit breaker and audit; never an unbounded restart loop.
- **TARGET:** live configuration update where Connect API supports it.
- **REFRAME:** throughput metrics are shown only when metric sources make them observable.

## 8. ksqlDB, Kafka Streams & Data Lineage

- **TARGET:** ksqlDB query/editor integration.
- **REFRAME:** Kafka Streams topology is shown only from observable application metadata/metrics; Kafdeck does not pretend broker metadata contains full application topology.
- **REFRAME:** state-store/RocksDB/thread metrics require application telemetry/JMX/metrics integration.
- **TARGET:** lineage across Connect/topics/ksqlDB/Streams when evidence exists.
- **REFRAME:** every lineage edge carries provenance/confidence: observed, declared or inferred.

## 9. Security, Authentication & Data Governance

- **TARGET:** OIDC/OAuth2 first-class authentication.
- **TARGET:** LDAP/Active Directory through provider integration.
- **REFRAME:** Keycloak/GitHub are standard identity-provider integrations, not bespoke Kafdeck security models.
- **DEFER:** direct SAML support unless OIDC/federation/reverse-proxy integration is insufficient and an RFC justifies it.
- **TARGET:** fine-grained RBAC by cluster/resource/pattern/action.
- **TARGET:** Kafka native ACL management.
- **TARGET:** server-side field masking/PII redaction so masked data never reaches browser/API/export for unauthorized access.
- **REFRAME:** wire-path masking/encryption requires an optional future Kafdeck Gateway; it is not part of the core out-of-band control plane.
- **TARGET:** complete audit log with configurable retention/export and tamper-evidence.
- **TARGET:** ownership catalog and self-service approval workflows.
- **TARGET:** read-only modes.
- **TARGET:** cost attribution/chargeback after ownership and trustworthy metric evidence exist.

## 10. Observability & Alerting

- **TARGET:** real-time cluster/topic/broker/consumer metrics where exposed.
- **TARGET:** historical metrics through an explicit persistence provider.
- **TARGET:** Prometheus/OpenTelemetry output and Grafana/Datadog interoperability.
- **TARGET:** notifier adapters for Slack, Teams, Telegram, Email, PagerDuty and generic Webhook.
- **TARGET:** lag/broker/partition/connect anomaly alerts.
- **DEFER:** data-quality monitoring until record access, schema and policy infrastructure exist.
- **TARGET:** SLO tracking with evidence/source attribution.

## 11. Automation & Integration

- **TARGET/INVARIANT:** every supported UI operation has a versioned API/application contract; no UI-only privileged back door.
- **TARGET:** CLI built on the API/application contracts.
- **DEFER/TARGET:** Terraform provider after stable declarative mutation contracts.
- **TARGET:** GitOps/configuration-as-code workflows.
- **TARGET:** MCP server for AI agents after identity/RBAC/audit; agents receive scoped identity and never bypass operation risk/approval.
- **TARGET:** MirrorMaker 2/cross-cluster replication management through adapters.
- **TARGET:** event webhooks/activity stream.
- **REFRAME:** "Revert" is available only for operations with a defined, verified compensating action; topic deletion, record purge and other irreversible operations are never advertised as reversible.

## 12. Developer & QA Tools

- **NOT CORE:** unrestricted embedded shell/kcat/kafka CLI terminal inside the web server because it creates an RCE/credential-exfiltration boundary.
- **REPLACEMENT TARGET:** Kafdeck CLI, safe API explorer, operation preview and "copy equivalent command" helpers where useful.
- **TARGET:** Smart Mock/Data Generator with schema awareness and strict messages/sec, bytes/sec, duration and total-record ceilings.
- **REFRAME:** Chaos Testing is product test/lab tooling and optional integrations (for example network fault proxies/Kubernetes chaos tools), not an unrestricted production operation exposed from the normal UI.

## 13. UX & Deployment

- **TARGET:** modern responsive UI, light/dark themes and keyboard navigation.
- **REFRAME:** Kafdeck targets a low resource footprint, but does not make an unconditional sub-100-MB RAM promise before benchmark evidence. RSS/CPU/startup budgets become measured release gates.
- **TARGET:** single OCI container and self-contained .NET binary distributions.
- **NOT APPLICABLE:** JAR distribution is not a Kafdeck target because the backend is .NET, not JVM.
- **TARGET:** Helm deployment; Kubernetes remains optional.
- **TARGET:** internationalization architecture; English is the authoritative project/documentation language, Persian can be an initial maintained UI translation and additional languages are community-extensible.
- **TARGET:** WCAG accessibility.
- **REFRAME:** internal adapter/SerDe/notifier/secret-provider extension points are designed early; a public dynamically loaded plugin SDK is post-v1 unless signing, trust, versioning and sandbox policy are approved earlier.

## Cross-cutting acceptance rules

A capability is not complete unless it has:
- explicit authorization semantics,
- Kafka/provider capability detection,
- bounded load behavior,
- cancellation/timeouts,
- audit behavior for sensitive actions,
- secret/redaction rules,
- real integration tests,
- API contract,
- documented failure/partial-result states,
- accessibility implications where UI is involved.
