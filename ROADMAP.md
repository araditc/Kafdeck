# Kafdeck Product Roadmap

Status: **Gate 0/Gate 1 accepted; capability expansion proposed for Gate 2**  
Baseline approval: **Gate 0 — 2026-09-16**  
v0.1 design approval: **Gate 1 — 2026-09-16**

Kafdeck is developed as an Apache-2.0, vendor-neutral, operations-first Kafka control plane. The roadmap is capability-driven rather than calendar-driven: a phase exits only when its quality, security, compatibility and governance gates are satisfied.

## Product principles

1. **Safe by default.** Read-only is the initial operating posture; destructive and high-risk operations require explicit authorization, validation, preview, confirmation/approval and audit.
2. **Read access is not data access.** Metadata permissions and record-payload permissions are separate capabilities.
3. **Kafdeck must not become a reliability risk to Kafka.** Polling, browsing, diagnostics, search and administration use bounded concurrency, cancellation, timeouts, rate/byte limits, pagination, caching and per-cluster bulkheads.
4. **Air-gapped and on-premise are first-class.** Core functionality has no required SaaS, cloud account or Internet dependency.
5. **Vendor neutrality is a core requirement.** Apache Kafka semantics define the core; managed-service/vendor features live behind capability-gated adapters.
6. **No mandatory data-plane proxy.** Core Kafdeck remains out of the Kafka message path. A future optional gateway requires a separate RFC and failure-domain analysis.
7. **Zero-write observation.** Read-only Kafdeck does not create internal topics or require write access to the managed Kafka cluster merely to store Kafdeck state.
8. **API parity by design.** Product UI capability is backed by versioned application/API contracts. CLI, Terraform and MCP build on those contracts rather than bypassing them.
9. **Application state is explicit.** Stateless releases need no database; durable Kafdeck state uses embedded SQLite by default and PostgreSQL where an external production database is appropriate. Kafka is not the default application database.
10. **Security controls apply to humans and automation equally.** API clients, Terraform, CLI and AI/MCP agents are subject to the same RBAC, risk, approval, masking and audit rules.
11. **Claims are measured.** Performance, resource footprint, search throughput and compatibility are published from repeatable benchmarks rather than fixed marketing assumptions.
12. **Decisions are versioned.** Material product, architecture, security, compatibility and governance changes are recorded in ADRs/RFCs and approval logs.

## Phase 0 — Foundation & Governance

Goal: establish a production-grade OSS repository before feature implementation.

Deliverables include governance, security/testing standards, CI quality gates, supply-chain controls, repository enforcement and local development infrastructure.

**Open administrative dependency:** repository-level GitHub rules/security settings tracked in Issue #3 must be closed before v0.1 implementation begins.

## v0.1 — Cluster Explorer — ACCEPTED

Primary posture: **read-only metadata/control-plane observation**.

Capabilities:
- Multiple immutable configuration-driven cluster profiles
- TLS and mTLS
- SASL/PLAIN and SASL/SCRAM
- Cluster identity and broker metadata
- Controller visibility through broker metadata
- Topics and partitions
- Leaders, replicas, ISR and explainable partition health
- Topic configuration inspection
- Broker configuration inspection where authorized/supported
- Capability and partial-access detection
- Evidence-based health summary
- Bounded per-cluster snapshots and refresh
- REST/OpenAPI contract

Compatibility:
- KRaft-era design is primary.
- No direct ZooKeeper integration.
- ZooKeeper-mode clusters may be observed only through compatible Kafka broker/client APIs when explicitly tested.

Exit criteria remain those accepted by Gate 1/RFC-0001.

## v0.2 — Safe Data Explorer

Goal: inspect Kafka record data without turning Kafdeck into an unbounded consumer or data-exfiltration surface.

Capabilities:
- Record browsing by topic/partition
- Offset and timestamp navigation/time-travel debugging
- Key/value/header inspection
- JSON, UTF-8 text and binary/hex views
- Read-only Schema Registry adapters for Avro, Protobuf and JSON Schema decoding
- Bounded live tailing
- Search by key, partition, offset, timestamp and headers
- Regex plus deterministic CEL and jq-style expression filtering
- Tree/structured payload view
- Server-side field masking/redaction before data reaches browser/API/export
- Policy-controlled JSON/CSV/NDJSON export
- Explicit scan range, byte/time/rate budgets and cancellation

Not in v0.2:
- arbitrary server-side JavaScript execution,
- unbounded whole-topic scans,
- a promise of a fixed "millions of messages/sec" search rate,
- native unbounded SQL stream processing,
- message production/replay.

Invariant: Kafka record payloads are not persisted by Kafdeck by default.

## v0.3 — Consumers, Schemas & Ecosystem Read Views

Capabilities:
- Consumer groups, state, members and assignments
- Current/committed offsets
- Per-partition and aggregate lag
- Produce-vs-consume rate comparison where metrics are available
- Inactive/stalled consumer diagnostics
- Rebalance observations; historical rebalance analysis only when history storage exists
- Schema Registry subjects/versions/references
- Schema diff and compatibility inspection
- Kafka Connect cluster/connector/task read-only status
- ksqlDB discovery/read-only metadata where configured
- Topic documentation/catalog metadata foundations

## v0.4 — Identity, Policy, Masking & Audit

This phase intentionally precedes general write administration.

Capabilities:
- OIDC/OAuth2 as the primary enterprise identity path
- Standalone/local authentication where justified
- LDAP/Active Directory integration through an identity-provider boundary
- Federated SSO; direct SAML implementation only if an RFC demonstrates need beyond OIDC/federation
- Fine-grained RBAC for cluster/topic/group/schema/connector/action scope
- Topic-pattern permissions
- Explicit metadata-vs-payload access separation
- Session management
- Role/resource-aware masking policies
- Explain Access/effective-permission view
- Read-only deployment/role modes
- Tamper-evident audit design with export sinks

Representative permissions include `cluster.read`, `topic.*`, `record.*`, `consumer.*`, `schema.*`, `connect.*`, `acl.*` and `settings.manage`.

Exit criterion: general mutation endpoints do not ship before this identity/authorization/audit foundation is enforced.

## v0.5 — Safe Administration & Controlled Mutations

Initial mutations:
- Create/alter/delete topics
- Bulk topic operations
- Alter topic configurations
- Increase partitions
- Produce individual/batch/template records with schema validation
- Reset/shift consumer offsets with preview
- Delete consumer groups/offsets where supported
- Schema create/update/delete and compatibility validation
- Kafka Connect connector/task mutations
- DeleteRecords-based truncate/purge by offset; timestamp purge resolves timestamp to explicit offsets before preview

All mutations use:

`Request -> Authorization -> Validation -> Risk Classification -> Preview -> Confirmation/Approval -> Execute -> Verify -> Audit`

Risk classes: LOW, MODERATE, HIGH, CRITICAL.

Irreversible operations such as topic deletion and record purge are never represented as generally reversible.

## v0.6 — Fleet Operations, Broker Maintenance & Kafka Security

Capabilities:
- Kafka ACL browser/editor
- SCRAM administration where supported
- Quota visibility/management
- Principal/resource access analysis
- Dynamic broker/cluster configuration where Kafka reports the config as dynamically alterable
- Preferred leader election
- Partition/replica reassignment planner
- Replication-factor change through reassignment
- Estimated transfer impact where log/metric evidence is available
- Reassignment throttling and progress
- Partition skew analysis
- Kafka 4.x broker/log-directory cordon and decommission workflows where supported
- Cross-cluster transfer/clone jobs with rate limits, idempotency and audit
- MirrorMaker 2/replication integration rather than reimplementing replication

There is no fabricated generic Kafka "broker read-only mode"; maintenance actions are capability-gated to actual Kafka/provider primitives.

## v0.7 — Developer & Streaming Ecosystem Platform

Schema Registry:
- Confluent-compatible baseline adapters
- Karapace/Apicurio compatibility through tested adapters/capabilities
- Full subject/schema lifecycle
- Avro, Protobuf, JSON Schema
- Compatibility, references, diff and mock-payload generation

Kafka Connect:
- Multiple Connect clusters
- Smart configuration forms
- CRUD, pause/resume/restart and task stack traces
- Metrics where exposed
- Optional bounded auto-restart policy with backoff/circuit-breaker; disabled by default

Data tooling:
- Additional SerDes such as CBOR, XML and MessagePack through controlled extension points
- Replay/reprocess, DLQ forwarding and cross-topic/cross-cluster forwarding as governed jobs
- Smart Mock/Data Generator with strict rate/byte ceilings and environment policy
- Command Palette for fast cluster/resource navigation
- Topic documentation/catalog views

Streaming ecosystem:
- ksqlDB query/editor integration
- Kafka Streams topology views only from observable application/metric evidence
- RocksDB/state-store metrics only when application telemetry exposes them
- Data lineage with provenance/confidence labels distinguishing observed from inferred edges

Kafdeck does not implement a full stream-processing SQL engine merely to duplicate ksqlDB/Flink.

## v0.8 — Observability, Automation & Platform APIs

Capabilities:
- OpenTelemetry traces/metrics/logs
- Prometheus-compatible metrics
- Historical metrics through an explicit persistence provider
- Broker/topic/consumer throughput and latency views
- Consumer lag trends
- Operational SLO tracking
- Data-quality policies/monitoring where record access is authorized
- Alert routing to Webhook, Email, Slack, Teams, Telegram and PagerDuty through notifier adapters
- REST API parity for supported UI operations
- Kafdeck CLI
- Configuration-as-code and GitOps workflows
- Terraform provider after mutating API contracts are stable enough to manage declaratively
- Activity/event webhooks
- MCP server/agent tools only after RBAC and mutation safety are enforced; MCP has no privileged bypass
- Revert/compensation only for operations with a defined safe inverse

## v0.9 — Governance, Enterprise Operations & Hardening

Governance:
- Application/team/topic ownership catalog
- Self-service requests and approval workflows
- Policy-as-code foundations
- Cost attribution/chargeback from ownership plus measured resource/traffic evidence
- Governance dashboards

Hardening and UX:
- Scale, soak, failure-path and compatibility testing
- Chaos/fault injection as controlled test infrastructure, not an unrestricted production UI action
- Threat-model validation and independent security review
- WCAG accessibility validation
- Internationalization framework; English remains the authoritative project language, with Persian and community translations supported
- Upgrade/migration and backup/restore testing
- Air-gap installation verification
- OCI single-container distribution, self-contained .NET binaries and Helm packaging
- Published CPU/RSS/startup/load benchmark budgets
- Performance-regression gates

Extension model:
- Internal provider/SerDe/notifier/secret-store ports are established before v1.0.
- Public runtime plugin SDK/loading is deferred until contracts and supply-chain/sandbox policy are stable.

## v1.0 — Stable Release

v1.0 requires:
- Stable public contracts or an explicit compatibility policy
- Reliable persistence migrations
- Supported upgrade path
- Complete security baseline
- Tested Kafka/provider compatibility matrix
- Documented deployment/operations model
- No known unresolved critical security vulnerability
- Release provenance, SBOM, signed artifacts/containers and reproducible release procedure
- Published performance/resource benchmark methodology
- No mutation path that bypasses authorization/risk/audit controls

After v1.0, Semantic Versioning governs compatibility and releases.

## Provider compatibility strategy

Kafdeck does not claim blanket compatibility merely because a service exposes a Kafka endpoint.

Compatibility is recorded as tested capability profiles:
- Apache Kafka — reference semantics
- Confluent Platform/Cloud — Kafka protocol plus optional ecosystem adapters
- AWS MSK — Kafka protocol plus optional IAM authentication adapter
- Redpanda — Kafka-compatible core plus capability-gated provider features
- Aiven for Apache Kafka — Kafka-compatible core plus managed-service limitations
- Google Cloud Managed Service for Apache Kafka — provider profile when tested
- Strimzi — an Apache Kafka deployment/operator environment, not a separate Kafka wire protocol dialect
- Azure Event Hubs — Kafka-compatible service profile with explicit feature limitations; never assumed equivalent to full Apache Kafka administration

## Post-v1.0 candidate themes

Subject to RFC approval:
- Optional **Kafdeck Gateway** for wire-path policy/encryption/masking; separate process and failure domain, never mandatory for core Kafdeck
- Public plugin/provider SDK with signing/sandbox/trust policy
- Fleet-scale policy orchestration
- Advanced anomaly detection with explainable evidence
- Optional indexed/historical record search architecture
- Advanced automated remediation with approvals and circuit breakers
