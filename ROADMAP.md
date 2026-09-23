# Kafdeck Product Roadmap

Status: **Gate 0–Gate 6 scope accepted; v0.1-v0.5 released; v0.5.1 corrective release published; v0.6 planning pending governed admission; v0.7+ inactive**  
Baseline approval: **Gate 0 — 2026-09-16**  
v0.1 design approval: **Gate 1 — 2026-09-16**  
Long-term capability roadmap approval: **Gate 2 — 2026-09-16**

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

## Sequencing reconciliation — v0.2/v0.3

The Gate 2 capability order remains authoritative, but its original version-number mapping was superseded by later owner decisions.

- Issue #57 advanced **Operator Identity/RBAC** into v0.2; v0.2 is released.
- Issue #75 accepted **Safe Data Explorer + Server-Side Masking** as v0.3; v0.3 is released.
- Consumers/Schemas/Ecosystem Read Views therefore move to v0.4.
- v0.5+ sequencing remains unchanged unless a later approved scope decision supersedes it.

Historical Gate 2 records remain unchanged as approval evidence.

## v0.2 — Operator Identity/RBAC — RELEASED

Primary posture: **human operator identity + default-deny authorization while Kafka remains metadata-only/read-only**.

Capabilities delivered:
- OIDC Authorization Code + PKCE,
- server-side sessions,
- Local/Token/OIDC access-mode separation,
- issuer+subject canonical identity,
- configuration-driven immutable RBAC,
- subject/group bindings,
- cluster/resource-scoped permissions,
- backend-authoritative enforcement,
- structured security audit,
- preserved v0.1 Local/Token compatibility.

v0.2 introduced no Kafka mutation and no payload browsing. RFC-0002 and the v0.2 release evidence remain authoritative for its exact contract.

## v0.3 — Safe Data Explorer + Server-Side Masking — RELEASED

Goal: inspect Kafka record data under the released v0.2 identity/RBAC boundary without creating an unbounded consumer, payload database, or data-exfiltration bypass.

Capabilities:
- topic/partition record browsing,
- offset and timestamp navigation/time-travel debugging,
- bounded forward/previous-page reads,
- bounded live tail,
- key/value/header inspection,
- JSON, UTF-8 text and binary/hex views,
- read-only Schema Registry-assisted Avro/Protobuf/JSON Schema decoding,
- bounded key/header/range/regex/CEL/jq-style filtering,
- server-side masking/redaction before browser/API/export,
- explicit `record.read` authorization separate from metadata permissions,
- separate `record.export` authorization,
- bounded JSON/CSV/NDJSON export,
- explicit record/byte/time/rate/concurrency budgets,
- cancellation and per-cluster load isolation,
- record-read/export audit events,
- REST/OpenAPI/UI parity,
- repeatable load/benchmark evidence.

Not in v0.3:
- Kafka mutation,
- record production/replay,
- consumer offset mutation,
- payload persistence/indexing by default,
- unbounded whole-topic scans,
- arbitrary server-side JavaScript,
- native general-purpose SQL stream processing,
- full Schema Registry explorer.

Invariant: an active masking rule cannot be bypassed by browser/API/export/raw/hex paths.

## v0.4 — Consumers, Schemas & Ecosystem Read Views — RELEASED

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

Scope authority: Issue #99 / Gate 4 / RFC-0004. v0.4 remains read-only: no consumer-offset, Schema Registry, Connect, ksqlDB, Kafka topic/configuration, or record mutation is introduced.

## v0.5 — Safe Administration & Controlled Mutations — RELEASED

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

Scope authority: **Issue #121 / Gate 5 / RFC-0005**. W32-W40 were governed-admitted through tracker #122 and the v0.5 release was explicitly owner-approved. The scope-neutral v0.5.1 corrective release is complete under Issue #144. Gate 6 now accepts v0.6 scope; implementation remains gated by its separate planning admission.

Mutation mode requires durable Kafdeck operation persistence. CRITICAL operations require a distinct eligible approver; a deployment that cannot establish that property fails closed for CRITICAL mutation.

Irreversible operations such as topic deletion and record purge are never represented as generally reversible.

## v0.6 — Fleet Operations, Broker Maintenance & Kafka Security — SCOPE ACCEPTED

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

Scope authority: **Issue #145 / Gate 6 — owner accepted 2026-09-23**. Tracker **#146** owns the [RFC-0006 planning package](docs/implementation/v0.6-planning-package.md). W41-W50 may start only after protected-main governed planning admission with fresh exact-head checks, required CODEOWNER approval and resolved substantive review threads. No v0.6 executable surface is activated by scope acceptance or this roadmap change. Release/tag/publication remains a separate owner gate; v0.7+ remains inactive.

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
