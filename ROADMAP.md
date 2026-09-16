# Kafdeck Product Roadmap

Status: **Approved baseline**  
Baseline approval: **Gate 0 — 2026-09-16**

Kafdeck is developed as an open-source, vendor-neutral, operations-first Kafka control plane. The roadmap is capability-driven rather than calendar-driven: a phase exits only when its defined quality and governance gates are satisfied.

## Product principles

1. **Safe by default.** Read-only is the initial operating posture; destructive and high-risk operations require explicit authorization, preview/validation, confirmation, and audit.
2. **Read access is not data access.** Metadata permissions and record-payload permissions are separate capabilities.
3. **Kafdeck must not become a reliability risk to Kafka.** All polling, browsing, diagnostics, and administration must use bounded concurrency, cancellation, timeouts, pagination, caching, and load controls.
4. **Air-gapped and on-premise are first-class.** Core functionality has no cloud or Internet dependency.
5. **Vendor neutrality is a core requirement.** Apache Kafka semantics define the core. Vendor-specific capabilities live behind adapters.
6. **Decisions are versioned.** Material product, architecture, security, compatibility, and governance decisions are recorded in ADRs/RFCs and approval logs.

## Phase 0 — Foundation & Governance

Goal: establish a production-grade OSS repository before feature implementation.

Deliverables:
- Product vision and scope
- Architecture baseline and ADR process
- Engineering standards
- Security baseline and threat-model process
- Testing and release standards
- Contribution and governance model
- Git/PR policy
- CI quality gates
- Dependency and supply-chain controls
- Local development environment design

Exit criteria:
- Baseline documents accepted and merged
- Initial ADRs accepted
- Repository protection and PR workflow enabled
- CI skeleton in place
- No unresolved architecture decisions required to start v0.1

## v0.1 — Cluster Explorer

Primary posture: **read-only**.

Capabilities:
- Multi-cluster connection profiles
- TLS and mTLS
- SASL/PLAIN and SASL/SCRAM
- Cluster identity and metadata
- Brokers and controllers
- Topics and partitions
- Leaders, replicas, ISR, partition health
- Topic configuration inspection
- Broker configuration inspection where supported
- Cluster capability detection
- Health summary

Exit criteria:
- Supported Kafka compatibility matrix validated in CI
- No write/admin mutation endpoints exposed
- Integration tests run against real containerized Kafka
- Operational load limits documented and tested

## v0.2 — Data Explorer

Capabilities:
- Record browsing by topic/partition
- Offset and timestamp navigation
- Key/value/header inspection
- JSON, UTF-8 text, binary/hex representation
- Avro, Protobuf, and JSON Schema decoding through registry adapters
- Bounded live tailing
- Filtering
- Data masking
- Permission-controlled export

Invariant: Kafka record payloads are not persisted by Kafdeck by default.

## v0.3 — Consumers & Diagnostics

Capabilities:
- Consumer groups
- Members and assignments
- Current and committed offsets
- Per-partition and aggregate lag
- Inactive/stalled consumer detection
- Rebalance visibility where available
- Diagnostic views and actionable explanations

## v0.4 — Safe Administration

Initial mutations:
- Create/alter/delete topics
- Increase partitions
- Alter topic configurations
- Produce records
- Reset consumer offsets
- Delete consumer groups

All mutations use the operation pipeline:

`Request -> Authorization -> Validation -> Risk Classification -> Preview -> Confirmation -> Execute -> Audit`

Risk classes:
- LOW
- MODERATE
- HIGH
- CRITICAL

Critical examples include topic deletion and ACL modification.

## v0.5 — Identity, Authorization & Audit

Capabilities:
- Local authentication for standalone deployments
- OIDC/OAuth2
- RBAC
- Cluster- and resource-scoped permissions
- Topic-pattern permissions
- Explicit metadata-vs-payload access separation
- Session management
- Data masking policies
- Tamper-evident audit trail design

Representative permissions:
- `cluster.read`
- `topic.read`, `topic.create`, `topic.alter`, `topic.delete`
- `record.read`, `record.produce`
- `consumer.read`, `consumer.offset.reset`, `consumer.delete`
- `acl.read`, `acl.manage`
- `schema.read`, `schema.manage`
- `connect.read`, `connect.manage`
- `settings.manage`

## v0.6 — Kafka Security Administration

Capabilities:
- ACL browser/editor
- SCRAM management where supported
- Quota visibility and management
- Principal/resource access analysis
- Effective-permission explanation (`Explain Access`)

## v0.7 — Kafka Ecosystem

Schema Registry:
- Confluent-compatible APIs
- Avro, Protobuf, JSON Schema
- Versions, references, compatibility, diffs

Kafka Connect:
- Multiple Connect clusters
- Connector/task state
- Pause/resume/restart
- Configuration and diagnostics

Provider expansion may include Redpanda, Confluent Platform, AWS MSK, and compatible Kafka services without coupling the core domain to vendor APIs.

## v0.8 — Observability

Capabilities:
- OpenTelemetry traces, metrics, logs
- Prometheus-compatible metrics endpoint
- Internal diagnostics and health
- Request and Kafka operation latency
- Error/timeout rates
- Broker/partition health indicators
- Resource-utilization visibility

## v0.9 — Hardening

Focus:
- Scale and soak tests
- Chaos/failure-path tests
- Threat-model validation
- Security review
- Accessibility validation
- Upgrade/migration testing
- Performance-regression gates
- Kubernetes/Helm deployment
- Backup/restore procedures
- Air-gap installation verification

## v1.0 — Stable Release

v1.0 requires:
- Stable public contracts or an explicit public-API policy
- Reliable persistence migrations
- Supported upgrade path
- Complete security baseline
- Tested Kafka compatibility matrix
- Documented deployment/operations model
- No known unresolved critical security vulnerability
- Release provenance, SBOM, signed artifacts, and reproducible release procedure

After v1.0, Semantic Versioning governs compatibility and releases.

## Post-v1.0 themes

Potential themes, subject to RFC approval:
- Approval workflows for critical operations
- Policy-as-code
- Historical observability store
- Fleet-scale cluster management
- Plugin/provider SDK
- Enterprise directory integrations
- Automation/API clients
- Operational recommendations with explainable evidence
