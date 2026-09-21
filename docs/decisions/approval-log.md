# Project Approval Log

This file records accepted project-level approval gates. It is an auditable project record, not a replacement for ADRs/RFCs.

## Gate 0 — Product, Architecture and Engineering Baseline

- **Date:** 2026-09-16
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope:** Initial Kafdeck product/engineering governance baseline

### Accepted decisions

- Product positioning: **Kafdeck — The Open Source Kafka Control Plane**
- Official repository/documentation language: **English**
- License: **Apache License 2.0**
- Contribution attestation: **DCO**
- Backend: **ASP.NET Core / .NET 10 LTS**
- Frontend: **React + TypeScript**
- Architecture style: **Modular Monolith**
- Kafka client boundary: **Confluent.Kafka behind Kafdeck-owned ports/adapters**
- Kafka posture: **KRaft-era design; no direct ZooKeeper support**
- Compatibility direction: **currently supported Kafka 4.x lines as Tier 1, Kafka 3.9 latest patch as early Tier 2**
- Initial product operating mode: **read-only for v0.1**
- Deployment principle: **air-gapped/on-premise first-class, zero required cloud dependency**
- Telemetry principle: **no required external telemetry; any future usage telemetry is opt-in and requires governance review**
- Persistence: **provider abstraction; SQLite default/standalone and PostgreSQL production-oriented provider**
- Security baseline: **OWASP ASVS 5.0 L2 direction + OWASP API/Top 10 threat inputs + OpenSSF OSPS controls**
- Supply-chain direction: **SBOM, signed artifacts/containers, SLSA-aligned provenance**
- Observability: **OpenTelemetry**
- Development workflow: **short-lived branches, PR-only changes after bootstrap, squash merge, exact-head validation**

### Bootstrap exception

The repository was created completely empty and had no commit or branch object. Git hosting requires an initial commit before a feature/docs branch can be created. A one-time `README.md` bootstrap commit was therefore made directly to `main`. All subsequent changes are governed by the PR workflow.

### Open items

No Gate 0 decision remains open. Implementation-level details that do not alter this baseline may proceed through normal ADR/PR governance. Material changes require a new decision record and approval.

## Gate 1 — v0.1 Cluster Explorer Design

- **Date:** 2026-09-16
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope:** v0.1 product/runtime/security/API/resilience/test contract
- **RFC:** RFC-0001

### Accepted decisions

1. **Cluster registration:** configuration-driven immutable profiles for v0.1.
2. **Deployment access:** loopback/local-only by default; non-loopback access requires a deployment access token.
3. **Secret handling:** environment/file secret references only in v0.1; no credential database.
4. **Metadata refresh:** in-memory per-cluster snapshots, bounded TTLs, single-flight refresh and on-demand expensive configuration reads.
5. **Version/capability reporting:** capability-first; no authoritative guessed broker version.
6. **KRaft controller scope:** controller visibility through broker metadata only; no direct quorum administration in v0.1.
7. **Compatibility:** Tier 1 Kafka 4.3.1/4.2.1/4.1.2 and Tier 2 Kafka 3.9.2 at the design date.
8. **Health semantics:** evidence-based `Healthy/Degraded/Unavailable/Unknown`, with authorization/capability limitations represented separately.

### Owner approval

> **Gate 1 — v0.1 Cluster Explorer design با Decisions 1–8 و گزینه‌های پیشنهادی تأیید است.**

### Consequences

- RFC-0001 and the v0.1 design package are accepted.
- Kafka mutation features remain excluded from v0.1.
- Implementation may begin only after the remaining Phase 0 administrative repository controls are closed.

## Gate 2 — Long-Term Capability Map and Roadmap

- **Date:** 2026-09-16
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope:** v0.2+ capability map, roadmap sequencing, market positioning and architecture/security boundaries

### Accepted decisions

1. Governed Apache-2.0 control plane without a mandatory data-plane proxy.
2. Stateless where possible; SQLite standalone default and PostgreSQL optional; no mandatory Kafka internal state topics.
3. Identity/RBAC/Masking/Audit precede general Kafka mutation capabilities.
4. Provider compatibility is capability-profile based; KRaft first; no direct ZooKeeper integration.
5. Server-side Kafdeck masking is the core model; any future wire-path Gateway requires a separate RFC.
6. Search uses bounded scans with regex/CEL/jq-style deterministic filters plus ksqlDB integration; no arbitrary server-side JavaScript.
7. No unrestricted embedded web shell; Kafdeck CLI/API explorer/safe command tooling instead.
8. REST/CLI/Terraform/MCP share the same identity, RBAC, risk, approval and audit controls.
9. Auto-remediation is bounded and opt-in; chaos/fault injection belongs to controlled test/lab environments.
10. Resource targets are measured and published; no unproven `<100 MB` guarantee.
11. Internal stable extension ports come first; a public runtime plugin SDK is deferred until trust/versioning/sandbox policy matures.
12. API-first is continuous; CLI follows API contracts and Terraform follows stable declarative mutation contracts.

### Owner approval

> **Gate 2 — Long-Term Capability Map and Roadmap با Decisions 1–12 و گزینه‌های پیشنهادی تأیید است.**

### Consequences

- The consolidated 13-axis capability map becomes the authoritative long-term product target.
- The accepted v0.1 Gate 1 contract is unchanged.
- Roadmap sequencing is v0.1 Cluster Explorer → v0.2 Safe Data Explorer → v0.3 Consumers/Schemas/Ecosystem Read Views → v0.4 Identity/Policy/Masking/Audit → v0.5 Safe Administration → v0.6 Fleet/Kafka Security → v0.7 Developer/Streaming Ecosystem → v0.8 Observability/Automation/Platform APIs → v0.9 Governance/Hardening → v1.0 Stable.
- Material departures from these boundaries require a new RFC/approval gate.


## Gate 3 — v0.3 Safe Data Explorer + Server-Side Masking Scope

- **Date:** 2026-09-20
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope:** v0.3 product scope and sequencing reconciliation
- **Scope issue:** #75
- **RFC:** RFC-0003 planning package

### Accepted decision

**Option A — v0.3 Safe Data Explorer + Server-Side Masking**

Accepted scope includes bounded topic/partition record browsing, offset/timestamp navigation, bounded live tail, key/value/header inspection, JSON/text/binary views, read-only schema-assisted decoding, deterministic bounded filtering, authoritative server-side masking, separate `record.read` / `record.export` permissions, bounded export, hard load budgets, cancellation/isolation, audit, API/UI parity and benchmark evidence.

### Hard invariants

- no Kafka mutation,
- no record production/replay,
- no consumer-offset mutation,
- no payload persistence by default,
- no unbounded scan,
- no arbitrary server-side JavaScript,
- no general-purpose SQL stream engine,
- no masking bypass,
- authorization uncertainty fails closed,
- metadata permissions do not imply payload access.

### Sequencing consequence

Gate 2's capability principles remain accepted, but its original version-number mapping was superseded:

1. Issue #57 advanced Operator Identity/RBAC to v0.2; v0.2 is released.
2. Issue #75 assigns Safe Data Explorer + Server-Side Masking to v0.3.
3. Consumers/Schemas/Ecosystem Read Views move to v0.4.
4. v0.5+ remains unchanged unless a later approved scope gate supersedes it.

Historical Gate 2 text remains unchanged as auditable approval evidence.

### Owner approval

> **Option A — v0.3 Safe Data Explorer + Server-Side Masking تأیید است.**

### Implementation gate

Implementation begins only after the RFC-0003 planning package receives exact-head CI, independent review, resolved required review threads and governed merge.

## Gate 4 — v0.4 Consumers, Schemas & Ecosystem Read Views

- **Date:** 2026-09-21
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope issue:** #99
- **Tracker:** #100
- **RFC:** RFC-0004 planning package

### Accepted decision

Kafdeck v0.4 is **Consumers, Schemas & Ecosystem Read Views**.

Accepted scope includes consumer groups/members/assignments/committed offsets/lag, evidence-based diagnostics, read-only Schema Registry subject/version/reference/diff/compatibility inspection, read-only Kafka Connect status, safe ksqlDB discovery/metadata where configured, topic catalog foundations, API/UI parity, bounded provider isolation and release evidence.

### Implementation decisions

1. v0.4 remains fully non-mutating.
2. New permissions are independent: `consumer.read`, `schema.read`, `connect.read`, `ksql.read`, `catalog.read`.
3. No v0.4 action implies `record.read` or `record.export`.
4. Produce/consume rate data is shown only from an explicit trustworthy metrics provider; otherwise it is unavailable.
5. Historical rebalance/progress analysis is shown only with an explicit history provider; v0.4 has no mandatory durable history store.
6. v0.4 supports at most one optional Connect profile and one optional ksqlDB profile per Kafka cluster; multi-Connect remains later roadmap work.
7. ksqlDB arbitrary statement/query execution is excluded.
8. Ecosystem URLs are deployment-configured, validated origins; there is no generic proxy endpoint.
9. Connector secret-like fields are redacted server-side.
10. Topic catalog foundations are configuration-backed descriptive metadata and do not affect authorization.

### Hard invariants

- no Kafka mutation or produce/replay,
- no consumer-offset mutation/commit,
- no Schema Registry mutation,
- no Connect mutation,
- no ksqlDB query execution/mutation,
- no unbounded polling/listing,
- no arbitrary server-side code execution,
- no masking bypass,
- v0.3 payload authorization/masking/export controls remain unchanged.

### Owner approval

> **v0.4 Consumers / Schemas / Ecosystem Read Views تأیید است؛ طراحی و اجرای کامل آن را شروع کن.**

### Execution

W25–W31 may proceed autonomously under protected-main governance. Dependency-valid stacked work may continue while a review gate is pending; no approval may be fabricated. Tag/publication requires a separate explicit v0.4 release decision.


## Gate 5 — v0.5 Safe Administration & Controlled Mutations

- **Date:** 2026-09-21
- **Status:** SCOPE ACCEPTED
- **Authority:** Project Owner
- **Scope issue:** #121
- **Tracker:** #122
- **RFC:** RFC-0005 planning package

### Accepted decision

Kafdeck v0.5 is **Safe Administration & Controlled Mutations**.

The accepted scope introduces governed topic, record-production, consumer-offset/group, Schema Registry, Kafka Connect and DeleteRecords mutation capabilities behind the common server-authoritative pipeline:

`Request -> Authentication -> Authorization -> Capability Check -> Validation -> Risk Classification -> Preview -> Confirmation/Approval -> Execute -> Verify -> Audit`

### Accepted invariants

- explicit mutation authorization actions; read permission never implies write/admin permission,
- LOW / MODERATE / HIGH / CRITICAL server-owned risk classes,
- preview binding and stale-preview rejection,
- durable idempotency / operation state,
- truthful ambiguous/partial execution semantics,
- no blind retry after potentially successful external dispatch,
- no generic Kafka/provider command tunnel,
- no durable plaintext record-payload or connector-secret staging by default,
- CRITICAL operations require a distinct eligible approver,
- irreversible operations are never represented as generally reversible,
- release/tag/publication remains separately approval-bound.

### Owner approval

> **v0.5 Safe Administration & Controlled Mutations با mutation pipeline، risk classes، hard safety invariants، explicit authorization actions و non-goals این scope تأیید است؛ planning package را کامل کن و implementation را فقط بعد از governed planning admission فعال کن.**

### Implementation gate

Implementation remains blocked until the RFC-0005 planning package receives fresh exact-head required checks, fresh required CODEOWNER approval, resolved required review threads and protected-main merge. After admission, W32-W40 may proceed autonomously under Issue #122. v0.5 release/publication requires a later explicit owner decision.
