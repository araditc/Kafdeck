# Gate 2 — Long-Term Capability Map and Roadmap

- **Status:** AWAITING PROJECT OWNER DECISION
- **Date prepared:** 2026-09-16
- **Scope:** v0.2+ capability roadmap, market positioning and architecture boundaries
- **Preserves:** Gate 0 and accepted Gate 1/v0.1

Gate 2 does not change the accepted v0.1 scope. It establishes how the consolidated 13-axis capability inventory is incorporated into the long-term Kafdeck roadmap without weakening the project's security, vendor-neutrality or operational-safety principles.

## Decision 1 — Competitive positioning

### Option A — Governed Apache-2.0 control plane without mandatory proxy (**recommended**)

Position Kafdeck around governed safe operations, zero-write observation, explainable evidence, air-gap operation and API/agent parity.

### Option B — Compete primarily on feature count/message browsing

This directly collides with mature OSS products such as Kafbat and gives Kafdeck a weaker durable identity.

**Recommendation:** A.

## Decision 2 — Kafdeck application state

### Option A — Stateless when possible; SQLite embedded default; PostgreSQL optional; no mandatory Kafka internal state topics (**recommended**)

Read-only Kafdeck can run with Kafka DESCRIBE/READ-class permissions and does not mutate a customer's cluster for its own persistence.

### Option B — Store Kafdeck durable state inside the managed Kafka cluster by default

This introduces internal topics, Kafka write ACLs, bootstrap/circular dependency and a surprising operational footprint.

**Recommendation:** A.

## Decision 3 — Identity before general mutation

### Option A — Move Identity/RBAC/Masking/Audit before Safe Administration (**recommended**)

General write/admin operations ship only after user identity, authorization and audit semantics exist.

### Option B — Keep mutations before full identity/RBAC

A deployment access token is adequate for v0.1 observation but is not a sufficient principal/authorization model for multi-user destructive administration.

**Recommendation:** A.

## Decision 4 — Provider and ZooKeeper compatibility

### Option A — Capability profiles; KRaft first; no direct ZooKeeper; broker-API compatibility for explicitly tested ZooKeeper-mode clusters (**recommended**)

Managed services such as Event Hubs receive explicit limited profiles.

### Option B — Claim generic support for KRaft, ZooKeeper and every Kafka-compatible service

This creates false precision and forces Kafdeck to model interfaces that are not equivalent.

**Recommendation:** A.

## Decision 5 — Data masking boundary

### Option A — Server-side Kafdeck masking on every authorized data-read path; optional wire-path Gateway only in a future separate RFC (**recommended**)

Masking begins with the first payload-reading release (v0.2). Unauthorized clear values never reach browser/API/export.

### Option B — Put Kafdeck in the Kafka data path for wire masking from the main product

This changes Kafdeck from out-of-band control plane into a producer/consumer availability dependency.

**Recommendation:** A.

## Decision 6 — Search/query execution

### Option A — Bounded scan + regex/CEL/jq-style deterministic filters + ksqlDB integration (**recommended**)

No arbitrary server-side JavaScript and no fixed millions-msg/s promise. Native indexed/SQL architecture can be proposed later with explicit storage/load design.

### Option B — Arbitrary JS plus native SQL/unbounded scans from the initial Data Explorer

This substantially increases security/resource risk and duplicates dedicated stream-processing systems.

**Recommendation:** A.

## Decision 7 — Embedded terminal

### Option A — No unrestricted web shell; provide Kafdeck CLI, API explorer and safe command-generation helpers (**recommended**)

### Option B — Browser terminal capable of invoking kcat/kafka CLI on the Kafdeck host

This creates a high-value RCE/credential escape surface that is difficult to constrain consistently with RBAC.

**Recommendation:** A.

## Decision 8 — Automation and AI/MCP

### Option A — API/CLI/Terraform/MCP use the same scoped identity, RBAC, risk, approval and audit pipeline (**recommended**)

### Option B — Separate privileged automation interface

This would create a governance bypass and invalidate human-facing controls.

**Recommendation:** A.

## Decision 9 — Auto-remediation and chaos

### Option A — Bounded, opt-in remediation policies; chaos/fault injection belongs to controlled test/lab infrastructure (**recommended**)

Connector auto-restart requires limits/backoff/circuit breaker. Kafdeck does not expose unrestricted production broker/network failure injection.

### Option B — General self-healing and chaos actions directly available from normal production UI

Too easy to create restart storms or intentionally damage production availability.

**Recommendation:** A.

## Decision 10 — Lightweight deployment claim

### Option A — Set and publish measured RSS/CPU/startup budgets; optimize aggressively but do not promise `<100 MB` until proven (**recommended**)

Distribution target is self-contained .NET binary + single OCI image + Helm.

### Option B — Make `<100 MB RAM` a product requirement immediately

It may distort architecture before representative TLS/Kafka/UI/metrics workloads are measured.

**Recommendation:** A.

## Decision 11 — Plugin model

### Option A — Internal stable ports/adapters first; public runtime plugin SDK after contract/signing/sandbox/versioning policy matures (**recommended**)

### Option B — Dynamic third-party plugin loading early

Early runtime plugins magnify supply-chain and compatibility risk before Kafdeck's public contracts stabilize.

**Recommendation:** A.

## Decision 12 — API parity, CLI and Terraform

### Option A — API-first is continuous; CLI follows the API; Terraform follows stable declarative mutation contracts (**recommended**)

### Option B — Build UI features first and retrofit API/IaC later

This creates divergent behavior and makes governance/automation inconsistent.

**Recommendation:** A.

## Resulting roadmap shape

- v0.1 Cluster Explorer — accepted/read-only
- v0.2 Safe Data Explorer + server-side masking
- v0.3 Consumers, Schemas & read-only ecosystem views
- v0.4 Identity, Policy, Masking & Audit
- v0.5 Safe Administration
- v0.6 Fleet Operations, Broker Maintenance & Kafka Security
- v0.7 Developer & Streaming Ecosystem Platform
- v0.8 Observability, Automation & Platform APIs
- v0.9 Governance, Enterprise Operations & Hardening
- v1.0 Stable Release

## Approval statement

If accepted, record:

> **Gate 2 — Long-Term Capability Map and Roadmap is approved with Decisions 1–12 using the recommended options.**

After acceptance, this proposal can be merged as the authoritative v0.2+ roadmap without changing the already accepted v0.1 contract.
