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
