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
