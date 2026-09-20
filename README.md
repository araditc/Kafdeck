# Kafdeck

**Kafdeck — The Open Source Kafka Control Plane**

Kafdeck is a secure, vendor-neutral, operations-first control plane for Apache Kafka.

**v0.1 Cluster Explorer is released. v0.2 Operator Identity/RBAC has passed its explicit release decision and is in governed publication.** Kafdeck remains intentionally read-only toward Kafka: it exposes metadata, health, broker/topic/partition information and configuration reads without Kafka mutation controls or payload browsing.

## v0.1 highlights

- multi-cluster read-only exploration,
- bounded Kafka load with per-cluster isolation, snapshots, deadlines and stale semantics,
- cluster/broker health and topic/partition visibility,
- TLS, mTLS and accepted SASL modes,
- React operator UI plus versioned HTTP API,
- single non-root OCI image,
- local-first deployment with token-gated non-loopback access,
- containerized Kafka compatibility/security validation,
- SBOM, vulnerability-scan and signed-image release workflow.

## v0.2 highlights

- OIDC operator identity using Authorization Code + PKCE,
- server-side sessions and strict access-mode separation,
- immutable default-deny RBAC with subject/group bindings,
- action-, cluster-, and resource-scoped authorization,
- backend-authoritative API/UI enforcement,
- structured security audit events while preserving the read-only Kafka boundary.

v0.1 release notes are in `docs/releases/v0.1.md`. v0.2 release notes and readiness evidence are under `docs/releases/`. v0.2 OIDC/RBAC and upgrade guidance is under `docs/operator/`.

## Project principles

- Safe by default.
- Read access is not data access.
- A management tool must never become a reliability risk to the system it manages.
- Air-gapped and on-premise deployments are first-class use cases.
- Vendor neutrality is a core requirement.
- Product, architecture, security, and engineering decisions are documented and versioned.

## Governance

Changes follow the repository pull-request, review, CI and release controls documented under `docs/` and enforced by the protected `main` ruleset.
