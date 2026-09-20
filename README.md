# Kafdeck

**Kafdeck — The Open Source Kafka Control Plane**

Kafdeck is a secure, vendor-neutral, operations-first control plane for Apache Kafka.

**v0.1 Cluster Explorer is released. v0.2 Operator Identity/RBAC is under release-readiness validation and is not yet released.** v0.1 is intentionally read-only: it exposes Kafka metadata, health, broker/topic/partition information and configuration reads without Kafka mutation controls or payload browsing.

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

Release notes are in `docs/releases/v0.1.md`. v0.1 operations guidance is in `docs/operator/v0.1-operator-guide.md`. v0.2 OIDC/RBAC and upgrade guidance is under `docs/operator/`; `docs/releases/v0.2-readiness.md` records readiness evidence without declaring a release.

## Project principles

- Safe by default.
- Read access is not data access.
- A management tool must never become a reliability risk to the system it manages.
- Air-gapped and on-premise deployments are first-class use cases.
- Vendor neutrality is a core requirement.
- Product, architecture, security, and engineering decisions are documented and versioned.

## Governance

Changes follow the repository pull-request, review, CI and release controls documented under `docs/` and enforced by the protected `main` ruleset.
