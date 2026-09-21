# Kafdeck

**Kafdeck — The Open Source Kafka Control Plane**

Kafdeck is a secure, vendor-neutral, operations-first control plane for Apache Kafka.

**v0.1 Cluster Explorer, v0.2 Operator Identity/RBAC, and v0.3 Safe Data Explorer + Server-Side Masking are released. v0.4 Consumers, Schemas & Ecosystem Read Views is active.** Kafdeck remains outside the Kafka data path and v0.4 preserves the no-mutation posture.

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
- structured security audit events.

## v0.3 highlights

- separately authorized bounded Kafka record browsing,
- offset/timestamp navigation and bounded live tail,
- Avro/Protobuf/JSON Schema decode through read-only Schema Registry integration,
- deterministic bounded filtering,
- authoritative server-side masking/redaction,
- separately authorized bounded export,
- explicit record/byte/time/rate/concurrency budgets,
- API/UI parity and release benchmark evidence,
- no Kafka produce/replay/offset mutation/payload persistence by default.

## v0.4 active scope

- consumer groups, members, assignments, committed offsets and lag,
- evidence-based consumer diagnostics,
- read-only Schema Registry subjects/versions/references/diff/compatibility inspection,
- read-only Kafka Connect status,
- safe ksqlDB discovery/metadata when configured,
- configuration-backed topic catalog foundations,
- no offset/schema/connect/ksql/Kafka mutation.

Release notes and release evidence are under `docs/releases/`. Operator and upgrade guidance is under `docs/operator/`.

## Project principles

- Safe by default.
- Read access is not data access.
- A management tool must never become a reliability risk to the system it manages.
- Air-gapped and on-premise deployments are first-class use cases.
- Vendor neutrality is a core requirement.
- Product, architecture, security, and engineering decisions are documented and versioned.

## Governance

Changes follow the repository pull-request, review, CI and release controls documented under `docs/` and enforced by the protected `main` ruleset.
