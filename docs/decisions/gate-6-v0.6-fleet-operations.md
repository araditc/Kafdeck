# Gate 6 — v0.6 Fleet Operations, Broker Maintenance & Kafka Security

- **Date:** 2026-09-23
- **Status:** SCOPE ACCEPTED / PLANNING AUTHORIZED
- **Authority:** Project Owner
- **Scope issue:** #145
- **Tracker:** #146
- **RFC:** RFC-0006 planning package

## Accepted decision

Kafdeck v0.6 is **Fleet Operations, Broker Maintenance & Kafka Security**.

Accepted capability families:
- typed Kafka ACL administration,
- SCRAM administration where supported,
- quota visibility/management,
- evidence-based principal/resource access analysis,
- dynamic broker/cluster configuration for server-allowlisted keys proven dynamically alterable,
- preferred leader election,
- explicit partition/replica reassignment and replication-factor change,
- measured/derived/unknown transfer-impact evidence,
- reassignment throttling/progress and skew analysis,
- capability-gated Kafka 4.x broker/log-directory evacuation/decommission,
- bounded cross-cluster transfer/clone,
- MirrorMaker 2/provider replication integration rather than a bespoke replication engine.

## Inherited governance

The v0.5 mutation kernel remains the single execution boundary. All RFC-0005 controls continue to apply: explicit authorization, server-owned risk floors, preview binding, durable idempotency/operation state/resource claims, stale-preview revalidation, independent CRITICAL approval, truthful ambiguous/partial outcomes, no blind retry after possible dispatch, no generic provider tunnel and no durable raw payload/secret staging by default.

## Additional Gate 6 boundaries

- no arbitrary ACL/SCRAM/quota/config passthrough,
- SCRAM secrets are write-only/ephemeral and never retrievable,
- long-running operations use durable lease/fencing and typed bounded progress,
- reassignment/decommission uses materialized plans and current-state fingerprints,
- no fabricated broker read-only/cordon primitive,
- RF reductions are CRITICAL and must satisfy safety guardrails,
- cross-cluster movement requires independent source record read/export and destination produce authorization plus transfer permission,
- provider support is capability-profile based and unknown mutation capability fails closed,
- v0.7+ remains inactive.

## Owner approval

> **v0.6 Fleet Operations, Broker Maintenance & Kafka Security طبق scope و safety boundaries ثبت‌شده در Issue #145 تأیید است؛ planning package را کامل کن و implementation را فقط بعد از governed planning admission شروع کن.**

## Planning gate

Planning is authorized. Implementation remains blocked until RFC-0006, product/security/runtime/ADR/API/test/implementation documents, roadmap and approval-log reconciliation pass exact-head CI/review and are merged through protected `main` with the expected-head guard.

v0.6 release/tag/publication remains a separate explicit owner decision.