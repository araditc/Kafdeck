# Gate 1 — v0.1 Cluster Explorer Design

- **Status:** ACCEPTED
- **Date prepared:** 2026-09-16
- **Date accepted:** 2026-09-16
- **Authority:** Project Owner
- **RFC:** RFC-0001

Gate 1 authorizes the detailed v0.1 product/runtime contract. It does not authorize Kafka mutation features.

## Decision 1 — Cluster registration

### Option A — Configuration-driven immutable profiles (**accepted**)

Clusters are defined at process startup through application configuration. Secrets are external references. Runtime UI/API edits are deferred.

**Impact:** smallest attack surface, no settings authorization problem before v0.5, excellent air-gap/container ergonomics, restart required for profile change.

### Option B — Persisted profiles editable in UI

**Impact:** better convenience but immediately requires authenticated settings administration, secure secret persistence, migrations, audit expectations and additional threat surface.

**Decision:** Option A for v0.1.

## Decision 2 — Browser/API deployment access before v0.5 Identity/RBAC

### Option A — Local-only default + mandatory static deployment token for non-loopback binding (**accepted**)

**Impact:** preserves safe-by-default without prematurely building the v0.5 identity system. Token is deployment access, not RBAC identity.

### Option B — Local-only with no supported remote mode

**Impact:** strongest simplification but makes container/server use impractical for many operators.

### Option C — Allow unauthenticated remote deployment with documentation warning

**Impact:** simplest code but incompatible with Kafdeck's security posture because Kafka metadata and topology are sensitive.

**Decision:** Option A.

## Decision 3 — Secret handling

### Option A — Environment/file secret references only in v0.1 (**accepted**)

**Impact:** no credential database or encryption-key lifecycle yet; integrates with Docker/Kubernetes/system secret injection.

### Option B — Encrypt and persist secrets in SQLite/PostgreSQL

**Impact:** enables UI-managed profiles but adds master-key management, backup/restore secret semantics and a larger breach surface.

**Decision:** Option A.

## Decision 4 — Metadata refresh model

### Option A — In-memory per-cluster snapshots, TTLs, single-flight refresh, on-demand expensive config reads (**accepted**)

**Impact:** bounded Kafka load and simple stateless observations; no historical state.

### Option B — Poll every metadata/config dimension continuously

**Impact:** fresher screens but materially higher cluster load and memory/CPU cost, especially with large topic counts.

**Decision:** Option A.

## Decision 5 — Version/capability reporting

### Option A — Capability-first; do not display guessed broker version as authoritative (**accepted**)

**Impact:** behavior follows what the cluster/client actually supports and avoids false version precision.

### Option B — Infer/display a broker version from protocol heuristics

**Impact:** superficially useful but can be inaccurate across rolling upgrades and compatible distributions.

**Decision:** Option A.

## Decision 6 — KRaft controller scope

### Option A — Show controller visible through broker metadata; no direct controller/quorum administration (**accepted**)

**Impact:** keeps v0.1 on standard broker Admin APIs and read-only topology scope.

### Option B — Add controller-quorum inspection in v0.1

**Impact:** richer KRaft diagnostics but expands client support, permissions, compatibility, tests and product scope.

**Decision:** Option A; revisit for diagnostics roadmap.

## Decision 7 — Initial Kafka compatibility matrix

### Accepted

Tier 1 CI:

- 4.3.1
- 4.2.1
- 4.1.2

Tier 2:

- 3.9.2

**Impact:** Tier 1 follows currently supported Apache Kafka releases as of the design date; Tier 2 preserves Gate 0's 3.9 compatibility direction.

## Decision 8 — v0.1 health semantics

### Option A — Evidence-based `Healthy/Degraded/Unavailable/Unknown` plus separate capability/access limitations (**accepted**)

**Impact:** avoids conflating ACL denial with Kafka outage and ensures health has explainable evidence.

### Option B — Single green/red connectivity indicator

**Impact:** simpler UI but hides partial failures, authorization limitations and partition anomalies.

**Decision:** Option A.

## Approval statement

Accepted by the Project Owner on 2026-09-16:

> **Gate 1 — v0.1 Cluster Explorer design با Decisions 1–8 و گزینه‌های پیشنهادی تأیید است.**

## Consequences

1. RFC-0001 and its v0.1 design documents are accepted.
2. The design PR may be merged with exact-head validation.
3. The implementation scaffold may be prepared only after Phase 0 administrative repository controls are closed.
4. Kafka mutation features remain outside v0.1.
