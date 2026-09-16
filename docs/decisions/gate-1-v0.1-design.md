# Gate 1 — v0.1 Cluster Explorer Design

- **Status:** AWAITING PROJECT OWNER DECISION
- **Date prepared:** 2026-09-16
- **RFC:** RFC-0001

Gate 1 authorizes the detailed v0.1 product/runtime contract. It does not authorize Kafka mutation features.

## Decision 1 — Cluster registration

### Option A — Configuration-driven immutable profiles (**recommended**)

Clusters are defined at process startup through application configuration. Secrets are external references. Runtime UI/API edits are deferred.

**Impact:** smallest attack surface, no settings authorization problem before v0.5, excellent air-gap/container ergonomics, restart required for profile change.

### Option B — Persisted profiles editable in UI

**Impact:** better convenience but immediately requires authenticated settings administration, secure secret persistence, migrations, audit expectations and additional threat surface.

**Recommendation:** A for v0.1.

## Decision 2 — Browser/API deployment access before v0.5 Identity/RBAC

### Option A — Local-only default + mandatory static deployment token for non-loopback binding (**recommended**)

**Impact:** preserves safe-by-default without prematurely building the v0.5 identity system. Token is deployment access, not RBAC identity.

### Option B — Local-only with no supported remote mode

**Impact:** strongest simplification but makes container/server use impractical for many operators.

### Option C — Allow unauthenticated remote deployment with documentation warning

**Impact:** simplest code but incompatible with Kafdeck's security posture because Kafka metadata and topology are sensitive.

**Recommendation:** A.

## Decision 3 — Secret handling

### Option A — Environment/file secret references only in v0.1 (**recommended**)

**Impact:** no credential database or encryption-key lifecycle yet; integrates with Docker/Kubernetes/system secret injection.

### Option B — Encrypt and persist secrets in SQLite/PostgreSQL

**Impact:** enables UI-managed profiles but adds master-key management, backup/restore secret semantics and a larger breach surface.

**Recommendation:** A.

## Decision 4 — Metadata refresh model

### Option A — In-memory per-cluster snapshots, TTLs, single-flight refresh, on-demand expensive config reads (**recommended**)

**Impact:** bounded Kafka load and simple stateless observations; no historical state.

### Option B — Poll every metadata/config dimension continuously

**Impact:** fresher screens but materially higher cluster load and memory/CPU cost, especially with large topic counts.

**Recommendation:** A.

## Decision 5 — Version/capability reporting

### Option A — Capability-first; do not display guessed broker version as authoritative (**recommended**)

**Impact:** behavior follows what the cluster/client actually supports and avoids false version precision.

### Option B — Infer/display a broker version from protocol heuristics

**Impact:** superficially useful but can be inaccurate across rolling upgrades and compatible distributions.

**Recommendation:** A.

## Decision 6 — KRaft controller scope

### Option A — Show controller visible through broker metadata; no direct controller/quorum administration (**recommended**)

**Impact:** keeps v0.1 on standard broker Admin APIs and read-only topology scope.

### Option B — Add controller-quorum inspection in v0.1

**Impact:** richer KRaft diagnostics but expands client support, permissions, compatibility, tests and product scope.

**Recommendation:** A; revisit for diagnostics roadmap.

## Decision 7 — Initial Kafka compatibility matrix

### Recommended

Tier 1 CI:

- 4.3.1
- 4.2.1
- 4.1.2

Tier 2:

- 3.9.2

**Impact:** Tier 1 follows currently supported Apache Kafka releases as of the design date; Tier 2 preserves Gate 0's 3.9 compatibility direction.

## Decision 8 — v0.1 health semantics

### Option A — Evidence-based `Healthy/Degraded/Unavailable/Unknown` plus separate capability/access limitations (**recommended**)

**Impact:** avoids conflating ACL denial with Kafka outage and ensures health has explainable evidence.

### Option B — Single green/red connectivity indicator

**Impact:** simpler UI but hides partial failures, authorization limitations and partition anomalies.

**Recommendation:** A.

## Approval statement

If accepted, record:

> **Gate 1 — v0.1 Cluster Explorer design is approved with Decisions 1–8 using the recommended options.**

After acceptance:

1. change RFC-0001 and these design documents to ACCEPTED,
2. append Gate 1 to `docs/decisions/approval-log.md`,
3. merge the design PR with exact-head validation,
4. create the implementation scaffold PR,
5. do not introduce Kafka mutations.
