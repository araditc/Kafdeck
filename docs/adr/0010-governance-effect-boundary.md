# ADR-0010 — Governance at the existing effect boundary

- Status: Accepted; W72 PR #437, protected main `d8d9e6319b536760993d840ce084dfc832c543de`, exact-head CODEOWNER review and seven successful applicable main workflows; admission recorded in #426.
- Date: 2026-11-05
- Related: [RFC-0009](../rfcs/0009-v0.9-governance-enterprise-hardening.md), [ADR-0006](0006-durable-mutation-operations.md), [ADR-0009](0009-automation-principals-telemetry-declarative-ownership.md)

## Context

Business approval may outlive a mutation preview. Catalog owners may lack RBAC. Direct v0.8 routes, automation and recovered work must not bypass newly activated policy. Separate connection reads can deadlock SQLite or observe an inconsistent request/policy revision during an effect commit.

## Decision

Keep original actor, current authority, scoped resource identity, operation kind, risk, plan fingerprint, idempotency and restore generation in the existing mutation kernel. Add an immutable request-revision association to the operation; do not rewrite its material/fingerprint to attach approval. Request approval satisfies only the business gate. Every dispatch revalidates original actor and executor rights, current RBAC, active governance digest, required current request revision and critical distinct eligible human approval.

Policy is authority narrowing only. The conjunction of existing authorization and governance must succeed. The shared dispatch boundary serves every client and legacy direct route; hiding a button or adding a request controller is insufficient. No active policy means existing v0.8 behavior. A changed active governance digest makes an affected preview stale; no approval is silently transferred to a new plan.

Application-owned transaction ports atomically check live operation, policy/request revision, expected version and restore generation on the owning provider connection/transaction. Infrastructure owns SQL; modules do not query other modules' tables. SQLite uses bounded serialized writes; PostgreSQL uses row/CAS concurrency. No external broker/network effect occurs inside a long-lived database transaction. Existing durable dispatch/unknown-evidence protocol remains responsible for the commit-to-effect gap.

Expired/stale operation requires explicit replan and new immutable request revision; old approvals are invalid. Restore increments/fences generation before dispatch, reauthenticates and re-evaluates live policy; unresolved dispatched effects remain ExecutionUnknown until positive reconciliation evidence. Reject/cancel cannot undo dispatch, clear unknown or manufacture rollback.

## Consequences

The additional gate costs a bounded transactional read and requires client-wide negative tests. Business status and execution status remain separately visible. Defaults preserve disabled-mode compatibility, while enabling stricter policy can deliberately block old plans. No new identity provider, policy engine service, persistence technology or transport authority is introduced. Public plugin execution remains outside scope.

## Alternatives considered

Controller-only approval misses direct/client paths. Catalog-derived rights turn descriptive imports into privilege escalation. Automatic replan hides changed material. External executable policy introduces a new arbitrary execution and availability boundary. These approaches do not satisfy the accepted safety invariants.
