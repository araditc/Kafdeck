# ADR-0007 — Durable Fleet Progress under One Mutation Authority

- Status: **PROPOSED FOR GOVERNED PLANNING ADMISSION**
- Date: 2026-09-23
- Extends: [ADR-0006](0006-durable-mutation-operations.md)
- Related: RFC-0006, #145, #146

## Context

At inspected baseline `e5de0ec142d5dd06ef52f59718c60c3109d1070a`, `IMutationOperationRepository` exposes optimistic saves, execution lease renewal, cluster slots and resource claims. `MutationOperationSnapshot` has one operation/claim identity; `MutationAuthorization.NormalizeTargets` intentionally accepts one expected action and one cluster. Neither is already a general durable multi-cluster workflow contract.

Fleet mutations can remain active at a broker after the dispatching process or its lease disappears. Record transfers can acknowledge destination writes before checkpoint persistence fails. A separate job engine or an automatically expiring resource lock would create an authorization or duplicate-effect boundary.

## Decision

Keep `MutationOperationSnapshot` as the sole approval, idempotency and final-outcome authority. Add versioned subordinate `FleetOperationProgress`, bounded typed `FleetStep` records, `FleetTransferCheckpoint` records and durable `FleetConflictObligation` records in the same configured database/provider. Their names are conceptual until W41 implementation; they are not existing tables.

All records reference one `OperationId`. Updates validate parent version, worker generation and step identity in one database transaction. Unique keys cover `(OperationId, StepId)`, `(OperationId, source topic identity, partition)` and canonical conflict identity. A transaction records dispatch intent before I/O and acknowledgement/checkpoint/state/audit event together after I/O. No database transaction spans Kafka I/O.

A renewable worker/observer lease and a durable outstanding-effect obligation are different things. Expiry of the former does not delete the latter. An obligation blocks incompatible new work across process restart until bounded readback establishes completion/non-application or a governed uncertainty disposition is admitted under the closed contract below. Manual editing of rows is not a supported clearance path.

### Governed uncertainty disposition

W41 MUST introduce a closed Kafdeck-local operation kind `FleetUncertaintyDisposition` and explicit authorization action `mutation.reconcile`. This operation never invokes Kafka/provider mutation APIs and cannot rewrite the historical dispatch evidence of the original operation. Its only purpose is to govern what Kafdeck may do when bounded readback cannot resolve an outstanding effect.

The disposition is always **CRITICAL** and requires:
- `mutation.reconcile` on every canonical conflict resource covered by the unresolved obligation,
- the complete current authorization conjunction that would be required for the original unresolved effect,
- a distinct eligible approver,
- immutable references to the original `OperationId`, step IDs, conflict keys, last readback evidence, provider capability/version and disposition reason,
- typed confirmation that uncertainty remains and that the disposition does **not** prove non-application,
- append-only audit evidence and a durable tombstone preventing redispatch of the original step/idempotency identity.

Closed disposition outcomes are:

1. `ObservedNonApplication` — system/readback evidence proves the external effect did not apply. This may release the corresponding obligation without a manual uncertainty override; evidence is retained.
2. `ObservedTerminalEffect` — system/readback evidence proves the effect reached a terminal applied/failed state. The parent result is reconciled from evidence and the corresponding obligation may release.
3. `QuarantineUnknown` — uncertainty is explicitly accepted for investigation, but the obligation remains blocking. This never enables conflicting dispatch.
4. `SupersedeUnknownForNewIntent` — the only manual path that can permit later conflicting work while the prior external effect remains unresolved. It is CRITICAL, preserves the original parent as `ExecutionUnknown`, retains a permanent no-redispatch tombstone, and marks the obligation as superseded rather than erased. Any later conflicting operation MUST bind the unresolved predecessor `OperationId`/step IDs into its own canonical preview, escalate to at least CRITICAL, obtain fresh independent approval, and revalidate current external state before any new effect. A disposition cannot itself perform or silently schedule that new mutation.

There is no generic `Clear`, `Ignore`, `MarkNotApplied`, or row-delete disposition. Provider credentials, repository write access, operator role names, or the original requester's stale permissions cannot satisfy `mutation.reconcile`. The dedicated action is a safety control only; it does not authorize any new provider capability.

W41/W50 MUST test denial without `mutation.reconcile`, denial without the original effect's current authorization conjunction, independent-approver enforcement, immutable evidence/tombstones, stale disposition preview, crash/retry idempotency, and proof that `QuarantineUnknown` cannot unblock conflicts. They MUST also test that `SupersedeUnknownForNewIntent` cannot redispatch the old step and that any subsequent conflicting intent carries the unresolved predecessor into its CRITICAL preview/approval.

Retain existing v0.5 enum numbers and canonical formats. A versioned v0.6 compound-authorization schema supplies server-derived conjunctive targets for the new closed kinds only. Old kinds still reject mixed actions/clusters. `ClusterId` remains the primary coordination cluster; the full ordered cluster set and physical identities are additionally bound for multi-cluster operations. Parent risk is at least the maximum child risk and all child effects are materialized in the approved plan.

## Recovery and secret-material consequence

Recovery may resume observation under a dedicated least-privilege read path without impersonating a requester. It cannot submit a new provider mutation without current requester/required-approver authorization and valid operation admission. Saved claims or worker credentials are not a source of user authorization.

For SCRAM, independent approval completes against non-secret intent and HMAC-bound material. A later requester finalization supplies the secret again. Only the common coordinator may dispatch while it is in memory. Process loss before dispatch needs re-submission; process loss after possible dispatch remains unknown. This is a narrow extension of the delayed-material restriction described in the v0.5 runtime design, not secret persistence or approval bypass.

## Migration and rollback

Use additive versioned migrations for SQLite standalone and PostgreSQL HA. Mutation startup checks schema compatibility, migration ownership and deployment execution-version compatibility. Old v0.5 executors must not run beside active v0.6 executors: their lease cleanup and closed-kind assumptions cannot safely interpret new obligations. Use a maintenance window/drain, apply migration, verify backup/restore, then enable v0.6. Read-only availability is assessed separately.

A rollback disables dispatch, preserves unknown/in-progress obligations and all evidence, and restores a compatible application/schema pair only after reconciliation. A stale database restore must never replay Ready/Executing work automatically; restoration enters reconciliation-only mode until external effects are inspected. Do not garbage-collect live obligations, unresolved operations or their idempotency bindings to meet retention limits; refuse new work at capacity instead.

## Rejected alternatives

A parallel job store/executor duplicates policy. In-memory mutexes do not provide HA exclusion. Treating lease expiry as proof of non-application is unsafe. Kafka internal state topics add a circular managed-cluster dependency. Persisting raw records/passwords to enable restart violates the approved data boundary. A generic SQL/JSON workflow interpreter is not a typed operation contract. An untyped/manual obligation-clear button is rejected because it can turn uncertainty into duplicate external effects.

## Consequences

The operation store becomes the durable source of workflow truth but cannot fence Kafka itself. Generation checks fence Kafdeck writers only; after possible dispatch, successors observe instead of redispatching. Conservative blocking is preferable to duplicate side effects. When uncertainty cannot be resolved automatically, only the explicit CRITICAL reconciliation contract above can govern supersession; it never changes historical evidence or silently authorizes a new mutation. W41 must prove this on both persistence providers and retain v0.5 behavior before any fleet adapter is admitted.
