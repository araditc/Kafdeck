# ADR-0007 — Long-Running Mutation Progress Uses Subordinate Durable Checkpoints

- **Status:** Proposed for governed admission
- **Date:** 2026-09-23
- **Scope:** v0.6
- **Related:** ADR-0004, ADR-0006, RFC-0006, Issue #145, Issue #146

## Context

ADR-0006 established durable mutation operations, idempotency, execution claims and resource fencing for v0.5. v0.6 introduces operations whose provider-side execution may last substantially longer than one HTTP request or one worker lease: partition reassignment, broker/log-directory evacuation/decommission and bounded cross-cluster transfer/replication integration.

Embedding unbounded progress history inside the core operation aggregate would increase contention and blur the distinction between immutable admitted intent and changing execution evidence. A parallel job system, however, would create a second authority and weaken RFC-0005 safety invariants.

## Decision

Kafdeck retains **one authoritative `MutationOperation` identity and state machine** and introduces **typed subordinate durable progress/checkpoint records** for operation kinds that require long-running observation.

The parent operation remains authoritative for:
- actor/approver identity,
- canonical intent and preview hash,
- risk and approval,
- idempotency,
- resource claims,
- execution generation/fencing,
- dispatch boundary,
- terminal result,
- audit correlation.

A progress/checkpoint record may contain only safe bounded execution evidence:
- typed phase,
- safe provider execution identifier/token,
- completed/remaining target summary,
- safe per-target status where bounded,
- last observed fingerprint,
- evidence timestamp,
- next poll time,
- attempt sequence,
- parent execution fencing generation,
- optimistic version,
- bounded diagnostic code.

It must not contain raw Kafka record payloads, SCRAM secrets, credentials, generic provider response bodies or arbitrary opaque blobs.

## Concurrency and fencing

Every progress mutation requires the current parent execution fencing generation. A stale worker cannot update progress after losing its durable lease. Takeover after lease expiry must observe provider state before continuing and must not blindly repeat a potentially-applied step.

PostgreSQL is required for multi-instance/HA long-running execution. SQLite remains supported only for standalone/single-instance mutation execution.

## Polling

Polling authority is durable (`NextPollAtUtc` or equivalent), not an in-memory timer. Polling is bounded, cancellable and isolated from ordinary read workloads. Provider observation does not itself authorize new mutation dispatch.

## Consequences

Positive:
- preserves one mutation governance boundary,
- supports crash/restart and HA takeover,
- avoids rewriting large immutable intents for every progress update,
- makes long-running partial/ambiguous outcomes observable,
- keeps payload/secret material out of durable operation state.

Costs:
- adds persistence schema/migration and retention concerns,
- requires explicit progress DTOs per operation family,
- requires fencing-aware repository APIs and recovery tests,
- requires careful bounded per-target evidence design.

## Rejected alternatives

### Separate generic job engine

Rejected because it would create another execution authority, authorization path and retry model.

### Store full provider responses as checkpoints

Rejected because provider responses may be unbounded, unstable, sensitive and adapter-specific.

### Keep all progress only in memory

Rejected because reassignment/maintenance/transfer must survive process failure and HA worker takeover.

### Append all progress into the parent aggregate JSON

Rejected because it couples mutable execution telemetry to the immutable admitted plan and increases contention/storage growth.

## Implementation constraint

This ADR authorizes the architectural shape only after the planning PR is governed-merged. It does not itself authorize executable v0.6 provider mutations.