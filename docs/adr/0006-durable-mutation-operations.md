# ADR-0006: Durable Mutation Operation State

- Status: Proposed for governed admission
- Date: 2026-09-21
- Related: RFC-0005 / Issue #121 / Issue #122

## Context

Kafdeck v0.1-v0.4 can operate read-only without durable application persistence. v0.5 introduces external side effects where process restart, concurrent requests, duplicate HTTP retries, independent approval and ambiguous provider outcomes can otherwise cause unsafe duplicate execution or loss of evidence.

Running destructive operations only from transient HTTP request state would make it impossible to reliably bind preview/confirmation/approval, coordinate multiple instances, recover operation status after restart, or distinguish a pre-dispatch failure from a possibly-applied mutation.

## Decision

v0.5 mutation capability requires durable Kafdeck operation persistence through the existing provider strategy from ADR-0004.

- SQLite supports standalone/single-instance mutation execution.
- PostgreSQL is required for multi-instance/HA mutation execution.
- A deployment without writable supported persistence remains read-only; mutation capability is unavailable rather than downgraded to transient execution.
- CRITICAL operations additionally require a durable independent-principal approval record.
- Mutation operation state stores non-secret normalized intent and cryptographic digests for intentionally ephemeral execution material.
- Raw Kafka record payloads and secret connector values are not durably persisted by default.
- Execution is claimed atomically from durable state; post-dispatch ambiguous failures are not blindly retried.
- Resource conflict claims are durable Kafdeck coordination records but do not pretend to lock out external Kafka/provider administrators.

## Consequences

Positive:
- request retries can be idempotent,
- process restart preserves operation truth,
- approval evidence is auditable,
- HA workers can coordinate,
- `ExecutionUnknown` can be represented honestly,
- destructive operations fail closed when state durability is unavailable.

Trade-offs:
- v0.5 mutation mode is no longer database-optional,
- PostgreSQL becomes an operational prerequisite for HA mutation execution,
- database migrations/backups now protect mutation/audit workflow state,
- operations with intentionally non-persistent payload/secret material need re-submission and digest verification,
- Kafdeck still cannot provide exactly-once guarantees beyond what external Kafka/provider APIs expose.

## Rejected alternatives

### Execute mutations directly in HTTP requests without durable state
Rejected because restart/network retry/approval/concurrency behavior is unsafe.

### Store mutation workflow state in Kafka internal topics
Rejected because Kafdeck's application-state strategy does not require write access to the managed Kafka cluster and must avoid a circular dependency.

### Persist all record/connector execution material in plaintext
Rejected because it creates an unnecessary payload/secret database and violates existing data-handling principles.

## Security note

Persistence durability does not authorize a mutation. Identity/RBAC, capability checks, risk classification, preview binding, confirmation/approval and pre-dispatch revalidation remain mandatory independent controls.
