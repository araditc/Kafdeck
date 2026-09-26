# ADR-0008 — Governed Ecosystem Execution and Stable Provider Profiles

- **Status:** PROPOSED
- **Date:** 2026-09-26
- **Scope:** v0.7 / Issues #178, #180, #181
- **Related:** RFC-0007; ADR-0006 durable mutation operations; ADR-0007 durable fleet operation progress

## Context

v0.7 introduces external ecosystem effects beyond Kafka broker administration:

- Schema Registry lifecycle mutation;
- Kafka Connect lifecycle mutation;
- bounded Connect auto-restart;
- replay/reprocess/forwarding jobs;
- bounded mock/data generation;
- read-oriented ksqlDB query execution.

These effects target provider profiles whose identity can drift independently from the Kafka cluster profile. Some effects are one-shot HTTP calls, while others are finite multi-step jobs. Existing v0.5/v0.6 operation semantics already solve durable admission, approval, fencing and ambiguous external effects, but v0.7 also requires stable provider identity and explicit automation authority.

## Decision

### 1. Stable provider profile identity

Every ecosystem provider is addressed by a Kafdeck-owned stable profile ID plus a validated canonical origin fingerprint.

For Kafka Connect v0.7:

- new configuration uses `ConnectProfiles[]`;
- each profile has a stable `Id`;
- legacy singular `Connect` normalizes to profile ID `default`;
- legacy singular and profile-list configuration cannot coexist;
- profile IDs are unique per Kafka cluster profile;
- exact provider origin/auth/TLS policy participates in preview/fingerprint evidence.

A profile ID is not proof that the endpoint is unchanged. Runtime revalidation compares the current provider fingerprint to the admitted preview before each new external effect.

### 2. Existing durable mutation operation remains parent authority

v0.7 does not create a parallel job/automation persistence engine.

Schema/Connect mutations and long-running data jobs use the existing durable operation authority. Typed subordinate progress extends the parent snapshot/repository through versioned bounded records.

The parent owns:

- requester/approver identity;
- risk and confirmation;
- canonical intent/preview hash;
- lease/fencing;
- terminal/ambiguous state;
- audit sequence.

Subordinate progress may own only bounded domain-specific evidence such as:

- schema provider readback identity;
- connector/task attempt count and next eligible time;
- data-job per-partition checkpoint and cumulative budgets;
- generator cumulative counters;
- finite query execution status where persistence is required.

### 3. Automation never owns independent permanent authority

A restart policy or other automation rule is not itself a standing mutation credential.

Before each new external effect, Kafdeck must revalidate:

- active policy;
- current provider/profile identity;
- current capability;
- current action/resource authorization of the designated automation/requester principal;
- remaining finite budget;
- unresolved prior effect state;
- inherited replication/masking/security guards.

If the current principal is unavailable or no longer authorized, automation stops/blocks.

### 4. Dispatch marker precedes uncertain external effects

For any effect that cannot be proven idempotent across process restart, durable progress records a dispatch-start marker before I/O. A crash/timeout after that point remains ambiguous until bounded readback evidence proves a stronger result.

A new UI/API retry cannot erase or bypass the unresolved predecessor.

### 5. No raw payload or secret in durable progress

Durable v0.7 progress excludes:

- record values/keys/headers;
- generated payloads;
- connector secret bytes;
- Schema Registry/provider credentials;
- ksql query result rows.

Safe hashes/bindings are keyed/domain-separated where low-entropy secret material could otherwise be exposed to offline guessing.

### 6. Finite budgets are lifetime totals

Record/byte/rate/duration/retry budgets are cumulative lifetime values for the parent operation/policy activation.

They do not reset on:

- worker restart;
- lease renewal;
- failover;
- fresh confirmation;
- fresh approval;
- retry request;
- provider reconnect.

## Consequences

### Positive

- one governance/persistence model covers v0.5–v0.7 mutations;
- provider aliases cannot silently retarget an admitted operation;
- automation remains revocable and auditable;
- ambiguous outcomes remain truthful;
- HA failover cannot replenish finite budgets;
- secret/payload durability exposure stays minimized.

### Costs

- provider fingerprint/version migration requires explicit handling;
- some provider operations that look simple require durable dispatch/readback state;
- automated restart can stop when authority changes even if the connector remains unhealthy;
- adapters need typed readback semantics rather than generic HTTP success checks.

## Rejected alternatives

### Generic provider URL + method persistence

Rejected because it becomes a durable generic execution tunnel and makes policy/audit reasoning impossible.

### Separate scheduler database for auto-restart/data jobs

Rejected because it creates a second authority/fencing model and risks bypassing the mutation governance pipeline.

### Treat profile ID as immutable provider identity

Rejected because deployment configuration can retarget a stable alias.

### Automatic retry after timeout

Rejected because timeout does not prove non-application.

### Persist payloads to simplify replay

Rejected because it expands Kafdeck into a sensitive data store and violates the default no-payload-persistence boundary.

## Admission requirement

This ADR is not active until the W51 planning PR is admitted to protected `main`.
