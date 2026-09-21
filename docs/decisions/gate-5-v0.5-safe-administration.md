# Gate 5 — v0.5 Safe Administration & Controlled Mutations

- **Status:** SCOPE ACCEPTED; PLANNING PACKAGE PROPOSED FOR GOVERNED ADMISSION
- **Date:** 2026-09-21
- **Authority:** Project Owner
- **Scope issue:** #121
- **Tracker:** #122
- **RFC:** RFC-0005
- **Preserves:** Gate 0-Gate 4, released v0.1-v0.4 boundaries

## Accepted owner scope

v0.5 is **Safe Administration & Controlled Mutations**.

The owner approved:
- the mutation pipeline,
- risk classes,
- hard safety invariants,
- explicit authorization actions,
- listed non-goals,
- completion of the planning package,
- implementation only after governed planning admission.

## Planning decisions

### Decision 1 — One mutation policy boundary

All admitted mutation clients use:

`Request -> Authentication -> Authorization -> Capability Check -> Validation -> Risk Classification -> Preview -> Confirmation/Approval -> Execute -> Verify -> Audit`

There is no privileged UI/API/provider bypass.

### Decision 2 — Durable operation state

Mutation capability requires durable operation state (ADR-0006).

Read-only mode remains database-optional. Mutation mode fails closed when supported persistence is unavailable.

### Decision 3 — Explicit actions

Mutation authorization uses typed actions:
`topic.create`, `topic.alter`, `topic.delete`, `record.produce`,
`consumer.offset.alter`, `consumer.delete`,
`schema.create`, `schema.alter`, `schema.delete`,
`connect.create`, `connect.alter`, `connect.delete`, `records.purge`.

No broad generic admin permission bypass is introduced.

### Decision 4 — Server-owned risk

LOW/MODERATE/HIGH/CRITICAL are server-calculated risk floors. Deployment policy may raise but never lower.

Topic delete and DeleteRecords purge are CRITICAL.

### Decision 5 — Approval

Every mutation requires preview + confirmation.

CRITICAL requires distinct eligible approver. Deployments that cannot establish durable distinct-principal approval cannot execute CRITICAL mutations.

### Decision 6 — Idempotency and ambiguity

Client retries bind through durable idempotency keys.

Once external dispatch may have occurred, a timeout can become `ExecutionUnknown`; Kafdeck does not blindly redispatch.

### Decision 7 — Typed provider ports

Provider/Kafka mutation adapters expose only admitted operation kinds. No arbitrary Kafka AdminClient command or provider HTTP proxy is public.

### Decision 8 — Payload/secret staging

Raw record payloads and connector secret values are not persisted by default merely to support workflow state. Where required, execution material is re-submitted and digest-bound to the preview.

### Decision 9 — Concurrency

Kafdeck uses durable operation claims + resource conflict claims and revalidates provider state before dispatch. Out-of-band provider changes can still invalidate a preview.

### Decision 10 — Release separation

Planning admission authorizes implementation workstreams W32-W40 only. It does not authorize v0.5 release/tag/OCI/GitHub Release/publication.

## Implementation admission gate

Implementation becomes ACTIVE only after the planning PR:
- passes fresh exact-head required checks,
- has fresh required CODEOWNER approval,
- has all required review threads resolved,
- merges through protected `main`.

## Owner approval

> **v0.5 Safe Administration & Controlled Mutations با mutation pipeline، risk classes، hard safety invariants، explicit authorization actions و non-goals این scope تأیید است؛ planning package را کامل کن و implementation را فقط بعد از governed planning admission فعال کن.**
