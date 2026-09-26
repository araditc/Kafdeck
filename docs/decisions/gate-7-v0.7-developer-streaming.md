# Gate 7 — v0.7 Developer & Streaming Ecosystem Platform Scope

- **Date:** 2026-09-26
- **Status:** SCOPE ACCEPTED; PLANNING ADMISSION PENDING
- **Authority:** Project Owner, Ammar Heidari
- **Scope issue:** #178
- **Tracker:** #180
- **Planning workstream:** #181 / W51
- **RFC:** RFC-0007 planning package

## Owner approval

> **v0.7 scope در Issue #178 طبق متن فعلی تأیید است. آماده‌سازی W51 governed planning package مجاز است؛ implementation فقط پس از protected-main planning admission شروع شود.**

## Accepted scope

v0.7 is **Developer & Streaming Ecosystem Platform**.

Accepted workstreams:

- W51 — governed planning package, contracts and compatibility matrix;
- W52 — Schema Registry lifecycle administration;
- W53 — schema developer tooling;
- W54 — Kafka Connect multi-cluster administration;
- W55 — bounded Connect auto-restart;
- W56 — controlled CBOR/XML/MessagePack SerDe tooling;
- W57 — finite governed replay/reprocess/DLQ/cross-topic/cross-cluster forwarding jobs;
- W58 — bounded Smart Mock/Data Generator;
- W59 — bounded ksqlDB query/editor plus evidence-based Streams/state-store/lineage views;
- W60 — developer/operator UX, command palette, topic catalog and v0.7 readiness.

## Accepted hard boundaries

1. Existing OIDC/RBAC, explicit actions, risk, preview, confirmation/approval, durable operation, fencing, ambiguity and audit remain authoritative.
2. Read permissions never imply mutation/data-execution permissions.
3. No generic Schema Registry, Connect or ksqlDB HTTP proxy.
4. No CLI/raw-protocol/reflection/sidecar bypass for missing provider primitives.
5. Provider secrets and connector write-only material remain server-side and do not enter browser-visible state/audit/logs.
6. MM2/replication activation guards apply to all new Connect effect-capable routes.
7. Replay/reprocess/forward/generator/auto-restart are finite and bounded; no unlimited setting.
8. Automated effects revalidate current authority and policy before every new dispatch.
9. Raw record/generated/query payload persistence remains off by default.
10. Server-side masking/export boundaries cannot be bypassed by byte-preserving data jobs.
11. ksqlDB v0.7 is read-oriented/bounded; persistent-query/DDL/DML mutation is not admitted.
12. Lineage distinguishes observed from inferred evidence; inference never grants authority.
13. Public runtime plugin loading remains outside v0.7.
14. No mandatory Kafdeck data-plane proxy is introduced.
15. Release/tag/GHCR/GitHub Release publication remains a later separate owner decision.

## Planning authorization

W51 planning preparation is authorized.

The W51 package may define detailed contracts, provider matrices, risk floors, hard budgets, API schemas, threat controls, tests and workstream dependencies, provided they stay inside the accepted scope and do not activate runtime implementation.

## Implementation gate

W52–W60 implementation is blocked until the complete W51 planning package:

- is proposed from exact protected `main`;
- passes all applicable exact-head repository checks;
- receives substantive architecture/security review;
- receives fresh required CODEOWNER approval;
- has zero unresolved required review threads;
- remains mergeable/base-current;
- is merged with expected-head protection;
- is verified on protected `main`;
- and tracker #180 records planning admission.

## Release gate

Planning admission does not authorize a v0.7 release. W60 readiness must complete before the owner separately authorizes tag, OCI promotion/signing and GitHub Release publication.

## Relationship to prior gates

Gate 7 extends Gate 2's long-term roadmap and inherits all applicable Gate 4–6 security/governance decisions. Earlier release identities and historical approval records remain immutable evidence.
