# Project Approval Log

## Bounded v1.0 G06 / E01 / Q01 request — 2026-11-13

The owner explicitly requested resolving G06 and the still-open E01/Q01 in #469 while preserving the bounded v1.0 stabilization scope and reusing successful v0.9 evidence. [ADR-0011](../adr/0011-managed-state-format-freeze.md) records the proposed format-freeze/verified-restore policy. Its acceptance is conditional on the linked implementation PR's exact-head CI, required CODEOWNER review and protected-main admission recorded in #469. Existing authorization, mutation generation fences, signed v0.9 bytes and the 128-entry/30-minute in-memory credential limits remain. This request and technical admission authorize no v1.0 signing or publication; final RC/media and owner release decisions remain separate gates.

This file records accepted project-level approval gates. It is an auditable project record, not a replacement for ADRs/RFCs.

## Gate 0 — Product, Architecture and Engineering Baseline

- **Date:** 2026-09-16
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope:** Initial Kafdeck product/engineering governance baseline

### Accepted decisions

- Product positioning: **Kafdeck — The Open Source Kafka Control Plane**
- Official repository/documentation language: **English**
- License: **Apache License 2.0**
- Contribution attestation: **DCO**
- Backend: **ASP.NET Core / .NET 10 LTS**
- Frontend: **React + TypeScript**
- Architecture style: **Modular Monolith**
- Kafka client boundary: **Confluent.Kafka behind Kafdeck-owned ports/adapters**
- Kafka posture: **KRaft-era design; no direct ZooKeeper support**
- Compatibility direction: **currently supported Kafka 4.x lines as Tier 1, Kafka 3.9 latest patch as early Tier 2**
- Initial product operating mode: **read-only for v0.1**
- Deployment principle: **air-gapped/on-premise first-class, zero required cloud dependency**
- Telemetry principle: **no required external telemetry; any future usage telemetry is opt-in and requires governance review**
- Persistence: **provider abstraction; SQLite default/standalone and PostgreSQL production-oriented provider**
- Security baseline: **OWASP ASVS 5.0 L2 direction + OWASP API/Top 10 threat inputs + OpenSSF OSPS controls**
- Supply-chain direction: **SBOM, signed artifacts/containers, SLSA-aligned provenance**
- Observability: **OpenTelemetry**
- Development workflow: **short-lived branches, PR-only changes after bootstrap, squash merge, exact-head validation**

### Bootstrap exception

The repository was created completely empty and had no commit or branch object. Git hosting requires an initial commit before a feature/docs branch can be created. A one-time `README.md` bootstrap commit was therefore made directly to `main`. All subsequent changes are governed by the PR workflow.

### Open items

No Gate 0 decision remains open. Implementation-level details that do not alter this baseline may proceed through normal ADR/PR governance. Material changes require a new decision record and approval.

## Gate 1 — v0.1 Cluster Explorer Design

- **Date:** 2026-09-16
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope:** v0.1 product/runtime/security/API/resilience/test contract
- **RFC:** RFC-0001

### Accepted decisions

1. **Cluster registration:** configuration-driven immutable profiles for v0.1.
2. **Deployment access:** loopback/local-only by default; non-loopback access requires a deployment access token.
3. **Secret handling:** environment/file secret references only in v0.1; no credential database.
4. **Metadata refresh:** in-memory per-cluster snapshots, bounded TTLs, single-flight refresh and on-demand expensive configuration reads.
5. **Version/capability reporting:** capability-first; no authoritative guessed broker version.
6. **KRaft controller scope:** controller visibility through broker metadata only; no direct quorum administration in v0.1.
7. **Compatibility:** Tier 1 Kafka 4.3.1/4.2.1/4.1.2 and Tier 2 Kafka 3.9.2 at the design date.
8. **Health semantics:** evidence-based `Healthy/Degraded/Unavailable/Unknown`, with authorization/capability limitations represented separately.

### Owner approval

> **Gate 1 — v0.1 Cluster Explorer design با Decisions 1–8 و گزینه‌های پیشنهادی تأیید است.**

### Consequences

- RFC-0001 and the v0.1 design package are accepted.
- Kafka mutation features remain excluded from v0.1.
- Implementation may begin only after the remaining Phase 0 administrative repository controls are closed.

## Gate 2 — Long-Term Capability Map and Roadmap

- **Date:** 2026-09-16
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope:** v0.2+ capability map, roadmap sequencing, market positioning and architecture/security boundaries

### Accepted decisions

1. Governed Apache-2.0 control plane without a mandatory data-plane proxy.
2. Stateless where possible; SQLite standalone default and PostgreSQL optional; no mandatory Kafka internal state topics.
3. Identity/RBAC/Masking/Audit precede general Kafka mutation capabilities.
4. Provider compatibility is capability-profile based; KRaft first; no direct ZooKeeper integration.
5. Server-side Kafdeck masking is the core model; any future wire-path Gateway requires a separate RFC.
6. Search uses bounded scans with regex/CEL/jq-style deterministic filters plus ksqlDB integration; no arbitrary server-side JavaScript.
7. No unrestricted embedded web shell; Kafdeck CLI/API explorer/safe command tooling instead.
8. REST/CLI/Terraform/MCP share the same identity, RBAC, risk, approval and audit controls.
9. Auto-remediation is bounded and opt-in; chaos/fault injection belongs to controlled test/lab environments.
10. Resource targets are measured and published; no unproven `<100 MB` guarantee.
11. Internal stable extension ports come first; a public runtime plugin SDK is deferred until trust/versioning/sandbox policy matures.
12. API-first is continuous; CLI follows API contracts and Terraform follows stable declarative mutation contracts.

### Owner approval

> **Gate 2 — Long-Term Capability Map and Roadmap با Decisions 1–12 و گزینه‌های پیشنهادی تأیید است.**

### Consequences

- The consolidated 13-axis capability map becomes the authoritative long-term product target.
- The accepted v0.1 Gate 1 contract is unchanged.
- Roadmap sequencing is v0.1 Cluster Explorer → v0.2 Safe Data Explorer → v0.3 Consumers/Schemas/Ecosystem Read Views → v0.4 Identity/Policy/Masking/Audit → v0.5 Safe Administration → v0.6 Fleet/Kafka Security → v0.7 Developer/Streaming Ecosystem → v0.8 Observability/Automation/Platform APIs → v0.9 Governance/Hardening → v1.0 Stable.
- Material departures from these boundaries require a new RFC/approval gate.


## Gate 3 — v0.3 Safe Data Explorer + Server-Side Masking Scope

- **Date:** 2026-09-20
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope:** v0.3 product scope and sequencing reconciliation
- **Scope issue:** #75
- **RFC:** RFC-0003 planning package

### Accepted decision

**Option A — v0.3 Safe Data Explorer + Server-Side Masking**

Accepted scope includes bounded topic/partition record browsing, offset/timestamp navigation, bounded live tail, key/value/header inspection, JSON/text/binary views, read-only schema-assisted decoding, deterministic bounded filtering, authoritative server-side masking, separate `record.read` / `record.export` permissions, bounded export, hard load budgets, cancellation/isolation, audit, API/UI parity and benchmark evidence.

### Hard invariants

- no Kafka mutation,
- no record production/replay,
- no consumer-offset mutation,
- no payload persistence by default,
- no unbounded scan,
- no arbitrary server-side JavaScript,
- no general-purpose SQL stream engine,
- no masking bypass,
- authorization uncertainty fails closed,
- metadata permissions do not imply payload access.

### Sequencing consequence

Gate 2's capability principles remain accepted, but its original version-number mapping was superseded:

1. Issue #57 advanced Operator Identity/RBAC to v0.2; v0.2 is released.
2. Issue #75 assigns Safe Data Explorer + Server-Side Masking to v0.3.
3. Consumers/Schemas/Ecosystem Read Views move to v0.4.
4. v0.5+ remains unchanged unless a later approved scope gate supersedes it.

Historical Gate 2 text remains unchanged as auditable approval evidence.

### Owner approval

> **Option A — v0.3 Safe Data Explorer + Server-Side Masking تأیید است.**

### Implementation gate

Implementation begins only after the RFC-0003 planning package receives exact-head CI, independent review, resolved required review threads and governed merge.

## Gate 4 — v0.4 Consumers, Schemas & Ecosystem Read Views

- **Date:** 2026-09-21
- **Status:** ACCEPTED
- **Authority:** Project Owner
- **Scope issue:** #99
- **Tracker:** #100
- **RFC:** RFC-0004 planning package

### Accepted decision

Kafdeck v0.4 is **Consumers, Schemas & Ecosystem Read Views**.

Accepted scope includes consumer groups/members/assignments/committed offsets/lag, evidence-based diagnostics, read-only Schema Registry subject/version/reference/diff/compatibility inspection, read-only Kafka Connect status, safe ksqlDB discovery/metadata where configured, topic catalog foundations, API/UI parity, bounded provider isolation and release evidence.

### Implementation decisions

1. v0.4 remains fully non-mutating.
2. New permissions are independent: `consumer.read`, `schema.read`, `connect.read`, `ksql.read`, `catalog.read`.
3. No v0.4 action implies `record.read` or `record.export`.
4. Produce/consume rate data is shown only from an explicit trustworthy metrics provider; otherwise it is unavailable.
5. Historical rebalance/progress analysis is shown only with an explicit history provider; v0.4 has no mandatory durable history store.
6. v0.4 supports at most one optional Connect profile and one optional ksqlDB profile per Kafka cluster; multi-Connect remains later roadmap work.
7. ksqlDB arbitrary statement/query execution is excluded.
8. Ecosystem URLs are deployment-configured, validated origins; there is no generic proxy endpoint.
9. Connector secret-like fields are redacted server-side.
10. Topic catalog foundations are configuration-backed descriptive metadata and do not affect authorization.

### Hard invariants

- no Kafka mutation or produce/replay,
- no consumer-offset mutation/commit,
- no Schema Registry mutation,
- no Connect mutation,
- no ksqlDB query execution/mutation,
- no unbounded polling/listing,
- no arbitrary server-side code execution,
- no masking bypass,
- v0.3 payload authorization/masking/export controls remain unchanged.

### Owner approval

> **v0.4 Consumers / Schemas / Ecosystem Read Views تأیید است؛ طراحی و اجرای کامل آن را شروع کن.**

### Execution

W25–W31 may proceed autonomously under protected-main governance. Dependency-valid stacked work may continue while a review gate is pending; no approval may be fabricated. Tag/publication requires a separate explicit v0.4 release decision.


## Gate 5 — v0.5 Safe Administration & Controlled Mutations

- **Date:** 2026-09-21
- **Status:** SCOPE ACCEPTED
- **Authority:** Project Owner
- **Scope issue:** #121
- **Tracker:** #122
- **RFC:** RFC-0005 planning package

### Accepted decision

Kafdeck v0.5 is **Safe Administration & Controlled Mutations**.

The accepted scope introduces governed topic, record-production, consumer-offset/group, Schema Registry, Kafka Connect and DeleteRecords mutation capabilities behind the common server-authoritative pipeline:

`Request -> Authentication -> Authorization -> Capability Check -> Validation -> Risk Classification -> Preview -> Confirmation/Approval -> Execute -> Verify -> Audit`

### Accepted invariants

- explicit mutation authorization actions; read permission never implies write/admin permission,
- LOW / MODERATE / HIGH / CRITICAL server-owned risk classes,
- preview binding and stale-preview rejection,
- durable idempotency / operation state,
- truthful ambiguous/partial execution semantics,
- no blind retry after potentially successful external dispatch,
- no generic Kafka/provider command tunnel,
- no durable plaintext record-payload or connector-secret staging by default,
- CRITICAL operations require a distinct eligible approver,
- irreversible operations are never represented as generally reversible,
- release/tag/publication remains separately approval-bound.

### Owner approval

> **v0.5 Safe Administration & Controlled Mutations با mutation pipeline، risk classes، hard safety invariants، explicit authorization actions و non-goals این scope تأیید است؛ planning package را کامل کن و implementation را فقط بعد از governed planning admission فعال کن.**

### Implementation gate

Implementation remains blocked until the RFC-0005 planning package receives fresh exact-head required checks, fresh required CODEOWNER approval, resolved required review threads and protected-main merge. After admission, W32-W40 may proceed autonomously under Issue #122. v0.5 release/publication requires a later explicit owner decision.

## Gate 6 — v0.6 Fleet Operations, Broker Maintenance & Kafka Security

- **Date:** 2026-09-23
- **Status:** DETAILED SCOPE ACCEPTED; PLANNING ADMISSION PENDING
- **Authority:** Project Owner, Ammar Heidari
- **Scope issue:** #145
- **Tracker:** #146
- **RFC:** RFC-0006 planning package
- **Gate record:** [Gate 6](gate-6-v0.6-fleet-security.md)

### Accepted decision

The detailed Issue #145 scope and safety boundaries are accepted: typed Kafka ACL/SCRAM/quota administration, evidence-based access analysis, allowlisted dynamically alterable broker/cluster configuration, preferred leadership, reassignment/RF-change planning and progress, supported broker/log-directory maintenance, bounded cross-cluster transfer and tested replication integration.

All inherited v0.5 invariants remain mandatory. CRITICAL requires a distinct eligible principal; read permissions never imply mutations. No generic provider execution, arbitrary code, durable raw payload/secret staging, fabricated broker read-only mode, or uncontrolled cross-cluster exfiltration path is authorized.

### Owner approval

> **v0.6 Fleet Operations, Broker Maintenance & Kafka Security طبق scope و safety boundaries ثبت‌شده در Issue #145 تأیید است؛ planning package را کامل کن و implementation را فقط بعد از governed planning admission شروع کن.**

### Implementation and release gates

Planning is authorized under #146. The [complete planning package](../implementation/v0.6-planning-package.md) must receive fresh exact-head applicable checks, substantive architecture/security review, fresh required CODEOWNER approval, zero unresolved required threads and an expected-head guarded protected-main merge. Only after that admission and post-merge verification may W41-W50 proceed dependency-valid under the tracker.

This scope approval does not approve a release/tag/OCI promotion/publication, v0.7+, or a material departure from Issue #145. Published v0.5/v0.5.1 identities remain untouched. Earlier gate records above remain historical evidence, not substitutes for live implementation/admission status.

## Gate 7 — v0.7 Developer & Streaming Ecosystem Platform

- **Date:** 2026-09-26
- **Status:** SCOPE ACCEPTED; PLANNING ADMISSION PENDING
- **Authority:** Project Owner, Ammar Heidari
- **Scope issue:** #178
- **Tracker:** #180
- **Planning workstream:** #181 / W51
- **RFC:** RFC-0007 planning package
- **Gate record:** [Gate 7](gate-7-v0.7-developer-streaming.md)

### Accepted decision

Kafdeck v0.7 is **Developer & Streaming Ecosystem Platform**.

The accepted scope covers governed Schema Registry lifecycle administration, schema developer tooling, multi-Connect administration, bounded Connect auto-restart, controlled CBOR/XML/MessagePack tooling, finite governed replay/reprocess/forwarding jobs, bounded mock/data generation, read-oriented ksqlDB query/editor integration, evidence-based Streams/state-store/lineage views, and the corresponding operator/developer UX and release-readiness work.

### Accepted invariants

- existing OIDC/RBAC, explicit mutation actions, risk, preview, confirmation/approval, durable operations, fencing, ambiguity and audit remain authoritative;
- no generic Schema Registry/Connect/ksqlDB proxy;
- no CLI/raw-protocol/reflection/sidecar bypass for missing provider primitives;
- provider/connector secrets remain server-side/write-only;
- v0.6 MM2/replication guards apply to all new Connect effect-capable routes;
- replay, forwarding, generation and auto-restart remain finite and hard-bounded;
- automated effects revalidate current authorization/policy before every new dispatch;
- no raw record/generated/query payload persistence by default;
- server-side masking/export boundaries remain non-bypassable;
- ksqlDB persistent-query/DDL/DML mutation is not admitted in v0.7;
- inferred lineage is labelled and cannot grant authority;
- public runtime plugin loading remains excluded;
- release/publication remains separately approval-bound.

### Owner approval

> **v0.7 scope در Issue #178 طبق متن فعلی تأیید است. آماده‌سازی W51 governed planning package مجاز است؛ implementation فقط پس از protected-main planning admission شروع شود.**

### Implementation gate

W51 planning is authorized. W52–W60 implementation remains blocked until the complete RFC-0007 planning package receives fresh exact-head applicable CI, substantive architecture/security review, fresh required CODEOWNER approval, zero unresolved required threads and expected-head guarded protected-main merge, followed by post-merge verification and tracker activation.

### Release gate

Planning admission and implementation completion do not authorize publication. v0.7 tag/GHCR/GitHub Release promotion requires a separate explicit owner decision.



## Gate 8 — v0.8 Observability, Automation & Platform APIs

- **Date:** 2026-09-27
- **Status:** SCOPE ACCEPTED; PLANNING ADMISSION PENDING
- **Authority:** Project Owner, Ammar Heidari
- **Scope issue:** #208
- **Tracker:** #209
- **Planning workstream:** #210 / W61
- **Gate record:** [Gate 8](gate-8-v0.8-observability-automation.md)

### Accepted decision

Kafdeck v0.8 is **Observability, Automation & Platform APIs**.

The accepted scope adds bounded OpenTelemetry/Prometheus observability, explicit historical metrics, operational analytics/SLOs, bounded data-quality monitoring, typed notifier/webhook delivery, REST/CLI parity, declarative GitOps/Terraform management, MCP product tools and safe compensation semantics.

### Accepted invariants

- no secret/raw protected payload leakage through telemetry/alerts;
- no unbounded high-cardinality telemetry;
- no arbitrary outbound notifier/exporter endpoint;
- no privileged CLI/Terraform/GitOps/MCP bypass;
- explicit automation principals and effect-time authorization revalidation;
- CRITICAL distinct-principal approval preserved;
- no generic automation scripting/shell/raw provider execution;
- no generic rollback of irreversible effects;
- release/publication separately owner-gated.

### Owner approval

> **v0.8 scope در Issue #208 طبق متن فعلی تأیید است؛ W61 governed planning package را شروع کن. implementation فقط بعد از protected-main planning admission مجاز است.**

### Implementation gate

W62–W71 remain blocked until W61 is admitted through protected-main governance.


## Gate 8 delivery and separate v0.8 publication — 2026-11-05

- **Status:** W61–W71 / G0–G6 COMPLETE; v0.8 PUBLISHED.
- **Authority:** Project owner, explicit Codex chat publication decision.
- **Accepted source:** `cbedd025ab97db10fb21407ea5188c08d6a4d8ce`; G5/tracker #220/#209 closed after all 14 main producer workflows and the complete 124-file strict Main packet passed.
- **Owner-reviewed archive SHA256:** `6c333ee26e8d433ea49faf7d578238b9db5577656959c31404ce032fbad6bd34`.

The owner answered the exact-SHA/archive publication request:

> انتشار همین SHA را تأیید می‌کنم

[Publication run41986146327](https://github.com/araditc/Kafdeck/actions/runs/41986146327) published [v0.8](https://github.com/araditc/Kafdeck/releases/tag/v0.8), immutable source tag and signed image `ghcr.io/araditc/kafdeck@sha256:7b64f67b7f050b3c8cb5c2678b6ce94f6886e854cebfca6d6730170164fedb45` from that archive without rebuilding. Five Terraform ZIPs, MCP nupkg and SHA256SUMS were checked. Registry digest/config and cosign signature/readiness/SPDX verification succeeded with owner-authenticated access; GHCR retains its existing private policy. The first malformed-input dispatch stopped before publication; the corrected structured-input execution used the identical approved source/archive.

Original Gate 8 scope and planning-admission entries above remain historical records. Detailed evidence: [accepted v0.8 release evidence](../implementation/evidence/v0.8-g5-accepted-20261105.json). This decision does not authorize production deployment, Terraform/NuGet Registry publication, or detailed v0.9 scope/implementation/publication.

## Gate 9 — v0.9 full scope and W72 authorization — 2026-11-05

- Status: SCOPE ACCEPTED; W72 PREPARATION ACTIVE; PLANNING ADMISSION PENDING.
- Authority: Project owner, explicit Codex chat answer to complete scope #424 and W72 preparation request.
- Scope: [#424](https://github.com/araditc/Kafdeck/issues/424); tracker [#425](https://github.com/araditc/Kafdeck/issues/425); W72 [#426](https://github.com/araditc/Kafdeck/issues/426).
- Decision record: [Gate 9](gate-9-v0.9-governance-hardening.md); [RFC-0009](../rfcs/0009-v0.9-governance-enterprise-hardening.md).

> تأیید کامل؛ شروع W72

All W72–W82 themes remain accepted: catalog, requests/approvals, policy, measured attribution/dashboards, hardening/compatibility, independent security review, accessibility/i18n/Persian, upgrade/restore/air-gap, OCI/native/Helm and measured budgets/readiness. Runtime implementation starts only after complete governed planning admission on protected main and successful applicable main checks. New authority/bypass, arbitrary execution, production chaos/deployment, public runtime plugins and publication are not authorized. Exact-SHA/artifact publication remains separately owner-gated. v0.8 release identity and registry access remain unchanged.

### W72 planning admission — 2026-11-05

PR #437 was approved by CODEOWNER `akhiabanchian` on exact head `a6bfc207d025b2114488e08c537d3d4e460fdcbf` with all 14 checks successful and no unresolved threads. The merged tree at `d8d9e6319b536760993d840ce084dfc832c543de` equals that reviewed tree; all seven applicable main workflows succeeded. W72 #426 closed and tracker #425 activated dependency-valid lanes. The reviewed package contains the author architecture/security assessment; W78 independent implemented-system review remains required. This admission authorizes implementation under the complete scope and preserves separate runtime acceptance and publication gates.

### W78 KAF-02 conditional residual-risk acceptance — 2026-11-11

The project owner accepted deployment-enforced egress as the KAF-02 mitigation, conditional on a successful egress test for each deployment and explicit support boundaries.

> پذیرش مشروط به egress آزموده‌شده

The [independent technical review and disposition](../security/v0.9-w78-independent-review.md) record the reviewed source, reviewer independence, evidence and topology limitations. This decision does not substitute for the remaining specialist adoption, artifact or publication gates.

### W82 numerical budget admission — 2026-11-11

The project owner explicitly accepted the complete measured proposal with SHA256 `049d4633741ec1c805f65a0a5fa65dcc346f852b61e8698501e7ac8338d7f3c6`, including observed-worst steady and soak bounds, zero errors or HTTP 429, separate CPU and binary baselines per provider, and the exact native-only default-build-equivalent snapshot.

> این حدود و هم‌ارزی را می‌پذیرم

The [versioned limits](../performance/v0.9-budget-candidate.json) retain the accepted numbers and identities. The baseline default-build fingerprint is `30188143a7d6dd579ed518ee356a7854f9570be2fb6e057a86f9f98b80b96238`. The explicitly equivalent native-only input fingerprint is `8e5bba002f1e620bff82d1d1d2fe88b8a26eb81b21a6be2455f382c26ebc010a`. Their delta adds the conditional native restore target and 95 RID lockfiles, with no change to existing default build configuration or portable dependency graphs. A different source, binary, CPU or build-input snapshot requires its own admitted evidence. This acceptance grants no signature, registry push, tag or release authorization.


## v0.9 official publication — 2026-11-12

The owner explicitly authorized official v0.9 publication after final CI, adopting the final W80/W81 acceptance and locked publisher proposal `0d4efb89cb4db0d44b33d3e046e811678d6b5674621465e846f8b950bf0c923a` for selection `96725a495baaf3e29aac0dd1788b291c45777459218f37a3bdead9328dcdcebf`. Publisher head `1abb0a89b37ab80630766a2188828cdbfe1d378f` had fresh CODEOWNER approval and 16 successful checks.

Protected-main readiness [43230886146](https://github.com/araditc/Kafdeck/actions/runs/43230886146) admitted the exact source, manifest, trust root and W80 bytes with `releaseReady: true` and no missing gate. Publication [43232262853](https://github.com/araditc/Kafdeck/actions/runs/43232262853) published [v0.9](https://github.com/araditc/Kafdeck/releases/tag/v0.9) at source `01c7553425fbc61ba4a7a21918265c562086ba02`, independently downloaded/hash-checked all 51 remote assets and verified the exact registry signatures/attestations. Private GHCR access, original offline media/signature and the KAF-02 deployment-tested-egress condition remain. [Public release evidence](../implementation/evidence/v0.9-release-published-20261112.json)
