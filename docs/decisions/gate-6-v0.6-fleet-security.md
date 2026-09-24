# Gate 6 — v0.6 Fleet Operations, Broker Maintenance & Kafka Security

- Date: 2026-09-23
- Status: **DETAILED SCOPE ACCEPTED; PLANNING ADMISSION PENDING**
- Authority: Ammar Heidari, Project Owner
- Scope issue: [#145](https://github.com/araditc/Kafdeck/issues/145)
- Planning/implementation tracker: [#146](https://github.com/araditc/Kafdeck/issues/146)
- Baseline: `e5de0ec142d5dd06ef52f59718c60c3109d1070a`

## Explicit owner approval

> v0.6 Fleet Operations, Broker Maintenance & Kafka Security طبق scope و safety boundaries ثبت‌شده در Issue #145 تأیید است؛ planning package را کامل کن و implementation را فقط بعد از governed planning admission شروع کن.

English interpretation: the detailed Issue #145 scope and safety boundaries are approved; complete the planning package; begin implementation only after governed planning admission. The original wording above is retained as approval evidence; repository design prose remains English.

## Accepted capability boundary

Typed Kafka ACL/SCRAM/quota administration and evidence-based access analysis; proven dynamically alterable allowlisted broker/cluster configuration; preferred leader election; explicit replica reassignment/RF-change planning, throttling/progress/estimates/skew; supported broker/log-directory cordon/decommission; finite cross-cluster transfer/clone and tested MirrorMaker 2/replication integration.

All RFC-0005 safety invariants are inherited. New security/maintenance actions remain deny-by-default and server-risked; CRITICAL requires a distinct eligible principal. Secrets are write-only/ephemeral. Cross-cluster data movement requires source read/export, destination produce and explicit transfer requirements, never an exfiltration bypass. There is no generic provider/CLI/REST surface, arbitrary code, hidden durable raw material, mandatory proxy or fabricated broker read-only mode.

## Consequences and open gates

The scope-definition gate is complete; #146 owns planning admission. [RFC-0006](../rfcs/0006-v0.6-fleet-operations-kafka-security.md) and the [package index](../implementation/v0.6-planning-package.md) describe the proposed design. W41–W50 remain inactive until this package is protected-main admitted with fresh exact-head checks, required reviews and resolved threads.

The owner has not authorized a v0.6 release, tag, OCI promotion, publication, v0.7 activation or a material expansion/weakening of #145. Existing v0.5/v0.5.1 releases remain unchanged. API/client capability gaps are implementation evidence gates; they must be surfaced rather than hidden behind unreviewed generic execution or false completion claims.
