# Gate 9 — v0.9 Governance, Enterprise Operations & Hardening

- Date: 2026-11-05
- Status: SCOPE ACCEPTED; W72 PREPARATION ACTIVE; PLANNING ADMISSION PENDING
- Authority: Project owner, explicit Codex chat decision
- Scope: [#424](https://github.com/araditc/Kafdeck/issues/424)
- Tracker: [#425](https://github.com/araditc/Kafdeck/issues/425); planning [W72 #426](https://github.com/araditc/Kafdeck/issues/426)

## Owner decision

The owner accepted the complete proposed W72–W82 scope and authorized W72:

> تأیید کامل؛ شروع W72

## Accepted scope and boundaries

Ownership catalog, self-service/approvals, policy-as-code, measured attribution/dashboards, scale/soak/isolated faults/compatibility, threat validation/independent security review, WCAG/i18n/Persian, migration/backup/restore/air-gap, OCI/self-contained .NET/Helm, measured budgets/regression/readiness are all retained. [Execution plan](../implementation/v0.9-full-scope-delivery-execution-plan.md) maps every theme to an issue and evidence gate.

Catalog, approvals, policies and model prompts never grant RBAC. Preserve original actor/current effect-time authority, critical distinct-human approval, idempotency, fencing, audit and honest unknown recovery. No arbitrary execution/proxy, payload/secret persistence, payments, production chaos, production deployment or blanket support claims. Public runtime plugins remain deferred under the Roadmap extension policy.

## Admission

Owner scope authorization permits preparing W72. W73–W82 remain blocked until the complete planning package passes exact-head applicable CI, substantive architecture/security review, fresh required CODEOWNER approval, zero unresolved required threads, expected-head guarded protected-main merge, applicable main verification and tracker activation. RFC/ADR proposals become admitted only through that recorded gate. Preparation or passing documentation checks alone is not admission.

## Open gates

- W72 planning PR and protected-main admission evidence.
- Implementation and actual complete W73–W82 acceptance.
- Separate owner exact-SHA/artifact publication decision.

Released v0.8 remains unchanged; this approval does not authorize v0.9 publication, registry visibility changes or production deployment.
