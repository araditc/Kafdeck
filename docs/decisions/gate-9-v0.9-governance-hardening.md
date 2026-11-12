# Gate 9 — v0.9 Governance, Enterprise Operations & Hardening

- Date: 2026-11-05
- Status: RELEASED v0.9 on 2026-11-12; full W72–W82 acceptance retained
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

## Completed admission and publication

W72 admission completed through #437; subsequent W73–W82 implementation, security, accessibility, recovery, signed distribution and numerical budgets were admitted through the linked execution/evidence records. The owner accepted KAF-02 conditionally on deployment-tested egress and accepted the exact W82 limits/build equivalence. On 2026-11-12 the owner separately authorized official v0.9 publication after green final CI, including adoption of the final W80/W81 packets and locked publisher identity.

Protected-main readiness [43230886146](https://github.com/araditc/Kafdeck/actions/runs/43230886146) returned `releaseReady: true` with no missing gates. Publication [43232262853](https://github.com/araditc/Kafdeck/actions/runs/43232262853) published tag/release v0.9 at `01c7553425fbc61ba4a7a21918265c562086ba02`, verified all 51 remote assets and exact registry signatures/attestations, preserving private GHCR access. [Exact acceptance/publication facts](../implementation/evidence/v0.9-release-published-20261112.json)

The initial scope decision did not authorize publication; that separate decision is now recorded above. It still grants no blanket provider/CNI support, registry visibility change or automatic production deployment. Historical v0.8 artifacts remain unchanged.
