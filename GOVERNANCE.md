# Kafdeck Governance

This document defines decision authority, contribution governance, architectural control, and approval gates for Kafdeck.

## 1. Governance objectives

Kafdeck governance exists to keep the project:
- technically coherent,
- secure by default,
- easy to contribute to,
- transparent in decision-making,
- vendor-neutral,
- maintainable over a long OSS lifecycle.

## 2. Official project language

The official language for repository documentation, source comments intended for maintainers, ADRs, RFCs, issues used for engineering decisions, pull-request descriptions, and release notes is **English**.

Translations may be added later, but the English version is authoritative unless a future RFC changes this rule.

## 3. Roles

### Maintainer
A maintainer may review and merge ordinary changes, triage issues, and enforce project standards.

### Architecture Maintainer
An architecture maintainer additionally reviews ADR/RFC changes, module boundaries, public contracts, data models, and compatibility decisions.

### Security Maintainer
A security maintainer additionally reviews threat models, authentication/authorization, secret handling, audit, dependency risk, and security-sensitive changes.

### Contributor
Any person contributing code, tests, documentation, design, or review feedback under the project contribution rules.

### Product Owner / Final Approval Authority
Material product direction and project-level approval gates are accepted by the project owner. Accepted approvals must be recorded in the repository approval log and, where applicable, reflected in ADR/RFC status.

## 4. Decision classes

### Routine implementation decision
May be resolved in a PR when it does not change approved architecture, security posture, compatibility guarantees, or public behavior.

### Architecture Decision Record (ADR)
Required for a durable architectural decision. ADRs are immutable in history; superseding decisions create a new ADR and reference the superseded record.

### Request for Comments (RFC)
Required before implementing a substantial capability or a change with broad design impact when the design is not yet accepted.

Typical RFC triggers:
- new major subsystem,
- plugin architecture,
- public API contract,
- new persistent store,
- authorization-model change,
- deployment-model change,
- compatibility-policy change,
- telemetry-policy change,
- critical-operation approval workflows.

## 5. Approval gates

A gate record must include:
- gate identifier,
- date,
- scope,
- accepted decisions,
- open items if any,
- approval status.

An accepted gate is versioned in `docs/decisions/approval-log.md`.

### Gate 0 — Product/Engineering Baseline
Covers product positioning, roadmap, technology baseline, architecture style, compatibility, persistence, open-source governance, security baseline, and documentation language.

### Future gates
Subsequent gates are created when a phase needs product-owner acceptance before implementation or release. The exact gate names are documented in the roadmap or relevant RFC.

## 6. Pull-request governance

All non-bootstrap repository changes use pull requests.

Direct push to `main` is prohibited by policy and should be technically blocked by repository rules as soon as Phase 0 repository configuration is implemented.

A PR must document, where relevant:
- why the change exists,
- what changes,
- architecture impact,
- security impact,
- compatibility/breaking impact,
- test evidence,
- documentation impact,
- migration impact.

No PR may merge with unresolved required review threads or failing required checks.

Architecture- or security-sensitive changes require the relevant specialist review in addition to ordinary review.

## 7. Merge model

Default merge method: **squash merge**.

`main` must remain releasable.

Before merge, the exact PR head must be revalidated. If the head moves after approval or validation, required checks/reviews must be rerun according to repository rules.

## 8. Open-source contribution model

License: **Apache License 2.0**.

Contribution attestation: **Developer Certificate of Origin (DCO)** rather than a CLA for the initial governance model.

Dependency licenses:
- Generally acceptable: Apache-2.0, MIT, BSD, ISC and similarly permissive licenses.
- Review required: LGPL/MPL and reciprocal licenses with distribution implications.
- Explicit approval required: GPL/AGPL or unclear/custom licenses.

## 9. Security governance

Security-sensitive findings must not be disclosed through a public issue when responsible disclosure is appropriate. `SECURITY.md` defines reporting expectations.

Security architecture decisions that affect trust boundaries, identity, authorization, secrets, payload exposure, or audit require security review.

## 10. Governance changes

Material governance changes require a PR and an accepted ADR or RFC when they alter decision authority, merge controls, licensing, contribution attestation, or release/security policy.
