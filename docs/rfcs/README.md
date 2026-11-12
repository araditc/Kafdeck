# Requests for Comments

RFCs are used for substantial design work before implementation when a decision is not yet accepted.

## RFC triggers

Examples:
- major subsystem or product capability,
- plugin/provider architecture,
- public API compatibility contract,
- new persistence technology,
- identity/authorization-model change,
- telemetry-policy change,
- deployment-model change,
- Kafka compatibility-policy change,
- critical-operation approval workflow.

## Lifecycle

`Draft -> Review -> Accepted/Rejected -> Implemented/Superseded`

Implementation should not outrun an unresolved RFC when the RFC is required by governance.


## RFC index and released disposition

The original RFC headers preserve status at drafting/admission time. The release links below establish delivered scope; they do not claim every long-term target was implemented or rewrite old proposal decisions. See the [current availability guide](../product/current-capabilities.md).

| RFC | Released scope / current disposition |
| --- | --- |
| [RFC-0001 — v0.1 Cluster Explorer](0001-v0.1-cluster-explorer.md) | [v0.1 release](../releases/v0.1.md); original design/admission record retained |
| [RFC-0002 — v0.2 Operator Identity and RBAC](0002-v0.2-operator-identity-rbac.md) | [v0.2 release](../releases/v0.2.md); original design/admission record retained |
| [RFC-0003 — v0.3 Safe Data Explorer and Server-Side Masking](0003-v0.3-safe-data-explorer.md) | [v0.3 release](../releases/v0.3.md); original design/admission record retained |
| [RFC-0004 — v0.4 Consumers, Schemas & Ecosystem Read Views](0004-v0.4-consumers-schemas-ecosystem-read-views.md) | [v0.4 release](../releases/v0.4.md); original design/admission record retained |
| [RFC-0005 — v0.5 Safe Administration & Controlled Mutations](0005-v0.5-safe-administration-controlled-mutations.md) | [v0.5 release](../releases/v0.5.md); original design/admission record retained |
| [RFC-0006 — v0.6 Fleet Operations, Broker Maintenance & Kafka Security](0006-v0.6-fleet-operations-kafka-security.md) | [v0.6 release](../releases/v0.6.md); original design/admission record retained |
| [RFC-0007 — v0.7 Developer & Streaming Ecosystem Platform](0007-v0.7-developer-streaming-ecosystem.md) | [v0.7 release](../releases/v0.7.md); original design/admission record retained |
| [RFC-0008 — v0.8 Observability, Automation & Platform APIs](0008-v0.8-observability-automation-platform-apis.md) | [v0.8 release](../releases/v0.8.md); original design/admission record retained |
| [RFC-0009 — v0.9 Governance, Enterprise Operations & Hardening](0009-v0.9-governance-enterprise-hardening.md) | [v0.9 release](../releases/v0.9.md); original design/admission record retained |

v0.9 W72 admission: #437 at `d8d9e6319b536760993d840ce084dfc832c543de`. Full W72-W82 acceptance and publication are closed in the [execution ledger](../implementation/v0.9-full-scope-delivery-execution-plan.md#final-closure-ledger).
