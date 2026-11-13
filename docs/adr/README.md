# Architecture Decision Records

ADRs record durable architecture decisions.

## Naming

`NNNN-short-title.md`

## Required sections

- Title
- Status (`Proposed`, `Accepted`, `Superseded`, `Rejected`)
- Date
- Context
- Decision
- Consequences
- References/related RFCs where relevant

Accepted ADRs are not rewritten to hide history. A later change creates a new ADR and marks the previous ADR as superseded.

## Decision index

Original headers preserve their recorded status/date. Proposed headers in older design records are not silently promoted by this index; implementation/release disposition is linked separately. [RFCs](../rfcs/README.md) and [release notes](../releases/) supply scope/admission context.

- [ADR-0001: Use a Modular Monolith](0001-modular-monolith.md).
- [ADR-0002: Technology Stack Baseline](0002-technology-stack.md).
- [ADR-0003: Kafka Compatibility and ZooKeeper Policy](0003-kafka-compatibility.md).
- [ADR-0004: Persistence Provider Strategy](0004-persistence.md).
- [ADR-0005: Open-Source License and Contribution Attestation](0005-open-source-governance.md).
- [ADR-0006: Durable Mutation Operation State](0006-durable-mutation-operations.md).
- [ADR-0007 — Durable Fleet Progress under One Mutation Authority](0007-durable-fleet-operation-progress.md).
- [ADR-0008 — Governed Ecosystem Execution and Stable Provider Profiles](0008-governed-ecosystem-execution.md).
- [ADR-0009 — Automation Principals, Telemetry Authority and Declarative Ownership](0009-automation-principals-telemetry-declarative-ownership.md).
- [ADR-0010 — Governance at the existing effect boundary](0010-governance-effect-boundary.md).
- [ADR-0011 — Managed-state format freeze and verified key-context restore](0011-managed-state-format-freeze.md) (proposed for G06 admission; tracked in #469).

ADR-0009 was admitted through W61 #221; ADR-0010 through W72 #437. Their headers retain exact main admission identities. Current architecture is summarized in the [architecture overview](../architecture/architecture.md).
