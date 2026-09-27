# ADR-0009 — Automation Principals, Telemetry Authority and Declarative Ownership

- **Status:** PROPOSED
- **Scope:** v0.8
- **Authority:** #208 / #209 / W61 #210

## Context

v0.8 introduces background automation and non-browser clients. Without a single authority model, CLI/GitOps/Terraform/MCP or alert automation could become a privileged bypass.

Observability also introduces new outbound surfaces where secrets or protected data could leak.

## Decision

### 1. Explicit principals

Every API/CLI/GitOps/Terraform/MCP/background effect runs as an explicit principal recognized by the existing authentication/authorization model.

No durable automation stores a transferable browser session as standing authority.

### 2. Fresh effect revalidation

Before every new external effect, Kafdeck revalidates:

- principal eligibility;
- exact action/resource authorization;
- deployment policy;
- provider/resource identity;
- capability state;
- immutable preview/precondition fingerprints;
- conflict/fencing state;
- remaining finite budget;
- unresolved earlier dispatch state.

### 3. Observability has no authority

Metrics/traces/logs/SLO state are evidence. They do not authorize mutation.

An alert can create a proposed governed operation only through an explicitly admitted typed automation rule; the resulting operation still follows ordinary risk/approval rules.

### 4. Declarative ownership

Each declarative resource family defines one ownership mode:

- ObservedOnly;
- KafdeckManaged;
- ExternallyManaged;
- Adoptable.

Import/adoption requires exact current-state identity and does not silently seize ownership.

### 5. Declarative plans are immutable server objects

A declarative plan has a server-issued `planId`, immutable plan fingerprint, finite expiry and exact current-state/precondition fingerprints.

Apply binds to that exact plan. Expired, drifted or mismatched plans fail closed and require a newly generated plan; clients cannot submit a plan ID while changing desired state/effects out of band.

### 6. Desired state excludes plaintext secrets

Desired state contains:

- ordinary normalized values;
- secret references;
- write-only placeholders where unavoidable.

Readback never returns secret bytes.

### 7. MCP parity

MCP tools are typed projections of versioned Kafdeck APIs. Tool metadata/prompt content cannot strengthen authority.

### 8. Compensation is independently governed

Compensation is represented as a new immutable operation, not a mutation-history rewind.

## Consequences

- clients can evolve independently while server policy stays authoritative;
- headless automation may legitimately stop at AwaitingApproval;
- declarative workflows must model asynchronous governed operations;
- there is no generic Terraform/MCP/CLI provider escape route;
- telemetry leaks are treated as security boundary violations;
- every new background effect is auditable to a principal and policy version.
