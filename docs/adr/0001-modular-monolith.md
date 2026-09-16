# ADR-0001: Use a Modular Monolith

- Status: Accepted
- Date: 2026-09-16
- Gate: 0

## Context

Kafdeck is an OSS control plane that should be simple to deploy in developer, on-premise, and air-gapped environments. Premature service decomposition would add network, deployment, consistency, observability, and contributor complexity before those costs are justified.

## Decision

Kafdeck begins as a modular monolith with explicit module contracts and inward dependency direction.

## Consequences

Positive:
- single/simple deployment,
- lower operational burden,
- easier OSS contribution and local development,
- transaction and configuration simplicity,
- module boundaries can still support future extraction.

Constraints:
- modules may not bypass contracts to modify each other's persistence,
- infrastructure/vendor SDK types may not leak into domain/application contracts,
- future service extraction requires evidence and a new RFC/ADR.
