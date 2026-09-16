# ADR-0004: Persistence Provider Strategy

- Status: Accepted
- Date: 2026-09-16
- Gate: 0

## Context

Kafdeck needs a low-friction standalone deployment while also supporting durable multi-user production environments. Coupling the application to a single database would make deployment unnecessarily rigid.

## Decision

Kafdeck owns a persistence abstraction with:
- SQLite as the default/standalone provider,
- PostgreSQL as the production-oriented provider.

Kafka record payloads are not application persistence data and are not stored by default.

## Consequences

Persistence migrations must be deterministic and tested against every supported provider. Module boundaries remain independent of provider-specific database features unless an ADR explicitly accepts such coupling.
