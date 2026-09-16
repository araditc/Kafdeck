# Testing Standards

Kafdeck uses risk-based, layered testing. Mock-heavy tests are not considered sufficient evidence for Kafka integration behavior.

## Test layers

### Unit tests
Fast, deterministic tests for pure domain/application logic and isolated frontend logic.

### Component/module tests
Validate a module with controlled adapters while preserving realistic internal wiring.

### Integration tests
Validate database, authentication/authorization boundaries, HTTP contracts, and infrastructure adapters.

### Kafka integration tests
Run against real containerized Kafka for behavior that depends on Kafka protocol/admin semantics. Mock Kafka is acceptable for unit tests but not as the only validation for Kafka integration code.

### Contract tests
Validate OpenAPI/API behavior and provider adapters where external contracts exist.

### Frontend tests
Include unit/component tests and targeted end-to-end paths. Accessibility checks are required for shared UI primitives and critical workflows.

### Security tests
Cover authorization-denial paths, privilege boundaries, input validation, secret redaction, and destructive-operation safeguards.

### Performance/scale tests
Required for polling, large-topic/partition views, record browsing, consumer-lag aggregation, and other potentially expensive flows.

## Required properties

Tests must be:
- deterministic,
- isolated from the public Internet,
- repeatable locally/CI where practical,
- explicit about timeouts,
- free of production credentials/data.

## Kafka compatibility matrix

The CI matrix must test the Kafka versions currently declared as supported tiers. A compatibility claim may not exist only in documentation; representative automated integration coverage is required.

## Regression rule

Every defect fix should add a regression test when the failure can be reproduced in an automated test without unreasonable cost.

## Flaky tests

Flaky tests are defects. Quarantining a test requires a tracked issue, explicit reason, and owner; quarantine may not become a permanent substitute for fixing it.

## Merge gate

Required checks must pass on the exact PR head intended for merge. If the head changes, stale validation is not considered evidence for the new head.
