# Engineering Standards

These standards are mandatory unless an approved ADR/RFC explicitly overrides them.

## 1. General engineering rules

- Prefer simple, explicit designs over framework-heavy abstractions.
- Avoid speculative generalization; abstractions require a real boundary or demonstrated duplication.
- Keep modules cohesive and dependencies directed inward.
- Make failure modes explicit.
- Never trade Kafka-cluster safety for UI convenience.
- Security and observability are part of feature design, not post-implementation tasks.

## 2. Backend rules (.NET)

- Target .NET 10 LTS unless an ADR changes the baseline.
- Nullable reference types remain enabled.
- Treat compiler/analyzer warnings as engineering debt; CI should progressively enforce warnings-as-errors for project-owned code.
- Public/internal contracts use cancellation-aware async APIs for I/O.
- Avoid sync-over-async and unbounded `Task.WhenAll` over Kafka resources.
- Use `CancellationToken` for Kafka/network/database operations.
- Use dependency injection at module composition boundaries; avoid service-locator patterns.
- Domain/application code must not depend directly on `Confluent.Kafka` types.
- Exceptions are not a normal control-flow mechanism.
- Secrets and Kafka payloads must not appear in logs/exceptions.
- UTC is used for persisted/transport timestamps unless a contract explicitly states otherwise.

## 3. Frontend rules (React/TypeScript)

- TypeScript strict mode is required.
- Avoid `any` except at explicitly validated external boundaries.
- API models are generated or centrally typed from contracts; do not hand-duplicate shapes across unrelated features.
- Server state and local UI state must remain conceptually distinct.
- Large lists/tables require pagination or virtualization.
- Pages must not generate N+1 API/Kafka access patterns.
- Loading, empty, partial-error, denied, and stale-data states must be designed explicitly.
- Keyboard accessibility and WCAG 2.2 AA are the target.

## 4. API rules

- REST APIs are documented through OpenAPI 3.1.
- Errors use RFC 9457-compatible Problem Details semantics.
- Publicly committed contracts require explicit versioning policy before compatibility guarantees are made.
- IDs and opaque tokens are not overloaded with presentation meaning.
- Collection endpoints are bounded and paginated where result size can grow.
- Expensive operations expose limits and cancellation behavior.

## 5. Kafka interaction rules

Every Kafka call path must consider:
- timeout,
- cancellation,
- concurrency limit,
- retry safety/idempotency,
- pagination/bounds,
- caching/freshness,
- authorization,
- audit when mutating,
- potential cluster load.

Retries are limited to clearly transient failures. Destructive/mutating operations are never blindly retried when outcome ambiguity could duplicate or compound the action.

## 6. Dependency rules

- Minimize dependencies.
- New runtime dependencies require justification in the PR.
- Security/maintenance status and license compatibility must be reviewed.
- Dependency versions are pinned/locked according to ecosystem best practices.
- Do not introduce GPL/AGPL/custom restrictive licenses without explicit approval.

## 7. Logging and observability

- Structured logging only for application events.
- Correlation/request identifiers flow across application boundaries.
- OpenTelemetry is the standard instrumentation model.
- Do not log passwords, tokens, private keys, raw connection strings with credentials, or Kafka record payloads.
- Metrics must avoid uncontrolled cardinality (for example, raw topic names as labels require deliberate review).

## 8. Performance rules

- Performance-sensitive work requires measurable budgets or benchmarks.
- Avoid unbounded memory buffering of Kafka records or metadata collections.
- Use streaming/bounded processing where data size can be large.
- Any feature that polls Kafka must define minimum/maximum interval and concurrency behavior.
- Performance regressions identified by repeatable benchmarks block release when they materially affect supported usage.

## 9. Configuration

- Defaults are safe.
- Security weakening must be opt-in and visible.
- Environment variables/config files may reference secrets but must not encourage storing secrets in version control.
- Configuration changes that alter security or destructive-operation behavior require documentation.

## 10. Code review focus

Reviewers assess:
- correctness,
- architecture boundaries,
- security,
- failure behavior,
- Kafka load impact,
- test evidence,
- observability,
- compatibility/migration impact,
- maintainability.

## 11. Definition of Done

Where applicable, a change is Done only when it includes:
- implementation,
- appropriate automated tests,
- error/failure handling,
- authorization/security controls,
- observability,
- documentation,
- accessibility for UI changes,
- compatibility/migration analysis.
